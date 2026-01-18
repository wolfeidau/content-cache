package pypi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/store"
)

const (
	// cacheTimeout is the maximum time allowed for background caching operations.
	cacheTimeout = 5 * time.Minute
)

// Handler implements the PyPI Simple Repository API as an HTTP handler.
type Handler struct {
	index       *Index
	store       store.Store
	upstream    *Upstream
	logger      *slog.Logger
	metadataTTL time.Duration

	// Lifecycle management for background goroutines
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithLogger sets the logger for the handler.
func WithLogger(logger *slog.Logger) HandlerOption {
	return func(h *Handler) {
		h.logger = logger
	}
}

// WithUpstream sets the upstream PyPI client.
func WithUpstream(upstream *Upstream) HandlerOption {
	return func(h *Handler) {
		h.upstream = upstream
	}
}

// WithMetadataTTL sets the TTL for cached project metadata.
func WithMetadataTTL(ttl time.Duration) HandlerOption {
	return func(h *Handler) {
		h.metadataTTL = ttl
	}
}

// NewHandler creates a new PyPI Simple API handler.
func NewHandler(index *Index, store store.Store, opts ...HandlerOption) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		index:       index,
		store:       store,
		upstream:    NewUpstream(),
		logger:      slog.Default(),
		metadataTTL: 5 * time.Minute,
		ctx:         ctx,
		cancel:      cancel,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Close shuts down the handler and waits for background operations to complete.
func (h *Handler) Close() {
	h.cancel()
	h.wg.Wait()
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path

	// Route: /simple/ - root index
	if path == "/simple/" || path == "/simple" {
		if path == "/simple" {
			http.Redirect(w, r, "/simple/", http.StatusMovedPermanently)
			return
		}
		h.handleRoot(w, r)
		return
	}

	// Route: /simple/{project}/ - project page
	if strings.HasPrefix(path, "/simple/") {
		projectPath := strings.TrimPrefix(path, "/simple/")
		projectPath = strings.TrimSuffix(projectPath, "/")

		// Redirect if missing trailing slash
		if !strings.HasSuffix(path, "/") {
			http.Redirect(w, r, path+"/", http.StatusMovedPermanently)
			return
		}

		h.handleProject(w, r, projectPath)
		return
	}

	// Route: /packages/{path...} - file download
	if strings.HasPrefix(path, "/packages/") {
		h.handleFile(w, r)
		return
	}

	http.NotFound(w, r)
}

// handleRoot handles the root index listing all cached projects.
func (h *Handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := h.logger.With("endpoint", "root")

	projects, err := h.index.ListProjects(ctx)
	if err != nil {
		logger.Error("failed to list projects", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	sort.Strings(projects)

	// Check Accept header for content negotiation
	if wantsJSON(r) {
		h.writeRootJSON(w, projects)
	} else {
		h.writeRootHTML(w, projects)
	}
}

// handleProject handles project page requests.
func (h *Handler) handleProject(w http.ResponseWriter, r *http.Request, project string) {
	ctx := r.Context()
	normalized := NormalizeProjectName(project)
	logger := h.logger.With("project", normalized, "endpoint", "project")

	// Try cache first
	cached, err := h.index.GetCachedProject(ctx, normalized)
	if err == nil && !h.index.IsExpired(cached, h.metadataTTL) {
		logger.Debug("cache hit")
		h.writeProjectResponse(w, r, cached, normalized)
		return
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		logger.Error("cache read failed", "error", err)
	}

	// Fetch from upstream
	logger.Debug("cache miss or expired, fetching from upstream")
	body, contentType, err := h.upstream.FetchProjectPage(ctx, normalized)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		logger.Error("upstream fetch failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	// Parse the response to extract files
	var files []ParsedFile
	if strings.Contains(contentType, "json") {
		// Parse JSON response
		var page ProjectPage
		if err := json.Unmarshal(body, &page); err != nil {
			logger.Error("failed to parse JSON response", "error", err)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		for _, f := range page.Files {
			files = append(files, ParsedFile{
				Filename:       f.Filename,
				URL:            f.URL,
				Hashes:         f.Hashes,
				RequiresPython: f.RequiresPython,
				Yanked:         f.Yanked,
			})
		}
	} else {
		// Parse HTML response
		baseURL := h.upstream.baseURL + normalized + "/"
		files, err = ParseProjectPageHTML(body, baseURL)
		if err != nil {
			logger.Error("failed to parse HTML response", "error", err)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
	}

	// Create cached project from parsed files
	cached = &CachedProject{
		Name:    normalized,
		Files:   make(map[string]*CachedFile),
		RawPage: body,
	}
	for _, f := range files {
		cached.Files[f.Filename] = &CachedFile{
			Filename:       f.Filename,
			Hashes:         f.Hashes,
			RequiresPython: f.RequiresPython,
			UpstreamURL:    f.URL,
		}
	}

	// Cache metadata asynchronously
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		// Use context.Background() for cache operations that should complete even during shutdown.
		// The WaitGroup ensures we wait for this to finish before Close() returns.
		cacheCtx, cancel := context.WithTimeout(context.Background(), cacheTimeout)
		defer cancel()
		if err := h.index.PutCachedProject(cacheCtx, cached); err != nil {
			logger.Error("failed to cache project metadata", "error", err)
		} else {
			logger.Debug("cached project metadata")
		}
	}()

	h.writeProjectResponse(w, r, cached, normalized)
}

// handleFile handles file download requests.
func (h *Handler) handleFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Path format: /packages/{project}/{filename}
	path := strings.TrimPrefix(r.URL.Path, "/packages/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}

	project := parts[0]
	filename := parts[1]
	logger := h.logger.With("project", project, "filename", filename, "endpoint", "file")

	// Check for expected hash in URL fragment (for verification)
	expectedHash := ""
	if r.URL.Fragment != "" {
		hashes := parseHashFragment(r.URL.Fragment)
		if h, ok := hashes["sha256"]; ok {
			expectedHash = h
		}
	}

	// Try cache first
	cachedFile, err := h.index.GetCachedFile(ctx, project, filename)
	if err == nil && !cachedFile.ContentHash.IsZero() {
		rc, err := h.store.Get(ctx, cachedFile.ContentHash)
		if err == nil {
			logger.Debug("cache hit")
			defer func() { _ = rc.Close() }()
			w.Header().Set("Content-Type", "application/octet-stream")
			if _, err := io.Copy(w, rc); err != nil {
				logger.Error("failed to stream file", "error", err)
			}
			return
		}
		logger.Warn("file hash in index but not in store", "hash", cachedFile.ContentHash.ShortString(), "error", err)
	}

	// Get upstream URL from cache or construct it
	var upstreamURL string
	var fileHashes map[string]string
	var requiresPython string

	if cachedFile != nil && cachedFile.UpstreamURL != "" {
		upstreamURL = cachedFile.UpstreamURL
		fileHashes = cachedFile.Hashes
		requiresPython = cachedFile.RequiresPython
	} else {
		// Fetch project metadata to get upstream URL
		cached, err := h.index.GetCachedProject(ctx, project)
		if err == nil {
			if f, ok := cached.Files[filename]; ok {
				upstreamURL = f.UpstreamURL
				fileHashes = f.Hashes
				requiresPython = f.RequiresPython
			}
		}
	}

	if upstreamURL == "" {
		logger.Error("no upstream URL for file")
		http.NotFound(w, r)
		return
	}

	// Use expected hash from file metadata if not in URL
	if expectedHash == "" {
		if h, ok := fileHashes["sha256"]; ok {
			expectedHash = h
		}
	}

	// Fetch from upstream
	logger.Debug("cache miss, fetching from upstream")
	rc, err := h.upstream.FetchFile(ctx, upstreamURL)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		logger.Error("upstream fetch failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer func() { _ = rc.Close() }()

	// Create temp file to avoid memory exhaustion for large files
	tmpFile, err := os.CreateTemp("", "pypi-file-*")
	if err != nil {
		logger.Error("failed to create temp file", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	tmpPath := tmpFile.Name()
	// Note: temp file cleanup is handled by the background caching goroutine,
	// NOT via defer here, to avoid a race condition where the file is deleted
	// before the goroutine can read it.

	// Stream to temp file while computing hashes
	hr := contentcache.NewHashingReader(rc)
	sha256Hash := sha256.New()
	teeReader := io.TeeReader(hr, sha256Hash)

	size, err := io.Copy(tmpFile, teeReader)
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		logger.Error("failed to read file", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	contentHash := hr.Sum()
	computedSha256 := hex.EncodeToString(sha256Hash.Sum(nil))

	// Verify integrity before serving to client
	if expectedHash != "" && computedSha256 != expectedHash {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		logger.Error("integrity check failed",
			"expected_sha256", expectedHash,
			"computed_sha256", computedSha256,
		)
		http.Error(w, "integrity check failed", http.StatusBadGateway)
		return
	}

	if expectedHash != "" {
		logger.Debug("integrity check passed", "sha256", computedSha256)
	}

	// Seek to beginning for sending to client
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		logger.Error("failed to seek temp file", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Write to client
	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := io.Copy(w, tmpFile); err != nil {
		logger.Error("failed to write response", "error", err)
	}
	_ = tmpFile.Close()

	// Update hashes with computed value
	if fileHashes == nil {
		fileHashes = make(map[string]string)
	}
	fileHashes["sha256"] = computedSha256

	// Cache asynchronously
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer func() { _ = os.Remove(tmpPath) }() // Clean up temp file after caching

		// Use context.Background() for cache operations that should complete even during shutdown.
		// The WaitGroup ensures we wait for this to finish before Close() returns.
		cacheCtx, cancel := context.WithTimeout(context.Background(), cacheTimeout)
		defer cancel()

		h.cacheFile(cacheCtx, project, filename, contentHash, size, fileHashes, upstreamURL, requiresPython, tmpPath, logger)
	}()
}

// cacheFile stores a file in the cache from a temp file.
func (h *Handler) cacheFile(ctx context.Context, project, filename string, hash contentcache.Hash, size int64, hashes map[string]string, upstreamURL, requiresPython, tmpPath string, logger *slog.Logger) {
	tmpFile, err := os.Open(tmpPath)
	if err != nil {
		logger.Error("failed to open temp file for caching", "error", err)
		return
	}
	defer func() { _ = tmpFile.Close() }()

	storedHash, err := h.store.Put(ctx, tmpFile)
	if err != nil {
		logger.Error("failed to store file", "error", err)
		return
	}

	if storedHash != hash {
		logger.Warn("hash mismatch during caching", "expected", hash.ShortString(), "got", storedHash.ShortString())
	}

	if err := h.index.SetFileHash(ctx, project, filename, hash, size, hashes, upstreamURL, requiresPython); err != nil {
		logger.Error("failed to update index", "error", err)
		return
	}

	logger.Info("cached file", "filename", filename, "hash", hash.ShortString(), "size", size)
}

// writeProjectResponse writes the project page response in HTML or JSON format.
func (h *Handler) writeProjectResponse(w http.ResponseWriter, r *http.Request, cached *CachedProject, normalized string) {
	// Build file list for response
	var files []ProjectFile
	for _, f := range cached.Files {
		// Rewrite URL to point to our proxy
		// Note: We include /pypi prefix since the server strips it before passing to this handler
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		proxyURL := fmt.Sprintf("%s://%s/pypi/packages/%s/%s", scheme, r.Host, normalized, f.Filename)

		pf := ProjectFile{
			Filename:       f.Filename,
			URL:            proxyURL,
			Hashes:         f.Hashes,
			RequiresPython: f.RequiresPython,
		}
		files = append(files, pf)
	}

	// Sort files for consistent output
	sort.Slice(files, func(i, j int) bool {
		return files[i].Filename < files[j].Filename
	})

	if wantsJSON(r) {
		h.writeProjectJSON(w, normalized, files)
	} else {
		h.writeProjectHTML(w, normalized, files)
	}
}

// wantsJSON checks if the client prefers JSON response.
func wantsJSON(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	return strings.Contains(accept, ContentTypeJSON)
}

// writeRootJSON writes the root index in JSON format.
func (h *Handler) writeRootJSON(w http.ResponseWriter, projects []string) {
	resp := ProjectList{
		Meta: APIMeta{APIVersion: CurrentAPIVersion},
	}
	for _, p := range projects {
		resp.Projects = append(resp.Projects, ProjectSummary{Name: p})
	}

	w.Header().Set("Content-Type", ContentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Error("failed to encode JSON response", "error", err)
	}
}

// writeRootHTML writes the root index in HTML format.
func (h *Handler) writeRootHTML(w http.ResponseWriter, projects []string) {
	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html>\n<html>\n<head><title>Simple Index</title></head>\n<body>\n")
	for _, p := range projects {
		buf.WriteString(fmt.Sprintf("<a href=\"/simple/%s/\">%s</a><br/>\n", p, p))
	}
	buf.WriteString("</body>\n</html>")

	w.Header().Set("Content-Type", ContentTypeHTML)
	if _, err := w.Write(buf.Bytes()); err != nil {
		h.logger.Error("failed to write HTML response", "error", err)
	}
}

// writeProjectJSON writes the project page in JSON format.
func (h *Handler) writeProjectJSON(w http.ResponseWriter, project string, files []ProjectFile) {
	resp := ProjectPage{
		Meta:  APIMeta{APIVersion: CurrentAPIVersion},
		Name:  project,
		Files: files,
	}

	w.Header().Set("Content-Type", ContentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Error("failed to encode JSON response", "error", err)
	}
}

// projectPageTemplate is the HTML template for project pages.
var projectPageTemplate = template.Must(template.New("project").Parse(`<!DOCTYPE html>
<html>
<head><title>Links for {{.Name}}</title></head>
<body>
<h1>Links for {{.Name}}</h1>
{{range .Files}}<a href="{{.URL}}{{if .HashFragment}}#{{.HashFragment}}{{end}}"{{if .RequiresPython}} data-requires-python="{{.RequiresPython}}"{{end}}>{{.Filename}}</a><br/>
{{end}}</body>
</html>`))

type projectPageData struct {
	Name  string
	Files []fileData
}

type fileData struct {
	URL            string
	Filename       string
	HashFragment   string
	RequiresPython string
}

// writeProjectHTML writes the project page in HTML format.
func (h *Handler) writeProjectHTML(w http.ResponseWriter, project string, files []ProjectFile) {
	data := projectPageData{
		Name: project,
	}

	for _, f := range files {
		fd := fileData{
			URL:            f.URL,
			Filename:       f.Filename,
			RequiresPython: template.HTMLEscapeString(f.RequiresPython),
		}
		// Add hash fragment if available
		if sha256, ok := f.Hashes["sha256"]; ok {
			fd.HashFragment = "sha256=" + sha256
		}
		data.Files = append(data.Files, fd)
	}

	w.Header().Set("Content-Type", ContentTypeHTML)
	if err := projectPageTemplate.Execute(w, data); err != nil {
		h.logger.Error("failed to execute template", "error", err)
	}
}
