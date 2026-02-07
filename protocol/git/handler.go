package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/download"
	"github.com/wolfeidau/content-cache/store"
	"github.com/wolfeidau/content-cache/telemetry"
)

// Handler implements the Git Smart HTTP proxy as an HTTP handler.
type Handler struct {
	index              *Index
	store              store.Store
	upstream           *Upstream
	logger             *slog.Logger
	downloader         *download.Downloader
	allowedHosts       map[string]bool
	maxRequestBodySize int64
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithLogger sets the logger for the handler.
func WithLogger(logger *slog.Logger) HandlerOption {
	return func(h *Handler) {
		h.logger = logger
	}
}

// WithUpstream sets the upstream Git client.
func WithUpstream(upstream *Upstream) HandlerOption {
	return func(h *Handler) {
		h.upstream = upstream
	}
}

// WithDownloader sets the singleflight downloader for deduplicating concurrent fetches.
func WithDownloader(dl *download.Downloader) HandlerOption {
	return func(h *Handler) {
		h.downloader = dl
	}
}

// WithAllowedHosts sets the allowlist of permitted upstream Git hosts.
func WithAllowedHosts(hosts []string) HandlerOption {
	return func(h *Handler) {
		h.allowedHosts = make(map[string]bool, len(hosts))
		for _, host := range hosts {
			h.allowedHosts[host] = true
		}
	}
}

// WithMaxRequestBodySize sets the maximum size for upload-pack request bodies.
func WithMaxRequestBodySize(size int64) HandlerOption {
	return func(h *Handler) {
		h.maxRequestBodySize = size
	}
}

// NewHandler creates a new Git proxy handler.
func NewHandler(index *Index, store store.Store, opts ...HandlerOption) *Handler {
	h := &Handler{
		index:              index,
		store:              store,
		upstream:           NewUpstream(),
		logger:             slog.Default(),
		allowedHosts:       make(map[string]bool),
		maxRequestBodySize: DefaultMaxRequestBodySize,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// gitPathResult holds the parsed components of a Git proxy path.
type gitPathResult struct {
	Repo   RepoRef
	Action string // "info/refs" or "git-upload-pack" or "git-receive-pack"
}

// parseGitPath extracts the repository reference and action from a request path.
// Expected format: /{host}/{repoPath}.git/{action}
// Example: /github.com/user/repo.git/info/refs
// Example: /gitlab.com/group/sub/repo.git/git-upload-pack
func parseGitPath(path string) (*gitPathResult, error) {
	// Remove leading slash
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil, fmt.Errorf("empty path")
	}

	// Reject path traversal
	for seg := range strings.SplitSeq(path, "/") {
		if seg == ".." || seg == "" {
			return nil, fmt.Errorf("invalid path segment")
		}
	}

	// Find .git/ separator
	repoSection, action, found := strings.Cut(path, ".git/")
	if !found {
		return nil, fmt.Errorf("missing .git/ in path")
	}

	// Trim trailing dot if present (e.g., "repo." from "repo.git/")
	repoSection = strings.TrimSuffix(repoSection, ".")

	// Split into host and repo path
	host, repoPath, found := strings.Cut(repoSection, "/")
	if !found {
		return nil, fmt.Errorf("missing repository path")
	}

	if host == "" || repoPath == "" {
		return nil, fmt.Errorf("empty host or repository path")
	}

	// Validate action
	switch action {
	case "info/refs", "git-upload-pack", "git-receive-pack":
		// valid
	default:
		return nil, fmt.Errorf("unknown action: %s", action)
	}

	return &gitPathResult{
		Repo:   RepoRef{Host: host, RepoPath: repoPath},
		Action: action,
	}, nil
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parsed, err := parseGitPath(r.URL.Path)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// Validate host against allowlist
	if !h.allowedHosts[parsed.Repo.Host] {
		http.Error(w, "host not allowed", http.StatusForbidden)
		return
	}

	switch parsed.Action {
	case "info/refs":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h.handleInfoRefs(w, r, parsed.Repo)

	case "git-upload-pack":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h.handleUploadPack(w, r, parsed.Repo)

	case "git-receive-pack":
		http.Error(w, "push not allowed", http.StatusForbidden)

	default:
		http.NotFound(w, r)
	}
}

// handleInfoRefs proxies the info/refs discovery request to upstream.
// This is always a passthrough — never cached, since refs change.
func (h *Handler) handleInfoRefs(w http.ResponseWriter, r *http.Request, repo RepoRef) {
	ctx := r.Context()
	logger := h.logger.With("repo", repo.String(), "endpoint", "info/refs")

	telemetry.SetEndpoint(r, "info/refs")

	// Reject git-receive-pack (push discovery)
	if r.URL.Query().Get("service") == "git-receive-pack" {
		http.Error(w, "push not allowed", http.StatusForbidden)
		return
	}

	gitProtocol := r.Header.Get("Git-Protocol")

	rc, contentType, err := h.upstream.FetchInfoRefs(ctx, repo, gitProtocol)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.Error(w, "repository not found", http.StatusNotFound)
			return
		}
		logger.Error("upstream info/refs failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", contentType)
	if _, err := io.Copy(w, rc); err != nil {
		logger.Error("failed to stream info/refs response", "error", err)
	}
}

// handleUploadPack handles git-upload-pack requests with pack-level caching.
func (h *Handler) handleUploadPack(w http.ResponseWriter, r *http.Request, repo RepoRef) {
	ctx := r.Context()
	logger := h.logger.With("repo", repo.String(), "endpoint", "upload-pack")

	telemetry.SetEndpoint(r, "upload-pack")

	// Read and limit the request body
	r.Body = http.MaxBytesReader(w, r.Body, h.maxRequestBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		logger.Error("failed to read request body", "error", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	gitProtocol := r.Header.Get("Git-Protocol")
	bodyHash := contentcache.HashBytes(body)
	cacheKey := fmt.Sprintf("%s:%s:%s", repo.String(), gitProtocol, bodyHash.String())

	// Check cache
	cached, err := h.index.GetCachedPack(ctx, cacheKey)
	if err == nil {
		logger.Debug("cache hit", "hash", cached.ResponseHash.ShortString())
		telemetry.SetCacheResult(r, telemetry.CacheHit)

		download.ServeFromStore(ctx, w, r, h.store, &download.Result{
			Hash: cached.ResponseHash,
			Size: cached.ResponseSize,
		}, download.ServeOptions{ContentType: ContentTypeUploadPackResult}, logger)
		return
	}
	if !errors.Is(err, ErrNotFound) {
		logger.Error("cache lookup failed", "error", err)
	}

	logger.Debug("cache miss, fetching from upstream")
	telemetry.SetCacheResult(r, telemetry.CacheMiss)

	sfKey := fmt.Sprintf("git:upload-pack:%s", cacheKey)

	result, _, err := h.downloader.Do(ctx, sfKey, func(dlCtx context.Context) (*download.Result, error) {
		return h.fetchAndStorePack(dlCtx, repo, gitProtocol, body, bodyHash, cacheKey, logger)
	})

	download.HandleResult(download.HandleResultParams{
		Writer:     w,
		Request:    r,
		Downloader: h.downloader,
		Key:        sfKey,
		Result:     result,
		Err:        err,
		Store:      h.store,
		IsNotFound: func(e error) bool { return errors.Is(e, ErrNotFound) },
		Opts:       download.ServeOptions{ContentType: ContentTypeUploadPackResult},
		Logger:     logger,
	})
}

// fetchAndStorePack fetches an upload-pack response from upstream, stores it in CAFS,
// and updates the index.
func (h *Handler) fetchAndStorePack(ctx context.Context, repo RepoRef, gitProtocol string, body []byte, bodyHash contentcache.Hash, cacheKey string, logger *slog.Logger) (*download.Result, error) {
	rc, _, err := h.upstream.FetchUploadPack(ctx, repo, gitProtocol, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	// Stream response to temp file while computing hash
	tmpFile, err := os.CreateTemp("", "git-pack-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	hr := contentcache.NewHashingReader(rc)

	size, err := io.Copy(tmpFile, hr)
	if err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("reading pack data: %w", err)
	}
	hash := hr.Sum()

	// Seek to beginning for CAFS storage
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("seeking temp file: %w", err)
	}

	// Store in CAFS
	storedHash, err := h.store.Put(ctx, tmpFile)
	if err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("storing pack: %w", err)
	}
	_ = tmpFile.Close()

	if storedHash != hash {
		logger.Warn("hash mismatch during storage", "expected", hash.ShortString(), "got", storedHash.ShortString())
	}

	// Update index
	cached := &CachedPack{
		RequestHash:  bodyHash,
		ResponseHash: hash,
		ResponseSize: size,
		Repo:         repo.String(),
		GitProtocol:  gitProtocol,
		CachedAt:     time.Now(),
	}
	if err := h.index.PutCachedPack(ctx, cacheKey, cached); err != nil {
		logger.Error("failed to update index", "error", err)
	} else {
		logger.Info("cached pack", "hash", hash.ShortString(), "size", size)
	}

	return &download.Result{Hash: hash, Size: size}, nil
}
