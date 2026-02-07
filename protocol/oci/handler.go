package oci

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/download"
	"github.com/wolfeidau/content-cache/store"
	"github.com/wolfeidau/content-cache/telemetry"
)

const (
	// cacheTimeout is the maximum time allowed for background caching operations.
	cacheTimeout = 5 * time.Minute

	// DefaultTagTTL is the default TTL for tag->digest mappings.
	DefaultTagTTL = 5 * time.Minute
)

// Path patterns for OCI Distribution API.
var (
	// Matches /v2/{name}/manifests/{reference}
	manifestPathRegex = regexp.MustCompile(`^/v2/(.+)/manifests/(.+)$`)

	// Matches /v2/{name}/blobs/{digest}
	blobPathRegex = regexp.MustCompile(`^/v2/(.+)/blobs/(sha256:[a-f0-9]{64}|sha512:[a-f0-9]{128})$`)
)

// Handler implements the OCI Distribution v2 protocol as an HTTP handler.
type Handler struct {
	index      *Index
	store      store.Store
	upstream   *Upstream
	logger     *slog.Logger
	downloader *download.Downloader

	// Tag TTL - how long before re-validating tag->digest mapping
	tagTTL time.Duration

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

// WithUpstream sets the upstream registry.
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

// WithTagTTL sets the TTL for tag->digest cache entries.
func WithTagTTL(ttl time.Duration) HandlerOption {
	return func(h *Handler) {
		h.tagTTL = ttl
	}
}

// NewHandler creates a new OCI registry handler.
func NewHandler(index *Index, store store.Store, opts ...HandlerOption) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		index:    index,
		store:    store,
		upstream: NewUpstream(),
		logger:   slog.Default(),
		tagTTL:   DefaultTagTTL,
		ctx:      ctx,
		cancel:   cancel,
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

	// Handle /v2/ version check
	if path == "/v2/" || path == "/v2" {
		h.handleVersionCheck(w, r)
		return
	}

	// Handle manifest requests
	if matches := manifestPathRegex.FindStringSubmatch(path); matches != nil {
		name := matches[1]
		reference := matches[2]
		if r.Method == http.MethodHead {
			h.handleHeadManifest(w, r, name, reference)
		} else {
			h.handleGetManifest(w, r, name, reference)
		}
		return
	}

	// Handle blob requests
	if matches := blobPathRegex.FindStringSubmatch(path); matches != nil {
		name := matches[1]
		digest := matches[2]
		if r.Method == http.MethodHead {
			h.handleHeadBlob(w, r, name, digest)
		} else {
			h.handleGetBlob(w, r, name, digest)
		}
		return
	}

	http.Error(w, "not found", http.StatusNotFound)
}

// handleVersionCheck handles GET /v2/ requests.
func (h *Handler) handleVersionCheck(w http.ResponseWriter, r *http.Request) {
	telemetry.SetEndpoint(r, "version")
	telemetry.SetCacheResult(r, telemetry.CacheBypass)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{}"))
}

// handleGetManifest handles GET /v2/{name}/manifests/{reference} requests.
func (h *Handler) handleGetManifest(w http.ResponseWriter, r *http.Request, name, reference string) {
	telemetry.SetEndpoint(r, "manifest")
	ctx := r.Context()
	logger := h.logger.With("name", name, "reference", reference, "endpoint", "manifest")

	// Determine if reference is a digest or tag
	isDigest := IsDigestReference(reference)

	var digest string
	var cachedManifest *CachedManifest

	if isDigest {
		// Direct digest lookup - immutable, no TTL needed
		digest = reference
		cached, err := h.index.GetManifest(ctx, digest)
		if err == nil {
			cachedManifest = cached
		}
	} else {
		// Tag lookup - check cache with TTL
		cachedDigest, refreshedAt, err := h.index.GetTagDigest(ctx, name, reference)
		if err == nil {
			// Check if tag mapping is still fresh
			if time.Since(refreshedAt) < h.tagTTL {
				digest = cachedDigest
				cached, err := h.index.GetManifest(ctx, digest)
				if err == nil {
					cachedManifest = cached
					logger.Debug("cache hit", "digest", digest)
				}
			} else {
				// Tag is stale, need to revalidate
				logger.Debug("tag stale, revalidating", "age", time.Since(refreshedAt))
			}
		}
	}

	// If we have a cached manifest, serve it
	if cachedManifest != nil {
		rc, err := h.store.Get(ctx, cachedManifest.ContentHash)
		if err == nil {
			telemetry.SetCacheResult(r, telemetry.CacheHit)
			defer func() { _ = rc.Close() }()
			w.Header().Set("Content-Type", cachedManifest.MediaType)
			w.Header().Set(DockerContentDigestHeader, cachedManifest.Digest)
			if _, err := io.Copy(w, rc); err != nil {
				logger.Error("failed to stream manifest", "error", err)
			}
			return
		}
		logger.Warn("manifest hash in index but not in store", "hash", cachedManifest.ContentHash.ShortString(), "error", err)
	}

	// Fetch from upstream
	telemetry.SetCacheResult(r, telemetry.CacheMiss)
	logger.Debug("cache miss, fetching from upstream")
	content, mediaType, upstreamDigest, err := h.upstream.FetchManifest(ctx, name, reference)
	if err != nil {
		if err == ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		logger.Error("upstream fetch failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	// Verify digest if we're fetching by digest
	if isDigest {
		d, err := ParseDigest(reference)
		if err == nil {
			if err := d.Verify(content); err != nil {
				logger.Error("digest verification failed", "error", err)
				http.Error(w, "digest mismatch", http.StatusBadGateway)
				return
			}
		}
	}

	// Write response
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set(DockerContentDigestHeader, upstreamDigest)
	if _, err := w.Write(content); err != nil {
		logger.Error("failed to write response", "error", err)
		return
	}

	// Cache asynchronously
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		cacheCtx, cancel := context.WithTimeout(h.ctx, cacheTimeout)
		defer cancel()
		h.cacheManifest(cacheCtx, name, reference, upstreamDigest, mediaType, content, logger)
	}()
}

// handleHeadManifest handles HEAD /v2/{name}/manifests/{reference} requests.
func (h *Handler) handleHeadManifest(w http.ResponseWriter, r *http.Request, name, reference string) {
	telemetry.SetEndpoint(r, "manifest-head")
	ctx := r.Context()
	logger := h.logger.With("name", name, "reference", reference, "endpoint", "manifest-head")

	isDigest := IsDigestReference(reference)

	if isDigest {
		// Check cache for digest
		cached, err := h.index.GetManifest(ctx, reference)
		if err == nil {
			telemetry.SetCacheResult(r, telemetry.CacheHit)
			w.Header().Set("Content-Type", cached.MediaType)
			w.Header().Set(DockerContentDigestHeader, cached.Digest)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", cached.Size))
			w.WriteHeader(http.StatusOK)
			return
		}
	} else {
		// Check tag cache
		digest, refreshedAt, err := h.index.GetTagDigest(ctx, name, reference)
		if err == nil && time.Since(refreshedAt) < h.tagTTL {
			cached, err := h.index.GetManifest(ctx, digest)
			if err == nil {
				telemetry.SetCacheResult(r, telemetry.CacheHit)
				w.Header().Set("Content-Type", cached.MediaType)
				w.Header().Set(DockerContentDigestHeader, cached.Digest)
				w.Header().Set("Content-Length", fmt.Sprintf("%d", cached.Size))
				w.WriteHeader(http.StatusOK)
				return
			}
		}
	}

	// Fetch from upstream
	telemetry.SetCacheResult(r, telemetry.CacheMiss)
	digest, size, mediaType, err := h.upstream.HeadManifest(ctx, name, reference)
	if err != nil {
		if err == ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		logger.Error("upstream head failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	if mediaType != "" {
		w.Header().Set("Content-Type", mediaType)
	}
	w.Header().Set(DockerContentDigestHeader, digest)
	if size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	}
	w.WriteHeader(http.StatusOK)
}

// handleGetBlob handles GET /v2/{name}/blobs/{digest} requests.
func (h *Handler) handleGetBlob(w http.ResponseWriter, r *http.Request, name, digestStr string) {
	telemetry.SetEndpoint(r, "blob")
	ctx := r.Context()
	logger := h.logger.With("name", name, "digest", digestStr, "endpoint", "blob")

	// Check cache first
	cached, err := h.index.GetBlob(ctx, digestStr)
	if err == nil {
		rc, err := h.store.Get(ctx, cached.ContentHash)
		if err == nil {
			defer func() { _ = rc.Close() }()
			logger.Debug("cache hit")
			telemetry.SetCacheResult(r, telemetry.CacheHit)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set(DockerContentDigestHeader, digestStr)
			if cached.Size > 0 {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", cached.Size))
			}
			if _, err := io.Copy(w, rc); err != nil {
				logger.Error("failed to stream blob", "error", err)
			}
			return
		}
		logger.Warn("blob hash in index but not in store", "hash", cached.ContentHash.ShortString(), "error", err)
	}

	// Fetch from upstream
	telemetry.SetCacheResult(r, telemetry.CacheMiss)
	logger.Debug("cache miss, fetching from upstream")

	if h.downloader != nil {
		h.handleGetBlobWithDownloader(w, r, name, digestStr, logger)
		return
	}

	h.handleGetBlobDirect(w, r, name, digestStr, logger)
}

// handleGetBlobWithDownloader uses the singleflight downloader to deduplicate concurrent blob fetches.
func (h *Handler) handleGetBlobWithDownloader(w http.ResponseWriter, r *http.Request, name, digestStr string, logger *slog.Logger) {
	ctx := r.Context()
	key := fmt.Sprintf("oci:blob:%s", digestStr)

	result, _, err := h.downloader.Do(ctx, key, func(dlCtx context.Context) (*download.Result, error) {
		return h.fetchAndStoreBlob(dlCtx, name, digestStr, logger)
	})
	if err != nil {
		h.downloader.Forget(key)
		if err == ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusGatewayTimeout)
			return
		}
		logger.Error("download failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	// Serve from CAFS
	rc, err := h.store.Get(ctx, result.Hash)
	if err != nil {
		logger.Error("failed to read from store after download", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(DockerContentDigestHeader, digestStr)
	if result.Size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", result.Size))
	}
	if _, err := io.Copy(w, rc); err != nil {
		logger.Error("failed to stream blob", "error", err)
	}
}

// fetchAndStoreBlob fetches a blob from upstream, verifies its digest, stores in CAFS, and updates the index.
func (h *Handler) fetchAndStoreBlob(ctx context.Context, name, digestStr string, logger *slog.Logger) (*download.Result, error) {
	rc, _, err := h.upstream.FetchBlob(ctx, name, digestStr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	expectedDigest, err := ParseDigest(digestStr)
	if err != nil {
		return nil, fmt.Errorf("invalid digest: %w", err)
	}

	// Create temp file
	tmpFile, err := os.CreateTemp("", "oci-blob-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	// Stream to temp file while computing hashes
	blake3Reader := contentcache.NewHashingReader(rc)
	digestHasher, err := expectedDigest.NewHasher()
	if err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("unsupported digest algorithm: %w", err)
	}
	teeReader := io.TeeReader(blake3Reader, digestHasher)

	written, err := io.Copy(tmpFile, teeReader)
	if err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("reading blob: %w", err)
	}
	blake3Hash := blake3Reader.Sum()

	// Verify digest
	computedHex := fmt.Sprintf("%x", digestHasher.Sum(nil))
	if computedHex != expectedDigest.Hex {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("digest verification failed: expected %s, got %s", expectedDigest.Hex, computedHex)
	}

	logger.Debug("digest verified", "digest", digestStr)

	// Seek to beginning for storing in CAFS
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("seeking temp file: %w", err)
	}

	storedHash, err := h.store.Put(ctx, tmpFile)
	if err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("storing blob: %w", err)
	}
	_ = tmpFile.Close()

	if storedHash != blake3Hash {
		logger.Warn("hash mismatch during storage", "expected", blake3Hash.ShortString(), "got", storedHash.ShortString())
	}

	// Update blob index
	if err := h.index.PutBlob(ctx, digestStr, blake3Hash, written); err != nil {
		logger.Error("failed to update blob index", "error", err)
	} else {
		logger.Info("cached blob", "digest", digestStr, "hash", blake3Hash.ShortString(), "size", written)
	}

	return &download.Result{Hash: blake3Hash, Size: written}, nil
}

// handleGetBlobDirect handles blob requests without singleflight deduplication (legacy path).
func (h *Handler) handleGetBlobDirect(w http.ResponseWriter, r *http.Request, name, digestStr string, logger *slog.Logger) {
	ctx := r.Context()

	rc, size, err := h.upstream.FetchBlob(ctx, name, digestStr)
	if err != nil {
		if err == ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		logger.Error("upstream fetch failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer func() { _ = rc.Close() }()

	expectedDigest, err := ParseDigest(digestStr)
	if err != nil {
		logger.Error("invalid digest", "error", err)
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	tmpFile, err := os.CreateTemp("", "oci-blob-*")
	if err != nil {
		logger.Error("failed to create temp file", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	tmpPath := tmpFile.Name()

	blake3Reader := contentcache.NewHashingReader(rc)
	digestHasher, err := expectedDigest.NewHasher()
	if err != nil {
		_ = tmpFile.Close()
		logger.Error("unsupported digest algorithm", "error", err)
		http.Error(w, "unsupported digest", http.StatusBadRequest)
		return
	}
	teeReader := io.TeeReader(blake3Reader, digestHasher)

	written, err := io.Copy(tmpFile, teeReader)
	if err != nil {
		_ = tmpFile.Close()
		logger.Error("failed to read blob", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	blake3Hash := blake3Reader.Sum()

	computedHex := fmt.Sprintf("%x", digestHasher.Sum(nil))
	if computedHex != expectedDigest.Hex {
		_ = tmpFile.Close()
		logger.Error("digest verification failed",
			"expected", expectedDigest.Hex,
			"computed", computedHex,
		)
		http.Error(w, "digest mismatch", http.StatusBadGateway)
		return
	}

	logger.Debug("digest verified", "digest", digestStr)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		logger.Error("failed to seek temp file", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(DockerContentDigestHeader, digestStr)
	if size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	}

	if _, err := io.Copy(w, tmpFile); err != nil {
		logger.Error("failed to write response", "error", err)
	}
	_ = tmpFile.Close()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		cacheCtx, cancel := context.WithTimeout(h.ctx, cacheTimeout)
		defer cancel()
		h.cacheBlob(cacheCtx, digestStr, blake3Hash, written, tmpPath, logger)
	}()
}

// handleHeadBlob handles HEAD /v2/{name}/blobs/{digest} requests.
func (h *Handler) handleHeadBlob(w http.ResponseWriter, r *http.Request, name, digestStr string) {
	telemetry.SetEndpoint(r, "blob-head")
	ctx := r.Context()
	logger := h.logger.With("name", name, "digest", digestStr, "endpoint", "blob-head")

	// Check cache first
	cached, err := h.index.GetBlob(ctx, digestStr)
	if err == nil {
		telemetry.SetCacheResult(r, telemetry.CacheHit)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(DockerContentDigestHeader, digestStr)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cached.Size))
		w.WriteHeader(http.StatusOK)
		return
	}

	// Check upstream
	telemetry.SetCacheResult(r, telemetry.CacheMiss)
	size, err := h.upstream.HeadBlob(ctx, name, digestStr)
	if err != nil {
		if err == ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		logger.Error("upstream head failed", "error", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(DockerContentDigestHeader, digestStr)
	if size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	}
	w.WriteHeader(http.StatusOK)
}

// cacheManifest stores a manifest in the cache.
func (h *Handler) cacheManifest(ctx context.Context, name, reference, digest, mediaType string, content []byte, logger *slog.Logger) {
	// Store content in CAFS
	hash, err := h.store.Put(ctx, strings.NewReader(string(content)))
	if err != nil {
		logger.Error("failed to store manifest", "error", err)
		return
	}

	// Update manifest index
	if err := h.index.PutManifest(ctx, digest, mediaType, hash, int64(len(content))); err != nil {
		logger.Error("failed to update manifest index", "error", err)
		return
	}

	// Update tag mapping if this was a tag reference
	if !IsDigestReference(reference) {
		if err := h.index.SetTagDigest(ctx, name, reference, digest); err != nil {
			logger.Error("failed to update tag index", "error", err)
			return
		}
	}

	logger.Info("cached manifest", "digest", digest, "hash", hash.ShortString(), "size", len(content))
}

// cacheBlob stores a blob in the cache from a temp file.
func (h *Handler) cacheBlob(ctx context.Context, digest string, hash contentcache.Hash, size int64, tmpPath string, logger *slog.Logger) {
	// Clean up temp file when done
	defer func() { _ = os.Remove(tmpPath) }()

	// Open temp file for reading
	tmpFile, err := os.Open(tmpPath)
	if err != nil {
		logger.Error("failed to open temp file for caching", "error", err)
		return
	}
	defer func() { _ = tmpFile.Close() }()

	// Store in CAFS
	storedHash, err := h.store.Put(ctx, tmpFile)
	if err != nil {
		logger.Error("failed to store blob", "error", err)
		return
	}

	// Verify hash matches what we computed during streaming
	if storedHash != hash {
		logger.Warn("hash mismatch during caching", "expected", hash.ShortString(), "got", storedHash.ShortString())
	}

	// Update blob index
	if err := h.index.PutBlob(ctx, digest, hash, size); err != nil {
		logger.Error("failed to update blob index", "error", err)
		return
	}

	logger.Info("cached blob", "digest", digest, "hash", hash.ShortString(), "size", size)
}
