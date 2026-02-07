package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/store"
)

// ServeOptions configures how ServeFromStore writes the HTTP response.
type ServeOptions struct {
	ContentType  string
	ExtraHeaders map[string]string // e.g., Docker-Content-Digest
}

// HandleDownloadError writes an appropriate HTTP error response for download
// errors that are not protocol-specific (e.g., not ErrNotFound). It handles
// context cancellation/timeout and generic upstream failures.
//
// Returns true if an error was handled, false if err is nil.
func HandleDownloadError(w http.ResponseWriter, logger *slog.Logger, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		http.Error(w, "request timeout", http.StatusGatewayTimeout)
		return
	}
	logger.Error("download failed", "error", err)
	http.Error(w, "upstream error", http.StatusBadGateway)
}

// ServeFromStore retrieves content from the CAFS store and writes it to the
// HTTP response. It sets Content-Type, Content-Length, and any extra headers
// from opts. For HEAD requests, it writes headers but skips the body.
func ServeFromStore(ctx context.Context, w http.ResponseWriter, r *http.Request, s store.Store, result *Result, opts ServeOptions, logger *slog.Logger) {
	rc, err := s.Get(ctx, result.Hash)
	if err != nil {
		logger.Error("failed to read from store after download", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", opts.ContentType)
	if result.Size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", result.Size))
	}
	for k, v := range opts.ExtraHeaders {
		w.Header().Set(k, v)
	}

	if r.Method != http.MethodHead {
		if _, err := io.Copy(w, rc); err != nil {
			logger.Error("failed to stream response", "error", err)
		}
	}
}

// ForgetOnDownloadError calls Forget on the downloader if the error represents
// a real download failure (not a caller context timeout). Returns the error
// unchanged for further handling.
func ForgetOnDownloadError(d *Downloader, key string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	d.Forget(key)
}

// IsNotFoundFunc is a function that checks if an error is a protocol-specific
// not-found error. Each protocol provides its own implementation.
type IsNotFoundFunc func(error) bool

// HandleResult processes the result of a Downloader.Do call, handling errors
// and serving from the store on success. This consolidates the common pattern
// used across all protocol handlers.
//
// The isNotFound function checks for protocol-specific not-found errors.
// The notFoundHandler is called when the error matches not-found.
func HandleResult(
	w http.ResponseWriter, r *http.Request,
	d *Downloader, key string,
	result *Result, err error,
	s store.Store,
	isNotFound IsNotFoundFunc,
	notFoundHandler func(),
	opts ServeOptions,
	logger *slog.Logger,
) {
	if err != nil {
		if isNotFound(err) {
			d.Forget(key)
			notFoundHandler()
			return
		}
		ForgetOnDownloadError(d, key, err)
		HandleDownloadError(w, logger, err)
		return
	}

	ServeFromStore(r.Context(), w, r, s, result, opts, logger)
}

// Hash re-exports contentcache.Hash for convenience in protocol packages.
type Hash = contentcache.Hash
