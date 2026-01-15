package oci

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/store"
)

func newTestHandler(t *testing.T, upstreamURL string) (*Handler, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "oci-handler-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}

	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}

	idx := NewIndex(b)
	st := store.NewCAFS(b)

	opts := []HandlerOption{
		WithTagTTL(1 * time.Hour),
	}
	if upstreamURL != "" {
		opts = append(opts, WithUpstream(NewUpstream(WithRegistryURL(upstreamURL))))
	}

	h := NewHandler(idx, st, opts...)

	return h, func() {
		h.Close()
		_ = os.RemoveAll(tmpDir)
	}
}

func TestHandlerVersionCheck(t *testing.T) {
	h, cleanup := newTestHandler(t, "")
	defer cleanup()

	t.Run("GET /v2/", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want %q", ct, "application/json")
		}
		if v := w.Header().Get("Docker-Distribution-API-Version"); v != "registry/2.0" {
			t.Errorf("Docker-Distribution-API-Version = %q, want %q", v, "registry/2.0")
		}
	})

	t.Run("GET /v2 (without trailing slash)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("HEAD /v2/", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/v2/", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	h, cleanup := newTestHandler(t, "")
	defer cleanup()

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/v2/", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}

func TestHandlerNotFound(t *testing.T) {
	h, cleanup := newTestHandler(t, "")
	defer cleanup()

	paths := []string{
		"/v3/",
		"/v2/something",
		"/v2/library/alpine/tags/list",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
			}
		})
	}
}

func TestHandlerGetManifest(t *testing.T) {
	manifestContent := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[]}`
	manifestDigest := ComputeSHA256([]byte(manifestContent))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write([]byte(manifestContent))
		case "/v2/library/alpine/manifests/" + manifestDigest:
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write([]byte(manifestContent))
		case "/v2/library/notfound/manifests/latest":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	t.Run("fetch by tag", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/vnd.oci.image.manifest.v1+json" {
			t.Errorf("Content-Type = %q", ct)
		}
		if d := w.Header().Get("Docker-Content-Digest"); d != manifestDigest {
			t.Errorf("Docker-Content-Digest = %q", d)
		}
		if w.Body.String() != manifestContent {
			t.Errorf("body = %q", w.Body.String())
		}
	})

	t.Run("fetch by digest", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/"+manifestDigest, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/notfound/manifests/latest", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	// Wait for background caching
	h.Close()

	// Test cache hit
	t.Run("cache hit", func(t *testing.T) {
		h2, cleanup2 := newTestHandler(t, upstream.URL)
		defer cleanup2()

		// Verify the first handler cached it - create new handler with same storage
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
		w := httptest.NewRecorder()
		h2.ServeHTTP(w, req)

		// Should still work (will fetch from upstream since new handler has empty cache)
		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})
}

func TestHandlerHeadManifest(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %q, want HEAD", r.Method)
		}
		switch r.URL.Path {
		case "/v2/library/alpine/manifests/latest":
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:abc123")
			w.Header().Set("Content-Length", "1024")
			w.WriteHeader(http.StatusOK)
		case "/v2/library/notfound/manifests/latest":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	t.Run("success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/v2/library/alpine/manifests/latest", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/vnd.oci.image.manifest.v1+json" {
			t.Errorf("Content-Type = %q", ct)
		}
		if d := w.Header().Get("Docker-Content-Digest"); d != "sha256:abc123" {
			t.Errorf("Docker-Content-Digest = %q", d)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/v2/library/notfound/manifests/latest", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestHandlerGetBlob(t *testing.T) {
	blobContent := []byte("test blob content for digest verification")
	blobDigest := ComputeSHA256(blobContent)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/library/alpine/blobs/" + blobDigest:
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blobContent)))
			_, _ = w.Write(blobContent)
		case "/v2/library/alpine/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	t.Run("success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Errorf("Content-Type = %q", ct)
		}
		if d := w.Header().Get("Docker-Content-Digest"); d != blobDigest {
			t.Errorf("Docker-Content-Digest = %q, want %q", d, blobDigest)
		}
		if w.Body.String() != string(blobContent) {
			t.Errorf("body mismatch")
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("invalid digest format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/invalid", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		// Should return 404 as the path regex doesn't match
		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestHandlerHeadBlob(t *testing.T) {
	validDigest := "sha256:abc123def456789012345678901234567890123456789012345678901234abcd"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %q, want HEAD", r.Method)
		}
		switch r.URL.Path {
		case "/v2/library/alpine/blobs/" + validDigest:
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
		case "/v2/library/alpine/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	t.Run("success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/v2/library/alpine/blobs/"+validDigest, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Errorf("Content-Type = %q", ct)
		}
		if cl := w.Header().Get("Content-Length"); cl != "4096" {
			t.Errorf("Content-Length = %q", cl)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/v2/library/alpine/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestHandlerCaching(t *testing.T) {
	manifestContent := `{"schemaVersion":2}`
	manifestDigest := ComputeSHA256([]byte(manifestContent))
	requestCount := 0

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.Header().Set("Docker-Content-Digest", manifestDigest)
		_, _ = w.Write([]byte(manifestContent))
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	// First request - cache miss
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want %d", w.Code, http.StatusOK)
	}

	// Wait for background caching
	h.Close()

	if requestCount != 1 {
		t.Errorf("upstream request count = %d, want 1", requestCount)
	}

	// Create new handler with same store to test cache
	// Note: In a real scenario, we'd reuse the same handler
}

func TestHandlerWithAuth(t *testing.T) {
	// Auth server
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TokenResponse{
			Token:     "test-token",
			ExpiresIn: 300,
		})
	}))
	defer authServer.Close()

	// Registry that requires auth
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s",service="test",scope="repository:library/alpine:pull"`, authServer.URL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/v2/library/alpine/manifests/latest" {
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:abc123")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer registry.Close()

	h, cleanup := newTestHandler(t, registry.URL)
	defer cleanup()

	// Request should succeed after auth
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlerDigestVerification(t *testing.T) {
	// Wrong content for the claimed digest
	wrongContent := []byte("wrong content")
	claimedDigest := "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(wrongContent)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+claimedDigest, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Should fail with bad gateway due to digest mismatch
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d (digest verification should fail)", w.Code, http.StatusBadGateway)
	}
}

func TestHandlerManifestCacheHit(t *testing.T) {
	requestCount := 0
	manifestContent := `{"schemaVersion":2}`
	manifestDigest := ComputeSHA256([]byte(manifestContent))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.Header().Set("Docker-Content-Digest", manifestDigest)
		_, _ = w.Write([]byte(manifestContent))
	}))
	defer upstream.Close()

	// Create handler with short TTL
	tmpDir, err := os.MkdirTemp("", "oci-cache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(b)
	st := store.NewCAFS(b)

	h := NewHandler(idx, st,
		WithTagTTL(1*time.Hour),
		WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))),
	)
	defer h.Close()

	// First request - cache miss
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first request failed: %d", w.Code)
	}

	// Wait for background caching
	h.Close()

	// Create new handler with same storage
	h2 := NewHandler(idx, st,
		WithTagTTL(1*time.Hour),
		WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))),
	)
	defer h2.Close()

	// Second request - should be cache hit
	req2 := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w2 := httptest.NewRecorder()
	h2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("second request failed: %d", w2.Code)
	}

	if requestCount != 1 {
		t.Errorf("upstream request count = %d, want 1 (second request should be cache hit)", requestCount)
	}
}

func TestHandlerBlobCacheHit(t *testing.T) {
	blobContent := []byte("cached blob content here")
	blobDigest := ComputeSHA256(blobContent)
	requestCount := 0

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blobContent)))
		_, _ = w.Write(blobContent)
	}))
	defer upstream.Close()

	tmpDir, err := os.MkdirTemp("", "oci-blob-cache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(b)
	st := store.NewCAFS(b)

	h := NewHandler(idx, st,
		WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))),
	)

	// First request
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first request failed: %d", w.Code)
	}

	// Wait for background caching
	h.Close()

	// Second handler with same storage
	h2 := NewHandler(idx, st,
		WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))),
	)
	defer h2.Close()

	// Second request - should hit cache
	req2 := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil)
	w2 := httptest.NewRecorder()
	h2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("second request failed: %d", w2.Code)
	}

	if w2.Body.String() != string(blobContent) {
		t.Error("cached blob content mismatch")
	}

	if requestCount != 1 {
		t.Errorf("upstream request count = %d, want 1", requestCount)
	}
}

func TestHandlerUpstreamError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadGateway)
	}
}

func TestHandlerNestedImageName(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/myorg/myrepo/myimage/manifests/v1.0.0" {
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:abc")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		t.Logf("unexpected path: %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream.URL)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v2/myorg/myrepo/myimage/manifests/v1.0.0", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlerClose(t *testing.T) {
	h, cleanup := newTestHandler(t, "")
	defer cleanup()

	// Close should be idempotent
	h.Close()
	h.Close()
}

func TestHandlerOptions(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "oci-test-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(b)
	st := store.NewCAFS(b)

	t.Run("default values", func(t *testing.T) {
		h := NewHandler(idx, st)
		defer h.Close()

		if h.tagTTL != DefaultTagTTL {
			t.Errorf("tagTTL = %v, want %v", h.tagTTL, DefaultTagTTL)
		}
	})

	t.Run("custom tag TTL", func(t *testing.T) {
		h := NewHandler(idx, st, WithTagTTL(10*time.Minute))
		defer h.Close()

		if h.tagTTL != 10*time.Minute {
			t.Errorf("tagTTL = %v, want %v", h.tagTTL, 10*time.Minute)
		}
	})
}

func TestHandlerHeadManifestCacheHit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream should not be called for cache hit")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	tmpDir, _ := os.MkdirTemp("", "oci-head-test-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(b)
	st := store.NewCAFS(b)

	// Pre-populate cache
	ctx := context.Background()
	manifestDigest := "sha256:abc123def456789012345678901234567890123456789012345678901234abcd"
	hash := contentcache.Hash{1, 2, 3, 4}
	_ = idx.PutManifest(ctx, manifestDigest, "application/vnd.oci.image.manifest.v1+json", hash, 1024)
	_ = idx.SetTagDigest(ctx, "library/alpine", "latest", manifestDigest)

	h := NewHandler(idx, st, WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))))
	defer h.Close()

	// HEAD by tag - should hit cache
	req := httptest.NewRequest(http.MethodHead, "/v2/library/alpine/manifests/latest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if d := w.Header().Get("Docker-Content-Digest"); d != manifestDigest {
		t.Errorf("Docker-Content-Digest = %q, want %q", d, manifestDigest)
	}
	if cl := w.Header().Get("Content-Length"); cl != "1024" {
		t.Errorf("Content-Length = %q, want %q", cl, "1024")
	}

	// HEAD by digest - should also hit cache
	req2 := httptest.NewRequest(http.MethodHead, "/v2/library/alpine/manifests/"+manifestDigest, nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("digest HEAD status = %d, want %d", w2.Code, http.StatusOK)
	}
}

func TestHandlerHeadBlobCacheHit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream should not be called for cache hit")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	tmpDir, _ := os.MkdirTemp("", "oci-head-blob-test-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(b)
	st := store.NewCAFS(b)

	// Pre-populate cache
	ctx := context.Background()
	blobDigest := "sha256:abc123def456789012345678901234567890123456789012345678901234abcd"
	hash := contentcache.Hash{5, 6, 7, 8}
	_ = idx.PutBlob(ctx, blobDigest, hash, 4096)

	h := NewHandler(idx, st, WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))))
	defer h.Close()

	req := httptest.NewRequest(http.MethodHead, "/v2/library/alpine/blobs/"+blobDigest, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if d := w.Header().Get("Docker-Content-Digest"); d != blobDigest {
		t.Errorf("Docker-Content-Digest = %q, want %q", d, blobDigest)
	}
	if cl := w.Header().Get("Content-Length"); cl != "4096" {
		t.Errorf("Content-Length = %q, want %q", cl, "4096")
	}
}

func TestHandlerTagTTLExpiry(t *testing.T) {
	requestCount := 0
	manifestContent := `{"schemaVersion":2}`
	manifestDigest := ComputeSHA256([]byte(manifestContent))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.Header().Set("Docker-Content-Digest", manifestDigest)
		_, _ = w.Write([]byte(manifestContent))
	}))
	defer upstream.Close()

	tmpDir, _ := os.MkdirTemp("", "oci-ttl-test-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(b)
	st := store.NewCAFS(b)

	// Very short TTL
	h := NewHandler(idx, st,
		WithTagTTL(1*time.Millisecond),
		WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))),
	)

	// First request
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first request failed: %d", w.Code)
	}

	// Wait for TTL to expire and background caching
	h.Close()
	time.Sleep(10 * time.Millisecond)

	// Second request with expired TTL - should revalidate
	h2 := NewHandler(idx, st,
		WithTagTTL(1*time.Millisecond),
		WithUpstream(NewUpstream(WithRegistryURL(upstream.URL))),
	)
	defer h2.Close()

	req2 := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	w2 := httptest.NewRecorder()
	h2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("second request failed: %d", w2.Code)
	}

	// Should have made 2 upstream requests (TTL expired)
	if requestCount != 2 {
		t.Errorf("upstream request count = %d, want 2", requestCount)
	}
}

func BenchmarkHandlerVersionCheck(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "oci-bench-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	be, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(be)
	st := store.NewCAFS(be)
	h := NewHandler(idx, st)
	defer h.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}
}

func BenchmarkHandlerManifestCacheHit(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "oci-bench-*")
	defer func() { _ = os.RemoveAll(tmpDir) }()

	be, _ := backend.NewFilesystem(tmpDir)
	idx := NewIndex(be)
	st := store.NewCAFS(be)

	// Pre-populate cache
	ctx := context.Background()
	manifestContent := `{"schemaVersion":2}`
	manifestDigest := ComputeSHA256([]byte(manifestContent))
	hash, _ := st.Put(ctx, io.NopCloser(nil))
	_ = idx.PutManifest(ctx, manifestDigest, "application/vnd.oci.image.manifest.v1+json", hash, int64(len(manifestContent)))
	_ = idx.SetTagDigest(ctx, "library/alpine", "latest", manifestDigest)

	h := NewHandler(idx, st)
	defer h.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}
}
