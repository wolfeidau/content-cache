package npm

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/store"
)

func newTestHandler(t *testing.T, upstreamServer *httptest.Server) (*Handler, func()) {
	t.Helper()
	// Use a manual temp dir instead of t.TempDir() to avoid race with async goroutines
	tmpDir, err := os.MkdirTemp("", "npm-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	cafs := store.NewCAFS(b)
	idx := NewIndex(b)

	opts := []UpstreamOption{}
	if upstreamServer != nil {
		opts = append(opts, WithRegistryURL(upstreamServer.URL))
	}
	upstream := NewUpstream(opts...)

	h := NewHandler(idx, cafs, WithUpstream(upstream))
	return h, func() {
		// Wait for background goroutines to complete before cleanup
		h.Close()
		_ = os.RemoveAll(tmpDir)
	}
}

func TestHandlerMetadata(t *testing.T) {
	// Create mock upstream
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lodash" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"name": "lodash",
				"dist-tags": {"latest": "4.17.21"},
				"versions": {
					"4.17.21": {
						"name": "lodash",
						"version": "4.17.21",
						"dist": {
							"tarball": "https://registry.npmjs.org/lodash/-/lodash-4.17.21.tgz",
							"shasum": "abc123"
						}
					}
				}
			}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	t.Run("get metadata", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/lodash", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), `"name":"lodash"`) {
			t.Error("Response should contain package name")
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nonexistent-pkg", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("abbreviated metadata", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/lodash", nil)
		req.Header.Set("Accept", "application/vnd.npm.install-v1+json")
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
		// Abbreviated response should have limited fields
		body := w.Body.String()
		if !strings.Contains(body, `"name"`) {
			t.Error("Abbreviated response should contain name")
		}
	})
}

func TestHandlerTarball(t *testing.T) {
	tarballContent := []byte("fake-tarball-content-12345")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test-pkg" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"name": "test-pkg",
				"versions": {
					"1.0.0": {
						"name": "test-pkg",
						"version": "1.0.0",
						"dist": {
							"tarball": "` + r.Host + `/test-pkg/-/test-pkg-1.0.0.tgz",
							"shasum": "8a698aa23442e52ee3e7009f7f8578d8ee3c6bc1"
						}
					}
				}
			}`))
			return
		}
		if r.URL.Path == "/test-pkg/-/test-pkg-1.0.0.tgz" {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(tarballContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	t.Run("download tarball", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test-pkg/-/test-pkg-1.0.0.tgz", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
		if w.Body.String() != string(tarballContent) {
			t.Errorf("Body = %q, want %q", w.Body.String(), tarballContent)
		}
	})

	t.Run("tarball not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nonexistent/-/nonexistent-1.0.0.tgz", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestHandlerScopedPackage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// URL should have encoded scope
		if r.URL.Path == "/@babel%2fcore" || r.URL.Path == "/@babel/core" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"name": "@babel/core",
				"versions": {
					"7.23.0": {
						"name": "@babel/core",
						"version": "7.23.0",
						"dist": {
							"tarball": "https://registry.npmjs.org/@babel/core/-/core-7.23.0.tgz",
							"shasum": "1b37b9c77f147faf4f645dfa32d7b233233b0755"
						}
					}
				}
			}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/-/core-7.23.0.tgz") {
			_, _ = w.Write([]byte("scoped-tarball-content"))
			return
		}
		t.Logf("404 for path: %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	t.Run("scoped package metadata", func(t *testing.T) {
		// URL-encoded scoped package
		req := httptest.NewRequest(http.MethodGet, "/@babel%2fcore", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
	})

	t.Run("scoped tarball", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/@babel%2fcore/-/core-7.23.0.tgz", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
	})
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	h, cleanup := newTestHandler(t, nil)
	defer cleanup()

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/test", nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("Status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}

func TestHandlerTarballURLRewrite(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"name": "test-pkg",
			"versions": {
				"1.0.0": {
					"name": "test-pkg",
					"version": "1.0.0",
					"dist": {
						"tarball": "https://registry.npmjs.org/test-pkg/-/test-pkg-1.0.0.tgz"
					}
				}
			}
		}`))
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/test-pkg", nil)
	req.Host = "localhost:8080"
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	body := w.Body.String()
	// URL should be rewritten to point to our proxy
	if !strings.Contains(body, "http://localhost:8080/npm/test-pkg/-/test-pkg-1.0.0.tgz") {
		t.Errorf("Tarball URL not rewritten correctly: %s", body)
	}
	if strings.Contains(body, "registry.npmjs.org") {
		t.Errorf("Original registry URL should be replaced: %s", body)
	}
}

func TestExtractVersionFromTarball(t *testing.T) {
	tests := []struct {
		name        string
		packageName string
		tarballName string
		want        string
	}{
		{
			name:        "simple package",
			packageName: "lodash",
			tarballName: "lodash-4.17.21",
			want:        "4.17.21",
		},
		{
			name:        "scoped package",
			packageName: "@babel/core",
			tarballName: "core-7.23.0",
			want:        "7.23.0",
		},
		{
			name:        "prerelease version",
			packageName: "test",
			tarballName: "test-1.0.0-beta.1",
			want:        "1.0.0-beta.1",
		},
		{
			name:        "no match",
			packageName: "other",
			tarballName: "test-1.0.0",
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractVersionFromTarball(tt.packageName, tt.tarballName)
			if got != tt.want {
				t.Errorf("extractVersionFromTarball() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHandlerCacheHit(t *testing.T) {
	tarballContent := []byte("cached-tarball-content")
	fetchCount := 0

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cached-pkg" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"name": "cached-pkg",
				"versions": {
					"1.0.0": {
						"name": "cached-pkg",
						"version": "1.0.0",
						"dist": {"tarball": "http://example.com/cached-pkg/-/cached-pkg-1.0.0.tgz"}
					}
				}
			}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "cached-pkg-1.0.0.tgz") {
			fetchCount++
			_, _ = w.Write(tarballContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	// First request - should fetch from upstream
	req1 := httptest.NewRequest(http.MethodGet, "/cached-pkg/-/cached-pkg-1.0.0.tgz", nil)
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First request status = %d, want %d", w1.Code, http.StatusOK)
	}
	if w1.Body.String() != string(tarballContent) {
		t.Errorf("First request body = %q, want %q", w1.Body.String(), tarballContent)
	}
	if fetchCount != 1 {
		t.Errorf("Upstream fetch count = %d, want 1", fetchCount)
	}
}
