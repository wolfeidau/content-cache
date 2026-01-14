package goproxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/store"
)

func TestHandlerList(t *testing.T) {
	// Setup mock upstream
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/@v/list") {
			w.Write([]byte("v1.0.0\nv1.1.0\nv1.2.0\n"))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler := newTestHandler(t, mockUpstream.URL)

	// Request version list
	req := httptest.NewRequest("GET", "/github.com/test/module/@v/list", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "v1.0.0") {
		t.Errorf("response missing v1.0.0: %s", body)
	}
}

func TestHandlerInfo(t *testing.T) {
	infoTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.info") {
			json.NewEncoder(w).Encode(VersionInfo{
				Version: "v1.0.0",
				Time:    infoTime,
			})
			return
		}
		// Also handle mod and zip for background caching
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.mod") {
			w.Write([]byte("module github.com/test/module\n"))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.zip") {
			w.Write([]byte("fake zip content"))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler := newTestHandler(t, mockUpstream.URL)

	req := httptest.NewRequest("GET", "/github.com/test/module/@v/v1.0.0.info", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var info VersionInfo
	if err := json.NewDecoder(rec.Body).Decode(&info); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if info.Version != "v1.0.0" {
		t.Errorf("Version = %q, want %q", info.Version, "v1.0.0")
	}
}

func TestHandlerMod(t *testing.T) {
	modContent := "module github.com/test/module\n\ngo 1.21\n"

	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.mod") {
			w.Write([]byte(modContent))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.info") {
			json.NewEncoder(w).Encode(VersionInfo{Version: "v1.0.0"})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.zip") {
			w.Write([]byte("fake zip"))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler := newTestHandler(t, mockUpstream.URL)

	req := httptest.NewRequest("GET", "/github.com/test/module/@v/v1.0.0.mod", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if rec.Body.String() != modContent {
		t.Errorf("body = %q, want %q", rec.Body.String(), modContent)
	}
}

func TestHandlerZip(t *testing.T) {
	zipContent := "PK\x03\x04fake zip content"

	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.zip") {
			w.Header().Set("Content-Type", "application/zip")
			w.Write([]byte(zipContent))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.info") {
			json.NewEncoder(w).Encode(VersionInfo{Version: "v1.0.0"})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.mod") {
			w.Write([]byte("module test\n"))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler := newTestHandler(t, mockUpstream.URL)

	req := httptest.NewRequest("GET", "/github.com/test/module/@v/v1.0.0.zip", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if rec.Header().Get("Content-Type") != "application/zip" {
		t.Errorf("Content-Type = %q, want application/zip", rec.Header().Get("Content-Type"))
	}

	if rec.Body.String() != zipContent {
		t.Errorf("body length = %d, want %d", rec.Body.Len(), len(zipContent))
	}
}

func TestHandlerNotFound(t *testing.T) {
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler := newTestHandler(t, mockUpstream.URL)

	req := httptest.NewRequest("GET", "/github.com/nonexistent/module/@v/v1.0.0.info", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandlerCacheHit(t *testing.T) {
	// Track upstream calls
	upstreamCalls := 0
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.info") {
			json.NewEncoder(w).Encode(VersionInfo{Version: "v1.0.0"})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.mod") {
			w.Write([]byte("module test\n"))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/v1.0.0.zip") {
			w.Write([]byte("zip content"))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler, index, cafsStore := newTestHandlerWithComponents(t, mockUpstream.URL)

	ctx := context.Background()

	// Pre-populate cache
	zipHash, _ := cafsStore.Put(ctx, strings.NewReader("cached zip content"))
	mv := &ModuleVersion{
		Info:    VersionInfo{Version: "v1.0.0"},
		ZipHash: zipHash,
	}
	index.PutModuleVersion(ctx, "github.com/cached/module", "v1.0.0", mv, []byte("module cached\n"))

	// Request cached module
	req := httptest.NewRequest("GET", "/github.com/cached/module/@v/v1.0.0.info", nil)
	rec := httptest.NewRecorder()

	initialCalls := upstreamCalls
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Should not have called upstream
	if upstreamCalls != initialCalls {
		t.Errorf("upstream was called %d times for cached module", upstreamCalls-initialCalls)
	}
}

func TestHandlerZipCacheHit(t *testing.T) {
	upstreamCalls := 0
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler, index, cafsStore := newTestHandlerWithComponents(t, mockUpstream.URL)

	ctx := context.Background()

	// Pre-populate cache with zip
	zipContent := "cached zip content for testing"
	zipHash, _ := cafsStore.Put(ctx, strings.NewReader(zipContent))
	mv := &ModuleVersion{
		Info:    VersionInfo{Version: "v1.0.0"},
		ZipHash: zipHash,
	}
	index.PutModuleVersion(ctx, "github.com/cached/module", "v1.0.0", mv, []byte("module cached\n"))

	// Request cached zip
	req := httptest.NewRequest("GET", "/github.com/cached/module/@v/v1.0.0.zip", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Verify content
	body, _ := io.ReadAll(rec.Body)
	if string(body) != zipContent {
		t.Errorf("body = %q, want %q", body, zipContent)
	}

	// Should not have called upstream
	if upstreamCalls != 0 {
		t.Errorf("upstream was called %d times for cached zip", upstreamCalls)
	}
}

func TestHandlerUppercaseModule(t *testing.T) {
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the path is correctly encoded
		if strings.Contains(r.URL.Path, "!azure") {
			json.NewEncoder(w).Encode(VersionInfo{Version: "v1.0.0"})
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
		http.NotFound(w, r)
	}))
	defer mockUpstream.Close()

	handler := newTestHandler(t, mockUpstream.URL)

	// Request with encoded uppercase path
	req := httptest.NewRequest("GET", "/github.com/!azure/azure-sdk/@v/v1.0.0.info", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandlerInvalidPath(t *testing.T) {
	handler := newTestHandler(t, "http://localhost")

	tests := []struct {
		name string
		path string
	}{
		{"no @v", "/github.com/test/module"},
		{"invalid suffix", "/github.com/test/module/@v/v1.0.0.xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	handler := newTestHandler(t, "http://localhost")

	req := httptest.NewRequest("POST", "/github.com/test/module/@v/list", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// Helper functions

func newTestHandler(t *testing.T, upstreamURL string) *Handler {
	t.Helper()
	handler, _, _ := newTestHandlerWithComponents(t, upstreamURL)
	return handler
}

func newTestHandlerWithComponents(t *testing.T, upstreamURL string) (*Handler, *Index, store.Store) {
	t.Helper()

	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}

	idx := NewIndex(b)
	cafsStore := store.NewCAFS(b)
	upstream := NewUpstream(WithUpstreamURL(upstreamURL))

	handler := NewHandler(idx, cafsStore, WithUpstream(upstream))

	return handler, idx, cafsStore
}
