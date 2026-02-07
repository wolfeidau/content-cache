package git

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/download"
	"github.com/wolfeidau/content-cache/store"
	"github.com/wolfeidau/content-cache/store/metadb"
)

const (
	fakeInfoRefsBody   = "001e# service=git-upload-pack\n0000"
	fakeUploadPackBody = "PACK\x00\x00\x00\x02\x00\x00\x00\x00" // fake pack data
)

// fakeGitUpstream creates a test server that mimics a Git smart HTTP server.
func fakeGitUpstream(t *testing.T) *httptest.Server {
	t.Helper()

	var fetchCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && contains(r.URL.Path, "/info/refs"):
			w.Header().Set("Content-Type", ContentTypeUploadPackAdvertisement)
			_, _ = w.Write([]byte(fakeInfoRefsBody))

		case r.Method == http.MethodPost && contains(r.URL.Path, "/git-upload-pack"):
			fetchCount.Add(1)
			w.Header().Set("Content-Type", ContentTypeUploadPackResult)
			_, _ = w.Write([]byte(fakeUploadPackBody))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	t.Cleanup(func() {
		srv.Close()
	})

	return srv
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestParseGitPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantHost   string
		wantRepo   string
		wantAction string
		wantErr    bool
	}{
		{
			name:       "simple repo",
			path:       "/github.com/user/repo.git/info/refs",
			wantHost:   "github.com",
			wantRepo:   "user/repo",
			wantAction: "info/refs",
		},
		{
			name:       "multi-segment GitLab path",
			path:       "/gitlab.com/group/sub/repo.git/git-upload-pack",
			wantHost:   "gitlab.com",
			wantRepo:   "group/sub/repo",
			wantAction: "git-upload-pack",
		},
		{
			name:       "git-receive-pack action",
			path:       "/github.com/user/repo.git/git-receive-pack",
			wantHost:   "github.com",
			wantRepo:   "user/repo",
			wantAction: "git-receive-pack",
		},
		{
			name:    "path traversal",
			path:    "/github.com/../etc/passwd.git/info/refs",
			wantErr: true,
		},
		{
			name:    "empty segments",
			path:    "/github.com//repo.git/info/refs",
			wantErr: true,
		},
		{
			name:    "missing .git",
			path:    "/github.com/user/repo/info/refs",
			wantErr: true,
		},
		{
			name:    "empty path",
			path:    "/",
			wantErr: true,
		},
		{
			name:    "unknown action",
			path:    "/github.com/user/repo.git/unknown",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseGitPath(tt.path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantHost, result.Repo.Host)
			require.Equal(t, tt.wantRepo, result.Repo.RepoPath)
			require.Equal(t, tt.wantAction, result.Action)
		})
	}
}

func TestHandlerInfoRefs(t *testing.T) {
	upstream := fakeGitUpstream(t)

	// Create handler that points upstream at the test server.
	// Since our upstream uses the repo's UpstreamURL (https://github.com/...),
	// we need to override the HTTP client's transport to redirect to our test server.
	h, cleanup := newTestHandlerWithTransport(t, upstream)
	defer cleanup()

	t.Run("passthrough", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/user/repo.git/info/refs?service=git-upload-pack", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, ContentTypeUploadPackAdvertisement, w.Header().Get("Content-Type"))
		require.Equal(t, fakeInfoRefsBody, w.Body.String())
	})

	t.Run("forwards Git-Protocol header", func(t *testing.T) {
		var receivedProtocol string
		protocolSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedProtocol = r.Header.Get("Git-Protocol")
			w.Header().Set("Content-Type", ContentTypeUploadPackAdvertisement)
			_, _ = w.Write([]byte(fakeInfoRefsBody))
		}))
		defer protocolSrv.Close()

		ph, pcleanup := newTestHandlerWithTransport(t, protocolSrv)
		defer pcleanup()

		req := httptest.NewRequest(http.MethodGet, "/github.com/user/repo.git/info/refs?service=git-upload-pack", nil)
		req.Header.Set("Git-Protocol", "version=2")
		w := httptest.NewRecorder()

		ph.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "version=2", receivedProtocol)
	})

	t.Run("rejects git-receive-pack service", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/user/repo.git/info/refs?service=git-receive-pack", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestHandlerUploadPack(t *testing.T) {
	upstream := fakeGitUpstream(t)
	h, cleanup := newTestHandlerWithTransport(t, upstream)
	defer cleanup()

	requestBody := []byte("0032want abc123\n00000009done\n")

	t.Run("cache miss then hit", func(t *testing.T) {
		// First request — cache miss, fetches from upstream
		req := httptest.NewRequest(http.MethodPost, "/github.com/user/repo.git/git-upload-pack", bytes.NewReader(requestBody))
		req.Header.Set("Content-Type", ContentTypeUploadPackRequest)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, ContentTypeUploadPackResult, w.Header().Get("Content-Type"))
		firstBody := w.Body.String()

		// Second request — same body, should be cache hit
		req2 := httptest.NewRequest(http.MethodPost, "/github.com/user/repo.git/git-upload-pack", bytes.NewReader(requestBody))
		req2.Header.Set("Content-Type", ContentTypeUploadPackRequest)
		w2 := httptest.NewRecorder()

		h.ServeHTTP(w2, req2)

		require.Equal(t, http.StatusOK, w2.Code)
		require.Equal(t, firstBody, w2.Body.String())
	})

	t.Run("cross-repo isolation", func(t *testing.T) {
		// Same request body to a different repo should not collide
		req := httptest.NewRequest(http.MethodPost, "/github.com/other/repo.git/git-upload-pack", bytes.NewReader(requestBody))
		req.Header.Set("Content-Type", ContentTypeUploadPackRequest)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		// Should succeed (fetches from upstream, not from cache of user/repo)
		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestHandlerHostAllowlist(t *testing.T) {
	upstream := fakeGitUpstream(t)
	h, cleanup := newTestHandlerWithTransport(t, upstream)
	defer cleanup()

	t.Run("allowed host", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/user/repo.git/info/refs?service=git-upload-pack", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("disallowed host", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/evil.com/user/repo.git/info/refs?service=git-upload-pack", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestHandlerPushRejection(t *testing.T) {
	upstream := fakeGitUpstream(t)
	h, cleanup := newTestHandlerWithTransport(t, upstream)
	defer cleanup()

	t.Run("POST git-receive-pack is forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/github.com/user/repo.git/git-receive-pack", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestHandlerRequestBodyLimit(t *testing.T) {
	upstream := fakeGitUpstream(t)

	tmpDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)
	cafs := store.NewCAFS(b)

	db := metadb.NewBoltDB(metadb.WithNoSync(true))
	require.NoError(t, db.Open(filepath.Join(tmpDir, "meta.db")))
	defer func() { _ = db.Close() }()

	packIdx, err := metadb.NewEnvelopeIndex(db, "git", "pack", 24*time.Hour)
	require.NoError(t, err)
	idx := NewIndex(packIdx)
	dl := download.New()

	h := NewHandler(idx, cafs,
		WithUpstream(NewUpstream(WithHTTPClient(redirectClient(upstream.URL)))),
		WithDownloader(dl),
		WithAllowedHosts([]string{"github.com"}),
		WithMaxRequestBodySize(10), // 10 bytes max
	)

	// Send body larger than limit
	bigBody := bytes.Repeat([]byte("x"), 100)
	req := httptest.NewRequest(http.MethodPost, "/github.com/user/repo.git/git-upload-pack", bytes.NewReader(bigBody))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestHandlerSingleflight(t *testing.T) {
	var fetchCount atomic.Int32
	var fetchGate sync.WaitGroup
	fetchGate.Add(1)

	slowUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && contains(r.URL.Path, "/git-upload-pack") {
			fetchCount.Add(1)
			fetchGate.Wait() // Block until released
			w.Header().Set("Content-Type", ContentTypeUploadPackResult)
			_, _ = w.Write([]byte(fakeUploadPackBody))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer slowUpstream.Close()

	h, cleanup := newTestHandlerWithTransport(t, slowUpstream)
	defer cleanup()

	body := []byte("0032want abc123\n00000009done\n")

	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, 3)

	for i := range 3 {
		wg.Add(1)
		results[i] = httptest.NewRecorder()
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/github.com/user/repo.git/git-upload-pack", bytes.NewReader(body))
			h.ServeHTTP(results[idx], req)
		}(i)
	}

	// Allow some time for all requests to hit the downloader
	time.Sleep(50 * time.Millisecond)

	// Release the upstream
	fetchGate.Done()
	wg.Wait()

	// Only one upstream fetch should have occurred
	require.Equal(t, int32(1), fetchCount.Load(), "expected single upstream fetch due to singleflight")

	// All responses should succeed
	for i, w := range results {
		require.Equal(t, http.StatusOK, w.Code, "request %d failed", i)
	}
}

// newTestHandlerWithTransport creates a test handler that redirects all HTTPS
// requests to the given test server via a custom transport.
func newTestHandlerWithTransport(t *testing.T, targetServer *httptest.Server) (*Handler, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)

	b, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)
	cafs := store.NewCAFS(b)

	db := metadb.NewBoltDB(metadb.WithNoSync(true))
	require.NoError(t, db.Open(filepath.Join(tmpDir, "meta.db")))

	packIdx, err := metadb.NewEnvelopeIndex(db, "git", "pack", 24*time.Hour)
	require.NoError(t, err)
	idx := NewIndex(packIdx)
	dl := download.New()

	upstream := NewUpstream(WithHTTPClient(redirectClient(targetServer.URL)))

	h := NewHandler(idx, cafs,
		WithUpstream(upstream),
		WithDownloader(dl),
		WithAllowedHosts([]string{"github.com", "gitlab.com"}),
	)

	return h, func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}
}

// redirectClient creates an HTTP client that rewrites all request URLs
// to point at the given target URL, preserving the path and query.
func redirectClient(targetURL string) *http.Client {
	return &http.Client{
		Transport: &rewriteTransport{target: targetURL},
	}
}

type rewriteTransport struct {
	target string
}

func (rt *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Rewrite the URL to point at the test server
	newURL := rt.target + req.URL.Path
	if req.URL.RawQuery != "" {
		newURL += "?" + req.URL.RawQuery
	}
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, newURL, req.Body)
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header
	return http.DefaultTransport.RoundTrip(newReq)
}
