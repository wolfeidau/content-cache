package pypi

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
	tmpDir, err := os.MkdirTemp("", "pypi-test-*")
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
		opts = append(opts, WithSimpleURL(upstreamServer.URL+"/simple/"))
	}
	upstream := NewUpstream(opts...)

	h := NewHandler(idx, cafs, WithUpstream(upstream))
	return h, func() {
		h.Close()
		_ = os.RemoveAll(tmpDir)
	}
}

func TestHandlerProject(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/simple/requests/" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>Links for requests</title></head>
<body>
<h1>Links for requests</h1>
<a href="https://files.pythonhosted.org/packages/requests-2.31.0.tar.gz#sha256=942c5a758f98d790eaed1a29cb6eefc7ffb0d1cf7af05c3d2791656dbd6ad1e1">requests-2.31.0.tar.gz</a><br/>
<a href="https://files.pythonhosted.org/packages/requests-2.31.0-py3-none-any.whl#sha256=58cd2187c01e70e6e26505bca751777aa9f2ee0b7f4300988b709f44e013003f" data-requires-python=">=3.7">requests-2.31.0-py3-none-any.whl</a><br/>
</body>
</html>`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	t.Run("get project page HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple/requests/", nil)
		req.Host = "localhost:8080"
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, "requests-2.31.0") {
			t.Error("Response should contain package filename")
		}
		// Check URL is rewritten to proxy with /pypi prefix
		if !strings.Contains(body, "/pypi/packages/requests/") {
			t.Error("Response should have rewritten URLs pointing to /pypi/packages/")
		}
	})

	t.Run("get project page JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple/requests/", nil)
		req.Header.Set("Accept", ContentTypeJSON)
		req.Host = "localhost:8080"
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
		body := w.Body.String()
		if !strings.Contains(body, `"api-version"`) {
			t.Error("JSON response should contain api-version")
		}
		if !strings.Contains(body, `"name":"requests"`) {
			t.Error("JSON response should contain project name")
		}
	})

	t.Run("project not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple/nonexistent-pkg/", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("redirect missing trailing slash", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple/requests", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMovedPermanently {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusMovedPermanently)
		}
		if loc := w.Header().Get("Location"); loc != "/simple/requests/" {
			t.Errorf("Location = %q, want /simple/requests/", loc)
		}
	})
}

func TestHandlerRoot(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	h, cleanup := newTestHandler(t, upstream)
	defer cleanup()

	t.Run("empty root HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple/", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
		if !strings.Contains(w.Body.String(), "Simple Index") {
			t.Error("Response should contain Simple Index title")
		}
	})

	t.Run("empty root JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple/", nil)
		req.Header.Set("Accept", ContentTypeJSON)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
		}
		body := w.Body.String()
		if !strings.Contains(body, `"api-version"`) {
			t.Error("JSON response should contain api-version")
		}
		if !strings.Contains(body, `"projects"`) {
			t.Error("JSON response should contain projects array")
		}
	})

	t.Run("redirect missing trailing slash", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/simple", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMovedPermanently {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusMovedPermanently)
		}
	})
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	h, cleanup := newTestHandler(t, nil)
	defer cleanup()

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/simple/test/", nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("Status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}

func TestNormalizeProjectName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"requests", "requests"},
		{"Requests", "requests"},
		{"requests_oauthlib", "requests-oauthlib"},
		{"requests.oauthlib", "requests-oauthlib"},
		{"requests-oauthlib", "requests-oauthlib"},
		{"Django", "django"},
		{"Pillow", "pillow"},
		{"some__weird___name", "some-weird-name"},
		{"A.B_C-D", "a-b-c-d"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := NormalizeProjectName(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeProjectName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseProjectPageHTML(t *testing.T) {
	html := `<!DOCTYPE html>
<html>
<head><title>Links for requests</title></head>
<body>
<h1>Links for requests</h1>
<a href="https://files.pythonhosted.org/packages/requests-2.31.0.tar.gz#sha256=942c5a758f">requests-2.31.0.tar.gz</a><br/>
<a href="../packages/requests-2.31.0-py3-none-any.whl#sha256=58cd2187c0" data-requires-python="&gt;=3.7">requests-2.31.0-py3-none-any.whl</a><br/>
<a href="https://example.com/yanked.whl" data-yanked="">yanked.whl</a><br/>
<a href="https://example.com/yanked-reason.whl" data-yanked="security issue">yanked-reason.whl</a><br/>
</body>
</html>`

	files, err := ParseProjectPageHTML([]byte(html), "https://pypi.org/simple/requests/")
	if err != nil {
		t.Fatalf("ParseProjectPageHTML() error = %v", err)
	}

	if len(files) != 4 {
		t.Fatalf("len(files) = %d, want 4", len(files))
	}

	// Check first file
	if files[0].Filename != "requests-2.31.0.tar.gz" {
		t.Errorf("files[0].Filename = %q, want requests-2.31.0.tar.gz", files[0].Filename)
	}
	if files[0].Hashes["sha256"] != "942c5a758f" {
		t.Errorf("files[0].Hashes[sha256] = %q, want 942c5a758f", files[0].Hashes["sha256"])
	}

	// Check second file with requires-python
	if files[1].RequiresPython != ">=3.7" {
		t.Errorf("files[1].RequiresPython = %q, want >=3.7", files[1].RequiresPython)
	}

	// Check relative URL resolution
	if !strings.HasPrefix(files[1].URL, "https://pypi.org/") {
		t.Errorf("Relative URL not resolved correctly: %s", files[1].URL)
	}

	// Check yanked files
	if files[2].Yanked != true {
		t.Errorf("files[2].Yanked = %v, want true", files[2].Yanked)
	}
	if files[3].Yanked != "security issue" {
		t.Errorf("files[3].Yanked = %v, want 'security issue'", files[3].Yanked)
	}
}

func TestParseHashFragment(t *testing.T) {
	tests := []struct {
		fragment string
		wantAlg  string
		wantHash string
	}{
		{"sha256=942c5a758f98d790", "sha256", "942c5a758f98d790"},
		{"md5=abc123def456", "md5", "abc123def456"},
		{"sha512=abcdef0123456789", "sha512", "abcdef0123456789"},
		{"invalid", "", ""},
		{"sha256=", "", ""},
		{"=abc123", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.fragment, func(t *testing.T) {
			hashes := parseHashFragment(tt.fragment)
			if tt.wantAlg == "" {
				if hashes != nil {
					t.Errorf("parseHashFragment(%q) = %v, want nil", tt.fragment, hashes)
				}
				return
			}
			if hashes == nil {
				t.Fatalf("parseHashFragment(%q) = nil, want non-nil", tt.fragment)
			}
			if hashes[tt.wantAlg] != tt.wantHash {
				t.Errorf("parseHashFragment(%q)[%s] = %q, want %q", tt.fragment, tt.wantAlg, hashes[tt.wantAlg], tt.wantHash)
			}
		})
	}
}

func TestHandlerFile(t *testing.T) {
	fileContent := []byte("fake-wheel-content-12345")

	var upstreamURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/simple/test-pkg/" {
			w.Header().Set("Content-Type", "text/html")
			// Use absolute URL to upstream server for the file
			_, _ = w.Write([]byte(`<!DOCTYPE html>
<html><body>
<a href="` + upstreamURL + `/files/test_pkg-1.0.0-py3-none-any.whl#sha256=e8d468b23a7abc7dfe5cf9832305250ad33b58a189ffa6037368d41278aab330">test_pkg-1.0.0-py3-none-any.whl</a>
</body></html>`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "test_pkg-1.0.0-py3-none-any.whl") {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(fileContent)
			return
		}
		t.Logf("404 for path: %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	upstreamURL = upstream.URL
	defer upstream.Close()

	// Create shared storage for this test
	tmpDir, err := os.MkdirTemp("", "pypi-file-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	cafs := store.NewCAFS(b)
	idx := NewIndex(b)

	pypiUpstream := NewUpstream(WithSimpleURL(upstream.URL + "/simple/"))
	h := NewHandler(idx, cafs, WithUpstream(pypiUpstream))

	// First fetch the project page to populate the index
	req := httptest.NewRequest(http.MethodGet, "/simple/test-pkg/", nil)
	req.Host = "localhost:8080"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Project page status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	// Wait for async caching to complete
	h.Close()

	// Create new handler with same storage
	h2 := NewHandler(idx, cafs, WithUpstream(pypiUpstream))
	defer h2.Close()

	t.Run("download file", func(t *testing.T) {
		// Note: In real usage, /pypi prefix is stripped by server, so handler sees /packages/...
		req := httptest.NewRequest(http.MethodGet, "/packages/test-pkg/test_pkg-1.0.0-py3-none-any.whl", nil)
		w := httptest.NewRecorder()

		h2.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}
		if w.Body.String() != string(fileContent) {
			t.Errorf("Body = %q, want %q", w.Body.String(), fileContent)
		}
	})

	t.Run("file not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/packages/nonexistent/file.whl", nil)
		w := httptest.NewRecorder()

		h2.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}
