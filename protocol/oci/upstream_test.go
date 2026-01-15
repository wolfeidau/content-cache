package oci

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpstreamCheckVersion(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v2/" {
				t.Errorf("path = %q, want /v2/", r.URL.Path)
			}
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		u := NewUpstream(WithRegistryURL(server.URL))
		err := u.CheckVersion(context.Background())
		if err != nil {
			t.Errorf("CheckVersion() error = %v", err)
		}
	})

	t.Run("auth required returns no error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer server.Close()

		u := NewUpstream(WithRegistryURL(server.URL))
		err := u.CheckVersion(context.Background())
		// 401 is acceptable for version check (means registry exists)
		if err != nil {
			t.Errorf("CheckVersion() error = %v", err)
		}
	})
}

func TestUpstreamFetchManifest(t *testing.T) {
	manifestContent := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`
	manifestDigest := "sha256:abc123"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/library/alpine/manifests/latest" {
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write([]byte(manifestContent))
			return
		}
		if r.URL.Path == "/v2/library/notfound/manifests/latest" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	u := NewUpstream(WithRegistryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		content, mediaType, digest, err := u.FetchManifest(context.Background(), "library/alpine", "latest")
		if err != nil {
			t.Fatalf("FetchManifest() error = %v", err)
		}
		if string(content) != manifestContent {
			t.Errorf("content = %q", content)
		}
		if mediaType != "application/vnd.oci.image.manifest.v1+json" {
			t.Errorf("mediaType = %q", mediaType)
		}
		if digest != manifestDigest {
			t.Errorf("digest = %q", digest)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, _, _, err := u.FetchManifest(context.Background(), "library/notfound", "latest")
		if err != ErrNotFound {
			t.Errorf("FetchManifest() error = %v, want ErrNotFound", err)
		}
	})
}

func TestUpstreamHeadManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %q, want HEAD", r.Method)
		}
		if r.URL.Path == "/v2/library/alpine/manifests/latest" {
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:abc123")
			w.Header().Set("Content-Length", "1024")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	u := NewUpstream(WithRegistryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		digest, size, mediaType, err := u.HeadManifest(context.Background(), "library/alpine", "latest")
		if err != nil {
			t.Fatalf("HeadManifest() error = %v", err)
		}
		if digest != "sha256:abc123" {
			t.Errorf("digest = %q", digest)
		}
		if size != 1024 {
			t.Errorf("size = %d", size)
		}
		if mediaType != "application/vnd.oci.image.manifest.v1+json" {
			t.Errorf("mediaType = %q", mediaType)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, _, _, err := u.HeadManifest(context.Background(), "library/notfound", "latest")
		if err != ErrNotFound {
			t.Errorf("HeadManifest() error = %v, want ErrNotFound", err)
		}
	})
}

func TestUpstreamFetchBlob(t *testing.T) {
	blobContent := []byte("blob content here")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/library/alpine/blobs/sha256:abc123" {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(blobContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	u := NewUpstream(WithRegistryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		rc, size, err := u.FetchBlob(context.Background(), "library/alpine", "sha256:abc123")
		if err != nil {
			t.Fatalf("FetchBlob() error = %v", err)
		}
		defer func() { _ = rc.Close() }()

		content, _ := io.ReadAll(rc)
		if string(content) != string(blobContent) {
			t.Errorf("content = %q", content)
		}
		_ = size // size may be -1 if Content-Length not set
	})

	t.Run("not found", func(t *testing.T) {
		_, _, err := u.FetchBlob(context.Background(), "library/alpine", "sha256:notfound")
		if err != ErrNotFound {
			t.Errorf("FetchBlob() error = %v, want ErrNotFound", err)
		}
	})
}

func TestUpstreamHeadBlob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %q, want HEAD", r.Method)
		}
		if r.URL.Path == "/v2/library/alpine/blobs/sha256:abc123" {
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	u := NewUpstream(WithRegistryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		size, err := u.HeadBlob(context.Background(), "library/alpine", "sha256:abc123")
		if err != nil {
			t.Fatalf("HeadBlob() error = %v", err)
		}
		if size != 4096 {
			t.Errorf("size = %d, want 4096", size)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := u.HeadBlob(context.Background(), "library/alpine", "sha256:notfound")
		if err != ErrNotFound {
			t.Errorf("HeadBlob() error = %v, want ErrNotFound", err)
		}
	})
}

func TestUpstreamWithAuth(t *testing.T) {
	authCallCount := 0

	// Auth server
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCallCount++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TokenResponse{
			Token:     "test-token",
			ExpiresIn: 300,
		})
	}))
	defer authServer.Close()

	// Registry server that requires auth
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+authServer.URL+`",service="test",scope="repository:library/alpine:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/v2/library/alpine/manifests/latest" {
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:abc")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer registry.Close()

	u := NewUpstream(WithRegistryURL(registry.URL))

	// First request should trigger auth
	_, _, _, err := u.FetchManifest(context.Background(), "library/alpine", "latest")
	if err != nil {
		t.Fatalf("FetchManifest() error = %v", err)
	}
	if authCallCount != 1 {
		t.Errorf("auth calls = %d, want 1", authCallCount)
	}

	// Second request should use cached token
	_, _, _, err = u.FetchManifest(context.Background(), "library/alpine", "latest")
	if err != nil {
		t.Fatalf("FetchManifest() error = %v", err)
	}
	if authCallCount != 1 {
		t.Errorf("auth calls = %d, want 1 (should use cached token)", authCallCount)
	}
}

func TestUpstreamOptions(t *testing.T) {
	t.Run("default registry", func(t *testing.T) {
		u := NewUpstream()
		if u.baseURL != DefaultRegistryURL {
			t.Errorf("baseURL = %q, want %q", u.baseURL, DefaultRegistryURL)
		}
	})

	t.Run("custom registry", func(t *testing.T) {
		u := NewUpstream(WithRegistryURL("https://gcr.io"))
		if u.baseURL != "https://gcr.io" {
			t.Errorf("baseURL = %q", u.baseURL)
		}
	})

	t.Run("trailing slash removed", func(t *testing.T) {
		u := NewUpstream(WithRegistryURL("https://gcr.io/"))
		if u.baseURL != "https://gcr.io" {
			t.Errorf("baseURL = %q", u.baseURL)
		}
	})

	t.Run("basic auth", func(t *testing.T) {
		u := NewUpstream(WithBasicAuth("user", "pass"))
		if u.username != "user" || u.password != "pass" {
			t.Errorf("credentials not set correctly")
		}
	})
}
