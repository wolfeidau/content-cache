package npm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpstreamTarballURL(t *testing.T) {
	u := NewUpstream()

	tests := []struct {
		name    string
		pkg     string
		version string
		want    string
	}{
		{
			name:    "simple package",
			pkg:     "lodash",
			version: "4.17.21",
			want:    "https://registry.npmjs.org/lodash/-/lodash-4.17.21.tgz",
		},
		{
			name:    "scoped package",
			pkg:     "@babel/core",
			version: "7.23.0",
			want:    "https://registry.npmjs.org/@babel%2fcore/-/core-7.23.0.tgz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := u.TarballURL(tt.pkg, tt.version)
			if got != tt.want {
				t.Errorf("TarballURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEncodePackageName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple package",
			input: "lodash",
			want:  "lodash",
		},
		{
			name:  "scoped package",
			input: "@babel/core",
			want:  "@babel%2fcore",
		},
		{
			name:  "package with special chars",
			input: "some-package",
			want:  "some-package",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodePackageName(tt.input)
			if got != tt.want {
				t.Errorf("encodePackageName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecodePackageName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "simple package",
			input: "lodash",
			want:  "lodash",
		},
		{
			name:  "scoped package encoded",
			input: "@babel%2fcore",
			want:  "@babel/core",
		},
		{
			name:  "already decoded scoped",
			input: "@babel/core",
			want:  "@babel/core",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodePackageName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodePackageName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("decodePackageName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUpstreamFetchPackageMetadataRaw(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lodash" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"lodash","versions":{}}`))
			return
		}
		if r.URL.Path == "/not-found" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	u := NewUpstream(WithRegistryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		data, err := u.FetchPackageMetadataRaw(context.Background(), "lodash")
		if err != nil {
			t.Fatalf("FetchPackageMetadataRaw() error = %v", err)
		}
		if string(data) != `{"name":"lodash","versions":{}}` {
			t.Errorf("FetchPackageMetadataRaw() = %q", data)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := u.FetchPackageMetadataRaw(context.Background(), "not-found")
		if err != ErrNotFound {
			t.Errorf("FetchPackageMetadataRaw() error = %v, want ErrNotFound", err)
		}
	})
}

func TestUpstreamFetchTarball(t *testing.T) {
	tarballContent := []byte("fake tarball content")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test/-/test-1.0.0.tgz" {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(tarballContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	u := NewUpstream(WithRegistryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		rc, err := u.FetchTarball(context.Background(), server.URL+"/test/-/test-1.0.0.tgz")
		if err != nil {
			t.Fatalf("FetchTarball() error = %v", err)
		}
		defer func() { _ = rc.Close() }()
	})

	t.Run("not found", func(t *testing.T) {
		_, err := u.FetchTarball(context.Background(), server.URL+"/nonexistent/-/nonexistent-1.0.0.tgz")
		if err != ErrNotFound {
			t.Errorf("FetchTarball() error = %v, want ErrNotFound", err)
		}
	})
}

func TestWithRegistryURL(t *testing.T) {
	u := NewUpstream(WithRegistryURL("https://custom.registry.com/"))

	// Should trim trailing slash
	url := u.TarballURL("test", "1.0.0")
	if url != "https://custom.registry.com/test/-/test-1.0.0.tgz" {
		t.Errorf("TarballURL() = %q", url)
	}
}
