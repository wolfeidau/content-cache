package oci

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthCache(t *testing.T) {
	ac := NewAuthCache()

	t.Run("get empty", func(t *testing.T) {
		token := ac.GetToken("repository:test:pull")
		if token != "" {
			t.Errorf("GetToken() = %q, want empty", token)
		}
	})

	t.Run("set and get", func(t *testing.T) {
		ac.SetToken("repository:test:pull", "test-token", 300)
		token := ac.GetToken("repository:test:pull")
		if token != "test-token" {
			t.Errorf("GetToken() = %q, want %q", token, "test-token")
		}
	})

	t.Run("different scope", func(t *testing.T) {
		token := ac.GetToken("repository:other:pull")
		if token != "" {
			t.Errorf("GetToken() = %q, want empty for different scope", token)
		}
	})

	t.Run("expired token", func(t *testing.T) {
		ac.SetToken("repository:expired:pull", "expired-token", 0)
		// Token with 0 expiry is immediately expired (with 30s buffer)
		token := ac.GetToken("repository:expired:pull")
		if token != "" {
			t.Errorf("GetToken() = %q, want empty for expired token", token)
		}
	})

	t.Run("clear", func(t *testing.T) {
		ac.SetToken("repository:clear:pull", "clear-token", 300)
		ac.Clear()
		token := ac.GetToken("repository:clear:pull")
		if token != "" {
			t.Errorf("GetToken() = %q, want empty after clear", token)
		}
	})
}

func TestParseWWWAuthenticate(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantRealm   string
		wantService string
		wantScope   string
		wantErr     bool
	}{
		{
			name:        "docker hub",
			header:      `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull"`,
			wantRealm:   "https://auth.docker.io/token",
			wantService: "registry.docker.io",
			wantScope:   "repository:library/alpine:pull",
		},
		{
			name:        "ghcr",
			header:      `Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:owner/repo:pull"`,
			wantRealm:   "https://ghcr.io/token",
			wantService: "ghcr.io",
			wantScope:   "repository:owner/repo:pull",
		},
		{
			name:        "no scope",
			header:      `Bearer realm="https://example.com/token",service="example.com"`,
			wantRealm:   "https://example.com/token",
			wantService: "example.com",
			wantScope:   "",
		},
		{
			name:    "basic auth",
			header:  `Basic realm="Registry"`,
			wantErr: true,
		},
		{
			name:    "missing realm",
			header:  `Bearer service="test"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			challenge, err := ParseWWWAuthenticate(tt.header)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWWWAuthenticate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if challenge.Realm != tt.wantRealm {
				t.Errorf("Realm = %q, want %q", challenge.Realm, tt.wantRealm)
			}
			if challenge.Service != tt.wantService {
				t.Errorf("Service = %q, want %q", challenge.Service, tt.wantService)
			}
			if challenge.Scope != tt.wantScope {
				t.Errorf("Scope = %q, want %q", challenge.Scope, tt.wantScope)
			}
		})
	}
}

func TestFetchToken(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("service") != "registry.docker.io" {
				t.Errorf("service = %q", r.URL.Query().Get("service"))
			}
			if r.URL.Query().Get("scope") != "repository:library/alpine:pull" {
				t.Errorf("scope = %q", r.URL.Query().Get("scope"))
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TokenResponse{
				Token:     "test-token-123",
				ExpiresIn: 300,
			})
		}))
		defer server.Close()

		challenge := &AuthChallenge{
			Realm:   server.URL,
			Service: "registry.docker.io",
			Scope:   "repository:library/alpine:pull",
		}

		resp, err := FetchToken(context.Background(), http.DefaultClient, challenge, "", "")
		if err != nil {
			t.Fatalf("FetchToken() error = %v", err)
		}
		if resp.Token != "test-token-123" {
			t.Errorf("Token = %q, want %q", resp.Token, "test-token-123")
		}
		if resp.ExpiresIn != 300 {
			t.Errorf("ExpiresIn = %d, want %d", resp.ExpiresIn, 300)
		}
	})

	t.Run("access_token field", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			// Some registries use access_token instead of token
			_ = json.NewEncoder(w).Encode(TokenResponse{
				AccessToken: "access-token-456",
				ExpiresIn:   600,
			})
		}))
		defer server.Close()

		challenge := &AuthChallenge{Realm: server.URL}
		resp, err := FetchToken(context.Background(), http.DefaultClient, challenge, "", "")
		if err != nil {
			t.Fatalf("FetchToken() error = %v", err)
		}
		if resp.Token != "access-token-456" {
			t.Errorf("Token = %q, want %q", resp.Token, "access-token-456")
		}
	})

	t.Run("with basic auth", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok || user != "testuser" || pass != "testpass" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TokenResponse{Token: "authed-token"})
		}))
		defer server.Close()

		challenge := &AuthChallenge{Realm: server.URL}
		resp, err := FetchToken(context.Background(), http.DefaultClient, challenge, "testuser", "testpass")
		if err != nil {
			t.Fatalf("FetchToken() error = %v", err)
		}
		if resp.Token != "authed-token" {
			t.Errorf("Token = %q, want %q", resp.Token, "authed-token")
		}
	})

	t.Run("default expiry", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TokenResponse{Token: "token"})
		}))
		defer server.Close()

		challenge := &AuthChallenge{Realm: server.URL}
		resp, err := FetchToken(context.Background(), http.DefaultClient, challenge, "", "")
		if err != nil {
			t.Fatalf("FetchToken() error = %v", err)
		}
		if resp.ExpiresIn != 300 {
			t.Errorf("ExpiresIn = %d, want 300 (default)", resp.ExpiresIn)
		}
	})

	t.Run("error response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("access denied"))
		}))
		defer server.Close()

		challenge := &AuthChallenge{Realm: server.URL}
		_, err := FetchToken(context.Background(), http.DefaultClient, challenge, "", "")
		if err == nil {
			t.Error("FetchToken() expected error")
		}
	})
}

func TestBuildScope(t *testing.T) {
	tests := []struct {
		name   string
		image  string
		action string
		want   string
	}{
		{
			name:   "simple pull",
			image:  "library/alpine",
			action: "pull",
			want:   "repository:library/alpine:pull",
		},
		{
			name:   "push",
			image:  "myrepo/myimage",
			action: "push",
			want:   "repository:myrepo/myimage:push",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildScope(tt.image, tt.action); got != tt.want {
				t.Errorf("BuildScope() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAuthCacheConcurrency(t *testing.T) {
	ac := NewAuthCache()
	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(i int) {
			ac.SetToken("scope", "token", 300)
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			_ = ac.GetToken("scope")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for goroutines")
		}
	}
}
