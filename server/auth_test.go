package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthMiddleware_NoToken_NoOp(t *testing.T) {
	s := &Server{config: Config{AuthToken: ""}}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/npm/react", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	s := &Server{config: Config{AuthToken: "test-token-123"}}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/npm/react", nil)
	req.Header.Set("Authorization", "Bearer test-token-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	s := &Server{config: Config{AuthToken: "test-token-123"}}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/npm/react", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))

	var body map[string]string
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err)
	require.Equal(t, "unauthorized", body["error"])
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	s := &Server{config: Config{AuthToken: "test-token-123"}}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/npm/react", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAuthMiddleware_WrongScheme(t *testing.T) {
	s := &Server{config: Config{AuthToken: "test-token-123"}}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/npm/react", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAuthMiddleware_ExemptPaths(t *testing.T) {
	s := &Server{
		config: Config{AuthToken: "test-token-123"},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, "path %s should be exempt from auth", path)
		})
	}
}

func TestAuthMiddleware_ProtectedPaths(t *testing.T) {
	s := &Server{
		config: Config{AuthToken: "test-token-123"},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	handler := s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/stats", "/admin/gc", "/admin/gc/status", "/npm/react", "/v2/", "/git/github.com/org/repo.git/info/refs"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code, "path %s should require auth", path)
		})
	}
}
