package server

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// authMiddleware returns middleware that validates Bearer token authentication.
// When AuthToken is empty, the middleware is a no-op (allows unauthenticated access).
// Exact paths /health and /metrics are exempt from authentication.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	if s.config.AuthToken == "" {
		return next
	}

	tokenBytes := []byte(s.config.AuthToken)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Exempt exact paths for health checks and metrics.
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			unauthorizedResponse(w)
			return
		}

		provided := []byte(strings.TrimPrefix(auth, "Bearer "))
		if subtle.ConstantTimeCompare(provided, tokenBytes) != 1 {
			unauthorizedResponse(w)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func unauthorizedResponse(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"}) //nolint:errcheck
}
