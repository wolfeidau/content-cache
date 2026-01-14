// Package server provides the HTTP server for the content cache.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/protocol/goproxy"
	"github.com/wolfeidau/content-cache/store"
)

// Config holds server configuration.
type Config struct {
	// Address to listen on (e.g., ":8080")
	Address string

	// StoragePath is the root path for storage
	StoragePath string

	// UpstreamGoProxy is the upstream Go module proxy URL
	UpstreamGoProxy string

	// Logger for the server
	Logger *slog.Logger
}

// Server is the HTTP server for the content cache.
type Server struct {
	config     Config
	httpServer *http.Server
	logger     *slog.Logger

	// Components
	backend  backend.Backend
	store    store.Store
	index    *goproxy.Index
	goproxy  *goproxy.Handler
}

// New creates a new server with the given configuration.
func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Address == "" {
		cfg.Address = ":8080"
	}
	if cfg.StoragePath == "" {
		cfg.StoragePath = "./cache"
	}
	if cfg.UpstreamGoProxy == "" {
		cfg.UpstreamGoProxy = goproxy.DefaultUpstreamURL
	}

	// Initialize storage backend
	fsBackend, err := backend.NewFilesystem(cfg.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("creating filesystem backend: %w", err)
	}

	// Initialize CAFS store
	cafsStore := store.NewCAFS(fsBackend)

	// Initialize goproxy components
	goIndex := goproxy.NewIndex(fsBackend)
	goUpstream := goproxy.NewUpstream(goproxy.WithUpstreamURL(cfg.UpstreamGoProxy))
	goHandler := goproxy.NewHandler(
		goIndex,
		cafsStore,
		goproxy.WithUpstream(goUpstream),
		goproxy.WithLogger(cfg.Logger.With("component", "goproxy")),
	)

	s := &Server{
		config:  cfg,
		logger:  cfg.Logger,
		backend: fsBackend,
		store:   cafsStore,
		index:   goIndex,
		goproxy: goHandler,
	}

	// Build HTTP server
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Addr:         cfg.Address,
		Handler:      s.loggingMiddleware(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute, // Long timeout for large zip downloads
		IdleTimeout:  60 * time.Second,
	}

	return s, nil
}

// registerRoutes sets up the HTTP routes.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Health check
	mux.HandleFunc("GET /health", s.handleHealth)

	// GOPROXY endpoints
	// The goproxy handler handles all paths under /goproxy/
	mux.Handle("GET /goproxy/", http.StripPrefix("/goproxy", s.goproxy))

	// Also support serving at root for direct GOPROXY usage
	// This allows: GOPROXY=http://localhost:8080
	mux.Handle("GET /{module...}", s.goproxy)
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

// loggingMiddleware logs HTTP requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		s.logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration", time.Since(start).String(),
			"remote", r.RemoteAddr,
		)
	})
}

// Start starts the server.
func (s *Server) Start() error {
	s.logger.Info("starting server", "address", s.config.Address)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down server")
	return s.httpServer.Shutdown(ctx)
}

// Address returns the server's listen address.
func (s *Server) Address() string {
	return s.config.Address
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
