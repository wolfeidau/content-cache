// Package server provides the HTTP server for the content cache.
package server

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/expiry"
	"github.com/wolfeidau/content-cache/protocol/goproxy"
	"github.com/wolfeidau/content-cache/protocol/maven"
	"github.com/wolfeidau/content-cache/protocol/npm"
	"github.com/wolfeidau/content-cache/protocol/oci"
	"github.com/wolfeidau/content-cache/protocol/pypi"
	"github.com/wolfeidau/content-cache/store"
	"github.com/wolfeidau/content-cache/telemetry"
)

// Config holds server configuration.
type Config struct {
	// Address to listen on (e.g., ":8080")
	Address string

	// StoragePath is the root path for storage
	StoragePath string

	// UpstreamGoProxy is the upstream Go module proxy URL
	UpstreamGoProxy string

	// UpstreamNPMRegistry is the upstream NPM registry URL
	UpstreamNPMRegistry string

	// UpstreamOCIRegistry is the upstream OCI registry URL
	UpstreamOCIRegistry string

	// OCIUsername for registry authentication (optional)
	OCIUsername string

	// OCIPassword for registry authentication (optional)
	OCIPassword string

	// OCITagTTL is how long to cache tag->digest mappings.
	// Default: 5 minutes (tags can change, unlike digests)
	OCITagTTL time.Duration

	// UpstreamPyPI is the upstream PyPI Simple API URL
	UpstreamPyPI string

	// PyPIMetadataTTL is how long to cache project metadata.
	// Default: 5 minutes (new versions may be published)
	PyPIMetadataTTL time.Duration

	// UpstreamMaven is the upstream Maven repository URL
	UpstreamMaven string

	// MavenMetadataTTL is how long to cache maven-metadata.xml.
	// Default: 5 minutes (new versions may be published)
	MavenMetadataTTL time.Duration

	// CacheTTL is the time-to-live for cached content.
	// Content not accessed within this duration is expired.
	// Zero disables TTL-based expiration.
	CacheTTL time.Duration

	// CacheMaxSize is the maximum size of the cache in bytes.
	// When exceeded, least-recently-used content is evicted.
	// Zero disables size-based eviction.
	CacheMaxSize int64

	// ExpiryCheckInterval is how often to check for expired content.
	// Default is 1 hour.
	ExpiryCheckInterval time.Duration

	// Logger for the server
	Logger *slog.Logger
}

// Server is the HTTP server for the content cache.
type Server struct {
	config     Config
	httpServer *http.Server
	logger     *slog.Logger

	// Components
	backend    backend.Backend
	store      store.Store
	index      *goproxy.Index
	goproxy    *goproxy.Handler
	npmIndex   *npm.Index
	npm        *npm.Handler
	ociIndex   *oci.Index
	oci        *oci.Handler
	pypiIndex  *pypi.Index
	pypi       *pypi.Handler
	mavenIndex *maven.Index
	maven      *maven.Handler
	metadata   *expiry.MetadataStore
	expiryMgr  *expiry.Manager
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

	// Initialize metadata store for expiration tracking
	metadataStore := expiry.NewMetadataStore(fsBackend)

	// Initialize CAFS store with metadata tracking
	cafsStore := store.NewCAFS(fsBackend, store.WithMetadataTracker(metadataStore))

	// Initialize expiry manager if TTL or MaxSize is configured
	var expiryMgr *expiry.Manager
	if cfg.CacheTTL > 0 || cfg.CacheMaxSize > 0 {
		expiryCfg := expiry.Config{
			TTL:           cfg.CacheTTL,
			MaxSize:       cfg.CacheMaxSize,
			CheckInterval: cfg.ExpiryCheckInterval,
			Logger:        cfg.Logger.With("component", "expiry"),
		}
		if expiryCfg.CheckInterval == 0 {
			expiryCfg.CheckInterval = 1 * time.Hour
		}
		expiryMgr = expiry.NewManager(metadataStore, fsBackend, expiryCfg)
	}

	// Initialize goproxy components
	goIndex := goproxy.NewIndex(fsBackend)
	goUpstream := goproxy.NewUpstream(goproxy.WithUpstreamURL(cfg.UpstreamGoProxy))
	goHandler := goproxy.NewHandler(
		goIndex,
		cafsStore,
		goproxy.WithUpstream(goUpstream),
		goproxy.WithLogger(cfg.Logger.With("component", "goproxy")),
	)

	// Initialize npm components
	npmIndex := npm.NewIndex(fsBackend)
	npmUpstreamOpts := []npm.UpstreamOption{}
	if cfg.UpstreamNPMRegistry != "" {
		npmUpstreamOpts = append(npmUpstreamOpts, npm.WithRegistryURL(cfg.UpstreamNPMRegistry))
	}
	npmUpstream := npm.NewUpstream(npmUpstreamOpts...)
	npmHandler := npm.NewHandler(
		npmIndex,
		cafsStore,
		npm.WithUpstream(npmUpstream),
		npm.WithLogger(cfg.Logger.With("component", "npm")),
	)

	// Initialize OCI components
	ociIndex := oci.NewIndex(fsBackend)
	ociUpstreamOpts := []oci.UpstreamOption{}
	if cfg.UpstreamOCIRegistry != "" {
		ociUpstreamOpts = append(ociUpstreamOpts, oci.WithRegistryURL(cfg.UpstreamOCIRegistry))
	}
	if cfg.OCIUsername != "" && cfg.OCIPassword != "" {
		ociUpstreamOpts = append(ociUpstreamOpts, oci.WithBasicAuth(cfg.OCIUsername, cfg.OCIPassword))
	}
	ociUpstream := oci.NewUpstream(ociUpstreamOpts...)
	ociHandlerOpts := []oci.HandlerOption{
		oci.WithUpstream(ociUpstream),
		oci.WithLogger(cfg.Logger.With("component", "oci")),
	}
	if cfg.OCITagTTL > 0 {
		ociHandlerOpts = append(ociHandlerOpts, oci.WithTagTTL(cfg.OCITagTTL))
	}
	ociHandler := oci.NewHandler(ociIndex, cafsStore, ociHandlerOpts...)

	// Initialize PyPI components
	pypiIndex := pypi.NewIndex(fsBackend)
	pypiUpstreamOpts := []pypi.UpstreamOption{}
	if cfg.UpstreamPyPI != "" {
		pypiUpstreamOpts = append(pypiUpstreamOpts, pypi.WithSimpleURL(cfg.UpstreamPyPI))
	}
	pypiUpstream := pypi.NewUpstream(pypiUpstreamOpts...)
	pypiHandlerOpts := []pypi.HandlerOption{
		pypi.WithUpstream(pypiUpstream),
		pypi.WithLogger(cfg.Logger.With("component", "pypi")),
	}
	if cfg.PyPIMetadataTTL > 0 {
		pypiHandlerOpts = append(pypiHandlerOpts, pypi.WithMetadataTTL(cfg.PyPIMetadataTTL))
	}
	pypiHandler := pypi.NewHandler(pypiIndex, cafsStore, pypiHandlerOpts...)

	// Initialize Maven components
	mavenIndex := maven.NewIndex(fsBackend)
	mavenUpstreamOpts := []maven.UpstreamOption{}
	if cfg.UpstreamMaven != "" {
		mavenUpstreamOpts = append(mavenUpstreamOpts, maven.WithRepositoryURL(cfg.UpstreamMaven))
	}
	mavenUpstream := maven.NewUpstream(mavenUpstreamOpts...)
	mavenHandlerOpts := []maven.HandlerOption{
		maven.WithUpstream(mavenUpstream),
		maven.WithLogger(cfg.Logger.With("component", "maven")),
	}
	if cfg.MavenMetadataTTL > 0 {
		mavenHandlerOpts = append(mavenHandlerOpts, maven.WithMetadataTTL(cfg.MavenMetadataTTL))
	}
	mavenHandler := maven.NewHandler(mavenIndex, cafsStore, mavenHandlerOpts...)

	s := &Server{
		config:     cfg,
		logger:     cfg.Logger,
		backend:    fsBackend,
		store:      cafsStore,
		index:      goIndex,
		goproxy:    goHandler,
		npmIndex:   npmIndex,
		npm:        npmHandler,
		ociIndex:   ociIndex,
		oci:        ociHandler,
		pypiIndex:  pypiIndex,
		pypi:       pypiHandler,
		mavenIndex: mavenIndex,
		maven:      mavenHandler,
		metadata:   metadataStore,
		expiryMgr:  expiryMgr,
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

	// Cache stats
	mux.HandleFunc("GET /stats", s.handleStats)

	// Prometheus metrics endpoint (returns 404 if not enabled)
	mux.Handle("GET /metrics", telemetry.PrometheusHandler())

	// NPM registry endpoints
	// The npm handler handles all paths under /npm/
	mux.Handle("GET /npm/", http.StripPrefix("/npm", s.npm))
	mux.Handle("HEAD /npm/", http.StripPrefix("/npm", s.npm))

	// OCI registry endpoints
	// The OCI handler handles all paths under /v2/
	mux.Handle("GET /v2/", s.oci)
	mux.Handle("HEAD /v2/", s.oci)

	// PyPI Simple API endpoints
	// The pypi handler handles all paths under /pypi/
	mux.Handle("GET /pypi/", http.StripPrefix("/pypi", s.pypi))
	mux.Handle("HEAD /pypi/", http.StripPrefix("/pypi", s.pypi))

	// Maven repository endpoints
	// The maven handler handles all paths under /maven/
	mux.Handle("GET /maven/", http.StripPrefix("/maven", s.maven))
	mux.Handle("HEAD /maven/", http.StripPrefix("/maven", s.maven))

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
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleStats handles cache statistics requests.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if s.expiryMgr == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":"expiry not enabled"}`))
		return
	}

	stats, err := s.expiryMgr.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"total_blobs":%d,"total_size":%d,"oldest_access":"%s","newest_access":"%s"}`,
		stats.TotalBlobs,
		stats.TotalSize,
		stats.OldestBlob.Format(time.RFC3339),
		stats.NewestBlob.Format(time.RFC3339),
	)
}

// loggingMiddleware logs HTTP requests with structured fields for analysis.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}

		// Inject request tags so handlers can set cache_result, endpoint, etc.
		r = telemetry.InjectTags(r)
		tags := telemetry.GetTags(r)

		// Derive protocol from path (simple prefix matching)
		protocol := deriveProtocol(r.URL.Path)

		// Wrap response writer to capture status and bytes
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)

		// Build log attributes
		attrs := []any{
			// Request identification
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,

			// Protocol classification (for filtering/grouping)
			"protocol", protocol,

			// Response details
			"status", wrapped.status,
			"status_class", telemetry.StatusClass(wrapped.status),
			"bytes_sent", wrapped.bytesWritten,

			// Timing
			"duration_ms", duration.Milliseconds(),
			"duration", duration.String(),

			// Client info
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
			"http_version", fmt.Sprintf("%d.%d", r.ProtoMajor, r.ProtoMinor),
		}

		// Add handler-set tags
		if tags.Endpoint != "" {
			attrs = append(attrs, "endpoint", tags.Endpoint)
		}
		if tags.CacheResult != "" {
			attrs = append(attrs, "cache_result", string(tags.CacheResult))
		}

		// Add content type if present
		if ct := wrapped.Header().Get("Content-Type"); ct != "" {
			attrs = append(attrs, "content_type", ct)
		}

		s.logger.Info("http request", attrs...)

		// Record OTel metrics
		telemetry.RecordHTTP(r.Context(), r, protocol, wrapped.status, wrapped.bytesWritten, duration)
	})
}

// Start starts the server.
func (s *Server) Start() error {
	// Start expiry manager if configured
	if s.expiryMgr != nil {
		s.logger.Info("starting expiry manager",
			"ttl", s.config.CacheTTL,
			"max_size", s.config.CacheMaxSize,
			"check_interval", s.config.ExpiryCheckInterval,
		)
		if err := s.expiryMgr.Start(context.Background()); err != nil {
			return fmt.Errorf("starting expiry manager: %w", err)
		}
	}

	s.logger.Info("starting server", "address", s.config.Address)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down server")

	// Stop expiry manager
	if s.expiryMgr != nil {
		s.expiryMgr.Stop()
	}

	return s.httpServer.Shutdown(ctx)
}

// Address returns the server's listen address.
func (s *Server) Address() string {
	return s.config.Address
}

// responseWriter wraps http.ResponseWriter to capture the status code and bytes written.
// It preserves http.Flusher and http.Hijacker interfaces for streaming support.
type responseWriter struct {
	http.ResponseWriter
	status       int
	bytesWritten int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// Flush implements http.Flusher for streaming responses.
func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker for connection upgrades.
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijacking not supported")
}

// Unwrap returns the underlying ResponseWriter.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// deriveProtocol extracts the protocol name from the request path.
func deriveProtocol(path string) string {
	switch {
	case path == "/health" || path == "/stats" || path == "/metrics":
		return "internal"
	case len(path) >= 5 && path[:5] == "/npm/":
		return "npm"
	case len(path) >= 6 && path[:6] == "/pypi/":
		return "pypi"
	case len(path) >= 7 && path[:7] == "/maven/":
		return "maven"
	case len(path) >= 9 && path[:9] == "/goproxy/":
		return "goproxy"
	case len(path) >= 3 && path[:3] == "/v2":
		return "oci"
	default:
		// Root paths are goproxy (GOPROXY=http://localhost:8080)
		if len(path) > 1 && path[0] == '/' {
			return "goproxy"
		}
		return "unknown"
	}
}
