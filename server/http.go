// Package server provides the HTTP server for the content cache.
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/download"
	"github.com/wolfeidau/content-cache/protocol/goproxy"
	"github.com/wolfeidau/content-cache/protocol/maven"
	"github.com/wolfeidau/content-cache/protocol/npm"
	"github.com/wolfeidau/content-cache/protocol/oci"
	"github.com/wolfeidau/content-cache/protocol/pypi"
	"github.com/wolfeidau/content-cache/protocol/rubygems"
	"github.com/wolfeidau/content-cache/store"
	"github.com/wolfeidau/content-cache/store/gc"
	"github.com/wolfeidau/content-cache/store/metadb"
	"github.com/wolfeidau/content-cache/telemetry"
	"go.opentelemetry.io/otel"
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

	// UpstreamRubyGems is the upstream RubyGems registry URL
	UpstreamRubyGems string

	// RubyGemsMetadataTTL is how long to cache versions/info/specs files.
	// Default: 5 minutes (new versions may be published)
	RubyGemsMetadataTTL time.Duration

	// SumDBName is the name of the checksum database to proxy.
	// Default: sum.golang.org
	SumDBName string

	// UpstreamSumDB is the upstream sumdb URL.
	// Default: https://sum.golang.org
	UpstreamSumDB string

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

	// GCInterval is how often to run garbage collection.
	GCInterval time.Duration

	// GCStartupDelay is the delay before first GC run.
	GCStartupDelay time.Duration

	// Logger for the server
	Logger *slog.Logger
}

// Server is the HTTP server for the content cache.
type Server struct {
	config     Config
	httpServer *http.Server
	logger     *slog.Logger

	// Components
	backend       backend.Backend
	store         store.Store
	index         *goproxy.Index
	goproxy       *goproxy.Handler
	npmIndex      *npm.Index
	npm           *npm.Handler
	ociIndex      *oci.Index
	oci           *oci.Handler
	pypiIndex     *pypi.Index
	pypi          *pypi.Handler
	mavenIndex    *maven.Index
	maven         *maven.Handler
	rubygemsIndex *rubygems.Index
	rubygems      *rubygems.Handler
	sumdbIndex    *goproxy.SumdbIndex
	sumdb         *goproxy.SumdbHandler
	metaDB        metadb.MetaDB
	gcManager     *gc.Manager
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

	// Initialize metadata database
	metaDB := metadb.New()
	if err := metaDB.Open(path.Join(cfg.StoragePath, "metadata.db")); err != nil {
		return nil, fmt.Errorf("opening metadata database: %w", err)
	}

	// Initialize GC manager
	var gcManager *gc.Manager
	if cfg.CacheMaxSize > 0 {
		gcInterval := cfg.GCInterval
		if gcInterval == 0 {
			gcInterval = 1 * time.Hour
		}
		gcStartupDelay := cfg.GCStartupDelay
		if gcStartupDelay == 0 {
			gcStartupDelay = 5 * time.Minute
		}
		gcMetrics, err := gc.NewMetrics(otel.Meter("github.com/wolfeidau/content-cache"))
		if err != nil {
			return nil, fmt.Errorf("creating gc metrics: %w", err)
		}
		gcConfig := gc.Config{
			Interval:      gcInterval,
			StartupDelay:  gcStartupDelay,
			MaxCacheBytes: cfg.CacheMaxSize,
			BatchSize:     1000,
		}
		gcManager = gc.New(metaDB, fsBackend, gcConfig, gcMetrics, cfg.Logger.With("component", "gc"))
	}

	// Initialize CAFS store with MetaDB tracking
	cafsStore := store.NewCAFS(fsBackend, store.WithMetaDB(metaDB))

	// Get BoltDB for EnvelopeIndex creation (used by all protocols)
	boltDB, ok := metaDB.(*metadb.BoltDB)
	if !ok {
		return nil, fmt.Errorf("metaDB must be *metadb.BoltDB for envelope storage")
	}

	// Create shared downloader for singleflight deduplication
	dl := download.New(download.WithLogger(cfg.Logger.With("component", "download")))

	// Initialize goproxy components using metadb EnvelopeIndex
	goproxyModIndex, err := metadb.NewEnvelopeIndex(boltDB, "goproxy", "mod", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating goproxy mod index: %w", err)
	}
	goproxyInfoIndex, err := metadb.NewEnvelopeIndex(boltDB, "goproxy", "info", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating goproxy info index: %w", err)
	}
	goproxyListIndex, err := metadb.NewEnvelopeIndex(boltDB, "goproxy", "list", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating goproxy list index: %w", err)
	}
	goIndex := goproxy.NewIndex(goproxyModIndex, goproxyInfoIndex, goproxyListIndex)
	goUpstream := goproxy.NewUpstream(goproxy.WithUpstreamURL(cfg.UpstreamGoProxy))
	goHandler := goproxy.NewHandler(
		goIndex,
		cafsStore,
		goproxy.WithUpstream(goUpstream),
		goproxy.WithLogger(cfg.Logger.With("component", "goproxy")),
		goproxy.WithDownloader(dl),
	)

	// Initialize npm components using metadb EnvelopeIndex
	npmMetadataIndex, err := metadb.NewEnvelopeIndex(boltDB, "npm", "metadata", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating npm metadata index: %w", err)
	}
	npmCacheIndex, err := metadb.NewEnvelopeIndex(boltDB, "npm", "cache", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating npm cache index: %w", err)
	}
	npmIndex := npm.NewIndex(npmMetadataIndex, npmCacheIndex)
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
		npm.WithDownloader(dl),
	)

	// Initialize OCI components using metadb EnvelopeIndex
	ociImageIndex, err := metadb.NewEnvelopeIndex(boltDB, "oci", "image", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating oci image index: %w", err)
	}
	ociManifestIndex, err := metadb.NewEnvelopeIndex(boltDB, "oci", "manifest", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating oci manifest index: %w", err)
	}
	ociBlobIndex, err := metadb.NewEnvelopeIndex(boltDB, "oci", "blob", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating oci blob index: %w", err)
	}
	ociIndex := oci.NewIndex(ociImageIndex, ociManifestIndex, ociBlobIndex)
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
		oci.WithDownloader(dl),
	}
	if cfg.OCITagTTL > 0 {
		ociHandlerOpts = append(ociHandlerOpts, oci.WithTagTTL(cfg.OCITagTTL))
	}
	ociHandler := oci.NewHandler(ociIndex, cafsStore, ociHandlerOpts...)

	// Initialize PyPI components using metadb EnvelopeIndex
	pypiProjectTTL := cfg.PyPIMetadataTTL
	if pypiProjectTTL == 0 {
		pypiProjectTTL = 5 * time.Minute
	}
	pypiProjectIndex, err := metadb.NewEnvelopeIndex(boltDB, "pypi", "project", pypiProjectTTL)
	if err != nil {
		return nil, fmt.Errorf("creating pypi project index: %w", err)
	}
	pypiIndex := pypi.NewIndex(pypiProjectIndex)
	pypiUpstreamOpts := []pypi.UpstreamOption{}
	if cfg.UpstreamPyPI != "" {
		pypiUpstreamOpts = append(pypiUpstreamOpts, pypi.WithSimpleURL(cfg.UpstreamPyPI))
	}
	pypiUpstream := pypi.NewUpstream(pypiUpstreamOpts...)
	pypiHandlerOpts := []pypi.HandlerOption{
		pypi.WithUpstream(pypiUpstream),
		pypi.WithLogger(cfg.Logger.With("component", "pypi")),
		pypi.WithDownloader(dl),
	}
	if cfg.PyPIMetadataTTL > 0 {
		pypiHandlerOpts = append(pypiHandlerOpts, pypi.WithMetadataTTL(cfg.PyPIMetadataTTL))
	}
	pypiHandler := pypi.NewHandler(pypiIndex, cafsStore, pypiHandlerOpts...)

	// Initialize Maven components using metadb EnvelopeIndex
	mavenTTL := cfg.MavenMetadataTTL
	if mavenTTL == 0 {
		mavenTTL = 5 * time.Minute
	}
	mavenMetadataIndex, err := metadb.NewEnvelopeIndex(boltDB, "maven", "metadata", mavenTTL)
	if err != nil {
		return nil, fmt.Errorf("creating maven metadata index: %w", err)
	}
	mavenArtifactIndex, err := metadb.NewEnvelopeIndex(boltDB, "maven", "artifact", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating maven artifact index: %w", err)
	}
	mavenIndex := maven.NewIndex(mavenMetadataIndex, mavenArtifactIndex)
	mavenUpstreamOpts := []maven.UpstreamOption{}
	if cfg.UpstreamMaven != "" {
		mavenUpstreamOpts = append(mavenUpstreamOpts, maven.WithRepositoryURL(cfg.UpstreamMaven))
	}
	mavenUpstream := maven.NewUpstream(mavenUpstreamOpts...)
	mavenHandlerOpts := []maven.HandlerOption{
		maven.WithUpstream(mavenUpstream),
		maven.WithLogger(cfg.Logger.With("component", "maven")),
		maven.WithDownloader(dl),
	}
	if cfg.MavenMetadataTTL > 0 {
		mavenHandlerOpts = append(mavenHandlerOpts, maven.WithMetadataTTL(cfg.MavenMetadataTTL))
	}
	mavenHandler := maven.NewHandler(mavenIndex, cafsStore, mavenHandlerOpts...)

	// Initialize RubyGems components using metadb EnvelopeIndex
	rubygemsTTL := cfg.RubyGemsMetadataTTL
	if rubygemsTTL == 0 {
		rubygemsTTL = 5 * time.Minute
	}
	rubygemsVersionsIndex, err := metadb.NewEnvelopeIndex(boltDB, "rubygems", "versions", rubygemsTTL)
	if err != nil {
		return nil, fmt.Errorf("creating rubygems versions index: %w", err)
	}
	rubygemsInfoIndex, err := metadb.NewEnvelopeIndex(boltDB, "rubygems", "info", rubygemsTTL)
	if err != nil {
		return nil, fmt.Errorf("creating rubygems info index: %w", err)
	}
	rubygemsSpecsIndex, err := metadb.NewEnvelopeIndex(boltDB, "rubygems", "specs", rubygemsTTL)
	if err != nil {
		return nil, fmt.Errorf("creating rubygems specs index: %w", err)
	}
	rubygemsGemIndex, err := metadb.NewEnvelopeIndex(boltDB, "rubygems", "gem", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating rubygems gem index: %w", err)
	}
	rubygemsGemspecIndex, err := metadb.NewEnvelopeIndex(boltDB, "rubygems", "gemspec", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating rubygems gemspec index: %w", err)
	}
	rubygemsIndex := rubygems.NewIndex(
		rubygemsVersionsIndex,
		rubygemsInfoIndex,
		rubygemsSpecsIndex,
		rubygemsGemIndex,
		rubygemsGemspecIndex,
	)
	rubygemsUpstreamOpts := []rubygems.UpstreamOption{}
	if cfg.UpstreamRubyGems != "" {
		rubygemsUpstreamOpts = append(rubygemsUpstreamOpts, rubygems.WithRegistryURL(cfg.UpstreamRubyGems))
	}
	rubygemsUpstream := rubygems.NewUpstream(rubygemsUpstreamOpts...)
	rubygemsHandlerOpts := []rubygems.HandlerOption{
		rubygems.WithUpstream(rubygemsUpstream),
		rubygems.WithLogger(cfg.Logger.With("component", "rubygems")),
		rubygems.WithDownloader(dl),
	}
	if cfg.RubyGemsMetadataTTL > 0 {
		rubygemsHandlerOpts = append(rubygemsHandlerOpts, rubygems.WithMetadataTTL(cfg.RubyGemsMetadataTTL))
	}
	rubygemsHandler := rubygems.NewHandler(rubygemsIndex, cafsStore, rubygemsHandlerOpts...)

	// Initialize sumdb components using metadb EnvelopeIndex
	// Sumdb responses are immutable, so we use a long TTL (or no TTL)
	sumdbEnvelope, err := metadb.NewEnvelopeIndex(boltDB, "sumdb", "cache", 0)
	if err != nil {
		return nil, fmt.Errorf("creating sumdb cache index: %w", err)
	}
	sumdbIndex := goproxy.NewSumdbIndex(sumdbEnvelope)
	sumdbUpstreamOpts := []goproxy.SumdbUpstreamOption{}
	if cfg.UpstreamSumDB != "" {
		sumdbUpstreamOpts = append(sumdbUpstreamOpts, goproxy.WithSumdbUpstreamURL(cfg.UpstreamSumDB))
	}
	sumdbUpstream := goproxy.NewSumdbUpstream(sumdbUpstreamOpts...)
	sumdbHandlerOpts := []goproxy.SumdbHandlerOption{
		goproxy.WithSumdbUpstream(sumdbUpstream),
		goproxy.WithSumdbLogger(cfg.Logger.With("component", "sumdb")),
	}
	if cfg.SumDBName != "" {
		sumdbHandlerOpts = append(sumdbHandlerOpts, goproxy.WithSumdbName(cfg.SumDBName))
	}
	sumdbHandler := goproxy.NewSumdbHandler(sumdbIndex, cafsStore, sumdbHandlerOpts...)

	s := &Server{
		config:        cfg,
		logger:        cfg.Logger,
		backend:       fsBackend,
		store:         cafsStore,
		index:         goIndex,
		goproxy:       goHandler,
		npmIndex:      npmIndex,
		npm:           npmHandler,
		ociIndex:      ociIndex,
		oci:           ociHandler,
		pypiIndex:     pypiIndex,
		pypi:          pypiHandler,
		mavenIndex:    mavenIndex,
		maven:         mavenHandler,
		rubygemsIndex: rubygemsIndex,
		rubygems:      rubygemsHandler,
		sumdbIndex:    sumdbIndex,
		sumdb:         sumdbHandler,
		metaDB:        metaDB,
		gcManager:     gcManager,
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

	// Admin endpoints
	mux.HandleFunc("POST /admin/gc", s.handleGCTrigger)
	mux.HandleFunc("GET /admin/gc/status", s.handleGCStatus)

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

	// RubyGems registry endpoints
	// The rubygems handler handles all paths under /rubygems/
	mux.Handle("GET /rubygems/", http.StripPrefix("/rubygems", s.rubygems))
	mux.Handle("HEAD /rubygems/", http.StripPrefix("/rubygems", s.rubygems))

	// Sumdb proxy endpoints
	// Handle both root and prefixed paths for sumdb
	// Root: GOPROXY=http://localhost:8080 -> /sumdb/sum.golang.org/...
	// Prefixed: GOPROXY=http://localhost:8080/goproxy -> /goproxy/sumdb/sum.golang.org/...
	mux.Handle("GET /sumdb/", s.sumdb)
	mux.Handle("GET /goproxy/sumdb/", http.StripPrefix("/goproxy", s.sumdb))

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
	if s.metaDB == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":"metadb not enabled"}`))
		return
	}

	totalSize, err := s.metaDB.TotalBlobSize(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"total_size":%d}`, totalSize)
}

func (s *Server) handleGCTrigger(w http.ResponseWriter, r *http.Request) {
	if s.gcManager == nil {
		http.Error(w, "GC not enabled", http.StatusServiceUnavailable)
		return
	}
	result, err := s.gcManager.RunNow(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (s *Server) handleGCStatus(w http.ResponseWriter, r *http.Request) {
	if s.gcManager == nil {
		http.Error(w, "GC not enabled", http.StatusServiceUnavailable)
		return
	}
	status := s.gcManager.Status()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
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
	if s.gcManager != nil {
		s.logger.Info("starting GC manager",
			"interval", s.config.GCInterval,
			"max_size", s.config.CacheMaxSize,
		)
		s.gcManager.Start(context.Background())
	}

	s.logger.Info("starting server", "address", s.config.Address)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down server")

	// Stop GC manager
	if s.gcManager != nil {
		if err := s.gcManager.Stop(ctx); err != nil {
			s.logger.Error("GC manager shutdown error", "error", err)
		}
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
func deriveProtocol(p string) string {
	switch {
	case p == "/health" || p == "/stats" || p == "/metrics":
		return "internal"
	case strings.HasPrefix(p, "/admin/"):
		return "admin"
	case strings.HasPrefix(p, "/npm/"):
		return "npm"
	case strings.HasPrefix(p, "/pypi/"):
		return "pypi"
	case strings.HasPrefix(p, "/maven/"):
		return "maven"
	case strings.HasPrefix(p, "/rubygems/"):
		return "rubygems"
	case strings.HasPrefix(p, "/sumdb/"):
		return "sumdb"
	case strings.HasPrefix(p, "/goproxy/"):
		return "goproxy"
	case strings.HasPrefix(p, "/v2"):
		return "oci"
	default:
		return "unknown"
	}
}
