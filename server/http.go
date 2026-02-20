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
	"github.com/wolfeidau/content-cache/credentials"
	"github.com/wolfeidau/content-cache/download"
	"github.com/wolfeidau/content-cache/protocol/git"
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

	// AuthToken is the bearer token for inbound authentication.
	// When empty, authentication is disabled.
	AuthToken string

	// Credentials holds resolved upstream credentials from the credentials file.
	// When nil, all protocols use their default upstreams with no auth.
	Credentials *credentials.Credentials

	// UpstreamGoProxy is the upstream Go module proxy URL
	UpstreamGoProxy string

	// UpstreamNPMRegistry is the upstream NPM registry URL
	UpstreamNPMRegistry string

	// UpstreamOCIRegistry is the upstream OCI registry URL
	UpstreamOCIRegistry string

	// OCIPrefix is the routing prefix for the OCI registry.
	// Default: "docker-hub"
	OCIPrefix string

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

	// GitAllowedHosts is the allowlist of permitted upstream Git hosts.
	GitAllowedHosts []string

	// GitMaxRequestBodySize is the maximum upload-pack request body size in bytes.
	// Default: 100MB
	GitMaxRequestBodySize int64

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

	// TLSCertFile is the path to the TLS certificate file.
	// When both TLSCertFile and TLSKeyFile are set, the server starts with TLS.
	TLSCertFile string

	// TLSKeyFile is the path to the TLS private key file.
	TLSKeyFile string

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
	gitIndex      *git.Index
	git           *git.Handler
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
	instrumentedBackend := backend.NewInstrumentedBackend(fsBackend, "filesystem")

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
		gcManager = gc.New(metaDB, instrumentedBackend, gcConfig, gcMetrics, cfg.Logger.With("component", "gc"))
	}

	// Initialize CAFS store with MetaDB tracking
	cafsStore := store.NewCAFS(instrumentedBackend, store.WithMetaDB(metaDB))

	// Get BoltDB for EnvelopeIndex creation (used by all protocols)
	boltDB, ok := metaDB.(*metadb.BoltDB)
	if !ok {
		return nil, fmt.Errorf("metaDB must be *metadb.BoltDB for envelope storage")
	}

	// Create shared downloader for singleflight deduplication
	dl := download.New()

	// Create instrumented HTTP clients for upstream fetch metrics
	goHTTPClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: telemetry.NewInstrumentedTransport(nil, "goproxy"),
	}
	npmHTTPClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: telemetry.NewInstrumentedTransport(nil, "npm"),
	}
	ociHTTPClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: telemetry.NewInstrumentedTransport(nil, "oci"),
	}
	pypiHTTPClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: telemetry.NewInstrumentedTransport(nil, "pypi"),
	}
	mavenHTTPClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: telemetry.NewInstrumentedTransport(nil, "maven"),
	}
	rubygemsHTTPClient := &http.Client{
		Timeout:   5 * time.Minute,
		Transport: telemetry.NewInstrumentedTransport(nil, "rubygems"),
	}
	gitHTTPClient := &http.Client{
		Transport: telemetry.NewInstrumentedTransport(nil, "git"),
	}

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
	goUpstream := goproxy.NewUpstream(goproxy.WithUpstreamURL(cfg.UpstreamGoProxy), goproxy.WithHTTPClient(goHTTPClient))
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
	npmHandlerOpts := []npm.HandlerOption{
		npm.WithLogger(cfg.Logger.With("component", "npm")),
		npm.WithDownloader(dl),
	}
	if cfg.Credentials != nil && cfg.Credentials.NPM != nil && len(cfg.Credentials.NPM.Routes) > 0 {
		// Build NPM router from credentials file routes.
		npmRoutes := make([]npm.Route, 0, len(cfg.Credentials.NPM.Routes))
		for _, r := range cfg.Credentials.NPM.Routes {
			upOpts := []npm.UpstreamOption{npm.WithHTTPClient(npmHTTPClient)}
			if r.RegistryURL != "" {
				upOpts = append(upOpts, npm.WithRegistryURL(r.RegistryURL))
			}
			if r.Token != "" {
				upOpts = append(upOpts, npm.WithBearerToken(r.Token))
			}
			npmRoutes = append(npmRoutes, npm.Route{
				Match:    npm.RouteMatch{Scope: r.Match.Scope, Any: r.Match.Any},
				Upstream: npm.NewUpstream(upOpts...),
			})
		}
		npmRouter, err := npm.NewRouter(npmRoutes, npm.WithRouterLogger(cfg.Logger.With("component", "npm-router")))
		if err != nil {
			return nil, fmt.Errorf("creating npm router: %w", err)
		}
		npmHandlerOpts = append(npmHandlerOpts, npm.WithRouter(npmRouter))
		cfg.Logger.Info("npm routing configured", "routes", len(npmRoutes))
	} else {
		// Default single upstream, no auth.
		npmUpstreamOpts := []npm.UpstreamOption{npm.WithHTTPClient(npmHTTPClient)}
		if cfg.UpstreamNPMRegistry != "" {
			npmUpstreamOpts = append(npmUpstreamOpts, npm.WithRegistryURL(cfg.UpstreamNPMRegistry))
		}
		npmHandlerOpts = append(npmHandlerOpts, npm.WithUpstream(npm.NewUpstream(npmUpstreamOpts...)))
	}
	npmHandler := npm.NewHandler(npmIndex, cafsStore, npmHandlerOpts...)

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

	var ociRegistries []oci.Registry
	if cfg.Credentials != nil && cfg.Credentials.OCI != nil && len(cfg.Credentials.OCI.Registries) > 0 {
		// Build OCI registries from credentials file.
		if cfg.UpstreamOCIRegistry != "" || cfg.OCIPrefix != "" {
			cfg.Logger.Info("credentials file defines OCI registries, ignoring --oci-upstream and --oci-prefix CLI flags")
		}
		for _, reg := range cfg.Credentials.OCI.Registries {
			upOpts := []oci.UpstreamOption{oci.WithHTTPClient(ociHTTPClient)}
			if reg.Upstream != "" {
				upOpts = append(upOpts, oci.WithRegistryURL(reg.Upstream))
			}
			if reg.Username != "" && reg.Password != "" {
				upOpts = append(upOpts, oci.WithBasicAuth(reg.Username, reg.Password))
			}
			ociReg := oci.Registry{
				Prefix:   reg.Prefix,
				Upstream: oci.NewUpstream(upOpts...),
			}
			if reg.TagTTL != "" {
				if ttl, err := time.ParseDuration(reg.TagTTL); err == nil {
					ociReg.TagTTL = ttl
				} else {
					cfg.Logger.Warn("invalid tag_ttl in OCI registry config", "prefix", reg.Prefix, "tag_ttl", reg.TagTTL, "error", err)
				}
			}
			ociRegistries = append(ociRegistries, ociReg)
		}
		cfg.Logger.Info("oci routing configured from credentials file", "registries", len(ociRegistries))
	} else {
		// Default single registry from CLI flags, no auth.
		ociUpstreamOpts := []oci.UpstreamOption{oci.WithHTTPClient(ociHTTPClient)}
		if cfg.UpstreamOCIRegistry != "" {
			ociUpstreamOpts = append(ociUpstreamOpts, oci.WithRegistryURL(cfg.UpstreamOCIRegistry))
		}
		ociRegistries = []oci.Registry{
			{Prefix: cfg.OCIPrefix, Upstream: oci.NewUpstream(ociUpstreamOpts...)},
		}
	}

	ociRouter, err := oci.NewRouter(ociRegistries, oci.WithRouterLogger(cfg.Logger.With("component", "oci-router")))
	if err != nil {
		return nil, fmt.Errorf("creating oci router: %w", err)
	}

	ociHandlerOpts := []oci.HandlerOption{
		oci.WithRouter(ociRouter),
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
	pypiUpstreamOpts := []pypi.UpstreamOption{pypi.WithHTTPClient(pypiHTTPClient)}
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
	mavenUpstreamOpts := []maven.UpstreamOption{maven.WithHTTPClient(mavenHTTPClient)}
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
	rubygemsUpstreamOpts := []rubygems.UpstreamOption{rubygems.WithHTTPClient(rubygemsHTTPClient)}
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

	// Initialize Git proxy components using metadb EnvelopeIndex
	gitPackIndex, err := metadb.NewEnvelopeIndex(boltDB, "git", "pack", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("creating git pack index: %w", err)
	}
	gitIndex := git.NewIndex(gitPackIndex)
	gitHandlerOpts := []git.HandlerOption{
		git.WithLogger(cfg.Logger.With("component", "git")),
		git.WithDownloader(dl),
	}
	if cfg.Credentials != nil && cfg.Credentials.Git != nil && len(cfg.Credentials.Git.Routes) > 0 {
		// Build Git router from credentials file routes.
		gitRoutes := make([]git.Route, 0, len(cfg.Credentials.Git.Routes))
		for _, r := range cfg.Credentials.Git.Routes {
			upOpts := []git.UpstreamOption{
				git.WithUpstreamLogger(cfg.Logger.With("component", "git")),
				git.WithHTTPClient(gitHTTPClient),
			}
			if r.Username != "" {
				upOpts = append(upOpts, git.WithBasicAuth(r.Username, r.Password))
			}
			gitRoutes = append(gitRoutes, git.Route{
				Match:    git.RouteMatch{RepoPrefix: r.Match.RepoPrefix, Any: r.Match.Any},
				Upstream: git.NewUpstream(upOpts...),
			})
		}
		gitRouter, err := git.NewRouter(gitRoutes, git.WithRouterLogger(cfg.Logger.With("component", "git-router")))
		if err != nil {
			return nil, fmt.Errorf("creating git router: %w", err)
		}
		gitHandlerOpts = append(gitHandlerOpts, git.WithRouter(gitRouter))
		cfg.Logger.Info("git routing configured", "routes", len(gitRoutes))
	} else {
		gitUpstream := git.NewUpstream(git.WithUpstreamLogger(cfg.Logger.With("component", "git")), git.WithHTTPClient(gitHTTPClient))
		gitHandlerOpts = append(gitHandlerOpts, git.WithUpstream(gitUpstream))
	}
	if len(cfg.GitAllowedHosts) > 0 {
		gitHandlerOpts = append(gitHandlerOpts, git.WithAllowedHosts(cfg.GitAllowedHosts))
	} else {
		cfg.Logger.Warn("git proxy has no allowed hosts configured, all git requests will be rejected — set --git-allowed-hosts to enable")
	}
	if cfg.GitMaxRequestBodySize > 0 {
		gitHandlerOpts = append(gitHandlerOpts, git.WithMaxRequestBodySize(cfg.GitMaxRequestBodySize))
	}
	gitHandler := git.NewHandler(gitIndex, cafsStore, gitHandlerOpts...)

	// Initialize sumdb components using metadb EnvelopeIndex
	// Sumdb responses are immutable, so we use a long TTL (or no TTL)
	sumdbEnvelope, err := metadb.NewEnvelopeIndex(boltDB, "sumdb", "cache", 0)
	if err != nil {
		return nil, fmt.Errorf("creating sumdb cache index: %w", err)
	}
	sumdbIndex := goproxy.NewSumdbIndex(sumdbEnvelope)
	sumdbHTTPClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: telemetry.NewInstrumentedTransport(nil, "sumdb"),
	}
	sumdbUpstreamOpts := []goproxy.SumdbUpstreamOption{goproxy.WithSumdbHTTPClient(sumdbHTTPClient)}
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
		backend:       instrumentedBackend,
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
		gitIndex:      gitIndex,
		git:           gitHandler,
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
		Handler:      s.loggingMiddleware(s.authMiddleware(mux)),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute, // Long timeout for large zip downloads
		IdleTimeout:  60 * time.Second,
	}

	return s, nil
}

// withProtocol returns middleware that sets the protocol tag on the request.
func withProtocol(protocol string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		telemetry.SetProtocol(r, protocol)
		h.ServeHTTP(w, r)
	})
}

// registerRoutes sets up the HTTP routes.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Internal / admin endpoints (cache_result = "na")
	internalHealth := withProtocol("internal", http.HandlerFunc(s.handleHealth))
	internalStats := withProtocol("internal", http.HandlerFunc(s.handleStats))
	internalMetrics := withProtocol("internal", telemetry.PrometheusHandler())
	adminGCTrigger := withProtocol("internal", http.HandlerFunc(s.handleGCTrigger))
	adminGCStatus := withProtocol("internal", http.HandlerFunc(s.handleGCStatus))

	mux.Handle("GET /health", internalHealth)
	mux.Handle("GET /stats", internalStats)
	mux.Handle("GET /metrics", internalMetrics)
	mux.Handle("POST /admin/gc", adminGCTrigger)
	mux.Handle("GET /admin/gc/status", adminGCStatus)

	// NPM registry endpoints
	npmHandler := withProtocol("npm", http.StripPrefix("/npm", s.npm))
	mux.Handle("GET /npm/", npmHandler)
	mux.Handle("HEAD /npm/", npmHandler)

	// OCI registry endpoints
	ociHandler := withProtocol("oci", s.oci)
	mux.Handle("GET /v2/", ociHandler)
	mux.Handle("HEAD /v2/", ociHandler)

	// PyPI Simple API endpoints
	pypiHandler := withProtocol("pypi", http.StripPrefix("/pypi", s.pypi))
	mux.Handle("GET /pypi/", pypiHandler)
	mux.Handle("HEAD /pypi/", pypiHandler)

	// Maven repository endpoints
	mavenHandler := withProtocol("maven", http.StripPrefix("/maven", s.maven))
	mux.Handle("GET /maven/", mavenHandler)
	mux.Handle("HEAD /maven/", mavenHandler)

	// RubyGems registry endpoints
	rubygemsHandler := withProtocol("rubygems", http.StripPrefix("/rubygems", s.rubygems))
	mux.Handle("GET /rubygems/", rubygemsHandler)
	mux.Handle("HEAD /rubygems/", rubygemsHandler)

	// Git proxy endpoints
	gitHandler := withProtocol("git", http.StripPrefix("/git", s.git))
	mux.Handle("GET /git/", gitHandler)
	mux.Handle("POST /git/", gitHandler)

	// Sumdb proxy endpoints
	// Handle both root and prefixed paths for sumdb
	sumdbHandler := withProtocol("sumdb", s.sumdb)
	mux.Handle("GET /sumdb/", sumdbHandler)
	mux.Handle("GET /goproxy/sumdb/", withProtocol("sumdb", http.StripPrefix("/goproxy", s.sumdb)))

	// GOPROXY endpoints
	goproxyHandler := withProtocol("goproxy", http.StripPrefix("/goproxy", s.goproxy))
	mux.Handle("GET /goproxy/", goproxyHandler)

	// Also support serving at root for direct GOPROXY usage
	// This allows: GOPROXY=http://localhost:8080
	mux.Handle("GET /{module...}", withProtocol("goproxy", s.goproxy))
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	telemetry.SetCacheResult(r, telemetry.CacheNA)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleStats handles cache statistics requests.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	telemetry.SetCacheResult(r, telemetry.CacheNA)
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
	telemetry.SetCacheResult(r, telemetry.CacheNA)
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
	telemetry.SetCacheResult(r, telemetry.CacheNA)
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

		// Wrap response writer to capture status and bytes
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)

		// Resolve protocol: prefer tag set by WithProtocol middleware, fall back to path derivation
		protocol := tags.Protocol
		if protocol == "" {
			protocol = deriveProtocol(r.URL.Path)
		}

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
		telemetry.RecordHTTP(r.Context(), r, wrapped.status, wrapped.bytesWritten, duration)
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

	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		s.logger.Info("starting server with TLS", "address", s.config.Address)
		return s.httpServer.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile)
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
	case strings.HasPrefix(p, "/git/"):
		return "git"
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
