// Command content-cache is a content-addressable cache server for Go modules and NPM packages.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/lmittmann/tint"
	"github.com/wolfeidau/content-cache/server"
	"github.com/wolfeidau/content-cache/telemetry"
)

var version = "dev"

type CLI struct {
	Version kong.VersionFlag `kong:"short='v',help='Print version and exit'"`

	Serve ServeCmd `kong:"cmd,default='1',help='Start the cache server'"`
}

type ServeCmd struct {
	ListenAddress string `kong:"name='listen',default=':8080',env='LISTEN_ADDRESS',help='Address to listen on',group='Server'"`
	Storage       string `kong:"name='storage',default='./cache',env='CACHE_STORAGE',help='Storage directory path',group='Server'"`

	GoUpstream       string `kong:"name='go-upstream',env='GO_UPSTREAM',help='Upstream Go module proxy URL (default: proxy.golang.org)',group='Upstream'"`
	NPMUpstream      string `kong:"name='npm-upstream',env='NPM_UPSTREAM',help='Upstream NPM registry URL (default: registry.npmjs.org)',group='Upstream'"`
	OCIUpstream      string `kong:"name='oci-upstream',env='OCI_UPSTREAM',help='Upstream OCI registry URL (default: registry-1.docker.io)',group='Upstream'"`
	PyPIUpstream     string `kong:"name='pypi-upstream',env='PYPI_UPSTREAM',help='Upstream PyPI Simple API URL (default: pypi.org/simple/)',group='Upstream'"`
	MavenUpstream    string `kong:"name='maven-upstream',env='MAVEN_UPSTREAM',help='Upstream Maven repository URL (default: repo.maven.apache.org/maven2)',group='Upstream'"`
	RubyGemsUpstream string `kong:"name='rubygems-upstream',env='RUBYGEMS_UPSTREAM',help='Upstream RubyGems registry URL (default: rubygems.org)',group='Upstream'"`

	OCIUsername     string        `kong:"name='oci-username',env='OCI_USERNAME',help='OCI registry username for authentication',group='OCI'"`
	OCIPassword     string        `kong:"name='oci-password',env='OCI_PASSWORD',help='OCI registry password for authentication',group='OCI'"`
	OCIPasswordFile string        `kong:"name='oci-password-file',env='OCI_PASSWORD_FILE',type='existingfile',help='Path to file containing OCI registry password',group='OCI'"`
	OCITagTTL       time.Duration `kong:"name='oci-tag-ttl',default='5m',env='OCI_TAG_TTL',help='TTL for OCI tag->digest cache mappings',group='OCI'"`

	PyPIMetadataTTL     time.Duration `kong:"name='pypi-metadata-ttl',default='5m',env='PYPI_METADATA_TTL',help='TTL for PyPI project metadata cache',group='TTL'"`
	MavenMetadataTTL    time.Duration `kong:"name='maven-metadata-ttl',default='5m',env='MAVEN_METADATA_TTL',help='TTL for maven-metadata.xml cache',group='TTL'"`
	RubyGemsMetadataTTL time.Duration `kong:"name='rubygems-metadata-ttl',default='5m',env='RUBYGEMS_METADATA_TTL',help='TTL for RubyGems metadata cache',group='TTL'"`

	CacheTTL            time.Duration `kong:"name='cache-ttl',default='168h',env='CACHE_TTL',help='Cache TTL (e.g., 168h for 7 days, 0 to disable)',group='Cache'"`
	CacheMaxSize        int64         `kong:"name='cache-max-size',default='10737418240',env='CACHE_MAX_SIZE',help='Maximum cache size in bytes (default: 10GB, 0 to disable)',group='Cache'"`
	ExpiryCheckInterval time.Duration `kong:"name='expiry-check-interval',default='1h',env='EXPIRY_CHECK_INTERVAL',help='How often to check for expired content',group='Cache'"`
	GCInterval          time.Duration `kong:"name='gc-interval',default='1h',env='GC_INTERVAL',help='How often to run garbage collection',group='Cache'"`
	GCStartupDelay      time.Duration `kong:"name='gc-startup-delay',default='5m',env='GC_STARTUP_DELAY',help='Delay before first GC run after startup',group='Cache'"`

	LogLevel  string `kong:"name='log-level',default='info',env='LOG_LEVEL',enum='debug,info,warn,error',help='Log level',group='Logging'"`
	LogFormat string `kong:"name='log-format',default='text',env='LOG_FORMAT',enum='text,json',help='Log format',group='Logging'"`

	MetricsOTLPEndpoint string        `kong:"name='metrics-otlp-endpoint',env='METRICS_OTLP_ENDPOINT',help='OTLP gRPC endpoint for metrics (e.g., localhost:4317)',group='Metrics'"`
	MetricsPrometheus   bool          `kong:"name='metrics-prometheus',env='METRICS_PROMETHEUS',help='Enable Prometheus /metrics endpoint',group='Metrics'"`
	MetricsInterval     time.Duration `kong:"name='metrics-interval',default='10s',env='METRICS_INTERVAL',help='Metrics export interval',group='Metrics'"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("content-cache"),
		kong.Description("A content-addressable cache server for Go modules, NPM packages, OCI images, PyPI, Maven, and RubyGems."),
		kong.Vars{"version": version},
		kong.UsageOnError(),
	)

	switch ctx.Command() {
	case "serve":
		return cli.Serve.Run()
	default:
		return fmt.Errorf("unknown command: %s", ctx.Command())
	}
}

func (cmd *ServeCmd) Run() error {
	// Resolve OCI password from file if specified
	ociPassword := cmd.OCIPassword
	if cmd.OCIPasswordFile != "" {
		data, err := os.ReadFile(cmd.OCIPasswordFile)
		if err != nil {
			return fmt.Errorf("reading OCI password file: %w", err)
		}
		ociPassword = strings.TrimSpace(string(data))
	}

	// Setup logger
	var level slog.Level
	switch cmd.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	switch cmd.LogFormat {
	case "text":
		handler = tint.NewHandler(os.Stderr, &tint.Options{
			Level: level,
		})
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(handler)

	// Initialize metrics
	metricsCfg := telemetry.MetricsConfig{
		ServiceName:      "content-cache",
		ServiceVersion:   "0.1.0",
		OTLPEndpoint:     cmd.MetricsOTLPEndpoint,
		EnablePrometheus: cmd.MetricsPrometheus,
		FlushInterval:    cmd.MetricsInterval,
	}
	metricsShutdown, err := telemetry.InitMetrics(context.Background(), metricsCfg)
	if err != nil {
		return fmt.Errorf("initializing metrics: %w", err)
	}
	if cmd.MetricsOTLPEndpoint != "" {
		logger.Info("metrics OTLP export enabled", "endpoint", cmd.MetricsOTLPEndpoint)
	}
	if cmd.MetricsPrometheus {
		logger.Info("metrics Prometheus endpoint enabled", "path", "/metrics")
	}

	// Create server
	cfg := server.Config{
		Address:             cmd.ListenAddress,
		StoragePath:         cmd.Storage,
		UpstreamGoProxy:     cmd.GoUpstream,
		UpstreamNPMRegistry: cmd.NPMUpstream,
		UpstreamOCIRegistry: cmd.OCIUpstream,
		OCIUsername:         cmd.OCIUsername,
		OCIPassword:         ociPassword,
		OCITagTTL:           cmd.OCITagTTL,
		UpstreamPyPI:        cmd.PyPIUpstream,
		PyPIMetadataTTL:     cmd.PyPIMetadataTTL,
		UpstreamMaven:       cmd.MavenUpstream,
		MavenMetadataTTL:    cmd.MavenMetadataTTL,
		UpstreamRubyGems:    cmd.RubyGemsUpstream,
		RubyGemsMetadataTTL: cmd.RubyGemsMetadataTTL,
		CacheTTL:            cmd.CacheTTL,
		CacheMaxSize:        cmd.CacheMaxSize,
		ExpiryCheckInterval: cmd.ExpiryCheckInterval,
		GCInterval:          cmd.GCInterval,
		GCStartupDelay:      cmd.GCStartupDelay,
		Logger:              logger,
	}

	srv, err := server.New(cfg)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errCh <- err
		}
	}()

	logger.Info("server started", "address", srv.Address())

	// Wait for shutdown or error
	select {
	case <-ctx.Done():
		// Graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		// Shutdown server first
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}

		// Flush and shutdown metrics (use shorter timeout for CI environments)
		metricsCtx, metricsCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer metricsCancel()
		if err := metricsShutdown(metricsCtx); err != nil {
			logger.Error("metrics shutdown error", "error", err)
		}
		logger.Info("metrics flushed")

		return nil
	case err := <-errCh:
		// Server error - still try to flush metrics
		metricsCtx, metricsCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer metricsCancel()
		_ = metricsShutdown(metricsCtx)
		return err
	}
}
