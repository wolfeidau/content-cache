// Command content-cache is a content-addressable cache server for Go modules and NPM packages.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wolfeidau/content-cache/server"
	"github.com/wolfeidau/content-cache/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Parse flags
	var (
		address          = flag.String("address", ":8080", "Address to listen on")
		storage          = flag.String("storage", "./cache", "Storage directory path")
		goUpstream       = flag.String("go-upstream", "", "Upstream Go module proxy URL (default: proxy.golang.org)")
		npmUpstream      = flag.String("npm-upstream", "", "Upstream NPM registry URL (default: registry.npmjs.org)")
		ociUpstream      = flag.String("oci-upstream", "", "Upstream OCI registry URL (default: registry-1.docker.io)")
		ociUsername      = flag.String("oci-username", "", "OCI registry username for authentication")
		ociPassword      = flag.String("oci-password", "", "OCI registry password for authentication")
		ociTagTTL        = flag.Duration("oci-tag-ttl", 5*time.Minute, "TTL for OCI tag->digest cache mappings")
		pypiUpstream     = flag.String("pypi-upstream", "", "Upstream PyPI Simple API URL (default: pypi.org/simple/)")
		pypiMetadataTTL  = flag.Duration("pypi-metadata-ttl", 5*time.Minute, "TTL for PyPI project metadata cache")
		mavenUpstream    = flag.String("maven-upstream", "", "Upstream Maven repository URL (default: repo.maven.apache.org/maven2)")
		mavenMetadataTTL = flag.Duration("maven-metadata-ttl", 5*time.Minute, "TTL for maven-metadata.xml cache")
		cacheTTL         = flag.Duration("cache-ttl", 7*24*time.Hour, "Cache TTL (e.g., 168h for 7 days, 0 to disable)")
		cacheMaxSize     = flag.Int64("cache-max-size", 10*1024*1024*1024, "Maximum cache size in bytes (default: 10GB, 0 to disable)")
		expiryCheck      = flag.Duration("expiry-check-interval", time.Hour, "How often to check for expired content")
		logLevel         = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		logFormat        = flag.String("log-format", "text", "Log format (text, json)")

		// Metrics flags
		metricsOTLP       = flag.String("metrics-otlp-endpoint", "", "OTLP gRPC endpoint for metrics (e.g., localhost:4317)")
		metricsPrometheus = flag.Bool("metrics-prometheus", false, "Enable Prometheus /metrics endpoint")
		metricsInterval   = flag.Duration("metrics-interval", 10*time.Second, "Metrics export interval")
	)
	flag.Parse()

	// Setup logger
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return fmt.Errorf("invalid log level: %s", *logLevel)
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	switch *logFormat {
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	default:
		return fmt.Errorf("invalid log format: %s", *logFormat)
	}
	logger := slog.New(handler)

	// Initialize metrics
	metricsCfg := telemetry.MetricsConfig{
		ServiceName:      "content-cache",
		ServiceVersion:   "0.1.0",
		OTLPEndpoint:     *metricsOTLP,
		EnablePrometheus: *metricsPrometheus,
		FlushInterval:    *metricsInterval,
	}
	metricsShutdown, err := telemetry.InitMetrics(context.Background(), metricsCfg)
	if err != nil {
		return fmt.Errorf("initializing metrics: %w", err)
	}
	if *metricsOTLP != "" {
		logger.Info("metrics OTLP export enabled", "endpoint", *metricsOTLP)
	}
	if *metricsPrometheus {
		logger.Info("metrics Prometheus endpoint enabled", "path", "/metrics")
	}

	// Create server
	cfg := server.Config{
		Address:             *address,
		StoragePath:         *storage,
		UpstreamGoProxy:     *goUpstream,
		UpstreamNPMRegistry: *npmUpstream,
		UpstreamOCIRegistry: *ociUpstream,
		OCIUsername:         *ociUsername,
		OCIPassword:         *ociPassword,
		OCITagTTL:           *ociTagTTL,
		UpstreamPyPI:        *pypiUpstream,
		PyPIMetadataTTL:     *pypiMetadataTTL,
		UpstreamMaven:       *mavenUpstream,
		MavenMetadataTTL:    *mavenMetadataTTL,
		CacheTTL:            *cacheTTL,
		CacheMaxSize:        *cacheMaxSize,
		ExpiryCheckInterval: *expiryCheck,
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
