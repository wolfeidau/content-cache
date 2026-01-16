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
		address      = flag.String("address", ":8080", "Address to listen on")
		storage      = flag.String("storage", "./cache", "Storage directory path")
		goUpstream   = flag.String("go-upstream", "", "Upstream Go module proxy URL (default: proxy.golang.org)")
		npmUpstream  = flag.String("npm-upstream", "", "Upstream NPM registry URL (default: registry.npmjs.org)")
		ociUpstream  = flag.String("oci-upstream", "", "Upstream OCI registry URL (default: registry-1.docker.io)")
		ociUsername  = flag.String("oci-username", "", "OCI registry username for authentication")
		ociPassword  = flag.String("oci-password", "", "OCI registry password for authentication")
		ociTagTTL    = flag.Duration("oci-tag-ttl", 5*time.Minute, "TTL for OCI tag->digest cache mappings")
		cacheTTL     = flag.Duration("cache-ttl", 7*24*time.Hour, "Cache TTL (e.g., 168h for 7 days, 0 to disable)")
		cacheMaxSize = flag.Int64("cache-max-size", 10*1024*1024*1024, "Maximum cache size in bytes (default: 10GB, 0 to disable)")
		expiryCheck  = flag.Duration("expiry-check-interval", time.Hour, "How often to check for expired content")
		logLevel     = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		logFormat    = flag.String("log-format", "text", "Log format (text, json)")
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

	// Print usage info
	logger.Info("server started",
		"address", srv.Address(),
		"goproxy_url", fmt.Sprintf("http://localhost%s/goproxy", srv.Address()),
		"npm_url", fmt.Sprintf("http://localhost%s/npm", srv.Address()),
		"oci_url", fmt.Sprintf("http://localhost%s/v2", srv.Address()),
	)
	fmt.Println()
	fmt.Println("To use as a Go module proxy:")
	fmt.Printf("  export GOPROXY=http://localhost%s/goproxy,direct\n", srv.Address())
	fmt.Println()
	fmt.Println("To use as an NPM registry:")
	fmt.Printf("  npm config set registry http://localhost%s/npm/\n", srv.Address())
	fmt.Println()
	fmt.Println("To use as an OCI registry mirror:")
	fmt.Printf("  docker pull localhost%s/library/alpine:latest\n", srv.Address())
	fmt.Println()

	// Wait for shutdown or error
	select {
	case <-ctx.Done():
		// Graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
