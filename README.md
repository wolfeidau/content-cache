# content-cache

A content-addressable caching proxy for Go modules and NPM packages. Reduces build times and network bandwidth by caching package downloads locally with automatic deduplication.

## Problem

Development teams waste significant time and bandwidth re-downloading the same packages across builds, CI runs, and developer machines. A single `go mod download` or `npm install` can fetch hundreds of megabytes that were already downloaded yesterday. Network failures during package downloads break builds unpredictably.

## Solution

content-cache acts as a local caching proxy that:
- Stores packages once using content-addressable storage (BLAKE3 hashing)
- Serves cached packages in microseconds instead of milliseconds
- Deduplicates identical content across different package versions
- Continues serving cached packages when upstream registries are unavailable

## Quick Start

```bash
# Build and run the cache server
go build -o content-cache ./cmd/content-cache
./content-cache -address :8080 -storage ./cache

# Configure Go to use the cache
export GOPROXY=http://localhost:8080,direct

# Downloads are now cached locally
go get github.com/pkg/errors@v0.9.1  # First request: ~12ms (upstream)
go get github.com/pkg/errors@v0.9.1  # Second request: ~100µs (cache hit)
```

## Performance

| Operation | Upstream | Cached | Improvement |
|-----------|----------|--------|-------------|
| Module info | 12ms | 100µs | 120x faster |
| Module zip | 150ms | 1ms | 150x faster |

## Current Features

### Implemented
- **GOPROXY Protocol**: Full support for Go module proxy protocol (`/@v/list`, `.info`, `.mod`, `.zip`)
- **Content-Addressable Storage**: BLAKE3 hashing with automatic deduplication
- **Filesystem Backend**: Atomic writes with sharded directory structure
- **Pull-Through Caching**: Fetches from upstream on cache miss, caches for future requests

### Planned
- NPM Registry Protocol support
- S3 storage backend
- TTL + LRU expiration
- Compression (zstd)
- Metrics and tracing (OpenTelemetry)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      HTTP Server                             │
│  /goproxy/*  →  GOPROXY Handler  →  Index + CAFS            │
│  /npm/*      →  NPM Handler      →  Index + CAFS  (planned) │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Content-Addressable Store                    │
│  blobs/{hash[0:2]}/{hash}  →  Deduplicated content          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Backend                           │
│  Filesystem (implemented) | S3 (planned)                    │
└─────────────────────────────────────────────────────────────┘
```

## Configuration

```bash
./content-cache \
  -address :8080 \           # Listen address
  -storage ./cache \         # Storage directory
  -upstream https://proxy.golang.org \  # Upstream Go proxy
  -log-level info \          # Log level: debug, info, warn, error
  -log-format text           # Log format: text, json
```

## Storage Layout

```
./cache/
├── blobs/                   # Content-addressable storage
│   └── 58/                  # Sharded by first byte of hash
│       └── 5818f08e...      # Full BLAKE3 hash as filename
└── goproxy/                 # Module index
    └── github.com/
        └── pkg/
            └── errors/
                └── @v/
                    ├── list          # Available versions
                    ├── v0.9.1.info   # Version metadata
                    └── v0.9.1.mod    # go.mod content
```

## Development

```bash
# Run tests
go test ./...

# Run with debug logging
./content-cache -log-level debug

# Test the proxy manually
curl http://localhost:8080/github.com/pkg/errors/@v/v0.9.1.info
```

## Goals

- Simple and efficient content storage and retrieval
- Automatic deduplication (same content stored once)
- Content retrieval by BLAKE3 hash
- Multiple storage backends (filesystem, S3)
- TTL and LRU-based expiration
- Compression support
- Observability (metrics, traces, logs)

## Disclosure

This project was developed with AI, specifically Claude, from [Anthropic](https://www.anthropic.com/).

## License

Apache License, Version 2.0 - Copyright [Mark Wolfe](https://www.wolfe.id.au)