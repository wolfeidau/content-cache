# content-cache Development Guide

## Quick Commands

```bash
# Build
go build -o content-cache ./cmd/content-cache

# Run tests
go test ./...

# Run tests with coverage
go test -coverprofile coverage.out -coverpkg=./... ./...

# Lint (matches CI)
golangci-lint run --verbose --timeout 3m

# Format code (auto-fix imports and formatting)
golangci-lint run --fix

# Run server locally
./content-cache -address :8080 -storage ./cache -log-level debug
```

## Project Structure

```
cmd/content-cache/     # Main entrypoint
server/                # HTTP server, routing, middleware
protocol/              # Protocol handlers (one package per protocol)
  ├── goproxy/         # Go module proxy (GOPROXY)
  ├── npm/             # NPM registry
  ├── oci/             # OCI/Docker registry
  ├── pypi/            # Python Package Index
  ├── maven/           # Maven repository
  └── git/             # Git Smart HTTP proxy
download/              # Singleflight-based download deduplication
store/                 # Content-addressable storage (CAFS)
backend/               # Storage backends (filesystem, future: S3)
expiry/                # TTL and LRU cache expiration
cache/                 # Local cache directory (gitignored)
```

## Protocol Handler Pattern

Each protocol package follows a consistent structure:
- `handler.go` - HTTP handler with ServeHTTP method
- `upstream.go` - Client for fetching from upstream registry
- `index.go` - Metadata storage and lookup
- `types.go` - Data structures and constants
- `*_test.go` - Tests for each component

When adding a new protocol:
1. Create a new package under `protocol/`
2. Implement Handler, Upstream, and Index types
3. Register routes in `server/http.go` (`registerRoutes`)
4. Add configuration flags in `cmd/content-cache/main.go`
5. Update README.md with usage examples

## Code Style

- **Logging**: ALWAYS use `"log/slog"` for all logging operations
- **Testing**: Use `testify/require` for assertions
- **Error handling**: Return errors up the stack, log at top level only
- **Package names**: Lowercase, descriptive (goproxy, npm, oci, pypi, maven, git)
- **Contexts**: Pass contexts for cancellation and tracing throughout
- **Options pattern**: Use functional options for configurable types (see `WithLogger`, `WithUpstream`)

## Documentation Style

When creating documentation (README, code comments, design docs):
- Start with the customer problem and work backwards
- Use clear, concise, and data-driven language
- Include specific examples and concrete details
- Structure with clear headings and bullet points
- Focus on operational excellence, security, and scalability
- Include implementation details and edge cases

## Commit Message Style

Use conventional commits format:
```
feat: add npm registry support and TTL/LRU cache expiration

- Add npm protocol handler with tarball caching and integrity verification
- Implement expiry system with TTL and LRU eviction policies
- Fix golangci-lint errors across codebase
```

Types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`

## CI/CD (Buildkite)

Pipeline runs on every push:
1. **base_image** - Rebuilds Docker image when `.buildkite/Dockerfile.build` changes (main only)
2. **QA group** - Runs in parallel:
   - `golangci-lint run --verbose --timeout 3m`
   - `go test -coverprofile coverage.out -coverpkg=./... ./...`

The base image is cached at `${BUILDKITE_HOSTED_REGISTRY_URL}/content_cache_base:latest`.

## Key Dependencies

- `github.com/zeebo/blake3` - BLAKE3 hashing for content addressing
- `github.com/google/uuid` - Request ID generation
- `github.com/stretchr/testify` - Test assertions
- `golang.org/x/net` - Extended networking utilities
- `golang.org/x/sync` - Singleflight for download deduplication

## Planned Features

- S3 storage backend
- Compression (zstd)
- OpenTelemetry metrics and tracing
