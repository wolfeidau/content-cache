package oci

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// ErrNoMatchingRegistry is returned when no registry prefix matches the request path.
var ErrNoMatchingRegistry = errors.New("no matching registry for path")

// Registry represents a configured upstream OCI registry with a routing prefix.
type Registry struct {
	Prefix   string
	Upstream *Upstream
	TagTTL   time.Duration
}

// Router routes OCI requests to the appropriate upstream registry based on prefix.
type Router struct {
	registries []Registry // sorted by prefix length descending (longest first)
	logger     *slog.Logger
}

// RouterOption configures a Router.
type RouterOption func(*Router)

// WithRouterLogger sets the logger for the router.
func WithRouterLogger(logger *slog.Logger) RouterOption {
	return func(r *Router) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// NewRouter creates a new Router with the given registries.
// It validates that prefixes are non-empty, lowercase, contain no slashes,
// are unique, and that no prefix is a prefix of another.
func NewRouter(registries []Registry, opts ...RouterOption) (*Router, error) {
	if len(registries) == 0 {
		return nil, errors.New("at least one registry is required")
	}

	// Validate prefixes
	seen := make(map[string]bool)
	for _, r := range registries {
		if r.Prefix == "" {
			return nil, errors.New("registry prefix must not be empty")
		}
		if r.Prefix != strings.ToLower(r.Prefix) {
			return nil, fmt.Errorf("registry prefix %q must be lowercase", r.Prefix)
		}
		if strings.Contains(r.Prefix, "/") {
			return nil, fmt.Errorf("registry prefix %q must not contain slashes", r.Prefix)
		}
		if seen[r.Prefix] {
			return nil, fmt.Errorf("duplicate registry prefix %q", r.Prefix)
		}
		seen[r.Prefix] = true
	}

	// Check for overlapping prefixes (one prefix is a prefix of another)
	prefixes := make([]string, 0, len(registries))
	for _, r := range registries {
		prefixes = append(prefixes, r.Prefix)
	}
	sort.Strings(prefixes)
	for i := 0; i < len(prefixes)-1; i++ {
		if strings.HasPrefix(prefixes[i+1], prefixes[i]) {
			return nil, fmt.Errorf("overlapping registry prefixes: %q is a prefix of %q", prefixes[i], prefixes[i+1])
		}
	}

	// Sort by prefix length descending for longest-prefix-first matching
	sorted := make([]Registry, len(registries))
	copy(sorted, registries)
	sort.Slice(sorted, func(i, j int) bool {
		return len(sorted[i].Prefix) > len(sorted[j].Prefix)
	})

	rt := &Router{
		registries: sorted,
		logger:     slog.Default(),
	}
	for _, opt := range opts {
		opt(rt)
	}
	return rt, nil
}

// Route matches a request path to a registry.
// The path should be the full path (e.g., "/v2/docker-hub/library/nginx/manifests/latest").
// Returns the matched registry, the remainder path (e.g., "library/nginx/manifests/latest"), and any error.
func (rt *Router) Route(path string) (*Registry, string, error) {
	// Strip /v2/ prefix
	remainder := strings.TrimPrefix(path, "/v2/")

	for i := range rt.registries {
		prefix := rt.registries[i].Prefix + "/"
		if after, ok := strings.CutPrefix(remainder, prefix); ok {
			return &rt.registries[i], after, nil
		}
	}

	return nil, "", ErrNoMatchingRegistry
}
