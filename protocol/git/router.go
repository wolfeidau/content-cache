package git

import (
	"fmt"
	"log/slog"
	"strings"
)

// Route defines a routing rule that maps repo prefixes to upstream credentials.
type Route struct {
	Match    GitRouteMatch
	Upstream *Upstream // pre-constructed upstream with credentials
}

// GitRouteMatch defines the matching criteria for a Git route.
type GitRouteMatch struct {
	RepoPrefix string // e.g., "github.com/orgA/" — prefix match against repo ref
	Any        bool   // catch-all route
}

// Router selects an upstream based on the repository reference.
type Router struct {
	routes   []Route
	fallback *Upstream // default upstream when no routes configured
	logger   *slog.Logger
}

// RouterOption configures a Router.
type RouterOption func(*Router)

// WithRouterLogger sets the logger for the router.
func WithRouterLogger(logger *slog.Logger) RouterOption {
	return func(r *Router) {
		r.logger = logger
	}
}

// WithFallback sets the fallback upstream used when no routes are configured.
func WithFallback(u *Upstream) RouterOption {
	return func(r *Router) {
		r.fallback = u
	}
}

// NewRouter creates a new Git router with the given routes.
//
// Validation rules:
//   - If routes is non-empty, the last route must have Any: true (catch-all required)
//   - Only the last route may have Any: true
//   - RepoPrefix values must end with "/" to prevent ambiguous matching
//   - RepoPrefix values are normalized to lowercase
//   - Prefixes must not be duplicated
func NewRouter(routes []Route, opts ...RouterOption) (*Router, error) {
	r := &Router{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(r)
	}

	if len(routes) == 0 {
		r.routes = routes
		return r, nil
	}

	// Validate and normalize routes.
	seenPrefixes := make(map[string]bool)
	normalized := make([]Route, len(routes))

	for i, route := range routes {
		isLast := i == len(routes)-1

		if route.Match.Any {
			if !isLast {
				return nil, fmt.Errorf("git route %d: only the last route may have any: true", i)
			}
			normalized[i] = route
			continue
		}

		if route.Match.RepoPrefix == "" {
			return nil, fmt.Errorf("git route %d: repo_prefix is required (or use any: true for catch-all)", i)
		}

		if !strings.HasSuffix(route.Match.RepoPrefix, "/") {
			return nil, fmt.Errorf("git route %d: repo_prefix %q must end with /", i, route.Match.RepoPrefix)
		}

		// Normalize to lowercase for case-insensitive host matching.
		lowerPrefix := strings.ToLower(route.Match.RepoPrefix)

		if seenPrefixes[lowerPrefix] {
			return nil, fmt.Errorf("git route %d: duplicate repo_prefix %q", i, route.Match.RepoPrefix)
		}
		seenPrefixes[lowerPrefix] = true

		normalized[i] = Route{
			Match:    GitRouteMatch{RepoPrefix: lowerPrefix},
			Upstream: route.Upstream,
		}
	}

	// Last route must be catch-all.
	if !routes[len(routes)-1].Match.Any {
		return nil, fmt.Errorf("git routes: last route must have any: true (catch-all required)")
	}

	r.routes = normalized
	return r, nil
}

// Match returns the upstream for the given repo reference.
// If no routes are configured, returns the fallback upstream.
func (r *Router) Match(repo RepoRef) *Upstream {
	if len(r.routes) == 0 {
		return r.fallback
	}

	// Normalize repo key to lowercase for matching.
	repoKey := strings.ToLower(repo.String())

	for _, route := range r.routes {
		if route.Match.Any {
			return route.Upstream
		}
		if strings.HasPrefix(repoKey, route.Match.RepoPrefix) {
			return route.Upstream
		}
	}

	// Should not reach here if validation passed (catch-all required).
	return r.routes[len(r.routes)-1].Upstream
}
