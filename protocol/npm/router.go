package npm

import (
	"fmt"
	"log/slog"
	"strings"
)

// Route defines a routing rule that maps package scopes to upstream registries.
type Route struct {
	Match    RouteMatch
	Registry *Upstream // pre-constructed upstream with credentials
}

// RouteMatch defines the matching criteria for an NPM route.
type RouteMatch struct {
	Scope string // e.g., "@mycompany" — matches scoped packages
	Any   bool   // catch-all route
}

// Router selects an upstream registry based on package scope.
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

// NewRouter creates a new NPM router with the given routes.
//
// Validation rules:
//   - If routes is non-empty, the last route must have Any: true (catch-all required)
//   - Only the last route may have Any: true
//   - Scopes must start with "@" and must not be duplicated
func NewRouter(routes []Route, opts ...RouterOption) (*Router, error) {
	r := &Router{
		routes: routes,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(r)
	}

	if len(routes) == 0 {
		return r, nil
	}

	// Validate routes.
	seenScopes := make(map[string]bool)

	for i, route := range routes {
		isLast := i == len(routes)-1

		if route.Match.Any {
			if !isLast {
				return nil, fmt.Errorf("npm route %d: only the last route may have any: true", i)
			}
			continue
		}

		if route.Match.Scope == "" {
			return nil, fmt.Errorf("npm route %d: scope is required (or use any: true for catch-all)", i)
		}

		if !strings.HasPrefix(route.Match.Scope, "@") {
			return nil, fmt.Errorf("npm route %d: scope %q must start with @", i, route.Match.Scope)
		}

		if seenScopes[route.Match.Scope] {
			return nil, fmt.Errorf("npm route %d: duplicate scope %q", i, route.Match.Scope)
		}
		seenScopes[route.Match.Scope] = true
	}

	// Last route must be catch-all.
	if !routes[len(routes)-1].Match.Any {
		return nil, fmt.Errorf("npm routes: last route must have any: true (catch-all required)")
	}

	return r, nil
}

// Match returns the upstream for the given package name.
// If no routes are configured, returns the fallback upstream.
func (r *Router) Match(packageName string) *Upstream {
	if len(r.routes) == 0 {
		return r.fallback
	}

	scope := extractScope(packageName)

	for _, route := range r.routes {
		if route.Match.Any {
			return route.Registry
		}
		if scope != "" && route.Match.Scope == scope {
			return route.Registry
		}
	}

	// Should not reach here if validation passed (catch-all required),
	// but return last route's registry as safety net.
	return r.routes[len(r.routes)-1].Registry
}

// extractScope returns the scope from a package name (e.g., "@mycompany/pkg" → "@mycompany").
// Returns empty string for unscoped packages.
func extractScope(name string) string {
	if !strings.HasPrefix(name, "@") {
		return ""
	}
	if idx := strings.Index(name, "/"); idx > 0 {
		return name[:idx]
	}
	return ""
}
