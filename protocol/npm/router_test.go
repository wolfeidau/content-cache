package npm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRouter_EmptyRoutes(t *testing.T) {
	fallback := NewUpstream()
	r, err := NewRouter(nil, WithFallback(fallback))
	require.NoError(t, err)
	require.Equal(t, fallback, r.Match("react"))
}

func TestNewRouter_ScopeMatching(t *testing.T) {
	privateUpstream := NewUpstream(WithRegistryURL("https://npm.pkg.github.com"))
	publicUpstream := NewUpstream(WithRegistryURL("https://registry.npmjs.org"))

	r, err := NewRouter([]Route{
		{Match: RouteMatch{Scope: "@mycompany"}, Registry: privateUpstream},
		{Match: RouteMatch{Any: true}, Registry: publicUpstream},
	})
	require.NoError(t, err)

	require.Equal(t, privateUpstream, r.Match("@mycompany/pkg"))
	require.Equal(t, publicUpstream, r.Match("react"))
	require.Equal(t, publicUpstream, r.Match("@other/pkg"))
}

func TestNewRouter_MultipleScopes(t *testing.T) {
	upstreamA := NewUpstream(WithRegistryURL("https://a.example.com"))
	upstreamB := NewUpstream(WithRegistryURL("https://b.example.com"))
	catchAll := NewUpstream(WithRegistryURL("https://registry.npmjs.org"))

	r, err := NewRouter([]Route{
		{Match: RouteMatch{Scope: "@orgA"}, Registry: upstreamA},
		{Match: RouteMatch{Scope: "@orgB"}, Registry: upstreamB},
		{Match: RouteMatch{Any: true}, Registry: catchAll},
	})
	require.NoError(t, err)

	require.Equal(t, upstreamA, r.Match("@orgA/pkg"))
	require.Equal(t, upstreamB, r.Match("@orgB/pkg"))
	require.Equal(t, catchAll, r.Match("react"))
	require.Equal(t, catchAll, r.Match("@unknown/pkg"))
}

func TestNewRouter_Validation_NoCatchAll(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{Scope: "@mycompany"}, Registry: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "catch-all required")
}

func TestNewRouter_Validation_CatchAllNotLast(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{Any: true}, Registry: NewUpstream()},
		{Match: RouteMatch{Scope: "@mycompany"}, Registry: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only the last route")
}

func TestNewRouter_Validation_DuplicateScope(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{Scope: "@mycompany"}, Registry: NewUpstream()},
		{Match: RouteMatch{Scope: "@mycompany"}, Registry: NewUpstream()},
		{Match: RouteMatch{Any: true}, Registry: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate scope")
}

func TestNewRouter_Validation_ScopeWithoutAt(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{Scope: "mycompany"}, Registry: NewUpstream()},
		{Match: RouteMatch{Any: true}, Registry: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must start with @")
}

func TestNewRouter_Validation_EmptyScope(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{}, Registry: NewUpstream()},
		{Match: RouteMatch{Any: true}, Registry: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scope is required")
}

func TestExtractScope(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"react", ""},
		{"@mycompany/pkg", "@mycompany"},
		{"@scope/nested/path", "@scope"},
		{"@", ""},
		{"lodash", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, extractScope(tt.name))
		})
	}
}
