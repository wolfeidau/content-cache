package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGitNewRouter_EmptyRoutes(t *testing.T) {
	fallback := NewUpstream()
	r, err := NewRouter(nil, WithFallback(fallback))
	require.NoError(t, err)
	require.Equal(t, fallback, r.Match(RepoRef{Host: "github.com", RepoPath: "org/repo"}))
}

func TestGitNewRouter_EmptyRoutesNoFallback(t *testing.T) {
	_, err := NewRouter(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no fallback upstream")
}

func TestGitNewRouter_PrefixMatching(t *testing.T) {
	orgAUpstream := NewUpstream(WithBasicAuth("x-access-token", "pat-A"))
	orgBUpstream := NewUpstream(WithBasicAuth("x-access-token", "pat-B"))
	catchAll := NewUpstream()

	r, err := NewRouter([]Route{
		{Match: RouteMatch{RepoPrefix: "github.com/orgA/"}, Upstream: orgAUpstream},
		{Match: RouteMatch{RepoPrefix: "github.com/orgB/"}, Upstream: orgBUpstream},
		{Match: RouteMatch{Any: true}, Upstream: catchAll},
	})
	require.NoError(t, err)

	require.Equal(t, orgAUpstream, r.Match(RepoRef{Host: "github.com", RepoPath: "orgA/repo"}))
	require.Equal(t, orgBUpstream, r.Match(RepoRef{Host: "github.com", RepoPath: "orgB/repo"}))
	require.Equal(t, catchAll, r.Match(RepoRef{Host: "github.com", RepoPath: "public/repo"}))
	require.Equal(t, catchAll, r.Match(RepoRef{Host: "gitlab.com", RepoPath: "any/repo"}))
}

func TestGitNewRouter_CaseInsensitive(t *testing.T) {
	orgUpstream := NewUpstream(WithBasicAuth("x-access-token", "pat"))
	catchAll := NewUpstream()

	r, err := NewRouter([]Route{
		{Match: RouteMatch{RepoPrefix: "GitHub.com/OrgA/"}, Upstream: orgUpstream},
		{Match: RouteMatch{Any: true}, Upstream: catchAll},
	})
	require.NoError(t, err)

	// Should match regardless of case in the repo ref.
	require.Equal(t, orgUpstream, r.Match(RepoRef{Host: "github.com", RepoPath: "orgA/repo"}))
	require.Equal(t, orgUpstream, r.Match(RepoRef{Host: "GITHUB.COM", RepoPath: "ORGA/repo"}))
}

func TestGitNewRouter_Validation_NoCatchAll(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{RepoPrefix: "github.com/org/"}, Upstream: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "catch-all required")
}

func TestGitNewRouter_Validation_CatchAllNotLast(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{Any: true}, Upstream: NewUpstream()},
		{Match: RouteMatch{RepoPrefix: "github.com/org/"}, Upstream: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only the last route")
}

func TestGitNewRouter_Validation_MissingTrailingSlash(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{RepoPrefix: "github.com/org"}, Upstream: NewUpstream()},
		{Match: RouteMatch{Any: true}, Upstream: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must end with /")
}

func TestGitNewRouter_Validation_DuplicatePrefix(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{RepoPrefix: "github.com/org/"}, Upstream: NewUpstream()},
		{Match: RouteMatch{RepoPrefix: "github.com/org/"}, Upstream: NewUpstream()},
		{Match: RouteMatch{Any: true}, Upstream: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate repo_prefix")
}

func TestGitNewRouter_Validation_EmptyPrefix(t *testing.T) {
	_, err := NewRouter([]Route{
		{Match: RouteMatch{}, Upstream: NewUpstream()},
		{Match: RouteMatch{Any: true}, Upstream: NewUpstream()},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "repo_prefix is required")
}

func TestGitNewRouter_PrefixSecurity(t *testing.T) {
	// Ensure trailing slash prevents matching github.com/orgA-evil/
	orgAUpstream := NewUpstream(WithBasicAuth("x-access-token", "pat-A"))
	catchAll := NewUpstream()

	r, err := NewRouter([]Route{
		{Match: RouteMatch{RepoPrefix: "github.com/orgA/"}, Upstream: orgAUpstream},
		{Match: RouteMatch{Any: true}, Upstream: catchAll},
	})
	require.NoError(t, err)

	// "orgA-evil" should NOT match the "orgA/" prefix
	require.Equal(t, catchAll, r.Match(RepoRef{Host: "github.com", RepoPath: "orgA-evil/repo"}))
	require.Equal(t, orgAUpstream, r.Match(RepoRef{Host: "github.com", RepoPath: "orgA/repo"}))
}
