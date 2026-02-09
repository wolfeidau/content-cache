package oci

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRouter(t *testing.T) {
	t.Run("single registry", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.NoError(t, err)
		require.NotNil(t, r)
	})

	t.Run("multiple registries", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
			{Prefix: "ghcr", Upstream: NewUpstream()},
		})
		require.NoError(t, err)
		require.NotNil(t, r)
	})

	t.Run("empty registries", func(t *testing.T) {
		_, err := NewRouter([]Registry{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one registry")
	})

	t.Run("empty prefix", func(t *testing.T) {
		_, err := NewRouter([]Registry{
			{Prefix: "", Upstream: NewUpstream()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "must not be empty")
	})

	t.Run("uppercase prefix rejected", func(t *testing.T) {
		_, err := NewRouter([]Registry{
			{Prefix: "DockerHub", Upstream: NewUpstream()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be lowercase")
	})

	t.Run("slash in prefix rejected", func(t *testing.T) {
		_, err := NewRouter([]Registry{
			{Prefix: "docker/hub", Upstream: NewUpstream()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "must not contain slashes")
	})

	t.Run("duplicate prefixes rejected", func(t *testing.T) {
		_, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate")
	})

	t.Run("overlapping prefixes rejected", func(t *testing.T) {
		_, err := NewRouter([]Registry{
			{Prefix: "ecr", Upstream: NewUpstream()},
			{Prefix: "ecr-public", Upstream: NewUpstream()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "overlapping")
	})

	t.Run("custom logger", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		}, WithRouterLogger(nil))
		require.NoError(t, err)
		require.NotNil(t, r)
	})
}

func TestRouterRoute(t *testing.T) {
	t.Run("single prefix match", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.NoError(t, err)

		reg, remainder, err := r.Route("/v2/docker-hub/library/nginx/manifests/latest")
		require.NoError(t, err)
		require.Equal(t, "docker-hub", reg.Prefix)
		require.Equal(t, "library/nginx/manifests/latest", remainder)
	})

	t.Run("multiple prefixes longest match", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "ghcr", Upstream: NewUpstream()},
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.NoError(t, err)

		reg, remainder, err := r.Route("/v2/docker-hub/library/nginx/manifests/latest")
		require.NoError(t, err)
		require.Equal(t, "docker-hub", reg.Prefix)
		require.Equal(t, "library/nginx/manifests/latest", remainder)

		reg2, remainder2, err := r.Route("/v2/ghcr/myorg/myimage/manifests/v1.0")
		require.NoError(t, err)
		require.Equal(t, "ghcr", reg2.Prefix)
		require.Equal(t, "myorg/myimage/manifests/v1.0", remainder2)
	})

	t.Run("no match returns error", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.NoError(t, err)

		_, _, err = r.Route("/v2/ghcr/myorg/myimage/manifests/v1.0")
		require.ErrorIs(t, err, ErrNoMatchingRegistry)
	})

	t.Run("blob path", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.NoError(t, err)

		reg, remainder, err := r.Route("/v2/docker-hub/library/alpine/blobs/sha256:abc123")
		require.NoError(t, err)
		require.Equal(t, "docker-hub", reg.Prefix)
		require.Equal(t, "library/alpine/blobs/sha256:abc123", remainder)
	})

	t.Run("prefix without trailing content does not match", func(t *testing.T) {
		r, err := NewRouter([]Registry{
			{Prefix: "docker-hub", Upstream: NewUpstream()},
		})
		require.NoError(t, err)

		// "/v2/docker-hub" without trailing slash+content should not match
		_, _, err = r.Route("/v2/docker-hub")
		require.ErrorIs(t, err, ErrNoMatchingRegistry)
	})
}
