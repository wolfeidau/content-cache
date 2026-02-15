package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTaggedRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	return InjectTags(r)
}

func TestInjectTags_DefaultsCacheResultToBypass(t *testing.T) {
	r := newTaggedRequest()
	tags := GetTags(r)
	require.NotNil(t, tags)
	require.Equal(t, CacheBypass, tags.CacheResult)
}

func TestInjectTags_DefaultsProtocolEmpty(t *testing.T) {
	r := newTaggedRequest()
	tags := GetTags(r)
	require.Empty(t, tags.Protocol)
}

func TestGetTags_NilWithoutInject(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	require.Nil(t, GetTags(r))
}

func TestSetProtocol(t *testing.T) {
	r := newTaggedRequest()
	SetProtocol(r, "npm")
	require.Equal(t, "npm", GetTags(r).Protocol)
}

func TestSetProtocol_NoopWithoutInject(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	SetProtocol(r, "npm") // should not panic
}

func TestSetCacheResult(t *testing.T) {
	r := newTaggedRequest()
	SetCacheResult(r, CacheHit)
	require.Equal(t, CacheHit, GetTags(r).CacheResult)
}

func TestSetCacheResult_OverridesDefault(t *testing.T) {
	r := newTaggedRequest()
	require.Equal(t, CacheBypass, GetTags(r).CacheResult)
	SetCacheResult(r, CacheMiss)
	require.Equal(t, CacheMiss, GetTags(r).CacheResult)
}

func TestSetEndpoint(t *testing.T) {
	r := newTaggedRequest()
	SetEndpoint(r, "blob")
	require.Equal(t, "blob", GetTags(r).Endpoint)
}

func TestTagsMutationVisibleThroughPointer(t *testing.T) {
	r := newTaggedRequest()
	tags := GetTags(r)

	SetProtocol(r, "oci")
	SetCacheResult(r, CacheHit)
	SetEndpoint(r, "manifest")

	require.Equal(t, "oci", tags.Protocol)
	require.Equal(t, CacheHit, tags.CacheResult)
	require.Equal(t, "manifest", tags.Endpoint)
}
