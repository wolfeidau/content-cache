package metadb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestIndex(t *testing.T, protocol string, ttl time.Duration) *Index {
	t.Helper()
	db := newTestBoltDB(t)
	return NewIndex(db, protocol, ttl)
}

func TestIndex_GetPutRoundTrip(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	key := "lodash@4.17.21"
	data := []byte(`{"name":"lodash","version":"4.17.21"}`)

	err := idx.Put(ctx, key, data)
	require.NoError(t, err)

	got, err := idx.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestIndex_GetReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	_, err := idx.Get(ctx, "nonexistent")
	require.ErrorIs(t, err, ErrNotFound)
}

type testPackage struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func TestIndex_GetJSONPutJSONRoundTrip(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	key := "express@4.18.0"
	pkg := testPackage{Name: "express", Version: "4.18.0"}

	err := idx.PutJSON(ctx, key, pkg)
	require.NoError(t, err)

	var got testPackage
	err = idx.GetJSON(ctx, key, &got)
	require.NoError(t, err)
	assert.Equal(t, pkg, got)
}

func TestIndex_GetJSONReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	var got testPackage
	err := idx.GetJSON(ctx, "nonexistent", &got)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestIndex_Delete(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	key := "react@18.0.0"
	data := []byte(`{"name":"react"}`)

	require.NoError(t, idx.Put(ctx, key, data))

	err := idx.Delete(ctx, key)
	require.NoError(t, err)

	_, err = idx.Get(ctx, key)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestIndex_List(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	require.NoError(t, idx.Put(ctx, "pkg1", []byte("data1")))
	require.NoError(t, idx.Put(ctx, "pkg2", []byte("data2")))
	require.NoError(t, idx.Put(ctx, "pkg3", []byte("data3")))

	keys, err := idx.List(ctx)
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.ElementsMatch(t, []string{"pkg1", "pkg2", "pkg3"}, keys)
}

func TestIndex_ListEmpty(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t, "npm", time.Hour)

	keys, err := idx.List(ctx)
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestIndex_ProtocolIsolation(t *testing.T) {
	ctx := context.Background()
	db := newTestBoltDB(t)

	npmIdx := NewIndex(db, "npm", time.Hour)
	pypiIdx := NewIndex(db, "pypi", time.Hour)

	require.NoError(t, npmIdx.Put(ctx, "shared-key", []byte("npm-data")))
	require.NoError(t, pypiIdx.Put(ctx, "shared-key", []byte("pypi-data")))

	npmData, err := npmIdx.Get(ctx, "shared-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("npm-data"), npmData)

	pypiData, err := pypiIdx.Get(ctx, "shared-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("pypi-data"), pypiData)
}
