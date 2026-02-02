package metadb

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupEnvelopeIndex(t *testing.T, protocol, kind string, ttl time.Duration) (*EnvelopeIndex, *BoltDB) {
	t.Helper()
	db := NewBoltDB(WithNoSync(true))
	path := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, db.Open(path))

	idx, err := NewEnvelopeIndex(db, protocol, kind, ttl)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return idx, db
}

func TestEnvelopeIndex_PutGetJSON(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	type Package struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}

	pkg := Package{Name: "lodash", Version: "4.17.21"}
	require.NoError(t, idx.PutJSON(ctx, "lodash", pkg, nil))

	var got Package
	require.NoError(t, idx.GetJSON(ctx, "lodash", &got))
	require.Equal(t, pkg, got)
}

func TestEnvelopeIndex_PutGetRaw(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "goproxy", "mod", time.Hour)
	ctx := context.Background()

	data := []byte("module github.com/example/foo\n\ngo 1.21\n")
	require.NoError(t, idx.Put(ctx, "github.com/example/foo@v1.0.0", data, ContentType_CONTENT_TYPE_TEXT, nil))

	got, err := idx.Get(ctx, "github.com/example/foo@v1.0.0")
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestEnvelopeIndex_CompressionRoundTrip(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	largeJSON := `{"versions":{` + strings.Repeat(`"v1.0.0":{"name":"test"},`, 200) + `"latest":{}}}`
	require.NoError(t, idx.Put(ctx, "large-pkg", []byte(largeJSON), ContentType_CONTENT_TYPE_JSON, nil))

	env, err := idx.GetEnvelope(ctx, "large-pkg")
	require.NoError(t, err)
	require.Equal(t, ContentEncoding_CONTENT_ENCODING_ZSTD, env.ContentEncoding)
	require.Less(t, len(env.Payload), len(largeJSON))

	got, err := idx.Get(ctx, "large-pkg")
	require.NoError(t, err)
	require.JSONEq(t, largeJSON, string(got))
}

func TestEnvelopeIndex_IntegrityVerification(t *testing.T) {
	idx, db := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	require.NoError(t, idx.Put(ctx, "pkg", []byte(`{"test":true}`), ContentType_CONTENT_TYPE_JSON, nil))

	env, err := db.GetEnvelope(ctx, "npm", "metadata", "pkg")
	require.NoError(t, err)
	env.Payload = []byte(`{"test":false}`)
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "pkg", env))

	_, err = idx.Get(ctx, "pkg")
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestEnvelopeIndex_BlobRefs(t *testing.T) {
	idx, db := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	hash := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: hash, Size: 100}))

	require.NoError(t, idx.Put(ctx, "pkg", []byte(`{}`), ContentType_CONTENT_TYPE_JSON, []string{hash}))

	refs, err := idx.GetBlobRefs(ctx, "pkg")
	require.NoError(t, err)
	require.Equal(t, []string{hash}, refs)

	blob, err := db.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, 1, blob.RefCount)
}

func TestEnvelopeIndex_Delete(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	require.NoError(t, idx.Put(ctx, "pkg", []byte(`{}`), ContentType_CONTENT_TYPE_JSON, nil))

	require.NoError(t, idx.Delete(ctx, "pkg"))

	_, err := idx.Get(ctx, "pkg")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestEnvelopeIndex_List(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	require.NoError(t, idx.Put(ctx, "a", []byte(`{}`), ContentType_CONTENT_TYPE_JSON, nil))
	require.NoError(t, idx.Put(ctx, "b", []byte(`{}`), ContentType_CONTENT_TYPE_JSON, nil))
	require.NoError(t, idx.Put(ctx, "c", []byte(`{}`), ContentType_CONTENT_TYPE_JSON, nil))

	keys, err := idx.List(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, keys)
}

func TestEnvelopeIndex_UpdateJSON(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	type Counter struct {
		Count int `json:"count"`
	}

	c := Counter{Count: 1}
	require.NoError(t, idx.PutJSON(ctx, "counter", c, nil))

	err := idx.UpdateJSON(ctx, "counter", &Counter{}, func(v any) ([]string, error) {
		c := v.(*Counter)
		c.Count++
		return nil, nil
	})
	require.NoError(t, err)

	var got Counter
	require.NoError(t, idx.GetJSON(ctx, "counter", &got))
	require.Equal(t, 2, got.Count)
}

func TestEnvelopeIndex_NegativeCache(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	require.NoError(t, idx.PutNegativeCache(ctx, "nonexistent-pkg", 404, 5*time.Minute))

	env, err := idx.GetEnvelope(ctx, "nonexistent-pkg")
	require.NoError(t, err)
	require.True(t, IsNegativeCache(env))
	require.Equal(t, uint32(404), env.UpstreamStatus)
	require.Empty(t, env.Payload)
}

func TestEnvelopeIndex_PutWithOptions(t *testing.T) {
	idx, _ := setupEnvelopeIndex(t, "npm", "metadata", time.Hour)
	ctx := context.Background()

	lastMod := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	opts := PutOptions{
		TTL:            2 * time.Hour,
		Etag:           `"abc123"`,
		LastModified:   lastMod,
		Upstream:       "registry.npmjs.org",
		UpstreamStatus: 200,
	}

	require.NoError(t, idx.PutWithOptions(ctx, "pkg", []byte(`{}`), ContentType_CONTENT_TYPE_JSON, nil, opts))

	env, err := idx.GetEnvelope(ctx, "pkg")
	require.NoError(t, err)
	require.Equal(t, `"abc123"`, env.Etag)
	require.Equal(t, lastMod.UnixMilli(), env.LastModifiedUnixMs)
	require.Equal(t, "registry.npmjs.org", env.Upstream)
	require.Equal(t, uint32(200), env.UpstreamStatus)
	require.Equal(t, int64(2*time.Hour.Seconds()), env.TtlSeconds)
}

func TestEnvelopeIndex_IsExpired(t *testing.T) {
	t.Run("not expired", func(t *testing.T) {
		env := &MetadataEnvelope{
			ExpiresAtUnixMs: time.Now().Add(time.Hour).UnixMilli(),
		}
		require.False(t, IsExpired(env))
	})

	t.Run("expired", func(t *testing.T) {
		env := &MetadataEnvelope{
			ExpiresAtUnixMs: time.Now().Add(-time.Hour).UnixMilli(),
		}
		require.True(t, IsExpired(env))
	})

	t.Run("no expiry", func(t *testing.T) {
		env := &MetadataEnvelope{
			ExpiresAtUnixMs: 0,
		}
		require.False(t, IsExpired(env))
	})
}

func TestEnvelopeIndex_MultipleKinds(t *testing.T) {
	db := NewBoltDB(WithNoSync(true))
	path := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, db.Open(path))
	defer db.Close()

	modIdx, err := NewEnvelopeIndex(db, "goproxy", "mod", time.Hour)
	require.NoError(t, err)

	infoIdx, err := NewEnvelopeIndex(db, "goproxy", "info", time.Hour)
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, modIdx.Put(ctx, "mod@v1.0.0", []byte("module content"), ContentType_CONTENT_TYPE_TEXT, nil))
	require.NoError(t, infoIdx.Put(ctx, "mod@v1.0.0", []byte(`{"Version":"v1.0.0"}`), ContentType_CONTENT_TYPE_JSON, nil))

	modData, err := modIdx.Get(ctx, "mod@v1.0.0")
	require.NoError(t, err)
	require.Equal(t, "module content", string(modData))

	infoData, err := infoIdx.Get(ctx, "mod@v1.0.0")
	require.NoError(t, err)
	require.JSONEq(t, `{"Version":"v1.0.0"}`, string(infoData))

	modKeys, err := modIdx.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"mod@v1.0.0"}, modKeys)

	infoKeys, err := infoIdx.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"mod@v1.0.0"}, infoKeys)
}
