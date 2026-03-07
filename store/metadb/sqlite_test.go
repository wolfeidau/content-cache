package metadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func newTestSQLiteDB(t *testing.T, opts ...SQLiteDBOption) *SQLiteDB {
	t.Helper()
	db := NewSQLiteDB(opts...)
	dbPath := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, db.Open(dbPath))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLiteDB_MetadataOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("PutMeta and GetMeta round-trip", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		data := []byte(`{"name":"lodash","version":"4.17.21"}`)
		require.NoError(t, db.PutMeta(ctx, "npm", "lodash@4.17.21", data, time.Hour))

		got, err := db.GetMeta(ctx, "npm", "lodash@4.17.21")
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("GetMeta returns ErrNotFound for missing key", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		_, err := db.GetMeta(ctx, "npm", "nonexistent")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("PutMeta overwrites existing entry", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg", []byte("v1"), time.Hour))
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg", []byte("v2"), time.Hour))

		got, err := db.GetMeta(ctx, "npm", "pkg")
		require.NoError(t, err)
		assert.Equal(t, []byte("v2"), got)
	})

	t.Run("DeleteMeta removes entry", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg", []byte("data"), time.Hour))
		require.NoError(t, db.DeleteMeta(ctx, "npm", "pkg"))

		_, err := db.GetMeta(ctx, "npm", "pkg")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("ListMeta returns keys for a protocol only", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg1", []byte("1"), time.Hour))
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg2", []byte("2"), time.Hour))
		require.NoError(t, db.PutMeta(ctx, "pypi", "other", []byte("x"), time.Hour))

		keys, err := db.ListMeta(ctx, "npm")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"pkg1", "pkg2"}, keys)
	})
}

func TestSQLiteDB_BlobOperations(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)

	t.Run("PutBlob and GetBlob round-trip", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		entry := &BlobEntry{
			Hash:       "blake3:aaaa",
			Size:       1024,
			CachedAt:   now,
			LastAccess: now,
		}
		require.NoError(t, db.PutBlob(ctx, entry))

		got, err := db.GetBlob(ctx, "blake3:aaaa")
		require.NoError(t, err)
		assert.Equal(t, entry.Hash, got.Hash)
		assert.Equal(t, entry.Size, got.Size)
		assert.WithinDuration(t, entry.CachedAt, got.CachedAt, time.Millisecond)
	})

	t.Run("GetBlob returns ErrNotFound", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		_, err := db.GetBlob(ctx, "blake3:missing")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("DeleteBlob removes entry", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:bb", Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.DeleteBlob(ctx, "blake3:bb"))
		_, err := db.GetBlob(ctx, "blake3:bb")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("IncrementBlobRef and DecrementBlobRef", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:cc", Size: 200, CachedAt: now, LastAccess: now}))

		require.NoError(t, db.IncrementBlobRef(ctx, "blake3:cc"))
		require.NoError(t, db.IncrementBlobRef(ctx, "blake3:cc"))

		got, err := db.GetBlob(ctx, "blake3:cc")
		require.NoError(t, err)
		assert.Equal(t, 2, got.RefCount)

		require.NoError(t, db.DecrementBlobRef(ctx, "blake3:cc"))
		got, err = db.GetBlob(ctx, "blake3:cc")
		require.NoError(t, err)
		assert.Equal(t, 1, got.RefCount)
	})

	t.Run("DecrementBlobRef does not go below zero", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:dd", Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.DecrementBlobRef(ctx, "blake3:dd"))

		got, err := db.GetBlob(ctx, "blake3:dd")
		require.NoError(t, err)
		assert.Equal(t, 0, got.RefCount)
	})

	t.Run("TouchBlob updates access count capped at 3", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:ee", Size: 100, CachedAt: now, LastAccess: now}))

		for i := 1; i <= 5; i++ {
			count, err := db.TouchBlob(ctx, "blake3:ee")
			require.NoError(t, err)
			expected := i
			if expected > 3 {
				expected = 3
			}
			assert.Equal(t, expected, count)
		}
	})

	t.Run("TouchBlob returns 0 for missing hash", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		count, err := db.TouchBlob(ctx, "blake3:missing")
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("TotalBlobSize sums all blob sizes", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:ff", Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:gg", Size: 200, CachedAt: now, LastAccess: now}))

		total, err := db.TotalBlobSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(300), total)
	})
}

func TestSQLiteDB_GetExpiredMeta(t *testing.T) {
	ctx := context.Background()
	fixedNow := time.Now()

	db := newTestSQLiteDB(t, WithSQLiteNow(func() time.Time { return fixedNow }))
	require.NoError(t, db.PutMeta(ctx, "npm", "expired", []byte("x"), time.Millisecond))
	require.NoError(t, db.PutMeta(ctx, "npm", "fresh", []byte("y"), time.Hour))

	entries, err := db.GetExpiredMeta(ctx, fixedNow.Add(time.Second), 100)
	require.NoError(t, err)

	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	assert.Contains(t, keys, "expired")
	assert.NotContains(t, keys, "fresh")
}

func TestSQLiteDB_GetUnreferencedBlobs(t *testing.T) {
	ctx := context.Background()
	past := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	recent := time.Now().Truncate(time.Millisecond)

	t.Run("returns blobs with zero ref count before cutoff", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:old", Size: 100, CachedAt: past, LastAccess: past}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:new", Size: 100, CachedAt: recent, LastAccess: recent}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:ref", Size: 100, CachedAt: past, LastAccess: past, RefCount: 1}))

		hashes, err := db.GetUnreferencedBlobs(ctx, time.Now().Add(-time.Hour), 100)
		require.NoError(t, err)
		assert.Contains(t, hashes, "blake3:old")
		assert.NotContains(t, hashes, "blake3:new")
		assert.NotContains(t, hashes, "blake3:ref")
	})

	t.Run("zero before returns all unreferenced blobs", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:a", Size: 100, CachedAt: past, LastAccess: past}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:b", Size: 100, CachedAt: recent, LastAccess: recent}))

		hashes, err := db.GetUnreferencedBlobs(ctx, time.Time{}, 100)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"blake3:a", "blake3:b"}, hashes)
	})
}

func TestSQLiteDB_PutMetaWithRefs(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	mh1 := blobHash("a1")
	mh2 := blobHash("a2")
	mh3 := blobHash("a3")

	t.Run("increments ref counts on add", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: mh1, Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutMetaWithRefs(ctx, "npm", "pkg", []byte("data"), time.Hour, []string{mh1}))

		blob, err := db.GetBlob(ctx, mh1)
		require.NoError(t, err)
		assert.Equal(t, 1, blob.RefCount)
	})

	t.Run("decrements removed refs on overwrite", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: mh2, Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: mh3, Size: 100, CachedAt: now, LastAccess: now}))

		require.NoError(t, db.PutMetaWithRefs(ctx, "npm", "pkg2", []byte("v1"), time.Hour, []string{mh2}))
		require.NoError(t, db.PutMetaWithRefs(ctx, "npm", "pkg2", []byte("v2"), time.Hour, []string{mh3}))

		b2, err := db.GetBlob(ctx, mh2)
		require.NoError(t, err)
		assert.Equal(t, 0, b2.RefCount)

		b3, err := db.GetBlob(ctx, mh3)
		require.NoError(t, err)
		assert.Equal(t, 1, b3.RefCount)
	})
}

func TestSQLiteDB_DeleteMetaWithRefs(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	xh1 := blobHash("b1")

	db := newTestSQLiteDB(t)
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: xh1, Size: 100, CachedAt: now, LastAccess: now}))
	require.NoError(t, db.PutMetaWithRefs(ctx, "npm", "pkgX", []byte("data"), time.Hour, []string{xh1}))
	require.NoError(t, db.DeleteMetaWithRefs(ctx, "npm", "pkgX"))

	_, err := db.GetMeta(ctx, "npm", "pkgX")
	require.ErrorIs(t, err, ErrNotFound)

	blob, err := db.GetBlob(ctx, xh1)
	require.NoError(t, err)
	assert.Equal(t, 0, blob.RefCount)
}

func TestSQLiteDB_UpdateJSON(t *testing.T) {
	ctx := context.Background()
	type counter struct{ Value int }

	t.Run("creates entry when not found", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		var c counter
		require.NoError(t, db.UpdateJSON(ctx, "npm", "ctr", time.Hour, func(v any) error {
			v.(*counter).Value = 42
			return nil
		}, &c))

		data, err := db.GetMeta(ctx, "npm", "ctr")
		require.NoError(t, err)
		var got counter
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, 42, got.Value)
	})

	t.Run("updates existing entry", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		var c counter
		require.NoError(t, db.UpdateJSON(ctx, "test", "ctr", time.Hour, func(v any) error {
			v.(*counter).Value = 10
			return nil
		}, &c))
		require.NoError(t, db.UpdateJSON(ctx, "test", "ctr", time.Hour, func(v any) error {
			v.(*counter).Value++
			return nil
		}, &counter{}))

		data, err := db.GetMeta(ctx, "test", "ctr")
		require.NoError(t, err)
		var got counter
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, 11, got.Value)
	})
}

func TestSQLiteDB_BlobRefErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("IncrementBlobRef returns ErrNotFound for missing blob", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		err := db.IncrementBlobRef(ctx, "blake3:missing")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("DecrementBlobRef returns ErrNotFound for missing blob", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		err := db.DecrementBlobRef(ctx, "blake3:missing")
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestSQLiteDB_TouchBlobUpdatesTime(t *testing.T) {
	ctx := context.Background()
	before := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	after := before.Add(2 * time.Hour)

	db := newTestSQLiteDB(t, WithSQLiteNow(func() time.Time { return after }))
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blake3:tt", Size: 50, CachedAt: before, LastAccess: before}))

	_, err := db.TouchBlob(ctx, "blake3:tt")
	require.NoError(t, err)

	got, err := db.GetBlob(ctx, "blake3:tt")
	require.NoError(t, err)
	assert.WithinDuration(t, after, got.LastAccess, time.Millisecond)
}

func TestSQLiteDB_SchemaVersionGuard(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "future.db")

	// Open normally to create schema at version 1.
	db := NewSQLiteDB()
	require.NoError(t, db.Open(dbPath))
	require.NoError(t, db.Close())

	// Manually bump user_version beyond what this binary supports.
	import_db, err := openRawSQLite(dbPath)
	require.NoError(t, err)
	_, err = import_db.Exec("PRAGMA user_version = 9999")
	require.NoError(t, err)
	require.NoError(t, import_db.Close())

	// Reopening should fail fast with a clear error.
	db2 := NewSQLiteDB()
	err = db2.Open(dbPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "newer than supported")
}

func TestSQLiteDB_PutEnvelopeValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects invalid blob ref format", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		env := &MetadataEnvelope{
			Payload:  []byte("{}"),
			BlobRefs: []string{"not-a-valid-ref"},
		}
		err := db.PutEnvelope(ctx, "npm", "meta", "pkg", env)
		require.ErrorIs(t, err, ErrInvalidDigestFormat)
	})

	t.Run("canonicalizes refs to lowercase deduplicated", func(t *testing.T) {
		ctx := context.Background()
		now := time.Now().Truncate(time.Millisecond)
		hash := "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		hashLower := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: hashLower, Size: 100, CachedAt: now, LastAccess: now}))

		env := &MetadataEnvelope{
			Payload:  []byte("{}"),
			BlobRefs: []string{hash, hash}, // uppercase duplicate
		}
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "pkg", env))

		refs, err := db.GetEnvelopeBlobRefs(ctx, "npm", "meta", "pkg")
		require.NoError(t, err)
		assert.Equal(t, []string{hashLower}, refs)

		blob, err := db.GetBlob(ctx, hashLower)
		require.NoError(t, err)
		assert.Equal(t, 1, blob.RefCount, "duplicate ref should only increment once")
	})
}

func TestSQLiteDB_TotalBlobSizeEmpty(t *testing.T) {
	ctx := context.Background()
	db := newTestSQLiteDB(t)
	total, err := db.TotalBlobSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
}

// openRawSQLite opens a SQLite file directly for test setup purposes.
func openRawSQLite(path string) (*sql.DB, error) {
	return sql.Open("sqlite", path)
}

// blobHash returns a valid 64-char blake3 hash for use in tests.
// The suffix is used to differentiate test hashes (e.g. "01", "02").
func blobHash(suffix string) string {
	const zeros = "0000000000000000000000000000000000000000000000000000000000000000"
	h := zeros[:64-len(suffix)] + suffix
	return "blake3:" + h
}

func TestSQLiteDB_EnvelopeOperations(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)

	h1 := blobHash("01")
	h2 := blobHash("02")
	h3 := blobHash("03")
	h4 := blobHash("04")
	h5 := blobHash("05")
	h6 := blobHash("06")

	makeEnv := func(refs []string, ttl time.Duration) *MetadataEnvelope {
		var expiresMs int64
		if ttl > 0 {
			expiresMs = now.Add(ttl).UnixMilli()
		}
		return &MetadataEnvelope{
			Payload:         []byte("payload"),
			BlobRefs:        refs,
			ExpiresAtUnixMs: expiresMs,
		}
	}

	t.Run("PutEnvelope and GetEnvelope round-trip", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: h1, Size: 100, CachedAt: now, LastAccess: now}))

		env := makeEnv([]string{h1}, time.Hour)
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "lodash", env))

		got, err := db.GetEnvelope(ctx, "npm", "meta", "lodash")
		require.NoError(t, err)
		assert.Equal(t, env.Payload, got.Payload)

		blob, err := db.GetBlob(ctx, h1)
		require.NoError(t, err)
		assert.Equal(t, 1, blob.RefCount)
	})

	t.Run("GetEnvelope returns ErrNotFound", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		_, err := db.GetEnvelope(ctx, "npm", "meta", "missing")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("DeleteEnvelope decrements blob refs", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: h2, Size: 100, CachedAt: now, LastAccess: now}))

		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "pkg", makeEnv([]string{h2}, time.Hour)))
		require.NoError(t, db.DeleteEnvelope(ctx, "npm", "meta", "pkg"))

		_, err := db.GetEnvelope(ctx, "npm", "meta", "pkg")
		require.ErrorIs(t, err, ErrNotFound)

		blob, err := db.GetBlob(ctx, h2)
		require.NoError(t, err)
		assert.Equal(t, 0, blob.RefCount)
	})

	t.Run("ListEnvelopeKeys returns keys for protocol/kind", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "a", makeEnv(nil, 0)))
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "b", makeEnv(nil, 0)))
		require.NoError(t, db.PutEnvelope(ctx, "npm", "tarball", "c", makeEnv(nil, 0)))

		keys, err := db.ListEnvelopeKeys(ctx, "npm", "meta")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"a", "b"}, keys)
	})

	t.Run("UpdateEnvelope swaps blob refs transactionally", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: h3, Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: h4, Size: 100, CachedAt: now, LastAccess: now}))

		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "up", makeEnv([]string{h3}, time.Hour)))
		require.NoError(t, db.UpdateEnvelope(ctx, "npm", "meta", "up",
			func(e *MetadataEnvelope) (*MetadataEnvelope, error) {
				e.BlobRefs = []string{h4}
				return e, nil
			}))

		b3, err := db.GetBlob(ctx, h3)
		require.NoError(t, err)
		assert.Equal(t, 0, b3.RefCount)

		b4, err := db.GetBlob(ctx, h4)
		require.NoError(t, err)
		assert.Equal(t, 1, b4.RefCount)
	})

	t.Run("UpdateEnvelope with nil return deletes entry", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: h5, Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "del", makeEnv([]string{h5}, time.Hour)))

		require.NoError(t, db.UpdateEnvelope(ctx, "npm", "meta", "del",
			func(_ *MetadataEnvelope) (*MetadataEnvelope, error) { return nil, nil }))

		_, err := db.GetEnvelope(ctx, "npm", "meta", "del")
		require.ErrorIs(t, err, ErrNotFound)

		b5, err := db.GetBlob(ctx, h5)
		require.NoError(t, err)
		assert.Equal(t, 0, b5.RefCount)
	})

	t.Run("GetExpiredEnvelopes returns entries before cutoff", func(t *testing.T) {
		db := newTestSQLiteDB(t, WithSQLiteNow(func() time.Time { return now }))
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "expired", makeEnv(nil, time.Millisecond)))
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "fresh", makeEnv(nil, time.Hour)))

		entries, err := db.GetExpiredEnvelopes(ctx, now.Add(time.Second), 100)
		require.NoError(t, err)

		keys := make([]string, len(entries))
		for i, e := range entries {
			keys[i] = e.Key
		}
		assert.Contains(t, keys, "expired")
		assert.NotContains(t, keys, "fresh")
	})

	t.Run("DeleteExpiredEnvelopes decrements blob refs", func(t *testing.T) {
		db := newTestSQLiteDB(t)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: h6, Size: 100, CachedAt: now, LastAccess: now}))

		expiredEnv := &MetadataEnvelope{
			Payload:         []byte("x"),
			BlobRefs:        []string{h6},
			ExpiresAtUnixMs: now.Add(-time.Second).UnixMilli(),
		}
		require.NoError(t, db.PutEnvelope(ctx, "npm", "meta", "exp", expiredEnv))

		entries, err := db.GetExpiredEnvelopes(ctx, now.Add(time.Second), 100)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		require.NoError(t, db.DeleteExpiredEnvelopes(ctx, entries))

		_, err = db.GetEnvelope(ctx, "npm", "meta", "exp")
		require.ErrorIs(t, err, ErrNotFound)

		blob, err := db.GetBlob(ctx, h6)
		require.NoError(t, err)
		assert.Equal(t, 0, blob.RefCount)
	})
}
