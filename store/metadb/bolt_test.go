package metadb

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBoltDB(t *testing.T, opts ...BoltDBOption) *BoltDB {
	t.Helper()
	db := NewBoltDB(opts...)
	dbPath := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, db.Open(dbPath))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestBoltDB_MetadataOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("PutMeta and GetMeta round-trip", func(t *testing.T) {
		db := newTestBoltDB(t)

		protocol := "npm"
		key := "lodash@4.17.21"
		data := []byte(`{"name":"lodash","version":"4.17.21"}`)

		err := db.PutMeta(ctx, protocol, key, data, time.Hour)
		require.NoError(t, err)

		got, err := db.GetMeta(ctx, protocol, key)
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("GetMeta returns ErrNotFound for missing key", func(t *testing.T) {
		db := newTestBoltDB(t)

		_, err := db.GetMeta(ctx, "npm", "nonexistent")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("DeleteMeta removes entry", func(t *testing.T) {
		db := newTestBoltDB(t)

		protocol := "npm"
		key := "express@4.18.0"
		data := []byte(`{"name":"express"}`)

		require.NoError(t, db.PutMeta(ctx, protocol, key, data, time.Hour))

		err := db.DeleteMeta(ctx, protocol, key)
		require.NoError(t, err)

		_, err = db.GetMeta(ctx, protocol, key)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("ListMeta returns all keys for a protocol", func(t *testing.T) {
		db := newTestBoltDB(t)

		require.NoError(t, db.PutMeta(ctx, "npm", "pkg1", []byte("data1"), time.Hour))
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg2", []byte("data2"), time.Hour))
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg3", []byte("data3"), time.Hour))
		require.NoError(t, db.PutMeta(ctx, "pypi", "other", []byte("other"), time.Hour))

		keys, err := db.ListMeta(ctx, "npm")
		require.NoError(t, err)
		assert.Len(t, keys, 3)
		assert.ElementsMatch(t, []string{"pkg1", "pkg2", "pkg3"}, keys)
	})
}

func TestBoltDB_BlobOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("PutBlob and GetBlob round-trip", func(t *testing.T) {
		db := newTestBoltDB(t)

		now := time.Now().Truncate(time.Second)
		entry := &BlobEntry{
			Hash:       "abc123",
			Size:       1024,
			CachedAt:   now,
			LastAccess: now,
			RefCount:   1,
		}

		err := db.PutBlob(ctx, entry)
		require.NoError(t, err)

		got, err := db.GetBlob(ctx, "abc123")
		require.NoError(t, err)
		assert.Equal(t, entry.Hash, got.Hash)
		assert.Equal(t, entry.Size, got.Size)
		assert.Equal(t, entry.RefCount, got.RefCount)
	})

	t.Run("IncrementBlobRef increases RefCount", func(t *testing.T) {
		db := newTestBoltDB(t)

		now := time.Now()
		entry := &BlobEntry{Hash: "hash1", Size: 100, CachedAt: now, LastAccess: now, RefCount: 1}
		require.NoError(t, db.PutBlob(ctx, entry))

		err := db.IncrementBlobRef(ctx, "hash1")
		require.NoError(t, err)

		got, err := db.GetBlob(ctx, "hash1")
		require.NoError(t, err)
		assert.Equal(t, 2, got.RefCount)
	})

	t.Run("DecrementBlobRef decreases RefCount", func(t *testing.T) {
		db := newTestBoltDB(t)

		now := time.Now()
		entry := &BlobEntry{Hash: "hash2", Size: 100, CachedAt: now, LastAccess: now, RefCount: 3}
		require.NoError(t, db.PutBlob(ctx, entry))

		err := db.DecrementBlobRef(ctx, "hash2")
		require.NoError(t, err)

		got, err := db.GetBlob(ctx, "hash2")
		require.NoError(t, err)
		assert.Equal(t, 2, got.RefCount)
	})

	t.Run("TouchBlob updates LastAccess", func(t *testing.T) {
		initialTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		updatedTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

		currentTime := initialTime
		db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

		entry := &BlobEntry{Hash: "hash3", Size: 100, CachedAt: initialTime, LastAccess: initialTime, RefCount: 1}
		require.NoError(t, db.PutBlob(ctx, entry))

		currentTime = updatedTime
		err := db.TouchBlob(ctx, "hash3")
		require.NoError(t, err)

		got, err := db.GetBlob(ctx, "hash3")
		require.NoError(t, err)
		assert.Equal(t, updatedTime, got.LastAccess)
	})

	t.Run("DeleteBlob removes entry", func(t *testing.T) {
		db := newTestBoltDB(t)

		now := time.Now()
		entry := &BlobEntry{Hash: "todelete", Size: 100, CachedAt: now, LastAccess: now, RefCount: 0}
		require.NoError(t, db.PutBlob(ctx, entry))

		err := db.DeleteBlob(ctx, "todelete")
		require.NoError(t, err)

		_, err = db.GetBlob(ctx, "todelete")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("TotalBlobSize returns sum of all blob sizes", func(t *testing.T) {
		db := newTestBoltDB(t)

		now := time.Now()
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "a", Size: 100, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "b", Size: 200, CachedAt: now, LastAccess: now}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "c", Size: 300, CachedAt: now, LastAccess: now}))

		total, err := db.TotalBlobSize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(600), total)
	})
}

func TestBoltDB_ExpiryQueries(t *testing.T) {
	ctx := context.Background()

	t.Run("GetExpiredMeta returns entries past expiry time", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		db := newTestBoltDB(t, WithNow(func() time.Time { return baseTime }))

		require.NoError(t, db.PutMeta(ctx, "npm", "expired1", []byte("data"), 10*time.Minute))
		require.NoError(t, db.PutMeta(ctx, "npm", "expired2", []byte("data"), 20*time.Minute))
		require.NoError(t, db.PutMeta(ctx, "npm", "valid", []byte("data"), 2*time.Hour))

		checkTime := baseTime.Add(30 * time.Minute)
		expired, err := db.GetExpiredMeta(ctx, checkTime, 0)
		require.NoError(t, err)
		assert.Len(t, expired, 2)

		var keys []string
		for _, e := range expired {
			keys = append(keys, e.Key)
		}
		assert.ElementsMatch(t, []string{"expired1", "expired2"}, keys)
	})

	t.Run("GetExpiredMeta respects limit", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		db := newTestBoltDB(t, WithNow(func() time.Time { return baseTime }))

		require.NoError(t, db.PutMeta(ctx, "npm", "e1", []byte("data"), 5*time.Minute))
		require.NoError(t, db.PutMeta(ctx, "npm", "e2", []byte("data"), 10*time.Minute))
		require.NoError(t, db.PutMeta(ctx, "npm", "e3", []byte("data"), 15*time.Minute))

		checkTime := baseTime.Add(time.Hour)
		expired, err := db.GetExpiredMeta(ctx, checkTime, 2)
		require.NoError(t, err)
		assert.Len(t, expired, 2)
	})

	t.Run("GetExpiredMeta returns empty for no expired entries", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		db := newTestBoltDB(t, WithNow(func() time.Time { return baseTime }))

		require.NoError(t, db.PutMeta(ctx, "npm", "fresh", []byte("data"), 24*time.Hour))

		checkTime := baseTime.Add(time.Hour)
		expired, err := db.GetExpiredMeta(ctx, checkTime, 0)
		require.NoError(t, err)
		assert.Empty(t, expired)
	})
}

func TestBoltDB_LRUQueries(t *testing.T) {
	ctx := context.Background()

	t.Run("GetLRUBlobs returns oldest accessed first", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		currentTime := baseTime
		db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

		currentTime = baseTime
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "oldest", Size: 100, CachedAt: currentTime, LastAccess: currentTime}))

		currentTime = baseTime.Add(time.Hour)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "middle", Size: 100, CachedAt: currentTime, LastAccess: currentTime}))

		currentTime = baseTime.Add(2 * time.Hour)
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "newest", Size: 100, CachedAt: currentTime, LastAccess: currentTime}))

		blobs, err := db.GetLRUBlobs(ctx, 0)
		require.NoError(t, err)
		require.Len(t, blobs, 3)
		assert.Equal(t, "oldest", blobs[0].Hash)
		assert.Equal(t, "middle", blobs[1].Hash)
		assert.Equal(t, "newest", blobs[2].Hash)
	})

	t.Run("GetLRUBlobs respects limit", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		currentTime := baseTime
		db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

		for i := 0; i < 5; i++ {
			currentTime = baseTime.Add(time.Duration(i) * time.Hour)
			require.NoError(t, db.PutBlob(ctx, &BlobEntry{
				Hash:       string(rune('a' + i)),
				Size:       100,
				CachedAt:   currentTime,
				LastAccess: currentTime,
			}))
		}

		blobs, err := db.GetLRUBlobs(ctx, 2)
		require.NoError(t, err)
		assert.Len(t, blobs, 2)
	})

	t.Run("GetUnreferencedBlobs returns blobs with RefCount == 0", func(t *testing.T) {
		db := newTestBoltDB(t)

		now := time.Now()
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "ref0", Size: 100, CachedAt: now, LastAccess: now, RefCount: 0}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "ref1", Size: 100, CachedAt: now, LastAccess: now, RefCount: 1}))
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "ref0-2", Size: 100, CachedAt: now, LastAccess: now, RefCount: 0}))

		hashes, err := db.GetUnreferencedBlobs(ctx, 0)
		require.NoError(t, err)
		assert.Len(t, hashes, 2)
		assert.ElementsMatch(t, []string{"ref0", "ref0-2"}, hashes)
	})
}

func TestBoltDB_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	db := newTestBoltDB(t)

	const numGoroutines = 10
	const numOps = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				protocol := "npm"
				key := string(rune('a'+id)) + string(rune('0'+j%10))
				data := []byte("data")

				_ = db.PutMeta(ctx, protocol, key, data, time.Hour)

				_, _ = db.GetMeta(ctx, protocol, key)

				now := time.Now()
				hash := string(rune('h'+id)) + string(rune('0'+j%10))
				_ = db.PutBlob(ctx, &BlobEntry{Hash: hash, Size: 100, CachedAt: now, LastAccess: now, RefCount: 1})

				_, _ = db.GetBlob(ctx, hash)

				_ = db.TouchBlob(ctx, hash)
			}
		}(i)
	}

	wg.Wait()

	keys, err := db.ListMeta(ctx, "npm")
	require.NoError(t, err)
	assert.NotEmpty(t, keys)

	total, err := db.TotalBlobSize(ctx)
	require.NoError(t, err)
	assert.Positive(t, total)
}
