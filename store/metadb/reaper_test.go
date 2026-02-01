package metadb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpiryReaper(t *testing.T) {
	ctx := context.Background()

	t.Run("reaps expired entries and decrements refs", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		currentTime := baseTime
		db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

		// Create a blob
		require.NoError(t, db.PutBlob(ctx, &BlobEntry{
			Hash:       "hash1",
			Size:       100,
			CachedAt:   currentTime,
			LastAccess: currentTime,
			RefCount:   0,
		}))

		// Put meta with refs and short TTL
		err := db.PutMetaWithRefs(ctx, "npm", "pkg1", []byte(`{"v":1}`), 10*time.Minute, []string{"hash1"})
		require.NoError(t, err)

		// Verify blob ref was incremented
		blob, err := db.GetBlob(ctx, "hash1")
		require.NoError(t, err)
		assert.Equal(t, 1, blob.RefCount)

		// Advance time past expiry
		currentTime = baseTime.Add(30 * time.Minute)

		// Create and run reaper
		reaper := NewExpiryReaper(db,
			WithReaperInterval(time.Minute),
			WithReaperBatchSize(10),
		)
		reaper.ReapNow(ctx)

		// Meta should be gone
		_, err = db.GetMeta(ctx, "npm", "pkg1")
		require.ErrorIs(t, err, ErrNotFound)

		// Blob ref should be decremented
		blob, err = db.GetBlob(ctx, "hash1")
		require.NoError(t, err)
		assert.Equal(t, 0, blob.RefCount)
	})

	t.Run("respects batch size", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		currentTime := baseTime
		db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

		// Put multiple entries with short TTL
		for i := 0; i < 10; i++ {
			key := "pkg" + string(rune('0'+i))
			require.NoError(t, db.PutMeta(ctx, "npm", key, []byte(`{}`), 5*time.Minute))
		}

		// Advance time past expiry
		currentTime = baseTime.Add(30 * time.Minute)

		// Reaper with batch size 3
		reaper := NewExpiryReaper(db, WithReaperBatchSize(3))

		// First reap: should delete 3
		reaper.ReapNow(ctx)
		keys1, err := db.ListMeta(ctx, "npm")
		require.NoError(t, err)
		assert.Len(t, keys1, 7)

		// Second reap: should delete 3 more
		reaper.ReapNow(ctx)
		keys2, err := db.ListMeta(ctx, "npm")
		require.NoError(t, err)
		assert.Len(t, keys2, 4)
	})

	t.Run("does nothing when no expired entries", func(t *testing.T) {
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		currentTime := baseTime
		db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

		// Put entry with long TTL
		require.NoError(t, db.PutMeta(ctx, "npm", "pkg1", []byte(`{}`), 24*time.Hour))

		// Advance time but not past expiry
		currentTime = baseTime.Add(time.Hour)

		reaper := NewExpiryReaper(db)
		reaper.ReapNow(ctx)

		// Entry should still exist
		_, err := db.GetMeta(ctx, "npm", "pkg1")
		require.NoError(t, err)
	})

	t.Run("Run stops on context cancel", func(t *testing.T) {
		db := newTestBoltDB(t)
		reaper := NewExpiryReaper(db, WithReaperInterval(10*time.Millisecond))

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			reaper.Run(ctx)
			close(done)
		}()

		// Let it run a bit
		time.Sleep(50 * time.Millisecond)

		// Cancel and wait for stop
		cancel()
		select {
		case <-done:
			// Success
		case <-time.After(time.Second):
			t.Fatal("reaper did not stop on context cancel")
		}
	})
}
