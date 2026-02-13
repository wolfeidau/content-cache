package expiry

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

func TestMetadataStoreCreateAndGet(t *testing.T) {
	meta, b, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	hash := contentcache.HashBytes([]byte("test content"))

	// Create metadata
	err := meta.Create(ctx, hash, 1024)
	require.NoError(t, err)

	// Get metadata
	got, err := meta.Get(ctx, hash)
	require.NoError(t, err)

	require.Equal(t, hash, got.Hash)
	require.Equal(t, int64(1024), got.Size)
	require.False(t, got.CreatedAt.IsZero())

	_ = b // Use b to avoid unused variable warning
}

func TestMetadataStoreTouch(t *testing.T) {
	meta, _, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	hash := contentcache.HashBytes([]byte("touch test"))

	// Create with old time
	oldTime := time.Now().Add(-24 * time.Hour)
	meta.now = func() time.Time { return oldTime }
	_ = meta.Create(ctx, hash, 100)

	// Verify initial time
	got, _ := meta.Get(ctx, hash)
	require.True(t, got.LastAccessed.Equal(oldTime))

	// Touch with new time
	newTime := time.Now()
	meta.now = func() time.Time { return newTime }
	_ = meta.Touch(ctx, hash)

	// Verify updated time
	got, _ = meta.Get(ctx, hash)
	require.True(t, got.LastAccessed.Equal(newTime))
}

func TestMetadataStoreDelete(t *testing.T) {
	meta, _, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	hash := contentcache.HashBytes([]byte("delete test"))

	_ = meta.Create(ctx, hash, 100)

	// Delete
	err := meta.Delete(ctx, hash)
	require.NoError(t, err)

	// Verify deleted
	_, err = meta.Get(ctx, hash)
	require.Error(t, err)
}

func TestMetadataStoreList(t *testing.T) {
	meta, _, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple blobs
	hashes := []contentcache.Hash{
		contentcache.HashBytes([]byte("blob1")),
		contentcache.HashBytes([]byte("blob2")),
		contentcache.HashBytes([]byte("blob3")),
	}

	for i, h := range hashes {
		_ = meta.Create(ctx, h, int64(100*(i+1)))
	}

	// List all
	blobs, err := meta.List(ctx)
	require.NoError(t, err)

	require.Len(t, blobs, len(hashes))
}

func TestMetadataStoreStats(t *testing.T) {
	meta, _, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create blobs with different times
	baseTime := time.Now()
	meta.now = func() time.Time { return baseTime.Add(-2 * time.Hour) }
	_ = meta.Create(ctx, contentcache.HashBytes([]byte("old")), 100)

	meta.now = func() time.Time { return baseTime.Add(-1 * time.Hour) }
	_ = meta.Create(ctx, contentcache.HashBytes([]byte("mid")), 200)

	meta.now = func() time.Time { return baseTime }
	_ = meta.Create(ctx, contentcache.HashBytes([]byte("new")), 300)

	stats, err := meta.GetStats(ctx)
	require.NoError(t, err)

	require.Equal(t, int64(3), stats.TotalBlobs)
	require.Equal(t, int64(600), stats.TotalSize)
}

func TestManagerTTLExpiration(t *testing.T) {
	meta, b, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	baseTime := time.Now()

	// Create blobs at different times
	oldHash := contentcache.HashBytes([]byte("old blob"))
	newHash := contentcache.HashBytes([]byte("new blob"))

	// Old blob - 10 days ago
	meta.now = func() time.Time { return baseTime.Add(-10 * 24 * time.Hour) }
	_ = meta.Create(ctx, oldHash, 100)
	_ = b.Write(ctx, contentcache.BlobStorageKey(oldHash), strings.NewReader("old blob"))

	// New blob - 1 day ago
	meta.now = func() time.Time { return baseTime.Add(-24 * time.Hour) }
	_ = meta.Create(ctx, newHash, 100)
	_ = b.Write(ctx, contentcache.BlobStorageKey(newHash), strings.NewReader("new blob"))

	// Create manager with 7 day TTL
	cfg := Config{
		TTL:           7 * 24 * time.Hour,
		CheckInterval: time.Hour,
	}
	mgr := NewManager(meta, b, cfg)
	mgr.now = func() time.Time { return baseTime }

	// Run expiration
	result := mgr.RunOnce(ctx)

	require.Equal(t, 1, result.TTLExpired)
	require.Equal(t, int64(100), result.BytesFreed)

	// Old blob should be gone
	exists, _ := b.Exists(ctx, contentcache.BlobStorageKey(oldHash))
	require.False(t, exists)

	// New blob should still exist
	exists, _ = b.Exists(ctx, contentcache.BlobStorageKey(newHash))
	require.True(t, exists)
}

func TestManagerLRUEviction(t *testing.T) {
	meta, b, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	baseTime := time.Now()

	// Create blobs that exceed max size
	hashes := make([]contentcache.Hash, 5)
	for i := 0; i < 5; i++ {
		content := strings.Repeat("x", 100)
		hashes[i] = contentcache.HashBytes([]byte(content + string(rune('a'+i))))

		// Stagger access times
		meta.now = func() time.Time { return baseTime.Add(time.Duration(i) * time.Hour) }
		_ = meta.Create(ctx, hashes[i], 100)
		_ = b.Write(ctx, contentcache.BlobStorageKey(hashes[i]), strings.NewReader(content))
	}

	// Create manager with 300 byte max (should evict 2 blobs)
	cfg := Config{
		MaxSize:       300,
		CheckInterval: time.Hour,
	}
	mgr := NewManager(meta, b, cfg)
	mgr.now = func() time.Time { return baseTime.Add(10 * time.Hour) }

	// Run eviction
	result := mgr.RunOnce(ctx)

	require.Equal(t, 2, result.LRUEvicted)

	// Oldest blobs should be gone
	for i := 0; i < 2; i++ {
		exists, _ := b.Exists(ctx, contentcache.BlobStorageKey(hashes[i]))
		require.False(t, exists)
	}

	// Newer blobs should remain
	for i := 2; i < 5; i++ {
		exists, _ := b.Exists(ctx, contentcache.BlobStorageKey(hashes[i]))
		require.True(t, exists)
	}
}

func TestManagerCombinedTTLAndLRU(t *testing.T) {
	meta, b, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	baseTime := time.Now()

	// Create a mix of old and recent blobs
	// Old blobs (should be TTL expired)
	oldHash1 := contentcache.HashBytes([]byte("old1"))
	meta.now = func() time.Time { return baseTime.Add(-10 * 24 * time.Hour) }
	_ = meta.Create(ctx, oldHash1, 100)
	_ = b.Write(ctx, contentcache.BlobStorageKey(oldHash1), strings.NewReader("old1"))

	// Recent blobs (should trigger LRU)
	recentHashes := make([]contentcache.Hash, 3)
	for i := 0; i < 3; i++ {
		recentHashes[i] = contentcache.HashBytes([]byte(strings.Repeat("r", i+1)))
		meta.now = func() time.Time { return baseTime.Add(time.Duration(i) * time.Hour) }
		_ = meta.Create(ctx, recentHashes[i], 100)
		_ = b.Write(ctx, contentcache.BlobStorageKey(recentHashes[i]), strings.NewReader(strings.Repeat("r", i+1)))
	}

	cfg := Config{
		TTL:           7 * 24 * time.Hour,
		MaxSize:       200, // Should keep only 2 recent blobs
		CheckInterval: time.Hour,
	}
	mgr := NewManager(meta, b, cfg)
	mgr.now = func() time.Time { return baseTime.Add(5 * time.Hour) }

	result := mgr.RunOnce(ctx)

	// 1 TTL expired + 1 LRU evicted
	require.Equal(t, 1, result.TTLExpired)
	require.Equal(t, 1, result.LRUEvicted)
}

func TestManagerForceExpire(t *testing.T) {
	meta, b, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()
	baseTime := time.Now()

	// Create blobs at -1h, -3h, -5h (avoiding exact boundary)
	ages := []time.Duration{1 * time.Hour, 3 * time.Hour, 5 * time.Hour}
	for i, age := range ages {
		hash := contentcache.HashBytes([]byte(strings.Repeat("f", i+1)))
		meta.now = func() time.Time { return baseTime.Add(-age) }
		_ = meta.Create(ctx, hash, 100)
		_ = b.Write(ctx, contentcache.BlobStorageKey(hash), strings.NewReader(strings.Repeat("f", i+1)))
	}

	cfg := Config{CheckInterval: time.Hour}
	mgr := NewManager(meta, b, cfg)
	mgr.now = func() time.Time { return baseTime }

	// Force expire anything older than 2 hours (should expire -3h and -5h blobs)
	result := mgr.ForceExpire(ctx, 2*time.Hour)

	require.Equal(t, 2, result.TTLExpired)
}

func TestManagerBackgroundRun(t *testing.T) {
	meta, b, cleanup := newTestMetadataStore(t)
	defer cleanup()

	ctx := context.Background()

	cfg := Config{
		TTL:           time.Hour,
		CheckInterval: 50 * time.Millisecond,
	}
	mgr := NewManager(meta, b, cfg)

	// Start manager
	err := mgr.Start(ctx)
	require.NoError(t, err)

	// Let it run a couple cycles
	time.Sleep(150 * time.Millisecond)

	// Stop manager
	mgr.Stop()

	// Should be able to stop again without issue
	mgr.Stop()
}

// Helper functions

func newTestMetadataStore(t *testing.T) (*MetadataStore, backend.Backend, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)
	return NewMetadataStore(b), b, func() {}
}
