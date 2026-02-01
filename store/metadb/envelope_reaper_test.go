package metadb

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupReaperTestDB(t *testing.T) *BoltDB {
	t.Helper()
	db := NewBoltDB(WithNoSync(true))
	path := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, db.Open(path))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestEnvelopeReaper_ReapsExpiredEntries(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	now := time.Now()

	expired1 := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-2 * time.Hour).UnixMilli(),
	}
	expired2 := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-1 * time.Hour).UnixMilli(),
	}
	notExpired := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(1 * time.Hour).UnixMilli(),
	}

	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "expired-1", expired1))
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "expired-2", expired2))
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "not-expired", notExpired))

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperBatchSize(100),
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)

	count := reaper.ReapNow(ctx)
	require.Equal(t, 2, count)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.Equal(t, []string{"not-expired"}, keys)
}

func TestEnvelopeReaper_DecrementsBlobRefs(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	hash := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: hash, Size: 100}))

	now := time.Now()
	env := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-1 * time.Hour).UnixMilli(),
		BlobRefs:        []string{hash},
	}
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "pkg-with-ref", env))

	blob, err := db.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, 1, blob.RefCount)

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)
	count := reaper.ReapNow(ctx)
	require.Equal(t, 1, count)

	blob, err = db.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, 0, blob.RefCount)
}

func TestEnvelopeReaper_BatchProcessing(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	now := time.Now()

	for i := 0; i < 25; i++ {
		env := &MetadataEnvelope{
			EnvelopeVersion: 1,
			ContentType:     ContentType_CONTENT_TYPE_JSON,
			Payload:         []byte(`{}`),
			ExpiresAtUnixMs: now.Add(-time.Duration(i+1) * time.Minute).UnixMilli(),
		}
		require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", string(rune('a'+i)), env))
	}

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperBatchSize(10),
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)

	count := reaper.ReapNow(ctx)
	require.Equal(t, 25, count)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.Empty(t, keys)

	stats := reaper.Stats()
	require.Equal(t, 25, stats.LastReapCount)
	require.Equal(t, int64(25), stats.TotalReaped)
}

func TestEnvelopeReaper_MaxDurationLimit(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	now := time.Now()

	for i := 0; i < 100; i++ {
		env := &MetadataEnvelope{
			EnvelopeVersion: 1,
			ContentType:     ContentType_CONTENT_TYPE_JSON,
			Payload:         []byte(`{}`),
			ExpiresAtUnixMs: now.Add(-time.Duration(i+1) * time.Minute).UnixMilli(),
		}
		require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", string(rune('a'+i%26))+string(rune('0'+i/26)), env))
	}

	callCount := atomic.Int32{}
	mockNow := func() time.Time {
		count := callCount.Add(1)
		if count > 3 {
			return now.Add(time.Minute)
		}
		return now
	}

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperBatchSize(10),
		WithEnvelopeReaperMaxDuration(30*time.Second),
		WithEnvelopeReaperNow(mockNow),
	)

	reaper.reapCycle(ctx)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.NotEmpty(t, keys, "should have stopped before processing all")
	require.Less(t, len(keys), 100, "should have processed some")
}

func TestEnvelopeReaper_NoExpiredEntries(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	now := time.Now()
	env := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(1 * time.Hour).UnixMilli(),
	}
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "pkg", env))

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)

	count := reaper.ReapNow(ctx)
	require.Equal(t, 0, count)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.Equal(t, []string{"pkg"}, keys)
}

func TestEnvelopeReaper_NoExpiryEntries(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	env := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: 0,
	}
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "no-expiry", env))

	reaper := NewEnvelopeReaper(db)
	count := reaper.ReapNow(ctx)
	require.Equal(t, 0, count)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.Equal(t, []string{"no-expiry"}, keys)
}

func TestEnvelopeReaper_MultipleProtocolsAndKinds(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	now := time.Now()

	expired := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-1 * time.Hour).UnixMilli(),
	}
	notExpired := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(1 * time.Hour).UnixMilli(),
	}

	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "exp-npm", expired))
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "keep-npm", notExpired))
	require.NoError(t, db.PutEnvelope(ctx, "pypi", "project", "exp-pypi", expired))
	require.NoError(t, db.PutEnvelope(ctx, "pypi", "project", "keep-pypi", notExpired))
	require.NoError(t, db.PutEnvelope(ctx, "goproxy", "mod", "exp-mod", expired))
	require.NoError(t, db.PutEnvelope(ctx, "goproxy", "info", "exp-info", expired))
	require.NoError(t, db.PutEnvelope(ctx, "goproxy", "info", "keep-info", notExpired))

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)
	count := reaper.ReapNow(ctx)
	require.Equal(t, 4, count)

	npmKeys, _ := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.Equal(t, []string{"keep-npm"}, npmKeys)

	pypiKeys, _ := db.ListEnvelopeKeys(ctx, "pypi", "project")
	require.Equal(t, []string{"keep-pypi"}, pypiKeys)

	modKeys, _ := db.ListEnvelopeKeys(ctx, "goproxy", "mod")
	require.Empty(t, modKeys)

	infoKeys, _ := db.ListEnvelopeKeys(ctx, "goproxy", "info")
	require.Equal(t, []string{"keep-info"}, infoKeys)
}

func TestEnvelopeReaper_RunLoop(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())

	now := time.Now()
	env := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-1 * time.Hour).UnixMilli(),
	}
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "expired", env))

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperInterval(10*time.Millisecond),
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)

	done := make(chan struct{})
	go func() {
		reaper.Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.Empty(t, keys, "reaper should have deleted expired entry")

	cancel()
	<-done
}

func TestEnvelopeReaper_Stats(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperInterval(time.Minute),
		WithEnvelopeReaperBatchSize(50),
	)

	stats := reaper.Stats()
	require.Equal(t, time.Minute, stats.Interval)
	require.Equal(t, 50, stats.BatchSize)
	require.True(t, stats.LastReapTime.IsZero())
	require.Equal(t, 0, stats.LastReapCount)
	require.Equal(t, int64(0), stats.TotalReaped)

	now := time.Now()
	env := &MetadataEnvelope{
		EnvelopeVersion: 1,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-time.Hour).UnixMilli(),
	}
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "pkg", env))

	_ = reaper.ReapNow(ctx)

	stats = reaper.Stats()
	require.False(t, stats.LastReapTime.IsZero())
	require.Equal(t, 1, stats.LastReapCount)
	require.Equal(t, int64(1), stats.TotalReaped)
}

func TestEnvelopeReaper_UpdatedExpiryIsRespected(t *testing.T) {
	db := setupReaperTestDB(t)
	ctx := context.Background()

	now := time.Now()

	env := &MetadataEnvelope{
		EnvelopeVersion: 1,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		Payload:         []byte(`{}`),
		ExpiresAtUnixMs: now.Add(-1 * time.Hour).UnixMilli(),
	}
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "pkg", env))

	env.ExpiresAtUnixMs = now.Add(1 * time.Hour).UnixMilli()
	require.NoError(t, db.PutEnvelope(ctx, "npm", "metadata", "pkg", env))

	reaper := NewEnvelopeReaper(db,
		WithEnvelopeReaperNow(func() time.Time { return now }),
	)
	count := reaper.ReapNow(ctx)
	require.Equal(t, 0, count)

	keys, err := db.ListEnvelopeKeys(ctx, "npm", "metadata")
	require.NoError(t, err)
	require.Equal(t, []string{"pkg"}, keys)
}
