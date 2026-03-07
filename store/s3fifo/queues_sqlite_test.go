package s3fifo

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/content-cache/store/metadb"
)

func newTestSQLiteQueues(t *testing.T) *SQLiteQueues {
	t.Helper()
	db := metadb.NewSQLiteDB()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, db.Open(dbPath))
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLiteQueues(db.DB())
}

func TestSQLiteQueues_PushPopTail(t *testing.T) {
	q := newTestSQLiteQueues(t)

	require.NoError(t, q.PushHead(QueueSmall, "hash1"))
	require.NoError(t, q.PushHead(QueueSmall, "hash2"))
	require.NoError(t, q.PushHead(QueueSmall, "hash3"))

	// FIFO: first pushed is first popped
	got, err := q.PopTail(QueueSmall)
	require.NoError(t, err)
	assert.Equal(t, "hash1", got)

	got, err = q.PopTail(QueueSmall)
	require.NoError(t, err)
	assert.Equal(t, "hash2", got)
}

func TestSQLiteQueues_PopTailEmpty(t *testing.T) {
	q := newTestSQLiteQueues(t)
	_, err := q.PopTail(QueueSmall)
	require.ErrorIs(t, err, ErrQueueEmpty)
}

func TestSQLiteQueues_Remove(t *testing.T) {
	q := newTestSQLiteQueues(t)
	require.NoError(t, q.PushHead(QueueSmall, "hash1"))
	require.NoError(t, q.PushHead(QueueSmall, "hash2"))

	removed, err := q.Remove(QueueSmall, "hash1")
	require.NoError(t, err)
	assert.True(t, removed)

	removed, err = q.Remove(QueueSmall, "hash1")
	require.NoError(t, err)
	assert.False(t, removed)

	n, err := q.Len(QueueSmall)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestSQLiteQueues_Len(t *testing.T) {
	q := newTestSQLiteQueues(t)

	n, err := q.Len(QueueSmall)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	require.NoError(t, q.PushHead(QueueSmall, "a"))
	require.NoError(t, q.PushHead(QueueSmall, "b"))

	n, err = q.Len(QueueSmall)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestSQLiteQueues_ForEach(t *testing.T) {
	q := newTestSQLiteQueues(t)
	require.NoError(t, q.PushHead(QueueSmall, "a"))
	require.NoError(t, q.PushHead(QueueSmall, "b"))
	require.NoError(t, q.PushHead(QueueSmall, "c"))

	var seen []string
	require.NoError(t, q.ForEach(QueueSmall, func(h string) error {
		seen = append(seen, h)
		return nil
	}))
	assert.Equal(t, []string{"a", "b", "c"}, seen)
}

func TestSQLiteQueues_PushHeadIgnoresDuplicate(t *testing.T) {
	q := newTestSQLiteQueues(t)
	require.NoError(t, q.PushHead(QueueSmall, "dup"))
	require.NoError(t, q.PushHead(QueueSmall, "dup"))

	n, err := q.Len(QueueSmall)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestSQLiteQueues_Ghost(t *testing.T) {
	q := newTestSQLiteQueues(t)

	ok, err := q.GhostContains("h1")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, q.GhostAdd("h1"))
	require.NoError(t, q.GhostAdd("h2"))
	require.NoError(t, q.GhostAdd("h1")) // duplicate — no-op

	ok, err = q.GhostContains("h1")
	require.NoError(t, err)
	assert.True(t, ok)

	n, err := q.GhostLen()
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	require.NoError(t, q.GhostRemove("h1"))
	ok, err = q.GhostContains("h1")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSQLiteQueues_GhostTrimToMaxSize(t *testing.T) {
	q := newTestSQLiteQueues(t)

	for _, h := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, q.GhostAdd(h))
	}

	require.NoError(t, q.GhostTrimToMaxSize(3))

	n, err := q.GhostLen()
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	// Verify the oldest entries were evicted (a, b removed; c, d, e kept)
	for _, h := range []string{"a", "b"} {
		ok, err := q.GhostContains(h)
		require.NoError(t, err)
		assert.False(t, ok, "expected %s to be evicted", h)
	}
	for _, h := range []string{"c", "d", "e"} {
		ok, err := q.GhostContains(h)
		require.NoError(t, err)
		assert.True(t, ok, "expected %s to be retained", h)
	}
}

func TestSQLiteQueues_GhostTrimToMaxSizeNoop(t *testing.T) {
	q := newTestSQLiteQueues(t)
	require.NoError(t, q.GhostAdd("a"))
	require.NoError(t, q.GhostAdd("b"))

	// Fewer entries than max — no-op
	require.NoError(t, q.GhostTrimToMaxSize(10))

	n, err := q.GhostLen()
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestSQLiteQueues_AdmitGhostHit(t *testing.T) {
	q := newTestSQLiteQueues(t)

	require.NoError(t, q.GhostAdd("h1"))

	require.NoError(t, q.AdmitGhostHit("h1"))

	// Should be removed from ghost and added to main queue
	ok, err := q.GhostContains("h1")
	require.NoError(t, err)
	assert.False(t, ok)

	n, err := q.Len(QueueMain)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	got, err := q.PopTail(QueueMain)
	require.NoError(t, err)
	assert.Equal(t, "h1", got)
}

func TestSQLiteQueues_AdmitGhostHitMissing(t *testing.T) {
	q := newTestSQLiteQueues(t)
	// Not in ghost — should be a no-op
	require.NoError(t, q.AdmitGhostHit("missing"))

	n, err := q.Len(QueueMain)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
