package s3fifo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func openTestDB(t *testing.T) *bbolt.DB {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "s3fifo-*.db")
	require.NoError(t, err)
	f.Close()

	db, err := bbolt.Open(f.Name(), 0o600, &bbolt.Options{NoSync: true})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func newTestQueues(t *testing.T) *Queues {
	t.Helper()
	q, err := NewQueues(openTestDB(t))
	require.NoError(t, err)
	return q
}

func TestPushHeadPopTailFIFO(t *testing.T) {
	q := newTestQueues(t)

	require.NoError(t, q.PushHead(QueueSmall, "a"))
	require.NoError(t, q.PushHead(QueueSmall, "b"))
	require.NoError(t, q.PushHead(QueueSmall, "c"))

	// Tail is oldest (first inserted).
	got, err := q.PopTail(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, "a", got)

	got, err = q.PopTail(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, "b", got)

	got, err = q.PopTail(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, "c", got)

	// Queue is now empty.
	_, err = q.PopTail(QueueSmall)
	require.ErrorIs(t, err, ErrQueueEmpty)
}

func TestPopTailEmptyQueue(t *testing.T) {
	q := newTestQueues(t)
	_, err := q.PopTail(QueueMain)
	require.ErrorIs(t, err, ErrQueueEmpty)
}

func TestRemove(t *testing.T) {
	q := newTestQueues(t)

	require.NoError(t, q.PushHead(QueueSmall, "x"))
	require.NoError(t, q.PushHead(QueueSmall, "y"))
	require.NoError(t, q.PushHead(QueueSmall, "z"))

	removed, err := q.Remove(QueueSmall, "y")
	require.NoError(t, err)
	require.True(t, removed)

	// "y" is gone; "x" and "z" remain in FIFO order.
	got, err := q.PopTail(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, "x", got)

	got, err = q.PopTail(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, "z", got)
}

func TestRemoveNotPresent(t *testing.T) {
	q := newTestQueues(t)
	removed, err := q.Remove(QueueSmall, "missing")
	require.NoError(t, err)
	require.False(t, removed)
}

func TestLen(t *testing.T) {
	q := newTestQueues(t)

	n, err := q.Len(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	require.NoError(t, q.PushHead(QueueSmall, "a"))
	require.NoError(t, q.PushHead(QueueSmall, "b"))

	n, err = q.Len(QueueSmall)
	require.NoError(t, err)
	require.Equal(t, 2, n)
}

func TestGhostContainsAddRemove(t *testing.T) {
	q := newTestQueues(t)

	found, err := q.GhostContains("h1")
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, q.GhostAdd("h1"))
	require.NoError(t, q.GhostAdd("h2"))

	found, err = q.GhostContains("h1")
	require.NoError(t, err)
	require.True(t, found)

	require.NoError(t, q.GhostRemove("h1"))

	found, err = q.GhostContains("h1")
	require.NoError(t, err)
	require.False(t, found)
}

func TestGhostTrimToMaxSize(t *testing.T) {
	q := newTestQueues(t)

	for _, h := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, q.GhostAdd(h))
	}
	n, err := q.GhostLen()
	require.NoError(t, err)
	require.Equal(t, 5, n)

	require.NoError(t, q.GhostTrimToMaxSize(3))

	n, err = q.GhostLen()
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// Oldest entries ("a" and "b") should have been removed.
	for _, evicted := range []string{"a", "b"} {
		found, err := q.GhostContains(evicted)
		require.NoError(t, err)
		require.False(t, found, "expected %q evicted from ghost", evicted)
	}
	for _, kept := range []string{"c", "d", "e"} {
		found, err := q.GhostContains(kept)
		require.NoError(t, err)
		require.True(t, found, "expected %q kept in ghost", kept)
	}
}

func TestAdmitGhostHit(t *testing.T) {
	q := newTestQueues(t)

	// Simulate an eviction: hash ends up in ghost.
	require.NoError(t, q.GhostAdd("gh1"))

	// On re-admission (ghost hit): remove from ghost, add to main.
	require.NoError(t, q.AdmitGhostHit("gh1"))

	found, err := q.GhostContains("gh1")
	require.NoError(t, err)
	require.False(t, found, "ghost hit should remove from ghost")

	n, err := q.Len(QueueMain)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestForEach(t *testing.T) {
	q := newTestQueues(t)

	for _, h := range []string{"x", "y", "z"} {
		require.NoError(t, q.PushHead(QueueSmall, h))
	}

	var got []string
	err := q.ForEach(QueueSmall, func(hash string) error {
		got = append(got, hash)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"x", "y", "z"}, got)
}
