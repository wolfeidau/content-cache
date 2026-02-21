// Package s3fifo implements the S3-FIFO cache eviction algorithm.
// See: https://www.pdl.cmu.edu/ftp/Storage/CMU-CS-24-149-juncheny.pdf
package s3fifo

import (
	"encoding/binary"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

// ErrQueueEmpty is returned when popping from an empty queue.
var ErrQueueEmpty = errors.New("s3fifo: queue is empty")

// Queue name constants.
const (
	QueueSmall = "small"
	QueueMain  = "main"
)

// bbolt bucket names for S3-FIFO state. All are prefixed with "s3fifo_" to
// avoid collisions with metadb buckets in the same database file.
var (
	bucketSmall       = []byte("s3fifo_small")         // seq(uint64BE) → hash
	bucketSmallByHash = []byte("s3fifo_small_by_hash") // hash → seq(uint64BE)
	bucketMain        = []byte("s3fifo_main")           // seq(uint64BE) → hash
	bucketMainByHash  = []byte("s3fifo_main_by_hash")  // hash → seq(uint64BE)
	bucketGhost       = []byte("s3fifo_ghost")          // hash → seq(uint64BE)
	bucketGhostBySeq  = []byte("s3fifo_ghost_by_seq")  // seq(uint64BE) → hash
)

// Queues provides bbolt-backed FIFO queue and ghost set operations for S3-FIFO.
// All methods are safe to call concurrently (bbolt handles its own locking),
// but the S3-FIFO Manager serialises mutation calls with its own mutex.
type Queues struct {
	db *bbolt.DB
}

// NewQueues creates a Queues instance backed by the given bbolt database and
// ensures all required buckets exist.
func NewQueues(db *bbolt.DB) (*Queues, error) {
	q := &Queues{db: db}
	return q, q.createBuckets()
}

func (q *Queues) createBuckets() error {
	return q.db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{
			bucketSmall, bucketSmallByHash,
			bucketMain, bucketMainByHash,
			bucketGhost, bucketGhostBySeq,
		} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("creating bucket %s: %w", name, err)
			}
		}
		return nil
	})
}

// PushHead inserts hash at the head (newest position) of the named queue.
// Subsequent PopTail calls return older entries first.
func (q *Queues) PushHead(queue, hash string) error {
	return q.db.Update(func(tx *bbolt.Tx) error {
		return txPushHead(tx, queueFwdName(queue), queueRevName(queue), hash)
	})
}

// PopTail removes and returns the oldest hash from the named queue.
// Returns ErrQueueEmpty when the queue has no entries.
func (q *Queues) PopTail(queue string) (string, error) {
	var hash string
	err := q.db.Update(func(tx *bbolt.Tx) error {
		var err error
		hash, err = txPopTail(tx, queueFwdName(queue), queueRevName(queue))
		return err
	})
	return hash, err
}

// Remove removes a specific hash from the named queue.
// Returns (true, nil) if the hash was present and removed, (false, nil) if absent.
func (q *Queues) Remove(queue, hash string) (bool, error) {
	var removed bool
	err := q.db.Update(func(tx *bbolt.Tx) error {
		fwd := tx.Bucket(queueFwdName(queue))
		rev := tx.Bucket(queueRevName(queue))

		seqVal := rev.Get([]byte(hash))
		if seqVal == nil {
			return nil
		}
		removed = true

		seqKey := make([]byte, len(seqVal))
		copy(seqKey, seqVal)

		if err := fwd.Delete(seqKey); err != nil {
			return err
		}
		return rev.Delete([]byte(hash))
	})
	return removed, err
}

// Len returns the number of entries in the named queue.
func (q *Queues) Len(queue string) (int, error) {
	var count int
	err := q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(queueFwdName(queue))
		if b != nil {
			count = b.Stats().KeyN
		}
		return nil
	})
	return count, err
}

// ForEach iterates all entries in a queue in FIFO order (oldest first) within a
// read-only transaction. fn must not perform any bbolt writes.
func (q *Queues) ForEach(queue string, fn func(hash string) error) error {
	return q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(queueFwdName(queue))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			return fn(string(v))
		})
	})
}

// AdmitGhostHit atomically removes hash from the ghost set and inserts it at
// the head of the main queue. Called on a ghost cache hit to bypass small queue.
func (q *Queues) AdmitGhostHit(hash string) error {
	return q.db.Update(func(tx *bbolt.Tx) error {
		ghostBucket := tx.Bucket(bucketGhost)
		seqBucket := tx.Bucket(bucketGhostBySeq)

		seqVal := ghostBucket.Get([]byte(hash))
		if seqVal == nil {
			// Not in ghost (possible race on restart); fall through to normal small admission.
			return nil
		}
		seqKey := make([]byte, len(seqVal))
		copy(seqKey, seqVal)

		if err := ghostBucket.Delete([]byte(hash)); err != nil {
			return err
		}
		if err := seqBucket.Delete(seqKey); err != nil {
			return err
		}

		return txPushHead(tx, bucketMain, bucketMainByHash, hash)
	})
}

// GhostContains reports whether hash is currently in the ghost set.
func (q *Queues) GhostContains(hash string) (bool, error) {
	var found bool
	err := q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketGhost)
		if b != nil {
			found = b.Get([]byte(hash)) != nil
		}
		return nil
	})
	return found, err
}

// GhostAdd inserts hash into the ghost set with the next sequence number.
func (q *Queues) GhostAdd(hash string) error {
	return q.db.Update(func(tx *bbolt.Tx) error {
		ghostBucket := tx.Bucket(bucketGhost)
		seqBucket := tx.Bucket(bucketGhostBySeq)

		seq, err := seqBucket.NextSequence()
		if err != nil {
			return err
		}
		seqKey := encodeSeq(seq)

		if err := ghostBucket.Put([]byte(hash), seqKey); err != nil {
			return err
		}
		return seqBucket.Put(seqKey, []byte(hash))
	})
}

// GhostRemove removes hash from the ghost set. No-op if not present.
func (q *Queues) GhostRemove(hash string) error {
	return q.db.Update(func(tx *bbolt.Tx) error {
		ghostBucket := tx.Bucket(bucketGhost)
		seqBucket := tx.Bucket(bucketGhostBySeq)

		seqVal := ghostBucket.Get([]byte(hash))
		if seqVal == nil {
			return nil
		}
		seqKey := make([]byte, len(seqVal))
		copy(seqKey, seqVal)

		if err := ghostBucket.Delete([]byte(hash)); err != nil {
			return err
		}
		return seqBucket.Delete(seqKey)
	})
}

// GhostTrimToMaxSize evicts the oldest ghost entries until the count is at or
// below maxEntries. Each trim step is O(1) using the ordered seq index.
func (q *Queues) GhostTrimToMaxSize(maxEntries int) error {
	return q.db.Update(func(tx *bbolt.Tx) error {
		ghostBucket := tx.Bucket(bucketGhost)
		seqBucket := tx.Bucket(bucketGhostBySeq)

		// Stats().KeyN reflects committed state and does not update within the
		// current transaction, so we track the count with a local variable.
		count := seqBucket.Stats().KeyN
		for count > maxEntries {
			c := seqBucket.Cursor()
			seqKey, hashVal := c.First()
			if seqKey == nil {
				break
			}
			seqKeyCopy := make([]byte, len(seqKey))
			copy(seqKeyCopy, seqKey)
			hashCopy := make([]byte, len(hashVal))
			copy(hashCopy, hashVal)

			if err := seqBucket.Delete(seqKeyCopy); err != nil {
				return err
			}
			if err := ghostBucket.Delete(hashCopy); err != nil {
				return err
			}
			count--
		}
		return nil
	})
}

// GhostLen returns the current number of entries in the ghost set.
func (q *Queues) GhostLen() (int, error) {
	var count int
	err := q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketGhost)
		if b != nil {
			count = b.Stats().KeyN
		}
		return nil
	})
	return count, err
}

// --- internal helpers ---

// txPushHead inserts hash into the queue using the next monotonic sequence number.
// Must be called inside a bbolt Update transaction.
func txPushHead(tx *bbolt.Tx, fwdName, revName []byte, hash string) error {
	fwd := tx.Bucket(fwdName)
	rev := tx.Bucket(revName)

	seq, err := fwd.NextSequence()
	if err != nil {
		return err
	}
	seqKey := encodeSeq(seq)

	if err := fwd.Put(seqKey, []byte(hash)); err != nil {
		return err
	}
	return rev.Put([]byte(hash), seqKey)
}

// txPopTail removes and returns the entry with the lowest sequence number
// (oldest / FIFO tail). Must be called inside a bbolt Update transaction.
func txPopTail(tx *bbolt.Tx, fwdName, revName []byte) (string, error) {
	fwd := tx.Bucket(fwdName)
	rev := tx.Bucket(revName)

	c := fwd.Cursor()
	k, v := c.First()
	if k == nil {
		return "", ErrQueueEmpty
	}

	// Copy before any mutation invalidates the cursor memory.
	seqKey := make([]byte, len(k))
	copy(seqKey, k)
	hashVal := make([]byte, len(v))
	copy(hashVal, v)

	if err := fwd.Delete(seqKey); err != nil {
		return "", err
	}
	if err := rev.Delete(hashVal); err != nil {
		return "", err
	}
	return string(hashVal), nil
}

// queueFwdName returns the forward bucket name (seq→hash) for a named queue.
func queueFwdName(queue string) []byte {
	if queue == QueueSmall {
		return bucketSmall
	}
	return bucketMain
}

// queueRevName returns the reverse bucket name (hash→seq) for a named queue.
func queueRevName(queue string) []byte {
	if queue == QueueSmall {
		return bucketSmallByHash
	}
	return bucketMainByHash
}

// encodeSeq encodes a uint64 sequence number as a big-endian 8-byte slice
// for lexicographic ordering in bbolt.
func encodeSeq(seq uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	return buf
}
