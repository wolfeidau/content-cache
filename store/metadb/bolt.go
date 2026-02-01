package metadb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
)

// BoltDB implements MetaDB using bbolt.
type BoltDB struct {
	db     *bbolt.DB
	logger *slog.Logger
	now    func() time.Time
	noSync bool // disables fsync per transaction (for testing only)
}

// BoltDBOption configures a BoltDB instance.
type BoltDBOption func(*BoltDB)

// WithLogger sets the logger for the database.
func WithLogger(logger *slog.Logger) BoltDBOption {
	return func(b *BoltDB) {
		b.logger = logger
	}
}

// WithNow sets the time function for testing.
func WithNow(now func() time.Time) BoltDBOption {
	return func(b *BoltDB) {
		b.now = now
	}
}

// WithNoSync disables fsync per transaction.
// WARNING: This improves write performance but risks data loss on crash.
// Use only for testing or benchmarking, never in production.
func WithNoSync(noSync bool) BoltDBOption {
	return func(b *BoltDB) {
		b.noSync = noSync
	}
}

// NewBoltDB creates a new BoltDB instance with options.
func NewBoltDB(opts ...BoltDBOption) *BoltDB {
	b := &BoltDB{
		logger: slog.Default(),
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Open opens the database at the given path.
func (b *BoltDB) Open(path string) error {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
		NoSync:  b.noSync,
	})
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	b.db = db

	if err := b.createBuckets(); err != nil {
		_ = db.Close()
		return err
	}

	b.logger.Debug("opened metadb", "path", path, "noSync", b.noSync)
	return nil
}

func (b *BoltDB) createBuckets() error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		buckets := [][]byte{
			bucketMeta,
			bucketBlobsByHash,
			bucketBlobsByAccess,
			bucketMetaByExpiry,
			bucketMetaExpiryByKey,
			bucketBlobAccessByHash,
		}
		for _, name := range buckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("creating bucket %s: %w", name, err)
			}
		}
		return nil
	})
}

// Close closes the database.
func (b *BoltDB) Close() error {
	if b.db == nil {
		return nil
	}
	b.logger.Debug("closing metadb")
	return b.db.Close()
}

// GetMeta retrieves protocol metadata.
func (b *BoltDB) GetMeta(_ context.Context, protocol, key string) ([]byte, error) {
	var data []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketMeta)
		if bucket == nil {
			return ErrNotFound
		}

		compoundKey := makeProtocolKey(protocol, key)
		val := bucket.Get(compoundKey)
		if val == nil {
			return ErrNotFound
		}

		data = make([]byte, len(val))
		copy(data, val)
		return nil
	})
	return data, err
}

// PutMeta stores protocol metadata with TTL.
func (b *BoltDB) PutMeta(_ context.Context, protocol, key string, data []byte, ttl time.Duration) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket(bucketMeta)
		if metaBucket == nil {
			return fmt.Errorf("meta bucket not found")
		}

		compoundKey := makeProtocolKey(protocol, key)

		// Store the data
		if err := metaBucket.Put(compoundKey, data); err != nil {
			return fmt.Errorf("putting meta: %w", err)
		}

		// Update expiry index (removes old entry, adds new if ttl > 0)
		var expiresAt *time.Time
		if ttl > 0 {
			t := b.now().Add(ttl)
			expiresAt = &t
		}
		if err := b.updateMetaExpiryIndex(tx, protocol, key, expiresAt); err != nil {
			return err
		}

		return nil
	})
}

func (b *BoltDB) removeMetaExpiryIndex(tx *bbolt.Tx, protocol, key string) error {
	return b.updateMetaExpiryIndex(tx, protocol, key, nil)
}

// updateMetaExpiryIndex updates the meta expiry forward+reverse indexes.
// If expiresAt is nil, only deletes existing index entries.
func (b *BoltDB) updateMetaExpiryIndex(tx *bbolt.Tx, protocol, key string, expiresAt *time.Time) error {
	expiryBucket := tx.Bucket(bucketMetaByExpiry)
	if expiryBucket == nil {
		return nil
	}

	reverseIndexBucket := tx.Bucket(bucketMetaExpiryByKey)
	if reverseIndexBucket == nil {
		return nil
	}

	compoundKey := makeProtocolKey(protocol, key)

	// Step 1-2: Delete old forward index entry via reverse index lookup (O(1)), then delete reverse index
	if tsBytes := reverseIndexBucket.Get(compoundKey); tsBytes != nil {
		oldExpiresAt := decodeTimestamp(tsBytes)
		expiryKey := makeMetaExpiryKey(oldExpiresAt, protocol, key)
		if err := expiryBucket.Delete(expiryKey); err != nil {
			return fmt.Errorf("deleting old expiry index: %w", err)
		}
		if err := reverseIndexBucket.Delete(compoundKey); err != nil {
			return fmt.Errorf("deleting reverse index: %w", err)
		}
	} else {
		// Fallback: scan for legacy entries without reverse index (self-healing migration)
		cursor := expiryBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if bytes.Equal(v, compoundKey) {
				if err := cursor.Delete(); err != nil {
					return fmt.Errorf("deleting old expiry index: %w", err)
				}
				break
			}
		}
	}

	// Step 3-4: If new value provided, write new forward and reverse index entries
	if expiresAt != nil {
		expiryKey := makeMetaExpiryKey(*expiresAt, protocol, key)
		if err := expiryBucket.Put(expiryKey, compoundKey); err != nil {
			return fmt.Errorf("putting expiry index: %w", err)
		}
		if err := reverseIndexBucket.Put(compoundKey, encodeTimestamp(*expiresAt)); err != nil {
			return fmt.Errorf("putting expiry reverse index: %w", err)
		}
	}

	return nil
}

// DeleteMeta removes protocol metadata.
func (b *BoltDB) DeleteMeta(_ context.Context, protocol, key string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket(bucketMeta)
		if metaBucket == nil {
			return nil
		}

		compoundKey := makeProtocolKey(protocol, key)

		// Remove expiry index
		if err := b.removeMetaExpiryIndex(tx, protocol, key); err != nil {
			return err
		}

		return metaBucket.Delete(compoundKey)
	})
}

// ListMeta returns all keys for a protocol.
func (b *BoltDB) ListMeta(_ context.Context, protocol string) ([]string, error) {
	var keys []string
	prefix := []byte(protocol + "\x00")

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketMeta)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			_, key := parseProtocolKey(k)
			keys = append(keys, key)
		}
		return nil
	})
	return keys, err
}

// GetBlob retrieves blob metadata by hash.
func (b *BoltDB) GetBlob(_ context.Context, hash string) (*BlobEntry, error) {
	var entry BlobEntry
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketBlobsByHash)
		if bucket == nil {
			return ErrNotFound
		}

		val := bucket.Get([]byte(hash))
		if val == nil {
			return ErrNotFound
		}

		return json.Unmarshal(val, &entry)
	})
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// PutBlob stores blob metadata.
func (b *BoltDB) PutBlob(_ context.Context, entry *BlobEntry) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		hashBucket := tx.Bucket(bucketBlobsByHash)
		if hashBucket == nil {
			return fmt.Errorf("blobs_by_hash bucket not found")
		}

		// Store blob entry
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshaling blob entry: %w", err)
		}

		if err := hashBucket.Put([]byte(entry.Hash), data); err != nil {
			return fmt.Errorf("putting blob: %w", err)
		}

		// Update access index (removes old entry, adds new)
		if err := b.updateBlobAccessIndex(tx, entry.Hash, entry.LastAccess); err != nil {
			return err
		}

		return nil
	})
}

// deleteBlobAccessIndex removes blob access index entries for a hash.
func (b *BoltDB) deleteBlobAccessIndex(tx *bbolt.Tx, hash string) error {
	accessBucket := tx.Bucket(bucketBlobsByAccess)
	if accessBucket == nil {
		return nil
	}

	reverseIndexBucket := tx.Bucket(bucketBlobAccessByHash)
	hashBytes := []byte(hash)

	// Step 1-2: Delete old forward index entry via reverse index lookup (O(1)), then delete reverse index
	if reverseIndexBucket != nil {
		if tsBytes := reverseIndexBucket.Get(hashBytes); tsBytes != nil {
			lastAccess := decodeTimestamp(tsBytes)
			accessKey := makeBlobAccessKey(lastAccess, hash)
			if err := accessBucket.Delete(accessKey); err != nil {
				return fmt.Errorf("deleting old access index: %w", err)
			}
			if err := reverseIndexBucket.Delete(hashBytes); err != nil {
				return fmt.Errorf("deleting access reverse index: %w", err)
			}
			return nil
		}
	}

	// Fallback: scan for legacy entries without reverse index (self-healing migration)
	cursor := accessBucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if bytes.Equal(v, hashBytes) {
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("deleting old access index: %w", err)
			}
			break
		}
	}
	return nil
}

// updateBlobAccessIndex updates the blob access forward+reverse indexes.
// Must be called after the base blob entry is written (needs LastAccess from entry).
func (b *BoltDB) updateBlobAccessIndex(tx *bbolt.Tx, hash string, lastAccess time.Time) error {
	accessBucket := tx.Bucket(bucketBlobsByAccess)
	if accessBucket == nil {
		return fmt.Errorf("blobs_by_access bucket not found")
	}

	reverseIndexBucket := tx.Bucket(bucketBlobAccessByHash)
	if reverseIndexBucket == nil {
		return fmt.Errorf("blob_access_by_hash bucket not found")
	}

	hashBytes := []byte(hash)

	// Step 1-2: Delete old forward index entry via reverse index lookup (O(1)), then delete reverse index
	if tsBytes := reverseIndexBucket.Get(hashBytes); tsBytes != nil {
		oldLastAccess := decodeTimestamp(tsBytes)
		accessKey := makeBlobAccessKey(oldLastAccess, hash)
		if err := accessBucket.Delete(accessKey); err != nil {
			return fmt.Errorf("deleting old access index: %w", err)
		}
		if err := reverseIndexBucket.Delete(hashBytes); err != nil {
			return fmt.Errorf("deleting access reverse index: %w", err)
		}
	} else {
		// Fallback: scan for legacy entries without reverse index (self-healing migration)
		cursor := accessBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if bytes.Equal(v, hashBytes) {
				if err := cursor.Delete(); err != nil {
					return fmt.Errorf("deleting old access index: %w", err)
				}
				break
			}
		}
	}

	// Step 3-4: Write new forward and reverse index entries
	accessKey := makeBlobAccessKey(lastAccess, hash)
	if err := accessBucket.Put(accessKey, hashBytes); err != nil {
		return fmt.Errorf("putting access index: %w", err)
	}
	if err := reverseIndexBucket.Put(hashBytes, encodeTimestamp(lastAccess)); err != nil {
		return fmt.Errorf("putting access reverse index: %w", err)
	}

	return nil
}

// DeleteBlob removes blob metadata.
func (b *BoltDB) DeleteBlob(_ context.Context, hash string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		hashBucket := tx.Bucket(bucketBlobsByHash)
		if hashBucket == nil {
			return nil
		}

		// Remove access index
		if err := b.deleteBlobAccessIndex(tx, hash); err != nil {
			return err
		}

		return hashBucket.Delete([]byte(hash))
	})
}

// IncrementBlobRef increments the reference count for a blob.
func (b *BoltDB) IncrementBlobRef(ctx context.Context, hash string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketBlobsByHash)
		if bucket == nil {
			return ErrNotFound
		}

		val := bucket.Get([]byte(hash))
		if val == nil {
			return ErrNotFound
		}

		var entry BlobEntry
		if err := json.Unmarshal(val, &entry); err != nil {
			return fmt.Errorf("unmarshaling blob entry: %w", err)
		}

		entry.RefCount++

		data, err := json.Marshal(&entry)
		if err != nil {
			return fmt.Errorf("marshaling blob entry: %w", err)
		}

		return bucket.Put([]byte(hash), data)
	})
}

// DecrementBlobRef decrements the reference count for a blob.
func (b *BoltDB) DecrementBlobRef(ctx context.Context, hash string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketBlobsByHash)
		if bucket == nil {
			return ErrNotFound
		}

		val := bucket.Get([]byte(hash))
		if val == nil {
			return ErrNotFound
		}

		var entry BlobEntry
		if err := json.Unmarshal(val, &entry); err != nil {
			return fmt.Errorf("unmarshaling blob entry: %w", err)
		}

		if entry.RefCount > 0 {
			entry.RefCount--
		}

		data, err := json.Marshal(&entry)
		if err != nil {
			return fmt.Errorf("marshaling blob entry: %w", err)
		}

		return bucket.Put([]byte(hash), data)
	})
}

// TouchBlob updates the last access time for a blob.
func (b *BoltDB) TouchBlob(_ context.Context, hash string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		hashBucket := tx.Bucket(bucketBlobsByHash)
		if hashBucket == nil {
			return ErrNotFound
		}

		val := hashBucket.Get([]byte(hash))
		if val == nil {
			return ErrNotFound
		}

		var entry BlobEntry
		if err := json.Unmarshal(val, &entry); err != nil {
			return fmt.Errorf("unmarshaling blob entry: %w", err)
		}

		// Update access time
		entry.LastAccess = b.now()

		data, err := json.Marshal(&entry)
		if err != nil {
			return fmt.Errorf("marshaling blob entry: %w", err)
		}

		if err := hashBucket.Put([]byte(hash), data); err != nil {
			return fmt.Errorf("putting blob: %w", err)
		}

		// Update access index (removes old entry, adds new)
		if err := b.updateBlobAccessIndex(tx, hash, entry.LastAccess); err != nil {
			return err
		}

		return nil
	})
}

// TotalBlobSize returns the total size of all blobs.
func (b *BoltDB) TotalBlobSize(_ context.Context) (int64, error) {
	var total int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketBlobsByHash)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(_, v []byte) error {
			var entry BlobEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return nil // Skip invalid entries
			}
			total += entry.Size
			return nil
		})
	})
	return total, err
}

// GetExpiredMeta returns metadata entries that have expired before the given time.
func (b *BoltDB) GetExpiredMeta(_ context.Context, before time.Time, limit int) ([]ExpiryEntry, error) {
	var entries []ExpiryEntry
	beforeTs := encodeTimestamp(before)

	err := b.db.View(func(tx *bbolt.Tx) error {
		expiryBucket := tx.Bucket(bucketMetaByExpiry)
		if expiryBucket == nil {
			return nil
		}

		metaBucket := tx.Bucket(bucketMeta)

		cursor := expiryBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			// Keys are sorted by timestamp, so stop when we pass the cutoff
			if bytes.Compare(k[:8], beforeTs) >= 0 {
				break
			}

			if limit > 0 && len(entries) >= limit {
				break
			}

			expiresAt, protocol, key := parseMetaExpiryKey(k)

			entry := ExpiryEntry{
				Protocol:  protocol,
				Key:       key,
				ExpiresAt: expiresAt,
			}

			// Try to get additional metadata
			if metaBucket != nil {
				if data := metaBucket.Get(v); data != nil {
					entry.Size = int64(len(data))
				}
			}

			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

// GetLRUBlobs returns the least recently used blobs.
func (b *BoltDB) GetLRUBlobs(_ context.Context, limit int) ([]BlobEntry, error) {
	var entries []BlobEntry

	err := b.db.View(func(tx *bbolt.Tx) error {
		accessBucket := tx.Bucket(bucketBlobsByAccess)
		if accessBucket == nil {
			return nil
		}

		hashBucket := tx.Bucket(bucketBlobsByHash)
		if hashBucket == nil {
			return nil
		}

		cursor := accessBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if limit > 0 && len(entries) >= limit {
				break
			}

			val := hashBucket.Get(v)
			if val == nil {
				continue
			}

			var entry BlobEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				continue
			}

			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

// GetUnreferencedBlobs returns blobs with RefCount == 0.
func (b *BoltDB) GetUnreferencedBlobs(_ context.Context, limit int) ([]string, error) {
	var hashes []string

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketBlobsByHash)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(k, v []byte) error {
			if limit > 0 && len(hashes) >= limit {
				return nil
			}

			var entry BlobEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return nil // Skip invalid entries
			}

			if entry.RefCount == 0 {
				hashes = append(hashes, string(k))
			}
			return nil
		})
	})
	return hashes, err
}

// Compile-time interface check
var _ MetaDB = (*BoltDB)(nil)
