package metadb

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

// Helper functions for inspecting bucket contents

// countBucketEntries counts the number of entries in a bucket.
func countBucketEntries(tx *bbolt.Tx, bucket []byte) int {
	b := tx.Bucket(bucket)
	if b == nil {
		return 0
	}
	count := 0
	_ = b.ForEach(func(_, _ []byte) error {
		count++
		return nil
	})
	return count
}

// getBucketEntriesForKey returns all keys in a bucket that contain the given substring.
func getBucketEntriesForKey(tx *bbolt.Tx, bucket []byte, keySubstring []byte) [][]byte {
	b := tx.Bucket(bucket)
	if b == nil {
		return nil
	}
	var keys [][]byte
	_ = b.ForEach(func(k, _ []byte) error {
		if bytes.Contains(k, keySubstring) {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
		}
		return nil
	})
	return keys
}

// getBucketEntriesForValue returns all keys in a bucket whose values contain the given substring.
func getBucketEntriesForValue(tx *bbolt.Tx, bucket []byte, valueSubstring []byte) [][]byte {
	b := tx.Bucket(bucket)
	if b == nil {
		return nil
	}
	var keys [][]byte
	_ = b.ForEach(func(k, v []byte) error {
		if bytes.Contains(v, valueSubstring) {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
		}
		return nil
	})
	return keys
}

func TestMetaExpiryIndex_SingleEntryAfterRepeatedUpdates(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	currentTime := baseTime
	db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

	protocol := "npm"
	key := "lodash@4.17.21"
	compoundKey := makeProtocolKey(protocol, key)

	// Put the same meta key 10 times with different TTLs
	for i := 0; i < 10; i++ {
		ttl := time.Duration(i+1) * time.Hour
		data := []byte(fmt.Sprintf(`{"version":%d}`, i))
		require.NoError(t, db.PutMeta(ctx, protocol, key, data, ttl))
		// Advance time slightly to ensure different expiry timestamps
		currentTime = currentTime.Add(time.Minute)
	}

	// Verify index consistency
	err := db.db.View(func(tx *bbolt.Tx) error {
		// Check meta_by_expiry bucket - should have exactly ONE entry for this key
		expiryEntries := getBucketEntriesForValue(tx, bucketMetaByExpiry, compoundKey)
		assert.Len(t, expiryEntries, 1, "should have exactly one entry in meta_by_expiry for this key")

		// Check meta_expiry_by_key bucket - should have exactly ONE entry for this key
		reverseEntries := getBucketEntriesForKey(tx, bucketMetaExpiryByKey, compoundKey)
		assert.Len(t, reverseEntries, 1, "should have exactly one entry in meta_expiry_by_key for this key")

		// Verify timestamps match between forward and reverse indexes
		if len(expiryEntries) == 1 && len(reverseEntries) == 1 {
			// Extract timestamp from forward index key
			forwardTs := decodeTimestamp(expiryEntries[0][:8])

			// Get timestamp from reverse index value
			reverseIndexBucket := tx.Bucket(bucketMetaExpiryByKey)
			require.NotNil(t, reverseIndexBucket)
			tsBytes := reverseIndexBucket.Get(compoundKey)
			require.NotNil(t, tsBytes)
			reverseTs := decodeTimestamp(tsBytes)

			assert.Equal(t, forwardTs.UnixNano(), reverseTs.UnixNano(), "timestamps in forward and reverse indexes should match")
		}

		return nil
	})
	require.NoError(t, err)
}

func TestMetaExpiryIndex_DeleteCleansUpIndexes(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	db := newTestBoltDB(t, WithNow(func() time.Time { return baseTime }))

	protocol := "npm"
	key := "express@4.18.0"
	compoundKey := makeProtocolKey(protocol, key)

	// Put meta with TTL
	require.NoError(t, db.PutMeta(ctx, protocol, key, []byte(`{"name":"express"}`), time.Hour))

	// Verify index entries exist before delete
	err := db.db.View(func(tx *bbolt.Tx) error {
		expiryEntries := getBucketEntriesForValue(tx, bucketMetaByExpiry, compoundKey)
		assert.Len(t, expiryEntries, 1, "should have expiry index entry before delete")

		reverseEntries := getBucketEntriesForKey(tx, bucketMetaExpiryByKey, compoundKey)
		assert.Len(t, reverseEntries, 1, "should have reverse index entry before delete")

		return nil
	})
	require.NoError(t, err)

	// Delete meta
	require.NoError(t, db.DeleteMeta(ctx, protocol, key))

	// Verify both forward and reverse index entries are removed
	err = db.db.View(func(tx *bbolt.Tx) error {
		expiryEntries := getBucketEntriesForValue(tx, bucketMetaByExpiry, compoundKey)
		assert.Empty(t, expiryEntries, "should have no expiry index entries after delete")

		reverseEntries := getBucketEntriesForKey(tx, bucketMetaExpiryByKey, compoundKey)
		assert.Empty(t, reverseEntries, "should have no reverse index entries after delete")

		return nil
	})
	require.NoError(t, err)
}

func TestIndexConsistency_AfterMixedOperations(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	currentTime := baseTime
	db := newTestBoltDB(t, WithNow(func() time.Time { return currentTime }))

	// Perform a mix of operations

	// Put some meta entries
	require.NoError(t, db.PutMeta(ctx, "npm", "pkg1", []byte("data1"), time.Hour))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutMeta(ctx, "npm", "pkg2", []byte("data2"), 2*time.Hour))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutMeta(ctx, "pypi", "pkg3", []byte("data3"), 3*time.Hour))

	// Update some entries (should not create duplicate index entries)
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutMeta(ctx, "npm", "pkg1", []byte("data1-updated"), 4*time.Hour))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutMeta(ctx, "npm", "pkg1", []byte("data1-updated-again"), 5*time.Hour))

	// Put some blobs
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blob1", Size: 100, CachedAt: currentTime, LastAccess: currentTime, RefCount: 1}))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blob2", Size: 200, CachedAt: currentTime, LastAccess: currentTime, RefCount: 2}))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, db.PutBlob(ctx, &BlobEntry{Hash: "blob3", Size: 300, CachedAt: currentTime, LastAccess: currentTime, RefCount: 0}))

	// Touch some blobs
	currentTime = currentTime.Add(time.Hour)
	_, touchErr := db.TouchBlob(ctx, "blob1")
	require.NoError(t, touchErr)
	currentTime = currentTime.Add(time.Hour)
	_, touchErr = db.TouchBlob(ctx, "blob1")
	require.NoError(t, touchErr)
	currentTime = currentTime.Add(time.Hour)
	_, touchErr = db.TouchBlob(ctx, "blob2")
	require.NoError(t, touchErr)

	// Delete some entries
	require.NoError(t, db.DeleteMeta(ctx, "npm", "pkg2"))
	require.NoError(t, db.DeleteBlob(ctx, "blob3"))

	// Verify consistency
	err := db.db.View(func(tx *bbolt.Tx) error {
		// Check meta expiry index consistency
		expiryBucket := tx.Bucket(bucketMetaByExpiry)
		reverseIndexBucket := tx.Bucket(bucketMetaExpiryByKey)
		require.NotNil(t, expiryBucket)
		require.NotNil(t, reverseIndexBucket)

		// Count entries in both buckets
		forwardMetaCount := countBucketEntries(tx, bucketMetaByExpiry)
		reverseMetaCount := countBucketEntries(tx, bucketMetaExpiryByKey)
		assert.Equal(t, forwardMetaCount, reverseMetaCount,
			"meta expiry forward and reverse index counts should match")

		// Verify every entry in forward index has a corresponding reverse index entry
		_ = expiryBucket.ForEach(func(k, v []byte) error {
			// v is the compound key (protocol+key)
			ts := reverseIndexBucket.Get(v)
			if !assert.NotNil(t, ts, "reverse index should exist for forward index entry") {
				return nil
			}
			// Verify timestamps match (compare as UnixNano to avoid timezone issues)
			forwardTs := decodeTimestamp(k[:8])
			reverseTs := decodeTimestamp(ts)
			assert.Equal(t, forwardTs.UnixNano(), reverseTs.UnixNano(), "timestamps should match for compound key %s", v)
			return nil
		})

		// Verify every entry in reverse index has a corresponding forward index entry
		_ = reverseIndexBucket.ForEach(func(compoundKey, ts []byte) error {
			// Look for forward index entry
			forwardFound := false
			_ = expiryBucket.ForEach(func(k, v []byte) error {
				if bytes.Equal(v, compoundKey) {
					forwardFound = true
					// Verify timestamp matches (compare as UnixNano to avoid timezone issues)
					forwardTs := decodeTimestamp(k[:8])
					reverseTs := decodeTimestamp(ts)
					assert.Equal(t, forwardTs.UnixNano(), reverseTs.UnixNano(), "timestamps should match")
				}
				return nil
			})
			assert.True(t, forwardFound, "forward index should exist for reverse index entry")
			return nil
		})

		return nil
	})
	require.NoError(t, err)

	// Final state assertions
	err = db.db.View(func(tx *bbolt.Tx) error {
		// Should have 2 meta entries (pkg1 and pkg3, pkg2 was deleted)
		metaCount := countBucketEntries(tx, bucketMeta)
		assert.Equal(t, 2, metaCount, "should have 2 meta entries")

		// Should have 2 blob entries (blob1 and blob2, blob3 was deleted)
		blobCount := countBucketEntries(tx, bucketBlobsByHash)
		assert.Equal(t, 2, blobCount, "should have 2 blob entries")

		return nil
	})
	require.NoError(t, err)
}
