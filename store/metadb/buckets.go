package metadb

import (
	"encoding/binary"
	"time"
)

// Bucket names for bbolt storage.
var (
	// Protocol metadata buckets - nested structure: protocol -> key -> data
	bucketMeta = []byte("meta")

	// Blob tracking buckets
	bucketBlobsByHash   = []byte("blobs_by_hash")   // hash -> BlobEntry JSON
	bucketBlobsByAccess = []byte("blobs_by_access") // timestamp+hash -> hash (LRU index)

	// Protocol metadata expiry index
	bucketMetaByExpiry    = []byte("meta_by_expiry")     // timestamp+protocol+key -> protocol+key
	bucketMetaExpiryByKey = []byte("meta_expiry_by_key") // protocol+key -> 8-byte timestamp (reverse index for O(1) delete)

	// Blob access reverse index
	bucketBlobAccessByHash = []byte("blob_access_by_hash") // hash -> 8-byte timestamp (reverse index for O(1) delete)

	// Meta blob refs bucket - tracks which blobs are referenced by each meta key
	bucketMetaBlobRefs = []byte("meta_blob_refs") // protocol+key -> JSON array of hashes

	// Envelope storage buckets (new 3-part key format: protocol|kind|key)
	bucketEnvelopes           = []byte("envelopes")              // protocol|kind|key -> protobuf MetadataEnvelope
	bucketEnvelopeByExpiry    = []byte("envelope_by_expiry")     // timestamp|protocol|kind|key -> protocol|kind|key
	bucketEnvelopeExpiryByKey = []byte("envelope_expiry_by_key") // protocol|kind|key -> 8-byte timestamp (reverse index)
	bucketEnvelopeBlobRefs    = []byte("envelope_blob_refs")     // protocol|kind|key -> JSON array of hashes
)

// encodeTimestamp converts a time.Time to a fixed-width big-endian byte slice.
// This ensures correct lexicographic ordering for time-based indexes.
// Uses an offset to handle negative nanosecond values (pre-1970 dates).
func encodeTimestamp(t time.Time) []byte {
	buf := make([]byte, 8)
	ns := t.UnixNano()
	// Offset by math.MinInt64 to convert signed to unsigned while preserving order.
	// The conversion is safe: we shift the range [MinInt64, MaxInt64] to [0, MaxUint64].
	binary.BigEndian.PutUint64(buf, uint64(ns-(-1<<63))) //nolint:gosec // intentional signed->unsigned shift
	return buf
}

// decodeTimestamp converts a big-endian byte slice back to time.Time.
func decodeTimestamp(b []byte) time.Time {
	if len(b) < 8 {
		return time.Time{}
	}
	u := binary.BigEndian.Uint64(b[:8])
	// Reverse the offset to get back the original nanoseconds.
	// The conversion is safe: we reverse the shift from [0, MaxUint64] back to [MinInt64, MaxInt64].
	ns := int64(u) + (-1 << 63) //nolint:gosec // intentional unsigned->signed shift
	return time.Unix(0, ns).UTC()
}

// makeBlobAccessKey creates a key for the blobs_by_access index.
// Format: [8-byte timestamp][hash string]
func makeBlobAccessKey(accessTime time.Time, hash string) []byte {
	ts := encodeTimestamp(accessTime)
	key := make([]byte, 8+len(hash))
	copy(key[:8], ts)
	copy(key[8:], hash)
	return key
}

// makeMetaExpiryKey creates a key for the meta_by_expiry index.
// Format: [8-byte timestamp][protocol][separator][key]
func makeMetaExpiryKey(expiresAt time.Time, protocol, key string) []byte {
	ts := encodeTimestamp(expiresAt)
	result := make([]byte, 8+len(protocol)+1+len(key))
	copy(result[:8], ts)
	copy(result[8:], protocol)
	result[8+len(protocol)] = 0 // null separator
	copy(result[8+len(protocol)+1:], key)
	return result
}

// parseMetaExpiryKey extracts protocol and key from a meta_by_expiry index key.
func parseMetaExpiryKey(data []byte) (expiresAt time.Time, protocol, key string) {
	if len(data) < 9 {
		return time.Time{}, "", ""
	}
	expiresAt = decodeTimestamp(data[:8])

	rest := data[8:]
	for i, b := range rest {
		if b == 0 {
			protocol = string(rest[:i])
			key = string(rest[i+1:])
			return
		}
	}
	return expiresAt, string(rest), ""
}

// makeProtocolKey creates a compound key for protocol metadata (legacy 2-part format).
// Format: [protocol][separator][key]
func makeProtocolKey(protocol, key string) []byte {
	result := make([]byte, len(protocol)+1+len(key))
	copy(result, protocol)
	result[len(protocol)] = 0 // null separator
	copy(result[len(protocol)+1:], key)
	return result
}

// parseProtocolKey extracts protocol and key from a compound key (legacy 2-part format).
func parseProtocolKey(data []byte) (protocol, key string) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), string(data[i+1:])
		}
	}
	return string(data), ""
}

// makeEnvelopeKey creates a compound key for envelope metadata (new 3-part format).
// Format: [protocol][separator][kind][separator][key]
func makeEnvelopeKey(protocol, kind, key string) []byte {
	result := make([]byte, len(protocol)+1+len(kind)+1+len(key))
	offset := 0
	copy(result[offset:], protocol)
	offset += len(protocol)
	result[offset] = 0 // null separator
	offset++
	copy(result[offset:], kind)
	offset += len(kind)
	result[offset] = 0 // null separator
	offset++
	copy(result[offset:], key)
	return result
}

// parseEnvelopeKey extracts protocol, kind, and key from a compound key (new 3-part format).
func parseEnvelopeKey(data []byte) (protocol, kind, key string) {
	separators := 0
	start := 0
	for i, b := range data {
		if b == 0 {
			switch separators {
			case 0:
				protocol = string(data[start:i])
			case 1:
				kind = string(data[start:i])
				key = string(data[i+1:])
				return
			}
			separators++
			start = i + 1
		}
	}
	return string(data), "", ""
}

// makeEnvelopeExpiryKey creates a key for the envelope expiry index.
// Format: [8-byte timestamp][protocol][separator][kind][separator][key]
func makeEnvelopeExpiryKey(expiresAt time.Time, protocol, kind, key string) []byte {
	ts := encodeTimestamp(expiresAt)
	result := make([]byte, 8+len(protocol)+1+len(kind)+1+len(key))
	copy(result[:8], ts)
	offset := 8
	copy(result[offset:], protocol)
	offset += len(protocol)
	result[offset] = 0
	offset++
	copy(result[offset:], kind)
	offset += len(kind)
	result[offset] = 0
	offset++
	copy(result[offset:], key)
	return result
}

// parseEnvelopeExpiryKey extracts expiry time and key parts from an envelope expiry key.
func parseEnvelopeExpiryKey(data []byte) (expiresAt time.Time, protocol, kind, key string) {
	if len(data) < 9 {
		return time.Time{}, "", "", ""
	}
	expiresAt = decodeTimestamp(data[:8])
	protocol, kind, key = parseEnvelopeKey(data[8:])
	return
}
