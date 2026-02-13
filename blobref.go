package contentcache

import (
	"fmt"
	"strings"
)

// Algorithm identifies the hash algorithm used in a blob reference.
type Algorithm string

const (
	AlgBLAKE3 Algorithm = "blake3"
	AlgSHA256 Algorithm = "sha256"
)

// BlobRef is a content-addressed reference to a blob, combining an algorithm
// identifier with a hash digest.
type BlobRef struct {
	Alg  Algorithm
	Hash Hash
}

// NewBlobRef creates a BlobRef using the default BLAKE3 algorithm.
func NewBlobRef(h Hash) BlobRef {
	return BlobRef{Alg: AlgBLAKE3, Hash: h}
}

// ParseBlobRef parses a blob reference string in the form "algorithm:hex".
// The algorithm is case-insensitive and normalised to lowercase.
// Plain hex strings (without an algorithm prefix) are accepted as legacy
// and assumed to be BLAKE3.
func ParseBlobRef(s string) (BlobRef, error) {
	if s == "" {
		return BlobRef{}, fmt.Errorf("empty blob ref")
	}

	algoStr, hexStr, hasPrefix := strings.Cut(s, ":")
	if !hasPrefix {
		// Legacy plain hex — assume BLAKE3.
		hexStr = algoStr
		algoStr = string(AlgBLAKE3)
	}

	algoStr = strings.ToLower(algoStr)

	var alg Algorithm
	switch Algorithm(algoStr) {
	case AlgBLAKE3:
		alg = AlgBLAKE3
	case AlgSHA256:
		alg = AlgSHA256
	default:
		return BlobRef{}, fmt.Errorf("unsupported algorithm %q in blob ref %q", algoStr, s)
	}

	h, err := ParseHash(strings.ToLower(hexStr))
	if err != nil {
		return BlobRef{}, fmt.Errorf("invalid hash in blob ref %q: %w", s, err)
	}

	return BlobRef{Alg: alg, Hash: h}, nil
}

// String returns the canonical string form "algorithm:hex".
func (r BlobRef) String() string {
	return string(r.Alg) + ":" + r.Hash.String()
}

// Hex returns the plain hex digest without the algorithm prefix.
func (r BlobRef) Hex() string {
	return r.Hash.String()
}

// Blob storage key layout.

const blobKeyPrefix = "blobs"

// BlobStorageKey returns the backend storage key for a blob.
// Format: blobs/{hex[:2]}/{hex}
func BlobStorageKey(h Hash) string {
	hex := h.String()
	return blobKeyPrefix + "/" + hex[:2] + "/" + hex
}

// ParseBlobStorageKey extracts a Hash from a backend storage key.
// It accepts both the canonical 2-level format (blobs/xx/hex) and the
// legacy 3-level format (blobs/xx/yy/hex) for backward compatibility.
func ParseBlobStorageKey(key string) (Hash, error) {
	parts := strings.Split(key, "/")

	switch len(parts) {
	case 3:
		// blobs/xx/hex
		if parts[0] != blobKeyPrefix {
			return Hash{}, fmt.Errorf("invalid blob key prefix: %s", key)
		}
		return ParseHash(parts[2])
	case 4:
		// blobs/xx/yy/hex (legacy 3-level)
		if parts[0] != blobKeyPrefix {
			return Hash{}, fmt.Errorf("invalid blob key prefix: %s", key)
		}
		return ParseHash(parts[3])
	default:
		return Hash{}, fmt.Errorf("invalid blob key format: %s", key)
	}
}
