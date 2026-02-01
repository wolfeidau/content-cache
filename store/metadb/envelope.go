package metadb

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	// CompressionThreshold is the minimum payload size before compression is considered.
	// 2KB threshold - zstd overhead not worth it for smaller payloads.
	CompressionThreshold = 2048

	// MaxPayloadSize is the maximum allowed uncompressed payload size.
	MaxPayloadSize = 10 * 1024 * 1024 // 10MB

	// MaxDecompressedSize is the hard cap during decompression to prevent compression bombs.
	MaxDecompressedSize = 10 * 1024 * 1024 // 10MB

	// MaxAttributeKeys is the maximum number of attribute keys allowed.
	MaxAttributeKeys = 10

	// MaxAttributesSize is the maximum total size of all attributes.
	MaxAttributesSize = 1024 // 1KB

	// CurrentEnvelopeVersion is the current envelope schema version.
	CurrentEnvelopeVersion = 1
)

var (
	// ErrPayloadTooLarge is returned when payload exceeds MaxPayloadSize.
	ErrPayloadTooLarge = errors.New("payload exceeds maximum size")

	// ErrDecompressionBomb is returned when decompressed size exceeds limit.
	ErrDecompressionBomb = errors.New("decompressed payload exceeds maximum size")

	// ErrCorrupted is returned when payload digest verification fails.
	ErrCorrupted = errors.New("payload digest mismatch")

	// ErrTooManyAttributes is returned when attributes exceed MaxAttributeKeys.
	ErrTooManyAttributes = errors.New("too many attributes")

	// ErrAttributesTooLarge is returned when attributes exceed MaxAttributesSize.
	ErrAttributesTooLarge = errors.New("attributes exceed maximum size")

	// ErrInvalidDigestFormat is returned when a blob ref has invalid format.
	ErrInvalidDigestFormat = errors.New("invalid digest format")
)

// EnvelopeCodec handles envelope encoding/decoding with optional compression.
// Encoder and decoder are goroutine-safe and can be reused.
type EnvelopeCodec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	mu      sync.RWMutex
}

// NewEnvelopeCodec creates a new codec with pooled zstd encoder/decoder.
func NewEnvelopeCodec() (*EnvelopeCodec, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("creating zstd decoder: %w", err)
	}

	return &EnvelopeCodec{
		encoder: enc,
		decoder: dec,
	}, nil
}

// Close releases encoder/decoder resources.
func (c *EnvelopeCodec) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.encoder != nil {
		c.encoder.Close()
		c.encoder = nil
	}
	if c.decoder != nil {
		c.decoder.Close()
		c.decoder = nil
	}
}

// EncodePayload compresses payload if beneficial and returns encoded bytes with encoding type.
// Also computes and returns the digest of the original (uncompressed) payload.
func (c *EnvelopeCodec) EncodePayload(data []byte) (payload []byte, encoding ContentEncoding, digest string, err error) {
	if len(data) > MaxPayloadSize {
		return nil, ContentEncoding_CONTENT_ENCODING_IDENTITY, "", ErrPayloadTooLarge
	}

	digest = computeDigest(data)

	if len(data) < CompressionThreshold {
		return data, ContentEncoding_CONTENT_ENCODING_IDENTITY, digest, nil
	}

	c.mu.RLock()
	enc := c.encoder
	c.mu.RUnlock()

	if enc == nil {
		return data, ContentEncoding_CONTENT_ENCODING_IDENTITY, digest, nil
	}

	compressed := enc.EncodeAll(data, nil)
	if len(compressed) >= len(data) {
		return data, ContentEncoding_CONTENT_ENCODING_IDENTITY, digest, nil
	}

	return compressed, ContentEncoding_CONTENT_ENCODING_ZSTD, digest, nil
}

// DecodePayload decompresses payload if needed and verifies digest.
func (c *EnvelopeCodec) DecodePayload(payload []byte, encoding ContentEncoding, expectedDigest string, expectedSize uint64) ([]byte, error) {
	if encoding == ContentEncoding_CONTENT_ENCODING_IDENTITY {
		if expectedDigest != "" && computeDigest(payload) != expectedDigest {
			return nil, ErrCorrupted
		}
		return payload, nil
	}

	if encoding != ContentEncoding_CONTENT_ENCODING_ZSTD {
		return nil, fmt.Errorf("unsupported encoding: %v", encoding)
	}

	if expectedSize > MaxDecompressedSize {
		return nil, ErrDecompressionBomb
	}

	c.mu.RLock()
	dec := c.decoder
	c.mu.RUnlock()

	if dec == nil {
		return nil, errors.New("decoder not initialized")
	}

	decompressed, err := dec.DecodeAll(payload, nil)
	if err != nil {
		return nil, fmt.Errorf("decompressing payload: %w", err)
	}

	if uint64(len(decompressed)) > MaxDecompressedSize {
		return nil, ErrDecompressionBomb
	}

	if expectedDigest != "" && computeDigest(decompressed) != expectedDigest {
		return nil, ErrCorrupted
	}

	return decompressed, nil
}

// computeDigest computes sha256 digest in canonical format.
func computeDigest(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

// ValidateEnvelope validates an envelope before storage.
func ValidateEnvelope(env *MetadataEnvelope) error {
	if env == nil {
		return errors.New("envelope is nil")
	}

	if len(env.Attributes) > MaxAttributeKeys {
		return ErrTooManyAttributes
	}

	var totalSize int
	for _, v := range env.Attributes {
		totalSize += len(v)
	}
	if totalSize > MaxAttributesSize {
		return ErrAttributesTooLarge
	}

	for _, ref := range env.BlobRefs {
		if err := ValidateDigestFormat(ref); err != nil {
			return err
		}
	}

	return nil
}

// ValidateDigestFormat validates that a digest string is in canonical format.
// Accepted formats: "sha256:<64 hex chars>" or "blake3:<64 hex chars>"
func ValidateDigestFormat(digest string) error {
	parts := strings.SplitN(digest, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%w: missing algorithm prefix in %q", ErrInvalidDigestFormat, digest)
	}

	algo, hash := parts[0], parts[1]
	algo = strings.ToLower(algo)

	switch algo {
	case "sha256", "blake3":
		if len(hash) != 64 {
			return fmt.Errorf("%w: expected 64 hex chars, got %d in %q", ErrInvalidDigestFormat, len(hash), digest)
		}
		for _, c := range hash {
			isDigit := c >= '0' && c <= '9'
			isLowerHex := c >= 'a' && c <= 'f'
			isUpperHex := c >= 'A' && c <= 'F'
			if !isDigit && !isLowerHex && !isUpperHex {
				return fmt.Errorf("%w: invalid hex character in %q", ErrInvalidDigestFormat, digest)
			}
		}
	default:
		return fmt.Errorf("%w: unsupported algorithm %q", ErrInvalidDigestFormat, algo)
	}

	return nil
}

// CanonicalizeRefs deduplicates, lowercases, and sorts blob refs.
func CanonicalizeRefs(refs []string) []string {
	if len(refs) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(refs))
	result := make([]string, 0, len(refs))

	for _, ref := range refs {
		lower := strings.ToLower(ref)
		if _, ok := seen[lower]; !ok {
			seen[lower] = struct{}{}
			result = append(result, lower)
		}
	}

	sort.Strings(result)
	return result
}

// DiffRefs computes added and removed refs between old and new sets.
// Both inputs should already be canonicalized.
func DiffRefs(oldRefs, newRefs []string) (added, removed []string) {
	oldSet := make(map[string]struct{}, len(oldRefs))
	newSet := make(map[string]struct{}, len(newRefs))

	for _, r := range oldRefs {
		oldSet[r] = struct{}{}
	}
	for _, r := range newRefs {
		newSet[r] = struct{}{}
	}

	for _, r := range newRefs {
		if _, ok := oldSet[r]; !ok {
			added = append(added, r)
		}
	}
	for _, r := range oldRefs {
		if _, ok := newSet[r]; !ok {
			removed = append(removed, r)
		}
	}

	return added, removed
}

// RefsEqual returns true if two ref slices contain the same refs (order-independent).
func RefsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aCopy := slices.Clone(a)
	bCopy := slices.Clone(b)
	sort.Strings(aCopy)
	sort.Strings(bCopy)
	return slices.Equal(aCopy, bCopy)
}
