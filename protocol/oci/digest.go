package oci

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
)

// Common errors for digest operations.
var (
	ErrInvalidDigest   = errors.New("invalid digest format")
	ErrDigestMismatch  = errors.New("digest mismatch")
	ErrUnsupportedAlgo = errors.New("unsupported digest algorithm")
)

// Digest represents an OCI content digest (algorithm:hex).
type Digest struct {
	Algorithm string // sha256, sha512
	Hex       string // hex-encoded hash
}

// ParseDigest parses a digest string like "sha256:abc123...".
func ParseDigest(s string) (Digest, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return Digest{}, fmt.Errorf("%w: missing algorithm prefix", ErrInvalidDigest)
	}

	algo := parts[0]
	hexStr := parts[1]

	// Validate algorithm and hex length
	switch algo {
	case "sha256":
		if len(hexStr) != 64 {
			return Digest{}, fmt.Errorf("%w: sha256 digest must be 64 hex chars", ErrInvalidDigest)
		}
	case "sha512":
		if len(hexStr) != 128 {
			return Digest{}, fmt.Errorf("%w: sha512 digest must be 128 hex chars", ErrInvalidDigest)
		}
	default:
		return Digest{}, fmt.Errorf("%w: %s", ErrUnsupportedAlgo, algo)
	}

	// Validate hex encoding
	if _, err := hex.DecodeString(hexStr); err != nil {
		return Digest{}, fmt.Errorf("%w: invalid hex encoding", ErrInvalidDigest)
	}

	return Digest{
		Algorithm: algo,
		Hex:       strings.ToLower(hexStr),
	}, nil
}

// String returns the canonical digest string (algorithm:hex).
func (d Digest) String() string {
	return fmt.Sprintf("%s:%s", d.Algorithm, d.Hex)
}

// IsZero returns true if the digest is uninitialized.
func (d Digest) IsZero() bool {
	return d.Algorithm == "" && d.Hex == ""
}

// NewHasher returns a hash.Hash for the digest's algorithm.
func (d Digest) NewHasher() (hash.Hash, error) {
	switch d.Algorithm {
	case "sha256":
		return sha256.New(), nil
	case "sha512":
		return sha512.New(), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedAlgo, d.Algorithm)
	}
}

// Verify checks if content matches the digest.
func (d Digest) Verify(content []byte) error {
	computed, err := d.compute(content)
	if err != nil {
		return err
	}
	if computed != d.Hex {
		return fmt.Errorf("%w: expected %s, got %s", ErrDigestMismatch, d.Hex, computed)
	}
	return nil
}

// VerifyReader reads all content and verifies the digest.
// Returns the content if verification succeeds.
func (d Digest) VerifyReader(r io.Reader) ([]byte, error) {
	h, err := d.NewHasher()
	if err != nil {
		return nil, err
	}

	content, err := io.ReadAll(io.TeeReader(r, h))
	if err != nil {
		return nil, fmt.Errorf("reading content: %w", err)
	}

	computed := hex.EncodeToString(h.Sum(nil))
	if computed != d.Hex {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrDigestMismatch, d.Hex, computed)
	}

	return content, nil
}

// compute calculates the digest of content and returns the hex string.
func (d Digest) compute(content []byte) (string, error) {
	switch d.Algorithm {
	case "sha256":
		sum := sha256.Sum256(content)
		return hex.EncodeToString(sum[:]), nil
	case "sha512":
		sum := sha512.Sum512(content)
		return hex.EncodeToString(sum[:]), nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedAlgo, d.Algorithm)
	}
}

// ComputeSHA256 computes the SHA256 digest of content.
func ComputeSHA256(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ComputeSHA256Reader computes the SHA256 digest from a reader.
func ComputeSHA256Reader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// IsDigestReference returns true if the reference is a digest (contains @sha256:).
func IsDigestReference(reference string) bool {
	return strings.Contains(reference, "sha256:") || strings.Contains(reference, "sha512:")
}
