package contentcache

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

// HashSize is the size of a BLAKE3 hash in bytes (256 bits).
const HashSize = 32

// Hash represents a BLAKE3 256-bit digest.
type Hash [HashSize]byte

// String returns the hex-encoded representation of the hash.
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// ShortString returns a shortened hex representation for display.
func (h Hash) ShortString() string {
	return hex.EncodeToString(h[:8])
}

// Dir returns the first two characters of the hex-encoded hash,
// used for sharding blobs into subdirectories.
func (h Hash) Dir() string {
	return hex.EncodeToString(h[:1])
}

// IsZero returns true if the hash is all zeros (uninitialized).
func (h Hash) IsZero() bool {
	return h == Hash{}
}

// MarshalText implements encoding.TextMarshaler.
func (h Hash) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (h *Hash) UnmarshalText(text []byte) error {
	if len(text) != HashSize*2 {
		return fmt.Errorf("invalid hash length: expected %d hex chars, got %d", HashSize*2, len(text))
	}
	_, err := hex.Decode(h[:], text)
	return err
}

// ParseHash parses a hex-encoded hash string.
func ParseHash(s string) (Hash, error) {
	var h Hash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return Hash{}, err
	}
	return h, nil
}

// HashBytes computes the BLAKE3 hash of the given bytes.
func HashBytes(data []byte) Hash {
	return Hash(blake3.Sum256(data))
}

// HashReader computes the BLAKE3 hash of content from the reader.
// It returns the hash and the number of bytes read.
func HashReader(r io.Reader) (Hash, int64, error) {
	h := blake3.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return Hash{}, n, fmt.Errorf("hashing content: %w", err)
	}
	var hash Hash
	h.Sum(hash[:0])
	return hash, n, nil
}

// Hasher wraps a BLAKE3 hasher for incremental hashing.
type Hasher struct {
	h *blake3.Hasher
}

// NewHasher creates a new Hasher for incremental hashing.
func NewHasher() *Hasher {
	return &Hasher{h: blake3.New()}
}

// Write implements io.Writer.
func (h *Hasher) Write(p []byte) (int, error) {
	return h.h.Write(p)
}

// Sum returns the current hash without resetting the hasher.
func (h *Hasher) Sum() Hash {
	var hash Hash
	h.h.Sum(hash[:0])
	return hash
}

// Reset resets the hasher to its initial state.
func (h *Hasher) Reset() {
	h.h.Reset()
}

// HashingReader wraps a reader and computes the hash as data is read.
type HashingReader struct {
	r io.Reader
	h *blake3.Hasher
	n int64
}

// NewHashingReader creates a reader that computes a hash as data is read.
func NewHashingReader(r io.Reader) *HashingReader {
	return &HashingReader{
		r: r,
		h: blake3.New(),
	}
}

// Read implements io.Reader.
func (hr *HashingReader) Read(p []byte) (int, error) {
	n, err := hr.r.Read(p)
	if n > 0 {
		hr.h.Write(p[:n])
		hr.n += int64(n)
	}
	return n, err
}

// Sum returns the hash of all data read so far.
func (hr *HashingReader) Sum() Hash {
	var hash Hash
	hr.h.Sum(hash[:0])
	return hash
}

// BytesRead returns the total number of bytes read.
func (hr *HashingReader) BytesRead() int64 {
	return hr.n
}

// HashingWriter wraps a writer and computes the hash as data is written.
type HashingWriter struct {
	w io.Writer
	h *blake3.Hasher
	n int64
}

// NewHashingWriter creates a writer that computes a hash as data is written.
func NewHashingWriter(w io.Writer) *HashingWriter {
	return &HashingWriter{
		w: w,
		h: blake3.New(),
	}
}

// Write implements io.Writer.
func (hw *HashingWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	if n > 0 {
		hw.h.Write(p[:n])
		hw.n += int64(n)
	}
	return n, err
}

// Sum returns the hash of all data written so far.
func (hw *HashingWriter) Sum() Hash {
	var hash Hash
	hw.h.Sum(hash[:0])
	return hash
}

// BytesWritten returns the total number of bytes written.
func (hw *HashingWriter) BytesWritten() int64 {
	return hw.n
}
