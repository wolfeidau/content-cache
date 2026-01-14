package contentcache

import (
	"bytes"
	"strings"
	"testing"
)

func TestHashString(t *testing.T) {
	// BLAKE3 hash of empty string
	h := HashBytes([]byte{})
	expected := "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	if got := h.String(); got != expected {
		t.Errorf("Hash.String() = %q, want %q", got, expected)
	}
}

func TestHashShortString(t *testing.T) {
	h := HashBytes([]byte("hello"))
	short := h.ShortString()
	if len(short) != 16 {
		t.Errorf("ShortString length = %d, want 16", len(short))
	}
	if !strings.HasPrefix(h.String(), short) {
		t.Errorf("ShortString %q is not prefix of full hash %q", short, h.String())
	}
}

func TestHashDir(t *testing.T) {
	h := HashBytes([]byte("test"))
	dir := h.Dir()
	if len(dir) != 2 {
		t.Errorf("Dir() length = %d, want 2", len(dir))
	}
	if !strings.HasPrefix(h.String(), dir) {
		t.Errorf("Dir %q is not prefix of hash %q", dir, h.String())
	}
}

func TestHashIsZero(t *testing.T) {
	var zero Hash
	if !zero.IsZero() {
		t.Error("zero hash should report IsZero() = true")
	}

	h := HashBytes([]byte("test"))
	if h.IsZero() {
		t.Error("non-zero hash should report IsZero() = false")
	}
}

func TestHashMarshalUnmarshal(t *testing.T) {
	original := HashBytes([]byte("test data"))

	// Marshal
	text, err := original.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText() error = %v", err)
	}

	// Unmarshal
	var parsed Hash
	if err := parsed.UnmarshalText(text); err != nil {
		t.Fatalf("UnmarshalText() error = %v", err)
	}

	if parsed != original {
		t.Errorf("round-trip failed: got %v, want %v", parsed, original)
	}
}

func TestParseHash(t *testing.T) {
	original := HashBytes([]byte("parse test"))
	hex := original.String()

	parsed, err := ParseHash(hex)
	if err != nil {
		t.Fatalf("ParseHash() error = %v", err)
	}

	if parsed != original {
		t.Errorf("ParseHash() = %v, want %v", parsed, original)
	}
}

func TestParseHashInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"too short", "abc123"},
		{"too long", strings.Repeat("a", 128)},
		{"invalid hex", strings.Repeat("zz", 32)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseHash(tt.input)
			if err == nil {
				t.Error("ParseHash() should return error for invalid input")
			}
		})
	}
}

func TestHashBytes(t *testing.T) {
	data := []byte("hello world")
	h1 := HashBytes(data)
	h2 := HashBytes(data)

	if h1 != h2 {
		t.Error("HashBytes should return same hash for same input")
	}

	h3 := HashBytes([]byte("different"))
	if h1 == h3 {
		t.Error("HashBytes should return different hash for different input")
	}
}

func TestHashReader(t *testing.T) {
	data := []byte("test content for hashing")
	reader := bytes.NewReader(data)

	hash, n, err := HashReader(reader)
	if err != nil {
		t.Fatalf("HashReader() error = %v", err)
	}

	if n != int64(len(data)) {
		t.Errorf("HashReader() bytes read = %d, want %d", n, len(data))
	}

	expected := HashBytes(data)
	if hash != expected {
		t.Errorf("HashReader() hash = %v, want %v", hash, expected)
	}
}

func TestHasher(t *testing.T) {
	hasher := NewHasher()

	// Write in chunks
	chunks := []string{"hello", " ", "world"}
	for _, chunk := range chunks {
		hasher.Write([]byte(chunk))
	}

	result := hasher.Sum()
	expected := HashBytes([]byte("hello world"))

	if result != expected {
		t.Errorf("Hasher.Sum() = %v, want %v", result, expected)
	}

	// Test reset
	hasher.Reset()
	hasher.Write([]byte("new data"))
	newResult := hasher.Sum()
	if newResult == result {
		t.Error("Hasher should produce different hash after reset and new data")
	}
}

func TestHashingReader(t *testing.T) {
	data := []byte("streaming hash test")
	reader := bytes.NewReader(data)
	hr := NewHashingReader(reader)

	// Read all data
	buf := make([]byte, 1024)
	total := 0
	for {
		n, err := hr.Read(buf[total:])
		total += n
		if err != nil {
			break
		}
	}

	if int64(total) != hr.BytesRead() {
		t.Errorf("BytesRead() = %d, want %d", hr.BytesRead(), total)
	}

	expected := HashBytes(data)
	if hr.Sum() != expected {
		t.Errorf("HashingReader.Sum() = %v, want %v", hr.Sum(), expected)
	}
}

func TestHashingWriter(t *testing.T) {
	var buf bytes.Buffer
	hw := NewHashingWriter(&buf)

	data := []byte("writing and hashing")
	n, err := hw.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if n != len(data) {
		t.Errorf("Write() = %d, want %d", n, len(data))
	}

	if hw.BytesWritten() != int64(len(data)) {
		t.Errorf("BytesWritten() = %d, want %d", hw.BytesWritten(), len(data))
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("data not written correctly to underlying writer")
	}

	expected := HashBytes(data)
	if hw.Sum() != expected {
		t.Errorf("HashingWriter.Sum() = %v, want %v", hw.Sum(), expected)
	}
}
