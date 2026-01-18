package contentcache

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashString(t *testing.T) {
	// BLAKE3 hash of empty string
	h := HashBytes([]byte{})
	expected := "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	require.Equal(t, expected, h.String())
}

func TestHashShortString(t *testing.T) {
	h := HashBytes([]byte("hello"))
	short := h.ShortString()
	require.Len(t, short, 16)
	require.True(t, strings.HasPrefix(h.String(), short))
}

func TestHashDir(t *testing.T) {
	h := HashBytes([]byte("test"))
	dir := h.Dir()
	require.Len(t, dir, 2)
	require.True(t, strings.HasPrefix(h.String(), dir))
}

func TestHashIsZero(t *testing.T) {
	var zero Hash
	require.True(t, zero.IsZero())

	h := HashBytes([]byte("test"))
	require.False(t, h.IsZero())
}

func TestHashMarshalUnmarshal(t *testing.T) {
	original := HashBytes([]byte("test data"))

	// Marshal
	text, err := original.MarshalText()
	require.NoError(t, err)

	// Unmarshal
	var parsed Hash
	err = parsed.UnmarshalText(text)
	require.NoError(t, err)

	require.Equal(t, original, parsed)
}

func TestParseHash(t *testing.T) {
	original := HashBytes([]byte("parse test"))
	hex := original.String()

	parsed, err := ParseHash(hex)
	require.NoError(t, err)

	require.Equal(t, original, parsed)
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
			require.Error(t, err)
		})
	}
}

func TestHashBytes(t *testing.T) {
	data := []byte("hello world")
	h1 := HashBytes(data)
	h2 := HashBytes(data)

	require.Equal(t, h1, h2)

	h3 := HashBytes([]byte("different"))
	require.NotEqual(t, h1, h3)
}

func TestHashReader(t *testing.T) {
	data := []byte("test content for hashing")
	reader := bytes.NewReader(data)

	hash, n, err := HashReader(reader)
	require.NoError(t, err)

	require.Equal(t, int64(len(data)), n)

	expected := HashBytes(data)
	require.Equal(t, expected, hash)
}

func TestHasher(t *testing.T) {
	hasher := NewHasher()

	// Write in chunks
	chunks := []string{"hello", " ", "world"}
	for _, chunk := range chunks {
		_, _ = hasher.Write([]byte(chunk))
	}

	result := hasher.Sum()
	expected := HashBytes([]byte("hello world"))

	require.Equal(t, expected, result)

	// Test reset
	hasher.Reset()
	_, _ = hasher.Write([]byte("new data"))
	newResult := hasher.Sum()
	require.NotEqual(t, result, newResult)
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

	require.Equal(t, int64(total), hr.BytesRead())

	expected := HashBytes(data)
	require.Equal(t, expected, hr.Sum())
}

func TestHashingWriter(t *testing.T) {
	var buf bytes.Buffer
	hw := NewHashingWriter(&buf)

	data := []byte("writing and hashing")
	n, err := hw.Write(data)
	require.NoError(t, err)

	require.Equal(t, len(data), n)

	require.Equal(t, int64(len(data)), hw.BytesWritten())

	require.Equal(t, data, buf.Bytes())

	expected := HashBytes(data)
	require.Equal(t, expected, hw.Sum())
}
