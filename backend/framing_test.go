package backend

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFramingRoundTrip(t *testing.T) {
	header := &BlobHeader{
		ContentType:   "application/octet-stream",
		ContentLength: 13,
		CachedAt:      "2024-01-15T10:30:00Z",
		UpstreamETag:  "abc123",
		ContentHash:   "blake3:deadbeef",
	}
	bodyData := []byte("hello, world!")

	// Write framed
	var buf bytes.Buffer
	err := WriteFramed(&buf, header, bytes.NewReader(bodyData))
	require.NoError(t, err)

	// Read framed
	readHeader, bodyReader, err := ReadFramed(&buf)
	require.NoError(t, err)

	// Verify header
	require.Equal(t, header.ContentType, readHeader.ContentType)
	require.Equal(t, header.ContentLength, readHeader.ContentLength)
	require.Equal(t, header.CachedAt, readHeader.CachedAt)
	require.Equal(t, header.UpstreamETag, readHeader.UpstreamETag)
	require.Equal(t, header.ContentHash, readHeader.ContentHash)

	// Verify body
	readBody, err := io.ReadAll(bodyReader)
	require.NoError(t, err)
	require.Equal(t, bodyData, readBody)
}

func TestFramingRoundTripEmptyETag(t *testing.T) {
	header := &BlobHeader{
		ContentType:   "text/plain",
		ContentLength: 4,
		CachedAt:      "2024-01-15T10:30:00Z",
		ContentHash:   "blake3:abcd1234",
	}
	bodyData := []byte("test")

	var buf bytes.Buffer
	err := WriteFramed(&buf, header, bytes.NewReader(bodyData))
	require.NoError(t, err)

	readHeader, _, err := ReadFramed(&buf)
	require.NoError(t, err)
	require.Empty(t, readHeader.UpstreamETag)
}

func TestIsFramedWithFramedFile(t *testing.T) {
	header := &BlobHeader{
		ContentType:   "text/plain",
		ContentLength: 5,
		CachedAt:      "2024-01-15T10:30:00Z",
		ContentHash:   "blake3:1234",
	}

	var buf bytes.Buffer
	err := WriteFramed(&buf, header, strings.NewReader("hello"))
	require.NoError(t, err)

	// Use bytes.Reader which implements ReadSeeker
	reader := bytes.NewReader(buf.Bytes())

	isFramed, err := IsFramed(reader)
	require.NoError(t, err)
	require.True(t, isFramed)

	// Verify reader is back at start
	pos, _ := reader.Seek(0, io.SeekCurrent)
	require.Equal(t, int64(0), pos)
}

func TestIsFramedWithLegacyFile(t *testing.T) {
	// Legacy file - raw content without framing
	legacyContent := []byte("this is raw content without magic bytes")
	reader := bytes.NewReader(legacyContent)

	isFramed, err := IsFramed(reader)
	require.NoError(t, err)
	require.False(t, isFramed)

	// Verify reader is back at start
	pos, _ := reader.Seek(0, io.SeekCurrent)
	require.Equal(t, int64(0), pos)
}

func TestIsFramedWithShortFile(t *testing.T) {
	// File shorter than 4 bytes
	shortContent := []byte("ab")
	reader := bytes.NewReader(shortContent)

	isFramed, err := IsFramed(reader)
	require.NoError(t, err)
	require.False(t, isFramed)
}

func TestReadFramedInvalidMagic(t *testing.T) {
	// Create a buffer with wrong magic bytes
	var buf bytes.Buffer
	buf.WriteString("XXXX") // wrong magic
	err := binary.Write(&buf, binary.BigEndian, uint32(10))
	require.NoError(t, err)
	buf.WriteString(`{"test":1}`)

	_, _, err = ReadFramed(&buf)
	require.ErrorIs(t, err, ErrInvalidMagic)
}

func TestWriteFramedHeaderTooLarge(t *testing.T) {
	// Create a header that will serialize to > 64 KiB
	header := &BlobHeader{
		ContentType:   strings.Repeat("x", MaxHeaderSize),
		ContentLength: 0,
		CachedAt:      "2024-01-15T10:30:00Z",
		ContentHash:   "blake3:1234",
	}

	var buf bytes.Buffer
	err := WriteFramed(&buf, header, strings.NewReader(""))
	require.ErrorIs(t, err, ErrHeaderTooLarge)
}

func TestReadFramedHeaderTooLarge(t *testing.T) {
	// Manually craft a buffer with header length > MaxHeaderSize
	var buf bytes.Buffer
	buf.Write(MagicBytes)
	err := binary.Write(&buf, binary.BigEndian, uint32(MaxHeaderSize+1))
	require.NoError(t, err)

	_, _, err = ReadFramed(&buf)
	require.ErrorIs(t, err, ErrHeaderTooLarge)
}

func TestReadFramedEmptyBody(t *testing.T) {
	header := &BlobHeader{
		ContentType:   "application/json",
		ContentLength: 0,
		CachedAt:      "2024-01-15T10:30:00Z",
		ContentHash:   "blake3:empty",
	}

	var buf bytes.Buffer
	err := WriteFramed(&buf, header, strings.NewReader(""))
	require.NoError(t, err)

	readHeader, bodyReader, err := ReadFramed(&buf)
	require.NoError(t, err)
	require.Equal(t, int64(0), readHeader.ContentLength)

	body, err := io.ReadAll(bodyReader)
	require.NoError(t, err)
	require.Empty(t, body)
}

func TestFramingLargeBody(t *testing.T) {
	header := &BlobHeader{
		ContentType:   "application/octet-stream",
		ContentLength: 1024 * 1024,
		CachedAt:      "2024-01-15T10:30:00Z",
		ContentHash:   "blake3:large",
	}
	largeBody := bytes.Repeat([]byte("x"), 1024*1024) // 1 MiB

	var buf bytes.Buffer
	err := WriteFramed(&buf, header, bytes.NewReader(largeBody))
	require.NoError(t, err)

	readHeader, bodyReader, err := ReadFramed(&buf)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024), readHeader.ContentLength)

	body, err := io.ReadAll(bodyReader)
	require.NoError(t, err)
	require.Equal(t, largeBody, body)
}
