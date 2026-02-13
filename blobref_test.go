package contentcache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobRefString(t *testing.T) {
	h := HashBytes([]byte("test"))
	ref := NewBlobRef(h)

	assert.Equal(t, "blake3:"+h.String(), ref.String())
	assert.Equal(t, h.String(), ref.Hex())
}

func TestParseBlobRef(t *testing.T) {
	validHex := HashBytes([]byte("test")).String()
	upperHex := "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"

	tests := []struct {
		name    string
		input   string
		wantAlg Algorithm
		wantHex string
		wantErr bool
	}{
		{
			name:    "blake3 lowercase",
			input:   "blake3:" + validHex,
			wantAlg: AlgBLAKE3,
			wantHex: validHex,
		},
		{
			name:    "BLAKE3 uppercase algo",
			input:   "BLAKE3:" + validHex,
			wantAlg: AlgBLAKE3,
			wantHex: validHex,
		},
		{
			name:    "sha256",
			input:   "sha256:" + validHex,
			wantAlg: AlgSHA256,
			wantHex: validHex,
		},
		{
			name:    "SHA256 uppercase",
			input:   "SHA256:" + validHex,
			wantAlg: AlgSHA256,
			wantHex: validHex,
		},
		{
			name:    "uppercase hex normalised",
			input:   "blake3:" + upperHex,
			wantAlg: AlgBLAKE3,
			wantHex: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		},
		{
			name:    "plain hex legacy",
			input:   validHex,
			wantAlg: AlgBLAKE3,
			wantHex: validHex,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "unknown algorithm",
			input:   "md5:" + validHex,
			wantErr: true,
		},
		{
			name:    "invalid hex",
			input:   "blake3:not-valid-hex",
			wantErr: true,
		},
		{
			name:    "short hex",
			input:   "blake3:abcdef",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseBlobRef(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantAlg, ref.Alg)
			assert.Equal(t, tt.wantHex, ref.Hex())
		})
	}
}

func TestParseBlobRefRoundTrip(t *testing.T) {
	h := HashBytes([]byte("roundtrip"))
	original := NewBlobRef(h)

	parsed, err := ParseBlobRef(original.String())
	require.NoError(t, err)
	assert.Equal(t, original, parsed)
}

func TestBlobStorageKey(t *testing.T) {
	h := HashBytes([]byte("test"))
	hex := h.String()

	key := BlobStorageKey(h)
	assert.Equal(t, "blobs/"+hex[:2]+"/"+hex, key)
}

func TestParseBlobStorageKey(t *testing.T) {
	h := HashBytes([]byte("test"))
	hex := h.String()

	tests := []struct {
		name    string
		key     string
		wantHex string
		wantErr bool
	}{
		{
			name:    "2-level canonical",
			key:     "blobs/" + hex[:2] + "/" + hex,
			wantHex: hex,
		},
		{
			name:    "3-level legacy",
			key:     "blobs/" + hex[:2] + "/" + hex[2:4] + "/" + hex,
			wantHex: hex,
		},
		{
			name:    "wrong prefix",
			key:     "meta/" + hex[:2] + "/" + hex,
			wantErr: true,
		},
		{
			name:    "empty",
			key:     "",
			wantErr: true,
		},
		{
			name:    "single segment",
			key:     "blobs",
			wantErr: true,
		},
		{
			name:    "invalid hex in key",
			key:     "blobs/ab/not-a-hash",
			wantErr: true,
		},
		{
			name:    "5 segments invalid",
			key:     "blobs/ab/cd/ef/" + hex,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBlobStorageKey(tt.key)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHex, got.String())
		})
	}
}

func TestBlobStorageKeyRoundTrip(t *testing.T) {
	h := HashBytes([]byte("roundtrip"))
	key := BlobStorageKey(h)

	got, err := ParseBlobStorageKey(key)
	require.NoError(t, err)
	assert.Equal(t, h, got)
}
