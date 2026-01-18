package oci

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDigest(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantAlgo  string
		wantHex   string
		wantErr   bool
		errString string
	}{
		{
			name:     "valid sha256",
			input:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			wantAlgo: "sha256",
			wantHex:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:     "valid sha512",
			input:    "sha512:cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
			wantAlgo: "sha512",
			wantHex:  "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
		},
		{
			name:     "uppercase hex normalized",
			input:    "sha256:E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
			wantAlgo: "sha256",
			wantHex:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:      "missing algorithm prefix",
			input:     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			wantErr:   true,
			errString: "missing algorithm prefix",
		},
		{
			name:      "unsupported algorithm",
			input:     "md5:d41d8cd98f00b204e9800998ecf8427e",
			wantErr:   true,
			errString: "unsupported digest algorithm",
		},
		{
			name:      "wrong sha256 length",
			input:     "sha256:e3b0c44298fc1c149afbf4c8996fb924",
			wantErr:   true,
			errString: "sha256 digest must be 64 hex chars",
		},
		{
			name:      "invalid hex",
			input:     "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
			wantErr:   true,
			errString: "invalid hex encoding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDigest(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errString != "" {
					require.Contains(t, err.Error(), tt.errString)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAlgo, got.Algorithm)
			require.Equal(t, tt.wantHex, got.Hex)
		})
	}
}

func TestDigestString(t *testing.T) {
	d := Digest{Algorithm: "sha256", Hex: "abc123"}
	require.Equal(t, "sha256:abc123", d.String())
}

func TestDigestIsZero(t *testing.T) {
	tests := []struct {
		name   string
		digest Digest
		want   bool
	}{
		{
			name:   "zero digest",
			digest: Digest{},
			want:   true,
		},
		{
			name:   "non-zero digest",
			digest: Digest{Algorithm: "sha256", Hex: "abc"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.digest.IsZero())
		})
	}
}

func TestDigestVerify(t *testing.T) {
	// SHA256 of empty string
	emptyDigest, _ := ParseDigest("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	// SHA256 of "hello"
	helloDigest, _ := ParseDigest("sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")

	tests := []struct {
		name    string
		digest  Digest
		content []byte
		wantErr bool
	}{
		{
			name:    "valid empty",
			digest:  emptyDigest,
			content: []byte{},
			wantErr: false,
		},
		{
			name:    "valid hello",
			digest:  helloDigest,
			content: []byte("hello"),
			wantErr: false,
		},
		{
			name:    "mismatch",
			digest:  emptyDigest,
			content: []byte("hello"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.digest.Verify(tt.content)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestComputeSHA256(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		want    string
	}{
		{
			name:    "empty",
			content: []byte{},
			want:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:    "hello",
			content: []byte("hello"),
			want:    "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ComputeSHA256(tt.content))
		})
	}
}

func TestIsDigestReference(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		want      bool
	}{
		{
			name:      "tag",
			reference: "latest",
			want:      false,
		},
		{
			name:      "version tag",
			reference: "v1.0.0",
			want:      false,
		},
		{
			name:      "sha256 digest",
			reference: "sha256:abc123",
			want:      true,
		},
		{
			name:      "sha512 digest",
			reference: "sha512:abc123",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsDigestReference(tt.reference))
		})
	}
}
