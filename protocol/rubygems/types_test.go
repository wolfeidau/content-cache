package rubygems

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseGemFilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     *ParsedGemFilename
		wantErr  bool
	}{
		{
			name:     "simple gem",
			filename: "rails-7.1.0.gem",
			want: &ParsedGemFilename{
				Name:     "rails",
				Version:  "7.1.0",
				Platform: "ruby",
			},
		},
		{
			name:     "hyphenated gem name",
			filename: "aws-sdk-s3-1.140.0.gem",
			want: &ParsedGemFilename{
				Name:     "aws-sdk-s3",
				Version:  "1.140.0",
				Platform: "ruby",
			},
		},
		{
			name:     "platform gem",
			filename: "nokogiri-1.15.4-x86_64-linux.gem",
			want: &ParsedGemFilename{
				Name:     "nokogiri",
				Version:  "1.15.4",
				Platform: "x86_64-linux",
			},
		},
		{
			name:     "platform gem with hyphenated name",
			filename: "grpc-1.59.0-x86_64-linux.gem",
			want: &ParsedGemFilename{
				Name:     "grpc",
				Version:  "1.59.0",
				Platform: "x86_64-linux",
			},
		},
		{
			name:     "prerelease version",
			filename: "rails-7.2.0.beta1.gem",
			want: &ParsedGemFilename{
				Name:     "rails",
				Version:  "7.2.0.beta1",
				Platform: "ruby",
			},
		},
		{
			name:     "complex hyphenated name",
			filename: "net-http-persistent-4.0.2.gem",
			want: &ParsedGemFilename{
				Name:     "net-http-persistent",
				Version:  "4.0.2",
				Platform: "ruby",
			},
		},
		{
			name:     "java platform",
			filename: "jruby-openssl-0.14.0-java.gem",
			want: &ParsedGemFilename{
				Name:     "jruby-openssl",
				Version:  "0.14.0",
				Platform: "java",
			},
		},
		{
			name:     "darwin platform",
			filename: "nokogiri-1.15.4-arm64-darwin.gem",
			want: &ParsedGemFilename{
				Name:     "nokogiri",
				Version:  "1.15.4",
				Platform: "arm64-darwin",
			},
		},
		{
			name:     "windows platform",
			filename: "ffi-1.15.5-x64-mingw-ucrt.gem",
			want: &ParsedGemFilename{
				Name:     "ffi",
				Version:  "1.15.5",
				Platform: "x64-mingw-ucrt",
			},
		},
		{
			name:     "rc version",
			filename: "activesupport-7.1.0.rc1.gem",
			want: &ParsedGemFilename{
				Name:     "activesupport",
				Version:  "7.1.0.rc1",
				Platform: "ruby",
			},
		},
		{
			name:     "missing .gem suffix",
			filename: "rails-7.1.0",
			wantErr:  true,
		},
		{
			name:     "no version",
			filename: "rails.gem",
			wantErr:  true,
		},
		{
			name:     "empty name",
			filename: "-1.0.0.gem",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGemFilename(tt.filename)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want.Name, got.Name, "name mismatch")
			require.Equal(t, tt.want.Version, got.Version, "version mismatch")
			require.Equal(t, tt.want.Platform, got.Platform, "platform mismatch")
		})
	}
}

func TestParsedGemFilename_VersionPlatformKey(t *testing.T) {
	tests := []struct {
		name     string
		parsed   *ParsedGemFilename
		expected string
	}{
		{
			name: "ruby platform",
			parsed: &ParsedGemFilename{
				Name:     "rails",
				Version:  "7.1.0",
				Platform: "ruby",
			},
			expected: "7.1.0",
		},
		{
			name: "empty platform",
			parsed: &ParsedGemFilename{
				Name:     "rails",
				Version:  "7.1.0",
				Platform: "",
			},
			expected: "7.1.0",
		},
		{
			name: "linux platform",
			parsed: &ParsedGemFilename{
				Name:     "nokogiri",
				Version:  "1.15.4",
				Platform: "x86_64-linux",
			},
			expected: "1.15.4-x86_64-linux",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.parsed.VersionPlatformKey()
			require.Equal(t, tt.expected, got)
		})
	}
}
