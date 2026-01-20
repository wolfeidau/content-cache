package rubygems

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseInfoChecksums(t *testing.T) {
	// Sample /info/nokogiri content (simplified)
	content := `---
1.15.4 mini_portile2:~>2.8.2,racc:~>1.4|checksum:abc123def456,ruby:>=2.7
1.15.4-x86_64-linux racc:~>1.4|checksum:789xyz000111,ruby:>=2.7
1.15.4-arm64-darwin racc:~>1.4|checksum:aabbccdd1122,ruby:>=2.7
1.15.5 mini_portile2:~>2.8.2|checksum:newversion123,ruby:>=2.7
`

	checksums := ParseInfoChecksums([]byte(content))

	require.Len(t, checksums, 4)
	require.Equal(t, "abc123def456", checksums["1.15.4"])
	require.Equal(t, "789xyz000111", checksums["1.15.4-x86_64-linux"])
	require.Equal(t, "aabbccdd1122", checksums["1.15.4-arm64-darwin"])
	require.Equal(t, "newversion123", checksums["1.15.5"])
}

func TestParseInfoChecksums_NoChecksum(t *testing.T) {
	// Line without checksum requirement
	content := `---
1.0.0 dep:>=1.0|ruby:>=2.7
`

	checksums := ParseInfoChecksums([]byte(content))
	require.Empty(t, checksums)
}

func TestParseInfoChecksums_EmptyDeps(t *testing.T) {
	// Line with no dependencies, only requirements
	content := `---
1.0.0 |checksum:abc123
`

	checksums := ParseInfoChecksums([]byte(content))
	require.Len(t, checksums, 1)
	require.Equal(t, "abc123", checksums["1.0.0"])
}

func TestParseVersionsMD5(t *testing.T) {
	content := `created_at: 2024-01-01T00:00:00Z
---
rails 7.0.0,7.0.1,7.1.0 abc123def456
nokogiri 1.15.0,1.15.1 111222333444
activesupport 7.0.0 555666777888
`

	md5s := ParseVersionsMD5([]byte(content))

	require.Len(t, md5s, 3)
	require.Equal(t, "abc123def456", md5s["rails"])
	require.Equal(t, "111222333444", md5s["nokogiri"])
	require.Equal(t, "555666777888", md5s["activesupport"])
}

func TestParseVersionsMD5_AppendedUpdates(t *testing.T) {
	// When a gem is updated, a new line is appended. We should use the last MD5.
	content := `---
rails 7.0.0 oldmd5hash11
rails 7.0.1 oldmd5hash22
rails 7.1.0 newmd5hash33
`

	md5s := ParseVersionsMD5([]byte(content))

	require.Len(t, md5s, 1)
	require.Equal(t, "newmd5hash33", md5s["rails"])
}

func TestParseVersionsMD5_YankedVersions(t *testing.T) {
	// Yanked versions are prefixed with -
	content := `---
rails 7.0.0,-7.0.1,7.1.0 abc123def456
`

	md5s := ParseVersionsMD5([]byte(content))

	require.Len(t, md5s, 1)
	require.Equal(t, "abc123def456", md5s["rails"])
}
