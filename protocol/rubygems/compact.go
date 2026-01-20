package rubygems

import (
	"bufio"
	"bytes"
	"strings"
)

// ParseInfoChecksums parses the /info/{gem} file content and extracts checksums.
// Returns a map of version[-platform] -> SHA256 checksum.
//
// Info file format (one line per version):
// VERSION[-PLATFORM] DEPENDENCIES|REQUIREMENTS
//
// The REQUIREMENTS section contains checksum:sha256hash
// Example line:
// 1.15.4-x86_64-linux mini_portile2:~>2.8.2|checksum:abc123...,ruby:>=2.7
func ParseInfoChecksums(content []byte) map[string]string {
	checksums := make(map[string]string)

	scanner := bufio.NewScanner(bytes.NewReader(content))
	inHeader := true

	for scanner.Scan() {
		line := scanner.Text()

		// Skip header lines (before ---)
		if inHeader {
			if line == "---" {
				inHeader = false
			}
			continue
		}

		// Skip empty lines
		if line == "" {
			continue
		}

		// Parse: VERSION[-PLATFORM] DEPS|REQUIREMENTS
		parts := strings.SplitN(line, " ", 2)
		if len(parts) < 2 {
			continue
		}

		versionPlatform := parts[0]
		rest := parts[1]

		// Find the pipe separator between dependencies and requirements
		pipeIdx := strings.LastIndex(rest, "|")
		if pipeIdx == -1 {
			continue
		}

		requirements := rest[pipeIdx+1:]

		// Parse requirements: comma-separated key:value pairs
		// Example: checksum:abc123,ruby:>=2.7,rubygems:>=3.0
		for _, req := range strings.Split(requirements, ",") {
			if strings.HasPrefix(req, "checksum:") {
				checksum := strings.TrimPrefix(req, "checksum:")
				checksums[versionPlatform] = checksum
				break
			}
		}
	}

	return checksums
}

// ParseVersionsMD5 parses the /versions file and extracts MD5 hashes for each gem.
// Returns a map of gem name -> MD5 hash (the most recent/last one for each gem).
//
// Versions file format:
// created_at: timestamp
// ---
// GEMNAME VERSION1,VERSION2,... MD5
//
// Note: A gem can appear multiple times (append-only); use the last MD5 seen.
func ParseVersionsMD5(content []byte) map[string]string {
	md5s := make(map[string]string)

	scanner := bufio.NewScanner(bytes.NewReader(content))
	inHeader := true

	for scanner.Scan() {
		line := scanner.Text()

		// Skip header lines (before ---)
		if inHeader {
			if line == "---" {
				inHeader = false
			}
			continue
		}

		// Skip empty lines
		if line == "" {
			continue
		}

		// Parse: GEMNAME VERSIONS MD5
		// The versions section can contain multiple versions and - prefixes for yanked
		parts := strings.Split(line, " ")
		if len(parts) < 3 {
			continue
		}

		gemName := parts[0]
		md5 := parts[len(parts)-1] // MD5 is always last

		md5s[gemName] = md5
	}

	return md5s
}
