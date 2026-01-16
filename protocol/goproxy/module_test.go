package goproxy

import (
	"context"
	"errors"
	"testing"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

func TestEncodePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"github.com/pkg/errors", "github.com/pkg/errors"},
		{"github.com/Azure/azure-sdk", "github.com/!azure/azure-sdk"},
		{"github.com/BurntSushi/toml", "github.com/!burnt!sushi/toml"},
		{"golang.org/x/mod", "golang.org/x/mod"},
		{"Example.COM/Foo/Bar", "!example.!c!o!m/!foo/!bar"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := encodePath(tt.input)
			if got != tt.expected {
				t.Errorf("encodePath(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestDecodePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		wantErr  bool
	}{
		{"github.com/pkg/errors", "github.com/pkg/errors", false},
		{"github.com/!azure/azure-sdk", "github.com/Azure/azure-sdk", false},
		{"github.com/!burnt!sushi/toml", "github.com/BurntSushi/toml", false},
		{"invalid!", "", true},  // Trailing escape
		{"invalid!1", "", true}, // Invalid escape sequence
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := decodePath(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodePath(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("decodePath(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	paths := []string{
		"github.com/pkg/errors",
		"github.com/Azure/azure-sdk-for-go",
		"github.com/BurntSushi/toml",
		"golang.org/x/mod",
		"Example.COM/Foo/Bar",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			encoded := encodePath(path)
			decoded, err := decodePath(encoded)
			if err != nil {
				t.Fatalf("decodePath() error = %v", err)
			}
			if decoded != path {
				t.Errorf("round-trip failed: got %q, want %q", decoded, path)
			}
		})
	}
}

func TestIndexListVersions(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	modulePath := "github.com/test/module"

	// Initially empty
	versions, err := idx.ListVersions(ctx, modulePath)
	if err != nil {
		t.Fatalf("ListVersions() error = %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("ListVersions() = %v, want empty", versions)
	}

	// Add some versions
	mv := &ModuleVersion{
		Info: VersionInfo{
			Version: "v1.0.0",
			Time:    time.Now(),
		},
		ZipHash: contentcache.HashBytes([]byte("fake zip")),
	}

	for _, v := range []string{"v1.0.0", "v1.0.1", "v1.1.0"} {
		mv.Info.Version = v
		if err := idx.PutModuleVersion(ctx, modulePath, v, mv, []byte("module test")); err != nil {
			t.Fatalf("PutModuleVersion(%s) error = %v", v, err)
		}
	}

	// List should return all versions
	versions, err = idx.ListVersions(ctx, modulePath)
	if err != nil {
		t.Fatalf("ListVersions() error = %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("ListVersions() returned %d versions, want 3", len(versions))
	}
}

func TestIndexGetVersionInfo(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	modulePath := "github.com/test/module"
	version := "v1.2.3"

	// Not found initially
	_, err := idx.GetVersionInfo(ctx, modulePath, version)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetVersionInfo() error = %v, want ErrNotFound", err)
	}

	// Store version
	now := time.Now().Truncate(time.Second)
	mv := &ModuleVersion{
		Info: VersionInfo{
			Version: version,
			Time:    now,
		},
		ZipHash: contentcache.HashBytes([]byte("zip content")),
	}
	modFile := []byte("module github.com/test/module\n\ngo 1.21\n")

	if err := idx.PutModuleVersion(ctx, modulePath, version, mv, modFile); err != nil {
		t.Fatalf("PutModuleVersion() error = %v", err)
	}

	// Get version info
	info, err := idx.GetVersionInfo(ctx, modulePath, version)
	if err != nil {
		t.Fatalf("GetVersionInfo() error = %v", err)
	}

	if info.Version != version {
		t.Errorf("Version = %q, want %q", info.Version, version)
	}
	if !info.Time.Equal(now) {
		t.Errorf("Time = %v, want %v", info.Time, now)
	}
}

func TestIndexGetMod(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	modulePath := "github.com/test/module"
	version := "v1.0.0"
	modContent := []byte("module github.com/test/module\n\ngo 1.21\n")

	// Store version with mod file
	mv := &ModuleVersion{
		Info: VersionInfo{
			Version: version,
			Time:    time.Now(),
		},
		ZipHash: contentcache.HashBytes([]byte("zip")),
	}

	if err := idx.PutModuleVersion(ctx, modulePath, version, mv, modContent); err != nil {
		t.Fatalf("PutModuleVersion() error = %v", err)
	}

	// Get mod file
	got, err := idx.GetMod(ctx, modulePath, version)
	if err != nil {
		t.Fatalf("GetMod() error = %v", err)
	}

	if string(got) != string(modContent) {
		t.Errorf("GetMod() = %q, want %q", got, modContent)
	}
}

func TestIndexDeleteModuleVersion(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	modulePath := "github.com/test/module"
	version := "v1.0.0"

	// Store version
	mv := &ModuleVersion{
		Info:    VersionInfo{Version: version},
		ZipHash: contentcache.HashBytes([]byte("zip")),
	}
	if err := idx.PutModuleVersion(ctx, modulePath, version, mv, []byte("mod")); err != nil {
		t.Fatalf("PutModuleVersion() error = %v", err)
	}

	// Verify it exists
	_, err := idx.GetVersionInfo(ctx, modulePath, version)
	if err != nil {
		t.Fatalf("GetVersionInfo() error = %v", err)
	}

	// Delete it
	if err := idx.DeleteModuleVersion(ctx, modulePath, version); err != nil {
		t.Fatalf("DeleteModuleVersion() error = %v", err)
	}

	// Verify it's gone
	_, err = idx.GetVersionInfo(ctx, modulePath, version)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetVersionInfo() error = %v, want ErrNotFound", err)
	}

	// Version list should be empty
	versions, _ := idx.ListVersions(ctx, modulePath)
	if len(versions) != 0 {
		t.Errorf("ListVersions() = %v, want empty", versions)
	}
}

func TestIndexCaseInsensitiveModule(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Test with uppercase module path
	modulePath := "github.com/Azure/azure-sdk-for-go"
	version := "v1.0.0"

	mv := &ModuleVersion{
		Info:    VersionInfo{Version: version},
		ZipHash: contentcache.HashBytes([]byte("zip")),
	}
	if err := idx.PutModuleVersion(ctx, modulePath, version, mv, []byte("mod")); err != nil {
		t.Fatalf("PutModuleVersion() error = %v", err)
	}

	// Should be retrievable with same path
	_, err := idx.GetVersionInfo(ctx, modulePath, version)
	if err != nil {
		t.Fatalf("GetVersionInfo() error = %v", err)
	}
}

// Helper functions

func newTestIndex(t *testing.T) (*Index, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	return NewIndex(b), func() {}
}
