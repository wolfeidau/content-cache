package npm

import (
	"context"
	"testing"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

func newTestIndex(t *testing.T) (*Index, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	return NewIndex(b), func() {}
}

func TestIndexPackageMetadata(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	name := "lodash"
	metadata := []byte(`{"name":"lodash","version":"4.17.21"}`)

	// Get should fail initially
	_, err := idx.GetPackageMetadata(ctx, name)
	if err != ErrNotFound {
		t.Errorf("GetPackageMetadata() error = %v, want ErrNotFound", err)
	}

	// Put metadata
	if err := idx.PutPackageMetadata(ctx, name, metadata); err != nil {
		t.Fatalf("PutPackageMetadata() error = %v", err)
	}

	// Get should succeed
	got, err := idx.GetPackageMetadata(ctx, name)
	if err != nil {
		t.Fatalf("GetPackageMetadata() error = %v", err)
	}
	if string(got) != string(metadata) {
		t.Errorf("GetPackageMetadata() = %q, want %q", got, metadata)
	}
}

func TestIndexScopedPackageMetadata(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	name := "@babel/core"
	metadata := []byte(`{"name":"@babel/core","version":"7.23.0"}`)

	// Put metadata for scoped package
	if err := idx.PutPackageMetadata(ctx, name, metadata); err != nil {
		t.Fatalf("PutPackageMetadata() error = %v", err)
	}

	// Get should succeed
	got, err := idx.GetPackageMetadata(ctx, name)
	if err != nil {
		t.Fatalf("GetPackageMetadata() error = %v", err)
	}
	if string(got) != string(metadata) {
		t.Errorf("GetPackageMetadata() = %q, want %q", got, metadata)
	}
}

func TestIndexCachedPackage(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	name := "test-pkg"

	pkg := &CachedPackage{
		Name: name,
		Versions: map[string]*CachedVersion{
			"1.0.0": {
				Version:     "1.0.0",
				TarballHash: contentcache.HashBytes([]byte("v1")),
				TarballSize: 100,
			},
		},
	}

	// Put cached package
	if err := idx.PutCachedPackage(ctx, pkg); err != nil {
		t.Fatalf("PutCachedPackage() error = %v", err)
	}

	// Get should succeed
	got, err := idx.GetCachedPackage(ctx, name)
	if err != nil {
		t.Fatalf("GetCachedPackage() error = %v", err)
	}
	if got.Name != name {
		t.Errorf("Name = %q, want %q", got.Name, name)
	}
	if got.Versions["1.0.0"].TarballSize != 100 {
		t.Errorf("TarballSize = %d, want 100", got.Versions["1.0.0"].TarballSize)
	}
	if got.CachedAt.IsZero() {
		t.Error("CachedAt should not be zero")
	}
	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}
}

func TestIndexVersionTarballHash(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	name := "test-pkg"
	version := "1.0.0"
	hash := contentcache.HashBytes([]byte("test content"))

	// Get should fail initially
	_, err := idx.GetVersionTarballHash(ctx, name, version)
	if err != ErrNotFound {
		t.Errorf("GetVersionTarballHash() error = %v, want ErrNotFound", err)
	}

	// Set tarball hash
	if err := idx.SetVersionTarballHash(ctx, name, version, hash, 1024, "abc123", "sha512-xyz"); err != nil {
		t.Fatalf("SetVersionTarballHash() error = %v", err)
	}

	// Get should succeed
	gotHash, err := idx.GetVersionTarballHash(ctx, name, version)
	if err != nil {
		t.Fatalf("GetVersionTarballHash() error = %v", err)
	}
	if gotHash != hash {
		t.Errorf("GetVersionTarballHash() = %v, want %v", gotHash, hash)
	}

	// Verify cached package was created
	cached, err := idx.GetCachedPackage(ctx, name)
	if err != nil {
		t.Fatalf("GetCachedPackage() error = %v", err)
	}
	if cached.Versions[version].Shasum != "abc123" {
		t.Errorf("Shasum = %q, want abc123", cached.Versions[version].Shasum)
	}
	if cached.Versions[version].Integrity != "sha512-xyz" {
		t.Errorf("Integrity = %q, want sha512-xyz", cached.Versions[version].Integrity)
	}
}

func TestIndexDeletePackage(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	name := "delete-me"

	// Put metadata and cached package
	_ = idx.PutPackageMetadata(ctx, name, []byte(`{}`))
	_ = idx.SetVersionTarballHash(ctx, name, "1.0.0", contentcache.HashBytes([]byte("x")), 10, "", "")

	// Delete
	if err := idx.DeletePackage(ctx, name); err != nil {
		t.Fatalf("DeletePackage() error = %v", err)
	}

	// Metadata should be gone
	_, err := idx.GetPackageMetadata(ctx, name)
	if err != ErrNotFound {
		t.Errorf("GetPackageMetadata() error = %v, want ErrNotFound", err)
	}

	// Cached package should be gone
	_, err = idx.GetCachedPackage(ctx, name)
	if err != ErrNotFound {
		t.Errorf("GetCachedPackage() error = %v, want ErrNotFound", err)
	}
}

func TestIndexListPackages(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Create some packages
	packages := []string{"pkg-a", "pkg-b", "@scope/pkg-c"}
	for _, name := range packages {
		_ = idx.SetVersionTarballHash(ctx, name, "1.0.0", contentcache.HashBytes([]byte(name)), 10, "", "")
	}

	// List packages
	got, err := idx.ListPackages(ctx)
	if err != nil {
		t.Fatalf("ListPackages() error = %v", err)
	}

	if len(got) != len(packages) {
		t.Errorf("ListPackages() returned %d, want %d", len(got), len(packages))
	}

	// Verify all packages are listed
	gotMap := make(map[string]bool)
	for _, name := range got {
		gotMap[name] = true
	}
	for _, name := range packages {
		if !gotMap[name] {
			t.Errorf("ListPackages() missing %q", name)
		}
	}
}
