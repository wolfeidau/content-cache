package npm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

func newTestIndex(t *testing.T) (*Index, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)
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
	require.ErrorIs(t, err, ErrNotFound)

	// Put metadata
	err = idx.PutPackageMetadata(ctx, name, metadata)
	require.NoError(t, err)

	// Get should succeed
	got, err := idx.GetPackageMetadata(ctx, name)
	require.NoError(t, err)
	require.Equal(t, string(metadata), string(got))
}

func TestIndexScopedPackageMetadata(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()
	name := "@babel/core"
	metadata := []byte(`{"name":"@babel/core","version":"7.23.0"}`)

	// Put metadata for scoped package
	err := idx.PutPackageMetadata(ctx, name, metadata)
	require.NoError(t, err)

	// Get should succeed
	got, err := idx.GetPackageMetadata(ctx, name)
	require.NoError(t, err)
	require.Equal(t, string(metadata), string(got))
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
	err := idx.PutCachedPackage(ctx, pkg)
	require.NoError(t, err)

	// Get should succeed
	got, err := idx.GetCachedPackage(ctx, name)
	require.NoError(t, err)
	require.Equal(t, name, got.Name)
	require.Equal(t, int64(100), got.Versions["1.0.0"].TarballSize)
	require.False(t, got.CachedAt.IsZero())
	require.False(t, got.UpdatedAt.IsZero())
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
	require.ErrorIs(t, err, ErrNotFound)

	// Set tarball hash
	err = idx.SetVersionTarballHash(ctx, name, version, hash, 1024, "abc123", "sha512-xyz")
	require.NoError(t, err)

	// Get should succeed
	gotHash, err := idx.GetVersionTarballHash(ctx, name, version)
	require.NoError(t, err)
	require.Equal(t, hash, gotHash)

	// Verify cached package was created
	cached, err := idx.GetCachedPackage(ctx, name)
	require.NoError(t, err)
	require.Equal(t, "abc123", cached.Versions[version].Shasum)
	require.Equal(t, "sha512-xyz", cached.Versions[version].Integrity)
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
	err := idx.DeletePackage(ctx, name)
	require.NoError(t, err)

	// Metadata should be gone
	_, err = idx.GetPackageMetadata(ctx, name)
	require.ErrorIs(t, err, ErrNotFound)

	// Cached package should be gone
	_, err = idx.GetCachedPackage(ctx, name)
	require.ErrorIs(t, err, ErrNotFound)
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
	require.NoError(t, err)
	require.Len(t, got, len(packages))

	// Verify all packages are listed
	gotMap := make(map[string]bool)
	for _, name := range got {
		gotMap[name] = true
	}
	for _, name := range packages {
		require.True(t, gotMap[name])
	}
}
