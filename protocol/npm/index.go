package npm

import (
	"context"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/store/metadb"
)

const (
	// Key prefixes for npm metadata in metadb
	prefixMetadata = "metadata/" // Raw JSON metadata from upstream
	prefixCache    = "cache/"    // CachedPackage with tarball hashes
)

// Index manages the NPM package index using metadb.
type Index struct {
	db  *metadb.Index
	mu  sync.RWMutex
	now func() time.Time
}

// NewIndex creates a new NPM package index.
func NewIndex(db *metadb.Index) *Index {
	return &Index{
		db:  db,
		now: time.Now,
	}
}

// GetPackageMetadata retrieves cached metadata for a package.
func (idx *Index) GetPackageMetadata(ctx context.Context, name string) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := prefixMetadata + encodePackageName(name)
	data, err := idx.db.Get(ctx, key)
	if err != nil {
		if err == metadb.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

// PutPackageMetadata stores package metadata.
func (idx *Index) PutPackageMetadata(ctx context.Context, name string, metadata []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key := prefixMetadata + encodePackageName(name)
	return idx.db.Put(ctx, key, metadata)
}

// GetCachedPackage retrieves the cached package info including tarball hashes.
func (idx *Index) GetCachedPackage(ctx context.Context, name string) (*CachedPackage, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := prefixCache + encodePackageName(name)
	var cached CachedPackage
	if err := idx.db.GetJSON(ctx, key, &cached); err != nil {
		if err == metadb.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &cached, nil
}

// PutCachedPackage stores cached package info with blob references.
func (idx *Index) PutCachedPackage(ctx context.Context, pkg *CachedPackage) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	pkg.UpdatedAt = idx.now()
	if pkg.CachedAt.IsZero() {
		pkg.CachedAt = pkg.UpdatedAt
	}

	key := prefixCache + encodePackageName(pkg.Name)

	// Collect all blob refs from cached versions
	refs := collectBlobRefs(pkg)

	return idx.db.PutJSONWithRefs(ctx, key, pkg, refs)
}

// GetVersionTarballHash retrieves the tarball hash for a specific version.
func (idx *Index) GetVersionTarballHash(ctx context.Context, name, version string) (contentcache.Hash, error) {
	cached, err := idx.GetCachedPackage(ctx, name)
	if err != nil {
		return contentcache.Hash{}, err
	}

	ver, ok := cached.Versions[version]
	if !ok {
		return contentcache.Hash{}, ErrNotFound
	}

	return ver.TarballHash, nil
}

// SetVersionTarballHash stores the tarball hash for a specific version.
func (idx *Index) SetVersionTarballHash(ctx context.Context, name, version string, hash contentcache.Hash, size int64, shasum, integrity string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key := prefixCache + encodePackageName(name)

	// Get or create cached package
	var cached CachedPackage
	_ = idx.db.GetJSON(ctx, key, &cached) // Ignore not found

	if cached.Name == "" {
		cached.Name = name
		cached.CachedAt = idx.now()
	}
	if cached.Versions == nil {
		cached.Versions = make(map[string]*CachedVersion)
	}

	cached.Versions[version] = &CachedVersion{
		Version:     version,
		TarballHash: hash,
		TarballSize: size,
		Shasum:      shasum,
		Integrity:   integrity,
		CachedAt:    idx.now(),
	}
	cached.UpdatedAt = idx.now()

	// Collect all blob refs
	refs := collectBlobRefs(&cached)

	return idx.db.PutJSONWithRefs(ctx, key, &cached, refs)
}

// DeletePackage removes all cached data for a package.
func (idx *Index) DeletePackage(ctx context.Context, name string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	encodedName := encodePackageName(name)

	// Delete metadata
	if err := idx.db.Delete(ctx, prefixMetadata+encodedName); err != nil && err != metadb.ErrNotFound {
		return err
	}

	// Delete cache info (with blob refs cleanup)
	if err := idx.db.DeleteWithRefs(ctx, prefixCache+encodedName); err != nil && err != metadb.ErrNotFound {
		return err
	}

	return nil
}

// ListPackages returns all cached package names.
func (idx *Index) ListPackages(ctx context.Context) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, err := idx.db.List(ctx)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, key := range keys {
		// Only include cache keys (not metadata keys)
		if len(key) > len(prefixCache) && key[:len(prefixCache)] == prefixCache {
			encodedName := key[len(prefixCache):]
			if decoded, err := decodePackageName(encodedName); err == nil {
				names = append(names, decoded)
			}
		}
	}

	return names, nil
}

// collectBlobRefs extracts all blob hashes from a CachedPackage.
func collectBlobRefs(pkg *CachedPackage) []string {
	if pkg == nil || pkg.Versions == nil {
		return nil
	}

	refs := make([]string, 0, len(pkg.Versions))
	for _, ver := range pkg.Versions {
		if ver != nil && !ver.TarballHash.IsZero() {
			refs = append(refs, ver.TarballHash.String())
		}
	}
	return refs
}
