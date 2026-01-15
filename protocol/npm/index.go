package npm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

const (
	// npmPrefix is the storage prefix for NPM package data.
	npmPrefix = "npm"
)

// Index manages the NPM package index.
type Index struct {
	backend backend.Backend
	mu      sync.RWMutex
	now     func() time.Time
}

// NewIndex creates a new NPM package index.
func NewIndex(b backend.Backend) *Index {
	return &Index{
		backend: b,
		now:     time.Now,
	}
}

// GetPackageMetadata retrieves cached metadata for a package.
func (idx *Index) GetPackageMetadata(ctx context.Context, name string) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := idx.metadataKey(name)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	defer func() { _ = rc.Close() }()

	return io.ReadAll(rc)
}

// PutPackageMetadata stores package metadata.
func (idx *Index) PutPackageMetadata(ctx context.Context, name string, metadata []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key := idx.metadataKey(name)
	if err := idx.backend.Write(ctx, key, strings.NewReader(string(metadata))); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}

	return nil
}

// GetCachedPackage retrieves the cached package info including tarball hashes.
func (idx *Index) GetCachedPackage(ctx context.Context, name string) (*CachedPackage, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := idx.cacheKey(name)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading cache info: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var cached CachedPackage
	if err := json.NewDecoder(rc).Decode(&cached); err != nil {
		return nil, fmt.Errorf("decoding cache info: %w", err)
	}

	return &cached, nil
}

// PutCachedPackage stores cached package info.
func (idx *Index) PutCachedPackage(ctx context.Context, pkg *CachedPackage) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	pkg.UpdatedAt = idx.now()
	if pkg.CachedAt.IsZero() {
		pkg.CachedAt = pkg.UpdatedAt
	}

	key := idx.cacheKey(pkg.Name)
	data, err := json.Marshal(pkg)
	if err != nil {
		return fmt.Errorf("encoding cache info: %w", err)
	}

	if err := idx.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing cache info: %w", err)
	}

	return nil
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

	// Get or create cached package
	key := idx.cacheKey(name)
	var cached CachedPackage

	rc, err := idx.backend.Read(ctx, key)
	if err == nil {
		_ = json.NewDecoder(rc).Decode(&cached)
		_ = rc.Close()
	}

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

	data, err := json.Marshal(&cached)
	if err != nil {
		return fmt.Errorf("encoding cache info: %w", err)
	}

	if err := idx.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing cache info: %w", err)
	}

	return nil
}

// DeletePackage removes all cached data for a package.
func (idx *Index) DeletePackage(ctx context.Context, name string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Delete metadata
	if err := idx.backend.Delete(ctx, idx.metadataKey(name)); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return fmt.Errorf("deleting metadata: %w", err)
	}

	// Delete cache info
	if err := idx.backend.Delete(ctx, idx.cacheKey(name)); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return fmt.Errorf("deleting cache info: %w", err)
	}

	return nil
}

// ListPackages returns all cached package names.
func (idx *Index) ListPackages(ctx context.Context) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, err := idx.backend.List(ctx, path.Join(npmPrefix, "cache"))
	if err != nil {
		return nil, fmt.Errorf("listing packages: %w", err)
	}

	var names []string
	for _, key := range keys {
		// Extract package name from key
		// Key format: npm/cache/{encoded-name}.json
		name := strings.TrimPrefix(key, path.Join(npmPrefix, "cache")+"/")
		name = strings.TrimSuffix(name, ".json")
		if decoded, err := decodePackageName(name); err == nil {
			names = append(names, decoded)
		}
	}

	return names, nil
}

// Key generation helpers

func (idx *Index) metadataKey(name string) string {
	return path.Join(npmPrefix, "metadata", encodePackageName(name)+".json")
}

func (idx *Index) cacheKey(name string) string {
	return path.Join(npmPrefix, "cache", encodePackageName(name)+".json")
}
