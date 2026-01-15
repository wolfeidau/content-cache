package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

const ociPrefix = "oci"

// ErrNotFound is returned when a resource is not found in the index.
var ErrNotFound = errors.New("not found")

// Index manages the OCI image cache index.
type Index struct {
	backend backend.Backend
	mu      sync.RWMutex
	now     func() time.Time
}

// NewIndex creates a new OCI index.
func NewIndex(b backend.Backend) *Index {
	return &Index{
		backend: b,
		now:     time.Now,
	}
}

// GetTagDigest returns the cached digest for a tag and when it was last refreshed.
func (idx *Index) GetTagDigest(ctx context.Context, name, tag string) (string, time.Time, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	image, err := idx.getImage(ctx, name)
	if err != nil {
		return "", time.Time{}, err
	}

	cachedTag, ok := image.Tags[tag]
	if !ok {
		return "", time.Time{}, ErrNotFound
	}

	return cachedTag.Digest, cachedTag.RefreshedAt, nil
}

// SetTagDigest updates the tag->digest mapping for an image.
func (idx *Index) SetTagDigest(ctx context.Context, name, tag, digest string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	image, err := idx.getImage(ctx, name)
	if errors.Is(err, ErrNotFound) {
		image = &CachedImage{
			Name:      name,
			Tags:      make(map[string]*CachedTag),
			CachedAt:  idx.now(),
			UpdatedAt: idx.now(),
		}
	} else if err != nil {
		return err
	}

	now := idx.now()
	image.Tags[tag] = &CachedTag{
		Tag:         tag,
		Digest:      digest,
		CachedAt:    now,
		RefreshedAt: now,
	}
	image.UpdatedAt = now

	return idx.putImage(ctx, name, image)
}

// RefreshTag updates the RefreshedAt timestamp for a tag without changing the digest.
func (idx *Index) RefreshTag(ctx context.Context, name, tag string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	image, err := idx.getImage(ctx, name)
	if err != nil {
		return err
	}

	cachedTag, ok := image.Tags[tag]
	if !ok {
		return ErrNotFound
	}

	cachedTag.RefreshedAt = idx.now()
	image.UpdatedAt = idx.now()

	return idx.putImage(ctx, name, image)
}

// GetManifest returns the cached manifest metadata by digest.
func (idx *Index) GetManifest(ctx context.Context, digest string) (*CachedManifest, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := idx.manifestKey(digest)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		return nil, ErrNotFound
	}
	defer func() { _ = rc.Close() }()

	var manifest CachedManifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}

	return &manifest, nil
}

// PutManifest stores manifest metadata in the index.
func (idx *Index) PutManifest(ctx context.Context, digest, mediaType string, hash contentcache.Hash, size int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	manifest := &CachedManifest{
		Digest:      digest,
		MediaType:   mediaType,
		ContentHash: hash,
		Size:        size,
		CachedAt:    idx.now(),
	}

	key := idx.manifestKey(digest)
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encoding manifest: %w", err)
	}

	return idx.backend.Write(ctx, key, strings.NewReader(string(data)))
}

// GetBlob returns the cached blob metadata by digest.
func (idx *Index) GetBlob(ctx context.Context, digest string) (*CachedBlob, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := idx.blobKey(digest)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		return nil, ErrNotFound
	}
	defer func() { _ = rc.Close() }()

	var blob CachedBlob
	if err := json.NewDecoder(rc).Decode(&blob); err != nil {
		return nil, fmt.Errorf("decoding blob: %w", err)
	}

	return &blob, nil
}

// PutBlob stores blob metadata in the index.
func (idx *Index) PutBlob(ctx context.Context, digest string, hash contentcache.Hash, size int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	blob := &CachedBlob{
		Digest:      digest,
		ContentHash: hash,
		Size:        size,
		CachedAt:    idx.now(),
	}

	key := idx.blobKey(digest)
	data, err := json.Marshal(blob)
	if err != nil {
		return fmt.Errorf("encoding blob: %w", err)
	}

	return idx.backend.Write(ctx, key, strings.NewReader(string(data)))
}

// ListImages returns a list of all cached image names.
func (idx *Index) ListImages(ctx context.Context) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	prefix := path.Join(ociPrefix, "images") + "/"
	keys, err := idx.backend.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing images: %w", err)
	}

	var names []string
	for _, key := range keys {
		// Remove prefix and .json suffix
		name := strings.TrimPrefix(key, prefix)
		name = strings.TrimSuffix(name, ".json")
		decoded, err := decodeImageName(name)
		if err != nil {
			continue
		}
		names = append(names, decoded)
	}

	return names, nil
}

// getImage reads image metadata from storage.
func (idx *Index) getImage(ctx context.Context, name string) (*CachedImage, error) {
	key := idx.imageKey(name)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		return nil, ErrNotFound
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading image: %w", err)
	}

	var image CachedImage
	if err := json.Unmarshal(data, &image); err != nil {
		return nil, fmt.Errorf("decoding image: %w", err)
	}

	return &image, nil
}

// putImage writes image metadata to storage.
func (idx *Index) putImage(ctx context.Context, name string, image *CachedImage) error {
	key := idx.imageKey(name)
	data, err := json.Marshal(image)
	if err != nil {
		return fmt.Errorf("encoding image: %w", err)
	}

	return idx.backend.Write(ctx, key, strings.NewReader(string(data)))
}

// imageKey returns the storage key for an image.
func (idx *Index) imageKey(name string) string {
	return path.Join(ociPrefix, "images", encodeImageName(name)+".json")
}

// manifestKey returns the storage key for a manifest.
func (idx *Index) manifestKey(digest string) string {
	d, err := ParseDigest(digest)
	if err != nil {
		// Fallback to using the raw digest
		return path.Join(ociPrefix, "manifests", digest+".json")
	}
	return path.Join(ociPrefix, "manifests", d.Algorithm, d.Hex+".json")
}

// blobKey returns the storage key for a blob.
func (idx *Index) blobKey(digest string) string {
	d, err := ParseDigest(digest)
	if err != nil {
		// Fallback to using the raw digest
		return path.Join(ociPrefix, "blobs", digest+".json")
	}
	return path.Join(ociPrefix, "blobs", d.Algorithm, d.Hex+".json")
}

// encodeImageName encodes an image name for use in storage keys.
// Replaces forward slashes with %2F to create flat key structure.
func encodeImageName(name string) string {
	return url.PathEscape(name)
}

// decodeImageName decodes a storage key back to an image name.
func decodeImageName(encoded string) (string, error) {
	return url.PathUnescape(encoded)
}
