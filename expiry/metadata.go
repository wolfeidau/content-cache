// Package expiry provides TTL and LRU-based cache expiration.
package expiry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

const (
	// metadataPrefix is the storage prefix for blob metadata.
	metadataPrefix = "metadata"
)

// BlobMetadata contains metadata about a stored blob.
type BlobMetadata struct {
	Hash         contentcache.Hash `json:"hash"`
	Size         int64             `json:"size"`
	CreatedAt    time.Time         `json:"created_at"`
	LastAccessed time.Time         `json:"last_accessed"`
}

// MetadataStore manages blob metadata for expiration tracking.
type MetadataStore struct {
	backend backend.Backend
	mu      sync.RWMutex
	now     func() time.Time // For testing
}

// NewMetadataStore creates a new metadata store.
func NewMetadataStore(b backend.Backend) *MetadataStore {
	return &MetadataStore{
		backend: b,
		now:     time.Now,
	}
}

// Get retrieves metadata for a blob.
func (m *MetadataStore) Get(ctx context.Context, hash contentcache.Hash) (*BlobMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.hashToKey(hash)
	rc, err := m.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, backend.ErrNotFound
		}
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var meta BlobMetadata
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}

	return &meta, nil
}

// Put stores metadata for a blob.
func (m *MetadataStore) Put(ctx context.Context, meta *BlobMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.putLocked(ctx, meta)
}

func (m *MetadataStore) putLocked(ctx context.Context, meta *BlobMetadata) error {
	key := m.hashToKey(meta.Hash)
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}

	if err := m.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}

	return nil
}

// Touch updates the last accessed time for a blob.
func (m *MetadataStore) Touch(ctx context.Context, hash contentcache.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.hashToKey(hash)
	rc, err := m.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return backend.ErrNotFound
		}
		return fmt.Errorf("reading metadata: %w", err)
	}

	var meta BlobMetadata
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		_ = rc.Close()
		return fmt.Errorf("decoding metadata: %w", err)
	}
	_ = rc.Close()

	meta.LastAccessed = m.now()
	return m.putLocked(ctx, &meta)
}

// Delete removes metadata for a blob.
func (m *MetadataStore) Delete(ctx context.Context, hash contentcache.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.hashToKey(hash)
	return m.backend.Delete(ctx, key)
}

// Create stores metadata for a newly created blob.
func (m *MetadataStore) Create(ctx context.Context, hash contentcache.Hash, size int64) error {
	now := m.now()
	meta := &BlobMetadata{
		Hash:         hash,
		Size:         size,
		CreatedAt:    now,
		LastAccessed: now,
	}
	return m.Put(ctx, meta)
}

// List returns all blob metadata, sorted by last accessed time (oldest first).
func (m *MetadataStore) List(ctx context.Context) ([]*BlobMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys, err := m.backend.List(ctx, metadataPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing metadata: %w", err)
	}

	var results []*BlobMetadata
	for _, key := range keys {
		rc, err := m.backend.Read(ctx, key)
		if err != nil {
			continue // Skip on error
		}

		var meta BlobMetadata
		if err := json.NewDecoder(rc).Decode(&meta); err != nil {
			_ = rc.Close()
			continue
		}
		_ = rc.Close()

		results = append(results, &meta)
	}

	return results, nil
}

// Stats returns aggregate statistics about stored blobs.
type Stats struct {
	TotalBlobs int64
	TotalSize  int64
	OldestBlob time.Time
	NewestBlob time.Time
}

// GetStats returns aggregate statistics.
func (m *MetadataStore) GetStats(ctx context.Context) (*Stats, error) {
	blobs, err := m.List(ctx)
	if err != nil {
		return nil, err
	}

	stats := &Stats{}
	for _, meta := range blobs {
		stats.TotalBlobs++
		stats.TotalSize += meta.Size

		if stats.OldestBlob.IsZero() || meta.LastAccessed.Before(stats.OldestBlob) {
			stats.OldestBlob = meta.LastAccessed
		}
		if meta.LastAccessed.After(stats.NewestBlob) {
			stats.NewestBlob = meta.LastAccessed
		}
	}

	return stats, nil
}

func (m *MetadataStore) hashToKey(hash contentcache.Hash) string {
	return fmt.Sprintf("%s/%s", metadataPrefix, hash.String())
}

// TrackedStore wraps a store to automatically track metadata.
type TrackedStore struct {
	backend  backend.Backend
	metadata *MetadataStore
}

// NewTrackedStore creates a store that tracks blob metadata.
func NewTrackedStore(b backend.Backend, meta *MetadataStore) *TrackedStore {
	return &TrackedStore{
		backend:  b,
		metadata: meta,
	}
}

// Put stores content and tracks its metadata.
func (ts *TrackedStore) Put(ctx context.Context, r io.Reader) (contentcache.Hash, error) {
	// Read content to compute hash and get size
	hr := contentcache.NewHashingReader(r)
	data, err := io.ReadAll(hr)
	if err != nil {
		return contentcache.Hash{}, fmt.Errorf("reading content: %w", err)
	}

	hash := hr.Sum()
	size := hr.BytesRead()

	// Check if already exists
	exists, err := ts.backend.Exists(ctx, contentcache.BlobStorageKey(hash))
	if err != nil {
		return contentcache.Hash{}, fmt.Errorf("checking existence: %w", err)
	}

	if exists {
		// Just update access time (best effort)
		_ = ts.metadata.Touch(ctx, hash)
		return hash, nil
	}

	// Write blob
	if err := ts.backend.Write(ctx, contentcache.BlobStorageKey(hash), strings.NewReader(string(data))); err != nil {
		return contentcache.Hash{}, fmt.Errorf("writing blob: %w", err)
	}

	// Create metadata (best effort - don't fail the put)
	_ = ts.metadata.Create(ctx, hash, size)

	return hash, nil
}

// Get retrieves content and updates access time.
func (ts *TrackedStore) Get(ctx context.Context, hash contentcache.Hash) (io.ReadCloser, error) {
	rc, err := ts.backend.Read(ctx, contentcache.BlobStorageKey(hash))
	if err != nil {
		return nil, err
	}

	// Update access time (best effort)
	go func() { _ = ts.metadata.Touch(context.Background(), hash) }()

	return rc, nil
}

// Has checks if content exists.
func (ts *TrackedStore) Has(ctx context.Context, hash contentcache.Hash) (bool, error) {
	return ts.backend.Exists(ctx, contentcache.BlobStorageKey(hash))
}

// Delete removes content and its metadata.
func (ts *TrackedStore) Delete(ctx context.Context, hash contentcache.Hash) error {
	if err := ts.backend.Delete(ctx, contentcache.BlobStorageKey(hash)); err != nil {
		return err
	}
	return ts.metadata.Delete(ctx, hash)
}

// Size returns the size of content.
func (ts *TrackedStore) Size(ctx context.Context, hash contentcache.Hash) (int64, error) {
	meta, err := ts.metadata.Get(ctx, hash)
	if err == nil {
		return meta.Size, nil
	}

	// Fall back to reading from backend
	if sb, ok := ts.backend.(backend.SizeAwareBackend); ok {
		return sb.Size(ctx, contentcache.BlobStorageKey(hash))
	}

	return 0, backend.ErrNotFound
}

// Metadata returns the metadata store.
func (ts *TrackedStore) Metadata() *MetadataStore {
	return ts.metadata
}

