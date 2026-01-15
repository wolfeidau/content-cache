package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

const (
	// blobPrefix is the prefix for blob storage keys.
	blobPrefix = "blobs"
)

// MetadataTracker tracks blob metadata for expiration.
type MetadataTracker interface {
	// Create records metadata for a new blob.
	Create(ctx context.Context, hash contentcache.Hash, size int64) error
	// Touch updates the last access time for a blob.
	Touch(ctx context.Context, hash contentcache.Hash) error
	// Delete removes metadata for a blob.
	Delete(ctx context.Context, hash contentcache.Hash) error
}

// CAFS implements content-addressable file storage.
// Content is stored in a sharded directory structure based on hash.
type CAFS struct {
	backend  backend.Backend
	metadata MetadataTracker
}

// CAFSOption configures a CAFS instance.
type CAFSOption func(*CAFS)

// WithMetadataTracker sets a metadata tracker for expiration support.
func WithMetadataTracker(tracker MetadataTracker) CAFSOption {
	return func(c *CAFS) {
		c.metadata = tracker
	}
}

// NewCAFS creates a new content-addressable file store.
func NewCAFS(b backend.Backend, opts ...CAFSOption) *CAFS {
	c := &CAFS{backend: b}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Put stores content and returns its hash.
func (c *CAFS) Put(ctx context.Context, r io.Reader) (contentcache.Hash, error) {
	result, err := c.PutWithResult(ctx, r)
	if err != nil {
		return contentcache.Hash{}, err
	}
	return result.Hash, nil
}

// PutWithResult stores content and returns detailed information.
// Uses a temp file to avoid memory exhaustion for large content.
func (c *CAFS) PutWithResult(ctx context.Context, r io.Reader) (*PutResult, error) {
	// Create temp file for streaming content to avoid memory exhaustion
	tmpFile, err := os.CreateTemp("", "cafs-upload-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	// Stream content to temp file while computing hash
	hr := contentcache.NewHashingReader(r)
	if _, err := io.Copy(tmpFile, hr); err != nil {
		return nil, fmt.Errorf("reading content: %w", err)
	}

	hash := hr.Sum()
	size := hr.BytesRead()
	key := c.hashToKey(hash)

	// Check if content already exists
	exists, err := c.backend.Exists(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("checking existence: %w", err)
	}

	if exists {
		// Update access time if tracking metadata (best effort, don't fail the operation)
		if c.metadata != nil {
			_ = c.metadata.Touch(ctx, hash)
		}
		return &PutResult{
			Hash:   hash,
			Size:   size,
			Exists: true,
		}, nil
	}

	// Seek to beginning of temp file for writing to backend
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking temp file: %w", err)
	}

	// Write content to backend
	if err := c.backend.Write(ctx, key, tmpFile); err != nil {
		return nil, fmt.Errorf("writing content: %w", err)
	}

	// Track metadata for new blob (best effort, don't fail the operation)
	if c.metadata != nil {
		_ = c.metadata.Create(ctx, hash, size)
	}

	return &PutResult{
		Hash:   hash,
		Size:   size,
		Exists: false,
	}, nil
}

// PutBytes is a convenience method for storing bytes.
func (c *CAFS) PutBytes(ctx context.Context, data []byte) (contentcache.Hash, error) {
	return c.Put(ctx, bytes.NewReader(data))
}

// Get retrieves content by its hash.
func (c *CAFS) Get(ctx context.Context, h contentcache.Hash) (io.ReadCloser, error) {
	key := c.hashToKey(h)
	rc, err := c.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, backend.ErrNotFound
		}
		return nil, fmt.Errorf("reading content: %w", err)
	}

	// Update access time asynchronously (best effort)
	if c.metadata != nil {
		go func() { _ = c.metadata.Touch(context.Background(), h) }()
	}

	return rc, nil
}

// GetBytes is a convenience method for retrieving content as bytes.
func (c *CAFS) GetBytes(ctx context.Context, h contentcache.Hash) ([]byte, error) {
	rc, err := c.Get(ctx, h)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading content: %w", err)
	}
	return data, nil
}

// Has checks if content with the given hash exists.
func (c *CAFS) Has(ctx context.Context, h contentcache.Hash) (bool, error) {
	key := c.hashToKey(h)
	return c.backend.Exists(ctx, key)
}

// Delete removes content by its hash.
func (c *CAFS) Delete(ctx context.Context, h contentcache.Hash) error {
	key := c.hashToKey(h)
	if err := c.backend.Delete(ctx, key); err != nil {
		return err
	}

	// Clean up metadata (best effort)
	if c.metadata != nil {
		_ = c.metadata.Delete(ctx, h)
	}

	return nil
}

// Size returns the size of content with the given hash.
func (c *CAFS) Size(ctx context.Context, h contentcache.Hash) (int64, error) {
	key := c.hashToKey(h)

	// Try the SizeAwareBackend interface first
	if sb, ok := c.backend.(backend.SizeAwareBackend); ok {
		size, err := sb.Size(ctx, key)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				return 0, backend.ErrNotFound
			}
			return 0, fmt.Errorf("getting size: %w", err)
		}
		return size, nil
	}

	// Fall back to reading the content
	rc, err := c.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return 0, backend.ErrNotFound
		}
		return 0, fmt.Errorf("reading content: %w", err)
	}
	defer func() { _ = rc.Close() }()

	size, err := io.Copy(io.Discard, rc)
	if err != nil {
		return 0, fmt.Errorf("reading content for size: %w", err)
	}
	return size, nil
}

// List returns all hashes in the store.
func (c *CAFS) List(ctx context.Context) ([]contentcache.Hash, error) {
	keys, err := c.backend.List(ctx, blobPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing blobs: %w", err)
	}

	hashes := make([]contentcache.Hash, 0, len(keys))
	for _, key := range keys {
		h, err := c.keyToHash(key)
		if err != nil {
			// Skip invalid keys (shouldn't happen in normal use)
			continue
		}
		hashes = append(hashes, h)
	}
	return hashes, nil
}

// hashToKey converts a hash to a storage key.
// Format: blobs/{first-byte-hex}/{full-hash-hex}
func (c *CAFS) hashToKey(h contentcache.Hash) string {
	hex := h.String()
	return fmt.Sprintf("%s/%s/%s", blobPrefix, hex[:2], hex)
}

// keyToHash extracts a hash from a storage key.
func (c *CAFS) keyToHash(key string) (contentcache.Hash, error) {
	// Expected format: blobs/xx/xxxxxxxx...
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] != blobPrefix {
		return contentcache.Hash{}, fmt.Errorf("invalid key format: %s", key)
	}
	return contentcache.ParseHash(parts[2])
}

// Compile-time interface checks
var (
	_ Store         = (*CAFS)(nil)
	_ ExtendedStore = (*CAFS)(nil)
)
