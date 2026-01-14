package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

const (
	// blobPrefix is the prefix for blob storage keys.
	blobPrefix = "blobs"
)

// CAFS implements content-addressable file storage.
// Content is stored in a sharded directory structure based on hash.
type CAFS struct {
	backend backend.Backend
}

// NewCAFS creates a new content-addressable file store.
func NewCAFS(b backend.Backend) *CAFS {
	return &CAFS{backend: b}
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
func (c *CAFS) PutWithResult(ctx context.Context, r io.Reader) (*PutResult, error) {
	// Read all content to compute hash
	// For large files, we could use a temp file, but for now we buffer in memory
	var buf bytes.Buffer
	hr := contentcache.NewHashingReader(r)
	if _, err := io.Copy(&buf, hr); err != nil {
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
		return &PutResult{
			Hash:   hash,
			Size:   size,
			Exists: true,
		}, nil
	}

	// Write content to backend
	if err := c.backend.Write(ctx, key, &buf); err != nil {
		return nil, fmt.Errorf("writing content: %w", err)
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
	return rc, nil
}

// GetBytes is a convenience method for retrieving content as bytes.
func (c *CAFS) GetBytes(ctx context.Context, h contentcache.Hash) ([]byte, error) {
	rc, err := c.Get(ctx, h)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

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
	return c.backend.Delete(ctx, key)
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
	defer rc.Close()

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
