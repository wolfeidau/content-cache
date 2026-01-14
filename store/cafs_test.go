package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

func TestCAFSPutGet(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("test content for CAFS")

	// Put
	hash, err := cafs.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if hash.IsZero() {
		t.Error("Put() returned zero hash")
	}

	// Verify hash is correct
	expectedHash := contentcache.HashBytes(data)
	if hash != expectedHash {
		t.Errorf("Put() hash = %v, want %v", hash, expectedHash)
	}

	// Get
	rc, err := cafs.Get(ctx, hash)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Get() = %q, want %q", got, data)
	}
}

func TestCAFSPutWithResult(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("test content")

	// First put
	result1, err := cafs.PutWithResult(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutWithResult() error = %v", err)
	}

	if result1.Exists {
		t.Error("first put should report Exists = false")
	}
	if result1.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", result1.Size, len(data))
	}

	// Second put of same content
	result2, err := cafs.PutWithResult(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutWithResult() second call error = %v", err)
	}

	if !result2.Exists {
		t.Error("second put should report Exists = true")
	}
	if result2.Hash != result1.Hash {
		t.Error("hash should be same for same content")
	}
}

func TestCAFSPutBytes(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("bytes convenience method")

	hash, err := cafs.PutBytes(ctx, data)
	if err != nil {
		t.Fatalf("PutBytes() error = %v", err)
	}

	// Verify via GetBytes
	got, err := cafs.GetBytes(ctx, hash)
	if err != nil {
		t.Fatalf("GetBytes() error = %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("GetBytes() = %q, want %q", got, data)
	}
}

func TestCAFSGetNotFound(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()

	// Try to get non-existent hash
	fakeHash := contentcache.HashBytes([]byte("does not exist"))

	_, err := cafs.Get(ctx, fakeHash)
	if !errors.Is(err, backend.ErrNotFound) {
		t.Errorf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestCAFSHas(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("existence check")

	hash := contentcache.HashBytes(data)

	// Before put
	exists, err := cafs.Has(ctx, hash)
	if err != nil {
		t.Fatalf("Has() error = %v", err)
	}
	if exists {
		t.Error("Has() = true before put, want false")
	}

	// Put
	_, err = cafs.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// After put
	exists, err = cafs.Has(ctx, hash)
	if err != nil {
		t.Fatalf("Has() error = %v", err)
	}
	if !exists {
		t.Error("Has() = false after put, want true")
	}
}

func TestCAFSDelete(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("delete me")

	// Put
	hash, err := cafs.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Delete
	err = cafs.Delete(ctx, hash)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify deleted
	exists, _ := cafs.Has(ctx, hash)
	if exists {
		t.Error("content still exists after Delete")
	}

	// Delete again should not error
	err = cafs.Delete(ctx, hash)
	if err != nil {
		t.Errorf("Delete() of non-existent hash error = %v, want nil", err)
	}
}

func TestCAFSSize(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("size check content")

	hash, err := cafs.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	size, err := cafs.Size(ctx, hash)
	if err != nil {
		t.Fatalf("Size() error = %v", err)
	}

	if size != int64(len(data)) {
		t.Errorf("Size() = %d, want %d", size, len(data))
	}
}

func TestCAFSSizeNotFound(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	fakeHash := contentcache.HashBytes([]byte("not stored"))

	_, err := cafs.Size(ctx, fakeHash)
	if !errors.Is(err, backend.ErrNotFound) {
		t.Errorf("Size() error = %v, want ErrNotFound", err)
	}
}

func TestCAFSList(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple items
	data := [][]byte{
		[]byte("content one"),
		[]byte("content two"),
		[]byte("content three"),
	}

	expectedHashes := make(map[contentcache.Hash]bool)
	for _, d := range data {
		hash, err := cafs.Put(ctx, bytes.NewReader(d))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		expectedHashes[hash] = true
	}

	// List
	hashes, err := cafs.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(hashes) != len(data) {
		t.Errorf("List() returned %d hashes, want %d", len(hashes), len(data))
	}

	for _, h := range hashes {
		if !expectedHashes[h] {
			t.Errorf("unexpected hash in List(): %v", h)
		}
	}
}

func TestCAFSDeduplication(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("deduplicated content")

	// Put same content multiple times
	hash1, _ := cafs.Put(ctx, bytes.NewReader(data))
	hash2, _ := cafs.Put(ctx, bytes.NewReader(data))
	hash3, _ := cafs.Put(ctx, bytes.NewReader(data))

	// All hashes should be the same
	if hash1 != hash2 || hash2 != hash3 {
		t.Error("same content should produce same hash")
	}

	// List should show only one item
	hashes, err := cafs.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(hashes) != 1 {
		t.Errorf("List() returned %d items, want 1 (deduplication failed)", len(hashes))
	}
}

func TestCAFSEmptyContent(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte{}

	hash, err := cafs.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, err := cafs.GetBytes(ctx, hash)
	if err != nil {
		t.Fatalf("GetBytes() error = %v", err)
	}

	if len(got) != 0 {
		t.Errorf("GetBytes() returned %d bytes, want 0", len(got))
	}
}

func TestCAFSLargeContent(t *testing.T) {
	cafs, cleanup := newTestCAFS(t)
	defer cleanup()

	ctx := context.Background()

	// 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	hash, err := cafs.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, err := cafs.GetBytes(ctx, hash)
	if err != nil {
		t.Fatalf("GetBytes() error = %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Error("large content not stored/retrieved correctly")
	}
}

// Helper functions

func newTestCAFS(t *testing.T) (*CAFS, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	return NewCAFS(b), func() {}
}
