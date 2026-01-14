package backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestNewFilesystem(t *testing.T) {
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "cache")

	fs, err := NewFilesystem(root)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}

	if fs.Root() != root {
		t.Errorf("Root() = %q, want %q", fs.Root(), root)
	}

	// Check directory was created
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("root directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("root is not a directory")
	}
}

func TestFilesystemWriteRead(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "test/data.txt"
	data := []byte("hello, world!")

	// Write
	err := fs.Write(ctx, key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Read
	rc, err := fs.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Read() = %q, want %q", got, data)
	}
}

func TestFilesystemReadNotFound(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()

	_, err := fs.Read(ctx, "nonexistent/key")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Read() error = %v, want ErrNotFound", err)
	}
}

func TestFilesystemExists(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "exists/test.txt"

	// Before write
	exists, err := fs.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Exists() = true before write, want false")
	}

	// Write
	err = fs.Write(ctx, key, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// After write
	exists, err = fs.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false after write, want true")
	}
}

func TestFilesystemDelete(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "delete/test.txt"

	// Write
	err := fs.Write(ctx, key, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Delete
	err = fs.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify deleted
	exists, _ := fs.Exists(ctx, key)
	if exists {
		t.Error("key still exists after Delete")
	}

	// Delete nonexistent should not error (idempotent)
	err = fs.Delete(ctx, "nonexistent")
	if err != nil {
		t.Errorf("Delete() of nonexistent key error = %v, want nil", err)
	}
}

func TestFilesystemSize(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "size/test.txt"
	data := []byte("test data for size check")

	// Write
	err := fs.Write(ctx, key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Size
	size, err := fs.Size(ctx, key)
	if err != nil {
		t.Fatalf("Size() error = %v", err)
	}

	if size != int64(len(data)) {
		t.Errorf("Size() = %d, want %d", size, len(data))
	}
}

func TestFilesystemSizeNotFound(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()

	_, err := fs.Size(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Size() error = %v, want ErrNotFound", err)
	}
}

func TestFilesystemList(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()

	// Write multiple files
	keys := []string{
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir1/subdir/file3.txt",
		"dir2/file4.txt",
	}

	for _, key := range keys {
		err := fs.Write(ctx, key, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Write(%s) error = %v", key, err)
		}
	}

	// List all
	all, err := fs.List(ctx, "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	sort.Strings(all)
	sort.Strings(keys)
	if !equalStringSlices(all, keys) {
		t.Errorf("List() = %v, want %v", all, keys)
	}

	// List with prefix
	dir1Files, err := fs.List(ctx, "dir1")
	if err != nil {
		t.Fatalf("List(dir1) error = %v", err)
	}
	expected := []string{"dir1/file1.txt", "dir1/file2.txt", "dir1/subdir/file3.txt"}
	sort.Strings(dir1Files)
	sort.Strings(expected)
	if !equalStringSlices(dir1Files, expected) {
		t.Errorf("List(dir1) = %v, want %v", dir1Files, expected)
	}
}

func TestFilesystemWriter(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "writer/test.txt"
	data := []byte("written via Writer interface")

	// Get writer
	w, err := fs.Writer(ctx, key)
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}

	// Write data
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %d, want %d", n, len(data))
	}

	// Close to commit
	err = w.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify
	rc, err := fs.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("Read() = %q, want %q", got, data)
	}
}

func TestFilesystemAtomicWrite(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "atomic/test.txt"
	originalData := []byte("original content")

	// Write initial data
	err := fs.Write(ctx, key, bytes.NewReader(originalData))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Simulate failed write by using Writer and aborting
	w, err := fs.Writer(ctx, key)
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}
	w.Write([]byte("partial"))

	// Get atomicWriter to call Abort
	aw := w.(*atomicWriter)
	aw.Abort()

	// Original data should still be there
	rc, err := fs.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, originalData) {
		t.Errorf("data corrupted after aborted write: got %q, want %q", got, originalData)
	}
}

func TestFilesystemOverwrite(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "overwrite/test.txt"

	// Write initial
	err := fs.Write(ctx, key, bytes.NewReader([]byte("initial")))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Overwrite
	newData := []byte("new content that is longer")
	err = fs.Write(ctx, key, bytes.NewReader(newData))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify overwrite
	rc, err := fs.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, newData) {
		t.Errorf("Read() = %q, want %q", got, newData)
	}
}

// Helper functions

func newTestFilesystem(t *testing.T) (*Filesystem, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	fs, err := NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	return fs, func() {}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
