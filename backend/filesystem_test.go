package backend

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewFilesystem(t *testing.T) {
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "cache")

	fs, err := NewFilesystem(root)
	require.NoError(t, err)

	require.Equal(t, root, fs.Root())

	// Check directory was created
	info, err := os.Stat(root)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestFilesystemWriteRead(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "test/data.txt"
	data := []byte("hello, world!")

	// Write
	err := fs.Write(ctx, key, bytes.NewReader(data))
	require.NoError(t, err)

	// Read
	rc, err := fs.Read(ctx, key)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)

	require.Equal(t, data, got)
}

func TestFilesystemReadNotFound(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()

	_, err := fs.Read(ctx, "nonexistent/key")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestFilesystemExists(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "exists/test.txt"

	// Before write
	exists, err := fs.Exists(ctx, key)
	require.NoError(t, err)
	require.False(t, exists)

	// Write
	err = fs.Write(ctx, key, bytes.NewReader([]byte("data")))
	require.NoError(t, err)

	// After write
	exists, err = fs.Exists(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestFilesystemDelete(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "delete/test.txt"

	// Write
	err := fs.Write(ctx, key, bytes.NewReader([]byte("data")))
	require.NoError(t, err)

	// Delete
	err = fs.Delete(ctx, key)
	require.NoError(t, err)

	// Verify deleted
	exists, _ := fs.Exists(ctx, key)
	require.False(t, exists)

	// Delete nonexistent should not error (idempotent)
	err = fs.Delete(ctx, "nonexistent")
	require.NoError(t, err)
}

func TestFilesystemSize(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "size/test.txt"
	data := []byte("test data for size check")

	// Write
	err := fs.Write(ctx, key, bytes.NewReader(data))
	require.NoError(t, err)

	// Size
	size, err := fs.Size(ctx, key)
	require.NoError(t, err)

	require.Equal(t, int64(len(data)), size)
}

func TestFilesystemSizeNotFound(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()

	_, err := fs.Size(ctx, "nonexistent")
	require.ErrorIs(t, err, ErrNotFound)
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
		require.NoError(t, err)
	}

	// List all
	all, err := fs.List(ctx, "")
	require.NoError(t, err)
	sort.Strings(all)
	sort.Strings(keys)
	require.Equal(t, keys, all)

	// List with prefix
	dir1Files, err := fs.List(ctx, "dir1")
	require.NoError(t, err)
	expected := []string{"dir1/file1.txt", "dir1/file2.txt", "dir1/subdir/file3.txt"}
	sort.Strings(dir1Files)
	sort.Strings(expected)
	require.Equal(t, expected, dir1Files)
}

func TestFilesystemWriter(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "writer/test.txt"
	data := []byte("written via Writer interface")

	// Get writer
	w, err := fs.Writer(ctx, key)
	require.NoError(t, err)

	// Write data
	n, err := w.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// Close to commit
	err = w.Close()
	require.NoError(t, err)

	// Verify
	rc, err := fs.Read(ctx, key)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()

	got, _ := io.ReadAll(rc)
	require.Equal(t, data, got)
}

func TestFilesystemAtomicWrite(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "atomic/test.txt"
	originalData := []byte("original content")

	// Write initial data
	err := fs.Write(ctx, key, bytes.NewReader(originalData))
	require.NoError(t, err)

	// Simulate failed write by using Writer and aborting
	w, err := fs.Writer(ctx, key)
	require.NoError(t, err)
	_, _ = w.Write([]byte("partial"))

	// Get atomicWriter to call Abort
	aw := w.(*atomicWriter)
	_ = aw.Abort()

	// Original data should still be there
	rc, err := fs.Read(ctx, key)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()

	got, _ := io.ReadAll(rc)
	require.Equal(t, originalData, got)
}

func TestFilesystemOverwrite(t *testing.T) {
	fs, cleanup := newTestFilesystem(t)
	defer cleanup()

	ctx := context.Background()
	key := "overwrite/test.txt"

	// Write initial
	err := fs.Write(ctx, key, bytes.NewReader([]byte("initial")))
	require.NoError(t, err)

	// Overwrite
	newData := []byte("new content that is longer")
	err = fs.Write(ctx, key, bytes.NewReader(newData))
	require.NoError(t, err)

	// Verify overwrite
	rc, err := fs.Read(ctx, key)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()

	got, _ := io.ReadAll(rc)
	require.Equal(t, newData, got)
}

// Helper functions

func newTestFilesystem(t *testing.T) (*Filesystem, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	fs, err := NewFilesystem(tmpDir)
	require.NoError(t, err)
	return fs, func() {}
}
