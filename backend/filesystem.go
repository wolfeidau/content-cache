package backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Filesystem implements Backend using the local filesystem.
// Writes are atomic using a temp file and rename pattern.
type Filesystem struct {
	root string
}

// NewFilesystem creates a new filesystem backend rooted at the given path.
// The directory will be created if it does not exist.
func NewFilesystem(root string) (*Filesystem, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolving root path: %w", err)
	}
	if err := os.MkdirAll(absRoot, 0755); err != nil {
		return nil, fmt.Errorf("creating root directory: %w", err)
	}
	return &Filesystem{root: absRoot}, nil
}

// Root returns the root directory path.
func (fs *Filesystem) Root() string {
	return fs.root
}

// Write stores data at the given key using atomic write.
func (fs *Filesystem) Write(ctx context.Context, key string, r io.Reader) error {
	path := fs.keyToPath(key)

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	// Write to temp file first
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()

	// Clean up temp file on error
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	// Copy data to temp file
	if _, err := io.Copy(tmp, r); err != nil {
		return fmt.Errorf("writing data: %w", err)
	}

	// Sync to disk
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("syncing file: %w", err)
	}

	// Close before rename
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}

	success = true
	return nil
}

// Read retrieves data at the given key.
func (fs *Filesystem) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	path := fs.keyToPath(key)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("opening file: %w", err)
	}
	return f, nil
}

// Delete removes data at the given key.
func (fs *Filesystem) Delete(ctx context.Context, key string) error {
	path := fs.keyToPath(key)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing file: %w", err)
	}
	return nil
}

// Exists checks if a key exists.
func (fs *Filesystem) Exists(ctx context.Context, key string) (bool, error) {
	path := fs.keyToPath(key)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking file: %w", err)
}

// List returns all keys with the given prefix.
func (fs *Filesystem) List(ctx context.Context, prefix string) ([]string, error) {
	dir := fs.keyToPath(prefix)

	// Check if the path exists
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat path: %w", err)
	}

	// If it's a file, return just that key
	if !info.IsDir() {
		return []string{prefix}, nil
	}

	var keys []string
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip temp files
		if strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		// Convert path back to key
		rel, err := filepath.Rel(fs.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking directory: %w", err)
	}
	return keys, nil
}

// Size returns the size of the data at the given key.
func (fs *Filesystem) Size(ctx context.Context, key string) (int64, error) {
	path := fs.keyToPath(key)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("stat file: %w", err)
	}
	return info.Size(), nil
}

// Writer returns a WriteCloser for writing to the given key.
// The write is atomic - data is written to a temp file and renamed on Close.
func (fs *Filesystem) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	path := fs.keyToPath(key)

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating directory %s: %w", dir, err)
	}

	// Create temp file
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	return &atomicWriter{
		f:       tmp,
		tmpPath: tmp.Name(),
		dstPath: path,
	}, nil
}

// keyToPath converts a key to a filesystem path.
func (fs *Filesystem) keyToPath(key string) string {
	// Convert forward slashes to OS-specific separator
	return filepath.Join(fs.root, filepath.FromSlash(key))
}

// atomicWriter wraps a file for atomic writing.
type atomicWriter struct {
	f       *os.File
	tmpPath string
	dstPath string
	closed  bool
}

// Write implements io.Writer.
func (w *atomicWriter) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Close commits the write by renaming the temp file.
func (w *atomicWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Sync to disk
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("syncing file: %w", err)
	}

	// Close the file
	if err := w.f.Close(); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(w.tmpPath, w.dstPath); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("renaming temp file: %w", err)
	}

	return nil
}

// Abort cancels the write and removes the temp file.
func (w *atomicWriter) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.f.Close()
	return os.Remove(w.tmpPath)
}

// Compile-time interface checks
var (
	_ Backend          = (*Filesystem)(nil)
	_ WriterBackend    = (*Filesystem)(nil)
	_ SizeAwareBackend = (*Filesystem)(nil)
)
