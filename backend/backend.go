// Package backend provides storage backend abstractions for the content cache.
package backend

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned when a key does not exist in the backend.
var ErrNotFound = errors.New("not found")

// Backend defines the interface for storage backends.
// Implementations must be safe for concurrent use.
type Backend interface {
	// Write stores data at the given key.
	// If the key already exists, it should be overwritten.
	Write(ctx context.Context, key string, r io.Reader) error

	// Read retrieves data at the given key.
	// Returns ErrNotFound if the key does not exist.
	// The caller must close the returned ReadCloser.
	Read(ctx context.Context, key string) (io.ReadCloser, error)

	// Delete removes data at the given key.
	// Returns nil if the key does not exist (idempotent).
	Delete(ctx context.Context, key string) error

	// Exists checks if a key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns all keys with the given prefix.
	// The prefix should use "/" as the path separator.
	List(ctx context.Context, prefix string) ([]string, error)
}

// WriterBackend extends Backend with direct writer access.
// This is optional and allows backends to provide more efficient writes
// for callers that can write directly rather than provide a reader.
type WriterBackend interface {
	Backend

	// Writer returns a WriteCloser for writing to the given key.
	// The write is only committed when Close returns nil.
	// If Close returns an error, the write should be considered failed.
	Writer(ctx context.Context, key string) (io.WriteCloser, error)
}

// SizeAwareBackend extends Backend with size information.
type SizeAwareBackend interface {
	Backend

	// Size returns the size in bytes of the data at the given key.
	// Returns ErrNotFound if the key does not exist.
	Size(ctx context.Context, key string) (int64, error)
}
