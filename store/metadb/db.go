package metadb

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when an entry does not exist.
var ErrNotFound = errors.New("metadb: not found")

// MetaDB provides metadata storage for the content cache.
type MetaDB interface {
	// Lifecycle
	Open(path string) error
	Close() error

	// Protocol metadata
	GetMeta(ctx context.Context, protocol, key string) ([]byte, error)
	PutMeta(ctx context.Context, protocol, key string, data []byte, ttl time.Duration) error
	DeleteMeta(ctx context.Context, protocol, key string) error
	ListMeta(ctx context.Context, protocol string) ([]string, error)

	// Blob tracking
	GetBlob(ctx context.Context, hash string) (*BlobEntry, error)
	PutBlob(ctx context.Context, entry *BlobEntry) error
	DeleteBlob(ctx context.Context, hash string) error
	IncrementBlobRef(ctx context.Context, hash string) error
	DecrementBlobRef(ctx context.Context, hash string) error
	// TouchBlob updates the last access time and increments the access counter.
	// Returns the new access count (capped at 3 for S3-FIFO), or 0 if the blob was not found.
	TouchBlob(ctx context.Context, hash string) (int, error)
	TotalBlobSize(ctx context.Context) (int64, error)

	// Eviction queries
	GetExpiredMeta(ctx context.Context, before time.Time, limit int) ([]ExpiryEntry, error)
	GetUnreferencedBlobs(ctx context.Context, limit int) ([]string, error)
}

// New creates a new MetaDB backed by bbolt.
func New() MetaDB {
	return NewBoltDB()
}
