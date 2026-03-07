package metadb

import (
	"context"
	"encoding/json"
	"time"
)

// Index provides protocol-specific metadata storage using MetaDB.
// It wraps MetaDB to provide a simpler interface for protocol handlers.
type Index struct {
	db       MetaDB
	protocol string
	ttl      time.Duration
}

// NewIndex creates a new protocol-specific index.
func NewIndex(db MetaDB, protocol string, ttl time.Duration) *Index {
	return &Index{
		db:       db,
		protocol: protocol,
		ttl:      ttl,
	}
}

// Get retrieves metadata for a key.
func (idx *Index) Get(ctx context.Context, key string) ([]byte, error) {
	return idx.db.GetMeta(ctx, idx.protocol, key)
}

// Put stores metadata for a key.
func (idx *Index) Put(ctx context.Context, key string, data []byte) error {
	return idx.db.PutMeta(ctx, idx.protocol, key, data, idx.ttl)
}

// GetJSON retrieves and unmarshals JSON metadata.
func (idx *Index) GetJSON(ctx context.Context, key string, v any) error {
	data, err := idx.db.GetMeta(ctx, idx.protocol, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// PutJSON marshals and stores JSON metadata.
func (idx *Index) PutJSON(ctx context.Context, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return idx.db.PutMeta(ctx, idx.protocol, key, data, idx.ttl)
}

// Delete removes metadata for a key.
func (idx *Index) Delete(ctx context.Context, key string) error {
	return idx.db.DeleteMeta(ctx, idx.protocol, key)
}

// List returns all keys for this protocol.
func (idx *Index) List(ctx context.Context) ([]string, error) {
	return idx.db.ListMeta(ctx, idx.protocol)
}

// IncrementBlobRef increments the reference count for a blob.
func (idx *Index) IncrementBlobRef(ctx context.Context, hash string) error {
	return idx.db.IncrementBlobRef(ctx, hash)
}

// DecrementBlobRef decrements the reference count for a blob.
func (idx *Index) DecrementBlobRef(ctx context.Context, hash string) error {
	return idx.db.DecrementBlobRef(ctx, hash)
}

// metaRefsWriter is implemented by backends that support atomic ref-tracked meta writes.
type metaRefsWriter interface {
	PutMetaWithRefs(ctx context.Context, protocol, key string, data []byte, ttl time.Duration, refs []string) error
}

// metaUpdateWriter is implemented by backends that support atomic read-modify-write.
type metaUpdateWriter interface {
	UpdateJSON(ctx context.Context, protocol, key string, ttl time.Duration, fn func(v any) error, v any) error
}

// PutJSONWithRefs marshals and stores JSON metadata with blob references.
// Uses transactional ref tracking when the backend supports it (BoltDB, SQLiteDB),
// falling back to a plain PutMeta without ref tracking for other backends.
func (idx *Index) PutJSONWithRefs(ctx context.Context, key string, v any, refs []string) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if store, ok := idx.db.(metaRefsWriter); ok {
		return store.PutMetaWithRefs(ctx, idx.protocol, key, data, idx.ttl, refs)
	}
	return idx.db.PutMeta(ctx, idx.protocol, key, data, idx.ttl)
}

// DeleteWithRefs removes metadata and decrements all associated blob refs.
func (idx *Index) DeleteWithRefs(ctx context.Context, key string) error {
	return idx.db.DeleteMetaWithRefs(ctx, idx.protocol, key)
}

// UpdateJSON performs read-modify-write in a single transaction.
// The function fn receives the current value (or zero value if not found)
// and should modify it in place. The modified value is then stored.
// This prevents lost updates from concurrent requests.
func (idx *Index) UpdateJSON(ctx context.Context, key string, fn func(v any) error, v any) error {
	if store, ok := idx.db.(metaUpdateWriter); ok {
		return store.UpdateJSON(ctx, idx.protocol, key, idx.ttl, fn, v)
	}
	// Non-atomic fallback for backends that don't implement metaUpdateWriter.
	_ = idx.GetJSON(ctx, key, v) // Ignore not found
	if err := fn(v); err != nil {
		return err
	}
	return idx.PutJSON(ctx, key, v)
}
