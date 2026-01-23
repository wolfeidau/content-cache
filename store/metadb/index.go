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
