package metadb

import (
	"context"
	"encoding/json"
	"time"
)

// EnvelopeIndex provides protocol/kind-specific metadata storage using envelope format.
// It wraps BoltDB envelope APIs with compression, integrity verification, and TTL defaults.
type EnvelopeIndex struct {
	db         *BoltDB
	codec      *EnvelopeCodec
	protocol   string
	kind       string
	defaultTTL time.Duration
}

// NewEnvelopeIndex creates a new envelope index for a specific protocol and kind.
func NewEnvelopeIndex(db *BoltDB, protocol, kind string, ttl time.Duration) (*EnvelopeIndex, error) {
	codec, err := NewEnvelopeCodec()
	if err != nil {
		return nil, err
	}
	return &EnvelopeIndex{
		db:         db,
		codec:      codec,
		protocol:   protocol,
		kind:       kind,
		defaultTTL: ttl,
	}, nil
}

// Close releases codec resources.
func (idx *EnvelopeIndex) Close() {
	if idx.codec != nil {
		idx.codec.Close()
	}
}

// Protocol returns the protocol name.
func (idx *EnvelopeIndex) Protocol() string {
	return idx.protocol
}

// Kind returns the kind name.
func (idx *EnvelopeIndex) Kind() string {
	return idx.kind
}

// Put stores raw bytes with optional blob refs.
// Automatically handles compression, integrity calculation, and TTL.
func (idx *EnvelopeIndex) Put(ctx context.Context, key string, data []byte, contentType ContentType, refs []string) error {
	return idx.PutWithOptions(ctx, key, data, contentType, refs, PutOptions{})
}

// PutOptions configures envelope storage options.
type PutOptions struct {
	TTL            time.Duration // Override default TTL (0 = use default)
	Etag           string        // ETag from upstream
	LastModified   time.Time     // Last-Modified from upstream
	Upstream       string        // Upstream host
	UpstreamStatus uint32        // HTTP status from upstream
}

// PutWithOptions stores raw bytes with full control over envelope fields.
func (idx *EnvelopeIndex) PutWithOptions(ctx context.Context, key string, data []byte, contentType ContentType, refs []string, opts PutOptions) error {
	payload, encoding, digest, err := idx.codec.EncodePayload(data)
	if err != nil {
		return err
	}

	ttl := opts.TTL
	if ttl == 0 {
		ttl = idx.defaultTTL
	}

	now := time.Now()
	var expiresAtMs int64
	if ttl > 0 {
		expiresAtMs = now.Add(ttl).UnixMilli()
	}

	env := &MetadataEnvelope{
		EnvelopeVersion: CurrentEnvelopeVersion,
		ContentType:     contentType,
		ContentEncoding: encoding,
		Payload:         payload,
		PayloadDigest:   digest,
		PayloadSize:     uint64(len(data)),
		FetchedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: expiresAtMs,
		TtlSeconds:      int64(ttl.Seconds()),
		Etag:            opts.Etag,
		Upstream:        opts.Upstream,
		UpstreamStatus:  opts.UpstreamStatus,
		BlobRefs:        refs,
	}

	if !opts.LastModified.IsZero() {
		env.LastModifiedUnixMs = opts.LastModified.UnixMilli()
	}

	return idx.db.PutEnvelope(ctx, idx.protocol, idx.kind, key, env)
}

// Get retrieves and decompresses raw bytes.
// Returns ErrNotFound if key doesn't exist.
// Returns ErrCorrupted if integrity check fails.
func (idx *EnvelopeIndex) Get(ctx context.Context, key string) ([]byte, error) {
	env, err := idx.db.GetEnvelope(ctx, idx.protocol, idx.kind, key)
	if err != nil {
		return nil, err
	}
	return idx.codec.DecodePayload(env.Payload, env.ContentEncoding, env.PayloadDigest, env.PayloadSize)
}

// GetEnvelope retrieves the full envelope including metadata.
// The payload is NOT decompressed; use DecodePayload to decompress.
func (idx *EnvelopeIndex) GetEnvelope(ctx context.Context, key string) (*MetadataEnvelope, error) {
	return idx.db.GetEnvelope(ctx, idx.protocol, idx.kind, key)
}

// GetWithEnvelope retrieves decompressed data along with the envelope metadata.
func (idx *EnvelopeIndex) GetWithEnvelope(ctx context.Context, key string) ([]byte, *MetadataEnvelope, error) {
	env, err := idx.db.GetEnvelope(ctx, idx.protocol, idx.kind, key)
	if err != nil {
		return nil, nil, err
	}
	data, err := idx.codec.DecodePayload(env.Payload, env.ContentEncoding, env.PayloadDigest, env.PayloadSize)
	if err != nil {
		return nil, nil, err
	}
	return data, env, nil
}

// PutJSON marshals value to JSON and stores with blob refs.
func (idx *EnvelopeIndex) PutJSON(ctx context.Context, key string, v any, refs []string) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return idx.Put(ctx, key, data, ContentType_CONTENT_TYPE_JSON, refs)
}

// PutJSONWithOptions marshals value to JSON with full options.
func (idx *EnvelopeIndex) PutJSONWithOptions(ctx context.Context, key string, v any, refs []string, opts PutOptions) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return idx.PutWithOptions(ctx, key, data, ContentType_CONTENT_TYPE_JSON, refs, opts)
}

// GetJSON retrieves and unmarshals JSON data.
func (idx *EnvelopeIndex) GetJSON(ctx context.Context, key string, v any) error {
	data, err := idx.Get(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// Delete removes the entry and decrements blob refs.
func (idx *EnvelopeIndex) Delete(ctx context.Context, key string) error {
	return idx.db.DeleteEnvelope(ctx, idx.protocol, idx.kind, key)
}

// List returns all keys for this protocol/kind.
func (idx *EnvelopeIndex) List(ctx context.Context) ([]string, error) {
	return idx.db.ListEnvelopeKeys(ctx, idx.protocol, idx.kind)
}

// GetBlobRefs returns the blob refs for a key.
func (idx *EnvelopeIndex) GetBlobRefs(ctx context.Context, key string) ([]string, error) {
	return idx.db.GetEnvelopeBlobRefs(ctx, idx.protocol, idx.kind, key)
}

// Update performs read-modify-write in a single transaction.
// The callback receives the current decompressed data (or nil if not found).
// Return new data to store, or nil to delete the entry.
func (idx *EnvelopeIndex) Update(ctx context.Context, key string, fn func(data []byte, env *MetadataEnvelope) ([]byte, []string, error)) error {
	return idx.db.UpdateEnvelope(ctx, idx.protocol, idx.kind, key, func(existing *MetadataEnvelope) (*MetadataEnvelope, error) {
		var existingData []byte
		if existing != nil {
			var err error
			existingData, err = idx.codec.DecodePayload(existing.Payload, existing.ContentEncoding, existing.PayloadDigest, existing.PayloadSize)
			if err != nil {
				return nil, err
			}
		}

		newData, newRefs, err := fn(existingData, existing)
		if err != nil {
			return nil, err
		}
		if newData == nil {
			return nil, nil // Delete
		}

		// Encode new payload
		payload, encoding, digest, err := idx.codec.EncodePayload(newData)
		if err != nil {
			return nil, err
		}

		// Preserve existing envelope fields or create new
		var newEnv *MetadataEnvelope
		if existing != nil {
			newEnv = existing
		} else {
			newEnv = &MetadataEnvelope{
				EnvelopeVersion: CurrentEnvelopeVersion,
				ContentType:     ContentType_CONTENT_TYPE_JSON,
			}
		}

		newEnv.ContentEncoding = encoding
		newEnv.Payload = payload
		newEnv.PayloadDigest = digest
		newEnv.PayloadSize = uint64(len(newData))
		newEnv.BlobRefs = newRefs

		// Update timestamps if this is an update (not create)
		now := time.Now()
		if existing == nil {
			newEnv.FetchedAtUnixMs = now.UnixMilli()
			if idx.defaultTTL > 0 {
				newEnv.ExpiresAtUnixMs = now.Add(idx.defaultTTL).UnixMilli()
				newEnv.TtlSeconds = int64(idx.defaultTTL.Seconds())
			}
		}

		return newEnv, nil
	})
}

// UpdateJSON performs read-modify-write for JSON data.
func (idx *EnvelopeIndex) UpdateJSON(ctx context.Context, key string, v any, fn func(v any) ([]string, error)) error {
	return idx.Update(ctx, key, func(data []byte, _ *MetadataEnvelope) ([]byte, []string, error) {
		if data != nil {
			if err := json.Unmarshal(data, v); err != nil {
				return nil, nil, err
			}
		}
		refs, err := fn(v)
		if err != nil {
			return nil, nil, err
		}
		newData, err := json.Marshal(v)
		if err != nil {
			return nil, nil, err
		}
		return newData, refs, nil
	})
}

// DecodePayload decompresses an envelope's payload.
// Use this after GetEnvelope when you need to decode the payload.
func (idx *EnvelopeIndex) DecodePayload(env *MetadataEnvelope) ([]byte, error) {
	return idx.codec.DecodePayload(env.Payload, env.ContentEncoding, env.PayloadDigest, env.PayloadSize)
}

// IsExpired checks if an envelope has expired.
func IsExpired(env *MetadataEnvelope) bool {
	if env.ExpiresAtUnixMs == 0 {
		return false
	}
	return time.Now().UnixMilli() > env.ExpiresAtUnixMs
}

// IsNegativeCache checks if an envelope represents a cached error response.
func IsNegativeCache(env *MetadataEnvelope) bool {
	return env.UpstreamStatus >= 400
}

// PutNegativeCache stores a negative cache entry (e.g., 404 response).
func (idx *EnvelopeIndex) PutNegativeCache(ctx context.Context, key string, statusCode uint32, ttl time.Duration) error {
	now := time.Now()
	env := &MetadataEnvelope{
		EnvelopeVersion: CurrentEnvelopeVersion,
		ContentType:     ContentType_CONTENT_TYPE_JSON,
		ContentEncoding: ContentEncoding_CONTENT_ENCODING_IDENTITY,
		Payload:         nil,
		UpstreamStatus:  statusCode,
		FetchedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(ttl).UnixMilli(),
		TtlSeconds:      int64(ttl.Seconds()),
	}
	return idx.db.PutEnvelope(ctx, idx.protocol, idx.kind, key, env)
}
