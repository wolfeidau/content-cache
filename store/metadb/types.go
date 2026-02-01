// Package metadb provides metadata storage using bbolt for the content cache.
package metadb

import "time"

// BlobEntry contains metadata about a stored blob.
type BlobEntry struct {
	Hash       string    `json:"hash"`
	Size       int64     `json:"size"`
	CachedAt   time.Time `json:"cached_at"`
	LastAccess time.Time `json:"last_access"`
	RefCount   int       `json:"ref_count"`
}

// ExpiryEntry contains metadata about protocol entries for expiration tracking.
type ExpiryEntry struct {
	Protocol   string    `json:"protocol"`
	Key        string    `json:"key"`
	ExpiresAt  time.Time `json:"expires_at"`
	LastAccess time.Time `json:"last_access"`
	Size       int64     `json:"size"`
}

// EnvelopeExpiryEntry contains metadata about envelope entries for expiration tracking.
type EnvelopeExpiryEntry struct {
	Protocol  string    `json:"protocol"`
	Kind      string    `json:"kind"`
	Key       string    `json:"key"`
	ExpiresAt time.Time `json:"expires_at"`
}
