package buildcache

import "errors"

// ErrNotFound is returned when a cache entry does not exist.
var ErrNotFound = errors.New("not found")

// ActionEntry records the mapping from an actionID to a stored blob.
type ActionEntry struct {
	OutputID string `json:"output_id"` // hex-encoded output ID from the go tool
	BlobHash string `json:"blob_hash"` // canonical blob ref: "blake3:<hex>"
	Size     int64  `json:"size"`      // blob size in bytes
}
