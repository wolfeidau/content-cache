// Package telemetry provides request tagging for structured logging and metrics.
package telemetry

import (
	"context"
	"net/http"
)

type contextKey string

// requestTagsKey is the context key for request tags holder.
const requestTagsKey contextKey = "request_tags"

// CacheResult represents the outcome of a cache lookup.
type CacheResult string

const (
	CacheHit    CacheResult = "hit"
	CacheMiss   CacheResult = "miss"
	CacheBypass CacheResult = "bypass"
	CacheNA     CacheResult = "na"
)

// RequestTags holds mutable request metadata that handlers can set for logging.
type RequestTags struct {
	Protocol    string
	CacheResult CacheResult
	Endpoint    string
}

// InjectTags creates a new request with an empty RequestTags in context.
// Call this in middleware before handlers run.
func InjectTags(r *http.Request) *http.Request {
	tags := &RequestTags{CacheResult: CacheBypass}
	return r.WithContext(context.WithValue(r.Context(), requestTagsKey, tags))
}

// GetTags retrieves the request tags from context.
// Returns nil if not in a request context with logging middleware.
func GetTags(r *http.Request) *RequestTags {
	if tags, ok := r.Context().Value(requestTagsKey).(*RequestTags); ok {
		return tags
	}
	return nil
}

// SetCacheResult sets the cache result for logging.
func SetCacheResult(r *http.Request, result CacheResult) {
	if tags := GetTags(r); tags != nil {
		tags.CacheResult = result
	}
}

// SetProtocol sets the protocol tag for metrics and logging.
func SetProtocol(r *http.Request, protocol string) {
	if tags := GetTags(r); tags != nil {
		tags.Protocol = protocol
	}
}

// SetEndpoint sets the endpoint type for logging.
func SetEndpoint(r *http.Request, endpoint string) {
	if tags := GetTags(r); tags != nil {
		tags.Endpoint = endpoint
	}
}
