package maven

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

const (
	// mavenPrefix is the storage prefix for Maven artifact data.
	mavenPrefix = "maven"
)

// Index manages the Maven artifact index.
type Index struct {
	backend backend.Backend
	mu      sync.RWMutex
	now     func() time.Time
}

// NewIndex creates a new Maven artifact index.
func NewIndex(b backend.Backend) *Index {
	return &Index{
		backend: b,
		now:     time.Now,
	}
}

// GetCachedMetadata retrieves cached maven-metadata.xml information.
func (idx *Index) GetCachedMetadata(ctx context.Context, groupID, artifactID string) (*CachedMetadata, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := idx.metadataKey(groupID, artifactID)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var cached CachedMetadata
	if err := json.NewDecoder(rc).Decode(&cached); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}

	return &cached, nil
}

// PutCachedMetadata stores cached maven-metadata.xml information.
func (idx *Index) PutCachedMetadata(ctx context.Context, metadata *CachedMetadata) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	metadata.UpdatedAt = idx.now()
	if metadata.CachedAt.IsZero() {
		metadata.CachedAt = metadata.UpdatedAt
	}

	key := idx.metadataKey(metadata.GroupID, metadata.ArtifactID)
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}

	if err := idx.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}

	return nil
}

// GetCachedArtifact retrieves cached artifact information.
func (idx *Index) GetCachedArtifact(ctx context.Context, coord ArtifactCoordinate) (*CachedArtifact, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := idx.artifactKey(coord)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading artifact: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var cached CachedArtifact
	if err := json.NewDecoder(rc).Decode(&cached); err != nil {
		return nil, fmt.Errorf("decoding artifact: %w", err)
	}

	return &cached, nil
}

// PutCachedArtifact stores cached artifact information.
func (idx *Index) PutCachedArtifact(ctx context.Context, artifact *CachedArtifact) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	artifact.CachedAt = idx.now()

	coord := ArtifactCoordinate{
		GroupID:    artifact.GroupID,
		ArtifactID: artifact.ArtifactID,
		Version:    artifact.Version,
		Classifier: artifact.Classifier,
		Extension:  artifact.Extension,
	}

	key := idx.artifactKey(coord)
	data, err := json.Marshal(artifact)
	if err != nil {
		return fmt.Errorf("encoding artifact: %w", err)
	}

	if err := idx.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing artifact: %w", err)
	}

	return nil
}

// GetArtifactHash retrieves the content hash for a specific artifact.
func (idx *Index) GetArtifactHash(ctx context.Context, coord ArtifactCoordinate) (contentcache.Hash, error) {
	cached, err := idx.GetCachedArtifact(ctx, coord)
	if err != nil {
		return contentcache.Hash{}, err
	}
	return cached.Hash, nil
}

// DeleteMetadata removes cached metadata for an artifact.
func (idx *Index) DeleteMetadata(ctx context.Context, groupID, artifactID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key := idx.metadataKey(groupID, artifactID)
	if err := idx.backend.Delete(ctx, key); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return fmt.Errorf("deleting metadata: %w", err)
	}

	return nil
}

// DeleteArtifact removes cached artifact information.
func (idx *Index) DeleteArtifact(ctx context.Context, coord ArtifactCoordinate) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	key := idx.artifactKey(coord)
	if err := idx.backend.Delete(ctx, key); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return fmt.Errorf("deleting artifact: %w", err)
	}

	return nil
}

// IsMetadataExpired checks if cached metadata has exceeded the TTL.
func (idx *Index) IsMetadataExpired(metadata *CachedMetadata, ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}
	return idx.now().Sub(metadata.UpdatedAt) > ttl
}

// ListArtifacts returns all cached artifact keys for a given group and artifact.
func (idx *Index) ListArtifacts(ctx context.Context, groupID, artifactID string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	prefix := path.Join(mavenPrefix, "artifacts", groupIDToPath(groupID), artifactID)
	keys, err := idx.backend.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing artifacts: %w", err)
	}

	return keys, nil
}

// Key generation helpers

func (idx *Index) metadataKey(groupID, artifactID string) string {
	return path.Join(mavenPrefix, "metadata", groupIDToPath(groupID), artifactID, "metadata.json")
}

func (idx *Index) artifactKey(coord ArtifactCoordinate) string {
	filename := coord.ArtifactID + "-" + coord.Version
	if coord.Classifier != "" {
		filename += "-" + coord.Classifier
	}
	filename += "." + coord.Extension + ".json"
	return path.Join(mavenPrefix, "artifacts", groupIDToPath(coord.GroupID), coord.ArtifactID, coord.Version, filename)
}
