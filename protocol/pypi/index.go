package pypi

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
	// pypiPrefix is the storage prefix for PyPI package data.
	pypiPrefix = "pypi"
)

// Index manages the PyPI package index.
type Index struct {
	backend backend.Backend
	mu      sync.RWMutex
	now     func() time.Time
}

// NewIndex creates a new PyPI package index.
func NewIndex(b backend.Backend) *Index {
	return &Index{
		backend: b,
		now:     time.Now,
	}
}

// GetCachedProject retrieves cached project info including file hashes.
func (idx *Index) GetCachedProject(ctx context.Context, name string) (*CachedProject, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	normalized := NormalizeProjectName(name)
	key := idx.projectKey(normalized)
	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading project info: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var cached CachedProject
	if err := json.NewDecoder(rc).Decode(&cached); err != nil {
		return nil, fmt.Errorf("decoding project info: %w", err)
	}

	return &cached, nil
}

// PutCachedProject stores cached project info.
func (idx *Index) PutCachedProject(ctx context.Context, project *CachedProject) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	project.UpdatedAt = idx.now()
	if project.CachedAt.IsZero() {
		project.CachedAt = project.UpdatedAt
	}

	normalized := NormalizeProjectName(project.Name)
	key := idx.projectKey(normalized)
	data, err := json.Marshal(project)
	if err != nil {
		return fmt.Errorf("encoding project info: %w", err)
	}

	if err := idx.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing project info: %w", err)
	}

	return nil
}

// GetFileHash retrieves the content hash for a specific file.
func (idx *Index) GetFileHash(ctx context.Context, project, filename string) (contentcache.Hash, error) {
	cached, err := idx.GetCachedProject(ctx, project)
	if err != nil {
		return contentcache.Hash{}, err
	}

	file, ok := cached.Files[filename]
	if !ok {
		return contentcache.Hash{}, ErrNotFound
	}

	return file.ContentHash, nil
}

// SetFileHash stores the content hash for a specific file.
func (idx *Index) SetFileHash(ctx context.Context, project, filename string, hash contentcache.Hash, size int64, hashes map[string]string, upstreamURL, requiresPython string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	normalized := NormalizeProjectName(project)
	key := idx.projectKey(normalized)

	// Get or create cached project
	var cached CachedProject
	rc, err := idx.backend.Read(ctx, key)
	if err == nil {
		_ = json.NewDecoder(rc).Decode(&cached)
		_ = rc.Close()
	}

	if cached.Name == "" {
		cached.Name = project
		cached.CachedAt = idx.now()
	}
	if cached.Files == nil {
		cached.Files = make(map[string]*CachedFile)
	}

	cached.Files[filename] = &CachedFile{
		Filename:       filename,
		ContentHash:    hash,
		Size:           size,
		Hashes:         hashes,
		RequiresPython: requiresPython,
		UpstreamURL:    upstreamURL,
		CachedAt:       idx.now(),
	}
	cached.UpdatedAt = idx.now()

	data, err := json.Marshal(&cached)
	if err != nil {
		return fmt.Errorf("encoding project info: %w", err)
	}

	if err := idx.backend.Write(ctx, key, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing project info: %w", err)
	}

	return nil
}

// GetCachedFile retrieves cached file info for a specific file.
func (idx *Index) GetCachedFile(ctx context.Context, project, filename string) (*CachedFile, error) {
	cached, err := idx.GetCachedProject(ctx, project)
	if err != nil {
		return nil, err
	}

	file, ok := cached.Files[filename]
	if !ok {
		return nil, ErrNotFound
	}

	return file, nil
}

// DeleteProject removes all cached data for a project.
func (idx *Index) DeleteProject(ctx context.Context, name string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	normalized := NormalizeProjectName(name)
	if err := idx.backend.Delete(ctx, idx.projectKey(normalized)); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return fmt.Errorf("deleting project: %w", err)
	}

	return nil
}

// ListProjects returns all cached project names.
func (idx *Index) ListProjects(ctx context.Context) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, err := idx.backend.List(ctx, path.Join(pypiPrefix, "projects"))
	if err != nil {
		return nil, fmt.Errorf("listing projects: %w", err)
	}

	var names []string
	for _, key := range keys {
		// Extract project name from key
		// Key format: pypi/projects/{normalized-name}.json
		name := strings.TrimPrefix(key, path.Join(pypiPrefix, "projects")+"/")
		name = strings.TrimSuffix(name, ".json")
		if name != "" {
			names = append(names, name)
		}
	}

	return names, nil
}

// IsExpired checks if a cached project has exceeded the TTL.
func (idx *Index) IsExpired(project *CachedProject, ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}
	return idx.now().Sub(project.UpdatedAt) > ttl
}

// Key generation helpers

func (idx *Index) projectKey(normalizedName string) string {
	return path.Join(pypiPrefix, "projects", normalizedName+".json")
}
