package rubygems

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"time"

	"github.com/wolfeidau/content-cache/backend"
)

// Index provides storage for RubyGems metadata.
type Index struct {
	backend backend.Backend
}

// NewIndex creates a new Index.
func NewIndex(b backend.Backend) *Index {
	return &Index{backend: b}
}

// Storage paths:
// rubygems/versions.json           - CachedVersions metadata
// rubygems/versions                 - Raw /versions file content
// rubygems/names.json              - CachedNames metadata (if needed)
// rubygems/names                    - Raw /names file content
// rubygems/info/{gem}.json         - CachedGemInfo metadata
// rubygems/info/{gem}              - Raw /info/{gem} content
// rubygems/specs/{type}.json       - CachedSpecs metadata
// rubygems/specs/{type}.4.8.gz     - Raw specs file content
// rubygems/gemspecs/{name}-{ver}[-{plat}].json  - CachedGemspec metadata
// rubygems/gems/{filename}.json    - CachedGem metadata

const (
	indexPrefix = "rubygems"
)

// GetVersions retrieves the cached /versions file and its metadata.
func (i *Index) GetVersions(ctx context.Context) (*CachedVersions, []byte, error) {
	metaPath := path.Join(indexPrefix, "versions.json")
	contentPath := path.Join(indexPrefix, "versions")

	// Read metadata
	metaReader, err := i.backend.Read(ctx, metaPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("reading versions metadata: %w", err)
	}
	defer func() { _ = metaReader.Close() }()

	var meta CachedVersions
	if err := json.NewDecoder(metaReader).Decode(&meta); err != nil {
		return nil, nil, fmt.Errorf("decoding versions metadata: %w", err)
	}

	// Read content
	contentReader, err := i.backend.Read(ctx, contentPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("reading versions content: %w", err)
	}
	defer func() { _ = contentReader.Close() }()

	content, err := io.ReadAll(contentReader)
	if err != nil {
		return nil, nil, fmt.Errorf("reading versions content: %w", err)
	}

	return &meta, content, nil
}

// PutVersions stores the /versions file and its metadata.
func (i *Index) PutVersions(ctx context.Context, meta *CachedVersions, content []byte) error {
	metaPath := path.Join(indexPrefix, "versions.json")
	contentPath := path.Join(indexPrefix, "versions")

	// Write metadata
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding versions metadata: %w", err)
	}
	if err := i.backend.Write(ctx, metaPath, bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("writing versions metadata: %w", err)
	}

	// Write content
	if err := i.backend.Write(ctx, contentPath, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("writing versions content: %w", err)
	}

	return nil
}

// AppendVersions appends content to the /versions file and updates metadata.
func (i *Index) AppendVersions(ctx context.Context, meta *CachedVersions, appendContent []byte) error {
	// Read existing content
	_, existingContent, err := i.GetVersions(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("reading existing versions: %w", err)
	}

	// Append new content
	newContent := make([]byte, len(existingContent)+len(appendContent))
	copy(newContent, existingContent)
	copy(newContent[len(existingContent):], appendContent)
	meta.Size = int64(len(newContent))

	return i.PutVersions(ctx, meta, newContent)
}

// GetInfo retrieves the cached /info/{gem} file and its metadata.
func (i *Index) GetInfo(ctx context.Context, gem string) (*CachedGemInfo, []byte, error) {
	metaPath := path.Join(indexPrefix, "info", gem+".json")
	contentPath := path.Join(indexPrefix, "info", gem)

	// Read metadata
	metaReader, err := i.backend.Read(ctx, metaPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("reading info metadata: %w", err)
	}
	defer func() { _ = metaReader.Close() }()

	var meta CachedGemInfo
	if err := json.NewDecoder(metaReader).Decode(&meta); err != nil {
		return nil, nil, fmt.Errorf("decoding info metadata: %w", err)
	}

	// Read content
	contentReader, err := i.backend.Read(ctx, contentPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("reading info content: %w", err)
	}
	defer func() { _ = contentReader.Close() }()

	content, err := io.ReadAll(contentReader)
	if err != nil {
		return nil, nil, fmt.Errorf("reading info content: %w", err)
	}

	return &meta, content, nil
}

// PutInfo stores the /info/{gem} file and its metadata.
func (i *Index) PutInfo(ctx context.Context, gem string, meta *CachedGemInfo, content []byte) error {
	metaPath := path.Join(indexPrefix, "info", gem+".json")
	contentPath := path.Join(indexPrefix, "info", gem)

	// Write metadata
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding info metadata: %w", err)
	}
	if err := i.backend.Write(ctx, metaPath, bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("writing info metadata: %w", err)
	}

	// Write content
	if err := i.backend.Write(ctx, contentPath, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("writing info content: %w", err)
	}

	return nil
}

// GetSpecs retrieves the cached specs file (specs, latest_specs, or prerelease_specs).
func (i *Index) GetSpecs(ctx context.Context, specsType string) (*CachedSpecs, []byte, error) {
	metaPath := path.Join(indexPrefix, "specs", specsType+".json")
	contentPath := path.Join(indexPrefix, "specs", specsType+".4.8.gz")

	// Read metadata
	metaReader, err := i.backend.Read(ctx, metaPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("reading specs metadata: %w", err)
	}
	defer func() { _ = metaReader.Close() }()

	var meta CachedSpecs
	if err := json.NewDecoder(metaReader).Decode(&meta); err != nil {
		return nil, nil, fmt.Errorf("decoding specs metadata: %w", err)
	}

	// Read content
	contentReader, err := i.backend.Read(ctx, contentPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("reading specs content: %w", err)
	}
	defer func() { _ = contentReader.Close() }()

	content, err := io.ReadAll(contentReader)
	if err != nil {
		return nil, nil, fmt.Errorf("reading specs content: %w", err)
	}

	return &meta, content, nil
}

// PutSpecs stores a specs file and its metadata.
func (i *Index) PutSpecs(ctx context.Context, specsType string, meta *CachedSpecs, content []byte) error {
	metaPath := path.Join(indexPrefix, "specs", specsType+".json")
	contentPath := path.Join(indexPrefix, "specs", specsType+".4.8.gz")

	// Write metadata
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding specs metadata: %w", err)
	}
	if err := i.backend.Write(ctx, metaPath, bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("writing specs metadata: %w", err)
	}

	// Write content
	if err := i.backend.Write(ctx, contentPath, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("writing specs content: %w", err)
	}

	return nil
}

// GetGem retrieves cached gem metadata.
func (i *Index) GetGem(ctx context.Context, filename string) (*CachedGem, error) {
	metaPath := path.Join(indexPrefix, "gems", filename+".json")

	metaReader, err := i.backend.Read(ctx, metaPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading gem metadata: %w", err)
	}
	defer func() { _ = metaReader.Close() }()

	var meta CachedGem
	if err := json.NewDecoder(metaReader).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding gem metadata: %w", err)
	}

	return &meta, nil
}

// PutGem stores gem metadata.
func (i *Index) PutGem(ctx context.Context, gem *CachedGem) error {
	metaPath := path.Join(indexPrefix, "gems", gem.Filename+".json")

	metaBytes, err := json.Marshal(gem)
	if err != nil {
		return fmt.Errorf("encoding gem metadata: %w", err)
	}
	if err := i.backend.Write(ctx, metaPath, bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("writing gem metadata: %w", err)
	}

	return nil
}

// GetGemspec retrieves cached gemspec metadata.
func (i *Index) GetGemspec(ctx context.Context, name, version, platform string) (*CachedGemspec, error) {
	filename := gemspecFilename(name, version, platform)
	metaPath := path.Join(indexPrefix, "gemspecs", filename+".json")

	metaReader, err := i.backend.Read(ctx, metaPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading gemspec metadata: %w", err)
	}
	defer func() { _ = metaReader.Close() }()

	var meta CachedGemspec
	if err := json.NewDecoder(metaReader).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding gemspec metadata: %w", err)
	}

	return &meta, nil
}

// PutGemspec stores gemspec metadata.
func (i *Index) PutGemspec(ctx context.Context, spec *CachedGemspec) error {
	filename := gemspecFilename(spec.Name, spec.Version, spec.Platform)
	metaPath := path.Join(indexPrefix, "gemspecs", filename+".json")

	metaBytes, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encoding gemspec metadata: %w", err)
	}
	if err := i.backend.Write(ctx, metaPath, bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("writing gemspec metadata: %w", err)
	}

	return nil
}

// IsExpired checks if the cached content is expired based on TTL.
func (i *Index) IsExpired(cachedAt time.Time, ttl time.Duration) bool {
	return time.Since(cachedAt) > ttl
}

// gemspecFilename builds the filename for a gemspec.
func gemspecFilename(name, version, platform string) string {
	if platform == "" || platform == "ruby" {
		return name + "-" + version
	}
	return name + "-" + version + "-" + platform
}
