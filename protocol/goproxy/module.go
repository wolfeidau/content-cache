// Package goproxy implements the GOPROXY protocol for caching Go modules.
package goproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

// ErrNotFound is returned when a module or version is not found.
var ErrNotFound = errors.New("module not found")

// VersionInfo contains metadata about a module version.
// This is the JSON format returned by the GOPROXY /@v/{version}.info endpoint.
type VersionInfo struct {
	Version string    `json:"Version"`
	Time    time.Time `json:"Time,omitempty"`
}

// ModuleVersion contains all cached data for a specific module version.
type ModuleVersion struct {
	Info    VersionInfo       `json:"info"`
	ModHash contentcache.Hash `json:"mod_hash,omitempty"` // Hash of go.mod in CAFS (if stored separately)
	ZipHash contentcache.Hash `json:"zip_hash"`           // Hash of module zip in CAFS
	ModFile []byte            `json:"-"`                  // go.mod content (loaded on demand)
}

// Index manages the module version index.
// It stores metadata about cached modules and maps versions to content hashes.
type Index struct {
	backend backend.Backend
}

// NewIndex creates a new module index using the given backend.
func NewIndex(b backend.Backend) *Index {
	return &Index{backend: b}
}

// ListVersions returns all cached versions for a module.
func (idx *Index) ListVersions(ctx context.Context, modulePath string) ([]string, error) {
	key := idx.listKey(modulePath)

	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, nil // No versions cached yet
		}
		return nil, fmt.Errorf("reading version list: %w", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading version list data: %w", err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	versions := strings.Split(strings.TrimSpace(string(data)), "\n")
	return versions, nil
}

// GetVersionInfo returns the info for a specific module version.
func (idx *Index) GetVersionInfo(ctx context.Context, modulePath, version string) (*VersionInfo, error) {
	mv, err := idx.GetModuleVersion(ctx, modulePath, version)
	if err != nil {
		return nil, err
	}
	return &mv.Info, nil
}

// GetModuleVersion returns all cached data for a module version.
func (idx *Index) GetModuleVersion(ctx context.Context, modulePath, version string) (*ModuleVersion, error) {
	key := idx.versionKey(modulePath, version)

	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading version info: %w", err)
	}
	defer rc.Close()

	var mv ModuleVersion
	if err := json.NewDecoder(rc).Decode(&mv); err != nil {
		return nil, fmt.Errorf("decoding version info: %w", err)
	}

	return &mv, nil
}

// GetMod returns the go.mod content for a module version.
func (idx *Index) GetMod(ctx context.Context, modulePath, version string) ([]byte, error) {
	key := idx.modKey(modulePath, version)

	rc, err := idx.backend.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading go.mod: %w", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading go.mod data: %w", err)
	}

	return data, nil
}

// PutModuleVersion stores a module version in the index.
func (idx *Index) PutModuleVersion(ctx context.Context, modulePath, version string, mv *ModuleVersion, modFile []byte) error {
	// Store version info
	versionKey := idx.versionKey(modulePath, version)
	data, err := json.Marshal(mv)
	if err != nil {
		return fmt.Errorf("encoding version info: %w", err)
	}
	if err := idx.backend.Write(ctx, versionKey, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("writing version info: %w", err)
	}

	// Store go.mod content
	modKey := idx.modKey(modulePath, version)
	if err := idx.backend.Write(ctx, modKey, strings.NewReader(string(modFile))); err != nil {
		return fmt.Errorf("writing go.mod: %w", err)
	}

	// Update version list
	if err := idx.addVersion(ctx, modulePath, version); err != nil {
		return fmt.Errorf("updating version list: %w", err)
	}

	return nil
}

// DeleteModuleVersion removes a module version from the index.
func (idx *Index) DeleteModuleVersion(ctx context.Context, modulePath, version string) error {
	// Delete version info
	if err := idx.backend.Delete(ctx, idx.versionKey(modulePath, version)); err != nil {
		return fmt.Errorf("deleting version info: %w", err)
	}

	// Delete go.mod
	if err := idx.backend.Delete(ctx, idx.modKey(modulePath, version)); err != nil {
		return fmt.Errorf("deleting go.mod: %w", err)
	}

	// Update version list
	if err := idx.removeVersion(ctx, modulePath, version); err != nil {
		return fmt.Errorf("updating version list: %w", err)
	}

	return nil
}

// addVersion adds a version to the module's version list.
func (idx *Index) addVersion(ctx context.Context, modulePath, version string) error {
	versions, err := idx.ListVersions(ctx, modulePath)
	if err != nil {
		return err
	}

	// Check if version already exists
	for _, v := range versions {
		if v == version {
			return nil
		}
	}

	versions = append(versions, version)
	sort.Strings(versions)

	return idx.writeVersionList(ctx, modulePath, versions)
}

// removeVersion removes a version from the module's version list.
func (idx *Index) removeVersion(ctx context.Context, modulePath, version string) error {
	versions, err := idx.ListVersions(ctx, modulePath)
	if err != nil {
		return err
	}

	// Filter out the version
	filtered := make([]string, 0, len(versions))
	for _, v := range versions {
		if v != version {
			filtered = append(filtered, v)
		}
	}

	if len(filtered) == 0 {
		// Delete the list file if no versions remain
		return idx.backend.Delete(ctx, idx.listKey(modulePath))
	}

	return idx.writeVersionList(ctx, modulePath, filtered)
}

// writeVersionList writes the version list for a module.
func (idx *Index) writeVersionList(ctx context.Context, modulePath string, versions []string) error {
	key := idx.listKey(modulePath)
	content := strings.Join(versions, "\n") + "\n"
	return idx.backend.Write(ctx, key, strings.NewReader(content))
}

// Key generation helpers.
// Keys follow the GOPROXY URL structure for consistency.

func (idx *Index) listKey(modulePath string) string {
	return path.Join("goproxy", encodePath(modulePath), "@v", "list")
}

func (idx *Index) versionKey(modulePath, version string) string {
	return path.Join("goproxy", encodePath(modulePath), "@v", version+".info")
}

func (idx *Index) modKey(modulePath, version string) string {
	return path.Join("goproxy", encodePath(modulePath), "@v", version+".mod")
}

// encodePath encodes a module path for use in URLs/filenames.
// Uppercase letters are replaced with '!' followed by the lowercase letter.
// This matches the Go module proxy protocol encoding.
func encodePath(modulePath string) string {
	var b strings.Builder
	for _, r := range modulePath {
		if r >= 'A' && r <= 'Z' {
			b.WriteByte('!')
			b.WriteRune(r + ('a' - 'A'))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// decodePath decodes a module path from URL/filename encoding.
func decodePath(encoded string) (string, error) {
	var b strings.Builder
	escape := false
	for _, r := range encoded {
		if escape {
			if r >= 'a' && r <= 'z' {
				b.WriteRune(r - ('a' - 'A'))
			} else {
				return "", fmt.Errorf("invalid escape sequence: !%c", r)
			}
			escape = false
		} else if r == '!' {
			escape = true
		} else {
			b.WriteRune(r)
		}
	}
	if escape {
		return "", errors.New("trailing escape character")
	}
	return b.String(), nil
}
