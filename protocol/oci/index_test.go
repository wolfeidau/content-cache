package oci

import (
	"context"
	"os"
	"testing"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

func newTestIndex(t *testing.T) (*Index, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "oci-index-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	b, err := backend.NewFilesystem(tmpDir)
	if err != nil {
		t.Fatalf("NewFilesystem() error = %v", err)
	}
	idx := NewIndex(b)
	return idx, func() {
		_ = os.RemoveAll(tmpDir)
	}
}

func TestIndexTagDigest(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("get non-existent tag", func(t *testing.T) {
		_, _, err := idx.GetTagDigest(ctx, "library/alpine", "latest")
		if err != ErrNotFound {
			t.Errorf("GetTagDigest() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("set and get tag", func(t *testing.T) {
		digest := "sha256:abc123def456"
		err := idx.SetTagDigest(ctx, "library/alpine", "latest", digest)
		if err != nil {
			t.Fatalf("SetTagDigest() error = %v", err)
		}

		gotDigest, refreshedAt, err := idx.GetTagDigest(ctx, "library/alpine", "latest")
		if err != nil {
			t.Fatalf("GetTagDigest() error = %v", err)
		}
		if gotDigest != digest {
			t.Errorf("GetTagDigest() digest = %q, want %q", gotDigest, digest)
		}
		if refreshedAt.IsZero() {
			t.Error("GetTagDigest() refreshedAt should not be zero")
		}
	})

	t.Run("update tag", func(t *testing.T) {
		newDigest := "sha256:newdigest789"
		err := idx.SetTagDigest(ctx, "library/alpine", "latest", newDigest)
		if err != nil {
			t.Fatalf("SetTagDigest() error = %v", err)
		}

		gotDigest, _, err := idx.GetTagDigest(ctx, "library/alpine", "latest")
		if err != nil {
			t.Fatalf("GetTagDigest() error = %v", err)
		}
		if gotDigest != newDigest {
			t.Errorf("GetTagDigest() digest = %q, want %q", gotDigest, newDigest)
		}
	})

	t.Run("multiple tags", func(t *testing.T) {
		_ = idx.SetTagDigest(ctx, "library/alpine", "3.18", "sha256:version318")
		_ = idx.SetTagDigest(ctx, "library/alpine", "3.19", "sha256:version319")

		d1, _, _ := idx.GetTagDigest(ctx, "library/alpine", "3.18")
		d2, _, _ := idx.GetTagDigest(ctx, "library/alpine", "3.19")

		if d1 != "sha256:version318" {
			t.Errorf("tag 3.18 = %q", d1)
		}
		if d2 != "sha256:version319" {
			t.Errorf("tag 3.19 = %q", d2)
		}
	})
}

func TestIndexRefreshTag(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Set up a tag
	_ = idx.SetTagDigest(ctx, "library/nginx", "latest", "sha256:original")
	_, originalRefresh, _ := idx.GetTagDigest(ctx, "library/nginx", "latest")

	// Wait a bit and refresh
	time.Sleep(10 * time.Millisecond)
	err := idx.RefreshTag(ctx, "library/nginx", "latest")
	if err != nil {
		t.Fatalf("RefreshTag() error = %v", err)
	}

	_, newRefresh, _ := idx.GetTagDigest(ctx, "library/nginx", "latest")
	if !newRefresh.After(originalRefresh) {
		t.Error("RefreshTag() should update refreshedAt")
	}
}

func TestIndexManifest(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("get non-existent manifest", func(t *testing.T) {
		_, err := idx.GetManifest(ctx, "sha256:nonexistent")
		if err != ErrNotFound {
			t.Errorf("GetManifest() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("put and get manifest", func(t *testing.T) {
		digest := "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		mediaType := "application/vnd.oci.image.manifest.v1+json"
		hash := contentcache.Hash{1, 2, 3, 4}
		size := int64(1024)

		err := idx.PutManifest(ctx, digest, mediaType, hash, size)
		if err != nil {
			t.Fatalf("PutManifest() error = %v", err)
		}

		manifest, err := idx.GetManifest(ctx, digest)
		if err != nil {
			t.Fatalf("GetManifest() error = %v", err)
		}
		if manifest.Digest != digest {
			t.Errorf("Digest = %q, want %q", manifest.Digest, digest)
		}
		if manifest.MediaType != mediaType {
			t.Errorf("MediaType = %q, want %q", manifest.MediaType, mediaType)
		}
		if manifest.ContentHash != hash {
			t.Errorf("ContentHash = %v, want %v", manifest.ContentHash, hash)
		}
		if manifest.Size != size {
			t.Errorf("Size = %d, want %d", manifest.Size, size)
		}
	})
}

func TestIndexBlob(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("get non-existent blob", func(t *testing.T) {
		_, err := idx.GetBlob(ctx, "sha256:nonexistent")
		if err != ErrNotFound {
			t.Errorf("GetBlob() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("put and get blob", func(t *testing.T) {
		digest := "sha256:abc123def456789"
		hash := contentcache.Hash{5, 6, 7, 8}
		size := int64(2048)

		err := idx.PutBlob(ctx, digest, hash, size)
		if err != nil {
			t.Fatalf("PutBlob() error = %v", err)
		}

		blob, err := idx.GetBlob(ctx, digest)
		if err != nil {
			t.Fatalf("GetBlob() error = %v", err)
		}
		if blob.Digest != digest {
			t.Errorf("Digest = %q, want %q", blob.Digest, digest)
		}
		if blob.ContentHash != hash {
			t.Errorf("ContentHash = %v, want %v", blob.ContentHash, hash)
		}
		if blob.Size != size {
			t.Errorf("Size = %d, want %d", blob.Size, size)
		}
	})
}

func TestIndexListImages(t *testing.T) {
	idx, cleanup := newTestIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Empty list
	images, err := idx.ListImages(ctx)
	if err != nil {
		t.Fatalf("ListImages() error = %v", err)
	}
	if len(images) != 0 {
		t.Errorf("ListImages() = %v, want empty", images)
	}

	// Add some images
	_ = idx.SetTagDigest(ctx, "library/alpine", "latest", "sha256:a")
	_ = idx.SetTagDigest(ctx, "library/nginx", "latest", "sha256:b")
	_ = idx.SetTagDigest(ctx, "myrepo/myimage", "v1", "sha256:c")

	images, err = idx.ListImages(ctx)
	if err != nil {
		t.Fatalf("ListImages() error = %v", err)
	}
	if len(images) != 3 {
		t.Errorf("ListImages() len = %d, want 3", len(images))
	}
}

func TestEncodeDecodeImageName(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"library/alpine"},
		{"myrepo/myimage"},
		{"gcr.io/project/image"},
		{"registry.example.com:5000/nested/path/image"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeImageName(tt.name)
			decoded, err := decodeImageName(encoded)
			if err != nil {
				t.Fatalf("decodeImageName() error = %v", err)
			}
			if decoded != tt.name {
				t.Errorf("round-trip: got %q, want %q", decoded, tt.name)
			}
		})
	}
}
