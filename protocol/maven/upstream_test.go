package maven

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupIDToPath(t *testing.T) {
	tests := []struct {
		name    string
		groupID string
		want    string
	}{
		{
			name:    "simple group",
			groupID: "org.apache.commons",
			want:    "org/apache/commons",
		},
		{
			name:    "single segment",
			groupID: "junit",
			want:    "junit",
		},
		{
			name:    "deep nesting",
			groupID: "com.google.guava",
			want:    "com/google/guava",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := groupIDToPath(tt.groupID)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPathToGroupID(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "simple path",
			path: "org/apache/commons",
			want: "org.apache.commons",
		},
		{
			name: "single segment",
			path: "junit",
			want: "junit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pathToGroupID(tt.path)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestArtifactCoordinate(t *testing.T) {
	tests := []struct {
		name     string
		coord    ArtifactCoordinate
		filename string
		fullPath string
	}{
		{
			name: "simple jar",
			coord: ArtifactCoordinate{
				GroupID:    "org.apache.commons",
				ArtifactID: "commons-lang3",
				Version:    "3.12.0",
				Extension:  "jar",
			},
			filename: "commons-lang3-3.12.0.jar",
			fullPath: "org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar",
		},
		{
			name: "sources jar",
			coord: ArtifactCoordinate{
				GroupID:    "org.apache.commons",
				ArtifactID: "commons-lang3",
				Version:    "3.12.0",
				Classifier: "sources",
				Extension:  "jar",
			},
			filename: "commons-lang3-3.12.0-sources.jar",
			fullPath: "org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0-sources.jar",
		},
		{
			name: "pom file",
			coord: ArtifactCoordinate{
				GroupID:    "org.apache.commons",
				ArtifactID: "commons-lang3",
				Version:    "3.12.0",
				Extension:  "pom",
			},
			filename: "commons-lang3-3.12.0.pom",
			fullPath: "org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.pom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.filename, tt.coord.Filename())
			require.Equal(t, tt.fullPath, tt.coord.FullPath())
		})
	}
}

func TestUpstreamArtifactURL(t *testing.T) {
	u := NewUpstream()

	coord := ArtifactCoordinate{
		GroupID:    "org.apache.commons",
		ArtifactID: "commons-lang3",
		Version:    "3.12.0",
		Extension:  "jar",
	}

	expected := "https://repo.maven.apache.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar"
	require.Equal(t, expected, u.ArtifactURL(coord))
}

func TestUpstreamMetadataURL(t *testing.T) {
	u := NewUpstream()

	expected := "https://repo.maven.apache.org/maven2/org/apache/commons/commons-lang3/maven-metadata.xml"
	require.Equal(t, expected, u.MetadataURL("org.apache.commons", "commons-lang3"))
}

func TestUpstreamFetchMetadataRaw(t *testing.T) {
	metadataXML := `<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>org.example</groupId>
  <artifactId>test</artifactId>
  <versioning>
    <latest>1.0.0</latest>
    <release>1.0.0</release>
    <versions>
      <version>1.0.0</version>
    </versions>
    <lastUpdated>20240101000000</lastUpdated>
  </versioning>
</metadata>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/org/example/test/maven-metadata.xml" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(metadataXML))
			return
		}
		if r.URL.Path == "/not/found/artifact/maven-metadata.xml" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	u := NewUpstream(WithRepositoryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		data, err := u.FetchMetadataRaw(context.Background(), "org.example", "test")
		require.NoError(t, err)
		require.Contains(t, string(data), "<groupId>org.example</groupId>")
	})

	t.Run("not found", func(t *testing.T) {
		_, err := u.FetchMetadataRaw(context.Background(), "not.found", "artifact")
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestUpstreamFetchMetadata(t *testing.T) {
	metadataXML := `<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>org.example</groupId>
  <artifactId>test</artifactId>
  <versioning>
    <latest>2.0.0</latest>
    <release>2.0.0</release>
    <versions>
      <version>1.0.0</version>
      <version>2.0.0</version>
    </versions>
    <lastUpdated>20240101000000</lastUpdated>
  </versioning>
</metadata>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(metadataXML))
	}))
	defer server.Close()

	u := NewUpstream(WithRepositoryURL(server.URL))

	meta, err := u.FetchMetadata(context.Background(), "org.example", "test")
	require.NoError(t, err)
	require.Equal(t, "org.example", meta.GroupID)
	require.Equal(t, "test", meta.ArtifactID)
	require.Equal(t, "2.0.0", meta.Versioning.Latest)
	require.Equal(t, "2.0.0", meta.Versioning.Release)
	require.Equal(t, []string{"1.0.0", "2.0.0"}, meta.Versioning.Versions.Version)
}

func TestUpstreamFetchArtifact(t *testing.T) {
	artifactContent := []byte("fake jar content")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/org/example/test/1.0.0/test-1.0.0.jar" {
			w.Header().Set("Content-Type", "application/java-archive")
			w.Header().Set("Content-Length", "16")
			_, _ = w.Write(artifactContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	u := NewUpstream(WithRepositoryURL(server.URL))

	t.Run("success", func(t *testing.T) {
		coord := ArtifactCoordinate{
			GroupID:    "org.example",
			ArtifactID: "test",
			Version:    "1.0.0",
			Extension:  "jar",
		}
		rc, size, err := u.FetchArtifact(context.Background(), coord)
		require.NoError(t, err)
		require.Equal(t, int64(16), size)
		defer func() { _ = rc.Close() }()
	})

	t.Run("not found", func(t *testing.T) {
		coord := ArtifactCoordinate{
			GroupID:    "not.found",
			ArtifactID: "artifact",
			Version:    "1.0.0",
			Extension:  "jar",
		}
		_, _, err := u.FetchArtifact(context.Background(), coord)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestUpstreamFetchChecksum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/org/example/test/1.0.0/test-1.0.0.jar.sha1":
			// Simple checksum format
			_, _ = w.Write([]byte("da39a3ee5e6b4b0d3255bfef95601890afd80709"))
		case "/org/example/test/1.0.0/test-1.0.0.jar.md5":
			// Checksum with filename format
			_, _ = w.Write([]byte("d41d8cd98f00b204e9800998ecf8427e  test-1.0.0.jar"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	u := NewUpstream(WithRepositoryURL(server.URL))

	coord := ArtifactCoordinate{
		GroupID:    "org.example",
		ArtifactID: "test",
		Version:    "1.0.0",
		Extension:  "jar",
	}

	t.Run("sha1 simple format", func(t *testing.T) {
		checksum, err := u.FetchChecksum(context.Background(), coord, ChecksumSHA1)
		require.NoError(t, err)
		require.Equal(t, "da39a3ee5e6b4b0d3255bfef95601890afd80709", checksum)
	})

	t.Run("md5 with filename format", func(t *testing.T) {
		checksum, err := u.FetchChecksum(context.Background(), coord, ChecksumMD5)
		require.NoError(t, err)
		require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", checksum)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := u.FetchChecksum(context.Background(), coord, ChecksumSHA256)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestWithRepositoryURL(t *testing.T) {
	u := NewUpstream(WithRepositoryURL("https://custom.repo.com/maven2/"))

	coord := ArtifactCoordinate{
		GroupID:    "org.example",
		ArtifactID: "test",
		Version:    "1.0.0",
		Extension:  "jar",
	}

	// Should trim trailing slash
	url := u.ArtifactURL(coord)
	require.Equal(t, "https://custom.repo.com/maven2/org/example/test/1.0.0/test-1.0.0.jar", url)
}
