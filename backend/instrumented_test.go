package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstrumentedBackend_Write(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	err = ib.Write(ctx, "test/key", strings.NewReader("hello world"))
	require.NoError(t, err)
}

func TestInstrumentedBackend_Read_CountsBytes(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	content := "hello, instrumented backend"
	require.NoError(t, ib.Write(ctx, "test/key", strings.NewReader(content)))

	rc, err := ib.Read(ctx, "test/key")
	require.NoError(t, err)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, string(got))

	// Close triggers metric recording — must not error
	require.NoError(t, rc.Close())
}

func TestInstrumentedBackend_Read_NotFound(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	_, err = ib.Read(ctx, "nonexistent/key")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestInstrumentedBackend_Exists(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	exists, err := ib.Exists(ctx, "missing/key")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, ib.Write(ctx, "present/key", strings.NewReader("data")))
	exists, err = ib.Exists(ctx, "present/key")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestInstrumentedBackend_Delete(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	require.NoError(t, ib.Write(ctx, "del/key", strings.NewReader("bye")))
	require.NoError(t, ib.Delete(ctx, "del/key"))

	exists, err := ib.Exists(ctx, "del/key")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestInstrumentedBackend_List(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	require.NoError(t, ib.Write(ctx, "list/a", strings.NewReader("a")))
	require.NoError(t, ib.Write(ctx, "list/b", strings.NewReader("b")))

	keys, err := ib.List(ctx, "list/")
	require.NoError(t, err)
	require.Len(t, keys, 2)
}

func TestInstrumentedBackend_Size(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	content := "size test content"
	require.NoError(t, ib.Write(ctx, "size/key", strings.NewReader(content)))

	size, err := ib.Size(ctx, "size/key")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)
}

func TestInstrumentedBackend_WriteFramed_ReadFramed(t *testing.T) {
	fs, err := NewFilesystem(t.TempDir())
	require.NoError(t, err)

	ib := NewInstrumentedBackend(fs, "filesystem")
	ctx := context.Background()

	body := []byte("framed content")
	header := &BlobHeader{ContentType: "application/octet-stream"}

	err = ib.WriteFramed(ctx, "framed/key", header, bytes.NewReader(body))
	require.NoError(t, err)

	gotHeader, rc, err := ib.ReadFramed(ctx, "framed/key")
	require.NoError(t, err)
	require.Equal(t, "application/octet-stream", gotHeader.ContentType)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got)
	require.NoError(t, rc.Close())
}

func TestOutcomeFromError(t *testing.T) {
	require.Equal(t, "success", outcomeFromError(nil))
	require.Equal(t, "not_found", outcomeFromError(ErrNotFound))
	require.Equal(t, "not_found", outcomeFromError(fmt.Errorf("wrap: %w", ErrNotFound)))
	require.Equal(t, "error", outcomeFromError(errors.New("some other error")))
}
