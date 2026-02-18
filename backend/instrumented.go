package backend

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/wolfeidau/content-cache/telemetry"
)

// InstrumentedBackend wraps a Backend with metrics recording.
type InstrumentedBackend struct {
	backend Backend
	name    string
}

// NewInstrumentedBackend creates a new instrumented backend wrapper.
func NewInstrumentedBackend(b Backend, name string) *InstrumentedBackend {
	return &InstrumentedBackend{backend: b, name: name}
}

func (ib *InstrumentedBackend) Write(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	cr := &countingReader{r: r}
	err := ib.backend.Write(ctx, key, cr)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "write", outcome, time.Since(start), cr.n)
	return err
}

func (ib *InstrumentedBackend) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := ib.backend.Read(ctx, key)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "read", outcome, time.Since(start), 0)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (ib *InstrumentedBackend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := ib.backend.Delete(ctx, key)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "delete", outcome, time.Since(start), 0)
	return err
}

func (ib *InstrumentedBackend) Exists(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	exists, err := ib.backend.Exists(ctx, key)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "exists", outcome, time.Since(start), 0)
	return exists, err
}

func (ib *InstrumentedBackend) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := ib.backend.List(ctx, prefix)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "list", outcome, time.Since(start), 0)
	return keys, err
}

// Size delegates to the underlying backend if it implements SizeAwareBackend.
func (ib *InstrumentedBackend) Size(ctx context.Context, key string) (int64, error) {
	sb, ok := ib.backend.(SizeAwareBackend)
	if !ok {
		return 0, ErrNotFound
	}
	start := time.Now()
	size, err := sb.Size(ctx, key)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "size", outcome, time.Since(start), 0)
	return size, err
}

// Writer delegates to the underlying backend if it implements WriterBackend.
func (ib *InstrumentedBackend) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	wb, ok := ib.backend.(WriterBackend)
	if !ok {
		return nil, fmt.Errorf("backend does not support Writer")
	}
	start := time.Now()
	wc, err := wb.Writer(ctx, key)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "writer", outcome, time.Since(start), 0)
	if err != nil {
		return nil, err
	}
	return wc, nil
}

// WriteFramed delegates to the underlying backend if it implements FramedBackend.
func (ib *InstrumentedBackend) WriteFramed(ctx context.Context, key string, header *BlobHeader, body io.Reader) error {
	fb, ok := ib.backend.(FramedBackend)
	if !ok {
		return fmt.Errorf("backend does not support framed writes")
	}
	start := time.Now()
	cr := &countingReader{r: body}
	err := fb.WriteFramed(ctx, key, header, cr)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "write_framed", outcome, time.Since(start), cr.n)
	return err
}

// ReadFramed delegates to the underlying backend if it implements FramedBackend.
func (ib *InstrumentedBackend) ReadFramed(ctx context.Context, key string) (*BlobHeader, io.ReadCloser, error) {
	fb, ok := ib.backend.(FramedBackend)
	if !ok {
		return nil, nil, fmt.Errorf("backend does not support framed reads")
	}
	start := time.Now()
	header, rc, err := fb.ReadFramed(ctx, key)
	outcome := outcomeFromError(err)
	telemetry.RecordBackendOp(ctx, ib.name, "read_framed", outcome, time.Since(start), 0)
	if err != nil {
		return nil, nil, err
	}
	return header, rc, nil
}

// Unwrap returns the underlying backend.
func (ib *InstrumentedBackend) Unwrap() Backend {
	return ib.backend
}

func outcomeFromError(err error) string {
	if err == nil {
		return "success"
	}
	if err == ErrNotFound {
		return "not_found"
	}
	return "error"
}

// countingReader wraps a reader and counts bytes read.
type countingReader struct {
	r io.Reader
	n int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

// Compile-time interface checks
var (
	_ Backend          = (*InstrumentedBackend)(nil)
	_ SizeAwareBackend = (*InstrumentedBackend)(nil)
)
