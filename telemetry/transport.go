package telemetry

import (
	"context"
	"io"
	"net/http"
	"time"
)

// InstrumentedTransport wraps an http.RoundTripper with upstream fetch metrics.
type InstrumentedTransport struct {
	base     http.RoundTripper
	protocol string
}

// NewInstrumentedTransport creates a new instrumented transport for a protocol.
// If base is nil, http.DefaultTransport is used.
func NewInstrumentedTransport(base http.RoundTripper, protocol string) *InstrumentedTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &InstrumentedTransport{base: base, protocol: protocol}
}

// RoundTrip implements http.RoundTripper with metrics recording.
func (t *InstrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()

	resp, err := t.base.RoundTrip(req)
	duration := time.Since(start)

	if err != nil {
		outcome := "error"
		if req.Context().Err() != nil {
			outcome = "canceled"
		}
		RecordUpstreamFetch(req.Context(), t.protocol, duration, 0, outcome)
		return nil, err
	}

	outcome := "success"
	if resp.StatusCode >= 500 {
		outcome = "5xx"
	} else if resp.StatusCode >= 400 {
		outcome = "4xx"
	}

	resp.Body = &instrumentedBody{
		ReadCloser: resp.Body,
		ctx:        req.Context(),
		protocol:   t.protocol,
		start:      start,
		outcome:    outcome,
	}

	return resp, nil
}

// instrumentedBody wraps a response body to record bytes read on close.
type instrumentedBody struct {
	io.ReadCloser
	ctx      context.Context
	protocol string
	start    time.Time
	bytes    int64
	outcome  string
	recorded bool
}

func (b *instrumentedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes += int64(n)
	return n, err
}

func (b *instrumentedBody) Close() error {
	if !b.recorded {
		b.recorded = true
		RecordUpstreamFetch(b.ctx, b.protocol, time.Since(b.start), b.bytes, b.outcome)
	}
	return b.ReadCloser.Close()
}
