package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// setupTransportMetrics extends setupTestMetrics to also register the upstream fetch instruments.
func setupTransportMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter(meterName)

	upstreamFetchDuration, err := meter.Float64Histogram("content_cache_upstream_fetch_duration_seconds")
	require.NoError(t, err)
	upstreamFetchTotal, err := meter.Int64Counter("content_cache_upstream_fetch_total")
	require.NoError(t, err)
	upstreamFetchBytesTotal, err := meter.Int64Counter("content_cache_upstream_fetch_bytes_total")
	require.NoError(t, err)

	globalMetrics = &Metrics{
		upstreamFetchDuration:   upstreamFetchDuration,
		upstreamFetchTotal:      upstreamFetchTotal,
		upstreamFetchBytesTotal: upstreamFetchBytesTotal,
		meterProvider:           mp,
	}

	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		globalMetrics = nil
	})

	return reader
}

func TestInstrumentedTransport_Success(t *testing.T) {
	reader := setupTransportMetrics(t)

	body := "response body content"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "goproxy")
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(got))
	require.NoError(t, resp.Body.Close())

	rm := collectMetrics(t, reader)

	// Fetch total should be recorded after body close
	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)
	require.EqualValues(t, 1, dps[0].Value)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "goproxy"))
	require.True(t, hasAttr(dps[0].Attributes, "outcome", "success"))

	// Bytes total should reflect response body size
	bytesDps := findCounter(rm, "content_cache_upstream_fetch_bytes_total")
	require.Len(t, bytesDps, 1)
	require.Equal(t, int64(len(body)), bytesDps[0].Value)

	// Duration histogram should have one data point
	histDps := findHistogram(rm, "content_cache_upstream_fetch_duration_seconds")
	require.Len(t, histDps, 1)
	require.Equal(t, uint64(1), histDps[0].Count)
}

func TestInstrumentedTransport_HTTP4xx(t *testing.T) {
	reader := setupTransportMetrics(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "npm")
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())

	rm := collectMetrics(t, reader)

	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)
	require.True(t, hasAttr(dps[0].Attributes, "outcome", "4xx"))
}

func TestInstrumentedTransport_HTTP5xx(t *testing.T) {
	reader := setupTransportMetrics(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "oci")
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())

	rm := collectMetrics(t, reader)

	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)
	require.True(t, hasAttr(dps[0].Attributes, "outcome", "5xx"))
}

func TestInstrumentedTransport_ConnectionError(t *testing.T) {
	reader := setupTransportMetrics(t)

	transport := NewInstrumentedTransport(nil, "pypi")
	client := &http.Client{Transport: transport, Timeout: 100 * time.Millisecond}

	// Use a port that is not listening
	_, err := client.Get("http://127.0.0.1:1")
	require.Error(t, err)

	rm := collectMetrics(t, reader)

	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "pypi"))
	// outcome is "error" for connection failures
	require.True(t, hasAttr(dps[0].Attributes, "outcome", "error"))
}

func TestInstrumentedTransport_Canceled(t *testing.T) {
	reader := setupTransportMetrics(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block indefinitely so the client times out
		<-r.Context().Done()
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "maven")
	client := &http.Client{Transport: transport}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	_, err = client.Do(req)
	require.Error(t, err)

	rm := collectMetrics(t, reader)

	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "maven"))
	require.True(t, hasAttr(dps[0].Attributes, "outcome", "canceled"))
}

func TestInstrumentedTransport_BodyCloseIdempotent(t *testing.T) {
	reader := setupTransportMetrics(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "rubygems")
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	// Second close must not double-record
	require.NoError(t, resp.Body.Close())

	rm := collectMetrics(t, reader)

	// Only one fetch recorded despite two Close calls
	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)
	require.EqualValues(t, 1, dps[0].Value)
}

func TestInstrumentedTransport_NilBaseUsesDefault(t *testing.T) {
	tr := NewInstrumentedTransport(nil, "git")
	require.Equal(t, http.DefaultTransport, tr.base)
}

func TestInstrumentedTransport_CustomBase(t *testing.T) {
	custom := &http.Transport{}
	tr := NewInstrumentedTransport(custom, "git")
	require.Equal(t, custom, tr.base)
}

func TestInstrumentedTransport_NilGlobalMetrics(t *testing.T) {
	globalMetrics = nil

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "git")
	client := &http.Client{Transport: transport}

	// Must not panic when metrics are not initialised
	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
}

func TestInstrumentedTransport_BytesOnlyRecordedWhenPositive(t *testing.T) {
	reader := setupTransportMetrics(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent) // 204, empty body
	}))
	defer srv.Close()

	transport := NewInstrumentedTransport(nil, "goproxy")
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())

	rm := collectMetrics(t, reader)

	// Fetch total recorded
	dps := findCounter(rm, "content_cache_upstream_fetch_total")
	require.Len(t, dps, 1)

	// Bytes total NOT recorded (body was empty)
	bytesDps := findCounter(rm, "content_cache_upstream_fetch_bytes_total")
	require.Empty(t, bytesDps)
}

// Ensure we satisfy the http.RoundTripper interface.
var _ http.RoundTripper = (*InstrumentedTransport)(nil)

// Ensure io.ReadCloser is satisfied by instrumentedBody.
var _ io.ReadCloser = (*instrumentedBody)(nil)

// Ensure the body keeps the reader readable before close.
func TestInstrumentedBody_ReadBeforeClose(t *testing.T) {
	inner := io.NopCloser(strings.NewReader("test data"))
	b := &instrumentedBody{
		ReadCloser: inner,
		ctx:        context.Background(),
		protocol:   "test",
		start:      time.Now(),
		outcome:    "success",
	}

	buf := make([]byte, 4)
	n, err := b.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "test", string(buf))
	require.EqualValues(t, 4, b.bytes)
}
