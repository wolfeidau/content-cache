package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// setupTestMetrics creates a Metrics instance backed by a ManualReader for testing.
// Returns the reader (to collect metrics) and a cleanup function.
func setupTestMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter(meterName)

	requestsTotal, err := meter.Int64Counter("content_cache_http_requests_total")
	require.NoError(t, err)

	responseBytesTotal, err := meter.Int64Counter("content_cache_http_response_bytes_total")
	require.NoError(t, err)

	requestDuration, err := meter.Float64Histogram("content_cache_http_request_duration_seconds")
	require.NoError(t, err)

	requestsByEndpointTotal, err := meter.Int64Counter("content_cache_http_requests_by_endpoint_total")
	require.NoError(t, err)

	globalMetrics = &Metrics{
		requestsTotal:           requestsTotal,
		responseBytesTotal:      responseBytesTotal,
		requestDuration:         requestDuration,
		requestsByEndpointTotal: requestsByEndpointTotal,
		meterProvider:           mp,
	}

	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		globalMetrics = nil
	})

	return reader
}

// collectMetrics reads all metrics from the ManualReader.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

// findCounter finds a counter metric by name and returns its data points.
func findCounter(rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					return sum.DataPoints
				}
			}
		}
	}
	return nil
}

// findHistogram finds a histogram metric by name and returns its data points.
func findHistogram(rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[float64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
					return hist.DataPoints
				}
			}
		}
	}
	return nil
}

// hasAttr checks if a data point's attribute set contains the given key-value pair.
func hasAttr(attrs attribute.Set, key, value string) bool {
	v, ok := attrs.Value(attribute.Key(key))
	return ok && v.AsString() == value
}

func TestRecordHTTP_SharedMetrics(t *testing.T) {
	reader := setupTestMetrics(t)

	r := httptest.NewRequest(http.MethodGet, "/npm/lodash", nil)
	r = InjectTags(r)
	SetProtocol(r, "npm")
	SetCacheResult(r, CacheHit)

	RecordHTTP(context.Background(), r, http.StatusOK, 1024, 50*time.Millisecond)

	rm := collectMetrics(t, reader)

	// Verify requests_total
	dps := findCounter(rm, "content_cache_http_requests_total")
	require.Len(t, dps, 1)
	require.EqualValues(t, 1, dps[0].Value)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "npm"))
	require.True(t, hasAttr(dps[0].Attributes, "status_class", "2xx"))
	require.True(t, hasAttr(dps[0].Attributes, "cache_result", "hit"))

	// Verify response_bytes_total
	bytesDps := findCounter(rm, "content_cache_http_response_bytes_total")
	require.Len(t, bytesDps, 1)
	require.EqualValues(t, 1024, bytesDps[0].Value)

	// Verify request_duration histogram
	histDps := findHistogram(rm, "content_cache_http_request_duration_seconds")
	require.Len(t, histDps, 1)
	require.Equal(t, uint64(1), histDps[0].Count)

	// Shared metrics must NOT include endpoint attribute
	_, hasEndpoint := dps[0].Attributes.Value(attribute.Key("endpoint"))
	require.False(t, hasEndpoint)
}

func TestRecordHTTP_DetailMetricWithEndpoint(t *testing.T) {
	reader := setupTestMetrics(t)

	r := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/sha256:abc", nil)
	r = InjectTags(r)
	SetProtocol(r, "oci")
	SetCacheResult(r, CacheMiss)
	SetEndpoint(r, "blob")

	RecordHTTP(context.Background(), r, http.StatusOK, 4096, 100*time.Millisecond)

	rm := collectMetrics(t, reader)

	dps := findCounter(rm, "content_cache_http_requests_by_endpoint_total")
	require.Len(t, dps, 1)
	require.EqualValues(t, 1, dps[0].Value)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "oci"))
	require.True(t, hasAttr(dps[0].Attributes, "endpoint", "blob"))
	require.True(t, hasAttr(dps[0].Attributes, "status_class", "2xx"))
	require.True(t, hasAttr(dps[0].Attributes, "cache_result", "miss"))
}

func TestRecordHTTP_NoDetailMetricWithoutEndpoint(t *testing.T) {
	reader := setupTestMetrics(t)

	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r = InjectTags(r)
	SetProtocol(r, "internal")
	SetCacheResult(r, CacheNA)
	// No SetEndpoint call

	RecordHTTP(context.Background(), r, http.StatusOK, 15, 1*time.Millisecond)

	rm := collectMetrics(t, reader)

	// Shared metrics should exist
	dps := findCounter(rm, "content_cache_http_requests_total")
	require.Len(t, dps, 1)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "internal"))
	require.True(t, hasAttr(dps[0].Attributes, "cache_result", "na"))

	// Detail metric should have no data points
	detailDps := findCounter(rm, "content_cache_http_requests_by_endpoint_total")
	require.Empty(t, detailDps)
}

func TestRecordHTTP_DefaultsWhenNoTags(t *testing.T) {
	reader := setupTestMetrics(t)

	// Request without InjectTags — simulates a request that bypasses middleware
	r := httptest.NewRequest(http.MethodGet, "/unknown", nil)

	RecordHTTP(context.Background(), r, http.StatusNotFound, 0, 1*time.Millisecond)

	rm := collectMetrics(t, reader)

	dps := findCounter(rm, "content_cache_http_requests_total")
	require.Len(t, dps, 1)
	require.True(t, hasAttr(dps[0].Attributes, "protocol", "unknown"))
	require.True(t, hasAttr(dps[0].Attributes, "cache_result", "bypass"))
	require.True(t, hasAttr(dps[0].Attributes, "status_class", "4xx"))
}

func TestRecordHTTP_NilGlobalMetrics(t *testing.T) {
	globalMetrics = nil

	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	r = InjectTags(r)

	// Should not panic
	RecordHTTP(context.Background(), r, http.StatusOK, 0, 1*time.Millisecond)
}

func TestStatusClass(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{200, "2xx"},
		{201, "2xx"},
		{299, "2xx"},
		{301, "3xx"},
		{304, "3xx"},
		{400, "4xx"},
		{404, "4xx"},
		{500, "5xx"},
		{503, "5xx"},
		{100, "unknown"},
		{0, "unknown"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, StatusClass(tt.status), "StatusClass(%d)", tt.status)
	}
}
