package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

const (
	meterName = "github.com/wolfeidau/content-cache"
)

// MetricsConfig configures the metrics system.
type MetricsConfig struct {
	// ServiceName is the name of the service for resource attributes.
	ServiceName string

	// ServiceVersion is the version of the service.
	ServiceVersion string

	// OTLPEndpoint is the OTLP gRPC endpoint (e.g., "localhost:4317").
	// If empty, OTLP export is disabled.
	OTLPEndpoint string

	// EnablePrometheus enables the Prometheus /metrics endpoint.
	EnablePrometheus bool

	// FlushInterval is how often to export metrics (default: 10s).
	FlushInterval time.Duration
}

// Metrics holds the OpenTelemetry metric instruments.
type Metrics struct {
	requestsTotal           metric.Int64Counter
	responseBytesTotal      metric.Int64Counter
	requestDuration         metric.Float64Histogram
	requestsByEndpointTotal metric.Int64Counter

	blobWriteSize           metric.Float64Histogram
	upstreamFetchDuration   metric.Float64Histogram
	upstreamFetchTotal      metric.Int64Counter
	upstreamFetchBytesTotal metric.Int64Counter
	blobTouchesTotal        metric.Int64Counter
	backendRequestDuration  metric.Float64Histogram
	backendRequestsTotal    metric.Int64Counter
	backendBytesTotal       metric.Int64Counter

	meterProvider *sdkmetric.MeterProvider
	promHandler   http.Handler
}

var (
	globalMetrics *Metrics
	initOnce      sync.Once
	initErr       error
)

// InitMetrics initializes the OpenTelemetry metrics system.
// Returns a shutdown function that should be called on application exit.
// Uses sync.Once to ensure single initialisation.
func InitMetrics(ctx context.Context, cfg MetricsConfig) (shutdown func(context.Context) error, err error) {
	initOnce.Do(func() {
		initErr = doInitMetrics(ctx, cfg)
	})

	if initErr != nil {
		return nil, initErr
	}

	return shutdownMetrics, nil
}

func doInitMetrics(ctx context.Context, cfg MetricsConfig) error {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "content-cache"
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 10 * time.Second
	}

	// Build resource with service info
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return err
	}

	var readers []sdkmetric.Reader
	var promHandler http.Handler

	// Setup OTLP exporter if endpoint configured
	if cfg.OTLPEndpoint != "" {
		otlpExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlpmetricgrpc.WithInsecure(), // Use WithTLSCredentials for production
		)
		if err != nil {
			return err
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(otlpExporter,
			sdkmetric.WithInterval(cfg.FlushInterval),
		))
	}

	// Setup Prometheus exporter if enabled
	if cfg.EnablePrometheus {
		promExp, err := promexporter.New()
		if err != nil {
			return err
		}
		readers = append(readers, promExp)
		promHandler = promhttp.Handler()
	}

	// If no exporters configured, use a no-op periodic reader to still collect metrics
	if len(readers) == 0 {
		readers = append(readers, sdkmetric.NewPeriodicReader(noopExporter{},
			sdkmetric.WithInterval(cfg.FlushInterval),
		))
	}

	// Build meter provider options
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, r := range readers {
		opts = append(opts, sdkmetric.WithReader(r))
	}

	mp := sdkmetric.NewMeterProvider(opts...)
	otel.SetMeterProvider(mp)

	// Create meter and instruments
	meter := mp.Meter(meterName)

	requestsTotal, err := meter.Int64Counter(
		"content_cache_http_requests_total",
		metric.WithDescription("Total number of HTTP requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return err
	}

	responseBytesTotal, err := meter.Int64Counter(
		"content_cache_http_response_bytes_total",
		metric.WithDescription("Total bytes sent in HTTP responses"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}

	requestDuration, err := meter.Float64Histogram(
		"content_cache_http_request_duration_seconds",
		metric.WithDescription("HTTP request duration in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return err
	}

	requestsByEndpointTotal, err := meter.Int64Counter(
		"content_cache_http_requests_by_endpoint_total",
		metric.WithDescription("Total number of HTTP requests by endpoint (detail metric)"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return err
	}

	blobWriteSize, err := meter.Float64Histogram(
		"content_cache_blob_write_size_bytes",
		metric.WithDescription("Size of blobs written to storage"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(128, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824),
	)
	if err != nil {
		return err
	}

	upstreamFetchDuration, err := meter.Float64Histogram(
		"content_cache_upstream_fetch_duration_seconds",
		metric.WithDescription("Duration of upstream fetch requests"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 60),
	)
	if err != nil {
		return err
	}

	upstreamFetchTotal, err := meter.Int64Counter(
		"content_cache_upstream_fetch_total",
		metric.WithDescription("Total number of upstream fetch requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return err
	}

	upstreamFetchBytesTotal, err := meter.Int64Counter(
		"content_cache_upstream_fetch_bytes_total",
		metric.WithDescription("Total bytes fetched from upstream"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}

	blobTouchesTotal, err := meter.Int64Counter(
		"content_cache_blob_touches_total",
		metric.WithDescription("Total blob access count increments"),
		metric.WithUnit("{touch}"),
	)
	if err != nil {
		return err
	}

	backendRequestDuration, err := meter.Float64Histogram(
		"content_cache_backend_request_duration_seconds",
		metric.WithDescription("Duration of backend storage operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
	)
	if err != nil {
		return err
	}

	backendRequestsTotal, err := meter.Int64Counter(
		"content_cache_backend_requests_total",
		metric.WithDescription("Total number of backend storage operations"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return err
	}

	backendBytesTotal, err := meter.Int64Counter(
		"content_cache_backend_bytes_total",
		metric.WithDescription("Total bytes transferred in backend operations"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}

	globalMetrics = &Metrics{
		requestsTotal:           requestsTotal,
		responseBytesTotal:      responseBytesTotal,
		requestDuration:         requestDuration,
		requestsByEndpointTotal: requestsByEndpointTotal,
		blobWriteSize:           blobWriteSize,
		upstreamFetchDuration:   upstreamFetchDuration,
		upstreamFetchTotal:      upstreamFetchTotal,
		upstreamFetchBytesTotal: upstreamFetchBytesTotal,
		blobTouchesTotal:        blobTouchesTotal,
		backendRequestDuration:  backendRequestDuration,
		backendRequestsTotal:    backendRequestsTotal,
		backendBytesTotal:       backendBytesTotal,
		meterProvider:           mp,
		promHandler:             promHandler,
	}

	return nil
}

// shutdownMetrics shuts down the metrics provider and clears the global state.
func shutdownMetrics(ctx context.Context) error {
	if globalMetrics == nil {
		return nil
	}
	err := globalMetrics.meterProvider.Shutdown(ctx)
	globalMetrics = nil
	return err
}

// RecordHTTP records HTTP request metrics.
// Call this from the logging middleware after the request completes.
// Protocol and cache result are read from request tags set by middleware and handlers.
func RecordHTTP(ctx context.Context, r *http.Request, status int, bytesSent int64, duration time.Duration) {
	if globalMetrics == nil {
		return
	}

	tags := GetTags(r)

	protocol := "unknown"
	cacheResult := string(CacheBypass)
	endpoint := ""
	if tags != nil {
		if tags.Protocol != "" {
			protocol = tags.Protocol
		}
		if tags.CacheResult != "" {
			cacheResult = string(tags.CacheResult)
		}
		endpoint = tags.Endpoint
	}

	statusClass := StatusClass(status)

	// Shared metrics: low cardinality {protocol, status_class, cache_result}
	sharedAttrs := []attribute.KeyValue{
		attribute.String("protocol", protocol),
		attribute.String("status_class", statusClass),
		attribute.String("cache_result", cacheResult),
	}
	globalMetrics.requestsTotal.Add(ctx, 1, metric.WithAttributes(sharedAttrs...))
	globalMetrics.responseBytesTotal.Add(ctx, bytesSent, metric.WithAttributes(sharedAttrs...))
	globalMetrics.requestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(sharedAttrs...))

	// Detail metric: higher cardinality, only when endpoint is set
	if endpoint != "" {
		detailAttrs := []attribute.KeyValue{
			attribute.String("protocol", protocol),
			attribute.String("endpoint", endpoint),
			attribute.String("status_class", statusClass),
			attribute.String("cache_result", cacheResult),
		}
		globalMetrics.requestsByEndpointTotal.Add(ctx, 1, metric.WithAttributes(detailAttrs...))
	}
}

// RecordBackendOp records backend operation metrics.
func RecordBackendOp(ctx context.Context, backend, op, outcome string, duration time.Duration, bytes int64) {
	if globalMetrics == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("backend", backend),
		attribute.String("op", op),
		attribute.String("outcome", outcome),
	}
	globalMetrics.backendRequestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	globalMetrics.backendRequestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	if bytes > 0 {
		globalMetrics.backendBytesTotal.Add(ctx, bytes, metric.WithAttributes(attrs...))
	}
}

// RecordBlobWrite records a blob write with its size.
func RecordBlobWrite(ctx context.Context, protocol string, size int64, isNew bool) {
	if globalMetrics == nil {
		return
	}

	result := "exists"
	if isNew {
		result = "new"
	}

	attrs := []attribute.KeyValue{
		attribute.String("protocol", protocol),
		attribute.String("result", result),
	}
	globalMetrics.blobWriteSize.Record(ctx, float64(size), metric.WithAttributes(attrs...))
}

// RecordUpstreamFetch records an upstream fetch request.
func RecordUpstreamFetch(ctx context.Context, protocol string, duration time.Duration, bytesRead int64, outcome string) {
	if globalMetrics == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("protocol", protocol),
		attribute.String("outcome", outcome),
	}
	globalMetrics.upstreamFetchDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	globalMetrics.upstreamFetchTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if bytesRead > 0 {
		globalMetrics.upstreamFetchBytesTotal.Add(ctx, bytesRead, metric.WithAttributes(attrs...))
	}
}

// RecordBlobTouch records a blob access count increment.
func RecordBlobTouch(ctx context.Context, protocol string, newAccessCount int) {
	if globalMetrics == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("protocol", protocol),
		attribute.String("new_access_count", fmt.Sprintf("%d", newAccessCount)),
	}
	globalMetrics.blobTouchesTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// PrometheusHandler returns the Prometheus metrics HTTP handler.
// Returns a handler that returns 404 if Prometheus export is not enabled,
// allowing safe registration regardless of initialization order.
func PrometheusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if globalMetrics == nil || globalMetrics.promHandler == nil {
			http.NotFound(w, r)
			return
		}
		globalMetrics.promHandler.ServeHTTP(w, r)
	})
}

// StatusClass returns the HTTP status class (2xx, 3xx, 4xx, 5xx).
func StatusClass(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500:
		return "5xx"
	default:
		return "unknown"
	}
}

// noopExporter is a no-op metrics exporter for when no exporters are configured.
type noopExporter struct{}

func (noopExporter) Temporality(_ sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (noopExporter) Aggregation(_ sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return nil
}

func (noopExporter) Export(_ context.Context, _ *metricdata.ResourceMetrics) error {
	return nil
}

func (noopExporter) ForceFlush(_ context.Context) error {
	return nil
}

func (noopExporter) Shutdown(_ context.Context) error {
	return nil
}
