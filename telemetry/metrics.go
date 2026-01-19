package telemetry

import (
	"context"
	"net/http"
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
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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
	requestsTotal  metric.Int64Counter
	responseBytesTotal metric.Int64Counter
	requestDuration metric.Float64Histogram

	meterProvider *sdkmetric.MeterProvider
	promHandler   http.Handler
}

var globalMetrics *Metrics

// InitMetrics initializes the OpenTelemetry metrics system.
// Returns a shutdown function that should be called on application exit.
func InitMetrics(ctx context.Context, cfg MetricsConfig) (shutdown func(context.Context) error, err error) {
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
		return nil, err
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
			return nil, err
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(otlpExporter,
			sdkmetric.WithInterval(cfg.FlushInterval),
		))
	}

	// Setup Prometheus exporter if enabled
	if cfg.EnablePrometheus {
		promExp, err := promexporter.New()
		if err != nil {
			return nil, err
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
		return nil, err
	}

	responseBytesTotal, err := meter.Int64Counter(
		"content_cache_http_response_bytes_total",
		metric.WithDescription("Total bytes sent in HTTP responses"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	requestDuration, err := meter.Float64Histogram(
		"content_cache_http_request_duration_seconds",
		metric.WithDescription("HTTP request duration in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	globalMetrics = &Metrics{
		requestsTotal:      requestsTotal,
		responseBytesTotal: responseBytesTotal,
		requestDuration:    requestDuration,
		meterProvider:      mp,
		promHandler:        promHandler,
	}

	return mp.Shutdown, nil
}

// RecordHTTP records HTTP request metrics.
// Call this from the logging middleware after the request completes.
func RecordHTTP(ctx context.Context, r *http.Request, protocol string, status int, bytesSent int64, duration time.Duration) {
	if globalMetrics == nil {
		return
	}

	tags := GetTags(r)

	// Build attributes
	endpoint := "unknown"
	cacheResult := "unknown"
	if tags != nil {
		if tags.Endpoint != "" {
			endpoint = tags.Endpoint
		}
		if tags.CacheResult != "" {
			cacheResult = string(tags.CacheResult)
		}
	}

	attrs := []attribute.KeyValue{
		attribute.String("protocol", protocol),
		attribute.String("endpoint", endpoint),
		attribute.String("cache_result", cacheResult),
		attribute.String("status_class", statusClass(status)),
		attribute.String("method", r.Method),
	}

	// Record metrics
	globalMetrics.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	globalMetrics.responseBytesTotal.Add(ctx, bytesSent, metric.WithAttributes(attrs...))
	globalMetrics.requestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// PrometheusHandler returns the Prometheus metrics HTTP handler.
// Returns nil if Prometheus export is not enabled.
func PrometheusHandler() http.Handler {
	if globalMetrics == nil {
		return nil
	}
	return globalMetrics.promHandler
}

// statusClass returns the HTTP status class (2xx, 3xx, 4xx, 5xx).
func statusClass(status int) string {
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
