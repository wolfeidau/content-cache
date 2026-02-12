package gc

import (
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds GC-related OpenTelemetry metric instruments.
type Metrics struct {
	runsTotal                metric.Int64Counter
	runDuration              metric.Float64Histogram
	unreferencedBlobsDeleted metric.Int64Counter
	orphanBlobsDeleted       metric.Int64Counter
	expiredMetaDeleted       metric.Int64Counter
	lruBlobsEvicted          metric.Int64Counter
	bytesReclaimed           metric.Int64Counter
	errorsTotal              metric.Int64Counter
	lastRunTimestamp         metric.Float64Gauge
	lastRunSuccess           metric.Float64Gauge
}

// NewMetrics creates a new Metrics instance with the given meter.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	runsTotal, err := meter.Int64Counter(
		"content_cache_gc_runs_total",
		metric.WithDescription("Total number of GC runs"),
		metric.WithUnit("{run}"),
	)
	if err != nil {
		return nil, err
	}

	runDuration, err := meter.Float64Histogram(
		"content_cache_gc_run_duration_seconds",
		metric.WithDescription("GC run duration in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 0.5, 1, 5, 10, 30, 60, 120, 300),
	)
	if err != nil {
		return nil, err
	}

	unreferencedBlobsDeleted, err := meter.Int64Counter(
		"content_cache_gc_unreferenced_blobs_deleted_total",
		metric.WithDescription("Total number of unreferenced blobs deleted (RefCount==0)"),
		metric.WithUnit("{blob}"),
	)
	if err != nil {
		return nil, err
	}

	orphanBlobsDeleted, err := meter.Int64Counter(
		"content_cache_gc_orphan_blobs_deleted_total",
		metric.WithDescription("Total number of orphan blobs deleted (on disk but missing from DB)"),
		metric.WithUnit("{blob}"),
	)
	if err != nil {
		return nil, err
	}

	expiredMetaDeleted, err := meter.Int64Counter(
		"content_cache_gc_expired_meta_deleted_total",
		metric.WithDescription("Total number of expired metadata entries deleted"),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return nil, err
	}

	lruBlobsEvicted, err := meter.Int64Counter(
		"content_cache_gc_lru_blobs_evicted_total",
		metric.WithDescription("Total number of blobs evicted by LRU policy"),
		metric.WithUnit("{blob}"),
	)
	if err != nil {
		return nil, err
	}

	bytesReclaimed, err := meter.Int64Counter(
		"content_cache_gc_bytes_reclaimed_total",
		metric.WithDescription("Total bytes reclaimed by GC"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	errorsTotal, err := meter.Int64Counter(
		"content_cache_gc_errors_total",
		metric.WithDescription("Total number of GC errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	lastRunTimestamp, err := meter.Float64Gauge(
		"content_cache_gc_last_run_timestamp_seconds",
		metric.WithDescription("Unix timestamp of last GC run"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	lastRunSuccess, err := meter.Float64Gauge(
		"content_cache_gc_last_run_success",
		metric.WithDescription("Whether last GC run was successful (1=success, 0=failure)"),
		metric.WithUnit("{status}"),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		runsTotal:                runsTotal,
		runDuration:              runDuration,
		unreferencedBlobsDeleted: unreferencedBlobsDeleted,
		orphanBlobsDeleted:       orphanBlobsDeleted,
		expiredMetaDeleted:       expiredMetaDeleted,
		lruBlobsEvicted:          lruBlobsEvicted,
		bytesReclaimed:           bytesReclaimed,
		errorsTotal:              errorsTotal,
		lastRunTimestamp:         lastRunTimestamp,
		lastRunSuccess:           lastRunSuccess,
	}, nil
}
