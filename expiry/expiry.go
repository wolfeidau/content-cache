package expiry

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
)

// Config holds expiration configuration.
type Config struct {
	// TTL is the time-to-live for blobs since last access.
	// Blobs not accessed within this duration are eligible for expiration.
	// Zero means no TTL-based expiration.
	TTL time.Duration

	// MaxSize is the maximum total size of cached blobs in bytes.
	// When exceeded, LRU eviction removes oldest blobs until under limit.
	// Zero means no size limit.
	MaxSize int64

	// CheckInterval is how often to run expiration checks.
	// Default is 1 hour.
	CheckInterval time.Duration

	// Logger for expiration events.
	Logger *slog.Logger
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		TTL:           7 * 24 * time.Hour,      // 7 days
		MaxSize:       10 * 1024 * 1024 * 1024, // 10 GB
		CheckInterval: 1 * time.Hour,
		Logger:        slog.Default(),
	}
}

// Manager handles blob expiration using TTL and LRU strategies.
type Manager struct {
	config   Config
	metadata *MetadataStore
	backend  backend.Backend
	logger   *slog.Logger
	now      func() time.Time

	mu      sync.Mutex
	running bool
	stopped bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewManager creates a new expiration manager.
func NewManager(meta *MetadataStore, b backend.Backend, cfg Config) *Manager {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 1 * time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Manager{
		config:   cfg,
		metadata: meta,
		backend:  b,
		logger:   cfg.Logger,
		now:      time.Now,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start begins background expiration checks.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	if m.running {
		m.mu.Unlock()
		return nil
	}
	m.running = true
	m.mu.Unlock()

	go m.run(ctx)
	return nil
}

// Stop stops background expiration checks.
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.running || m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	m.mu.Unlock()

	close(m.stopCh)
	<-m.doneCh
}

func (m *Manager) run(ctx context.Context) {
	defer close(m.doneCh)

	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	// Run immediately on start
	m.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.runOnce(ctx)
		}
	}
}

// RunOnce performs a single expiration check.
func (m *Manager) RunOnce(ctx context.Context) *ExpireResult {
	return m.runOnce(ctx)
}

// ExpireResult contains the results of an expiration run.
type ExpireResult struct {
	TTLExpired int
	LRUEvicted int
	BytesFreed int64
	Errors     int
	Duration   time.Duration
}

func (m *Manager) runOnce(ctx context.Context) *ExpireResult {
	start := m.now()
	result := &ExpireResult{}

	m.logger.Debug("starting expiration check")

	// Get all blob metadata
	blobs, err := m.metadata.List(ctx)
	if err != nil {
		m.logger.Error("failed to list metadata", "error", err)
		result.Errors++
		return result
	}

	// Phase 1: TTL expiration
	if m.config.TTL > 0 {
		ttlResult := m.expireByTTL(ctx, blobs)
		result.TTLExpired = ttlResult.expired
		result.BytesFreed += ttlResult.bytesFreed
		result.Errors += ttlResult.errors

		// Remove expired blobs from list for LRU phase
		blobs = ttlResult.remaining
	}

	// Phase 2: LRU eviction if over size limit
	if m.config.MaxSize > 0 {
		lruResult := m.evictByLRU(ctx, blobs)
		result.LRUEvicted = lruResult.evicted
		result.BytesFreed += lruResult.bytesFreed
		result.Errors += lruResult.errors
	}

	result.Duration = m.now().Sub(start)

	if result.TTLExpired > 0 || result.LRUEvicted > 0 {
		m.logger.Info("expiration complete",
			"ttl_expired", result.TTLExpired,
			"lru_evicted", result.LRUEvicted,
			"bytes_freed", result.BytesFreed,
			"duration", result.Duration,
		)
	} else {
		m.logger.Debug("expiration complete, nothing to expire")
	}

	return result
}

type ttlResult struct {
	expired    int
	bytesFreed int64
	errors     int
	remaining  []*BlobMetadata
}

func (m *Manager) expireByTTL(ctx context.Context, blobs []*BlobMetadata) ttlResult {
	result := ttlResult{}
	cutoff := m.now().Add(-m.config.TTL)

	for _, meta := range blobs {
		if meta.LastAccessed.Before(cutoff) {
			if err := m.deleteBlob(ctx, meta.Hash); err != nil {
				m.logger.Warn("failed to delete expired blob",
					"hash", meta.Hash.ShortString(),
					"error", err,
				)
				result.errors++
				continue
			}
			result.expired++
			result.bytesFreed += meta.Size
			m.logger.Debug("expired blob by TTL",
				"hash", meta.Hash.ShortString(),
				"last_accessed", meta.LastAccessed,
				"age", m.now().Sub(meta.LastAccessed),
			)
		} else {
			result.remaining = append(result.remaining, meta)
		}
	}

	return result
}

type lruResult struct {
	evicted    int
	bytesFreed int64
	errors     int
}

func (m *Manager) evictByLRU(ctx context.Context, blobs []*BlobMetadata) lruResult {
	result := lruResult{}

	// Calculate current total size
	var totalSize int64
	for _, meta := range blobs {
		totalSize += meta.Size
	}

	if totalSize <= m.config.MaxSize {
		return result // Under limit, nothing to do
	}

	// Sort by last accessed time (oldest first)
	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].LastAccessed.Before(blobs[j].LastAccessed)
	})

	// Evict oldest until under limit
	for _, meta := range blobs {
		if totalSize <= m.config.MaxSize {
			break
		}

		if err := m.deleteBlob(ctx, meta.Hash); err != nil {
			m.logger.Warn("failed to evict blob by LRU",
				"hash", meta.Hash.ShortString(),
				"error", err,
			)
			result.errors++
			continue
		}

		result.evicted++
		result.bytesFreed += meta.Size
		totalSize -= meta.Size

		m.logger.Debug("evicted blob by LRU",
			"hash", meta.Hash.ShortString(),
			"last_accessed", meta.LastAccessed,
			"size", meta.Size,
		)
	}

	return result
}

func (m *Manager) deleteBlob(ctx context.Context, hash contentcache.Hash) error {
	// Delete the blob data
	key := blobKey(hash)
	if err := m.backend.Delete(ctx, key); err != nil {
		return err
	}

	// Delete the metadata
	return m.metadata.Delete(ctx, hash)
}

// ForceExpire immediately expires blobs matching the given criteria.
func (m *Manager) ForceExpire(ctx context.Context, olderThan time.Duration) *ExpireResult {
	result := &ExpireResult{}
	start := m.now()

	blobs, err := m.metadata.List(ctx)
	if err != nil {
		result.Errors++
		return result
	}

	cutoff := m.now().Add(-olderThan)
	for _, meta := range blobs {
		if meta.LastAccessed.Before(cutoff) {
			if err := m.deleteBlob(ctx, meta.Hash); err != nil {
				result.Errors++
				continue
			}
			result.TTLExpired++
			result.BytesFreed += meta.Size
		}
	}

	result.Duration = m.now().Sub(start)
	return result
}

// GetStats returns current cache statistics.
func (m *Manager) GetStats(ctx context.Context) (*Stats, error) {
	return m.metadata.GetStats(ctx)
}
