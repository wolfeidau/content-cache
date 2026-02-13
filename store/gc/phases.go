package gc

import (
	"context"
	"errors"
	"fmt"
	"time"

	contentcache "github.com/wolfeidau/content-cache"
	"github.com/wolfeidau/content-cache/backend"
	"github.com/wolfeidau/content-cache/store/metadb"
)

// phaseExpireMeta deletes expired metadata entries.
func (m *Manager) phaseExpireMeta(ctx context.Context, result *Result) {
	m.logger.Debug("phase: expire metadata")

	expired, err := m.db.GetExpiredMeta(ctx, time.Now(), m.config.BatchSize)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("get expired meta: %v", err))
		m.logger.Error("failed to get expired metadata", "error", err)
		return
	}

	for _, entry := range expired {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := m.db.DeleteMeta(ctx, entry.Protocol, entry.Key); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("delete meta %s/%s: %v", entry.Protocol, entry.Key, err))
			m.logger.Error("failed to delete expired metadata",
				"protocol", entry.Protocol,
				"key", entry.Key,
				"error", err,
			)
			continue
		}

		result.ExpiredMetaDeleted++
		result.BytesReclaimed += entry.Size

		m.logger.Debug("deleted expired metadata",
			"protocol", entry.Protocol,
			"key", entry.Key,
			"expired_at", entry.ExpiresAt,
		)
	}
}

// phaseDeleteUnreferenced deletes blobs with RefCount == 0.
func (m *Manager) phaseDeleteUnreferenced(ctx context.Context, result *Result) {
	m.logger.Debug("phase: delete unreferenced blobs")

	hashes, err := m.db.GetUnreferencedBlobs(ctx, m.config.BatchSize)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("get unreferenced blobs: %v", err))
		m.logger.Error("failed to get unreferenced blobs", "error", err)
		return
	}

	for _, hash := range hashes {
		select {
		case <-ctx.Done():
			return
		default:
		}

		bytesReclaimed, err := m.deleteBlob(ctx, hash)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("delete unreferenced blob %s: %v", hash, err))
			m.logger.Error("failed to delete unreferenced blob", "hash", hash, "error", err)
			continue
		}

		result.UnreferencedBlobsDeleted++
		result.BytesReclaimed += bytesReclaimed

		m.logger.Debug("deleted unreferenced blob", "hash", hash, "size", bytesReclaimed)
	}
}

// phaseDeleteOrphans deletes blobs on disk but not in the index.
func (m *Manager) phaseDeleteOrphans(ctx context.Context, result *Result) {
	m.logger.Debug("phase: delete orphan blobs")

	keys, err := m.backend.List(ctx, "blobs/")
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("list backend blobs: %v", err))
		m.logger.Error("failed to list backend blobs", "error", err)
		return
	}

	processed := 0
	for _, key := range keys {
		if processed >= m.config.BatchSize {
			break
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		h, err := contentcache.ParseBlobStorageKey(key)
		if err != nil {
			continue
		}
		hash := h.String()

		_, err = m.db.GetBlob(ctx, hash)
		if err == nil {
			continue
		}

		if err != metadb.ErrNotFound {
			result.Errors = append(result.Errors, fmt.Sprintf("check blob %s: %v", hash, err))
			continue
		}

		var size int64
		if sizeBackend, ok := m.backend.(interface {
			Size(context.Context, string) (int64, error)
		}); ok {
			size, _ = sizeBackend.Size(ctx, key)
		}

		if err := m.backend.Delete(ctx, key); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("delete orphan blob %s: %v", key, err))
			m.logger.Error("failed to delete orphan blob", "key", key, "error", err)
			continue
		}

		result.OrphanBlobsDeleted++
		result.BytesReclaimed += size
		processed++

		m.logger.Debug("deleted orphan blob", "key", key, "size", size)
	}
}

// phaseLRUEviction evicts LRU blobs if over quota.
func (m *Manager) phaseLRUEviction(ctx context.Context, result *Result) {
	if m.config.MaxCacheBytes <= 0 {
		return
	}

	m.logger.Debug("phase: LRU eviction")

	totalSize, err := m.db.TotalBlobSize(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("get total blob size: %v", err))
		m.logger.Error("failed to get total blob size", "error", err)
		return
	}

	if totalSize <= m.config.MaxCacheBytes {
		m.logger.Debug("cache within quota", "total_size", totalSize, "max_size", m.config.MaxCacheBytes)
		return
	}

	bytesToFree := totalSize - m.config.MaxCacheBytes
	m.logger.Info("cache over quota, starting LRU eviction",
		"total_size", totalSize,
		"max_size", m.config.MaxCacheBytes,
		"bytes_to_free", bytesToFree,
	)

	var bytesFreed int64
	evicted := 0

	for bytesFreed < bytesToFree && evicted < m.config.BatchSize {
		select {
		case <-ctx.Done():
			return
		default:
		}

		blobs, err := m.db.GetLRUBlobs(ctx, 100)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("get LRU blobs: %v", err))
			m.logger.Error("failed to get LRU blobs", "error", err)
			break
		}

		if len(blobs) == 0 {
			break
		}

		for _, blob := range blobs {
			if bytesFreed >= bytesToFree {
				break
			}

			if blob.RefCount > 0 {
				continue
			}

			bytesReclaimed, err := m.deleteBlob(ctx, blob.Hash)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("delete LRU blob %s: %v", blob.Hash, err))
				m.logger.Error("failed to delete LRU blob", "hash", blob.Hash, "error", err)
				continue
			}

			bytesFreed += bytesReclaimed
			result.LRUBlobsEvicted++
			result.BytesReclaimed += bytesReclaimed
			evicted++

			m.logger.Debug("evicted LRU blob",
				"hash", blob.Hash,
				"size", bytesReclaimed,
				"last_access", blob.LastAccess,
			)
		}
	}
}

// deleteBlob deletes a blob from both the backend and metadata store.
func (m *Manager) deleteBlob(ctx context.Context, hash string) (int64, error) {
	entry, err := m.db.GetBlob(ctx, hash)
	if err != nil && err != metadb.ErrNotFound {
		return 0, fmt.Errorf("get blob metadata: %w", err)
	}

	var size int64
	if entry != nil {
		size = entry.Size
	}

	h, err := contentcache.ParseHash(hash)
	if err != nil {
		return 0, fmt.Errorf("parse hash: %w", err)
	}

	key := contentcache.BlobStorageKey(h)
	if err := m.backend.Delete(ctx, key); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return 0, fmt.Errorf("delete from backend: %w", err)
	}

	if err := m.db.DeleteBlob(ctx, hash); err != nil && err != metadb.ErrNotFound {
		return 0, fmt.Errorf("delete from metadb: %w", err)
	}

	return size, nil
}
