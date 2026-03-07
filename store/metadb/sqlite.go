package metadb

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

//go:embed migrations/001_initial.sql
var initialSchema string

const currentSchemaVersion = 1

// SQLiteDB implements MetaDB using SQLite via modernc.org/sqlite (pure Go, no CGO).
type SQLiteDB struct {
	db     *sql.DB
	codec  *EnvelopeCodec
	logger *slog.Logger
	now    func() time.Time
}

// SQLiteDBOption configures a SQLiteDB instance.
type SQLiteDBOption func(*SQLiteDB)

// WithSQLiteLogger sets the logger for the database.
func WithSQLiteLogger(logger *slog.Logger) SQLiteDBOption {
	return func(s *SQLiteDB) {
		s.logger = logger
	}
}

// WithSQLiteNow sets the time function (for testing).
func WithSQLiteNow(now func() time.Time) SQLiteDBOption {
	return func(s *SQLiteDB) {
		s.now = now
	}
}

// NewSQLiteDB creates a new SQLiteDB instance with options.
func NewSQLiteDB(opts ...SQLiteDBOption) *SQLiteDB {
	s := &SQLiteDB{
		logger: slog.Default(),
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Open opens the SQLite database at the given path, applying schema migrations.
func (s *SQLiteDB) Open(path string) error {
	dsn := path + "?_journal_mode=WAL&_synchronous=NORMAL&_foreign_keys=ON&_busy_timeout=5000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("opening sqlite database: %w", err)
	}
	// WAL allows concurrent readers during a write. SQLite still serialises writers
	// at the engine level — extra conns beyond 1 only benefit readers.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return fmt.Errorf("pinging sqlite database: %w", err)
	}

	s.db = db

	if err := s.migrate(); err != nil {
		_ = db.Close()
		s.db = nil
		return fmt.Errorf("migrating sqlite schema: %w", err)
	}

	codec, err := NewEnvelopeCodec()
	if err != nil {
		_ = db.Close()
		s.db = nil
		return fmt.Errorf("creating envelope codec: %w", err)
	}
	s.codec = codec

	s.logger.Debug("sqlite metadb opened", "path", path)
	return nil
}

// migrate checks PRAGMA user_version and applies the initial schema if needed.
// Fails fast if the database has a newer schema than this binary supports.
func (s *SQLiteDB) migrate() error {
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("reading schema version: %w", err)
	}

	if version > currentSchemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported %d — use a newer binary",
			version, currentSchemaVersion)
	}

	if version < currentSchemaVersion {
		if _, err := s.db.Exec(initialSchema); err != nil {
			return fmt.Errorf("applying initial schema: %w", err)
		}
		// PRAGMA does not support ? placeholders — Sprintf is intentional.
		if _, err := s.db.Exec(fmt.Sprintf("PRAGMA user_version = %d", currentSchemaVersion)); err != nil {
			return fmt.Errorf("setting schema version: %w", err)
		}
		s.logger.Info("sqlite schema applied", "version", currentSchemaVersion)
	}

	return nil
}

// Close closes the database and releases resources.
func (s *SQLiteDB) Close() error {
	if s.codec != nil {
		s.codec.Close()
		s.codec = nil
	}
	if s.db == nil {
		return nil
	}
	s.logger.Debug("closing sqlite metadb")
	return s.db.Close()
}

// Codec returns the shared envelope codec for use with EnvelopeIndex instances.
func (s *SQLiteDB) Codec() *EnvelopeCodec {
	return s.codec
}

// DB returns the underlying *sql.DB for use by SQLiteQueues.
func (s *SQLiteDB) DB() *sql.DB {
	return s.db
}

// =============================================================================
// Protocol metadata
// =============================================================================

// GetMeta retrieves protocol metadata by protocol and key.
func (s *SQLiteDB) GetMeta(ctx context.Context, protocol, key string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT data FROM meta WHERE protocol = ? AND key = ?`,
		protocol, key).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return data, err
}

// PutMeta stores protocol metadata with an optional TTL (0 = never expires).
// Does not update blob reference counts; use PutMetaWithRefs for ref-tracked writes.
func (s *SQLiteDB) PutMeta(ctx context.Context, protocol, key string, data []byte, ttl time.Duration) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO meta (protocol, key, data, expires_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (protocol, key) DO UPDATE SET
			data       = excluded.data,
			expires_at = excluded.expires_at`,
		protocol, key, data, ttlToExpiresAtMs(s.now, ttl))
	return err
}

// DeleteMeta removes protocol metadata without decrementing blob refs.
// Use DeleteMetaWithRefs when the meta entry tracks blob references.
func (s *SQLiteDB) DeleteMeta(ctx context.Context, protocol, key string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM meta WHERE protocol = ? AND key = ?`, protocol, key)
	return err
}

// DeleteMetaWithRefs removes metadata and decrements all associated blob ref counts.
// The deletion of meta_blob_refs rows is handled by ON DELETE CASCADE;
// ref_count decrements are applied explicitly before the parent row is removed.
func (s *SQLiteDB) DeleteMetaWithRefs(ctx context.Context, protocol, key string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	hashes, err := sqlQueryStrings(ctx, tx,
		`SELECT hash FROM meta_blob_refs WHERE protocol = ? AND key = ?`, protocol, key)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM meta WHERE protocol = ? AND key = ?`, protocol, key); err != nil {
		return err
	}

	for _, hash := range hashes {
		if _, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ListMeta returns all keys stored for a given protocol.
func (s *SQLiteDB) ListMeta(ctx context.Context, protocol string) ([]string, error) {
	return sqlQueryStrings(ctx, s.db,
		`SELECT key FROM meta WHERE protocol = ?`, protocol)
}

// PutMetaWithRefs stores metadata and atomically updates blob reference counts.
// It computes the diff between old and new refs, incrementing added and
// decrementing removed refs within a single transaction.
func (s *SQLiteDB) PutMetaWithRefs(ctx context.Context, protocol, key string, data []byte, ttl time.Duration, refs []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO meta (protocol, key, data, expires_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (protocol, key) DO UPDATE SET
			data       = excluded.data,
			expires_at = excluded.expires_at`,
		protocol, key, data, ttlToExpiresAtMs(s.now, ttl)); err != nil {
		return err
	}

	oldRefs, err := sqlQueryStrings(ctx, tx,
		`SELECT hash FROM meta_blob_refs WHERE protocol = ? AND key = ?`, protocol, key)
	if err != nil {
		return err
	}

	added, removed := DiffRefs(oldRefs, refs)

	for _, hash := range added {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO meta_blob_refs (protocol, key, hash) VALUES (?, ?, ?)`,
			protocol, key, hash); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = ref_count + 1 WHERE hash = ?`, hash); err != nil {
			return err
		}
	}

	for _, hash := range removed {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM meta_blob_refs WHERE protocol = ? AND key = ? AND hash = ?`,
			protocol, key, hash); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// UpdateJSON performs read-modify-write for JSON metadata in a single transaction.
// fn receives the current value (or zero value if not found) and modifies it in place.
func (s *SQLiteDB) UpdateJSON(ctx context.Context, protocol, key string, ttl time.Duration, fn func(v any) error, v any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	var existing []byte
	err = tx.QueryRowContext(ctx,
		`SELECT data FROM meta WHERE protocol = ? AND key = ?`, protocol, key).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if existing != nil {
		if err := json.Unmarshal(existing, v); err != nil {
			return fmt.Errorf("unmarshaling existing value: %w", err)
		}
	}

	if err := fn(v); err != nil {
		return err
	}

	newData, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling updated value: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO meta (protocol, key, data, expires_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (protocol, key) DO UPDATE SET
			data       = excluded.data,
			expires_at = excluded.expires_at`,
		protocol, key, newData, ttlToExpiresAtMs(s.now, ttl)); err != nil {
		return err
	}

	return tx.Commit()
}

// =============================================================================
// Blob tracking
// =============================================================================

// GetBlob retrieves blob metadata by hash.
func (s *SQLiteDB) GetBlob(ctx context.Context, hash string) (*BlobEntry, error) {
	var (
		entry        BlobEntry
		cachedAtMs   int64
		lastAccessMs int64
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT hash, size, cached_at, last_access, ref_count, access_count
		FROM blobs WHERE hash = ?`, hash).
		Scan(&entry.Hash, &entry.Size, &cachedAtMs, &lastAccessMs,
			&entry.RefCount, &entry.AccessCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	entry.CachedAt = time.UnixMilli(cachedAtMs).UTC()
	entry.LastAccess = time.UnixMilli(lastAccessMs).UTC()
	return &entry, nil
}

// PutBlob stores or replaces blob metadata.
func (s *SQLiteDB) PutBlob(ctx context.Context, entry *BlobEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO blobs (hash, size, cached_at, last_access, ref_count, access_count)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (hash) DO UPDATE SET
			size         = excluded.size,
			cached_at    = excluded.cached_at,
			last_access  = excluded.last_access,
			ref_count    = excluded.ref_count,
			access_count = excluded.access_count`,
		entry.Hash, entry.Size,
		entry.CachedAt.UnixMilli(), entry.LastAccess.UnixMilli(),
		entry.RefCount, entry.AccessCount)
	return err
}

// DeleteBlob removes blob metadata by hash.
func (s *SQLiteDB) DeleteBlob(ctx context.Context, hash string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM blobs WHERE hash = ?`, hash)
	return err
}

// IncrementBlobRef increments the reference count for a blob.
// Returns ErrNotFound if the blob does not exist.
func (s *SQLiteDB) IncrementBlobRef(ctx context.Context, hash string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE blobs SET ref_count = ref_count + 1 WHERE hash = ?`, hash)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// DecrementBlobRef decrements the reference count for a blob (floor 0).
// Returns ErrNotFound if the blob does not exist.
func (s *SQLiteDB) DecrementBlobRef(ctx context.Context, hash string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// TouchBlob updates the last access time and increments the access counter (capped at 3).
// Returns the new access count, or 0 if the blob was not found.
func (s *SQLiteDB) TouchBlob(ctx context.Context, hash string) (int, error) {
	var newCount int
	err := s.db.QueryRowContext(ctx, `
		UPDATE blobs
		SET last_access  = ?,
		    access_count = MIN(access_count + 1, 3)
		WHERE hash = ?
		RETURNING access_count`,
		s.now().UnixMilli(), hash).Scan(&newCount)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return newCount, err
}

// TotalBlobSize returns the sum of all blob sizes.
func (s *SQLiteDB) TotalBlobSize(ctx context.Context) (int64, error) {
	var total int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(size), 0) FROM blobs`).Scan(&total)
	return total, err
}

// =============================================================================
// Eviction queries
// =============================================================================

// GetExpiredMeta returns metadata entries whose expires_at is before the cutoff.
func (s *SQLiteDB) GetExpiredMeta(ctx context.Context, before time.Time, limit int) ([]ExpiryEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT protocol, key, expires_at, LENGTH(data) AS size
		FROM meta
		WHERE expires_at IS NOT NULL AND expires_at < ?
		ORDER BY expires_at
		LIMIT ?`,
		before.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ExpiryEntry
	for rows.Next() {
		var (
			e           ExpiryEntry
			expiresAtMs int64
		)
		if err := rows.Scan(&e.Protocol, &e.Key, &expiresAtMs, &e.Size); err != nil {
			return nil, err
		}
		e.ExpiresAt = time.UnixMilli(expiresAtMs).UTC()
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetUnreferencedBlobs returns hashes of blobs with ref_count == 0 that are
// older than the retention floor. When before is zero, all unreferenced blobs
// are returned regardless of age.
func (s *SQLiteDB) GetUnreferencedBlobs(ctx context.Context, before time.Time, limit int) ([]string, error) {
	if before.IsZero() {
		return sqlQueryStrings(ctx, s.db,
			`SELECT hash FROM blobs WHERE ref_count = 0 LIMIT ?`, limit)
	}
	beforeMs := before.UnixMilli()
	return sqlQueryStrings(ctx, s.db, `
		SELECT hash FROM blobs
		WHERE ref_count = 0
		  AND cached_at < ?
		  AND last_access < ?
		LIMIT ?`,
		beforeMs, beforeMs, limit)
}

// =============================================================================
// Envelope APIs
// =============================================================================

// PutEnvelope stores a metadata envelope with blob reference tracking.
// Computes the diff between old and new refs within a single transaction.
func (s *SQLiteDB) PutEnvelope(ctx context.Context, protocol, kind, key string, env *MetadataEnvelope) error {
	if err := ValidateEnvelope(env); err != nil {
		return fmt.Errorf("validating envelope: %w", err)
	}
	env.BlobRefs = CanonicalizeRefs(env.BlobRefs)

	data, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshaling envelope: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO envelopes (protocol, kind, key, data, expires_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (protocol, kind, key) DO UPDATE SET
			data       = excluded.data,
			expires_at = excluded.expires_at`,
		protocol, kind, key, data, int64OrNil(env.ExpiresAtUnixMs)); err != nil {
		return err
	}

	oldRefs, err := sqlQueryStrings(ctx, tx,
		`SELECT hash FROM envelope_blob_refs WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key)
	if err != nil {
		return err
	}

	if err := applyEnvelopeRefDiff(ctx, tx, protocol, kind, key, oldRefs, env.BlobRefs); err != nil {
		return err
	}

	return tx.Commit()
}

// GetEnvelope retrieves a metadata envelope by protocol, kind, and key.
// Returns ErrNotFound if not present.
func (s *SQLiteDB) GetEnvelope(ctx context.Context, protocol, kind, key string) (*MetadataEnvelope, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT data FROM envelopes WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var env MetadataEnvelope
	if err := proto.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshaling envelope: %w", err)
	}
	return &env, nil
}

// DeleteEnvelope removes a metadata envelope and decrements all associated blob refs.
func (s *SQLiteDB) DeleteEnvelope(ctx context.Context, protocol, kind, key string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	hashes, err := sqlQueryStrings(ctx, tx,
		`SELECT hash FROM envelope_blob_refs WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM envelopes WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key); err != nil {
		return err
	}

	for _, hash := range hashes {
		if _, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ListEnvelopeKeys returns all keys stored for a given protocol and kind.
func (s *SQLiteDB) ListEnvelopeKeys(ctx context.Context, protocol, kind string) ([]string, error) {
	return sqlQueryStrings(ctx, s.db,
		`SELECT key FROM envelopes WHERE protocol = ? AND kind = ?`, protocol, kind)
}

// GetEnvelopeBlobRefs returns the blob hashes referenced by an envelope key.
func (s *SQLiteDB) GetEnvelopeBlobRefs(ctx context.Context, protocol, kind, key string) ([]string, error) {
	return sqlQueryStrings(ctx, s.db,
		`SELECT hash FROM envelope_blob_refs WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key)
}

// UpdateEnvelope performs read-modify-write in a single transaction.
// fn receives the current envelope (nil if not found). Returning nil deletes the entry.
func (s *SQLiteDB) UpdateEnvelope(ctx context.Context, protocol, kind, key string, fn func(*MetadataEnvelope) (*MetadataEnvelope, error)) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	var rawData []byte
	err = tx.QueryRowContext(ctx,
		`SELECT data FROM envelopes WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key).Scan(&rawData)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	var existing *MetadataEnvelope
	if rawData != nil {
		existing = &MetadataEnvelope{}
		if err := proto.Unmarshal(rawData, existing); err != nil {
			return fmt.Errorf("unmarshaling existing envelope: %w", err)
		}
	}

	oldRefs, err := sqlQueryStrings(ctx, tx,
		`SELECT hash FROM envelope_blob_refs WHERE protocol = ? AND kind = ? AND key = ?`,
		protocol, kind, key)
	if err != nil {
		return err
	}

	newEnv, err := fn(existing)
	if err != nil {
		return err
	}

	if newEnv == nil {
		// Callback signalled delete.
		for _, hash := range oldRefs {
			if _, err := tx.ExecContext(ctx,
				`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM envelopes WHERE protocol = ? AND kind = ? AND key = ?`,
			protocol, kind, key); err != nil {
			return err
		}
		return tx.Commit()
	}

	if err := ValidateEnvelope(newEnv); err != nil {
		return fmt.Errorf("validating envelope: %w", err)
	}
	newEnv.BlobRefs = CanonicalizeRefs(newEnv.BlobRefs)

	if err := applyEnvelopeRefDiff(ctx, tx, protocol, kind, key, oldRefs, newEnv.BlobRefs); err != nil {
		return err
	}

	newData, err := proto.Marshal(newEnv)
	if err != nil {
		return fmt.Errorf("marshaling envelope: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO envelopes (protocol, kind, key, data, expires_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (protocol, kind, key) DO UPDATE SET
			data       = excluded.data,
			expires_at = excluded.expires_at`,
		protocol, kind, key, newData, int64OrNil(newEnv.ExpiresAtUnixMs)); err != nil {
		return err
	}

	return tx.Commit()
}

// GetExpiredEnvelopes returns envelope entries whose expires_at is before the cutoff.
func (s *SQLiteDB) GetExpiredEnvelopes(ctx context.Context, before time.Time, limit int) ([]EnvelopeExpiryEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT protocol, kind, key, expires_at
		FROM envelopes
		WHERE expires_at IS NOT NULL AND expires_at < ?
		ORDER BY expires_at
		LIMIT ?`,
		before.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []EnvelopeExpiryEntry
	for rows.Next() {
		var (
			e           EnvelopeExpiryEntry
			expiresAtMs int64
		)
		if err := rows.Scan(&e.Protocol, &e.Kind, &e.Key, &expiresAtMs); err != nil {
			return nil, err
		}
		e.ExpiresAt = time.UnixMilli(expiresAtMs).UTC()
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// DeleteExpiredEnvelopes batch-deletes expired envelopes in a single transaction,
// decrementing blob ref counts for each deleted envelope.
func (s *SQLiteDB) DeleteExpiredEnvelopes(ctx context.Context, entries []EnvelopeExpiryEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for _, entry := range entries {
		hashes, err := sqlQueryStrings(ctx, tx,
			`SELECT hash FROM envelope_blob_refs WHERE protocol = ? AND kind = ? AND key = ?`,
			entry.Protocol, entry.Kind, entry.Key)
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM envelopes WHERE protocol = ? AND kind = ? AND key = ?`,
			entry.Protocol, entry.Kind, entry.Key); err != nil {
			return err
		}

		for _, hash := range hashes {
			if _, err := tx.ExecContext(ctx,
				`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// =============================================================================
// Helpers
// =============================================================================

// sqlQuerier is satisfied by both *sql.DB and *sql.Tx, allowing a single
// helper to run read queries in either a transaction or the pool.
type sqlQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// sqlQueryStrings runs a query and collects all values from the first string column.
func sqlQueryStrings(ctx context.Context, q sqlQuerier, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// ttlToExpiresAtMs converts a TTL duration into a *int64 unix-millisecond timestamp,
// or nil when TTL is zero (meaning no expiry).
func ttlToExpiresAtMs(now func() time.Time, ttl time.Duration) *int64 {
	if ttl <= 0 {
		return nil
	}
	ms := now().Add(ttl).UnixMilli()
	return &ms
}

// int64OrNil returns nil when v == 0, otherwise &v.
// Used to convert proto int64 expiry (0 = never) to SQL NULL.
func int64OrNil(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

// applyEnvelopeRefDiff updates envelope_blob_refs and blob ref counts within tx.
func applyEnvelopeRefDiff(ctx context.Context, tx *sql.Tx, protocol, kind, key string, oldRefs, newRefs []string) error {
	added, removed := DiffRefs(oldRefs, newRefs)

	for _, hash := range added {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO envelope_blob_refs (protocol, kind, key, hash) VALUES (?, ?, ?, ?)`,
			protocol, kind, key, hash); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = ref_count + 1 WHERE hash = ?`, hash); err != nil {
			return err
		}
	}

	for _, hash := range removed {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM envelope_blob_refs WHERE protocol = ? AND kind = ? AND key = ? AND hash = ?`,
			protocol, kind, key, hash); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?`, hash); err != nil {
			return err
		}
	}

	return nil
}

// Compile-time interface checks.
var _ MetaDB = (*SQLiteDB)(nil)

// Verify SQLiteDB satisfies the optional backend interfaces used by Index for
// atomic ref-tracked writes and read-modify-write. If these drift from the
// concrete methods, the Index falls back to non-atomic paths silently —
// catching that at compile time is cheaper than debugging lost updates.
var _ metaRefsWriter = (*SQLiteDB)(nil)
var _ metaUpdateWriter = (*SQLiteDB)(nil)
