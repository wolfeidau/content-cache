package s3fifo

import (
	"context"
	"database/sql"
	"errors"
)

// SQLiteQueues implements the Queues interface backed by SQLite.
// It uses the s3fifo_queue and s3fifo_ghost tables created by the metadb migration.
// The *sql.DB is typically shared with the SQLiteDB metadata store.
//
// All methods use context.Background() because the Queues interface does not
// accept contexts; this is consistent with BoltQueues.
type SQLiteQueues struct {
	db *sql.DB
}

// NewSQLiteQueues creates a SQLiteQueues instance backed by the given database.
// The database must already have the s3fifo_queue and s3fifo_ghost tables
// (created by the metadb initial schema migration).
func NewSQLiteQueues(db *sql.DB) *SQLiteQueues {
	return &SQLiteQueues{db: db}
}

// validateQueueName panics on unknown queue names, consistent with BoltQueues.
// Queue names are compile-time constants; an unknown name is a programming error.
func validateQueueName(queue string) {
	switch queue {
	case QueueSmall, QueueMain:
		// valid
	default:
		panic("s3fifo: unknown queue name: " + queue)
	}
}

// PushHead inserts hash at the head (newest position) of the named queue.
// If the hash is already present in that queue, it is not re-inserted.
func (q *SQLiteQueues) PushHead(queue, hash string) error {
	validateQueueName(queue)
	_, err := q.db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO s3fifo_queue (queue_name, hash) VALUES (?, ?)`, queue, hash)
	return err
}

// PopTail removes and returns the oldest hash from the named queue.
// Returns ErrQueueEmpty when the queue has no entries.
func (q *SQLiteQueues) PopTail(queue string) (string, error) {
	validateQueueName(queue)
	var hash string
	err := q.db.QueryRowContext(context.Background(), `
		DELETE FROM s3fifo_queue
		WHERE id = (SELECT MIN(id) FROM s3fifo_queue WHERE queue_name = ?)
		RETURNING hash`, queue).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrQueueEmpty
	}
	return hash, err
}

// Remove removes a specific hash from the named queue.
// Returns (true, nil) if the hash was present and removed, (false, nil) if absent.
func (q *SQLiteQueues) Remove(queue, hash string) (bool, error) {
	validateQueueName(queue)
	res, err := q.db.ExecContext(context.Background(),
		`DELETE FROM s3fifo_queue WHERE queue_name = ? AND hash = ?`, queue, hash)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// Len returns the number of entries in the named queue.
func (q *SQLiteQueues) Len(queue string) (int, error) {
	validateQueueName(queue)
	var count int
	err := q.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM s3fifo_queue WHERE queue_name = ?`, queue).Scan(&count)
	return count, err
}

// ForEach iterates all entries in FIFO order (oldest first).
// fn must not perform writes that could cause database contention.
func (q *SQLiteQueues) ForEach(queue string, fn func(hash string) error) error {
	validateQueueName(queue)
	rows, err := q.db.QueryContext(context.Background(),
		`SELECT hash FROM s3fifo_queue WHERE queue_name = ? ORDER BY id ASC`, queue)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return err
		}
		if err := fn(hash); err != nil {
			return err
		}
	}
	return rows.Err()
}

// AdmitGhostHit atomically removes hash from the ghost set and inserts it at
// the head of the main queue. No-op if the hash is not in the ghost set.
func (q *SQLiteQueues) AdmitGhostHit(hash string) error {
	tx, err := q.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(context.Background(),
		`DELETE FROM s3fifo_ghost WHERE hash = ?`, hash)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// Not in ghost — possible race on restart; fall through to normal small admission.
		return tx.Commit()
	}

	if _, err := tx.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO s3fifo_queue (queue_name, hash) VALUES (?, ?)`,
		QueueMain, hash); err != nil {
		return err
	}

	return tx.Commit()
}

// GhostContains reports whether hash is currently in the ghost set.
func (q *SQLiteQueues) GhostContains(hash string) (bool, error) {
	var count int
	err := q.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM s3fifo_ghost WHERE hash = ?`, hash).Scan(&count)
	return count > 0, err
}

// GhostAdd inserts hash into the ghost set. No-op if already present.
func (q *SQLiteQueues) GhostAdd(hash string) error {
	_, err := q.db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO s3fifo_ghost (hash) VALUES (?)`, hash)
	return err
}

// GhostRemove removes hash from the ghost set. No-op if not present.
func (q *SQLiteQueues) GhostRemove(hash string) error {
	_, err := q.db.ExecContext(context.Background(),
		`DELETE FROM s3fifo_ghost WHERE hash = ?`, hash)
	return err
}

// GhostTrimToMaxSize evicts the oldest ghost entries until the count is at or
// below maxEntries. Uses AUTOINCREMENT ordering: lower id = older entry.
//
// The subquery selects the (maxEntries+1)-th newest row (OFFSET = maxEntries).
// Deleting WHERE id <= that row keeps exactly maxEntries rows. When the table
// has fewer than maxEntries rows the subquery returns NULL, making the WHERE
// clause false — no rows deleted, which is correct.
func (q *SQLiteQueues) GhostTrimToMaxSize(maxEntries int) error {
	if maxEntries <= 0 {
		_, err := q.db.ExecContext(context.Background(), `DELETE FROM s3fifo_ghost`)
		return err
	}
	// OFFSET maxEntries selects the (maxEntries+1)-th newest row and deletes
	// everything at or below it, keeping exactly maxEntries rows. When the table
	// has fewer than maxEntries rows, the subquery returns NULL and the WHERE
	// clause is false — no rows deleted, which is correct.
	_, err := q.db.ExecContext(context.Background(), `
		DELETE FROM s3fifo_ghost
		WHERE id <= (
			SELECT id FROM s3fifo_ghost ORDER BY id DESC LIMIT 1 OFFSET ?
		)`, maxEntries)
	return err
}

// GhostLen returns the current number of entries in the ghost set.
func (q *SQLiteQueues) GhostLen() (int, error) {
	var count int
	err := q.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM s3fifo_ghost`).Scan(&count)
	return count, err
}

// Compile-time interface check.
var _ Queues = (*SQLiteQueues)(nil)
