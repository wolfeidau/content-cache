-- Blob tracking
CREATE TABLE IF NOT EXISTS blobs (
    hash         TEXT    NOT NULL PRIMARY KEY,
    size         INTEGER NOT NULL,
    cached_at    INTEGER NOT NULL,  -- unix ms
    last_access  INTEGER NOT NULL,  -- unix ms
    ref_count    INTEGER NOT NULL DEFAULT 0,
    access_count INTEGER NOT NULL DEFAULT 0
) STRICT;

-- Speeds up GetUnreferencedBlobs (currently a full ForEach scan in BoltDB)
CREATE INDEX IF NOT EXISTS blobs_gc_idx ON blobs (last_access)
    WHERE ref_count = 0;

-- Protocol metadata (legacy 2-part key)
CREATE TABLE IF NOT EXISTS meta (
    protocol   TEXT NOT NULL,
    key        TEXT NOT NULL,
    data       BLOB NOT NULL,
    expires_at INTEGER,            -- unix ms, NULL = never expires
    PRIMARY KEY (protocol, key)
) STRICT;

-- Replaces bucketMetaByExpiry + bucketMetaExpiryByKey (4 buckets → 1 index)
CREATE INDEX IF NOT EXISTS meta_expiry_idx ON meta (expires_at)
    WHERE expires_at IS NOT NULL;

-- Replaces bucketMetaBlobRefs JSON array
CREATE TABLE IF NOT EXISTS meta_blob_refs (
    protocol TEXT NOT NULL,
    key      TEXT NOT NULL,
    hash     TEXT NOT NULL REFERENCES blobs (hash),
    PRIMARY KEY (protocol, key, hash),
    FOREIGN KEY (protocol, key) REFERENCES meta (protocol, key) ON DELETE CASCADE
) STRICT;

-- For "what refs this blob" queries (debugging, GC integrity checks)
CREATE INDEX IF NOT EXISTS meta_blob_refs_by_hash ON meta_blob_refs (hash);

-- Envelope metadata (3-part key)
CREATE TABLE IF NOT EXISTS envelopes (
    protocol   TEXT NOT NULL,
    kind       TEXT NOT NULL,
    key        TEXT NOT NULL,
    data       BLOB NOT NULL,      -- protobuf MetadataEnvelope bytes
    expires_at INTEGER,            -- unix ms, NULL = never expires
    PRIMARY KEY (protocol, kind, key)
) STRICT;

-- Replaces bucketEnvelopeByExpiry + bucketEnvelopeExpiryByKey
CREATE INDEX IF NOT EXISTS envelope_expiry_idx ON envelopes (expires_at)
    WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS envelope_blob_refs (
    protocol TEXT NOT NULL,
    kind     TEXT NOT NULL,
    key      TEXT NOT NULL,
    hash     TEXT NOT NULL REFERENCES blobs (hash),
    PRIMARY KEY (protocol, kind, key, hash),
    FOREIGN KEY (protocol, kind, key) REFERENCES envelopes (protocol, kind, key) ON DELETE CASCADE
) STRICT;

-- For "what refs this blob" queries (debugging, GC integrity checks)
CREATE INDEX IF NOT EXISTS envelope_blob_refs_by_hash ON envelope_blob_refs (hash);

-- S3-FIFO queues (AUTOINCREMENT gives monotonic FIFO ordering)
CREATE TABLE IF NOT EXISTS s3fifo_queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT    NOT NULL,
    hash       TEXT    NOT NULL,
    UNIQUE (queue_name, hash)
) STRICT;

CREATE INDEX IF NOT EXISTS s3fifo_queue_order_idx ON s3fifo_queue (queue_name, id);

-- Ghost cache is intentionally global (shared across all queues).
CREATE TABLE IF NOT EXISTS s3fifo_ghost (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT    NOT NULL UNIQUE
) STRICT;
