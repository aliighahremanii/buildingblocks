-- Command idempotency deduplication table.
--
-- Used by the optional Idempotency-Key HTTP header middleware to make
-- retried command submissions return a deterministic cached outcome rather
-- than re-executing the command (and potentially creating duplicate
-- effects). The deduplication record is written AFTER the underlying
-- command handler succeeds; a crash between command success and the
-- record-write leaves no cached response, so the retry re-executes and the
-- command's own optimistic-concurrency controls (NoStream on Register,
-- AtRevision on Update) handle the second execution safely.
--
-- The (idempotency_key, command_kind) pair is the natural key: a client
-- that reuses the same key for two different commands gets two records
-- rather than a false dedup hit. command_kind is the qualified handler
-- name, e.g. "sample1.item.register".
--
-- response_body is BYTEA, not JSONB. The idempotency wire contract requires
-- a retried request to return the EXACT response body bytes the first
-- attempt produced. JSONB canonicalizes whitespace on read (it adds a space
-- after ':' and ',' in its text output), which breaks byte-identity for any
-- byte-level mechanism (HMAC, exact-byte cache keys, strict replay
-- assertions). BYTEA stores and returns the cached bytes verbatim. See
-- decision D021 (regression of D016) and migration
-- 20260804000100_command_idempotency_response_body_bytea which converts
-- already-migrated databases.
CREATE TABLE IF NOT EXISTS command_idempotency (
    idempotency_key TEXT        NOT NULL,
    command_kind    TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    response_status INTEGER     NOT NULL,
    response_body   BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (idempotency_key, command_kind)
);

-- Index for the rare case where an operator wants to look up by
-- aggregate_id (e.g. "show me every command that touched this country").
CREATE INDEX IF NOT EXISTS idx_command_idempotency_aggregate_id
    ON command_idempotency (aggregate_id);
