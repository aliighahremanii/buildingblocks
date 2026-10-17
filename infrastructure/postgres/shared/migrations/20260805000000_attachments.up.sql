-- Phase 04 — Attachment blob + ref tables (master plan §5.3).
-- Blob bytes live in an external object store (RustFS / S3-compatible); this
-- table records blob METADATA (sha256 PK, size, mime, object key, bucket) and
-- per-upload references. Identical bytes under different filenames create ONE
-- blob row and SEPARATE attachment_refs rows (dedup by sha256).

CREATE TABLE IF NOT EXISTS attachment_blobs (
    sha256      TEXT PRIMARY KEY,
    size        BIGINT NOT NULL,
    mime        TEXT NOT NULL DEFAULT '',
    object_key  TEXT NOT NULL,
    bucket      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS attachment_refs (
    id           UUID PRIMARY KEY,
    blob_sha256  TEXT NOT NULL REFERENCES attachment_blobs(sha256) ON DELETE CASCADE,
    filename     TEXT NOT NULL,
    uploaded_by  TEXT NOT NULL DEFAULT '',
    uploaded_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attachment_refs_blob_sha256
    ON attachment_refs(blob_sha256);
