-- Projection dead-letter queue.
--
-- Failed projection events land here after exhausting bounded retries.
-- Each row captures the original event payload and metadata so operators
-- can inspect, replay, or reject the event without touching the event
-- store. The checkpoint-and-DLQ-write happens in a single Postgres
-- transaction (see internal/buildingblocks/infrastructure/eventstoredb/
-- projection/worker.go:flushCheckpointWithDLQ), so a crash between them
-- cannot lose the failed event nor advance the projection past it.
--
-- A row's lifecycle:
--   1. INSERT on exhaustion of retries (attempts = N).
--   2. Operator inspects via cmd/admin dlq-list.
--   3. Operator either:
--      a. cmd/admin dlq-replay — re-process; on success, DELETE the row;
--         the projection checkpoint is NOT rewound (replay writes the
--         sink directly; subsequent live events see the corrected state).
--      b. cmd/admin dlq-reject — DELETE the row, recording an audited
--         decision in es_projection_dlq_rejections (future work).
--
-- The (projection_name, event_id) pair is unique: redelivery of the same
-- failed event from another worker instance cannot create two DLQ rows.
-- event_id is the KurrentDB EventID (UUID), which is globally unique.
CREATE TABLE IF NOT EXISTS es_projection_dlq (
    id              BIGSERIAL    PRIMARY KEY,
    projection_name TEXT         NOT NULL,
    event_id        TEXT         NOT NULL,
    stream_id       TEXT         NOT NULL,
    stream_revision BIGINT       NOT NULL,
    event_type      TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    metadata        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    commit_position BIGINT       NOT NULL,
    error           TEXT         NOT NULL,
    attempts        INTEGER      NOT NULL,
    first_failed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_failed_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (projection_name, event_id)
);

-- Operator-facing indexes: list-by-projection, list-by-age.
CREATE INDEX IF NOT EXISTS idx_es_projection_dlq_projection
    ON es_projection_dlq (projection_name, last_failed_at);
CREATE INDEX IF NOT EXISTS idx_es_projection_dlq_age
    ON es_projection_dlq (last_failed_at);
