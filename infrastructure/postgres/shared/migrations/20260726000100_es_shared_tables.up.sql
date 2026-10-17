-- Shared event-sourcing support tables used by every event-sourced module.
--
-- es_snapshots: one row per (aggregate_type, aggregate_id) holding the most
-- recent serialized aggregate state and the KurrentDB stream revision it
-- corresponds to. Partitioned by aggregate_type so modules never collide.
--
-- es_projection_checkpoints: one row per projection name holding the last
-- KurrentDB $all commit position the projection has durably processed.
-- Projection names are module-namespaced (e.g. "geography-sqlserver").

CREATE TABLE IF NOT EXISTS es_snapshots (
    aggregate_type TEXT    NOT NULL,
    aggregate_id   TEXT    NOT NULL,
    version        BIGINT  NOT NULL,
    state          JSONB   NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE INDEX IF NOT EXISTS idx_es_snapshots_type
    ON es_snapshots (aggregate_type);

CREATE TABLE IF NOT EXISTS es_projection_checkpoints (
    projection_name TEXT       NOT NULL PRIMARY KEY,
    position        BIGINT     NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
