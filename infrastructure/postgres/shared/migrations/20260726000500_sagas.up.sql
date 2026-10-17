-- Saga framework support tables.
--
-- sagas: one row per saga instance. The saga framework's Coordinator
-- (internal/buildingblocks/infrastructure/saga) drives long-running,
-- multi-step workflows by reacting to KurrentDB domain events. Each
-- instance is durably persisted here so the saga survives worker restarts
-- and remains idempotent under event redelivery.
--
--   saga_id   — stable id (e.g. "sample1.processpending-<item-uuid>").
--   saga_type — the registered saga type (SagaType()), used by the timeout
--               sweep to dispatch to the right Saga implementation.
--   status    — one of: pending, running, completed, failed.
--   state     — opaque JSONB blob the saga implementation owns
--               (e.g. {"itemID":"...","startedAt":"...","status":"..."}).
--   version   — optimistic-concurrency guard. Save fails unless the stored
--               version equals the loaded version (Store.Save enforces via
--               ON CONFLICT ... WHERE version = new-1).
--   correlation_id / causation_id — tracing hooks for the event that
--               originated the saga instance.
--
-- saga_timeouts: durable reminder rows. A saga schedules a timeout
-- (Store.ScheduleTimeout); the Coordinator's RunTimeoutSweep polls
-- ListDueTimeouts and fires the saga's Timeout callback when due_at passes.
-- A (saga_id, kind) pair is unique so a saga can re-arm the same reminder
-- idempotently; kind defaults to '' for the single common timeout.

CREATE TABLE IF NOT EXISTS sagas (
    saga_id        TEXT       NOT NULL PRIMARY KEY,
    saga_type      TEXT       NOT NULL,
    status         TEXT       NOT NULL,
    state          JSONB      NOT NULL DEFAULT '{}'::jsonb,
    correlation_id TEXT       NOT NULL DEFAULT '',
    causation_id   TEXT       NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    version        BIGINT     NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sagas_type
    ON sagas (saga_type);

CREATE TABLE IF NOT EXISTS saga_timeouts (
    saga_id TEXT       NOT NULL,
    due_at  TIMESTAMPTZ NOT NULL,
    kind    TEXT       NOT NULL DEFAULT '',
    PRIMARY KEY (saga_id, kind)
);

CREATE INDEX IF NOT EXISTS idx_saga_timeouts_due_at
    ON saga_timeouts (due_at);
