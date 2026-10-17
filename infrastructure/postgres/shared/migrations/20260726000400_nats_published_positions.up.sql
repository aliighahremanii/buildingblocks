-- Migration: NATS publisher position tracking table.
--
-- The NATS-JS publisher projection worker (P2.1) tracks its progress
-- through the KurrentDB $all stream in this table, mirroring
-- es_projection_checkpoints. The publisher is the system's outbox for
-- downstream consumers: every domain event appended to KurrentDB is
-- republished to NATS-JetStream exactly-once (modulo consumer
-- redelivery), with the KurrentDB EventID as the Nats-Msg-Id header so
-- JetStream deduplicates on the consumer side.
--
-- A separate table from es_projection_checkpoints keeps the publisher's
-- position distinct from per-sink projection positions; the publisher
-- may lag or lead the read-model projections independently.
CREATE TABLE IF NOT EXISTS es_nats_published_positions (
    publisher_name TEXT       NOT NULL PRIMARY KEY,
    position       BIGINT     NOT NULL DEFAULT 0,
    events_published BIGINT   NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
