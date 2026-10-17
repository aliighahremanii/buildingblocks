-- Convert command_idempotency.response_body from JSONB to BYTEA.
--
-- The idempotency wire contract requires a retried request to return the
-- EXACT response body bytes the first attempt produced. JSONB canonicalizes
-- whitespace on read (its text output adds a space after ':' and ','),
-- which breaks byte-identity (D-R2-01, regression of D016). BYTEA stores
-- and returns the cached bytes verbatim.
--
-- This migration converts databases created by 20260726000200 (which
-- originally created the column as JSONB). Fresh databases created after
-- 20260726000200 was edited to use BYTEA still run this ALTER; for them
-- `response_body::text::bytea` is a valid bytea->text->bytea round-trip
-- for the ASCII JSON bodies the middleware caches, so it is a no-op.
--
-- See decisions D016 (original fix, defeated by JSONB) and D021 (this fix).
ALTER TABLE command_idempotency
    ALTER COLUMN response_body TYPE BYTEA USING response_body::text::bytea;
