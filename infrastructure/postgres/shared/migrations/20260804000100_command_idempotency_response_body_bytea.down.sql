-- Revert command_idempotency.response_body back to JSONB (undoes
-- 20260804000100). Restores the pre-fix schema. Byte-identity of idempotency
-- replays will regress (D-R2-01); this downgrade exists for completeness,
-- not for production use.
--
-- convert_from(..., 'UTF8') interprets the BYTEA bytes as raw UTF-8 text.
-- A plain `::text` cast would instead yield Postgres's bytea_output escape
-- representation (e.g. `\x7b22...`), which is not valid JSON.
ALTER TABLE command_idempotency
    ALTER COLUMN response_body TYPE JSONB USING convert_from(response_body, 'UTF8')::jsonb;
