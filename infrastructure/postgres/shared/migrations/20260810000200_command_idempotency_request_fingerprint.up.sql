-- Persist the raw request-body fingerprint used to reject altered same-key retries.
-- NULL preserves replay compatibility for records created before this migration.
ALTER TABLE command_idempotency
    ADD COLUMN IF NOT EXISTS request_fingerprint TEXT;
