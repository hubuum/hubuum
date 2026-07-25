-- A same-name remnant from a failed concurrent build must make the retry fail
-- instead of allowing Diesel to record an unusable index as migrated.
CREATE INDEX CONCURRENTLY idx_tokens_explicit_expiry_retention
    ON tokens (expires_at, id)
    WHERE expires_at IS NOT NULL;
