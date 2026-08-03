-- Revocation is a terminal lifecycle event. This index lets the retention
-- worker purge long-lived credentials by revocation time without scanning
-- tokens whose explicit expiry may still be years away.
--
-- A same-name remnant from a failed concurrent build must make the retry fail
-- instead of allowing Diesel to record an unusable index as migrated.
CREATE INDEX CONCURRENTLY idx_tokens_revoked_retention
    ON tokens (revoked_at, id)
    WHERE revoked_at IS NOT NULL;
