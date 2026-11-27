SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Older writers used the revoking transaction's start time. For a retained
-- token with an inverted timestamp, issuance is the earliest valid revocation.
UPDATE tokens SET revoked_at = issued WHERE revoked_at < issued;

ALTER TABLE tokens ADD CONSTRAINT tokens_revocation_after_issuance
    CHECK (revoked_at >= issued) NOT VALID;
ALTER TABLE tokens VALIDATE CONSTRAINT tokens_revocation_after_issuance;
