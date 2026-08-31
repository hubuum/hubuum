CREATE INDEX CONCURRENTLY tokens_token_hash_key_retirement_idx
    ON tokens (token_hash_key_id, revoked_at, expires_at);
