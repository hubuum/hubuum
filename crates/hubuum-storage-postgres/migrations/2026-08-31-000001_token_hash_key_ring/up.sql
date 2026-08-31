ALTER TABLE tokens
    ADD COLUMN token_format SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN token_hash_algorithm SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN token_hash_key_id VARCHAR(32);

ALTER TABLE tokens
    ADD CONSTRAINT tokens_token_format_supported
        CHECK (token_format IN (0, 1)) NOT VALID,
    ADD CONSTRAINT tokens_token_hash_algorithm_supported
        CHECK (token_hash_algorithm = 1) NOT VALID,
    ADD CONSTRAINT tokens_versioned_key_id_present
        CHECK (token_format = 0 OR token_hash_key_id IS NOT NULL) NOT VALID,
    ADD CONSTRAINT tokens_hash_key_id_valid
        CHECK (
            token_hash_key_id IS NULL
            OR token_hash_key_id ~ '^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$'
        ) NOT VALID;
