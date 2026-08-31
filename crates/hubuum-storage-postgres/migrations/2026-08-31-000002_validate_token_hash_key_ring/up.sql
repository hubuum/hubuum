ALTER TABLE tokens VALIDATE CONSTRAINT tokens_token_format_supported;

ALTER TABLE tokens VALIDATE CONSTRAINT tokens_token_hash_algorithm_supported;

ALTER TABLE tokens VALIDATE CONSTRAINT tokens_versioned_key_id_present;

ALTER TABLE tokens VALIDATE CONSTRAINT tokens_hash_key_id_valid;
