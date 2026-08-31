ALTER TABLE tokens
    DROP CONSTRAINT IF EXISTS tokens_hash_key_id_valid,
    DROP CONSTRAINT IF EXISTS tokens_versioned_key_id_present,
    DROP CONSTRAINT IF EXISTS tokens_token_hash_algorithm_supported,
    DROP CONSTRAINT IF EXISTS tokens_token_format_supported,
    DROP COLUMN IF EXISTS token_hash_key_id,
    DROP COLUMN IF EXISTS token_hash_algorithm,
    DROP COLUMN IF EXISTS token_format;
