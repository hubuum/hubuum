SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Deliberately retain evidence independently of token/principal deletion.
-- These records are operational authentication state, excluded from logical backups.
CREATE TABLE credential_approvals (
    id SERIAL PRIMARY KEY,
    actor_id INTEGER NOT NULL CHECK (actor_id > 0),
    token_id INTEGER NOT NULL CHECK (token_id > 0),
    operation TEXT NOT NULL CHECK (operation IN ('create_token', 'renew_token', 'create_user', 'update_user', 'import_credentials', 'confirm_restore')),
    restore_job_id BIGINT CHECK (restore_job_id > 0),
    invalidated_at TIMESTAMP,
    target_id INTEGER CHECK (target_id > 0),
    secret_digest TEXT NOT NULL UNIQUE CHECK (secret_digest ~ '^[0-9a-f]{64}$'),
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    authenticated_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    consumed_at TIMESTAMP,
    CHECK ((operation = 'confirm_restore') = (restore_job_id IS NOT NULL)),
    CHECK ((operation IN ('create_token', 'renew_token', 'update_user')) = (target_id IS NOT NULL)),
    CHECK (consumed_at IS NULL OR invalidated_at IS NULL),
    CHECK (invalidated_at IS NULL OR invalidated_at >= authenticated_at),
    CHECK (expires_at > authenticated_at AND expires_at <= authenticated_at + INTERVAL '120 seconds'),
    CHECK (consumed_at IS NULL OR (consumed_at >= authenticated_at AND consumed_at < expires_at))
);
