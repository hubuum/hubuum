-- Operational credentials are deliberately excluded from logical backups.
CREATE TABLE auth_sessions (
    id UUID PRIMARY KEY,
    principal_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    client_id VARCHAR(128) NOT NULL CHECK (client_id ~ '^[A-Za-z0-9._:-]{1,128}$'),
    authenticated_at TIMESTAMP NOT NULL,
    last_refreshed_at TIMESTAMP NOT NULL,
    access_seconds BIGINT NOT NULL CHECK (access_seconds > 0),
    idle_seconds BIGINT NOT NULL CHECK (idle_seconds >= access_seconds),
    absolute_expires_at TIMESTAMP NOT NULL,
    idle_expires_at TIMESTAMP NOT NULL,
    retain_until TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP,
    revocation_reason TEXT,
    UNIQUE (id, principal_id),
    CHECK (authenticated_at < idle_expires_at AND idle_expires_at <= absolute_expires_at),
    CHECK (absolute_expires_at < retain_until),
    CHECK ((revoked_at IS NULL) = (revocation_reason IS NULL))
);
CREATE INDEX auth_sessions_principal ON auth_sessions (principal_id, id);
CREATE INDEX auth_sessions_retention ON auth_sessions (retain_until, id);

CREATE TABLE refresh_tokens (
    id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES auth_sessions(id) ON DELETE CASCADE,
    digest TEXT NOT NULL UNIQUE,
    hash_key_id VARCHAR(32) NOT NULL,
    issued_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    consumed_at TIMESTAMP,
    reuse_detected_at TIMESTAMP,
    CHECK (expires_at > issued_at)
);
CREATE INDEX refresh_tokens_session ON refresh_tokens (session_id);

ALTER TABLE tokens ADD COLUMN auth_session_id UUID;
ALTER TABLE tokens ADD COLUMN credential_kind TEXT NOT NULL DEFAULT 'legacy'
    CHECK (credential_kind IN ('legacy', 'personal_access', 'service_account', 'session_access'));
ALTER TABLE tokens ADD CONSTRAINT tokens_session_kind
    CHECK ((credential_kind = 'session_access') = (auth_session_id IS NOT NULL));
ALTER TABLE tokens ADD CONSTRAINT tokens_session_owner
    FOREIGN KEY (auth_session_id, principal_id) REFERENCES auth_sessions(id, principal_id) ON DELETE CASCADE;
CREATE INDEX tokens_auth_session ON tokens (auth_session_id) WHERE auth_session_id IS NOT NULL;

-- One owner per durable credential or interactive family. Rotation retains history.
CREATE TABLE credential_activity (
    id BIGSERIAL PRIMARY KEY,
    token_id INTEGER UNIQUE REFERENCES tokens(id) ON DELETE CASCADE,
    session_id UUID UNIQUE REFERENCES auth_sessions(id) ON DELETE CASCADE,
    issuer_id INTEGER REFERENCES principals(id) ON DELETE SET NULL,
    issuance_method TEXT CHECK (issuance_method IN ('login','creation','renewal','refresh')),
    issued_at TIMESTAMP,
    creation_address TEXT,
    creation_method TEXT CHECK (creation_method IN ('connection_peer','trusted_forwarded','unavailable')),
    overflow_since TIMESTAMP,
    CHECK ((token_id IS NULL) <> (session_id IS NULL)),
    CHECK ((issuance_method IS NULL) = (issued_at IS NULL)),
    CHECK (creation_method IS NULL OR ((creation_method='unavailable') = (creation_address IS NULL)))
);
CREATE TABLE credential_sources (
    id BIGSERIAL PRIMARY KEY,
    activity_id BIGINT NOT NULL REFERENCES credential_activity(id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    address_method TEXT NOT NULL CHECK (address_method IN ('connection_peer','trusted_forwarded','unavailable')),
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    UNIQUE(activity_id,address,address_method),
    CHECK (first_seen <= last_seen),
    CHECK ((address_method='unavailable') = (address=''))
);
CREATE INDEX credential_sources_retention ON credential_sources(last_seen,id);
