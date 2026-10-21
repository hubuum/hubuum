DROP TABLE credential_sources;
DROP TABLE credential_activity;
DELETE FROM tokens WHERE auth_session_id IS NOT NULL;
ALTER TABLE tokens DROP CONSTRAINT tokens_session_owner;
ALTER TABLE tokens DROP CONSTRAINT tokens_session_kind;
ALTER TABLE tokens DROP COLUMN auth_session_id;
ALTER TABLE tokens DROP COLUMN credential_kind;
DROP TABLE refresh_tokens;
DROP TABLE auth_sessions;
