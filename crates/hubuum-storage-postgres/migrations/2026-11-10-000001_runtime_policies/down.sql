-- The append-only audit foreign key intentionally prevents rollback after use.
DELETE FROM runtime_settings WHERE setting_group IN ('observability', 'network', 'query_limits', 'tasks', 'remote_calls', 'tokens', 'login_rate_limit', 'events', 'exports', 'backups', 'restores');
ALTER TABLE runtime_settings DROP CONSTRAINT runtime_settings_group_check;
ALTER TABLE runtime_settings ADD CONSTRAINT runtime_settings_group_check
    CHECK (setting_group IN ('authentication', 'authorization', 'query_observations', 'pagination'));
