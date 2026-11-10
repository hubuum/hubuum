ALTER TABLE runtime_settings DROP CONSTRAINT runtime_settings_group_check;
ALTER TABLE runtime_settings ADD CONSTRAINT runtime_settings_group_check
    CHECK (setting_group IN ('authentication', 'authorization', 'query_observations', 'pagination', 'observability', 'network', 'query_limits', 'tasks', 'remote_calls', 'tokens', 'login_rate_limit', 'events', 'exports', 'backups', 'restores'));
INSERT INTO runtime_settings (setting_group, revision, value) VALUES
    ('observability', 1, NULL),
    ('network', 1, NULL),
    ('query_limits', 1, NULL),
    ('tasks', 1, NULL),
    ('remote_calls', 1, NULL),
    ('tokens', 1, NULL),
    ('login_rate_limit', 1, NULL),
    ('events', 1, NULL),
    ('exports', 1, NULL),
    ('backups', 1, NULL),
    ('restores', 1, NULL);
