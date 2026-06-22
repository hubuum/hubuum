DROP TRIGGER IF EXISTS update_remote_targets_updated_at ON remote_targets;
DROP TABLE IF EXISTS remote_call_results;
DROP TABLE IF EXISTS remote_targets;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_kind_check,
    ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'report', 'export', 'reindex'));

ALTER TABLE permissions
    DROP COLUMN IF EXISTS has_execute_remote_target,
    DROP COLUMN IF EXISTS has_delete_remote_target,
    DROP COLUMN IF EXISTS has_update_remote_target,
    DROP COLUMN IF EXISTS has_create_remote_target,
    DROP COLUMN IF EXISTS has_read_remote_target;
