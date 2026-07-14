DROP INDEX IF EXISTS idx_tasks_active_lease_expiry;

ALTER TABLE tasks
    DROP CONSTRAINT IF EXISTS tasks_lease_pair,
    DROP CONSTRAINT IF EXISTS tasks_attempt_count_nonnegative,
    DROP COLUMN IF EXISTS attempt_count,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS lease_token;
