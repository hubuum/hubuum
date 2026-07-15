CREATE INDEX CONCURRENTLY idx_tasks_active_capacity
    ON tasks (submitted_by, kind)
    WHERE submitted_by IS NOT NULL
      AND deleted_at IS NULL
      AND status IN ('queued', 'validating', 'running');
