-- A same-name remnant from a failed concurrent build must make the retry fail
-- instead of allowing Diesel to record an unusable index as migrated.
CREATE INDEX CONCURRENTLY idx_tasks_submitted_token_id
    ON tasks (submitted_token_id)
    WHERE submitted_token_id IS NOT NULL;
