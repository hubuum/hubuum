SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Old executors do not understand this internal payload. Remaining repairs are
-- derived work and can be requested again by the old synchronous read path.
DELETE FROM tasks WHERE request_payload->>'type' = 'computed_read_repairs';
ALTER TABLE tasks DROP CONSTRAINT tasks_computed_read_repair_bound;
DROP INDEX tasks_one_active_computed_read_repair;
