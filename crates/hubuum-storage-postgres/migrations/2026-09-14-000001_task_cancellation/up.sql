-- Deploy during a quiet period with old task workers drained. The new deadline
-- column is initially NULL, so the partial index starts empty, but its build
-- still scans tasks. Bound lock waits and scans; a timeout rolls back the entire
-- migration so it can be retried without leaving partial schema changes.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE tasks
    ADD COLUMN cancel_requested_at TIMESTAMP,
    ADD COLUMN cancel_requested_by INTEGER,
    ADD COLUMN cancel_reason TEXT,
    ADD COLUMN execution_deadline_at TIMESTAMP,
    ADD COLUMN import_effects_committed_at TIMESTAMP,
    ADD COLUMN remote_dispatched_at TIMESTAMP,
    ADD COLUMN terminal_reason VARCHAR(32);

ALTER TABLE tasks
    ADD CONSTRAINT tasks_cancel_requested_by_fkey FOREIGN KEY (cancel_requested_by)
        REFERENCES principals(id) ON DELETE SET NULL NOT VALID,
    ADD CONSTRAINT tasks_cancellation_shape CHECK (
        (cancel_requested_at IS NOT NULL OR (cancel_requested_by IS NULL AND cancel_reason IS NULL))
        AND (cancel_requested_at IS NULL OR isfinite(cancel_requested_at))
        AND (cancel_reason IS NULL OR (
            octet_length(cancel_reason) BETWEEN 1 AND 512
            AND length(btrim(cancel_reason)) > 0
            AND cancel_reason !~ '[[:cntrl:]]'
        ))
    ) NOT VALID,
    ADD CONSTRAINT tasks_execution_deadline_finite CHECK (
        execution_deadline_at IS NULL OR isfinite(execution_deadline_at)
    ) NOT VALID,
    ADD CONSTRAINT tasks_execution_phase CHECK (
        (import_effects_committed_at IS NULL OR (kind = 'import' AND isfinite(import_effects_committed_at)))
        AND (remote_dispatched_at IS NULL OR (kind = 'remote_call' AND isfinite(remote_dispatched_at)))
    ) NOT VALID,
    ADD CONSTRAINT tasks_terminal_reason CHECK (
        terminal_reason IS NULL OR (
            status = 'cancelled' AND (
                (terminal_reason = 'cancel_requested' AND cancel_requested_at IS NOT NULL)
                OR (terminal_reason = 'deadline_exceeded' AND execution_deadline_at IS NOT NULL)
            )
        )
    ) NOT VALID;

ALTER TABLE tasks
    VALIDATE CONSTRAINT tasks_cancel_requested_by_fkey,
    VALIDATE CONSTRAINT tasks_cancellation_shape,
    VALIDATE CONSTRAINT tasks_execution_deadline_finite,
    VALIDATE CONSTRAINT tasks_execution_phase,
    VALIDATE CONSTRAINT tasks_terminal_reason;

CREATE INDEX tasks_execution_deadline_active_idx ON tasks (execution_deadline_at, id)
    WHERE status IN ('queued', 'validating', 'running') AND execution_deadline_at IS NOT NULL; -- hubuum-compat: bounded-transactional-index

ALTER TABLE remote_call_results
    ADD COLUMN side_effect_state VARCHAR(32) NOT NULL DEFAULT 'legacy_unknown';

ALTER TABLE remote_call_results
    ADD CONSTRAINT remote_call_side_effect_state CHECK (
        side_effect_state IN ('not_sent', 'possibly_sent', 'response_received', 'legacy_unknown')
    ) NOT VALID;

ALTER TABLE remote_call_results VALIDATE CONSTRAINT remote_call_side_effect_state;

-- A cancellation request or deadline that wins before commit must abort the
-- same transaction as import domain writes and their durable receipts. The
-- row lock serializes the final decision with cancellation and lease changes.
CREATE OR REPLACE FUNCTION hubuum_fence_import_receipt()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    task_row tasks%ROWTYPE;
BEGIN
    IF NEW.execution_claim_token IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT * INTO task_row FROM tasks WHERE id = NEW.task_id FOR NO KEY UPDATE;
    IF NOT FOUND OR task_row.kind <> 'import'
        OR task_row.lease_token IS DISTINCT FROM NEW.execution_claim_token
        OR task_row.status NOT IN ('validating', 'running')
        OR task_row.deleted_at IS NOT NULL
        OR task_row.lease_expires_at IS NULL
        OR task_row.lease_expires_at <= clock_timestamp() AT TIME ZONE 'UTC' THEN
        RAISE EXCEPTION 'hubuum_import_claim_expired';
    END IF;
    IF task_row.cancel_requested_at IS NOT NULL
        AND (task_row.execution_deadline_at IS NULL
            OR task_row.cancel_requested_at <= task_row.execution_deadline_at) THEN
        RAISE EXCEPTION 'hubuum_task_cancelled';
    END IF;
    IF task_row.execution_deadline_at <= clock_timestamp() AT TIME ZONE 'UTC' THEN
        RAISE EXCEPTION 'hubuum_task_deadline_exceeded';
    END IF;
    IF task_row.cancel_requested_at IS NOT NULL THEN
        RAISE EXCEPTION 'hubuum_task_cancelled';
    END IF;
    RETURN NULL;
END;
$$;
