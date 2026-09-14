CREATE OR REPLACE FUNCTION hubuum_fence_import_receipt()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.execution_claim_token IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM tasks
        WHERE id = NEW.task_id AND kind = 'import'
          AND lease_token = NEW.execution_claim_token
          AND status IN ('validating', 'running')
          AND deleted_at IS NULL
          AND lease_expires_at > clock_timestamp() AT TIME ZONE 'UTC'
        FOR UPDATE
    ) THEN
        RAISE EXCEPTION 'hubuum_import_claim_expired';
    END IF;
    RETURN NULL;
END;
$$;

ALTER TABLE remote_call_results
    DROP CONSTRAINT remote_call_side_effect_state,
    DROP COLUMN side_effect_state;
DROP INDEX tasks_execution_deadline_active_idx;
ALTER TABLE tasks
    DROP CONSTRAINT tasks_terminal_reason,
    DROP CONSTRAINT tasks_execution_phase,
    DROP CONSTRAINT tasks_execution_deadline_finite,
    DROP CONSTRAINT tasks_cancellation_shape,
    DROP COLUMN terminal_reason,
    DROP COLUMN remote_dispatched_at,
    DROP COLUMN import_effects_committed_at,
    DROP COLUMN execution_deadline_at,
    DROP COLUMN cancel_reason,
    DROP COLUMN cancel_requested_by,
    DROP COLUMN cancel_requested_at;
