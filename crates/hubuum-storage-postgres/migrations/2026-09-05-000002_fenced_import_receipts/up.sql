-- Execution receipts are the ordinary typed results, committed with their
-- domain effects. Planning/dry-run results have a claim and no execution index.
-- Restored historical results have neither claim metadata nor execution index.
ALTER TABLE import_task_results
    ADD COLUMN execution_index BIGINT,
    ADD COLUMN execution_claim_token UUID;
ALTER TABLE import_task_results
    ADD CONSTRAINT import_execution_receipt_fields CHECK (
        execution_index IS NULL
        OR (execution_index IS NOT NULL AND execution_index >= 0 AND execution_claim_token IS NOT NULL)
    ) NOT VALID;

CREATE FUNCTION hubuum_fence_import_receipt()
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
CREATE CONSTRAINT TRIGGER import_execution_commit_fence
    AFTER INSERT ON import_task_results
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION hubuum_fence_import_receipt();
