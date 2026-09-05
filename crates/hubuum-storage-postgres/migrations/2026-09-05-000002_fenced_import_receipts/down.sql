DROP TRIGGER import_execution_commit_fence ON import_task_results;
DROP FUNCTION hubuum_fence_import_receipt();
ALTER TABLE import_task_results DROP CONSTRAINT import_execution_receipt_fields,
    DROP COLUMN execution_index, DROP COLUMN execution_claim_token;
