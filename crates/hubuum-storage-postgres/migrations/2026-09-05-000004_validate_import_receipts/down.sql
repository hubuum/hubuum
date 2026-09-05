ALTER TABLE import_task_results DROP CONSTRAINT import_execution_receipt_fields;
ALTER TABLE import_task_results ADD CONSTRAINT import_execution_receipt_fields CHECK (
    execution_index IS NULL
    OR (execution_index IS NOT NULL AND execution_index >= 0 AND execution_claim_token IS NOT NULL)
) NOT VALID;
