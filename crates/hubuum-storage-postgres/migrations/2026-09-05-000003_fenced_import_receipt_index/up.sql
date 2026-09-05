-- Fail closed if an interrupted concurrent build left an invalid index.
CREATE UNIQUE INDEX CONCURRENTLY import_execution_receipt_once
    ON import_task_results (task_id, execution_index)
    WHERE execution_index IS NOT NULL;
