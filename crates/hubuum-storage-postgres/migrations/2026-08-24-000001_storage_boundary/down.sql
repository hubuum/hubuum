BEGIN;

ALTER TABLE tasks
DROP CONSTRAINT tasks_total_items_nonnegative,
DROP CONSTRAINT tasks_processed_items_nonnegative,
DROP CONSTRAINT tasks_success_items_nonnegative,
DROP CONSTRAINT tasks_failed_items_nonnegative;

ALTER TABLE export_task_outputs
DROP CONSTRAINT export_task_outputs_warning_count_nonnegative,
DROP CONSTRAINT export_task_outputs_durations_nonnegative;

ALTER TABLE backup_task_outputs
DROP CONSTRAINT backup_task_outputs_size_matches_document,
DROP CONSTRAINT backup_task_outputs_sha256_is_hex;

DROP TRIGGER update_tasks_updated_at ON tasks;
CREATE TRIGGER update_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

DROP FUNCTION update_task_modified_column();

DROP TABLE event_retention_batches;

COMMIT;
