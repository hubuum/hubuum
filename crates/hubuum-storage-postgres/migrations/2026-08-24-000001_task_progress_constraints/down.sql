ALTER TABLE tasks
DROP CONSTRAINT tasks_total_items_nonnegative,
DROP CONSTRAINT tasks_processed_items_nonnegative,
DROP CONSTRAINT tasks_success_items_nonnegative,
DROP CONSTRAINT tasks_failed_items_nonnegative;

DROP TRIGGER update_tasks_updated_at ON tasks;
CREATE TRIGGER update_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

DROP FUNCTION update_task_modified_column();
