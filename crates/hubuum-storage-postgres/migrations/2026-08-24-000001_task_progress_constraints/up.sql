ALTER TABLE tasks
ADD CONSTRAINT tasks_total_items_nonnegative CHECK (total_items >= 0),
ADD CONSTRAINT tasks_processed_items_nonnegative CHECK (processed_items >= 0),
ADD CONSTRAINT tasks_success_items_nonnegative CHECK (success_items >= 0),
ADD CONSTRAINT tasks_failed_items_nonnegative CHECK (failed_items >= 0);

-- Task lifecycle timestamps use clock_timestamp() because claims and
-- transitions can occur inside transactions that outlive their start time.
-- Keep updated_at on the same clock so projections cannot contain a terminal
-- or start timestamp later than their last update timestamp.
CREATE FUNCTION update_task_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = clock_timestamp() AT TIME ZONE 'UTC';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER update_tasks_updated_at ON tasks;
CREATE TRIGGER update_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION update_task_modified_column();
