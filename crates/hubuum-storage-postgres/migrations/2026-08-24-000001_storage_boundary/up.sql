BEGIN;

CREATE TABLE event_retention_batches (
    claim_id UUID PRIMARY KEY,
    event_ids BIGINT[] NOT NULL,
    event_documents JSONB NOT NULL,
    delivery_cutoff TIMESTAMP NOT NULL,
    delivery_batch_size BIGINT NOT NULL CHECK (delivery_batch_size > 0),
    created_at TIMESTAMP NOT NULL DEFAULT (clock_timestamp() AT TIME ZONE 'UTC'),
    completed_at TIMESTAMP,
    pending_claim_slot BOOLEAN GENERATED ALWAYS AS (
        CASE WHEN completed_at IS NULL THEN TRUE END
    ) STORED,
    purged_events BIGINT,
    purged_terminal_deliveries BIGINT,
    CONSTRAINT event_retention_batches_documents_are_array
        CHECK (jsonb_typeof(event_documents) = 'array'),
    CONSTRAINT event_retention_batches_documents_match_ids
        CHECK (jsonb_array_length(event_documents) = cardinality(event_ids)),
    CONSTRAINT event_retention_batches_counts_are_non_negative
        CHECK (
            (purged_events IS NULL OR purged_events >= 0)
            AND (purged_terminal_deliveries IS NULL OR purged_terminal_deliveries >= 0)
        ),
    CONSTRAINT event_retention_batches_completion_follows_creation
        CHECK (completed_at IS NULL OR completed_at >= created_at),
    CONSTRAINT event_retention_batches_completion_is_complete
        CHECK (
            (completed_at IS NULL
                AND purged_events IS NULL
                AND purged_terminal_deliveries IS NULL)
            OR
            (completed_at IS NOT NULL
                AND purged_events IS NOT NULL
                AND purged_terminal_deliveries IS NOT NULL)
        ),
    CONSTRAINT event_retention_batches_one_pending_claim
        UNIQUE (pending_claim_slot)
);

ALTER TABLE tasks
ADD CONSTRAINT tasks_total_items_nonnegative CHECK (total_items >= 0) NOT VALID,
ADD CONSTRAINT tasks_processed_items_nonnegative CHECK (processed_items >= 0) NOT VALID,
ADD CONSTRAINT tasks_success_items_nonnegative CHECK (success_items >= 0) NOT VALID,
ADD CONSTRAINT tasks_failed_items_nonnegative CHECK (failed_items >= 0) NOT VALID;

ALTER TABLE export_task_outputs
ADD CONSTRAINT export_task_outputs_warning_count_nonnegative
    CHECK (warning_count >= 0) NOT VALID,
ADD CONSTRAINT export_task_outputs_durations_nonnegative
    CHECK (
        total_duration_ms >= 0
        AND query_duration_ms >= 0
        AND hydration_duration_ms >= 0
        AND render_duration_ms >= 0
    ) NOT VALID;

ALTER TABLE backup_task_outputs
ADD CONSTRAINT backup_task_outputs_size_matches_document
    CHECK (byte_size = octet_length(document)) NOT VALID,
ADD CONSTRAINT backup_task_outputs_sha256_is_hex
    CHECK (sha256 ~ '^[0-9a-f]{64}$') NOT VALID;

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

COMMIT;
BEGIN;

-- Validation scans may be long on a real installation. Run each separately
-- from the metadata and trigger changes so ordinary reads can continue.
ALTER TABLE tasks VALIDATE CONSTRAINT tasks_total_items_nonnegative;
COMMIT;
BEGIN;

ALTER TABLE tasks VALIDATE CONSTRAINT tasks_processed_items_nonnegative;
COMMIT;
BEGIN;

ALTER TABLE tasks VALIDATE CONSTRAINT tasks_success_items_nonnegative;
COMMIT;
BEGIN;

ALTER TABLE tasks VALIDATE CONSTRAINT tasks_failed_items_nonnegative;
COMMIT;
BEGIN;

ALTER TABLE export_task_outputs
VALIDATE CONSTRAINT export_task_outputs_warning_count_nonnegative;
COMMIT;
BEGIN;

ALTER TABLE export_task_outputs
VALIDATE CONSTRAINT export_task_outputs_durations_nonnegative;
COMMIT;
BEGIN;

ALTER TABLE backup_task_outputs
VALIDATE CONSTRAINT backup_task_outputs_size_matches_document;
COMMIT;
BEGIN;

ALTER TABLE backup_task_outputs
VALIDATE CONSTRAINT backup_task_outputs_sha256_is_hex;
COMMIT;
