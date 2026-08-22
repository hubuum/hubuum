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
