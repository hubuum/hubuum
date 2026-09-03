ALTER TABLE tasks
    ADD COLUMN trace_id VARCHAR(32),
    ADD COLUMN trace_span_id VARCHAR(16),
    ADD COLUMN trace_flags SMALLINT,
    ADD COLUMN trace_context_version SMALLINT,
    ADD CONSTRAINT tasks_trace_link_complete CHECK (
        num_nonnulls(trace_id, trace_span_id, trace_flags, trace_context_version) IN (0, 4)
    ),
    ADD CONSTRAINT tasks_trace_link_valid CHECK (
        trace_id ~ '^[0-9a-f]{32}$'
        AND trace_id !~ '^0+$'
        AND trace_span_id ~ '^[0-9a-f]{16}$'
        AND trace_span_id !~ '^0+$'
        AND trace_flags IN (0, 1)
        AND trace_context_version = 0
    );

ALTER TABLE events
    ADD COLUMN trace_id VARCHAR(32),
    ADD COLUMN trace_span_id VARCHAR(16),
    ADD COLUMN trace_flags SMALLINT,
    ADD COLUMN trace_context_version SMALLINT,
    ADD CONSTRAINT events_trace_link_complete CHECK (
        num_nonnulls(trace_id, trace_span_id, trace_flags, trace_context_version) IN (0, 4)
    ),
    ADD CONSTRAINT events_trace_link_valid CHECK (
        trace_id ~ '^[0-9a-f]{32}$'
        AND trace_id !~ '^0+$'
        AND trace_span_id ~ '^[0-9a-f]{16}$'
        AND trace_span_id !~ '^0+$'
        AND trace_flags IN (0, 1)
        AND trace_context_version = 0
    ),
    ADD CONSTRAINT events_correlation_id_bounded CHECK (
        correlation_id IS NULL
        OR (
            octet_length(correlation_id) BETWEEN 1 AND 128
            AND correlation_id ~ '^[!-~]+$'
        )
    ) NOT VALID;
