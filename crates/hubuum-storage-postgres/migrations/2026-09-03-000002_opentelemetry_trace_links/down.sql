ALTER TABLE events
    DROP CONSTRAINT events_correlation_id_bounded,
    DROP CONSTRAINT events_trace_link_valid,
    DROP CONSTRAINT events_trace_link_complete,
    DROP COLUMN trace_context_version,
    DROP COLUMN trace_flags,
    DROP COLUMN trace_span_id,
    DROP COLUMN trace_id;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_trace_link_valid,
    DROP CONSTRAINT tasks_trace_link_complete,
    DROP COLUMN trace_context_version,
    DROP COLUMN trace_flags,
    DROP COLUMN trace_span_id,
    DROP COLUMN trace_id;
