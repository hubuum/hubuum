ALTER TABLE tasks VALIDATE CONSTRAINT tasks_trace_link_complete;
ALTER TABLE tasks VALIDATE CONSTRAINT tasks_trace_link_valid;
ALTER TABLE events VALIDATE CONSTRAINT events_trace_link_complete;
ALTER TABLE events VALIDATE CONSTRAINT events_trace_link_valid;
ALTER TABLE events VALIDATE CONSTRAINT events_correlation_id_bounded;
