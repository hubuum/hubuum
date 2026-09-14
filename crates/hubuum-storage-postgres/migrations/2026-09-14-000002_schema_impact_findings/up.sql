SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Findings are append-only during a scan; the bounded cursor checkpoint is
-- committed in the same transaction. Historical IDs survive object deletion.
CREATE TABLE schema_impact_findings (
    task_id INTEGER NOT NULL REFERENCES public.schema_validation_work(task_id) ON DELETE CASCADE,
    object_id INTEGER NOT NULL CHECK (object_id > 0),
    reason JSONB NOT NULL CHECK (jsonb_typeof(reason) = 'object'),
    PRIMARY KEY (task_id, object_id)
);
