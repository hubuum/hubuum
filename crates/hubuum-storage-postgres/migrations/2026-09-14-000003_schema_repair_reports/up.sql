SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE schema_impact_findings ADD COLUMN snapshot JSONB
    CHECK (snapshot IS NULL OR jsonb_typeof(snapshot) = 'object');

-- One retained rendering per source analysis. Replacing a rendering does not
-- update the source checkpoint or its immutable findings.
CREATE TABLE schema_repair_reports (
    task_id INTEGER PRIMARY KEY REFERENCES public.schema_validation_work(task_id) ON DELETE CASCADE,
    document JSONB NOT NULL CHECK (jsonb_typeof(document) = 'object')
);
