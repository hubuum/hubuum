-- One durable queue and one worker claim across every server replica.
CREATE UNIQUE INDEX tasks_one_active_computed_read_repair
ON tasks ((1))
WHERE kind = 'reindex'
  AND deleted_at IS NULL
  AND status IN ('queued', 'validating', 'running')
  AND request_payload->>'type' = 'computed_read_repairs';

ALTER TABLE tasks ADD CONSTRAINT tasks_computed_read_repair_bound CHECK (
    request_payload->>'type' IS DISTINCT FROM 'computed_read_repairs'
    OR (kind = 'reindex'
        AND jsonb_typeof(request_payload->'objects') = 'array'
        AND jsonb_array_length(request_payload->'objects') <= 4096) IS TRUE
);
