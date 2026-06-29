-- Consolidate task lifecycle history into the unified events stream (#87).
--
-- Existing task_events rows are backfilled into events, preserving the public
-- task-history response fields as:
--   event_type -> events.action
--   message    -> events.summary
--   data       -> events.metadata.data

CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO events (
    event_id,
    occurred_at,
    entity_type,
    entity_id,
    action,
    actor_user_id,
    actor_kind,
    summary,
    metadata,
    schema_version
)
SELECT
    gen_random_uuid(),
    task_events.created_at,
    'task',
    task_events.task_id,
    task_events.event_type,
    CASE WHEN task_events.event_type = 'queued' THEN tasks.submitted_by ELSE NULL END,
    CASE
        WHEN task_events.event_type = 'queued' THEN 'user'
        WHEN task_events.event_type = 'cleanup' THEN 'system'
        ELSE 'worker'
    END,
    task_events.message,
    jsonb_strip_nulls(jsonb_build_object(
        'task_id', task_events.task_id,
        'task_kind', tasks.kind,
        'data', task_events.data
    )),
    1
FROM task_events
JOIN tasks ON tasks.id = task_events.task_id;

DROP TABLE task_events;
