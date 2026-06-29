CREATE TABLE task_events (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL,
    event_type VARCHAR NOT NULL,
    message TEXT NOT NULL,
    data JSONB NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO task_events (
    task_id,
    event_type,
    message,
    data,
    created_at
)
SELECT
    entity_id,
    action,
    summary,
    metadata -> 'data',
    occurred_at
FROM events
WHERE entity_type = 'task'
  AND entity_id IS NOT NULL
ORDER BY occurred_at, id;

CREATE INDEX idx_task_events_task_id_created_at ON task_events (task_id, created_at);

SELECT set_config('events.allow_purge', 'on', true);
DELETE FROM events
WHERE entity_type = 'task';
