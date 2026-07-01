DROP TRIGGER IF EXISTS events_fanout_notify ON events;
DROP FUNCTION IF EXISTS notify_events_fanout();

DROP TRIGGER IF EXISTS update_event_deliveries_updated_at ON event_deliveries;
DROP TABLE IF EXISTS event_deliveries;

DROP TRIGGER IF EXISTS update_event_subscriptions_updated_at ON event_subscriptions;
DROP TRIGGER IF EXISTS update_event_sinks_updated_at ON event_sinks;

DROP TABLE IF EXISTS event_subscriptions;
DROP TABLE IF EXISTS event_sinks;

ALTER TABLE permissions
    DROP COLUMN has_manage_event_subscription,
    DROP COLUMN has_read_audit;

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

DROP TABLE IF EXISTS events CASCADE;
DROP FUNCTION IF EXISTS enforce_events_append_only();

