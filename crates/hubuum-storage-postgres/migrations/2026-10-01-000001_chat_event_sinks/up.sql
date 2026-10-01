-- Offline upgrade: stop older API and worker processes before applying.
-- Normal deliveries retain one row per event/subscription; tests are separate.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE event_sinks DROP CONSTRAINT event_sinks_kind_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE event_sinks ADD CONSTRAINT event_sinks_kind_check
    CHECK (kind IN ('webhook', 'amqp', 'valkey_stream', 'email', 'slack', 'mattermost')) NOT VALID;
ALTER TABLE event_sinks VALIDATE CONSTRAINT event_sinks_kind_check;
ALTER TABLE event_sinks ADD COLUMN delivery_policy JSONB NOT NULL DEFAULT '{"min_interval_ms":null}';
ALTER TABLE event_sinks ADD CONSTRAINT event_sinks_delivery_policy_check CHECK (
    jsonb_typeof(delivery_policy) = 'object' AND
    CASE WHEN delivery_policy->'min_interval_ms' IS NULL OR delivery_policy->'min_interval_ms' = 'null'::jsonb THEN true
         WHEN jsonb_typeof(delivery_policy->'min_interval_ms') = 'number' THEN
            (delivery_policy->>'min_interval_ms')::numeric BETWEEN 1 AND 86400000
            AND trunc((delivery_policy->>'min_interval_ms')::numeric) = (delivery_policy->>'min_interval_ms')::numeric
         ELSE false END
) NOT VALID;
ALTER TABLE event_sinks VALIDATE CONSTRAINT event_sinks_delivery_policy_check;
ALTER TABLE event_subscriptions ALTER COLUMN collection_id DROP NOT NULL;
CREATE UNIQUE INDEX event_subscriptions_system_name_idx ON event_subscriptions(name) WHERE collection_id IS NULL; -- hubuum-compat: bounded-transactional-index
ALTER TABLE event_deliveries ADD COLUMN purpose TEXT NOT NULL DEFAULT 'event' CHECK (purpose IN ('event','test'));
ALTER TABLE event_deliveries ADD COLUMN deferred_reason TEXT NULL CHECK (deferred_reason IN ('configured_rate','provider_rate'));
CREATE UNIQUE INDEX event_deliveries_event_subscription_idx ON event_deliveries(event_id,subscription_id) WHERE purpose = 'event'; -- hubuum-compat: bounded-transactional-index
ALTER TABLE event_deliveries DROP CONSTRAINT event_deliveries_event_id_subscription_id_key; -- hubuum-compat: reviewed-offline-drop
CREATE TABLE event_sink_delivery_state (
    sink_id INT PRIMARY KEY REFERENCES event_sinks(id) ON DELETE CASCADE,
    next_allowed_at TIMESTAMP NOT NULL DEFAULT '-infinity',
    blocked_until TIMESTAMP NOT NULL DEFAULT '-infinity'
);
INSERT INTO event_sink_delivery_state(sink_id) SELECT id FROM event_sinks;
