DROP TRIGGER IF EXISTS update_event_subscriptions_updated_at ON event_subscriptions;
DROP TRIGGER IF EXISTS update_event_sinks_updated_at ON event_sinks;

DROP TABLE IF EXISTS event_subscriptions;
DROP TABLE IF EXISTS event_sinks;

ALTER TABLE permissions
    DROP COLUMN has_manage_event_subscription;
