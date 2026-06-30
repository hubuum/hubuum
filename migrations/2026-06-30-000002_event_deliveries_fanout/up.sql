CREATE TABLE event_deliveries (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT REFERENCES events (id) ON DELETE CASCADE NOT NULL,
    subscription_id INT REFERENCES event_subscriptions (id) ON DELETE CASCADE NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_flight', 'succeeded', 'failed', 'dead')),
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP NOT NULL DEFAULT now(),
    last_error TEXT NULL,
    locked_until TIMESTAMP NULL,
    claim_token UUID NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (event_id, subscription_id),
    CHECK (attempts >= 0)
);

CREATE INDEX idx_event_deliveries_event_id ON event_deliveries(event_id);
CREATE INDEX idx_event_deliveries_subscription_id ON event_deliveries(subscription_id);
CREATE INDEX idx_event_deliveries_pending
    ON event_deliveries(next_attempt_at, id)
    WHERE status IN ('pending', 'failed');
CREATE INDEX idx_event_deliveries_in_flight_locks
    ON event_deliveries(locked_until)
    WHERE status = 'in_flight';

CREATE TRIGGER update_event_deliveries_updated_at
BEFORE UPDATE ON event_deliveries
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE OR REPLACE FUNCTION notify_events_fanout()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('hubuum_events_fanout', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_fanout_notify
AFTER INSERT ON events
FOR EACH ROW
EXECUTE FUNCTION notify_events_fanout();
