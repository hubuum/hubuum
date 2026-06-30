DROP TRIGGER IF EXISTS events_fanout_notify ON events;
DROP FUNCTION IF EXISTS notify_events_fanout();

DROP TRIGGER IF EXISTS update_event_deliveries_updated_at ON event_deliveries;
DROP TABLE IF EXISTS event_deliveries;
