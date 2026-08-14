CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_deliveries_terminal_retention
    ON event_deliveries (updated_at, id)
    WHERE status IN ('succeeded', 'dead');
