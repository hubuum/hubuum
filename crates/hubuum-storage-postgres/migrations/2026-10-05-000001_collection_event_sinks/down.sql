SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM event_sinks WHERE collection_id IS NOT NULL) THEN
        RAISE EXCEPTION 'Remove collection-owned event sinks before rollback; older servers cannot preserve their ownership and credential binding';
    END IF;
END;
$$;

DROP TABLE event_sink_collection_grants;
ALTER TABLE event_sinks DROP CONSTRAINT collection_event_sink_configuration;
ALTER TABLE event_sinks DROP COLUMN collection_id;
