-- Offline upgrade: older writers and workers do not enforce sink grants.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE event_sinks ADD COLUMN collection_id integer REFERENCES collections(id) ON DELETE CASCADE;
ALTER TABLE event_sinks ADD CONSTRAINT collection_event_sink_configuration CHECK (
    collection_id IS NULL OR (
        kind = 'webhook' AND secret_ref IS NULL
        AND NOT (config ? 'url_secret_ref')
        AND jsonb_typeof(config -> 'destination_url') = 'string'
        AND length(config ->> 'destination_url') > 0
    ) IS TRUE
) NOT VALID;
ALTER TABLE event_sinks VALIDATE CONSTRAINT collection_event_sink_configuration;
CREATE INDEX event_sinks_collection_id_idx ON event_sinks(collection_id); -- hubuum-compat: bounded-transactional-index

CREATE TABLE event_sink_collection_grants (
    sink_id integer NOT NULL REFERENCES event_sinks(id) ON DELETE CASCADE,
    collection_id integer NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    PRIMARY KEY (sink_id, collection_id)
);
CREATE INDEX event_sink_collection_grants_collection_idx ON event_sink_collection_grants(collection_id, sink_id); -- hubuum-compat: bounded-transactional-index

-- Preserve existing collection integrations, without granting access to new collections.
INSERT INTO event_sink_collection_grants(sink_id, collection_id)
SELECT DISTINCT sink_id, collection_id FROM event_subscriptions WHERE collection_id IS NOT NULL;
