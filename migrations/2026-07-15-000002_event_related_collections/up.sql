CREATE TABLE event_related_collections (
    event_id BIGINT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    collection_id INTEGER NOT NULL CHECK (collection_id > 0),
    PRIMARY KEY (event_id, collection_id)
);

CREATE INDEX event_related_collections_collection_event_idx
    ON event_related_collections (collection_id, event_id);

CREATE FUNCTION record_event_related_collections()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO event_related_collections (event_id, collection_id)
    SELECT NEW.id, normalized.collection_id
    FROM (
        SELECT CASE
            WHEN value ~ '^[1-9][0-9]{0,9}$'
            THEN CASE
                WHEN value::numeric <= 2147483647 THEN value::integer
                ELSE NULL
            END
            ELSE NULL
        END AS collection_id
        FROM jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(NEW.metadata -> 'related_collection_ids') = 'array'
                THEN NEW.metadata -> 'related_collection_ids'
                ELSE '[]'::jsonb
            END
        ) AS related(value)
    ) AS normalized
    WHERE normalized.collection_id IS NOT NULL
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_related_collections
AFTER INSERT ON events
FOR EACH ROW EXECUTE FUNCTION record_event_related_collections();

INSERT INTO event_related_collections (event_id, collection_id)
SELECT event.id, normalized.collection_id
FROM events AS event
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN value ~ '^[1-9][0-9]{0,9}$'
        THEN CASE
            WHEN value::numeric <= 2147483647 THEN value::integer
            ELSE NULL
        END
        ELSE NULL
    END AS collection_id
    FROM jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(event.metadata -> 'related_collection_ids') = 'array'
            THEN event.metadata -> 'related_collection_ids'
            ELSE '[]'::jsonb
        END
    ) AS related(value)
) AS normalized
WHERE normalized.collection_id IS NOT NULL
ON CONFLICT DO NOTHING;
