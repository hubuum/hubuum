-- Pre-release terminology rename: namespaces are now collections.

ALTER TABLE namespaces RENAME TO collections;
ALTER TABLE collections RENAME CONSTRAINT namespaces_pkey TO collections_pkey;
ALTER TABLE collections RENAME CONSTRAINT namespaces_name_key TO collections_name_key;
ALTER INDEX idx_namespaces_name RENAME TO idx_collections_name;
ALTER TRIGGER update_namespaces_updated_at ON collections RENAME TO update_collections_updated_at;

ALTER TABLE namespaces_history RENAME TO collections_history;
ALTER TABLE collections_history RENAME CONSTRAINT namespaces_history_pkey TO collections_history_pkey;
ALTER SEQUENCE namespaces_history_seq RENAME TO collections_history_seq;
ALTER INDEX namespaces_history_id_from_idx RENAME TO collections_history_id_from_idx;
ALTER INDEX namespaces_history_actor_idx RENAME TO collections_history_actor_idx;

ALTER TABLE events RENAME COLUMN namespace_id TO collection_id;
ALTER INDEX events_namespace_occurred_idx RENAME TO events_collection_occurred_idx;

ALTER TABLE event_subscriptions RENAME COLUMN namespace_id TO collection_id;
ALTER INDEX idx_event_subscriptions_namespace_id RENAME TO idx_event_subscriptions_collection_id;

ALTER TABLE hubuumclass RENAME COLUMN namespace_id TO collection_id;
ALTER INDEX idx_hubuumclass_namespace_id RENAME TO idx_hubuumclass_collection_id;

ALTER TABLE hubuumclass_history RENAME COLUMN namespace_id TO collection_id;

ALTER TABLE hubuumobject RENAME COLUMN namespace_id TO collection_id;
ALTER INDEX idx_hubuumobject_namespace_id RENAME TO idx_hubuumobject_collection_id;

ALTER TABLE hubuumobject_history RENAME COLUMN namespace_id TO collection_id;

ALTER TABLE permissions RENAME COLUMN namespace_id TO collection_id;
ALTER TABLE permissions RENAME COLUMN has_read_namespace TO has_read_collection;
ALTER TABLE permissions RENAME COLUMN has_update_namespace TO has_update_collection;
ALTER TABLE permissions RENAME COLUMN has_delete_namespace TO has_delete_collection;
ALTER TABLE permissions RENAME COLUMN has_delegate_namespace TO has_delegate_collection;
ALTER INDEX idx_permissions_namespace_id RENAME TO idx_permissions_collection_id;

ALTER TABLE report_templates RENAME COLUMN namespace_id TO collection_id;
ALTER INDEX idx_report_templates_namespace_id RENAME TO idx_report_templates_collection_id;

ALTER TABLE report_templates_history RENAME COLUMN namespace_id TO collection_id;

ALTER TABLE remote_targets RENAME COLUMN namespace_id TO collection_id;
ALTER INDEX idx_remote_targets_namespace_id RENAME TO idx_remote_targets_collection_id;

ALTER TABLE remote_targets_history RENAME COLUMN namespace_id TO collection_id;

DROP FUNCTION get_transitively_linked_objects(INT, INT, INT[], INT);
DROP FUNCTION get_bidirectionally_related_objects(INT, INT[], INT);
DROP FUNCTION get_bidirectionally_related_classes(INT, INT[], INT, TEXT, INT[], BOOLEAN, TEXT, INT[], BOOLEAN);

CREATE FUNCTION get_transitively_linked_objects(
    start_object_id INT,
    target_class_id INT,
    valid_collection_ids INT[],
    max_depth INT DEFAULT 100
)
RETURNS TABLE (
    target_object_id INT,
    path INT[]
) AS $$
DECLARE
    start_class_id INT;
BEGIN
    SELECT hubuum_class_id INTO start_class_id
    FROM hubuumobject
    WHERE id = start_object_id;

    IF start_class_id IS NULL THEN
        RETURN;
    END IF;

    IF start_class_id <> target_class_id
       AND NOT EXISTS (
           SELECT 1
           FROM hubuumclass_reachability
           WHERE ancestor_class_id = LEAST(start_class_id, target_class_id)
             AND descendant_class_id = GREATEST(start_class_id, target_class_id)
       ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH RECURSIVE object_edges AS (
        SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation

        UNION ALL

        SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation
    ),
    graph_walk AS (
        SELECT
            start_object_id AS ancestor_object_id,
            object_edges.target_object_id AS descendant_object_id,
            1 AS depth,
            ARRAY[start_object_id, object_edges.target_object_id] AS traversal_path
        FROM object_edges
        JOIN hubuumobject target_object
          ON target_object.id = object_edges.target_object_id
        WHERE object_edges.source_object_id = start_object_id
          AND (max_depth IS NULL OR max_depth >= 1)
          AND (
              COALESCE(cardinality(valid_collection_ids), 0) = 0
              OR target_object.collection_id = ANY(valid_collection_ids)
          )
          AND (
              target_object.hubuum_class_id = target_class_id
              OR EXISTS (
                  SELECT 1
                  FROM hubuumclass_reachability
                  WHERE ancestor_class_id = LEAST(target_object.hubuum_class_id, target_class_id)
                    AND descendant_class_id = GREATEST(target_object.hubuum_class_id, target_class_id)
              )
          )

        UNION ALL

        SELECT
            graph_walk.ancestor_object_id,
            object_edges.target_object_id AS descendant_object_id,
            graph_walk.depth + 1,
            graph_walk.traversal_path || object_edges.target_object_id
        FROM graph_walk
        JOIN object_edges
          ON object_edges.source_object_id = graph_walk.descendant_object_id
        JOIN hubuumobject target_object
          ON target_object.id = object_edges.target_object_id
        WHERE NOT (object_edges.target_object_id = ANY(graph_walk.traversal_path))
          AND (max_depth IS NULL OR graph_walk.depth < max_depth)
          AND (
              COALESCE(cardinality(valid_collection_ids), 0) = 0
              OR target_object.collection_id = ANY(valid_collection_ids)
          )
          AND (
              target_object.hubuum_class_id = target_class_id
              OR EXISTS (
                  SELECT 1
                  FROM hubuumclass_reachability
                  WHERE ancestor_class_id = LEAST(target_object.hubuum_class_id, target_class_id)
                    AND descendant_class_id = GREATEST(target_object.hubuum_class_id, target_class_id)
              )
          )
    ),
    deduped_walk AS (
        SELECT DISTINCT ON (descendant_object_id)
            descendant_object_id,
            depth,
            traversal_path
        FROM graph_walk
        ORDER BY descendant_object_id ASC, depth ASC, traversal_path ASC
    )
    SELECT
        deduped_walk.descendant_object_id AS target_object_id,
        deduped_walk.traversal_path AS path
    FROM deduped_walk
    JOIN hubuumobject target_object ON target_object.id = deduped_walk.descendant_object_id
    WHERE target_object.hubuum_class_id = target_class_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_bidirectionally_related_objects(
    start_object_id INT,
    valid_collection_ids INT[],
    max_depth INT
)
RETURNS TABLE (
    ancestor_object_id INT,
    descendant_object_id INT,
    depth INT,
    path INT[],
    ancestor_name VARCHAR,
    descendant_name VARCHAR,
    ancestor_collection_id INT,
    descendant_collection_id INT,
    ancestor_class_id INT,
    descendant_class_id INT,
    ancestor_description VARCHAR,
    descendant_description VARCHAR,
    ancestor_data JSONB,
    descendant_data JSONB,
    ancestor_created_at TIMESTAMP,
    descendant_created_at TIMESTAMP,
    ancestor_updated_at TIMESTAMP,
    descendant_updated_at TIMESTAMP
) AS $$
    WITH RECURSIVE object_edges AS (
        SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation

        UNION ALL

        SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation
    ),
    graph_walk AS (
        SELECT
            start_object_id AS ancestor_object_id,
            object_edges.target_object_id AS descendant_object_id,
            1 AS depth,
            ARRAY[start_object_id, object_edges.target_object_id] AS path
        FROM object_edges
        JOIN hubuumobject target_object
          ON target_object.id = object_edges.target_object_id
        WHERE object_edges.source_object_id = start_object_id
          AND (max_depth IS NULL OR max_depth >= 1)
          AND (
              COALESCE(cardinality(valid_collection_ids), 0) = 0
              OR target_object.collection_id = ANY(valid_collection_ids)
          )

        UNION ALL

        SELECT
            graph_walk.ancestor_object_id,
            object_edges.target_object_id AS descendant_object_id,
            graph_walk.depth + 1,
            graph_walk.path || object_edges.target_object_id
        FROM graph_walk
        JOIN object_edges
          ON object_edges.source_object_id = graph_walk.descendant_object_id
        JOIN hubuumobject target_object
          ON target_object.id = object_edges.target_object_id
        WHERE NOT (object_edges.target_object_id = ANY(graph_walk.path))
          AND (max_depth IS NULL OR graph_walk.depth < max_depth)
          AND (
              COALESCE(cardinality(valid_collection_ids), 0) = 0
              OR target_object.collection_id = ANY(valid_collection_ids)
          )
    ),
    deduped_walk AS (
        SELECT DISTINCT ON (descendant_object_id)
            ancestor_object_id,
            descendant_object_id,
            depth,
            path
        FROM graph_walk
        ORDER BY descendant_object_id ASC, depth ASC, path ASC
    )
    SELECT
        source_object.id AS ancestor_object_id,
        target_object.id AS descendant_object_id,
        deduped_walk.depth,
        deduped_walk.path,
        source_object.name AS ancestor_name,
        target_object.name AS descendant_name,
        source_object.collection_id AS ancestor_collection_id,
        target_object.collection_id AS descendant_collection_id,
        source_object.hubuum_class_id AS ancestor_class_id,
        target_object.hubuum_class_id AS descendant_class_id,
        source_object.description AS ancestor_description,
        target_object.description AS descendant_description,
        source_object.data AS ancestor_data,
        target_object.data AS descendant_data,
        source_object.created_at AS ancestor_created_at,
        target_object.created_at AS descendant_created_at,
        source_object.updated_at AS ancestor_updated_at,
        target_object.updated_at AS descendant_updated_at
    FROM deduped_walk
    JOIN hubuumobject source_object ON source_object.id = deduped_walk.ancestor_object_id
    JOIN hubuumobject target_object ON target_object.id = deduped_walk.descendant_object_id
    WHERE (
            COALESCE(cardinality(valid_collection_ids), 0) = 0
            OR source_object.collection_id = ANY(valid_collection_ids)
          )
      AND (
            COALESCE(cardinality(valid_collection_ids), 0) = 0
            OR target_object.collection_id = ANY(valid_collection_ids)
          );
$$ LANGUAGE sql STABLE;

CREATE FUNCTION get_bidirectionally_related_classes(
    start_class_id INT,
    valid_collection_ids INT[],
    max_depth INT,
    filter_depth_op TEXT DEFAULT NULL,
    filter_depth_values INT[] DEFAULT NULL,
    filter_depth_negated BOOLEAN DEFAULT FALSE,
    filter_path_op TEXT DEFAULT NULL,
    filter_path_values INT[] DEFAULT NULL,
    filter_path_negated BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    ancestor_class_id INT,
    descendant_class_id INT,
    depth INT,
    path INT[],
    ancestor_name VARCHAR,
    descendant_name VARCHAR,
    ancestor_collection_id INT,
    descendant_collection_id INT,
    ancestor_json_schema JSONB,
    descendant_json_schema JSONB,
    ancestor_validate_schema BOOLEAN,
    descendant_validate_schema BOOLEAN,
    ancestor_description VARCHAR,
    descendant_description VARCHAR,
    ancestor_created_at TIMESTAMP,
    descendant_created_at TIMESTAMP,
    ancestor_updated_at TIMESTAMP,
    descendant_updated_at TIMESTAMP
) AS $$
    WITH related_classes AS (
        SELECT
            start_class_id AS ancestor_class_id,
            CASE
                WHEN hubuumclass_reachability.ancestor_class_id = start_class_id THEN hubuumclass_reachability.descendant_class_id
                ELSE hubuumclass_reachability.ancestor_class_id
            END AS descendant_class_id,
            hubuumclass_reachability.depth,
            CASE
                WHEN hubuumclass_reachability.ancestor_class_id = start_class_id THEN hubuumclass_reachability.path
                ELSE reverse_integer_array(hubuumclass_reachability.path)
            END AS path
        FROM hubuumclass_reachability
        WHERE (
                hubuumclass_reachability.ancestor_class_id = start_class_id
                OR hubuumclass_reachability.descendant_class_id = start_class_id
              )
          AND (
                max_depth IS NULL
                OR hubuumclass_reachability.depth <= max_depth
              )
    )
    SELECT
        source_class.id AS ancestor_class_id,
        target_class.id AS descendant_class_id,
        related_classes.depth,
        related_classes.path,
        source_class.name AS ancestor_name,
        target_class.name AS descendant_name,
        source_class.collection_id AS ancestor_collection_id,
        target_class.collection_id AS descendant_collection_id,
        source_class.json_schema AS ancestor_json_schema,
        target_class.json_schema AS descendant_json_schema,
        source_class.validate_schema AS ancestor_validate_schema,
        target_class.validate_schema AS descendant_validate_schema,
        source_class.description AS ancestor_description,
        target_class.description AS descendant_description,
        source_class.created_at AS ancestor_created_at,
        target_class.created_at AS descendant_created_at,
        source_class.updated_at AS ancestor_updated_at,
        target_class.updated_at AS descendant_updated_at
    FROM related_classes
    JOIN hubuumclass source_class ON source_class.id = related_classes.ancestor_class_id
    JOIN hubuumclass target_class ON target_class.id = related_classes.descendant_class_id
    WHERE (
            COALESCE(cardinality(valid_collection_ids), 0) = 0
            OR source_class.collection_id = ANY(valid_collection_ids)
          )
      AND (
            COALESCE(cardinality(valid_collection_ids), 0) = 0
            OR target_class.collection_id = ANY(valid_collection_ids)
          )
      AND (
            filter_depth_op IS NULL
            OR (
                (CASE filter_depth_op
                    WHEN 'equals' THEN related_classes.depth = ANY(filter_depth_values)
                    WHEN 'gt' THEN related_classes.depth > (SELECT MAX(v) FROM unnest(filter_depth_values) AS v)
                    WHEN 'gte' THEN related_classes.depth >= (SELECT MAX(v) FROM unnest(filter_depth_values) AS v)
                    WHEN 'lt' THEN related_classes.depth < (SELECT MIN(v) FROM unnest(filter_depth_values) AS v)
                    WHEN 'lte' THEN related_classes.depth <= (SELECT MIN(v) FROM unnest(filter_depth_values) AS v)
                    WHEN 'between' THEN (
                        cardinality(filter_depth_values) >= 2
                        AND related_classes.depth BETWEEN filter_depth_values[1] AND filter_depth_values[2]
                    )
                    ELSE FALSE
                END) != filter_depth_negated
            )
          )
      AND (
            filter_path_op IS NULL
            OR (
                (CASE filter_path_op
                    WHEN 'contains' THEN related_classes.path @> filter_path_values
                    WHEN 'equals' THEN related_classes.path = filter_path_values
                    ELSE FALSE
                END) != filter_path_negated
            )
          );
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION remote_target_subject_types_valid(subject_types JSONB)
RETURNS BOOLEAN AS $$
    SELECT CASE
        WHEN jsonb_typeof(subject_types) <> 'array' THEN FALSE
        ELSE jsonb_array_length(subject_types) > 0
            AND subject_types <@ '["collection", "class", "object", "class_relation", "object_relation"]'::jsonb
            AND NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(subject_types) AS item(value)
                GROUP BY item.value
                HAVING COUNT(*) > 1
            )
    END;
$$ LANGUAGE SQL IMMUTABLE;

DO $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'report_templates'::regclass
          AND pg_get_constraintdef(oid) LIKE '%namespaces%'
    LOOP
        EXECUTE format('ALTER TABLE report_templates DROP CONSTRAINT %I', constraint_name);
    END LOOP;

    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'remote_call_results'::regclass
          AND pg_get_constraintdef(oid) LIKE '%namespace%'
    LOOP
        EXECUTE format('ALTER TABLE remote_call_results DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

UPDATE report_templates
SET scope_kind = 'collections'
WHERE scope_kind = 'namespaces';

ALTER TABLE report_templates
ADD CONSTRAINT report_templates_scope_kind_shape_check CHECK (
    (kind = 'fragment' AND scope_kind IS NULL AND class_id IS NULL)
    OR
    (kind = 'report' AND scope_kind IN ('objects_in_class', 'related_objects') AND class_id IS NOT NULL)
    OR
    (kind = 'report' AND scope_kind IN ('collections', 'classes', 'class_relations', 'object_relations') AND class_id IS NULL)
);

UPDATE remote_call_results
SET subject_type = 'collection'
WHERE subject_type = 'namespace';

ALTER TABLE remote_call_results
ADD CONSTRAINT remote_call_results_subject_type_check CHECK (
    subject_type IN ('collection', 'class', 'object', 'class_relation', 'object_relation')
);

UPDATE remote_targets
SET allowed_subject_types = (
    SELECT jsonb_agg(CASE WHEN value = '"namespace"'::jsonb THEN '"collection"'::jsonb ELSE value END)
    FROM jsonb_array_elements(allowed_subject_types) AS item(value)
)
WHERE allowed_subject_types ? 'namespace';

UPDATE event_subscriptions
SET entity_types = (
    SELECT jsonb_agg(CASE WHEN value = '"namespace"'::jsonb THEN '"collection"'::jsonb ELSE value END)
    FROM jsonb_array_elements(entity_types) AS item(value)
)
WHERE entity_types ? 'namespace';

UPDATE events
SET entity_type = 'collection'
WHERE entity_type = 'namespace';

UPDATE events
SET metadata = metadata - 'related_namespace_ids'
    || jsonb_build_object('related_collection_ids', metadata->'related_namespace_ids')
WHERE metadata ? 'related_namespace_ids';

UPDATE events
SET metadata = metadata - 'namespace_id'
    || jsonb_build_object('collection_id', metadata->'namespace_id')
WHERE metadata ? 'namespace_id';

UPDATE events
SET metadata = jsonb_set(metadata, '{subject_type}', '"collection"', false)
WHERE metadata->>'subject_type' = 'namespace';

UPDATE import_task_results
SET entity_kind = CASE entity_kind
    WHEN 'namespace' THEN 'collection'
    WHEN 'namespace_permission' THEN 'collection_permission'
    ELSE entity_kind
END
WHERE entity_kind IN ('namespace', 'namespace_permission');

CREATE OR REPLACE FUNCTION enforce_events_append_only()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF current_setting('events.allow_purge', true) IS DISTINCT FROM 'on' THEN
            RAISE EXCEPTION 'events table is append-only: DELETE is not permitted';
        END IF;
        RETURN OLD;
    END IF;

    IF NEW.id             IS DISTINCT FROM OLD.id
       OR NEW.event_id    IS DISTINCT FROM OLD.event_id
       OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
       OR NEW.entity_type IS DISTINCT FROM OLD.entity_type
       OR NEW.entity_id   IS DISTINCT FROM OLD.entity_id
       OR NEW.entity_name IS DISTINCT FROM OLD.entity_name
       OR NEW.collection_id IS DISTINCT FROM OLD.collection_id
       OR NEW.action      IS DISTINCT FROM OLD.action
       OR NEW.actor_user_id IS DISTINCT FROM OLD.actor_user_id
       OR NEW.actor_kind  IS DISTINCT FROM OLD.actor_kind
       OR NEW.request_id  IS DISTINCT FROM OLD.request_id
       OR NEW.correlation_id IS DISTINCT FROM OLD.correlation_id
       OR NEW.summary     IS DISTINCT FROM OLD.summary
       OR NEW.before      IS DISTINCT FROM OLD.before
       OR NEW.after       IS DISTINCT FROM OLD.after
       OR NEW.metadata    IS DISTINCT FROM OLD.metadata
       OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    THEN
        RAISE EXCEPTION 'events table is append-only: only fan-out claim fields and dispatched_at may be updated';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
