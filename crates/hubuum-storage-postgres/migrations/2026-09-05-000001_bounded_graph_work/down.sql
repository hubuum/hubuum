CREATE OR REPLACE FUNCTION get_bidirectionally_related_objects(
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
    WITH RECURSIVE object_edges AS NOT MATERIALIZED (
        SELECT from_hubuum_object_id AS source_object_id,
               to_hubuum_object_id AS target_object_id
        FROM hubuumobject_relation

        UNION ALL

        SELECT to_hubuum_object_id AS source_object_id,
               from_hubuum_object_id AS target_object_id
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
    JOIN hubuumobject source_object
      ON source_object.id = deduped_walk.ancestor_object_id
    JOIN hubuumobject target_object
      ON target_object.id = deduped_walk.descendant_object_id
    WHERE (
            COALESCE(cardinality(valid_collection_ids), 0) = 0
            OR source_object.collection_id = ANY(valid_collection_ids)
          )
      AND (
            COALESCE(cardinality(valid_collection_ids), 0) = 0
            OR target_object.collection_id = ANY(valid_collection_ids)
          );
$$ LANGUAGE sql STABLE;

DROP FUNCTION get_bidirectionally_related_objects(INT, INT[], INT, INT);
DROP FUNCTION hubuum_require_graph_budget(BIGINT, INTEGER);
