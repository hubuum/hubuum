-- Keep this PR's schema changes atomic. These limits bound lock acquisition
-- and each statement; failure rolls back every change and the migration record.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Bound recursive work before sorting, deduplication, and response limits.
CREATE FUNCTION hubuum_require_graph_budget(observed BIGINT, maximum INTEGER)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    IF observed > maximum THEN
        RAISE EXCEPTION 'hubuum_graph_budget_exceeded';
    END IF;
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION get_bidirectionally_related_objects(
    start_object_id INT,
    valid_collection_ids INT[],
    max_depth INT,
    max_work_rows INT
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
          AND (graph_walk.depth < LEAST(COALESCE(max_depth, 100), 512))
          AND (
              COALESCE(cardinality(valid_collection_ids), 0) = 0
              OR target_object.collection_id = ANY(valid_collection_ids)
          )
    ),
    bounded_walk AS MATERIALIZED (
        SELECT * FROM graph_walk LIMIT LEAST(GREATEST(max_work_rows, 1), 50000) + 1
    ),
    walk_budget AS MATERIALIZED (
        SELECT hubuum_require_graph_budget(count(*), LEAST(GREATEST(max_work_rows, 1), 50000)) AS allowed FROM bounded_walk
    ),
    deduped_walk AS (
        SELECT DISTINCT ON (descendant_object_id)
            ancestor_object_id,
            descendant_object_id,
            depth,
            path
        FROM bounded_walk CROSS JOIN walk_budget
        WHERE walk_budget.allowed
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
    SELECT * FROM get_bidirectionally_related_objects(start_object_id, valid_collection_ids, COALESCE(max_depth, 100), 50000);
$$ LANGUAGE sql STABLE;

-- Execution receipts are the ordinary typed results, committed with their
-- domain effects. Planning/dry-run results have a claim and no execution index.
-- Restored historical results have neither claim metadata nor execution index.
ALTER TABLE import_task_results
    ADD COLUMN execution_index BIGINT,
    ADD COLUMN execution_claim_token UUID;
ALTER TABLE import_task_results
    ADD CONSTRAINT import_execution_receipt_fields CHECK (
        execution_index IS NULL
        OR (execution_index IS NOT NULL AND execution_index >= 0 AND execution_claim_token IS NOT NULL)
    ) NOT VALID;

CREATE FUNCTION hubuum_fence_import_receipt()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.execution_claim_token IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM tasks
        WHERE id = NEW.task_id AND kind = 'import'
          AND lease_token = NEW.execution_claim_token
          AND status IN ('validating', 'running')
          AND deleted_at IS NULL
          AND lease_expires_at > clock_timestamp() AT TIME ZONE 'UTC'
        FOR UPDATE
    ) THEN
        RAISE EXCEPTION 'hubuum_import_claim_expired';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER import_execution_commit_fence
    AFTER INSERT ON import_task_results
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION hubuum_fence_import_receipt();

-- Existing rows have NULL execution indexes, so this partial index starts
-- empty. Building it still scans history while holding the transaction's table
-- lock. The explicit time limits above make contention or a slow scan fail and
-- roll back, allowing the administrator to retry during a quiet period.
CREATE UNIQUE INDEX import_execution_receipt_once
    ON import_task_results (task_id, execution_index)
    WHERE execution_index IS NOT NULL; -- hubuum-compat: bounded-transactional-index

ALTER TABLE import_task_results VALIDATE CONSTRAINT import_execution_receipt_fields;
