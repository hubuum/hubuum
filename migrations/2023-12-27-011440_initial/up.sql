    -- Your SQL goes here
    DROP TABLE IF EXISTS users CASCADE;
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR NOT NULL UNIQUE,
        password VARCHAR NOT NULL,
        email VARCHAR NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    DROP TABLE IF EXISTS groups CASCADE;
    CREATE TABLE groups (
        id SERIAL PRIMARY KEY,
        groupname VARCHAR NOT NULL UNIQUE,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    DROP TABLE IF EXISTS user_groups CASCADE;
    CREATE TABLE user_groups (
        user_id INT REFERENCES users (id) ON DELETE CASCADE NOT NULL,
        group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
        PRIMARY KEY (user_id, group_id),
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    DROP TABLE IF EXISTS tokens CASCADE;
    CREATE TABLE tokens (
        token VARCHAR NOT NULL UNIQUE,
        user_id INT REFERENCES users (id) ON DELETE CASCADE NOT NULL,
        issued TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (token)
    );

    DROP TABLE IF EXISTS namespaces CASCADE;
    CREATE TABLE namespaces (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    DROP TABLE IF EXISTS permissions CASCADE;
    CREATE TABLE permissions (
        id SERIAL PRIMARY KEY,
        namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
        group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
        has_read_namespace BOOLEAN NOT NULL,
        has_update_namespace BOOLEAN NOT NULL,
        has_delete_namespace BOOLEAN NOT NULL,
        has_delegate_namespace BOOLEAN NOT NULL,
        has_create_class BOOLEAN NOT NULL,
        has_read_class BOOLEAN NOT NULL,
        has_update_class BOOLEAN NOT NULL,
        has_delete_class BOOLEAN NOT NULL,
        has_create_object BOOLEAN NOT NULL,
        has_read_object BOOLEAN NOT NULL,
        has_update_object BOOLEAN NOT NULL,
        has_delete_object BOOLEAN NOT NULL,
        has_create_class_relation BOOLEAN NOT NULL,
        has_read_class_relation BOOLEAN NOT NULL,
        has_update_class_relation BOOLEAN NOT NULL,
        has_delete_class_relation BOOLEAN NOT NULL,
        has_create_object_relation BOOLEAN NOT NULL,
        has_read_object_relation BOOLEAN NOT NULL,
        has_update_object_relation BOOLEAN NOT NULL,
        has_delete_object_relation BOOLEAN NOT NULL,
        has_read_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_create_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_update_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_delete_template BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (namespace_id, group_id)
    );

    DROP TABLE IF EXISTS hubuumclass CASCADE;
    CREATE TABLE hubuumclass (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE,
        namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
        json_schema JSONB DEFAULT NULL,
        validate_schema BOOLEAN DEFAULT false NOT NULL,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    DROP TABLE IF EXISTS hubuumobject CASCADE;
    CREATE TABLE hubuumobject (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
        hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
        data JSONB DEFAULT '{}'::jsonb NOT NULL,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (name, hubuum_class_id)
    );

    -- A bidirectional relation between classes
    DROP TABLE IF EXISTS hubuumclass_relation CASCADE;
    CREATE TABLE hubuumclass_relation (
        id SERIAL PRIMARY KEY,
        from_hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
        to_hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (from_hubuum_class_id, to_hubuum_class_id)
    );

    -- A bidirectional relation between objects
    DROP TABLE IF EXISTS hubuumobject_relation CASCADE;
    CREATE TABLE hubuumobject_relation (
        id SERIAL PRIMARY KEY,
        from_hubuum_object_id INT REFERENCES hubuumobject (id) ON DELETE CASCADE NOT NULL,
        to_hubuum_object_id INT REFERENCES hubuumobject (id) ON DELETE CASCADE NOT NULL,    
        class_relation_id INT REFERENCES hubuumclass_relation (id) ON DELETE CASCADE NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (from_hubuum_object_id, to_hubuum_object_id)
    );



    -- Table to store report templates
    DROP TABLE IF EXISTS report_templates CASCADE;
    CREATE TABLE report_templates (
        id SERIAL PRIMARY KEY,
        namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
        name VARCHAR NOT NULL,
        description VARCHAR NOT NULL,
        content_type VARCHAR NOT NULL,
        template TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (namespace_id, name),
        CHECK (content_type IN ('text/plain', 'text/html', 'text/csv'))
    );

    DROP TABLE IF EXISTS tasks CASCADE;
    CREATE TABLE tasks (
        id SERIAL PRIMARY KEY,
        kind VARCHAR NOT NULL CHECK (kind IN ('import', 'report', 'export', 'reindex')),
        status VARCHAR NOT NULL CHECK (
            status IN (
                'queued',
                'validating',
                'running',
                'succeeded',
                'failed',
                'partially_succeeded',
                'cancelled'
            )
        ),
        submitted_by INT REFERENCES users (id) ON DELETE SET NULL,
        idempotency_key VARCHAR NULL,
        request_hash VARCHAR NULL,
        request_payload JSONB NULL,
        summary TEXT NULL,
        total_items INT NOT NULL DEFAULT 0,
        processed_items INT NOT NULL DEFAULT 0,
        success_items INT NOT NULL DEFAULT 0,
        failed_items INT NOT NULL DEFAULT 0,
        request_redacted_at TIMESTAMP NULL,
        started_at TIMESTAMP NULL,
        finished_at TIMESTAMP NULL,
        deleted_at TIMESTAMP NULL,
        deleted_by INT NULL REFERENCES users (id) ON DELETE SET NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (submitted_by, idempotency_key)
    );

    DROP TABLE IF EXISTS task_events CASCADE;
    CREATE TABLE task_events (
        id SERIAL PRIMARY KEY,
        task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL,
        event_type VARCHAR NOT NULL,
        message TEXT NOT NULL,
        data JSONB NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );

    DROP TABLE IF EXISTS import_task_results CASCADE;
    CREATE TABLE import_task_results (
        id SERIAL PRIMARY KEY,
        task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL,
        item_ref VARCHAR NULL,
        entity_kind VARCHAR NOT NULL,
        action VARCHAR NOT NULL,
        identifier TEXT NULL,
        outcome VARCHAR NOT NULL,
        error TEXT NULL,
        details JSONB NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );

    ----------------------
    ---- Indexes
    ----------------------

    ---- Users and groups
    CREATE INDEX idx_users_username ON users(username);
    CREATE INDEX idx_groups_groupname ON groups(groupname);
    CREATE INDEX idx_user_groups_user_id ON user_groups(user_id);
    CREATE INDEX idx_user_groups_group_id ON user_groups(group_id);

    ---- Namespaces and tokens
    CREATE INDEX idx_namespaces_name ON namespaces(name);
    CREATE INDEX idx_tokens_user_id ON tokens(user_id);

    ---- Classes and objects
    CREATE INDEX idx_hubuumclass_namespace_id ON hubuumclass(namespace_id);
    CREATE INDEX idx_hubuumobject_namespace_id ON hubuumobject(namespace_id);
    CREATE INDEX idx_hubuumobject_hubuum_class_id ON hubuumobject(hubuum_class_id);

    ---- Permissions
    CREATE INDEX idx_permissions_namespace_id ON permissions(namespace_id);
    CREATE INDEX idx_permissions_group_id ON permissions(group_id);

    ---- Relations
    CREATE INDEX idx_hubuumclass_relation_on_from_to ON hubuumclass_relation (from_hubuum_class_id, to_hubuum_class_id);
    CREATE INDEX idx_hubuumobject_relation_on_from_to ON hubuumobject_relation (from_hubuum_object_id, to_hubuum_object_id);
    CREATE INDEX idx_hubuumobject_relation_on_to ON hubuumobject_relation (to_hubuum_object_id);
    CREATE INDEX idx_hubuumobject_relation_class_relation_id ON hubuumobject_relation (class_relation_id);

    ---- Report templates
    CREATE INDEX idx_report_templates_namespace_id ON report_templates(namespace_id);

    ---- Search
    CREATE INDEX idx_hubuumobject_data_search
        ON hubuumobject
        USING GIN (jsonb_to_tsvector('simple', data, '["string"]'));

    ---- Tasks and imports
    CREATE INDEX idx_tasks_status_created_at ON tasks (status, created_at);
    CREATE INDEX idx_tasks_submitted_by ON tasks (submitted_by);
    CREATE INDEX idx_tasks_deleted_at ON tasks (deleted_at);
    CREATE INDEX idx_tasks_active_status ON tasks (deleted_at, status);
    CREATE INDEX idx_task_events_task_id_created_at ON task_events (task_id, created_at);
    CREATE INDEX idx_import_task_results_task_id_created_at ON import_task_results (task_id, created_at);

    ----------------------
    ---- Functions
    ----------------------

    -- Update the updated_at column whenever a row is updated
    CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    -- In relation tables, ensure that the from entry is always less than the to entry, this ensures
    -- that we don't need to check for both directions when querying the database
    CREATE OR REPLACE FUNCTION enforce_class_relation_order()
    RETURNS TRIGGER AS $$
    DECLARE
        temp INT;
    BEGIN
        IF NEW.from_hubuum_class_id > NEW.to_hubuum_class_id THEN
            -- Swap the IDs if they are in the wrong order
            temp := NEW.from_hubuum_class_id;
            NEW.from_hubuum_class_id := NEW.to_hubuum_class_id;
            NEW.to_hubuum_class_id := temp;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION enforce_object_relation_order()
    RETURNS TRIGGER AS $$
    DECLARE
        temp INT;
    BEGIN
        IF NEW.from_hubuum_object_id > NEW.to_hubuum_object_id THEN
            -- Swap the IDs if they are in the wrong order
            temp := NEW.from_hubuum_object_id;
            NEW.from_hubuum_object_id := NEW.to_hubuum_object_id;
            NEW.to_hubuum_object_id := temp;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Function to validate object relations
    CREATE OR REPLACE FUNCTION validate_object_relation()
    RETURNS TRIGGER AS $$
    DECLARE
        from_class_id INT;
        to_class_id INT;
        relation_from_class_id INT;
        relation_to_class_id INT;
    BEGIN
        -- Get class IDs for the objects
        IF NEW.from_hubuum_object_id = NEW.to_hubuum_object_id THEN
            RAISE EXCEPTION 'Invalid object relation: objects cannot be related to themselves';
        END IF;

        SELECT hubuum_class_id INTO from_class_id FROM hubuumobject WHERE id = NEW.from_hubuum_object_id;
        SELECT hubuum_class_id INTO to_class_id FROM hubuumobject WHERE id = NEW.to_hubuum_object_id;

        IF from_class_id = to_class_id THEN
            RAISE EXCEPTION 'Invalid object relation: objects cannot be related to the same classes';
        END IF;

        -- Get class IDs for the relation
        SELECT from_hubuum_class_id, to_hubuum_class_id 
        INTO relation_from_class_id, relation_to_class_id 
        FROM hubuumclass_relation 
        WHERE id = NEW.class_relation_id;

        -- Check if the objects match the class relation
        IF (from_class_id != relation_from_class_id OR to_class_id != relation_to_class_id) AND
        (from_class_id != relation_to_class_id OR to_class_id != relation_from_class_id) THEN
            RAISE EXCEPTION 'Invalid object relation: objects do not match the specified class relation';
        END IF;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Function to get objects that use a class relation. 
    CREATE OR REPLACE FUNCTION get_affected_objects(class1_id INT, class2_id INT)
    RETURNS TABLE (from_object_id INT, to_object_id INT) AS $$
    BEGIN
        RETURN QUERY
        SELECT r.id, r.from_hubuum_object_id, r.to_hubuum_object_id, r.created_at, r.updated_at
        FROM hubuumobject_relation r
        JOIN hubuumobject o1 ON r.from_hubuum_object_id = o1.id
        JOIN hubuumobject o2 ON r.to_hubuum_object_id = o2.id
        WHERE (o1.hubuum_class_id = class1_id AND o2.hubuum_class_id = class2_id)
        OR (o1.hubuum_class_id = class2_id AND o2.hubuum_class_id = class1_id);
    END;
    $$ LANGUAGE plpgsql;

    -- Function to cleanup invalid object relations from the hubuumobject_relation table
    -- after a class relation has been deleted. This function is called by a trigger.
    -- Without closure tables, we validate via recursive class traversal.
    CREATE OR REPLACE FUNCTION cleanup_invalid_object_relations()
    RETURNS TRIGGER AS $$
    BEGIN
        DELETE FROM hubuumobject_relation
        WHERE NOT EXISTS (
            WITH RECURSIVE class_reachability AS (
                -- Direct relation
                SELECT from_hubuum_class_id, to_hubuum_class_id, 1 as depth
                FROM hubuumclass_relation
                
                UNION ALL
                
                -- Transitive
                SELECT cr.from_hubuum_class_id, cr2.to_hubuum_class_id, cr.depth + 1
                FROM class_reachability cr
                JOIN hubuumclass_relation cr2 ON cr.to_hubuum_class_id = cr2.from_hubuum_class_id
                WHERE cr.depth < 100  -- Reasonable recursion depth limit
            )
            SELECT 1
            FROM hubuumobject o1, hubuumobject o2
            WHERE o1.id = hubuumobject_relation.from_hubuum_object_id
            AND o2.id = hubuumobject_relation.to_hubuum_object_id
            AND EXISTS (
                SELECT 1 FROM class_reachability
                WHERE from_hubuum_class_id = LEAST(o1.hubuum_class_id, o2.hubuum_class_id)
                AND to_hubuum_class_id = GREATEST(o1.hubuum_class_id, o2.hubuum_class_id)
            )
        );
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION get_transitively_linked_objects(
        start_object_id INT, 
        target_class_id INT,
        valid_namespace_ids INT[]
    )
    RETURNS TABLE (
        target_object_id INT,
        path INT[]
    ) AS $$
    DECLARE
        start_class_id INT;
    BEGIN
        -- Get the class ID of the start object
        SELECT hubuum_class_id INTO start_class_id 
        FROM hubuumobject 
        WHERE id = start_object_id;

        RETURN QUERY
        WITH RECURSIVE class_reachability AS (
            -- Direct class relation
            SELECT from_hubuum_class_id, to_hubuum_class_id, 1 as depth
            FROM hubuumclass_relation
            
            UNION ALL
            
            -- Transitive class relation
            SELECT cr.from_hubuum_class_id, cr2.to_hubuum_class_id, cr.depth + 1
            FROM class_reachability cr
            JOIN hubuumclass_relation cr2 ON cr.to_hubuum_class_id = cr2.from_hubuum_class_id
            WHERE cr.depth < 100
        ),
        transitive_relations AS (
            -- Base case: direct relations
            SELECT 
                or1.to_hubuum_object_id as object_id,
                ARRAY[start_object_id, or1.to_hubuum_object_id] as path
            FROM hubuumobject_relation or1
            JOIN hubuumobject o ON o.id = or1.to_hubuum_object_id
            WHERE or1.from_hubuum_object_id = start_object_id
            AND o.namespace_id = ANY(valid_namespace_ids)

            UNION ALL

            -- Recursive case
            SELECT 
                or2.to_hubuum_object_id,
                tr.path || or2.to_hubuum_object_id
            FROM transitive_relations tr
            JOIN hubuumobject_relation or2 ON tr.object_id = or2.from_hubuum_object_id
            JOIN hubuumobject o ON o.id = or2.to_hubuum_object_id
            WHERE o.namespace_id = ANY(valid_namespace_ids)
        )
        SELECT DISTINCT ON (tr.object_id)
            tr.object_id as target_object_id,
            tr.path
        FROM transitive_relations tr
        JOIN hubuumobject o ON o.id = tr.object_id
        -- Verify target's class is reachable from start's class
        WHERE o.hubuum_class_id = target_class_id
        AND EXISTS (
            SELECT 1 FROM class_reachability
            WHERE from_hubuum_class_id = LEAST(start_class_id, target_class_id)
            AND to_hubuum_class_id = GREATEST(start_class_id, target_class_id)
        );
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION get_bidirectionally_related_objects(
        start_object_id INT,
        valid_namespace_ids INT[],
        max_depth INT
    )
    RETURNS TABLE (
        ancestor_object_id INT,
        descendant_object_id INT,
        depth INT,
        path INT[],
        ancestor_name VARCHAR,
        descendant_name VARCHAR,
        ancestor_namespace_id INT,
        descendant_namespace_id INT,
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
        WITH RECURSIVE graph_walk AS (
            SELECT
                start_object_id AS ancestor_object_id,
                CASE
                    WHEN rel.from_hubuum_object_id = start_object_id THEN rel.to_hubuum_object_id
                    ELSE rel.from_hubuum_object_id
                END AS descendant_object_id,
                1 AS depth,
                ARRAY[
                    start_object_id,
                    CASE
                        WHEN rel.from_hubuum_object_id = start_object_id THEN rel.to_hubuum_object_id
                        ELSE rel.from_hubuum_object_id
                    END
                ] AS path
            FROM hubuumobject_relation rel
            JOIN hubuumobject target_object
              ON target_object.id = CASE
                    WHEN rel.from_hubuum_object_id = start_object_id THEN rel.to_hubuum_object_id
                    ELSE rel.from_hubuum_object_id
                END
            WHERE (rel.from_hubuum_object_id = start_object_id OR rel.to_hubuum_object_id = start_object_id)
              AND (max_depth IS NULL OR max_depth >= 1)
              AND target_object.namespace_id = ANY(valid_namespace_ids)

            UNION ALL

            SELECT
                graph_walk.ancestor_object_id,
                CASE
                    WHEN rel.from_hubuum_object_id = graph_walk.descendant_object_id THEN rel.to_hubuum_object_id
                    ELSE rel.from_hubuum_object_id
                END AS descendant_object_id,
                graph_walk.depth + 1,
                graph_walk.path || CASE
                    WHEN rel.from_hubuum_object_id = graph_walk.descendant_object_id THEN rel.to_hubuum_object_id
                    ELSE rel.from_hubuum_object_id
                END
            FROM graph_walk
            JOIN hubuumobject_relation rel
              ON rel.from_hubuum_object_id = graph_walk.descendant_object_id
              OR rel.to_hubuum_object_id = graph_walk.descendant_object_id
            JOIN hubuumobject target_object
              ON target_object.id = CASE
                    WHEN rel.from_hubuum_object_id = graph_walk.descendant_object_id THEN rel.to_hubuum_object_id
                    ELSE rel.from_hubuum_object_id
                END
            WHERE NOT (
                CASE
                    WHEN rel.from_hubuum_object_id = graph_walk.descendant_object_id THEN rel.to_hubuum_object_id
                    ELSE rel.from_hubuum_object_id
                END = ANY(graph_walk.path)
            )
              AND (max_depth IS NULL OR graph_walk.depth < max_depth)
              AND target_object.namespace_id = ANY(valid_namespace_ids)
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
            source_object.namespace_id AS ancestor_namespace_id,
            target_object.namespace_id AS descendant_namespace_id,
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
        WHERE source_object.namespace_id = ANY(valid_namespace_ids)
          AND target_object.namespace_id = ANY(valid_namespace_ids);
    $$ LANGUAGE sql STABLE;

    CREATE OR REPLACE FUNCTION get_bidirectionally_related_classes(
        start_class_id INT,
        valid_namespace_ids INT[],
        max_depth INT
    )
    RETURNS TABLE (
        ancestor_class_id INT,
        descendant_class_id INT,
        depth INT,
        path INT[],
        ancestor_name VARCHAR,
        descendant_name VARCHAR,
        ancestor_namespace_id INT,
        descendant_namespace_id INT,
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
        WITH RECURSIVE graph_walk AS (
            SELECT
                start_class_id AS ancestor_class_id,
                CASE
                    WHEN rel.from_hubuum_class_id = start_class_id THEN rel.to_hubuum_class_id
                    ELSE rel.from_hubuum_class_id
                END AS descendant_class_id,
                1 AS depth,
                ARRAY[
                    start_class_id,
                    CASE
                        WHEN rel.from_hubuum_class_id = start_class_id THEN rel.to_hubuum_class_id
                        ELSE rel.from_hubuum_class_id
                    END
                ] AS path
            FROM hubuumclass_relation rel
            JOIN hubuumclass target_class
              ON target_class.id = CASE
                    WHEN rel.from_hubuum_class_id = start_class_id THEN rel.to_hubuum_class_id
                    ELSE rel.from_hubuum_class_id
                END
                        WHERE (rel.from_hubuum_class_id = start_class_id OR rel.to_hubuum_class_id = start_class_id)
                            AND (max_depth IS NULL OR max_depth >= 1)
                            AND (
                                        COALESCE(cardinality(valid_namespace_ids), 0) = 0
                                        OR target_class.namespace_id = ANY(valid_namespace_ids)
                                    )

            UNION ALL

            SELECT
                graph_walk.ancestor_class_id,
                CASE
                    WHEN rel.from_hubuum_class_id = graph_walk.descendant_class_id THEN rel.to_hubuum_class_id
                    ELSE rel.from_hubuum_class_id
                END AS descendant_class_id,
                graph_walk.depth + 1,
                graph_walk.path || CASE
                    WHEN rel.from_hubuum_class_id = graph_walk.descendant_class_id THEN rel.to_hubuum_class_id
                    ELSE rel.from_hubuum_class_id
                END
            FROM graph_walk
            JOIN hubuumclass_relation rel
              ON rel.from_hubuum_class_id = graph_walk.descendant_class_id
              OR rel.to_hubuum_class_id = graph_walk.descendant_class_id
            JOIN hubuumclass target_class
              ON target_class.id = CASE
                    WHEN rel.from_hubuum_class_id = graph_walk.descendant_class_id THEN rel.to_hubuum_class_id
                    ELSE rel.from_hubuum_class_id
                END
                        WHERE NOT (
                CASE
                    WHEN rel.from_hubuum_class_id = graph_walk.descendant_class_id THEN rel.to_hubuum_class_id
                    ELSE rel.from_hubuum_class_id
                END = ANY(graph_walk.path)
            )
              AND (max_depth IS NULL OR graph_walk.depth < max_depth)
                            AND (
                                        COALESCE(cardinality(valid_namespace_ids), 0) = 0
                                        OR target_class.namespace_id = ANY(valid_namespace_ids)
                                    )
        ),
        deduped_walk AS (
            SELECT DISTINCT ON (descendant_class_id)
                ancestor_class_id,
                descendant_class_id,
                depth,
                path
            FROM graph_walk
            ORDER BY descendant_class_id ASC, depth ASC, path ASC
        )
        SELECT
            source_class.id AS ancestor_class_id,
            target_class.id AS descendant_class_id,
            deduped_walk.depth,
            deduped_walk.path,
            source_class.name AS ancestor_name,
            target_class.name AS descendant_name,
            source_class.namespace_id AS ancestor_namespace_id,
            target_class.namespace_id AS descendant_namespace_id,
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
        FROM deduped_walk
        JOIN hubuumclass source_class ON source_class.id = deduped_walk.ancestor_class_id
        JOIN hubuumclass target_class ON target_class.id = deduped_walk.descendant_class_id
                WHERE (
                                COALESCE(cardinality(valid_namespace_ids), 0) = 0
                                OR source_class.namespace_id = ANY(valid_namespace_ids)
                            )
                    AND (
                                COALESCE(cardinality(valid_namespace_ids), 0) = 0
                                OR target_class.namespace_id = ANY(valid_namespace_ids)
                            );
    $$ LANGUAGE sql STABLE;


    ----------------------
    ---- Triggers
    ----------------------

    -- Enforce order of to_class / to_object and from_class / from_object in class and
    -- object relations. These ensure that the from entry is always the smaller of the 
    -- two values.
    DROP TRIGGER IF EXISTS before_insert_or_update_class_relation ON hubuumclass_relation;
    CREATE TRIGGER before_insert_or_update_class_relation
    BEFORE INSERT OR UPDATE ON hubuumclass_relation
    FOR EACH ROW
    EXECUTE FUNCTION enforce_class_relation_order();

    DROP TRIGGER IF EXISTS before_insert_or_update_object_relation ON hubuumobject_relation;
    CREATE TRIGGER before_insert_or_update_object_relation
    BEFORE INSERT OR UPDATE ON hubuumobject_relation
    FOR EACH ROW
    EXECUTE FUNCTION enforce_object_relation_order();

    DROP TRIGGER IF EXISTS update_users_updated_at ON users;
    CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_groups_updated_at ON groups;
    CREATE TRIGGER update_groups_updated_at
    BEFORE UPDATE ON groups
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_user_groups_updated_at ON user_groups;
    CREATE TRIGGER update_user_groups_updated_at
    BEFORE UPDATE ON user_groups
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_namespaces_updated_at ON namespaces;
    CREATE TRIGGER update_namespaces_updated_at
    BEFORE UPDATE ON namespaces
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_permissions_updated_at ON permissions;
    CREATE TRIGGER update_permissions_updated_at
    BEFORE UPDATE ON permissions
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_hubuumclass_updated_at ON hubuumclass;
    CREATE TRIGGER update_hubuumclass_updated_at
    BEFORE UPDATE ON hubuumclass
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_hubuumobject_updated_at ON hubuumobject;
    CREATE TRIGGER update_hubuumobject_updated_at
    BEFORE UPDATE ON hubuumobject
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_hubuumclass_relation_updated_at ON hubuumclass_relation;
    CREATE TRIGGER update_hubuumclass_relation_updated_at
    BEFORE UPDATE ON hubuumclass_relation
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_hubuumobject_relation_updated_at ON hubuumobject_relation;
    CREATE TRIGGER update_hubuumobject_relation_updated_at
    BEFORE UPDATE ON hubuumobject_relation
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    -- Trigger to enforce valid object relations
    DROP TRIGGER IF EXISTS check_object_relation ON hubuumobject_relation;
    CREATE TRIGGER check_object_relation
    BEFORE INSERT OR UPDATE ON hubuumobject_relation
    FOR EACH ROW EXECUTE FUNCTION validate_object_relation();

    -- Trigger to cleanup invalid object relations
    DROP TRIGGER IF EXISTS cleanup_object_relations ON hubuumclass_relation;
    CREATE TRIGGER cleanup_object_relations
    AFTER DELETE ON hubuumclass_relation
    FOR EACH STATEMENT EXECUTE FUNCTION cleanup_invalid_object_relations();

    -- Trigger to update report_templates updated_at column
    DROP TRIGGER IF EXISTS update_report_templates_updated_at ON report_templates;
    CREATE TRIGGER update_report_templates_updated_at
    BEFORE UPDATE ON report_templates
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_tasks_updated_at ON tasks;
    CREATE TRIGGER update_tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();
