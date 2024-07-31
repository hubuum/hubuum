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
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (namespace_id, group_id)
    );

    DROP TABLE IF EXISTS hubuumclass CASCADE;
    CREATE TABLE hubuumclass (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE,
        namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
        json_schema JSONB DEFAULT '{}'::jsonb NOT NULL,
        validate_schema BOOLEAN NOT NULL,
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
        UNIQUE (name, namespace_id)
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

    -- A table to store the transitive closure of class relations. If a relation exists
    -- between class A and B, and between B and C, then a relation between A and C is implied.
    -- This table is used to quickly determine if two classes are related and is updated by
    -- triggers.
    DROP TABLE IF EXISTS hubuumclass_closure CASCADE;
    CREATE TABLE hubuumclass_closure (
        ancestor_class_id INT REFERENCES hubuumclass(id) ON DELETE CASCADE,
        descendant_class_id INT REFERENCES hubuumclass(id) ON DELETE CASCADE,
        depth INT NOT NULL,
        path INT[] NOT NULL,
        PRIMARY KEY (ancestor_class_id, descendant_class_id, path),
        CHECK (ancestor_class_id < descendant_class_id)
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
    CREATE INDEX idx_hubuumobject_relation_class_relation_id ON hubuumobject_relation (class_relation_id);

    ---- Closure table
    CREATE INDEX idx_hubuumclass_closure_ancestor ON hubuumclass_closure(ancestor_class_id);
    CREATE INDEX idx_hubuumclass_closure_descendant ON hubuumclass_closure(descendant_class_id);
    -- Composite index on ancestor_class_id and descendant_class_id
    CREATE INDEX idx_hubuumclass_closure_ancestor_descendant ON hubuumclass_closure (ancestor_class_id, descendant_class_id);
    CREATE INDEX idx_hubuumclass_closure_path ON hubuumclass_closure USING GIN (path);

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

    CREATE OR REPLACE FUNCTION update_class_closure()
    RETURNS TRIGGER AS $$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            -- Insert the direct relation
            INSERT INTO hubuumclass_closure (ancestor_class_id, descendant_class_id, depth, path)
            VALUES (NEW.from_hubuum_class_id, NEW.to_hubuum_class_id, 1, ARRAY[NEW.from_hubuum_class_id, NEW.to_hubuum_class_id])
            ON CONFLICT DO NOTHING;

            -- Insert new transitive relations where the new class is the descendant
            INSERT INTO hubuumclass_closure (ancestor_class_id, descendant_class_id, depth, path)
            SELECT c1.ancestor_class_id, NEW.to_hubuum_class_id, c1.depth + 1, c1.path || NEW.to_hubuum_class_id
            FROM hubuumclass_closure c1
            WHERE c1.descendant_class_id = NEW.from_hubuum_class_id
            ON CONFLICT DO NOTHING;

            -- Insert new transitive relations where the new class is the ancestor
            INSERT INTO hubuumclass_closure (ancestor_class_id, descendant_class_id, depth, path)
            SELECT NEW.from_hubuum_class_id, c2.descendant_class_id, 1 + c2.depth, ARRAY[NEW.from_hubuum_class_id] || c2.path
            FROM hubuumclass_closure c2
            WHERE c2.ancestor_class_id = NEW.to_hubuum_class_id
            ON CONFLICT DO NOTHING;

            -- Insert new transitive relations that involve both the new ancestor and descendant
            INSERT INTO hubuumclass_closure (ancestor_class_id, descendant_class_id, depth, path)
            SELECT c1.ancestor_class_id, c2.descendant_class_id, c1.depth + 1 + c2.depth, c1.path || NEW.to_hubuum_class_id || c2.path
            FROM hubuumclass_closure c1
            JOIN hubuumclass_closure c2 ON c1.descendant_class_id = NEW.from_hubuum_class_id
                                    AND c2.ancestor_class_id = NEW.to_hubuum_class_id
            ON CONFLICT DO NOTHING;

        ELSIF TG_OP = 'DELETE' THEN
            -- Remove the direct relation
            DELETE FROM hubuumclass_closure
            WHERE ancestor_class_id = OLD.from_hubuum_class_id
            AND descendant_class_id = OLD.to_hubuum_class_id
            AND path = ARRAY[OLD.from_hubuum_class_id, OLD.to_hubuum_class_id];

            -- Remove paths where any class in the path no longer exists in hubuumclass
            -- This is the case when a class is deleted and we have a cascade delete propagating
            -- to the closure table.
            DELETE FROM hubuumclass_closure
            WHERE NOT EXISTS (
                SELECT 1 FROM hubuumclass
                WHERE id = ANY(hubuumclass_closure.path)
            );

        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    -- Function to check if classes are related
    CREATE OR REPLACE FUNCTION are_classes_related(class1_id INT, class2_id INT)
    RETURNS BOOLEAN AS $$
    BEGIN
        RETURN EXISTS (
            SELECT 1 FROM hubuumclass_closure
            WHERE ancestor_class_id = LEAST(class1_id, class2_id)
            AND descendant_class_id = GREATEST(class1_id, class2_id)
        );
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

CREATE TRIGGER check_object_relation
BEFORE INSERT OR UPDATE ON hubuumobject_relation
FOR EACH ROW EXECUTE FUNCTION validate_object_relation();

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
    CREATE OR REPLACE FUNCTION cleanup_invalid_object_relations()
    RETURNS TRIGGER AS $$
    BEGIN
        DELETE FROM hubuumobject_relation
        WHERE NOT EXISTS (
            SELECT 1
            FROM hubuumobject o1, hubuumobject o2, hubuumclass_closure cc
            WHERE o1.id = hubuumobject_relation.from_hubuum_object_id
            AND o2.id = hubuumobject_relation.to_hubuum_object_id
            AND cc.ancestor_class_id = LEAST(o1.hubuum_class_id, o2.hubuum_class_id)
            AND cc.descendant_class_id = GREATEST(o1.hubuum_class_id, o2.hubuum_class_id)
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
        WITH RECURSIVE transitive_relations AS (
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
        JOIN hubuumclass_closure cc ON cc.ancestor_class_id = start_class_id 
                                    AND cc.descendant_class_id = target_class_id
        WHERE o.hubuum_class_id = target_class_id;
    END;
    $$ LANGUAGE plpgsql;

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

    -- Trigger to maintain the closure table
    DROP TRIGGER IF EXISTS maintain_class_closure ON hubuumclass_relation;
    CREATE TRIGGER maintain_class_closure
    AFTER INSERT OR DELETE ON hubuumclass_relation
    FOR EACH ROW EXECUTE FUNCTION update_class_closure();

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