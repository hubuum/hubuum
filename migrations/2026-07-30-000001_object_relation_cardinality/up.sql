ALTER TABLE hubuumclass_relation
    ADD COLUMN from_max_relations INT NULL,
    ADD COLUMN to_max_relations INT NULL,
    ADD CONSTRAINT hubuumclass_relation_from_max_relations_positive
        CHECK (from_max_relations IS NULL OR from_max_relations > 0),
    ADD CONSTRAINT hubuumclass_relation_to_max_relations_positive
        CHECK (to_max_relations IS NULL OR to_max_relations > 0);

ALTER TABLE hubuumclass_relation_history
    ADD COLUMN from_max_relations INT NULL,
    ADD COLUMN to_max_relations INT NULL;

CREATE OR REPLACE FUNCTION enforce_class_relation_order()
RETURNS TRIGGER AS $$
DECLARE
    temp INT;
    temp_alias VARCHAR;
    temp_limit INT;
BEGIN
    IF NEW.from_hubuum_class_id > NEW.to_hubuum_class_id THEN
        temp := NEW.from_hubuum_class_id;
        NEW.from_hubuum_class_id := NEW.to_hubuum_class_id;
        NEW.to_hubuum_class_id := temp;

        temp_alias := NEW.forward_template_alias;
        NEW.forward_template_alias := NEW.reverse_template_alias;
        NEW.reverse_template_alias := temp_alias;

        temp_limit := NEW.from_max_relations;
        NEW.from_max_relations := NEW.to_max_relations;
        NEW.to_max_relations := temp_limit;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_object_relation()
RETURNS TRIGGER AS $$
DECLARE
    from_class_id INT;
    to_class_id INT;
    relation_from_class_id INT;
    relation_to_class_id INT;
    relation_from_max_relations INT;
    relation_to_max_relations INT;
    relation_from_object_id INT;
    relation_to_object_id INT;
    current_relation_count BIGINT;
    cardinality_constraint_name CONSTANT TEXT :=
        'hubuumobject_relation_cardinality';
    cardinality_error_template CONSTANT TEXT :=
        'Object relation cardinality exceeded: object %s is limited to %s relations by class relation %s';
BEGIN
    IF NEW.from_hubuum_object_id = NEW.to_hubuum_object_id THEN
        RAISE EXCEPTION 'Invalid object relation: objects cannot be related to themselves';
    END IF;

    SELECT hubuum_class_id
    INTO from_class_id
    FROM hubuumobject
    WHERE id = NEW.from_hubuum_object_id;

    SELECT hubuum_class_id
    INTO to_class_id
    FROM hubuumobject
    WHERE id = NEW.to_hubuum_object_id;

    IF from_class_id = to_class_id THEN
        RAISE EXCEPTION 'Invalid object relation: objects cannot be related to the same classes';
    END IF;

    -- Keep the class relation definition stable while this trigger runs without
    -- serializing concurrent object relation inserts.
    SELECT
        from_hubuum_class_id,
        to_hubuum_class_id,
        from_max_relations,
        to_max_relations
    INTO
        relation_from_class_id,
        relation_to_class_id,
        relation_from_max_relations,
        relation_to_max_relations
    FROM hubuumclass_relation
    WHERE id = NEW.class_relation_id
    FOR SHARE;

    IF (from_class_id != relation_from_class_id OR to_class_id != relation_to_class_id) AND
       (from_class_id != relation_to_class_id OR to_class_id != relation_from_class_id) THEN
        RAISE EXCEPTION 'Invalid object relation: objects do not match the specified class relation';
    END IF;

    IF from_class_id = relation_from_class_id THEN
        relation_from_object_id := NEW.from_hubuum_object_id;
        relation_to_object_id := NEW.to_hubuum_object_id;
    ELSE
        relation_from_object_id := NEW.to_hubuum_object_id;
        relation_to_object_id := NEW.from_hubuum_object_id;
    END IF;

    -- Serialize only inserts that compete for the same bounded object. Locking
    -- in object-ID order prevents deadlocks when both sides are bounded.
    IF relation_from_max_relations IS NOT NULL OR
       relation_to_max_relations IS NOT NULL THEN
        PERFORM id
        FROM hubuumobject
        WHERE (
            relation_from_max_relations IS NOT NULL
            AND id = relation_from_object_id
        ) OR (
            relation_to_max_relations IS NOT NULL
            AND id = relation_to_object_id
        )
        ORDER BY id
        FOR UPDATE;
    END IF;

    IF relation_from_max_relations IS NOT NULL THEN
        SELECT COUNT(*)
        INTO current_relation_count
        FROM hubuumobject_relation
        WHERE class_relation_id = NEW.class_relation_id
          AND id != NEW.id
          AND (
              from_hubuum_object_id = relation_from_object_id
              OR to_hubuum_object_id = relation_from_object_id
          );

        IF current_relation_count >= relation_from_max_relations THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = cardinality_constraint_name,
                MESSAGE = format(
                    cardinality_error_template,
                    relation_from_object_id,
                    relation_from_max_relations,
                    NEW.class_relation_id
                );
        END IF;
    END IF;

    IF relation_to_max_relations IS NOT NULL THEN
        SELECT COUNT(*)
        INTO current_relation_count
        FROM hubuumobject_relation
        WHERE class_relation_id = NEW.class_relation_id
          AND id != NEW.id
          AND (
              from_hubuum_object_id = relation_to_object_id
              OR to_hubuum_object_id = relation_to_object_id
          );

        IF current_relation_count >= relation_to_max_relations THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = cardinality_constraint_name,
                MESSAGE = format(
                    cardinality_error_template,
                    relation_to_object_id,
                    relation_to_max_relations,
                    NEW.class_relation_id
                );
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
