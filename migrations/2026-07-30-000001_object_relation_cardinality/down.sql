CREATE OR REPLACE FUNCTION enforce_class_relation_order()
RETURNS TRIGGER AS $$
DECLARE
    temp INT;
    temp_alias VARCHAR;
BEGIN
    IF NEW.from_hubuum_class_id > NEW.to_hubuum_class_id THEN
        temp := NEW.from_hubuum_class_id;
        NEW.from_hubuum_class_id := NEW.to_hubuum_class_id;
        NEW.to_hubuum_class_id := temp;
        temp_alias := NEW.forward_template_alias;
        NEW.forward_template_alias := NEW.reverse_template_alias;
        NEW.reverse_template_alias := temp_alias;
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

    SELECT from_hubuum_class_id, to_hubuum_class_id
    INTO relation_from_class_id, relation_to_class_id
    FROM hubuumclass_relation
    WHERE id = NEW.class_relation_id;

    IF (from_class_id != relation_from_class_id OR to_class_id != relation_to_class_id) AND
       (from_class_id != relation_to_class_id OR to_class_id != relation_from_class_id) THEN
        RAISE EXCEPTION 'Invalid object relation: objects do not match the specified class relation';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE hubuumclass_relation
    DROP CONSTRAINT hubuumclass_relation_from_max_relations_positive,
    DROP CONSTRAINT hubuumclass_relation_to_max_relations_positive,
    DROP COLUMN from_max_relations,
    DROP COLUMN to_max_relations;

ALTER TABLE hubuumclass_relation_history
    DROP COLUMN from_max_relations,
    DROP COLUMN to_max_relations;
