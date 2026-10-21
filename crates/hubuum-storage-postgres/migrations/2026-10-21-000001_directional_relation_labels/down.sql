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

ALTER TABLE hubuumclass_relation DROP COLUMN forward_label, DROP COLUMN reverse_label;
ALTER TABLE hubuumclass_relation_history DROP COLUMN forward_label, DROP COLUMN reverse_label;
