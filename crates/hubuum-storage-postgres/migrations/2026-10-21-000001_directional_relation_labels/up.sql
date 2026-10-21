SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE hubuumclass_relation
    ADD COLUMN forward_label VARCHAR(128),
    ADD COLUMN reverse_label VARCHAR(128);

ALTER TABLE hubuumclass_relation_history
    ADD COLUMN forward_label VARCHAR(128),
    ADD COLUMN reverse_label VARCHAR(128);

-- Match Rust's Unicode whitespace and control sets independently of database locale.
ALTER TABLE hubuumclass_relation
    ADD CONSTRAINT hubuumclass_relation_forward_label_valid
        CHECK (forward_label IS NULL OR (
            char_length(forward_label) BETWEEN 1 AND 128
            AND forward_label !~ U&'^[\0009-\000D\0020\0085\00A0\1680\2000-\200A\2028\2029\202F\205F\3000]|[\0009-\000D\0020\0085\00A0\1680\2000-\200A\2028\2029\202F\205F\3000]$|[\0001-\001F\007F-\009F]'
        )) NOT VALID,
    ADD CONSTRAINT hubuumclass_relation_reverse_label_valid
        CHECK (reverse_label IS NULL OR (
            char_length(reverse_label) BETWEEN 1 AND 128
            AND reverse_label !~ U&'^[\0009-\000D\0020\0085\00A0\1680\2000-\200A\2028\2029\202F\205F\3000]|[\0009-\000D\0020\0085\00A0\1680\2000-\200A\2028\2029\202F\205F\3000]$|[\0001-\001F\007F-\009F]'
        )) NOT VALID;

-- Existing rows have null labels; validate after installing the new-write checks.
ALTER TABLE hubuumclass_relation
    VALIDATE CONSTRAINT hubuumclass_relation_forward_label_valid,
    VALIDATE CONSTRAINT hubuumclass_relation_reverse_label_valid;

CREATE OR REPLACE FUNCTION enforce_class_relation_order()
RETURNS TRIGGER AS $$
DECLARE
    temp INT;
    temp_alias VARCHAR;
    temp_limit INT;
    temp_label VARCHAR;
BEGIN
    IF NEW.from_hubuum_class_id > NEW.to_hubuum_class_id THEN
        temp := NEW.from_hubuum_class_id;
        NEW.from_hubuum_class_id := NEW.to_hubuum_class_id;
        NEW.to_hubuum_class_id := temp;

        temp_label := NEW.forward_label;
        NEW.forward_label := NEW.reverse_label;
        NEW.reverse_label := temp_label;

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
