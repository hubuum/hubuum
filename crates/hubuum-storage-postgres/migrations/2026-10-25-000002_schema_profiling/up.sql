-- Bound lock waits and task-table constraint validation; retry after a timeout.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import','export','backup','reindex','remote_call','schema_validation','schema_profiling'));

CREATE OR REPLACE FUNCTION valid_task_discovery(metadata jsonb, task_kind text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE data jsonb := metadata->'data';
DECLARE target jsonb := data->'target';
DECLARE output jsonb := data->'output';
DECLARE allowed text[];
DECLARE field text;
DECLARE scalar text;
BEGIN
    IF metadata IS NULL THEN RETURN true; END IF;
    IF jsonb_typeof(metadata) IS DISTINCT FROM 'object' OR metadata->'version' IS DISTINCT FROM '1'::jsonb
       OR (metadata - ARRAY['version','data']) <> '{}'::jsonb OR jsonb_typeof(data) IS DISTINCT FROM 'object'
       OR data->>'kind' IS DISTINCT FROM task_kind THEN RETURN false; END IF;
    allowed := CASE task_kind
        WHEN 'import' THEN ARRAY['kind','dry_run','atomicity','collision_policy','permission_policy','has_failed_items']
        WHEN 'export' THEN ARRAY['kind','scope_kind','target','template_id','missing_data_policy','max_items','max_output_bytes','warning_count','truncated','output']
        WHEN 'backup' THEN ARRAY['kind','include_history','output']
        WHEN 'schema_profiling' THEN ARRAY['kind','class_id','output']
        WHEN 'schema_validation' THEN ARRAY['kind','class_id','schema_revision','work_kind']
        WHEN 'reindex' THEN ARRAY['kind','class_id','computation_revision']
        WHEN 'remote_call' THEN ARRAY['kind','remote_target_id','target']
        ELSE NULL END;
    IF allowed IS NULL OR (data - allowed) <> '{}'::jsonb THEN RETURN false; END IF;
    FOREACH field IN ARRAY ARRAY['dry_run','has_failed_items','truncated','include_history'] LOOP
        IF data->>field IS NOT NULL AND jsonb_typeof(data->field) <> 'boolean' THEN RETURN false; END IF;
    END LOOP;
    FOREACH field IN ARRAY ARRAY['class_id','remote_target_id','template_id','schema_revision','computation_revision','max_items','max_output_bytes','warning_count'] LOOP
        scalar := data->>field;
        IF scalar IS NOT NULL THEN
            IF jsonb_typeof(data->field) <> 'number' OR scalar !~ '^[0-9]+$' THEN RETURN false; END IF;
            IF scalar::numeric < (CASE WHEN field IN ('warning_count','computation_revision') THEN 0 ELSE 1 END) THEN RETURN false; END IF;
            IF field IN ('class_id','remote_target_id','template_id','warning_count') AND scalar::numeric > 2147483647 THEN RETURN false; END IF;
            IF field IN ('schema_revision','computation_revision') AND scalar::numeric > 9223372036854775807 THEN RETURN false; END IF;
            IF field IN ('max_items','max_output_bytes') AND scalar::numeric > 18446744073709551615 THEN RETURN false; END IF;
        END IF;
    END LOOP;
    IF (data->>'schema_revision' IS NOT NULL OR data->>'computation_revision' IS NOT NULL) AND data->>'class_id' IS NULL THEN RETURN false; END IF;
    IF data->>'atomicity' IS NOT NULL AND data->>'atomicity' NOT IN ('strict','best_effort') THEN RETURN false; END IF;
    IF data->>'collision_policy' IS NOT NULL AND data->>'collision_policy' NOT IN ('abort','overwrite') THEN RETURN false; END IF;
    IF data->>'permission_policy' IS NOT NULL AND data->>'permission_policy' NOT IN ('abort','continue') THEN RETURN false; END IF;
    IF data->>'missing_data_policy' IS NOT NULL AND data->>'missing_data_policy' NOT IN ('strict','null','omit') THEN RETURN false; END IF;
    IF data->>'work_kind' IS NOT NULL AND data->>'work_kind' NOT IN ('impact','revalidation') THEN RETURN false; END IF;
    IF data->>'scope_kind' IS NOT NULL AND data->>'scope_kind' NOT IN ('collections','classes','objects_in_class','class_relations','object_relations','related_objects') THEN RETURN false; END IF;
    IF target IS NOT NULL AND target <> 'null'::jsonb THEN
        IF jsonb_typeof(target) <> 'object' THEN RETURN false; END IF;
        allowed := CASE target->>'type'
            WHEN 'collection' THEN ARRAY['type','collection_id']
            WHEN 'class' THEN ARRAY['type','class_id']
            WHEN 'object' THEN ARRAY['type','class_id','object_id']
            WHEN 'class_relation' THEN ARRAY['type','relation_id']
            WHEN 'object_relation' THEN ARRAY['type','relation_id'] ELSE NULL END;
        IF allowed IS NULL OR target - allowed <> '{}'::jsonb THEN RETURN false; END IF;
        FOREACH field IN ARRAY allowed LOOP
            IF field = 'type' THEN CONTINUE; END IF;
            scalar := target->>field;
            IF target->>'type' = 'object' AND field = 'class_id' AND scalar IS NULL THEN CONTINUE; END IF;
            IF scalar IS NULL OR jsonb_typeof(target->field) <> 'number' OR scalar !~ '^[1-9][0-9]*$' OR scalar::numeric > 2147483647 THEN RETURN false; END IF;
        END LOOP;
        IF task_kind = 'export' AND (
            (data->>'scope_kind' = 'objects_in_class' AND target->>'type' = 'class') OR
            (data->>'scope_kind' = 'related_objects' AND target->>'type' = 'object' AND target->>'class_id' IS NOT NULL)
        ) IS NOT TRUE THEN RETURN false; END IF;
    END IF;
    IF output IS NOT NULL THEN
        IF jsonb_typeof(output) <> 'object' OR output->>'state' IS NULL THEN RETURN false; END IF;
        IF output->>'state' = 'produced' THEN
            IF output - ARRAY['state','expires_at'] <> '{}'::jsonb OR jsonb_typeof(output->'expires_at') IS DISTINCT FROM 'string'
                OR output->>'expires_at' !~ '(Z|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN false; END IF;
            PERFORM (output->>'expires_at')::timestamptz;
        ELSIF output->>'state' NOT IN ('unknown','not_produced') OR output - 'state' <> '{}'::jsonb THEN RETURN false;
        END IF;
    END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$;
CREATE TABLE schema_profile_outputs (
    task_id INTEGER PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    report JSONB NOT NULL CHECK (jsonb_typeof(report) = 'object'),
    output_expires_at TIMESTAMP NOT NULL
);
CREATE INDEX schema_profile_outputs_expiry ON schema_profile_outputs(output_expires_at);

CREATE FUNCTION retain_schema_profile_output() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE tasks SET discovery_metadata = jsonb_build_object('version', 1, 'data',
        COALESCE(discovery_metadata->'data', jsonb_build_object('kind','schema_profiling')) ||
        jsonb_build_object('output', jsonb_build_object('state','produced','expires_at',
            to_char(NEW.output_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))))
    WHERE id = NEW.task_id AND kind = 'schema_profiling';
    IF NOT FOUND THEN RAISE EXCEPTION 'Schema profile output requires a profiling task'; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER retain_schema_profile_output AFTER INSERT OR UPDATE ON schema_profile_outputs
FOR EACH ROW EXECUTE FUNCTION retain_schema_profile_output();
