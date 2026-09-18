-- Discovery backfill and indexes are installed atomically with the new column.
-- Deploy during a quiet period: the table/index builds take write locks.
-- Bounded lock waits and statements abort and roll back the whole migration;
-- retry after resolving contention or provisioning capacity for retained history.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE tasks ADD COLUMN discovery_metadata jsonb;
CREATE FUNCTION valid_task_discovery(metadata jsonb, task_kind text) RETURNS boolean
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
ALTER TABLE tasks ADD CONSTRAINT tasks_discovery_metadata_shape CHECK (valid_task_discovery(discovery_metadata, kind)) NOT VALID;


-- Artifact insertion already occurs inside the fenced finalization transaction.
-- Keep summaries on the task when retention subsequently deletes the artifact.
CREATE FUNCTION retain_task_output_discovery() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE task_kind text;
DECLARE facts jsonb;
BEGIN
    task_kind := CASE TG_TABLE_NAME WHEN 'export_task_outputs' THEN 'export' ELSE 'backup' END;
    facts := jsonb_build_object('output', jsonb_build_object('state', 'produced',
        'expires_at', to_char(NEW.output_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
    IF task_kind = 'export' THEN
        facts := facts || jsonb_build_object('warning_count', NEW.warning_count, 'truncated', NEW.truncated);
    END IF;
    UPDATE tasks SET discovery_metadata = jsonb_build_object('version', 1, 'data',
        COALESCE(discovery_metadata->'data', jsonb_build_object('kind', task_kind)) || facts)
    WHERE id = NEW.task_id AND kind = task_kind
      -- Restoring an artifact must not touch the retained task's updated_at.
      -- Compare timestamps semantically: canonical backup JSON may use fewer
      -- fractional digits than to_char, while representing the same instant.
      AND (
        discovery_metadata->'data'->'output'->>'state' IS DISTINCT FROM 'produced'
        OR (discovery_metadata->'data'->'output'->>'expires_at')::timestamptz
           IS DISTINCT FROM NEW.output_expires_at AT TIME ZONE 'UTC'
        OR (task_kind = 'export' AND (
          discovery_metadata->'data'->'warning_count' IS DISTINCT FROM facts->'warning_count'
          OR discovery_metadata->'data'->'truncated' IS DISTINCT FROM facts->'truncated'
        ))
      );
    RETURN NEW;
END;
$$;
CREATE TRIGGER retain_export_discovery AFTER INSERT OR UPDATE ON export_task_outputs
    FOR EACH ROW EXECUTE FUNCTION retain_task_output_discovery();
CREATE TRIGGER retain_backup_discovery AFTER INSERT OR UPDATE ON backup_task_outputs
    FOR EACH ROW EXECUTE FUNCTION retain_task_output_discovery();

CREATE FUNCTION retain_task_terminal_discovery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind = 'import' AND NEW.discovery_metadata IS NOT NULL
       AND NEW.status IN ('succeeded', 'failed', 'partially_succeeded', 'cancelled') THEN
        NEW.discovery_metadata := jsonb_set(NEW.discovery_metadata, '{data,has_failed_items}', to_jsonb(NEW.failed_items > 0));
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER retain_task_terminal_discovery BEFORE INSERT OR UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION retain_task_terminal_discovery();

-- Retained schema work is authoritative even after request redaction.
UPDATE tasks t SET discovery_metadata = jsonb_build_object('version', 1, 'data',
    jsonb_build_object('kind', t.kind, 'class_id', w.class_id, 'schema_revision', w.schema_revision, 'work_kind', w.kind))
FROM schema_validation_work w WHERE w.task_id = t.id AND t.kind = 'schema_validation';
-- Output rows are authoritative about production and outcome summaries.
UPDATE tasks t SET discovery_metadata = jsonb_build_object('version', 1, 'data',
    jsonb_build_object('kind', t.kind, 'warning_count', o.warning_count, 'truncated', o.truncated,
      'output', jsonb_build_object('state', 'produced', 'expires_at', to_char(o.output_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))))
FROM export_task_outputs o WHERE o.task_id = t.id AND t.kind = 'export';
UPDATE tasks t SET discovery_metadata = jsonb_build_object('version', 1, 'data',
    jsonb_build_object('kind', t.kind, 'output', jsonb_build_object('state', 'produced',
      'expires_at', to_char(o.output_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))))
FROM backup_task_outputs o WHERE o.task_id = t.id AND t.kind = 'backup';

-- Remote artifacts retain subject identity, but not an object's original class
-- context. Never reconstruct that context from current object membership.
UPDATE tasks t SET discovery_metadata = jsonb_build_object('version', 1, 'data',
    jsonb_build_object('kind', t.kind, 'remote_target_id', r.target_id, 'target',
      CASE r.subject_type
       WHEN 'collection' THEN jsonb_build_object('type','collection','collection_id',r.subject_id)
       WHEN 'class' THEN jsonb_build_object('type','class','class_id',r.subject_id)
       WHEN 'object' THEN jsonb_build_object('type','object','object_id',r.subject_id)
       WHEN 'class_relation' THEN jsonb_build_object('type','class_relation','relation_id',r.subject_id)
       WHEN 'object_relation' THEN jsonb_build_object('type','object_relation','relation_id',r.subject_id)
      END))
FROM remote_call_results r WHERE r.task_id = t.id AND t.kind = 'remote_call' AND r.subject_id > 0;
UPDATE tasks SET discovery_metadata = jsonb_build_object('version', 1, 'data',
    jsonb_build_object('kind', kind, 'has_failed_items', failed_items > 0))
WHERE kind = 'import' AND status IN ('succeeded','failed','partially_succeeded','cancelled');

-- Recover only explicit fields from retained payloads. Invalid or irrecoverable
-- historical data remains unknown; no resource membership or query text is used.
WITH candidates AS (
 SELECT id, kind, jsonb_build_object('version', 1, 'data',
    CASE kind
      WHEN 'import' THEN jsonb_build_object('kind',kind,
        'dry_run',COALESCE(request_payload->'dry_run','false'::jsonb),
        'atomicity',COALESCE(NULLIF(request_payload->'mode'->'atomicity','null'::jsonb),'"strict"'::jsonb),
        'collision_policy',COALESCE(NULLIF(request_payload->'mode'->'collision_policy','null'::jsonb),'"abort"'::jsonb),
        'permission_policy',COALESCE(NULLIF(request_payload->'mode'->'permission_policy','null'::jsonb),'"abort"'::jsonb))
      WHEN 'backup' THEN jsonb_build_object('kind',kind,'include_history',COALESCE(request_payload->'include_history','true'::jsonb))
      WHEN 'reindex' THEN jsonb_build_object('kind',kind,'class_id',request_payload->'class_id','computation_revision',request_payload->'target_revision')
      WHEN 'schema_validation' THEN jsonb_build_object('kind',kind,'class_id',request_payload->'class_id','schema_revision',request_payload->'schema_revision','work_kind',request_payload->'kind')
      WHEN 'remote_call' THEN jsonb_build_object('kind',kind,'remote_target_id',request_payload->'target_id','target',request_payload->'subject')
      WHEN 'export' THEN jsonb_build_object('kind',kind,
        'scope_kind', request_payload->'export'->'scope'->'kind',
        'target', CASE request_payload->'export'->'scope'->>'kind'
          WHEN 'objects_in_class' THEN jsonb_build_object('type','class','class_id',request_payload->'export'->'scope'->'class_id')
          WHEN 'related_objects' THEN jsonb_build_object('type','object','class_id',request_payload->'export'->'scope'->'class_id','object_id',request_payload->'export'->'scope'->'object_id') END,
        'template_id',request_payload->'template_id',
        'missing_data_policy',request_payload->'export'->'missing_data_policy',
        'max_items',request_payload->'export'->'limits'->'max_items',
        'max_output_bytes',request_payload->'export'->'limits'->'max_output_bytes')
    END || (jsonb_strip_nulls(COALESCE(discovery_metadata->'data','{}'::jsonb)) - CASE WHEN kind = 'remote_call' AND request_payload->>'subject' IS NOT NULL THEN 'target' ELSE '__unused' END)) AS metadata
 FROM tasks WHERE request_payload IS NOT NULL
)
UPDATE tasks t SET discovery_metadata = c.metadata FROM candidates c
WHERE c.id = t.id AND valid_task_discovery(c.metadata,c.kind);

ALTER TABLE tasks VALIDATE CONSTRAINT tasks_discovery_metadata_shape;

CREATE INDEX tasks_discovery_class_idx ON tasks
    ((COALESCE(discovery_metadata->'data'->>'class_id', discovery_metadata->'data'->'target'->>'class_id')), id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX tasks_discovery_object_idx ON tasks
    ((discovery_metadata->'data'->'target'->>'object_id'), id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX tasks_discovery_collection_idx ON tasks
    ((discovery_metadata->'data'->'target'->>'collection_id'), id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX tasks_discovery_relation_idx ON tasks
    (((discovery_metadata->'data'->'target'->>'type') || ':' || (discovery_metadata->'data'->'target'->>'relation_id')), id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX tasks_discovery_remote_target_idx ON tasks
    ((discovery_metadata->'data'->>'remote_target_id'), id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX tasks_discovery_template_idx ON tasks
    ((discovery_metadata->'data'->>'template_id'), id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX schema_validation_work_history_idx ON schema_validation_work
    (class_id, schema_revision, kind, task_id); -- hubuum-compat: bounded-transactional-index
