-- Identity mappings have no cascading dependency on replaceable application
-- rows. They retain evidence for deleted resources and discarded restore paths.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

CREATE SCHEMA IF NOT EXISTS hubuum_control;
REVOKE ALL ON SCHEMA hubuum_control FROM PUBLIC;

CREATE TABLE hubuum_control.identity_backfill (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    cohort UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    completed BOOLEAN NOT NULL DEFAULT FALSE
);
INSERT INTO hubuum_control.identity_backfill DEFAULT VALUES;

CREATE TABLE hubuum_control.identity_mapping (
    resource_identity UUID PRIMARY KEY,
    family TEXT NOT NULL,
    cohort UUID,
    legacy_key JSONB,
    CHECK ((cohort IS NULL) = (legacy_key IS NULL)),
    UNIQUE (cohort, family, legacy_key)
);

-- A closed adapter-owned description, never supplied by an application caller.
CREATE FUNCTION hubuum_identity_layout()
RETURNS TABLE (relation_name TEXT, family TEXT, mode TEXT)
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $function$
    VALUES
        ('collections', 'collection', 'resource'),
        ('hubuumclass', 'class', 'resource'),
        ('hubuumobject', 'object', 'resource'),
        ('hubuumclass_relation', 'class_relation', 'resource'),
        ('hubuumobject_relation', 'object_relation', 'resource'),
        ('export_templates', 'export_template', 'resource'),
        ('remote_targets', 'remote_target', 'resource'),
        ('identity_scopes', 'identity_scope', 'resource'),
        ('groups', 'group', 'resource'),
        ('computed_field_definitions', 'computed_definition', 'resource'),
        ('event_sinks', 'event_sink', 'resource'),
        ('event_subscriptions', 'event_subscription', 'resource'),
        ('query_usage_declarations', 'query_usage_declaration', 'resource'),
        ('principals', 'principal', 'principal'),
        ('users', 'principal', 'principal_extension'),
        ('service_accounts', 'principal', 'principal_extension'),
        ('tasks', 'task', 'task'),
        ('collections_history', 'collection', 'history'),
        ('hubuumclass_history', 'class', 'history'),
        ('hubuumobject_history', 'object', 'history'),
        ('hubuumclass_relation_history', 'class_relation', 'history'),
        ('hubuumobject_relation_history', 'object_relation', 'history'),
        ('export_templates_history', 'export_template', 'history'),
        ('remote_targets_history', 'remote_target', 'history'),
        ('computed_field_history', 'class', 'definition_history'),
        ('class_schema_revisions', 'class', 'schema_revision'),
        ('class_schema_history', 'class', 'schema_history'),
        ('group_memberships', 'membership', 'association'),
        ('group_membership_sources', 'membership_source', 'association'),
        ('permissions', 'permission', 'association'),
        ('event_sink_collection_grants', 'event_sink_grant', 'association'),
        ('object_schema_evidence', 'object_schema_evidence', 'association'),
        ('import_task_results', 'import_result', 'association'),
        ('export_task_outputs', 'export_output', 'association'),
        ('backup_task_outputs', 'backup_output', 'association'),
        ('remote_call_results', 'remote_result', 'association'),
        ('schema_profile_outputs', 'schema_profile', 'association'),
        ('schema_validation_work', 'schema_validation', 'association'),
        ('schema_impact_findings', 'schema_impact', 'association'),
        ('schema_repair_reports', 'schema_repair', 'association'),
        ('events', 'event', 'association'),
        ('event_deliveries', 'event_delivery', 'association'),
        ('restore_jobs', 'restore_job', 'association'),
        ('restore_success_receipts', 'restore_receipt', 'association')
$function$;

CREATE FUNCTION hubuum_legacy_resource_identity(identity_family TEXT, source_row JSONB)
RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE
    source_cohort UUID;
    source_key JSONB;
    result UUID;
BEGIN
    IF source_row->>'id' IS NULL OR source_row->>'created_at' IS NULL THEN
        RAISE EXCEPTION 'Legacy resource identity requires an id and creation timestamp';
    END IF;
    SELECT cohort INTO STRICT source_cohort FROM hubuum_control.identity_backfill WHERE singleton;
    -- Creation instants are evidence, their JSON formatting is not. In
    -- particular, query-usage timestamps include the session timezone while
    -- the other resource families store timezone-free UTC timestamps.
    source_key := jsonb_build_array(source_row->'id',
        CASE WHEN identity_family = 'query_usage_declaration'
            THEN extract(epoch FROM (source_row->>'created_at')::TIMESTAMPTZ)
            ELSE extract(epoch FROM (source_row->>'created_at')::TIMESTAMP)
        END);
    SELECT resource_identity INTO result FROM hubuum_control.identity_mapping
     WHERE cohort = source_cohort AND family = identity_family AND legacy_key = source_key;
    IF result IS NOT NULL THEN RETURN result; END IF;
    INSERT INTO hubuum_control.identity_mapping (resource_identity, family, cohort, legacy_key)
    VALUES (gen_random_uuid(), identity_family, source_cohort, source_key)
    ON CONFLICT (cohort, family, legacy_key) DO NOTHING;
    SELECT resource_identity INTO STRICT result FROM hubuum_control.identity_mapping
     WHERE cohort = source_cohort AND family = identity_family AND legacy_key = source_key;
    RETURN result;
END
$function$;

CREATE FUNCTION hubuum_known_identity(identity UUID)
RETURNS JSONB LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $function$
    SELECT jsonb_build_object('state', 'known', 'identity', identity)
$function$;

CREATE FUNCTION hubuum_unresolved_identity(legacy_id BIGINT, legacy_revision BIGINT DEFAULT NULL)
RETURNS JSONB LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $function$
    SELECT jsonb_build_object('state', 'unresolved', 'legacy',
        jsonb_strip_nulls(jsonb_build_object('cohort', cohort, 'legacy_id', legacy_id, 'revision', legacy_revision)))
    FROM hubuum_control.identity_backfill WHERE singleton
$function$;

-- The JSON evidence is an internal logical contract. It is projected and
-- validated as crate-owned types at the adapter boundary, never as a pool or
-- arbitrary query builder in application code.
DO $columns$
DECLARE layout RECORD;
BEGIN
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() LOOP
        EXECUTE format('ALTER TABLE public.%I ADD COLUMN logical_identity JSONB', layout.relation_name);
    END LOOP;
END
$columns$;

-- Reference descriptors use logical backup field names. Only these explicitly
-- described fields are identities: arbitrary user JSON is never traversed.
CREATE FUNCTION hubuum_identity_reference_layout()
RETURNS TABLE (column_name TEXT, logical_name TEXT, family TEXT)
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $function$
    VALUES
        ('parent_collection_id', 'parent_collection_id', 'collection'),
        ('collection_id', 'collection_id', 'collection'),
        ('hubuum_class_id', 'class_id', 'class'),
        ('class_id', 'class_id', 'class'),
        ('from_hubuum_class_id', 'from_class_id', 'class'),
        ('from_class_id', 'from_class_id', 'class'),
        ('to_hubuum_class_id', 'to_class_id', 'class'),
        ('to_class_id', 'to_class_id', 'class'),
        ('from_hubuum_object_id', 'from_object_id', 'object'),
        ('from_object_id', 'from_object_id', 'object'),
        ('to_hubuum_object_id', 'to_object_id', 'object'),
        ('to_object_id', 'to_object_id', 'object'),
        ('object_id', 'object_id', 'object'),
        ('class_relation_id', 'class_relation_id', 'class_relation'),
        ('identity_scope_id', 'identity_scope_id', 'identity_scope'),
        ('source_scope_id', 'source_scope_id', 'identity_scope'),
        ('group_id', 'group_id', 'group'),
        ('owner_group_id', 'owner_group_id', 'group'),
        ('sink_id', 'sink_id', 'event_sink'),
        ('subscription_id', 'subscription_id', 'event_subscription'),
        ('target_id', 'target_id', 'remote_target'),
        ('principal_id', 'principal_id', 'principal'),
        ('actor_id', 'actor_principal_id', 'principal'),
        ('actor_user_id', 'actor_principal_id', 'principal'),
        ('actor_principal_id', 'actor_principal_id', 'principal'),
        ('initiator_user_id', 'initiator_principal_id', 'principal'),
        ('initiator_principal_id', 'initiator_principal_id', 'principal'),
        ('submitted_by', 'submitted_by', 'principal'),
        ('created_by', 'created_by', 'principal'),
        ('updated_by', 'updated_by', 'principal'),
        ('deleted_by', 'deleted_by', 'principal'),
        ('cancel_requested_by', 'cancel_requested_by', 'principal'),
        ('requested_by', 'requested_by', 'principal'),
        ('owner_user_id', 'owner_principal_id', 'principal'),
        ('task_id', 'task_id', 'task')
$function$;

CREATE FUNCTION hubuum_identity_reference(identity_family TEXT, legacy_id BIGINT, historical BOOLEAN)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE layout RECORD; target JSONB; reference JSONB;
BEGIN
    IF NOT historical THEN
        SELECT * INTO STRICT layout FROM public.hubuum_identity_layout()
        WHERE family = identity_family AND mode IN ('resource', 'principal', 'task');
        EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', layout.relation_name)
            INTO target USING legacy_id;
        IF target IS NOT NULL THEN
            reference := target #> '{logical_identity,subject,identity}';
            IF reference IS NULL THEN
                reference := public.hubuum_known_identity(
                    public.hubuum_legacy_resource_identity(identity_family, target));
            END IF;
        END IF;
    END IF;
    reference := COALESCE(reference, public.hubuum_unresolved_identity(legacy_id));
    IF identity_family IN ('principal', 'task') THEN
        RETURN jsonb_build_object('kind', identity_family, 'reference', reference);
    END IF;
    RETURN jsonb_build_object('kind', 'resource', 'family', identity_family, 'reference', reference);
END
$function$;

CREATE FUNCTION hubuum_identity_version(identity_family TEXT, legacy_id BIGINT, legacy_revision BIGINT, historical BOOLEAN)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE relation_name TEXT; target JSONB; reference JSONB;
BEGIN
    IF NOT historical THEN
        IF identity_family = 'schema' THEN
            relation_name := 'class_schema_revisions';
            SELECT to_jsonb(r) INTO target FROM public.class_schema_revisions r
                WHERE class_id = legacy_id AND revision = legacy_revision;
        ELSE
            SELECT l.relation_name INTO STRICT relation_name FROM public.hubuum_identity_layout() l
                WHERE l.family = identity_family AND mode = 'resource';
            EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1 AND revision = $2', relation_name)
                INTO target USING legacy_id, legacy_revision;
            IF target IS NULL AND EXISTS (SELECT 1 FROM public.hubuum_identity_layout() l
                WHERE l.family = identity_family AND l.mode = 'history') THEN
                -- A native audit append follows the mutation. Its before-version
                -- is already closed; a deletion may have removed the live row.
                EXECUTE format('SELECT to_jsonb(h) FROM public.%I h WHERE id = $1
                    AND revision = $2 AND op <> ''D'' ORDER BY history_id DESC LIMIT 1', relation_name || '_history')
                    INTO target USING legacy_id, legacy_revision;
                relation_name := relation_name || '_history';
            END IF;
        END IF;
        IF target IS NOT NULL THEN
            reference := public.hubuum_known_identity((COALESCE(NULLIF(target->'logical_identity', 'null'),
                public.hubuum_legacy_identity(relation_name, target)) #>> '{subject,version}')::UUID);
        END IF;
    END IF;
    RETURN jsonb_build_object('kind', 'version', 'reference', COALESCE(reference,
        public.hubuum_unresolved_identity(legacy_id, legacy_revision)));
END
$function$;

CREATE FUNCTION hubuum_capture_identity_references(source_row JSONB, previous_row JSONB, historical BOOLEAN)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE descriptor RECORD; result JSONB := '{}'; reference JSONB; path TEXT;
    embedded JSONB; member RECORD; prefix TEXT; nested JSONB; reference_family TEXT;
    pointer TEXT[]; parent_pointer TEXT[]; original JSONB;
BEGIN
    FOR descriptor IN SELECT * FROM public.hubuum_identity_reference_layout() LOOP
        path := '/' || descriptor.logical_name;
        IF source_row->>descriptor.column_name IS NULL THEN
            -- SET NULL removes the compatibility FK, not the captured actor or
            -- task attribution retained by this record.
            reference := previous_row #> ARRAY['logical_identity', 'references', path];
            IF descriptor.family IN ('principal', 'task') AND reference IS NOT NULL AND NOT (result ? path) THEN
                result := result || jsonb_build_object(path, reference);
            END IF;
            CONTINUE;
        END IF;
        reference := NULL;
        IF source_row->descriptor.column_name = previous_row->descriptor.column_name THEN
            reference := previous_row #> ARRAY['logical_identity', 'references', path];
        END IF;
        IF previous_row IS NULL THEN
            reference := COALESCE(reference, source_row #> ARRAY['logical_identity', 'references', path]);
        END IF;
        IF reference IS NULL THEN
            reference := public.hubuum_identity_reference(
                descriptor.family, (source_row->>descriptor.column_name)::BIGINT, historical);
        END IF;
        result := result || jsonb_build_object(path, reference);
    END LOOP;
    IF source_row->>'schema_revision' IS NOT NULL THEN
        result := result || jsonb_build_object('/schema_revision', public.hubuum_identity_version('schema',
            (source_row->>'class_id')::BIGINT, (source_row->>'schema_revision')::BIGINT, historical));
    END IF;
    IF source_row->>'object_revision' IS NOT NULL THEN
        result := result || jsonb_build_object('/object_revision', public.hubuum_identity_version('object',
            (source_row->>'object_id')::BIGINT, (source_row->>'object_revision')::BIGINT, historical));
    END IF;
    IF jsonb_typeof(source_row #> '{include,related_objects}') = 'object' THEN
        FOR member IN SELECT * FROM jsonb_each(source_row #> '{include,related_objects}') LOOP
            prefix := '/include/related_objects/' || replace(replace(member.key, '~', '~0'), '/', '~1');
            FOR descriptor IN SELECT * FROM (VALUES ('class_id', 'class'), ('class_relation_id', 'class_relation')) d(field, family) LOOP
                IF member.value->>descriptor.field IS NOT NULL THEN
                    result := result || jsonb_build_object(prefix || '/' || descriptor.field,
                        public.hubuum_identity_reference(descriptor.family, (member.value->>descriptor.field)::BIGINT, historical));
                END IF;
            END LOOP;
        END LOOP;
    END IF;
    IF jsonb_typeof(source_row->'definitions') = 'array' THEN
        FOR member IN SELECT value, ordinality FROM jsonb_array_elements(source_row->'definitions') WITH ORDINALITY LOOP
            prefix := '/definitions/' || (member.ordinality - 1)::TEXT;
            result := result || jsonb_build_object(
                prefix || '/id', public.hubuum_identity_reference('computed_definition', (member.value->>'id')::BIGINT, historical),
                prefix || '/class_id', public.hubuum_identity_reference('class', (member.value->>'class_id')::BIGINT, historical),
                prefix || '/revision', public.hubuum_identity_version('computed_definition',
                    (member.value->>'id')::BIGINT, (member.value->>'revision')::BIGINT, historical));
        END LOOP;
    END IF;
    IF jsonb_typeof(source_row->'snapshot') = 'object' THEN
        embedded := source_row->'snapshot';
        nested := public.hubuum_capture_identity_references(embedded, NULL, historical);
        FOR member IN SELECT * FROM jsonb_each(nested) LOOP
            result := result || jsonb_build_object('/snapshot' || member.key, member.value);
        END LOOP;
        IF embedded->>'class_id' IS NOT NULL AND embedded->>'revision' IS NOT NULL THEN
            reference := embedded #> '{logical_identity,subject,version}';
            reference := CASE WHEN reference IS NULL THEN
                public.hubuum_identity_version('schema', (embedded->>'class_id')::BIGINT,
                    (embedded->>'revision')::BIGINT, TRUE)
                ELSE jsonb_build_object('kind', 'version', 'reference', jsonb_build_object('state', 'known', 'identity', reference)) END;
            result := result || jsonb_build_object('/snapshot/revision', reference);
        END IF;
    END IF;
    IF jsonb_typeof(source_row #> '{discovery_metadata,data}') = 'object' THEN
        embedded := source_row #> '{discovery_metadata,data}';
        nested := public.hubuum_capture_identity_references(embedded, NULL, historical);
        FOR member IN SELECT * FROM jsonb_each(nested) LOOP
            result := result || jsonb_build_object('/discovery_metadata/data' || member.key, member.value);
        END LOOP;
        FOR descriptor IN SELECT * FROM (VALUES ('template_id', 'export_template'), ('remote_target_id', 'remote_target')) d(field, family) LOOP
            IF embedded->>descriptor.field IS NOT NULL THEN
                result := result || jsonb_build_object('/discovery_metadata/data/' || descriptor.field,
                    public.hubuum_identity_reference(descriptor.family, (embedded->>descriptor.field)::BIGINT, historical));
            END IF;
        END LOOP;
        IF jsonb_typeof(embedded->'target') = 'object' THEN
            nested := public.hubuum_capture_identity_references(embedded->'target', NULL, historical);
            FOR member IN SELECT * FROM jsonb_each(nested) LOOP
                result := result || jsonb_build_object('/discovery_metadata/data/target' || member.key, member.value);
            END LOOP;
            reference_family := embedded #>> '{target,type}';
            IF reference_family IN ('class_relation', 'object_relation') THEN
                result := result || jsonb_build_object('/discovery_metadata/data/target/relation_id',
                    public.hubuum_identity_reference(reference_family, (embedded #>> '{target,relation_id}')::BIGINT, historical));
            END IF;
        END IF;
    END IF;
    FOR descriptor IN SELECT * FROM (VALUES ('entity_id', 'entity_type'), ('subject_id', 'subject_type')) d(field, kind) LOOP
        IF source_row->>descriptor.field IS NULL THEN CONTINUE; END IF;
        reference_family := CASE source_row->>descriptor.kind
            WHEN 'collection' THEN 'collection' WHEN 'class' THEN 'class' WHEN 'class_schema' THEN 'class'
            WHEN 'object' THEN 'object' WHEN 'object_validation' THEN 'object'
            WHEN 'class_relation' THEN 'class_relation' WHEN 'object_relation' THEN 'object_relation'
            WHEN 'group' THEN 'group' WHEN 'user' THEN 'principal' WHEN 'service_account' THEN 'principal'
            WHEN 'export_template' THEN 'export_template' WHEN 'remote_target' THEN 'remote_target'
            WHEN 'task' THEN 'task' WHEN 'event_sink' THEN 'event_sink' WHEN 'event_subscription' THEN 'event_subscription'
            WHEN 'computed_field_definition' THEN 'computed_definition' WHEN 'query_usage_declaration' THEN 'query_usage_declaration'
            ELSE NULL END;
        IF reference_family IS NULL THEN CONTINUE; END IF;
        reference := public.hubuum_identity_reference(reference_family, (source_row->>descriptor.field)::BIGINT, historical);
        IF NOT historical AND reference #>> '{reference,state}' = 'unresolved' THEN
            SELECT l.relation_name INTO prefix FROM public.hubuum_identity_layout() l WHERE l.family = reference_family AND l.mode = 'history';
            IF prefix IS NOT NULL THEN
                EXECUTE format('SELECT logical_identity #> ''{subject,identity}'' FROM public.%I WHERE id = $1 ORDER BY history_id DESC LIMIT 1', prefix)
                    INTO embedded USING (source_row->>descriptor.field)::BIGINT;
                IF embedded IS NOT NULL THEN reference := jsonb_set(reference, '{reference}', embedded); END IF;
            END IF;
        END IF;
        result := result || jsonb_build_object('/' || descriptor.field, reference);
        IF descriptor.field = 'entity_id' AND reference_family NOT IN ('principal', 'task') THEN
            FOREACH path IN ARRAY ARRAY['before_revision', 'after_revision'] LOOP
                IF source_row->>path IS NOT NULL THEN
                    result := result || jsonb_build_object('/' || path, public.hubuum_identity_version(reference_family,
                        (source_row->>'entity_id')::BIGINT, (source_row->>path)::BIGINT, historical));
                END IF;
            END LOOP;
        END IF;
    END LOOP;
    IF jsonb_typeof(source_row->'event_id') = 'number' THEN
        reference := NULL;
        IF NOT historical THEN
            SELECT public.hubuum_known_identity(e.event_id) INTO reference FROM public.events e WHERE id = (source_row->>'event_id')::BIGINT;
        END IF;
        result := result || jsonb_build_object('/event_id', jsonb_build_object('kind', 'event', 'reference',
            COALESCE(reference, public.hubuum_unresolved_identity((source_row->>'event_id')::BIGINT))));
    END IF;
    IF source_row->>'entity_type' = 'restore' AND source_row #>> '{metadata,restore_job_id}' IS NOT NULL THEN
        reference := NULL;
        IF NOT historical THEN
            SELECT j.logical_identity #> '{references,/requested_by}' INTO reference
                FROM public.restore_jobs j WHERE id = (source_row #>> '{metadata,restore_job_id}')::BIGINT;
        END IF;
        IF reference IS NULL AND source_row #>> '{metadata,initiated_by,principal_id}' IS NOT NULL THEN
            reference := public.hubuum_identity_reference('principal', (source_row #>> '{metadata,initiated_by,principal_id}')::BIGINT, TRUE);
        END IF;
        IF reference IS NOT NULL THEN
            result := result || jsonb_build_object('/metadata/initiated_by/principal_id', reference);
        END IF;
    END IF;
    IF source_row->>'entity_type' IS NOT NULL AND jsonb_typeof(source_row->'metadata') = 'object' THEN
        embedded := source_row->'metadata';
        FOR descriptor IN SELECT * FROM public.hubuum_identity_reference_layout() LOOP
            IF jsonb_typeof(embedded->descriptor.column_name) = 'number' THEN
                result := result || jsonb_build_object('/metadata/' || descriptor.column_name,
                    public.hubuum_identity_reference(descriptor.family, (embedded->>descriptor.column_name)::BIGINT, historical));
            END IF;
        END LOOP;
        IF jsonb_typeof(embedded->'related_collection_ids') = 'array' THEN
            FOR member IN SELECT value, ordinality FROM jsonb_array_elements(embedded->'related_collection_ids') WITH ORDINALITY LOOP
                IF jsonb_typeof(member.value) = 'number' THEN
                    result := result || jsonb_build_object('/metadata/related_collection_ids/' || (member.ordinality - 1)::TEXT,
                        public.hubuum_identity_reference('collection', member.value::BIGINT, historical));
                END IF;
            END LOOP;
        END IF;
        FOREACH path IN ARRAY ARRAY['schema', 'before_schema'] LOOP
            IF jsonb_typeof(embedded->path) = 'object' AND embedded #>> ARRAY[path, 'class_id'] IS NOT NULL THEN
                result := result || jsonb_build_object('/metadata/' || path || '/class_id',
                    public.hubuum_identity_reference('class', (embedded #>> ARRAY[path, 'class_id'])::BIGINT, historical),
                    '/metadata/' || path || '/revision', public.hubuum_identity_version('schema',
                        (embedded #>> ARRAY[path, 'class_id'])::BIGINT, (embedded #>> ARRAY[path, 'revision'])::BIGINT, historical));
            END IF;
        END LOOP;
        FOREACH path IN ARRAY ARRAY['schema_revision', 'before_schema_revision'] LOOP
            IF jsonb_typeof(embedded->path) = 'number' THEN
                result := result || jsonb_build_object('/metadata/' || path, public.hubuum_identity_version('schema',
                    COALESCE(embedded->>'class_id', source_row->>'entity_id')::BIGINT, (embedded->>path)::BIGINT, historical));
            END IF;
        END LOOP;
        IF jsonb_typeof(embedded->'object_revision') = 'number' THEN
            result := result || jsonb_build_object('/metadata/object_revision', public.hubuum_identity_version('object',
                (source_row->>'entity_id')::BIGINT, (embedded->>'object_revision')::BIGINT, historical));
        END IF;
        IF jsonb_typeof(embedded->'dependent_rebuild_task_id') = 'number' THEN
            result := result || jsonb_build_object('/metadata/dependent_rebuild_task_id',
                public.hubuum_identity_reference('task', (embedded->>'dependent_rebuild_task_id')::BIGINT, historical));
        END IF;
    END IF;
    -- Only changed dependencies may be recaptured. In particular, a task's
    -- terminal-status update must not rebind unchanged discovery metadata to
    -- integers that happen to exist after restore. Nested snapshots carry their
    -- own captured evidence when first copied into history.
    original := COALESCE(previous_row, source_row);
    FOR member IN SELECT * FROM jsonb_each(COALESCE(original #> '{logical_identity,references}', '{}')) LOOP
        IF member.key NOT LIKE '/definitions/%'
           AND member.key NOT LIKE '/include/%'
           AND member.key NOT LIKE '/snapshot/%'
           AND member.key NOT LIKE '/discovery_metadata/%'
           AND member.key NOT LIKE '/metadata/%'
           AND member.key NOT IN ('/schema_revision', '/object_revision', '/entity_id', '/subject_id', '/event_id', '/before_revision', '/after_revision') THEN CONTINUE; END IF;
        SELECT array_agg(replace(replace(token, '~1', '/'), '~0', '~') ORDER BY ordinal)
          INTO pointer FROM unnest(string_to_array(substring(member.key FROM 2), '/')) WITH ORDINALITY AS p(token, ordinal);
        IF (source_row #> pointer) IS DISTINCT FROM (original #> pointer) THEN CONTINUE; END IF;
        IF member.value->>'kind' = 'version' THEN
            parent_pointer := pointer[1:cardinality(pointer)-1];
            IF (source_row #> (parent_pointer || ARRAY['id'])) IS DISTINCT FROM (original #> (parent_pointer || ARRAY['id']))
               OR (source_row #> (parent_pointer || ARRAY['class_id'])) IS DISTINCT FROM (original #> (parent_pointer || ARRAY['class_id']))
               OR (source_row #> (parent_pointer || ARRAY['object_id'])) IS DISTINCT FROM (original #> (parent_pointer || ARRAY['object_id'])) THEN CONTINUE; END IF;
        END IF;
        result := result || jsonb_build_object(member.key, member.value);
    END LOOP;
    RETURN result;
END
$function$;

-- Legacy version assignment is also authoritative and retry-safe. The key is
-- a retained version's own identifier, never a reusable resource revision alone.
CREATE TABLE hubuum_control.identity_versions (
    version_identity UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cohort UUID NOT NULL,
    family TEXT NOT NULL,
    legacy_key JSONB NOT NULL,
    UNIQUE (cohort, family, legacy_key)
);

CREATE FUNCTION hubuum_legacy_version_identity(identity_family TEXT, version_key JSONB)
RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE source_cohort UUID; result UUID;
BEGIN
    SELECT cohort INTO STRICT source_cohort FROM hubuum_control.identity_backfill WHERE singleton;
    INSERT INTO hubuum_control.identity_versions (cohort, family, legacy_key)
    VALUES (source_cohort, identity_family, version_key)
    ON CONFLICT (cohort, family, legacy_key) DO NOTHING;
    SELECT version_identity INTO STRICT result FROM hubuum_control.identity_versions
    WHERE cohort = source_cohort AND family = identity_family AND legacy_key = version_key;
    RETURN result;
END
$function$;

CREATE FUNCTION hubuum_legacy_identity(relation_name TEXT, source_row JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE layout RECORD; subject JSONB; reference JSONB; version UUID; retained_history JSONB;
BEGIN
    IF source_row->'logical_identity' IS NOT NULL AND source_row->'logical_identity' <> 'null' THEN
        RETURN source_row->'logical_identity';
    END IF;
    SELECT * INTO STRICT layout FROM public.hubuum_identity_layout() l WHERE l.relation_name = $1;
    IF layout.mode IN ('resource', 'history', 'principal', 'task') THEN
        reference := public.hubuum_known_identity(
            public.hubuum_legacy_resource_identity(layout.family, source_row));
        IF layout.mode IN ('principal', 'task') THEN
            subject := jsonb_build_object('kind', layout.mode, 'identity', reference);
        ELSE
            -- The live projection and its open historical version share the
            -- version UUID. Closed versions use their own history identifier.
            IF layout.mode = 'resource' AND EXISTS (
                SELECT 1 FROM public.hubuum_identity_layout() h
                WHERE h.family = layout.family AND h.mode = 'history'
            ) THEN
                EXECUTE format('SELECT to_jsonb(h) FROM public.%I h WHERE id = $1
                    AND created_at = $2::timestamp AND valid_to IS NULL ORDER BY history_id DESC LIMIT 1',
                    layout.relation_name || '_history') INTO retained_history
                    USING (source_row->>'id')::INTEGER, source_row->>'created_at';
            END IF;
            IF retained_history IS NOT NULL THEN
                version := (public.hubuum_legacy_identity(layout.relation_name || '_history', retained_history)
                    #>> '{subject,version}')::UUID;
            ELSE
                version := public.hubuum_legacy_version_identity(layout.relation_name,
                    jsonb_build_array(reference, COALESCE(source_row->'history_id', source_row->'revision')));
            END IF;
            subject := jsonb_build_object('kind', 'resource', 'identity', reference, 'version', version);
        END IF;
    ELSIF layout.mode = 'principal_extension' THEN
        reference := public.hubuum_identity_reference('principal', (source_row->>'id')::BIGINT, FALSE);
        subject := jsonb_build_object('kind', 'principal', 'identity', reference->'reference');
    ELSIF layout.mode IN ('definition_history', 'schema_revision', 'schema_history') THEN
        subject := jsonb_build_object('kind', 'resource',
            'identity', public.hubuum_unresolved_identity((source_row->>'class_id')::BIGINT),
            'version', public.hubuum_legacy_version_identity(layout.relation_name,
                jsonb_build_array(source_row->'class_id', source_row->'revision',
                    source_row->'history_id', source_row->'id')));
    ELSIF layout.family = 'event' THEN
        subject := jsonb_build_object('kind', 'event', 'identity', source_row->'event_id');
    END IF;
    RETURN jsonb_build_object('subject', subject, 'references',
        public.hubuum_capture_identity_references(source_row, NULL, TRUE));
END
$function$;

CREATE FUNCTION hubuum_manage_logical_identity()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE
    layout RECORD;
    source_row JSONB := to_jsonb(NEW);
    previous_row JSONB := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END;
    evidence JSONB;
    subject JSONB;
    class_reference JSONB;
    trusted_restore BOOLEAN := current_setting('hubuum.restore_identities', TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user, current_user, 'MEMBER');
BEGIN
    IF trusted_restore THEN
        IF NEW.logical_identity IS NULL THEN
            RAISE EXCEPTION 'Restore requires upgraded logical identity evidence' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO STRICT layout FROM public.hubuum_identity_layout() WHERE relation_name = TG_TABLE_NAME;
    IF TG_OP = 'INSERT' AND NEW.logical_identity IS NOT NULL
        AND layout.mode NOT IN ('history', 'schema_history') THEN
        IF TG_TABLE_NAME = 'restore_success_receipts' AND pg_has_role(session_user, current_user, 'MEMBER') THEN
            -- The executor copies the pre-replacement requester evidence from
            -- its retained stage. It must not resolve that integer after restore.
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'New records cannot supply logical identity evidence' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.logical_identity IS DISTINCT FROM OLD.logical_identity THEN
            RAISE EXCEPTION 'Logical identity evidence cannot be supplied by an update' USING ERRCODE = '23514';
        END IF;
        evidence := COALESCE(OLD.logical_identity, public.hubuum_legacy_identity(TG_TABLE_NAME, previous_row));
        previous_row := jsonb_set(previous_row, '{logical_identity}', evidence);
        subject := evidence->'subject';
        IF layout.mode = 'resource'
            AND (source_row - 'logical_identity') IS DISTINCT FROM (previous_row - 'logical_identity') THEN
            subject := jsonb_set(subject, '{version}', to_jsonb(gen_random_uuid()));
        END IF;
    ELSIF layout.mode IN ('resource', 'principal', 'task') THEN
        IF NEW.logical_identity IS NOT NULL THEN
            RAISE EXCEPTION 'New resources must mint a fresh logical identity' USING ERRCODE = '23514';
        END IF;
        subject := jsonb_build_object('kind', CASE WHEN layout.mode = 'resource' THEN 'resource' ELSE layout.mode END,
            'identity', public.hubuum_known_identity(gen_random_uuid()));
        IF layout.mode = 'resource' THEN
            subject := subject || jsonb_build_object('version', gen_random_uuid());
        END IF;
    ELSIF layout.mode = 'history' THEN
        subject := NEW.logical_identity->'subject';
        IF subject IS NULL THEN
            -- Deletion can race a bounded backfill before its live row is visited.
            subject := public.hubuum_legacy_identity(TG_TABLE_NAME, source_row)->'subject';
        END IF;
        IF NEW.op = 'D' THEN
            subject := jsonb_set(subject, '{version}', to_jsonb(gen_random_uuid()));
        END IF;
    ELSIF layout.mode = 'principal_extension' THEN
        class_reference := public.hubuum_identity_reference('principal', (source_row->>'id')::BIGINT, FALSE);
        subject := jsonb_build_object('kind', 'principal', 'identity', class_reference->'reference');
    ELSIF layout.mode IN ('definition_history', 'schema_revision', 'schema_history') THEN
        class_reference := public.hubuum_identity_reference('class', (source_row->>'class_id')::BIGINT, FALSE);
        subject := jsonb_build_object('kind', 'resource', 'identity',
            COALESCE(source_row #> '{snapshot,logical_identity,subject,identity}', class_reference->'reference'),
            'version', gen_random_uuid());
    ELSIF layout.family = 'event' THEN
        subject := jsonb_build_object('kind', 'event', 'identity', source_row->'event_id');
    END IF;
    -- Closure changes neither the captured references nor the historical UUID.
    IF TG_OP = 'UPDATE' AND layout.mode IN ('history', 'definition_history', 'schema_history') THEN
        NEW.logical_identity := evidence;
    ELSE
        NEW.logical_identity := jsonb_build_object('subject', subject, 'references',
            public.hubuum_capture_identity_references(source_row, previous_row, FALSE));
    END IF;
    RETURN NEW;
END
$function$;

DO $triggers$
DECLARE layout RECORD; trigger_name TEXT;
BEGIN
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() LOOP
        -- Schema history is emitted by a BEFORE trigger, so assign its source
        -- UUID first. Resource revisions are managed before the other triggers.
        trigger_name := CASE WHEN layout.mode = 'schema_revision' THEN 'a_logical_identity' ELSE 'zz_logical_identity' END;
        EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON public.%I
            FOR EACH ROW EXECUTE FUNCTION public.hubuum_manage_logical_identity()', trigger_name, layout.relation_name);
        -- Repeated batches must not rescan an ever-growing assigned prefix.
        EXECUTE format('CREATE INDEX %I ON public.%I ((1)) WHERE logical_identity IS NULL',
            layout.relation_name || '_identity_backfill_pending', layout.relation_name); -- hubuum-compat: bounded-transactional-index
        IF layout.mode IN ('resource', 'principal', 'task') THEN
            EXECUTE format('CREATE UNIQUE INDEX %I ON public.%I ((logical_identity #>> ''{subject,identity,identity}''))',
                layout.relation_name || '_logical_identity_unique', layout.relation_name); -- hubuum-compat: bounded-transactional-index
        END IF;
        IF layout.mode = 'history' THEN
            EXECUTE format('CREATE INDEX %I ON public.%I
                ((logical_identity #>> ''{subject,identity,identity}''), valid_from DESC, history_id DESC)',
                layout.relation_name || '_identity_temporal_lookup', layout.relation_name); -- hubuum-compat: bounded-transactional-index
        END IF;
        IF layout.mode IN ('history', 'definition_history', 'schema_revision', 'schema_history') THEN
            EXECUTE format('CREATE UNIQUE INDEX %I ON public.%I ((logical_identity #>> ''{subject,version}''))',
                layout.relation_name || '_version_identity_unique', layout.relation_name); -- hubuum-compat: bounded-transactional-index
        END IF;
    END LOOP;
END
$triggers$;

-- Each call is one bounded transaction. SKIP LOCKED lets concurrent migrators
-- cooperate; unique mapping keys arbitrate identity assignment across tables.
CREATE FUNCTION hubuum_backfill_logical_identities(batch_size INTEGER)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE layout RECORD; candidate RECORD; changed INTEGER := 0;
    previous_history TEXT := current_setting('hubuum.restore_history', TRUE);
    previous_revisions TEXT := current_setting('hubuum.restore_revisions', TRUE);
    previous_events TEXT := current_setting('hubuum.restore_events', TRUE);
    previous_identities TEXT := current_setting('hubuum.restore_identities', TRUE);
    previous_seqscan TEXT := current_setting('enable_seqscan');
BEGIN
    IF NOT pg_has_role(session_user, current_user, 'MEMBER') THEN
        RAISE EXCEPTION 'Logical identity backfill requires schema-owner authority' USING ERRCODE = '42501';
    END IF;
    IF batch_size NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'Identity backfill batch size must be between 1 and 1000';
    END IF;
    IF (SELECT completed FROM hubuum_control.identity_backfill WHERE singleton) THEN RETURN 0; END IF;
    PERFORM set_config('hubuum.restore_history', 'on', TRUE),
        set_config('hubuum.restore_revisions', 'on', TRUE), set_config('hubuum.restore_events', 'on', TRUE),
        set_config('hubuum.restore_identities', 'on', TRUE);
    -- Statistics describe the pre-upgrade population until autovacuum catches
    -- up. Prefer the pending-row index even before its shrinking size is known.
    PERFORM set_config('enable_seqscan', 'off', TRUE);
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() LOOP
        FOR candidate IN EXECUTE format('SELECT ctid, to_jsonb(t) AS source FROM public.%I t
            WHERE logical_identity IS NULL LIMIT $1 FOR UPDATE SKIP LOCKED', layout.relation_name)
            USING batch_size - changed LOOP
            EXECUTE format('UPDATE public.%I SET logical_identity = $1 WHERE ctid = $2', layout.relation_name)
                USING public.hubuum_legacy_identity(layout.relation_name, candidate.source), candidate.ctid;
            changed := changed + 1;
        END LOOP;
        EXIT WHEN changed = batch_size;
    END LOOP;
    PERFORM set_config('hubuum.restore_history', COALESCE(previous_history, ''), TRUE),
        set_config('hubuum.restore_revisions', COALESCE(previous_revisions, ''), TRUE),
        set_config('hubuum.restore_events', COALESCE(previous_events, ''), TRUE),
        set_config('hubuum.restore_identities', COALESCE(previous_identities, ''), TRUE),
        set_config('enable_seqscan', previous_seqscan, TRUE);
    RETURN changed;
END
$function$;

CREATE FUNCTION hubuum_validate_logical_identities()
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE layout RECORD; missing BOOLEAN; owners_query TEXT := ''; owner_column TEXT;
BEGIN
    IF NOT pg_has_role(session_user, current_user, 'MEMBER') THEN
        RAISE EXCEPTION 'Identity validation requires schema-owner authority' USING ERRCODE = '42501';
    END IF;
    IF (SELECT completed FROM hubuum_control.identity_backfill WHERE singleton) THEN RETURN; END IF;
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I WHERE logical_identity IS NULL)', layout.relation_name)
            INTO missing;
        IF missing THEN
            RAISE EXCEPTION 'Logical identity backfill is incomplete for %; retry the upgrade', layout.relation_name;
        END IF;
        -- Scan without accumulating row payloads. Every defined dependency must
        -- be represented by a typed known or explicitly unresolved reference.
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I t WHERE
            jsonb_typeof(logical_identity->''references'') IS DISTINCT FROM ''object''
            OR EXISTS (
                SELECT 1 FROM jsonb_each(public.hubuum_capture_identity_references(to_jsonb(t), NULL, TRUE)) expected
                WHERE (logical_identity #>> ARRAY[''references'', expected.key, ''kind'']) IS DISTINCT FROM (expected.value->>''kind'')
                   OR (logical_identity #>> ARRAY[''references'', expected.key, ''family'']) IS DISTINCT FROM (expected.value->>''family'')
            ) OR EXISTS (
                SELECT 1 FROM jsonb_each(logical_identity->''references'') actual
                WHERE actual.value->>''kind'' NOT IN (''resource'', ''principal'', ''task'', ''version'', ''event'')
                   OR actual.value #>> ''{reference,state}'' NOT IN (''known'', ''unresolved'')
                   OR CASE actual.value #>> ''{reference,state}''
                      WHEN ''known'' THEN NOT COALESCE(pg_input_is_valid(actual.value #>> ''{reference,identity}'', ''uuid''), FALSE)
                      WHEN ''unresolved'' THEN NOT COALESCE(
                          pg_input_is_valid(actual.value #>> ''{reference,legacy,cohort}'', ''uuid'')
                          AND (actual.value #>> ''{reference,legacy,legacy_id}'')::BIGINT > 0, FALSE)
                      ELSE TRUE END
            ))', layout.relation_name) INTO missing;
        IF missing THEN
            RAISE EXCEPTION 'Logical identity reference coverage is invalid for %', layout.relation_name;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = format('public.%I', layout.relation_name)::regclass
            AND tgname = CASE WHEN layout.mode = 'schema_revision' THEN 'a_logical_identity' ELSE 'zz_logical_identity' END
            AND tgenabled = 'O') THEN
            RAISE EXCEPTION 'Logical identity immutability trigger is unavailable for %', layout.relation_name;
        END IF;
        IF layout.mode <> 'association' THEN
            owner_column := CASE WHEN layout.mode IN ('schema_revision', 'schema_history', 'definition_history')
                THEN 'class_id' ELSE 'id' END;
            IF owners_query <> '' THEN owners_query := owners_query || ' UNION ALL '; END IF;
            owners_query := owners_query || format(
                'SELECT %L::TEXT AS family, %I::BIGINT AS owner_id, %L::TEXT AS mode,
                    logical_identity #>> ''{subject,identity,identity}'' AS identity,
                    logical_identity #>> ''{subject,version}'' AS version,
                    CASE WHEN %L = ''history'' THEN to_jsonb(t)->>''valid_to'' IS NULL ELSE FALSE END AS open_history
                 FROM public.%I t', layout.family, owner_column, layout.mode, layout.mode, layout.relation_name);
        END IF;
    END LOOP;
    -- Per-table unique indexes cannot detect a UUID shared between unrelated
    -- families or between a live row and a different historical owner. Keep
    -- aggregation in PostgreSQL, where it can spill within work_mem bounds.
    EXECUTE 'WITH owners AS (' || owners_query || ')
        SELECT EXISTS (SELECT 1 FROM owners WHERE identity IS NOT NULL GROUP BY identity
            HAVING count(DISTINCT (family, owner_id)) > 1)
        OR EXISTS (SELECT 1 FROM owners WHERE version IS NOT NULL GROUP BY version
            HAVING count(*) > 1 AND NOT (count(*) = 2
                AND count(*) FILTER (WHERE mode = ''resource'') = 1
                AND count(*) FILTER (WHERE mode = ''history'' AND open_history) = 1
                AND count(DISTINCT (family, owner_id, identity)) = 1))' INTO missing;
    IF missing THEN
        RAISE EXCEPTION 'Logical resource or version identity is shared by different owners';
    END IF;
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() WHERE mode = 'history' LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I h JOIN public.%I r ON r.id = h.id
            WHERE h.valid_to IS NULL AND h.op <> ''D'' AND (h.logical_identity->''subject'') IS DISTINCT FROM (r.logical_identity->''subject''))',
            layout.relation_name, left(layout.relation_name, -8)) INTO missing;
        IF missing THEN
            RAISE EXCEPTION 'Live and open historical identities disagree for %', layout.relation_name;
        END IF;
    END LOOP;
    UPDATE hubuum_control.identity_backfill SET completed = TRUE WHERE singleton;
END
$function$;

REVOKE ALL ON FUNCTION hubuum_legacy_resource_identity(TEXT, JSONB),
    hubuum_legacy_version_identity(TEXT, JSONB), hubuum_legacy_identity(TEXT, JSONB),
    hubuum_identity_reference(TEXT, BIGINT, BOOLEAN),
    hubuum_identity_version(TEXT, BIGINT, BIGINT, BOOLEAN),
    hubuum_capture_identity_references(JSONB, JSONB, BOOLEAN),
    hubuum_manage_logical_identity(), hubuum_backfill_logical_identities(INTEGER),
    hubuum_validate_logical_identities(),
    hubuum_unresolved_identity(BIGINT, BIGINT) FROM PUBLIC;
