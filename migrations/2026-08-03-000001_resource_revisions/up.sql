-- Authoritative resource revisions. Revisions are database owned: callers may
-- restore an exact value only while hubuum.restore_revisions is transaction
-- locally enabled.

ALTER TABLE identity_scopes ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE principals ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE groups ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE collections ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE group_memberships ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE hubuumclass ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE hubuumobject ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE hubuumclass_relation ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE hubuumobject_relation ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE export_templates ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE remote_targets ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE event_sinks ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE event_subscriptions ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);
ALTER TABLE tokens ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0);

CREATE TABLE collection_authorization_state (
    collection_id INT PRIMARY KEY REFERENCES collections(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0)
);

INSERT INTO collection_authorization_state (collection_id)
SELECT id FROM collections;

-- The seven temporal resources predate revisions. An insert starts at one,
-- every stored update advances once, and a delete tombstone retains the last
-- live revision.
DO $$
DECLARE
    table_name TEXT;
    base_table TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'collections_history',
        'hubuumclass_history',
        'hubuumclass_relation_history',
        'hubuumobject_history',
        'hubuumobject_relation_history',
        'export_templates_history',
        'remote_targets_history'
    ]
    LOOP
        EXECUTE format('ALTER TABLE %I ADD COLUMN revision BIGINT', table_name);
        EXECUTE format(
            'WITH ranked AS (
                 SELECT history_id,
                        greatest(1, sum(CASE WHEN op IN (''I'', ''U'') THEN 1 ELSE 0 END)
                            OVER (PARTITION BY id ORDER BY history_id))::bigint AS revision
                 FROM %I
             )
             UPDATE %I history
                SET revision = ranked.revision
               FROM ranked
              WHERE history.history_id = ranked.history_id',
            table_name,
            table_name
        );
        EXECUTE format(
            'ALTER TABLE %I ALTER COLUMN revision SET NOT NULL, ADD CHECK (revision > 0)',
            table_name
        );
        EXECUTE format(
            'CREATE INDEX %I ON %I (id, revision, history_id)',
            table_name || '_id_revision_idx',
            table_name
        );

        base_table := replace(table_name, '_history', '');
        EXECUTE format('ALTER TABLE %I DISABLE TRIGGER USER', base_table);
        EXECUTE format(
            'UPDATE %I live
                SET revision = history.revision
               FROM %I history
              WHERE history.id = live.id AND history.valid_to IS NULL',
            base_table,
            table_name
        );
        EXECUTE format('ALTER TABLE %I ENABLE TRIGGER USER', base_table);
    END LOOP;
END $$;

ALTER TABLE events
    ADD COLUMN before_revision BIGINT NULL CHECK (before_revision > 0),
    ADD COLUMN after_revision BIGINT NULL CHECK (after_revision > 0);

CREATE OR REPLACE FUNCTION hubuum_revision_owner_first(owner_key TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    touched TEXT := current_setting('hubuum.revision_owners', true);
    marker TEXT := '|' || owner_key || '|';
BEGIN
    IF position(marker IN coalesce(touched, '')) > 0 THEN
        RETURN FALSE;
    END IF;
    PERFORM set_config('hubuum.revision_owners', coalesce(touched, '') || marker, true);
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION hubuum_revision_owner_key(table_name TEXT, row_data JSONB)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
BEGIN
    IF table_name = 'group_memberships' THEN
        RETURN table_name || ':' || (row_data->>'principal_id') || ':'
            || (row_data->>'group_id');
    END IF;
    RETURN table_name || ':' || coalesce(row_data->>'id', row_data->>'collection_id');
END;
$$;

-- Compare a request's If-Match revisions at the first authoritative row lock.
-- Later writes to the same owner in the transaction are part of the same
-- logical mutation and therefore do not compare against the already advanced
-- revision a second time.
CREATE OR REPLACE FUNCTION hubuum_assert_revision_precondition(
    owner_key TEXT,
    current_revision BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    expected_owner TEXT := current_setting('hubuum.if_match_owner', true);
    expected_revisions TEXT := current_setting('hubuum.if_match_revisions', true);
    checked_owner TEXT := current_setting('hubuum.if_match_checked', true);
BEGIN
    IF expected_owner IS NULL OR expected_owner = '' OR expected_owner <> owner_key THEN
        RETURN;
    END IF;
    IF checked_owner = owner_key THEN
        RETURN;
    END IF;
    IF expected_revisions IS NOT NULL
       AND expected_revisions <> ''
       AND NOT current_revision::TEXT = ANY(string_to_array(expected_revisions, ','))
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'hubuum_stale_resource';
    END IF;
    PERFORM set_config('hubuum.if_match_checked', owner_key, true);
END;
$$;

-- TG_ARGV:
--   0: whether this table has updated_at
--   1: whether changes are coalesced per owner transaction
--   2..: non-authoritative operational fields that persist without advancing
CREATE OR REPLACE FUNCTION hubuum_manage_resource_revision()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    restoring BOOLEAN := current_setting('hubuum.restore_revisions', true) IS NOT DISTINCT FROM 'on';
    internal_bump BOOLEAN := current_setting('hubuum.internal_revision_bump', true) IS NOT DISTINCT FROM 'on';
    has_updated_at BOOLEAN := TG_ARGV[0] = 'updated_at';
    coalesced BOOLEAN := TG_ARGV[1] = 'coalesce';
    ignored_fields TEXT[] := ARRAY['revision', 'created_at', 'updated_at'];
    domain_old JSONB;
    domain_new JSONB;
    operational_old JSONB;
    operational_new JSONB;
    owner_key TEXT;
    first_change BOOLEAN := TRUE;
    index INT;
BEGIN
    IF TG_NARGS > 2 THEN
        FOR index IN 2..TG_NARGS - 1 LOOP
            ignored_fields := array_append(ignored_fields, TG_ARGV[index]);
        END LOOP;
    END IF;

    IF restoring THEN
        IF NEW.revision <= 0 THEN
            RAISE EXCEPTION 'resource revision must be greater than zero';
        END IF;
        RETURN NEW;
    END IF;

    owner_key := hubuum_revision_owner_key(TG_TABLE_NAME, to_jsonb(NEW));

    IF TG_OP = 'INSERT' THEN
        IF NEW.revision <> 1 THEN
            RAISE EXCEPTION 'caller-supplied resource revision is not permitted';
        END IF;
        IF coalesced THEN
            PERFORM hubuum_revision_owner_first(owner_key);
        END IF;
        RETURN NEW;
    END IF;

    PERFORM hubuum_assert_revision_precondition(owner_key, OLD.revision);

    IF internal_bump THEN
        IF OLD.revision = 9223372036854775807 OR NEW.revision <> OLD.revision + 1 THEN
            RAISE EXCEPTION 'invalid internal resource revision advancement';
        END IF;
        IF has_updated_at THEN
            NEW.updated_at := clock_timestamp() AT TIME ZONE 'UTC';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.revision IS DISTINCT FROM OLD.revision THEN
        RAISE EXCEPTION 'caller-supplied resource revision changes are not permitted';
    END IF;

    domain_old := to_jsonb(OLD) - ignored_fields;
    domain_new := to_jsonb(NEW) - ignored_fields;
    IF domain_old = domain_new THEN
        -- Operational fields (for example token activity and directory sync
        -- bookkeeping) persist without changing revision or updated_at.
        operational_old := to_jsonb(OLD) - ARRAY['revision', 'created_at', 'updated_at'];
        operational_new := to_jsonb(NEW) - ARRAY['revision', 'created_at', 'updated_at'];
        IF operational_old = operational_new THEN
            RETURN NULL;
        END IF;
        NEW.revision := OLD.revision;
        IF has_updated_at THEN
            NEW.updated_at := OLD.updated_at;
        END IF;
        RETURN NEW;
    END IF;

    IF coalesced THEN
        first_change := hubuum_revision_owner_first(owner_key);
    END IF;
    IF first_change THEN
        IF OLD.revision = 9223372036854775807 THEN
            RAISE EXCEPTION 'resource revision overflow';
        END IF;
        NEW.revision := OLD.revision + 1;
    ELSE
        NEW.revision := OLD.revision;
    END IF;
    IF has_updated_at
       AND current_setting('hubuum.preserve_imported_timestamps', true) IS DISTINCT FROM 'on'
    THEN
        NEW.updated_at := clock_timestamp() AT TIME ZONE 'UTC';
    END IF;
    RETURN NEW;
END;
$$;

-- Aggregate child rows have no public revision of their own. They suppress
-- timestamp-only writes here and advance their owner from an AFTER trigger.
CREATE OR REPLACE FUNCTION hubuum_skip_unchanged_revision_child()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    has_updated_at BOOLEAN := TG_ARGV[0] = 'updated_at';
    ignored_fields TEXT[] := ARRAY['created_at', 'updated_at'];
BEGIN
    IF to_jsonb(OLD) - ignored_fields = to_jsonb(NEW) - ignored_fields THEN
        RETURN NULL;
    END IF;
    IF has_updated_at
       AND current_setting('hubuum.preserve_imported_timestamps', true) IS DISTINCT FROM 'on'
    THEN
        NEW.updated_at := clock_timestamp() AT TIME ZONE 'UTC';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION hubuum_bump_revision_owner()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    owner_table TEXT := TG_ARGV[0];
    owner_id TEXT;
    owner_key TEXT;
BEGIN
    IF current_setting('hubuum.restore_revisions', true) IS NOT DISTINCT FROM 'on' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    IF TG_ARGV[1] = 'membership' THEN
        owner_id := coalesce(to_jsonb(NEW)->>'principal_id', to_jsonb(OLD)->>'principal_id')
            || ':' || coalesce(to_jsonb(NEW)->>'group_id', to_jsonb(OLD)->>'group_id');
    ELSE
        owner_id := coalesce(to_jsonb(NEW)->>TG_ARGV[1], to_jsonb(OLD)->>TG_ARGV[1]);
    END IF;
    owner_key := owner_table || ':' || owner_id;
    IF NOT hubuum_revision_owner_first(owner_key) THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    PERFORM set_config('hubuum.internal_revision_bump', 'on', true);
    IF owner_table = 'principals' THEN
        UPDATE principals SET revision = revision + 1 WHERE id = owner_id::INT;
    ELSIF owner_table = 'group_memberships' THEN
        UPDATE group_memberships SET revision = revision + 1
         WHERE principal_id = split_part(owner_id, ':', 1)::INT
           AND group_id = split_part(owner_id, ':', 2)::INT;
    ELSIF owner_table = 'tokens' THEN
        UPDATE tokens SET revision = revision + 1 WHERE id = owner_id::INT;
    ELSIF owner_table = 'collection_authorization_state' THEN
        UPDATE collection_authorization_state SET revision = revision + 1
         WHERE collection_id = owner_id::INT;
    ELSE
        RAISE EXCEPTION 'unknown revision owner table %', owner_table;
    END IF;
    PERFORM set_config('hubuum.internal_revision_bump', 'off', true);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION hubuum_check_delete_revision()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF current_setting('hubuum.restore_revisions', true) IS DISTINCT FROM 'on' THEN
        PERFORM hubuum_assert_revision_precondition(
            hubuum_revision_owner_key(TG_TABLE_NAME, to_jsonb(OLD)),
            OLD.revision
        );
    END IF;
    RETURN OLD;
END;
$$;

-- Remove the old timestamp triggers. Revision triggers own both no-op
-- suppression and updated_at ordering for authoritative resources.
DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'identity_scopes', 'principals', 'groups', 'collections',
        'group_memberships', 'hubuumclass', 'hubuumobject',
        'hubuumclass_relation', 'hubuumobject_relation', 'export_templates',
        'remote_targets', 'event_sinks', 'event_subscriptions'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS update_%I_updated_at ON %I', table_name, table_name);
    END LOOP;
    DROP TRIGGER IF EXISTS update_users_updated_at ON users;
    DROP TRIGGER IF EXISTS update_service_accounts_updated_at ON service_accounts;
    DROP TRIGGER IF EXISTS update_group_membership_sources_updated_at ON group_membership_sources;
    DROP TRIGGER IF EXISTS update_permissions_updated_at ON permissions;
END $$;

-- Old temporal suppression triggers would run alongside the new contract.
DROP TRIGGER IF EXISTS collections_skip_unchanged_temporal_update_trg ON collections;
DROP TRIGGER IF EXISTS hubuumclass_skip_unchanged_temporal_update_trg ON hubuumclass;
DROP TRIGGER IF EXISTS hubuumobject_skip_unchanged_temporal_update_trg ON hubuumobject;
DROP TRIGGER IF EXISTS hubuumclass_relation_skip_unchanged_temporal_update_trg ON hubuumclass_relation;
DROP TRIGGER IF EXISTS hubuumobject_relation_skip_unchanged_temporal_update_trg ON hubuumobject_relation;
DROP TRIGGER IF EXISTS export_templates_skip_unchanged_temporal_update_trg ON export_templates;
DROP TRIGGER IF EXISTS remote_targets_skip_unchanged_temporal_update_trg ON remote_targets;

CREATE TRIGGER identity_scopes_revision BEFORE INSERT OR UPDATE ON identity_scopes
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER principals_revision BEFORE INSERT OR UPDATE ON principals
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision(
    'updated_at', 'coalesce', 'last_sync_attempted_at', 'last_sync_success_at'
);
CREATE TRIGGER groups_revision BEFORE INSERT OR UPDATE ON groups
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision(
    'updated_at', 'direct', 'last_sync_attempted_at', 'last_sync_success_at'
);
CREATE TRIGGER collections_revision BEFORE INSERT OR UPDATE ON collections
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER group_memberships_revision BEFORE INSERT OR UPDATE ON group_memberships
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'coalesce');
CREATE TRIGGER hubuumclass_revision BEFORE INSERT OR UPDATE ON hubuumclass
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER hubuumobject_revision BEFORE INSERT OR UPDATE ON hubuumobject
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER hubuumclass_relation_revision BEFORE INSERT OR UPDATE ON hubuumclass_relation
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER hubuumobject_relation_revision BEFORE INSERT OR UPDATE ON hubuumobject_relation
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER export_templates_revision BEFORE INSERT OR UPDATE ON export_templates
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER remote_targets_revision BEFORE INSERT OR UPDATE ON remote_targets
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER event_sinks_revision BEFORE INSERT OR UPDATE ON event_sinks
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER event_subscriptions_revision BEFORE INSERT OR UPDATE ON event_subscriptions
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('updated_at', 'direct');
CREATE TRIGGER computed_field_definitions_revision BEFORE INSERT OR UPDATE ON computed_field_definitions
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision(
    'updated_at', 'direct', 'created_by', 'updated_by'
);
CREATE TRIGGER tokens_revision BEFORE INSERT OR UPDATE ON tokens
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('none', 'coalesce', 'last_used_at');
CREATE TRIGGER collection_authorization_state_revision
BEFORE INSERT OR UPDATE ON collection_authorization_state
FOR EACH ROW EXECUTE FUNCTION hubuum_manage_resource_revision('none', 'coalesce');

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'identity_scopes', 'principals', 'groups', 'collections',
        'group_memberships', 'hubuumclass', 'hubuumobject',
        'hubuumclass_relation', 'hubuumobject_relation', 'export_templates',
        'remote_targets', 'event_sinks', 'event_subscriptions',
        'computed_field_definitions', 'tokens', 'collection_authorization_state'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE DELETE ON %I
             FOR EACH ROW EXECUTE FUNCTION hubuum_check_delete_revision()',
            table_name || '_delete_revision', table_name
        );
    END LOOP;
END $$;

CREATE TRIGGER users_revision_child BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_revision_child('updated_at');
CREATE TRIGGER service_accounts_revision_child BEFORE UPDATE ON service_accounts
FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_revision_child('updated_at');
CREATE TRIGGER group_membership_sources_revision_child BEFORE UPDATE ON group_membership_sources
FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_revision_child('updated_at');
CREATE TRIGGER permissions_revision_child BEFORE UPDATE ON permissions
FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_revision_child('updated_at');

CREATE TRIGGER users_bump_principal_revision AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE FUNCTION hubuum_bump_revision_owner('principals', 'id');
CREATE TRIGGER service_accounts_bump_principal_revision AFTER INSERT OR UPDATE OR DELETE ON service_accounts
FOR EACH ROW EXECUTE FUNCTION hubuum_bump_revision_owner('principals', 'id');
CREATE TRIGGER membership_sources_bump_revision AFTER INSERT OR UPDATE OR DELETE ON group_membership_sources
FOR EACH ROW EXECUTE FUNCTION hubuum_bump_revision_owner('group_memberships', 'membership');
CREATE TRIGGER permissions_bump_revision AFTER INSERT OR UPDATE OR DELETE ON permissions
FOR EACH ROW EXECUTE FUNCTION hubuum_bump_revision_owner('collection_authorization_state', 'collection_id');

CREATE OR REPLACE FUNCTION hubuum_create_collection_authorization_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('hubuum.restore_revisions', true) IS NOT DISTINCT FROM 'on' THEN
        RETURN NEW;
    END IF;
    INSERT INTO collection_authorization_state(collection_id) VALUES (NEW.id);
    RETURN NEW;
END;
$$;
CREATE TRIGGER collections_create_authorization_state
AFTER INSERT ON collections FOR EACH ROW
EXECUTE FUNCTION hubuum_create_collection_authorization_state();

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'token_scopes', 'token_collection_scopes', 'token_class_scopes', 'token_object_scopes'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I
             FOR EACH ROW EXECUTE FUNCTION hubuum_bump_revision_owner(''tokens'', ''token_id'')',
            table_name || '_bump_token_revision', table_name
        );
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION hubuum_fill_event_revisions()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('hubuum.restore_events', true) IS NOT DISTINCT FROM 'on' THEN
        RETURN NEW;
    END IF;
    IF NEW.before_revision IS NULL
       AND jsonb_typeof((NEW."before"::jsonb)->'revision') = 'number'
    THEN
        NEW.before_revision := ((NEW."before"::jsonb)->>'revision')::BIGINT;
    END IF;
    IF NEW.after_revision IS NULL
       AND jsonb_typeof((NEW."after"::jsonb)->'revision') = 'number'
    THEN
        NEW.after_revision := ((NEW."after"::jsonb)->>'revision')::BIGINT;
    END IF;
    IF NEW.before_revision IS NOT NULL OR NEW.after_revision IS NOT NULL THEN
        NEW.schema_version := 2;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER events_fill_revisions BEFORE INSERT ON events
FOR EACH ROW EXECUTE FUNCTION hubuum_fill_event_revisions();

CREATE OR REPLACE FUNCTION enforce_events_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF current_setting('events.allow_purge', true) IS DISTINCT FROM 'on' THEN
            RAISE EXCEPTION 'events table is append-only: DELETE is not permitted';
        END IF;
        RETURN OLD;
    END IF;

    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.event_id IS DISTINCT FROM OLD.event_id
       OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
       OR NEW.entity_type IS DISTINCT FROM OLD.entity_type
       OR NEW.entity_id IS DISTINCT FROM OLD.entity_id
       OR NEW.entity_name IS DISTINCT FROM OLD.entity_name
       OR NEW.collection_id IS DISTINCT FROM OLD.collection_id
       OR NEW.action IS DISTINCT FROM OLD.action
       OR NEW.actor_user_id IS DISTINCT FROM OLD.actor_user_id
       OR NEW.actor_kind IS DISTINCT FROM OLD.actor_kind
       OR NEW.initiator_user_id IS DISTINCT FROM OLD.initiator_user_id
       OR NEW.task_id IS DISTINCT FROM OLD.task_id
       OR NEW.request_id IS DISTINCT FROM OLD.request_id
       OR NEW.correlation_id IS DISTINCT FROM OLD.correlation_id
       OR NEW.summary IS DISTINCT FROM OLD.summary
       OR NEW.before IS DISTINCT FROM OLD.before
       OR NEW.after IS DISTINCT FROM OLD.after
       OR NEW.before_revision IS DISTINCT FROM OLD.before_revision
       OR NEW.after_revision IS DISTINCT FROM OLD.after_revision
       OR NEW.metadata IS DISTINCT FROM OLD.metadata
       OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    THEN
        RAISE EXCEPTION 'events table is append-only: only fan-out claim fields and dispatched_at may be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX identity_scopes_revision_id_idx ON identity_scopes (revision, id);
CREATE INDEX principals_revision_id_idx ON principals (revision, id);
CREATE INDEX groups_revision_id_idx ON groups (revision, id);
CREATE INDEX collections_parent_revision_id_idx ON collections (parent_collection_id, revision, id);
CREATE INDEX hubuumclass_collection_revision_id_idx ON hubuumclass (collection_id, revision, id);
CREATE INDEX hubuumobject_class_revision_id_idx ON hubuumobject (hubuum_class_id, revision, id);
CREATE INDEX hubuumclass_relation_revision_id_idx ON hubuumclass_relation (revision, id);
CREATE INDEX hubuumobject_relation_revision_id_idx ON hubuumobject_relation (revision, id);
CREATE INDEX export_templates_collection_revision_id_idx ON export_templates (collection_id, revision, id);
CREATE INDEX remote_targets_collection_revision_id_idx ON remote_targets (collection_id, revision, id);
CREATE INDEX event_sinks_revision_id_idx ON event_sinks (revision, id);
CREATE INDEX event_subscriptions_collection_revision_id_idx ON event_subscriptions (collection_id, revision, id);
CREATE INDEX computed_field_class_revision_id_idx ON computed_field_definitions (class_id, revision, id);
CREATE INDEX tokens_principal_revision_id_idx ON tokens (principal_id, revision, id);
CREATE INDEX group_memberships_group_revision_idx ON group_memberships (group_id, revision, principal_id);
CREATE INDEX events_before_revision_id_idx ON events (before_revision, id) WHERE before_revision IS NOT NULL;
CREATE INDEX events_after_revision_id_idx ON events (after_revision, id) WHERE after_revision IS NOT NULL;
