-- Remove every trigger that depends on the revision contract before dropping
-- its functions or columns.
DROP INDEX IF EXISTS computed_field_class_revision_id_idx;

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
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', table_name || '_revision', table_name);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', table_name || '_delete_revision', table_name);
    END LOOP;

    FOREACH table_name IN ARRAY ARRAY[
        'token_scopes', 'token_collection_scopes', 'token_class_scopes', 'token_object_scopes'
    ] LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I',
            table_name || '_bump_token_revision', table_name
        );
    END LOOP;
END $$;

DROP TRIGGER IF EXISTS users_revision_child ON users;
DROP TRIGGER IF EXISTS service_accounts_revision_child ON service_accounts;
DROP TRIGGER IF EXISTS group_membership_sources_revision_child ON group_membership_sources;
DROP TRIGGER IF EXISTS permissions_revision_child ON permissions;
DROP TRIGGER IF EXISTS users_bump_principal_revision ON users;
DROP TRIGGER IF EXISTS service_accounts_bump_principal_revision ON service_accounts;
DROP TRIGGER IF EXISTS membership_sources_bump_revision ON group_membership_sources;
DROP TRIGGER IF EXISTS permissions_bump_revision ON permissions;
DROP TRIGGER IF EXISTS events_fill_revisions ON events;
DROP TRIGGER IF EXISTS collections_create_authorization_state ON collections;

DROP FUNCTION IF EXISTS hubuum_fill_event_revisions();
DROP FUNCTION IF EXISTS hubuum_create_collection_authorization_state();
DROP FUNCTION IF EXISTS hubuum_check_delete_revision();
DROP FUNCTION IF EXISTS hubuum_bump_revision_owner();
DROP FUNCTION IF EXISTS hubuum_skip_unchanged_revision_child();
DROP FUNCTION IF EXISTS hubuum_manage_resource_revision();
DROP FUNCTION IF EXISTS hubuum_assert_revision_precondition(TEXT, BIGINT);
DROP FUNCTION IF EXISTS hubuum_revision_owner_key(TEXT, JSONB);
DROP FUNCTION IF EXISTS hubuum_revision_owner_first(TEXT);

DROP TABLE IF EXISTS collection_authorization_state;

ALTER TABLE events DROP COLUMN IF EXISTS before_revision, DROP COLUMN IF EXISTS after_revision;
ALTER TABLE identity_scopes DROP COLUMN IF EXISTS revision;
ALTER TABLE principals DROP COLUMN IF EXISTS revision;
ALTER TABLE groups DROP COLUMN IF EXISTS revision;
ALTER TABLE collections DROP COLUMN IF EXISTS revision;
ALTER TABLE group_memberships DROP COLUMN IF EXISTS revision;
ALTER TABLE hubuumclass DROP COLUMN IF EXISTS revision;
ALTER TABLE hubuumobject DROP COLUMN IF EXISTS revision;
ALTER TABLE hubuumclass_relation DROP COLUMN IF EXISTS revision;
ALTER TABLE hubuumobject_relation DROP COLUMN IF EXISTS revision;
ALTER TABLE export_templates DROP COLUMN IF EXISTS revision;
ALTER TABLE remote_targets DROP COLUMN IF EXISTS revision;
ALTER TABLE event_sinks DROP COLUMN IF EXISTS revision;
ALTER TABLE event_subscriptions DROP COLUMN IF EXISTS revision;
ALTER TABLE tokens DROP COLUMN IF EXISTS revision;

DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'collections_history', 'hubuumclass_history', 'hubuumclass_relation_history',
        'hubuumobject_history', 'hubuumobject_relation_history',
        'export_templates_history', 'remote_targets_history'
    ] LOOP
        EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS revision', table_name);
    END LOOP;
END $$;

-- Restore the timestamp and temporal no-op behavior from the preceding
-- migrations so a migration revert leaves a usable prior schema.
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'identity_scopes', 'principals', 'groups', 'group_memberships',
        'collections', 'hubuumclass', 'hubuumobject', 'hubuumclass_relation',
        'hubuumobject_relation', 'export_templates', 'remote_targets',
        'event_sinks', 'event_subscriptions'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION update_modified_column()',
            'update_' || table_name || '_updated_at', table_name
        );
    END LOOP;

    FOREACH table_name IN ARRAY ARRAY[
        'users', 'service_accounts', 'group_membership_sources', 'permissions'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION update_modified_column()',
            'update_' || table_name || '_updated_at', table_name
        );
    END LOOP;

    FOREACH table_name IN ARRAY ARRAY[
        'collections', 'hubuumclass', 'hubuumobject', 'hubuumclass_relation',
        'hubuumobject_relation', 'export_templates', 'remote_targets'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_temporal_update()',
            table_name || '_skip_unchanged_temporal_update_trg', table_name
        );
    END LOOP;
END $$;

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
       OR NEW.metadata IS DISTINCT FROM OLD.metadata
       OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    THEN
        RAISE EXCEPTION 'events table is append-only: only fan-out claim fields and dispatched_at may be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
