    -- Greenfield squashed schema. Defines the entire database in final form:
    -- principal abstraction (humans + service accounts), principal-centric group
    -- membership and tokens, rich token lifecycle + scopes, and the folded
    -- remote-target schema. No backwards-compatibility shims.

    ----------------------
    ---- Drop (reverse dependency order; CASCADE covers the rest)
    ----------------------
    DROP TABLE IF EXISTS remote_call_results CASCADE;
    DROP TABLE IF EXISTS report_task_outputs CASCADE;
    DROP TABLE IF EXISTS import_task_results CASCADE;
    DROP TABLE IF EXISTS event_deliveries CASCADE;
    DROP TABLE IF EXISTS event_subscriptions CASCADE;
    DROP TABLE IF EXISTS event_sinks CASCADE;
    DROP TABLE IF EXISTS events CASCADE;
    DROP TABLE IF EXISTS tasks CASCADE;
    DROP TABLE IF EXISTS token_scopes CASCADE;
    DROP TABLE IF EXISTS tokens CASCADE;
    DROP TABLE IF EXISTS remote_targets CASCADE;
    DROP TABLE IF EXISTS report_templates CASCADE;
    DROP TABLE IF EXISTS hubuumobject_relation CASCADE;
    DROP TABLE IF EXISTS hubuumclass_reachability CASCADE;
    DROP TABLE IF EXISTS hubuumclass_relation CASCADE;
    DROP TABLE IF EXISTS hubuumobject CASCADE;
    DROP TABLE IF EXISTS hubuumclass CASCADE;
    DROP TABLE IF EXISTS permissions CASCADE;
    DROP TABLE IF EXISTS group_memberships CASCADE;
    DROP TABLE IF EXISTS user_groups CASCADE;
    DROP TABLE IF EXISTS service_accounts CASCADE;
    DROP TABLE IF EXISTS users CASCADE;
    DROP TABLE IF EXISTS collections CASCADE;
    DROP TABLE IF EXISTS groups CASCADE;
    DROP TABLE IF EXISTS principals CASCADE;

    ----------------------
    ---- Functions needed before tables/constraints reference them
    ----------------------

    -- Update the updated_at column whenever a row is updated
    CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    CREATE OR REPLACE FUNCTION remote_target_subject_types_valid(subject_types JSONB)
    RETURNS BOOLEAN
    LANGUAGE sql
    IMMUTABLE
    AS $$
        SELECT CASE
            WHEN jsonb_typeof(subject_types) <> 'array' THEN FALSE
            ELSE jsonb_array_length(subject_types) > 0
                AND subject_types <@ '["collection", "class", "object", "class_relation", "object_relation"]'::jsonb
                AND (
                    SELECT COUNT(*)
                    FROM jsonb_array_elements_text(subject_types)
                ) = (
                    SELECT COUNT(DISTINCT value)
                    FROM jsonb_array_elements_text(subject_types) AS item(value)
                )
        END;
    $$;

    ----------------------
    ---- Identity: principals + subtypes
    ----------------------

    -- Parent identity table. Both humans and service accounts are principals; a
    -- principal id IS the user/service-account id (class-table inheritance).
    CREATE TABLE principals (
        id SERIAL PRIMARY KEY,
        kind VARCHAR NOT NULL CHECK (kind IN ('human', 'service_account')),
        name VARCHAR NOT NULL UNIQUE,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        -- Backs the composite (id, kind) FKs on the subtype tables.
        UNIQUE (id, kind)
    );

    CREATE TABLE groups (
        id SERIAL PRIMARY KEY,
        groupname VARCHAR NOT NULL UNIQUE,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    CREATE TABLE collections (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    -- Human user. id is the principal id; the login/display name is principals.name.
    CREATE TABLE users (
        id INT PRIMARY KEY,
        kind VARCHAR NOT NULL DEFAULT 'human' CHECK (kind = 'human'),
        password VARCHAR NOT NULL,
        proper_name VARCHAR NULL,
        email VARCHAR NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        anonymized_at TIMESTAMP NULL,
        FOREIGN KEY (id, kind) REFERENCES principals (id, kind) ON DELETE CASCADE
    );

    -- Service account. id is the principal id; the name is principals.name.
    CREATE TABLE service_accounts (
        id INT PRIMARY KEY,
        kind VARCHAR NOT NULL DEFAULT 'service_account' CHECK (kind = 'service_account'),
        description VARCHAR NOT NULL DEFAULT '',
        owner_group_id INT NOT NULL REFERENCES groups (id) ON DELETE RESTRICT,
        created_by INT NULL REFERENCES principals (id) ON DELETE SET NULL,
        disabled_at TIMESTAMP NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        FOREIGN KEY (id, kind) REFERENCES principals (id, kind) ON DELETE CASCADE
    );

    -- Group membership is principal-centric (humans and service accounts alike).
    CREATE TABLE group_memberships (
        principal_id INT REFERENCES principals (id) ON DELETE CASCADE NOT NULL,
        group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
        PRIMARY KEY (principal_id, group_id),
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    CREATE TABLE permissions (
        id SERIAL PRIMARY KEY,
        collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
        group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
        has_read_collection BOOLEAN NOT NULL,
        has_update_collection BOOLEAN NOT NULL,
        has_delete_collection BOOLEAN NOT NULL,
        has_delegate_collection BOOLEAN NOT NULL,
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
        has_read_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_create_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_update_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_delete_template BOOLEAN NOT NULL DEFAULT FALSE,
        has_read_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
        has_create_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
        has_update_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
        has_delete_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
        has_execute_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        has_read_audit BOOLEAN NOT NULL DEFAULT FALSE,
        has_manage_event_subscription BOOLEAN NOT NULL DEFAULT FALSE,
        UNIQUE (collection_id, group_id)
    );

    CREATE TABLE hubuumclass (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE,
        collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
        json_schema JSONB DEFAULT NULL,
        validate_schema BOOLEAN DEFAULT false NOT NULL,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    );

    CREATE TABLE hubuumobject (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
        hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
        data JSONB DEFAULT '{}'::jsonb NOT NULL,
        description VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (name, hubuum_class_id)
    );

    -- A bidirectional relation between classes
    CREATE TABLE hubuumclass_relation (
        id SERIAL PRIMARY KEY,
        from_hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
        to_hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
        forward_template_alias VARCHAR NULL,
        reverse_template_alias VARCHAR NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (from_hubuum_class_id, to_hubuum_class_id)
    );

    -- Canonical shortest-path reachability cache for class relations.
    CREATE TABLE hubuumclass_reachability (
        id BIGSERIAL PRIMARY KEY,
        ancestor_class_id INT NOT NULL,
        descendant_class_id INT NOT NULL,
        depth INT NOT NULL,
        path INT[] NOT NULL,
        CHECK (ancestor_class_id < descendant_class_id),
        UNIQUE (ancestor_class_id, descendant_class_id)
    );

    -- A bidirectional relation between objects
    CREATE TABLE hubuumobject_relation (
        id SERIAL PRIMARY KEY,
        from_hubuum_object_id INT REFERENCES hubuumobject (id) ON DELETE CASCADE NOT NULL,
        to_hubuum_object_id INT REFERENCES hubuumobject (id) ON DELETE CASCADE NOT NULL,
        class_relation_id INT REFERENCES hubuumclass_relation (id) ON DELETE CASCADE NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (from_hubuum_object_id, to_hubuum_object_id)
    );

    -- Table to store report templates
    CREATE TABLE report_templates (
        id SERIAL PRIMARY KEY,
        collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
        name VARCHAR NOT NULL,
        description VARCHAR NOT NULL,
        content_type VARCHAR NOT NULL,
        template TEXT NOT NULL,
        kind VARCHAR NOT NULL,
        scope_kind VARCHAR,
        class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE,
        default_query TEXT,
        include JSONB,
        relation_context JSONB,
        default_missing_data_policy VARCHAR,
        default_limits JSONB,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (collection_id, name),
        CHECK (content_type IN ('text/plain', 'text/html', 'text/csv')),
        CHECK (kind IN ('report', 'fragment')),
        CHECK (scope_kind IS NULL OR scope_kind IN (
            'collections', 'classes', 'objects_in_class',
            'class_relations', 'object_relations', 'related_objects'
        )),
        CHECK (default_missing_data_policy IS NULL OR default_missing_data_policy IN ('strict', 'null', 'omit')),
        CHECK (
            (kind = 'fragment' AND scope_kind IS NULL AND class_id IS NULL)
            OR
            (kind = 'report' AND scope_kind IN ('objects_in_class', 'related_objects') AND class_id IS NOT NULL)
            OR
            (kind = 'report' AND scope_kind IN ('collections', 'classes', 'class_relations', 'object_relations') AND class_id IS NULL)
        )
    );

    CREATE TABLE remote_targets (
        id SERIAL PRIMARY KEY,
        collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
        class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NULL,
        name VARCHAR NOT NULL,
        description VARCHAR NOT NULL DEFAULT '',
        method VARCHAR NOT NULL CHECK (method IN ('get', 'post', 'patch', 'delete')),
        url_template TEXT NOT NULL,
        headers_template JSONB NOT NULL DEFAULT '{}'::jsonb,
        body_template TEXT NULL,
        auth_config JSONB NOT NULL DEFAULT '{"type":"none"}'::jsonb,
        allowed_subject_types JSONB NOT NULL,
        timeout_ms INT NOT NULL DEFAULT 10000,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (collection_id, name),
        CHECK (timeout_ms > 0),
        CHECK (jsonb_typeof(headers_template) = 'object'),
        CHECK (jsonb_typeof(auth_config) = 'object'),
        CHECK (NOT (allowed_subject_types ? 'object') OR class_id IS NOT NULL),
        CHECK (remote_target_subject_types_valid(allowed_subject_types))
    );

    -- Bearer tokens are principal-centric with a full lifecycle (named, expiring,
    -- last-used tracked, soft-revocable, optionally scoped).
    CREATE TABLE tokens (
        id SERIAL PRIMARY KEY,
        token VARCHAR NOT NULL UNIQUE,
        principal_id INT REFERENCES principals (id) ON DELETE CASCADE NOT NULL,
        name VARCHAR NULL,
        description VARCHAR NULL,
        issued TIMESTAMP NOT NULL DEFAULT now(),
        expires_at TIMESTAMP NULL,
        last_used_at TIMESTAMP NULL,
        revoked_at TIMESTAMP NULL,
        scoped BOOLEAN NOT NULL DEFAULT FALSE
    );

    -- A token's scope set. Presence is governed by tokens.scoped, not by row count:
    -- scoped=false => unscoped (full principal authority); scoped=true with zero
    -- rows => deny-all.
    CREATE TABLE token_scopes (
        token_id INT REFERENCES tokens (id) ON DELETE CASCADE NOT NULL,
        permission VARCHAR NOT NULL,
        PRIMARY KEY (token_id, permission)
    );

    CREATE TABLE tasks (
        id SERIAL PRIMARY KEY,
        kind VARCHAR NOT NULL CHECK (kind IN ('import', 'report', 'export', 'reindex', 'remote_call')),
        status VARCHAR NOT NULL CHECK (
            status IN (
                'queued',
                'validating',
                'running',
                'succeeded',
                'failed',
                'partially_succeeded',
                'cancelled'
            )
        ),
        submitted_by INT REFERENCES principals (id) ON DELETE SET NULL,
        idempotency_key VARCHAR NULL,
        request_hash VARCHAR NULL,
        request_payload JSONB NULL,
        summary TEXT NULL,
        total_items INT NOT NULL DEFAULT 0,
        processed_items INT NOT NULL DEFAULT 0,
        success_items INT NOT NULL DEFAULT 0,
        failed_items INT NOT NULL DEFAULT 0,
        -- Scope snapshot: the submitting token's scope boundary, captured at
        -- enqueue time so async execution cannot exceed it even after the token
        -- is revoked.
        submitted_token_id INT NULL REFERENCES tokens (id) ON DELETE SET NULL,
        submitted_token_scoped BOOLEAN NOT NULL DEFAULT FALSE,
        submitted_token_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
        request_redacted_at TIMESTAMP NULL,
        started_at TIMESTAMP NULL,
        finished_at TIMESTAMP NULL,
        deleted_at TIMESTAMP NULL,
        deleted_by INT NULL REFERENCES principals (id) ON DELETE SET NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (submitted_by, idempotency_key)
    );

    CREATE TABLE import_task_results (
        id SERIAL PRIMARY KEY,
        task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL,
        item_ref VARCHAR NULL,
        entity_kind VARCHAR NOT NULL,
        action VARCHAR NOT NULL,
        identifier TEXT NULL,
        outcome VARCHAR NOT NULL,
        error TEXT NULL,
        details JSONB NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );

    -- Unified event & audit stream. Events intentionally do not hold foreign
    -- keys to domain tables: audit rows must survive deletion of the entity
    -- they describe, and the append-only trigger rejects FK-driven rewrites.
    CREATE TABLE events (
        id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL UNIQUE,
        occurred_at TIMESTAMP NOT NULL DEFAULT now(),
        entity_type TEXT NOT NULL,
        entity_id INTEGER NULL,
        entity_name TEXT NULL,
        collection_id INTEGER NULL,
        action TEXT NOT NULL,
        actor_user_id INTEGER NULL,
        actor_kind TEXT NOT NULL,
        request_id UUID NULL,
        correlation_id TEXT NULL,
        summary TEXT NOT NULL,
        "before" JSONB NULL,
        "after" JSONB NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version INTEGER NOT NULL DEFAULT 1,
        dispatched_at TIMESTAMP NULL,
        fanout_locked_until TIMESTAMP NULL,
        fanout_claim_token UUID NULL
    );

    CREATE TABLE event_sinks (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE,
        kind VARCHAR NOT NULL CHECK (kind IN ('webhook', 'amqp', 'valkey_stream', 'email')),
        config JSONB NOT NULL DEFAULT '{}'::jsonb,
        secret_ref VARCHAR NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        CHECK (jsonb_typeof(config) = 'object'),
        CHECK (secret_ref IS NULL OR length(trim(secret_ref)) > 0)
    );

    CREATE TABLE event_subscriptions (
        id SERIAL PRIMARY KEY,
        collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
        sink_id INT REFERENCES event_sinks (id) ON DELETE CASCADE NOT NULL,
        name VARCHAR NOT NULL,
        description VARCHAR NOT NULL DEFAULT '',
        entity_types JSONB NOT NULL,
        actions JSONB NOT NULL,
        filter JSONB NOT NULL DEFAULT '{}'::jsonb,
        routing JSONB NOT NULL DEFAULT '{}'::jsonb,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (collection_id, name),
        CHECK (jsonb_typeof(entity_types) = 'array'),
        CHECK (jsonb_array_length(entity_types) > 0),
        CHECK (jsonb_typeof(actions) = 'array'),
        CHECK (jsonb_array_length(actions) > 0),
        CHECK (jsonb_typeof(filter) = 'object'),
        CHECK (jsonb_typeof(routing) = 'object')
    );

    CREATE TABLE event_deliveries (
        id BIGSERIAL PRIMARY KEY,
        event_id BIGINT REFERENCES events (id) ON DELETE CASCADE NOT NULL,
        subscription_id INT REFERENCES event_subscriptions (id) ON DELETE CASCADE NOT NULL,
        status VARCHAR NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending', 'in_flight', 'succeeded', 'failed', 'dead')),
        attempts INT NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMP NOT NULL DEFAULT now(),
        last_error TEXT NULL,
        locked_until TIMESTAMP NULL,
        claim_token UUID NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        UNIQUE (event_id, subscription_id),
        CHECK (attempts >= 0)
    );

    CREATE TABLE report_task_outputs (
        id SERIAL PRIMARY KEY,
        task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL UNIQUE,
        template_name VARCHAR NULL,
        content_type VARCHAR NOT NULL,
        json_output JSONB NULL,
        text_output TEXT NULL,
        meta_json JSONB NOT NULL,
        warnings_json JSONB NOT NULL,
        warning_count INT NOT NULL DEFAULT 0,
        truncated BOOLEAN NOT NULL DEFAULT FALSE,
        output_expires_at TIMESTAMP NOT NULL,
        total_duration_ms INT NOT NULL DEFAULT 0,
        query_duration_ms INT NOT NULL DEFAULT 0,
        hydration_duration_ms INT NOT NULL DEFAULT 0,
        render_duration_ms INT NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        CHECK (
            (json_output IS NOT NULL AND text_output IS NULL)
            OR (json_output IS NULL AND text_output IS NOT NULL)
        )
    );

    CREATE TABLE remote_call_results (
        id SERIAL PRIMARY KEY,
        task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL UNIQUE,
        target_id INT REFERENCES remote_targets (id) ON DELETE SET NULL,
        subject_type VARCHAR NOT NULL CHECK (subject_type IN ('collection', 'class', 'object', 'class_relation', 'object_relation')),
        subject_id INT NOT NULL,
        method VARCHAR NOT NULL,
        rendered_url TEXT NOT NULL,
        response_status INT NULL,
        response_headers JSONB NULL,
        response_body_preview TEXT NULL,
        duration_ms INT NOT NULL DEFAULT 0,
        success BOOLEAN NOT NULL,
        error TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );

    ----------------------
    ---- Indexes
    ----------------------

    ---- Identity, groups, collections
    CREATE INDEX idx_principals_name ON principals(name);
    CREATE INDEX idx_principals_kind ON principals(kind);
    CREATE INDEX idx_groups_groupname ON groups(groupname);
    CREATE INDEX idx_group_memberships_principal_id ON group_memberships(principal_id);
    CREATE INDEX idx_group_memberships_group_id ON group_memberships(group_id);
    CREATE INDEX idx_service_accounts_owner_group_id ON service_accounts(owner_group_id);
    CREATE INDEX idx_service_accounts_disabled_at ON service_accounts(disabled_at) WHERE disabled_at IS NOT NULL;
    CREATE INDEX idx_collections_name ON collections(name);

    ---- Tokens
    CREATE INDEX idx_tokens_principal_id ON tokens(principal_id);
    CREATE INDEX idx_tokens_active ON tokens(token) WHERE revoked_at IS NULL;
    CREATE INDEX idx_tokens_principal_active_issued ON tokens(principal_id, issued DESC) WHERE revoked_at IS NULL;
    CREATE INDEX idx_token_scopes_token_id ON token_scopes(token_id);

    ---- Classes and objects
    CREATE INDEX idx_hubuumclass_collection_id ON hubuumclass(collection_id);
    CREATE INDEX idx_hubuumobject_collection_id ON hubuumobject(collection_id);
    CREATE INDEX idx_hubuumobject_hubuum_class_id ON hubuumobject(hubuum_class_id);

    ---- Permissions
    CREATE INDEX idx_permissions_collection_id ON permissions(collection_id);
    CREATE INDEX idx_permissions_group_id ON permissions(group_id);

    ---- Relations
    CREATE INDEX idx_hubuumclass_relation_on_from_to ON hubuumclass_relation (from_hubuum_class_id, to_hubuum_class_id);
    CREATE INDEX idx_hubuumclass_relation_on_to ON hubuumclass_relation (to_hubuum_class_id);
    CREATE INDEX idx_hubuumclass_reachability_ancestor ON hubuumclass_reachability (ancestor_class_id);
    CREATE INDEX idx_hubuumclass_reachability_descendant ON hubuumclass_reachability (descendant_class_id);
    CREATE INDEX idx_hubuumclass_reachability_ancestor_descendant ON hubuumclass_reachability (ancestor_class_id, descendant_class_id);
    CREATE INDEX idx_hubuumclass_reachability_path ON hubuumclass_reachability USING GIN (path);
    CREATE INDEX idx_hubuumobject_relation_on_from_to ON hubuumobject_relation (from_hubuum_object_id, to_hubuum_object_id);
    CREATE INDEX idx_hubuumobject_relation_on_to ON hubuumobject_relation (to_hubuum_object_id);
    CREATE INDEX idx_hubuumobject_relation_class_relation_id ON hubuumobject_relation (class_relation_id);

    ---- Report templates
    CREATE INDEX idx_report_templates_collection_id ON report_templates(collection_id);

    ---- Remote targets
    CREATE INDEX idx_remote_targets_collection_id ON remote_targets(collection_id);
    CREATE INDEX idx_remote_targets_class_id ON remote_targets(class_id);
    CREATE INDEX idx_remote_targets_enabled ON remote_targets(enabled);
    CREATE INDEX idx_remote_call_results_target_id ON remote_call_results(target_id);
    CREATE INDEX idx_remote_call_results_subject ON remote_call_results(subject_type, subject_id);

    ---- Search
    CREATE INDEX idx_hubuumobject_data_search
        ON hubuumobject
        USING GIN (jsonb_to_tsvector('simple', data, '["string"]'));

    ---- Tasks and imports
    CREATE INDEX idx_tasks_status_created_at ON tasks (status, created_at);
    CREATE INDEX idx_tasks_submitted_by ON tasks (submitted_by);
    CREATE INDEX idx_tasks_deleted_at ON tasks (deleted_at);
    CREATE INDEX idx_tasks_active_status ON tasks (deleted_at, status);
    CREATE INDEX idx_import_task_results_task_id_created_at ON import_task_results (task_id, created_at);
    CREATE INDEX idx_report_task_outputs_task_id_created_at ON report_task_outputs (task_id, created_at);
    CREATE INDEX idx_report_task_outputs_output_expires_at ON report_task_outputs (output_expires_at);

    ---- Events
    CREATE INDEX events_entity_idx ON events (entity_type, entity_id);
    CREATE INDEX events_collection_occurred_idx ON events (collection_id, occurred_at);
    CREATE INDEX events_occurred_idx ON events (occurred_at);
    CREATE INDEX events_metadata_gin_idx ON events USING GIN (metadata jsonb_path_ops);
    CREATE INDEX events_fanout_backlog_idx ON events (occurred_at) WHERE dispatched_at IS NULL;
    CREATE INDEX idx_event_sinks_enabled ON event_sinks(enabled);
    CREATE INDEX idx_event_subscriptions_collection_id ON event_subscriptions(collection_id);
    CREATE INDEX idx_event_subscriptions_sink_id ON event_subscriptions(sink_id);
    CREATE INDEX idx_event_subscriptions_enabled ON event_subscriptions(enabled);
    CREATE INDEX idx_event_deliveries_event_id ON event_deliveries(event_id);
    CREATE INDEX idx_event_deliveries_subscription_id ON event_deliveries(subscription_id);
    CREATE INDEX idx_event_deliveries_pending
        ON event_deliveries(next_attempt_at, id)
        WHERE status IN ('pending', 'failed');
    CREATE INDEX idx_event_deliveries_in_flight_locks
        ON event_deliveries(locked_until)
        WHERE status = 'in_flight';

    ----------------------
    ---- Functions
    ----------------------

    CREATE OR REPLACE FUNCTION try_inet(value TEXT)
    RETURNS inet
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        RETURN value::inet;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
    $$;

    CREATE OR REPLACE FUNCTION try_numeric(value TEXT)
    RETURNS numeric
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        RETURN value::numeric;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
    $$;

    CREATE OR REPLACE FUNCTION try_boolean(value TEXT)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        RETURN value::boolean;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
    $$;

    CREATE OR REPLACE FUNCTION try_timestamp(value TEXT)
    RETURNS timestamp
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        BEGIN
            RETURN value::timestamptz AT TIME ZONE 'UTC';
        EXCEPTION
            WHEN OTHERS THEN
                RETURN value::timestamp;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
    $$;

    CREATE OR REPLACE FUNCTION jsonb_contains_any(val jsonb, items text[])
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        IF jsonb_typeof(val) = 'array' THEN
            RETURN EXISTS (
                SELECT 1 FROM jsonb_array_elements_text(val) AS elem
                WHERE elem = ANY(items)
            );
        END IF;
        RETURN false;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN false;
    END;
    $$;

    CREATE OR REPLACE FUNCTION jsonb_contains_all(val jsonb, items text[])
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        IF jsonb_typeof(val) = 'array' THEN
            RETURN (
                SELECT count(DISTINCT elem) FROM jsonb_array_elements_text(val) AS elem
                WHERE elem = ANY(items)
            ) = array_length(items, 1);
        END IF;
        RETURN false;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN false;
    END;
    $$;

    CREATE OR REPLACE FUNCTION jsonb_has_key(val jsonb, k text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    AS $$
    BEGIN
        RETURN val ? k;
    EXCEPTION
        WHEN OTHERS THEN
        RETURN false;
    END;
    $$;

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

    CREATE OR REPLACE FUNCTION notify_events_fanout()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('hubuum_events_fanout', NEW.id::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE
      hist text := quote_ident(TG_TABLE_NAME || '_history');
      seq text := quote_literal(TG_TABLE_NAME || '_history_seq');
      ts timestamptz := clock_timestamp();
      actor int := nullif(current_setting('hubuum.actor_id', true), '')::int;
      base_cols text;
      hist_cols text;
    BEGIN
      SELECT string_agg(format('($1).%1$I', a.attname), ', ' ORDER BY a.attnum),
             string_agg(format('%1$I', a.attname), ', ' ORDER BY a.attnum)
        INTO base_cols, hist_cols
      FROM pg_attribute a
      WHERE a.attrelid = TG_RELID
        AND a.attnum > 0
        AND NOT a.attisdropped;

      IF TG_OP = 'INSERT' THEN
        EXECUTE format(
          'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id)
           SELECT %s, %L, $2, NULL, $3, nextval(%s)',
          hist, hist_cols, base_cols, 'I', seq)
          USING NEW, ts, actor;
        RETURN NEW;
      ELSIF TG_OP = 'UPDATE' THEN
        EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
          USING ts, OLD.id;
        EXECUTE format(
          'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id)
           SELECT %s, %L, $2, NULL, $3, nextval(%s)',
          hist, hist_cols, base_cols, 'U', seq)
          USING NEW, ts, actor;
        RETURN NEW;
      ELSE
        EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
          USING ts, OLD.id;
        EXECUTE format(
          'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id)
           SELECT %s, %L, $2, $2, $3, nextval(%s)',
          hist, hist_cols, base_cols, 'D', seq)
          USING OLD, ts, actor;
        RETURN OLD;
      END IF;
    END; $$;

    CREATE FUNCTION hubuum_skip_unchanged_temporal_update() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF to_jsonb(OLD) - 'updated_at' = to_jsonb(NEW) - 'updated_at' THEN
        RETURN NULL;
      END IF;
      RETURN NEW;
    END; $$;

    -- In relation tables, ensure that the from entry is always less than the to entry, this ensures
    -- that we don't need to check for both directions when querying the database
    CREATE OR REPLACE FUNCTION enforce_class_relation_order()
    RETURNS TRIGGER AS $$
    DECLARE
        temp INT;
        temp_alias VARCHAR;
    BEGIN
        IF NEW.from_hubuum_class_id > NEW.to_hubuum_class_id THEN
            -- Swap the IDs if they are in the wrong order
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

    CREATE OR REPLACE FUNCTION reverse_integer_array(path_input INT[])
    RETURNS INT[] AS $$
        SELECT COALESCE(
            ARRAY(
                SELECT element
                FROM unnest(path_input) WITH ORDINALITY AS values_with_ord(element, ord)
                ORDER BY ord DESC
            ),
            ARRAY[]::INT[]
        );
    $$ LANGUAGE sql IMMUTABLE;

    CREATE OR REPLACE FUNCTION rebuild_class_reachability_cache()
    RETURNS VOID AS $$
    BEGIN
        -- Class-relation writes are rare, so serialize cache rebuilds to avoid
        -- deadlocks under parallel test execution while keeping reads lock-free.
        PERFORM pg_advisory_xact_lock(214748301);

        DELETE FROM hubuumclass_reachability;

        INSERT INTO hubuumclass_reachability (
            ancestor_class_id,
            descendant_class_id,
            depth,
            path
        )
        WITH RECURSIVE class_edges AS (
            SELECT from_hubuum_class_id AS source_class_id, to_hubuum_class_id AS target_class_id
            FROM hubuumclass_relation

            UNION ALL

            SELECT to_hubuum_class_id AS source_class_id, from_hubuum_class_id AS target_class_id
            FROM hubuumclass_relation
        ),
        graph_walk AS (
            SELECT
                source_class_id AS start_class_id,
                target_class_id AS end_class_id,
                1 AS depth,
                ARRAY[source_class_id, target_class_id] AS path
            FROM class_edges

            UNION ALL

            SELECT
                graph_walk.start_class_id,
                class_edges.target_class_id AS end_class_id,
                graph_walk.depth + 1,
                graph_walk.path || class_edges.target_class_id
            FROM graph_walk
            JOIN class_edges
              ON class_edges.source_class_id = graph_walk.end_class_id
            WHERE NOT (class_edges.target_class_id = ANY(graph_walk.path))
        ),
        canonical_walk AS (
            SELECT
                LEAST(start_class_id, end_class_id) AS ancestor_class_id,
                GREATEST(start_class_id, end_class_id) AS descendant_class_id,
                depth,
                CASE
                    WHEN start_class_id < end_class_id THEN path
                    ELSE reverse_integer_array(path)
                END AS path
            FROM graph_walk
        ),
        deduped_walk AS (
            SELECT DISTINCT ON (ancestor_class_id, descendant_class_id)
                ancestor_class_id,
                descendant_class_id,
                depth,
                path
            FROM canonical_walk
            ORDER BY ancestor_class_id ASC, descendant_class_id ASC, depth ASC, path ASC
        )
        SELECT
            ancestor_class_id,
            descendant_class_id,
            depth,
            path
        FROM deduped_walk;
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION refresh_class_reachability_cache()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM rebuild_class_reachability_cache();
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    -- Function to cleanup invalid object relations from the hubuumobject_relation table
    -- after a class relation has been deleted. This rebuilds the reachability cache once
    -- per statement and scopes cleanup to the deleted class pairs.
    CREATE OR REPLACE FUNCTION cleanup_invalid_object_relations()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM rebuild_class_reachability_cache();

        DELETE FROM hubuumobject_relation oor
        USING hubuumobject o1, hubuumobject o2
        WHERE o1.id = oor.from_hubuum_object_id
          AND o2.id = oor.to_hubuum_object_id
          AND EXISTS (
              SELECT 1
              FROM deleted_relations
              WHERE deleted_relations.from_hubuum_class_id = LEAST(o1.hubuum_class_id, o2.hubuum_class_id)
                AND deleted_relations.to_hubuum_class_id = GREATEST(o1.hubuum_class_id, o2.hubuum_class_id)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM hubuumclass_reachability
              WHERE ancestor_class_id = LEAST(o1.hubuum_class_id, o2.hubuum_class_id)
                AND descendant_class_id = GREATEST(o1.hubuum_class_id, o2.hubuum_class_id)
          );
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION get_transitively_linked_objects(
        start_object_id INT,
        target_class_id INT,
        valid_collection_ids INT[],
        max_depth INT DEFAULT 100
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

        IF start_class_id IS NULL THEN
            RETURN;
        END IF;

        IF start_class_id <> target_class_id
           AND NOT EXISTS (
               SELECT 1
               FROM hubuumclass_reachability
               WHERE ancestor_class_id = LEAST(start_class_id, target_class_id)
                 AND descendant_class_id = GREATEST(start_class_id, target_class_id)
           ) THEN
            RETURN;
        END IF;

        RETURN QUERY
        WITH RECURSIVE object_edges AS (
            SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
            FROM hubuumobject_relation

            UNION ALL

            SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
            FROM hubuumobject_relation
        ),
        graph_walk AS (
            SELECT
                start_object_id AS ancestor_object_id,
                object_edges.target_object_id AS descendant_object_id,
                1 AS depth,
                ARRAY[start_object_id, object_edges.target_object_id] AS traversal_path
            FROM object_edges
            JOIN hubuumobject target_object
              ON target_object.id = object_edges.target_object_id
            WHERE object_edges.source_object_id = start_object_id
              AND (max_depth IS NULL OR max_depth >= 1)
              AND (
                  COALESCE(cardinality(valid_collection_ids), 0) = 0
                  OR target_object.collection_id = ANY(valid_collection_ids)
              )
              AND (
                  target_object.hubuum_class_id = target_class_id
                  OR EXISTS (
                      SELECT 1
                      FROM hubuumclass_reachability
                      WHERE ancestor_class_id = LEAST(target_object.hubuum_class_id, target_class_id)
                        AND descendant_class_id = GREATEST(target_object.hubuum_class_id, target_class_id)
                  )
              )

            UNION ALL

            SELECT
                graph_walk.ancestor_object_id,
                object_edges.target_object_id AS descendant_object_id,
                graph_walk.depth + 1,
                graph_walk.traversal_path || object_edges.target_object_id
            FROM graph_walk
            JOIN object_edges
              ON object_edges.source_object_id = graph_walk.descendant_object_id
            JOIN hubuumobject target_object
              ON target_object.id = object_edges.target_object_id
            WHERE NOT (object_edges.target_object_id = ANY(graph_walk.traversal_path))
              AND (max_depth IS NULL OR graph_walk.depth < max_depth)
              AND (
                  COALESCE(cardinality(valid_collection_ids), 0) = 0
                  OR target_object.collection_id = ANY(valid_collection_ids)
              )
              AND (
                  target_object.hubuum_class_id = target_class_id
                  OR EXISTS (
                      SELECT 1
                      FROM hubuumclass_reachability
                      WHERE ancestor_class_id = LEAST(target_object.hubuum_class_id, target_class_id)
                        AND descendant_class_id = GREATEST(target_object.hubuum_class_id, target_class_id)
                  )
              )
        ),
        deduped_walk AS (
            SELECT DISTINCT ON (descendant_object_id)
                descendant_object_id,
                depth,
                traversal_path
            FROM graph_walk
            ORDER BY descendant_object_id ASC, depth ASC, traversal_path ASC
        )
        SELECT
            deduped_walk.descendant_object_id AS target_object_id,
            deduped_walk.traversal_path AS path
        FROM deduped_walk
        JOIN hubuumobject target_object ON target_object.id = deduped_walk.descendant_object_id
        WHERE target_object.hubuum_class_id = target_class_id;
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION get_bidirectionally_related_objects(
        start_object_id INT,
        valid_collection_ids INT[],
        max_depth INT
    )
    RETURNS TABLE (
        ancestor_object_id INT,
        descendant_object_id INT,
        depth INT,
        path INT[],
        ancestor_name VARCHAR,
        descendant_name VARCHAR,
        ancestor_collection_id INT,
        descendant_collection_id INT,
        ancestor_class_id INT,
        descendant_class_id INT,
        ancestor_description VARCHAR,
        descendant_description VARCHAR,
        ancestor_data JSONB,
        descendant_data JSONB,
        ancestor_created_at TIMESTAMP,
        descendant_created_at TIMESTAMP,
        ancestor_updated_at TIMESTAMP,
        descendant_updated_at TIMESTAMP
    ) AS $$
        WITH RECURSIVE object_edges AS (
            SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
            FROM hubuumobject_relation

            UNION ALL

            SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
            FROM hubuumobject_relation
        ),
        graph_walk AS (
            SELECT
                start_object_id AS ancestor_object_id,
                object_edges.target_object_id AS descendant_object_id,
                1 AS depth,
                ARRAY[start_object_id, object_edges.target_object_id] AS path
            FROM object_edges
            JOIN hubuumobject target_object
              ON target_object.id = object_edges.target_object_id
            WHERE object_edges.source_object_id = start_object_id
              AND (max_depth IS NULL OR max_depth >= 1)
              AND (
                  COALESCE(cardinality(valid_collection_ids), 0) = 0
                  OR target_object.collection_id = ANY(valid_collection_ids)
              )

            UNION ALL

            SELECT
                graph_walk.ancestor_object_id,
                object_edges.target_object_id AS descendant_object_id,
                graph_walk.depth + 1,
                graph_walk.path || object_edges.target_object_id
            FROM graph_walk
            JOIN object_edges
              ON object_edges.source_object_id = graph_walk.descendant_object_id
            JOIN hubuumobject target_object
              ON target_object.id = object_edges.target_object_id
            WHERE NOT (object_edges.target_object_id = ANY(graph_walk.path))
              AND (max_depth IS NULL OR graph_walk.depth < max_depth)
              AND (
                  COALESCE(cardinality(valid_collection_ids), 0) = 0
                  OR target_object.collection_id = ANY(valid_collection_ids)
              )
        ),
        deduped_walk AS (
            SELECT DISTINCT ON (descendant_object_id)
                ancestor_object_id,
                descendant_object_id,
                depth,
                path
            FROM graph_walk
            ORDER BY descendant_object_id ASC, depth ASC, path ASC
        )
        SELECT
            source_object.id AS ancestor_object_id,
            target_object.id AS descendant_object_id,
            deduped_walk.depth,
            deduped_walk.path,
            source_object.name AS ancestor_name,
            target_object.name AS descendant_name,
            source_object.collection_id AS ancestor_collection_id,
            target_object.collection_id AS descendant_collection_id,
            source_object.hubuum_class_id AS ancestor_class_id,
            target_object.hubuum_class_id AS descendant_class_id,
            source_object.description AS ancestor_description,
            target_object.description AS descendant_description,
            source_object.data AS ancestor_data,
            target_object.data AS descendant_data,
            source_object.created_at AS ancestor_created_at,
            target_object.created_at AS descendant_created_at,
            source_object.updated_at AS ancestor_updated_at,
            target_object.updated_at AS descendant_updated_at
        FROM deduped_walk
        JOIN hubuumobject source_object ON source_object.id = deduped_walk.ancestor_object_id
        JOIN hubuumobject target_object ON target_object.id = deduped_walk.descendant_object_id
        WHERE (
                COALESCE(cardinality(valid_collection_ids), 0) = 0
                OR source_object.collection_id = ANY(valid_collection_ids)
              )
          AND (
                COALESCE(cardinality(valid_collection_ids), 0) = 0
                OR target_object.collection_id = ANY(valid_collection_ids)
              );
    $$ LANGUAGE sql STABLE;

    CREATE OR REPLACE FUNCTION get_bidirectionally_related_classes(
        start_class_id INT,
        valid_collection_ids INT[],
        max_depth INT,
        filter_depth_op TEXT DEFAULT NULL,
        filter_depth_values INT[] DEFAULT NULL,
        filter_depth_negated BOOLEAN DEFAULT FALSE,
        filter_path_op TEXT DEFAULT NULL,
        filter_path_values INT[] DEFAULT NULL,
        filter_path_negated BOOLEAN DEFAULT FALSE
    )
    RETURNS TABLE (
        ancestor_class_id INT,
        descendant_class_id INT,
        depth INT,
        path INT[],
        ancestor_name VARCHAR,
        descendant_name VARCHAR,
        ancestor_collection_id INT,
        descendant_collection_id INT,
        ancestor_json_schema JSONB,
        descendant_json_schema JSONB,
        ancestor_validate_schema BOOLEAN,
        descendant_validate_schema BOOLEAN,
        ancestor_description VARCHAR,
        descendant_description VARCHAR,
        ancestor_created_at TIMESTAMP,
        descendant_created_at TIMESTAMP,
        ancestor_updated_at TIMESTAMP,
        descendant_updated_at TIMESTAMP
    ) AS $$
        WITH related_classes AS (
            SELECT
                start_class_id AS ancestor_class_id,
                CASE
                    WHEN hubuumclass_reachability.ancestor_class_id = start_class_id THEN hubuumclass_reachability.descendant_class_id
                    ELSE hubuumclass_reachability.ancestor_class_id
                END AS descendant_class_id,
                hubuumclass_reachability.depth,
                CASE
                    WHEN hubuumclass_reachability.ancestor_class_id = start_class_id THEN hubuumclass_reachability.path
                    ELSE reverse_integer_array(hubuumclass_reachability.path)
                END AS path
            FROM hubuumclass_reachability
            WHERE (
                    hubuumclass_reachability.ancestor_class_id = start_class_id
                    OR hubuumclass_reachability.descendant_class_id = start_class_id
                  )
              AND (
                    max_depth IS NULL
                    OR hubuumclass_reachability.depth <= max_depth
                  )
        )
        SELECT
            source_class.id AS ancestor_class_id,
            target_class.id AS descendant_class_id,
            related_classes.depth,
            related_classes.path,
            source_class.name AS ancestor_name,
            target_class.name AS descendant_name,
            source_class.collection_id AS ancestor_collection_id,
            target_class.collection_id AS descendant_collection_id,
            source_class.json_schema AS ancestor_json_schema,
            target_class.json_schema AS descendant_json_schema,
            source_class.validate_schema AS ancestor_validate_schema,
            target_class.validate_schema AS descendant_validate_schema,
            source_class.description AS ancestor_description,
            target_class.description AS descendant_description,
            source_class.created_at AS ancestor_created_at,
            target_class.created_at AS descendant_created_at,
            source_class.updated_at AS ancestor_updated_at,
            target_class.updated_at AS descendant_updated_at
        FROM related_classes
        JOIN hubuumclass source_class ON source_class.id = related_classes.ancestor_class_id
        JOIN hubuumclass target_class ON target_class.id = related_classes.descendant_class_id
        WHERE (
                COALESCE(cardinality(valid_collection_ids), 0) = 0
                OR source_class.collection_id = ANY(valid_collection_ids)
              )
          AND (
                COALESCE(cardinality(valid_collection_ids), 0) = 0
                OR target_class.collection_id = ANY(valid_collection_ids)
              )
          AND (
                filter_depth_op IS NULL
                OR (
                    (CASE filter_depth_op
                        WHEN 'equals' THEN related_classes.depth = ANY(filter_depth_values)
                        WHEN 'gt' THEN related_classes.depth > (SELECT MAX(v) FROM unnest(filter_depth_values) AS v)
                        WHEN 'gte' THEN related_classes.depth >= (SELECT MAX(v) FROM unnest(filter_depth_values) AS v)
                        WHEN 'lt' THEN related_classes.depth < (SELECT MIN(v) FROM unnest(filter_depth_values) AS v)
                        WHEN 'lte' THEN related_classes.depth <= (SELECT MIN(v) FROM unnest(filter_depth_values) AS v)
                        WHEN 'between' THEN (
                            cardinality(filter_depth_values) >= 2
                            AND related_classes.depth BETWEEN filter_depth_values[1] AND filter_depth_values[2]
                        )
                        ELSE FALSE
                    END) != filter_depth_negated
                )
              )
          AND (
                filter_path_op IS NULL
                OR (
                    (CASE filter_path_op
                        WHEN 'contains' THEN related_classes.path @> filter_path_values
                        WHEN 'equals' THEN related_classes.path = filter_path_values
                        ELSE FALSE
                    END) != filter_path_negated
                )
              );
    $$ LANGUAGE sql STABLE;

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

    DROP TRIGGER IF EXISTS update_principals_updated_at ON principals;
    CREATE TRIGGER update_principals_updated_at
    BEFORE UPDATE ON principals
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_users_updated_at ON users;
    CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_service_accounts_updated_at ON service_accounts;
    CREATE TRIGGER update_service_accounts_updated_at
    BEFORE UPDATE ON service_accounts
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_groups_updated_at ON groups;
    CREATE TRIGGER update_groups_updated_at
    BEFORE UPDATE ON groups
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_group_memberships_updated_at ON group_memberships;
    CREATE TRIGGER update_group_memberships_updated_at
    BEFORE UPDATE ON group_memberships
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_collections_updated_at ON collections;
    CREATE TRIGGER update_collections_updated_at
    BEFORE UPDATE ON collections
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

    -- Trigger to enforce valid object relations
    DROP TRIGGER IF EXISTS check_object_relation ON hubuumobject_relation;
    CREATE TRIGGER check_object_relation
    BEFORE INSERT OR UPDATE ON hubuumobject_relation
    FOR EACH ROW EXECUTE FUNCTION validate_object_relation();

    -- Keep the class reachability cache up to date on class relation writes.
    DROP TRIGGER IF EXISTS refresh_class_reachability_cache ON hubuumclass_relation;
    CREATE TRIGGER refresh_class_reachability_cache
    AFTER INSERT OR UPDATE ON hubuumclass_relation
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_class_reachability_cache();

    -- Trigger to cleanup invalid object relations after class relation deletes.
    DROP TRIGGER IF EXISTS cleanup_object_relations ON hubuumclass_relation;
    CREATE TRIGGER cleanup_object_relations
    AFTER DELETE ON hubuumclass_relation
    REFERENCING OLD TABLE AS deleted_relations
    FOR EACH STATEMENT EXECUTE FUNCTION cleanup_invalid_object_relations();

    -- Trigger to update report_templates updated_at column
    DROP TRIGGER IF EXISTS update_report_templates_updated_at ON report_templates;
    CREATE TRIGGER update_report_templates_updated_at
    BEFORE UPDATE ON report_templates
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_remote_targets_updated_at ON remote_targets;
    CREATE TRIGGER update_remote_targets_updated_at
    BEFORE UPDATE ON remote_targets
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DROP TRIGGER IF EXISTS update_tasks_updated_at ON tasks;
    CREATE TRIGGER update_tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    CREATE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION enforce_events_append_only();

    CREATE TRIGGER events_fanout_notify
    AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION notify_events_fanout();

    CREATE TRIGGER update_event_sinks_updated_at
    BEFORE UPDATE ON event_sinks
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    CREATE TRIGGER update_event_subscriptions_updated_at
    BEFORE UPDATE ON event_subscriptions
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    CREATE TRIGGER update_event_deliveries_updated_at
    BEFORE UPDATE ON event_deliveries
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

    DO $$
    DECLARE
      t text;
      ts timestamptz := transaction_timestamp();
    BEGIN
      FOREACH t IN ARRAY ARRAY[
        'hubuumclass','hubuumobject','collections','hubuumclass_relation',
        'hubuumobject_relation','report_templates','remote_targets'
      ]
      LOOP
        EXECUTE format(
          'CREATE TABLE %1$I_history (
             LIKE %1$I,
             op varchar NOT NULL CHECK (op IN (''I'',''U'',''D'')),
             valid_from timestamptz NOT NULL,
             valid_to timestamptz,
             actor_id int,
             history_id bigint NOT NULL
           )', t);
        EXECUTE format('CREATE SEQUENCE %1$I_history_seq OWNED BY %1$I_history.history_id', t);
        EXECUTE format('ALTER TABLE %1$I_history ADD PRIMARY KEY (history_id)', t);
        EXECUTE format('CREATE INDEX %1$I_history_id_from_idx ON %1$I_history (id, valid_from)', t);
        EXECUTE format('CREATE INDEX %1$I_history_actor_idx ON %1$I_history (actor_id)', t);
        EXECUTE format(
          'CREATE TRIGGER %1$I_history_trg AFTER INSERT OR UPDATE OR DELETE ON %1$I
           FOR EACH ROW EXECUTE FUNCTION hubuum_record_history()', t);
        EXECUTE format(
          'INSERT INTO %1$I_history
           SELECT base.*, %2$L, $1, NULL, NULL, nextval(%3$L)
           FROM %1$I base',
          t, 'I', t || '_history_seq')
          USING ts;
      END LOOP;

      FOREACH t IN ARRAY ARRAY[
        'hubuumclass','hubuumobject','collections','report_templates','remote_targets'
      ]
      LOOP
        EXECUTE format(
          'CREATE TRIGGER %1$I_skip_unchanged_temporal_update_trg
           BEFORE UPDATE ON %1$I
           FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_temporal_update()', t);
      END LOOP;
    END $$;
