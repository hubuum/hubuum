-- Unified event & audit stream (#70/#71).
-- The `events` table is the single source of truth for both the internal audit
-- log and external event delivery (fan-out -> deliveries -> sinks).
--
-- Core property: an event is recorded iff its database transaction commits.
-- `emit_event` appends exactly one row inside the caller's `with_transaction`
-- block, so the event rolls back together with the domain mutation on failure.
--
-- Referential integrity note (deliberate):
-- The events table has NO foreign keys to domain tables. An audit log must
-- outlive the entities it references: a namespace or user deletion must not
-- rewrite or drop audit rows, and audit readers (#74) need the id values to
-- remain for scoping long after the referenced row is gone. The append-only
-- trigger (below) would in any case reject the SET NULL updates a FK with
-- ON DELETE SET NULL would issue. Keep actor/namespace/entity ids as plain
-- nullable integers with indexes instead.

CREATE TABLE events (
    id                  BIGSERIAL PRIMARY KEY,
    event_id            UUID        NOT NULL UNIQUE,
    occurred_at         TIMESTAMP   NOT NULL DEFAULT now(),
    entity_type         TEXT        NOT NULL,
    entity_id           INTEGER     NULL,
    entity_name         TEXT        NULL,
    namespace_id        INTEGER     NULL,
    action              TEXT        NOT NULL,
    actor_user_id       INTEGER     NULL,
    actor_kind          TEXT        NOT NULL,
    request_id          UUID        NULL,
    correlation_id      TEXT        NULL,
    summary             TEXT        NOT NULL,
    "before"            JSONB       NULL,
    "after"             JSONB       NULL,
    metadata            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    schema_version      INTEGER     NOT NULL DEFAULT 1,
    -- fan-out claim fields (Phase 2 worker recovery)
    dispatched_at        TIMESTAMP   NULL,
    fanout_locked_until  TIMESTAMP   NULL,
    fanout_claim_token   UUID        NULL
);

-- Audit / delivery lookups
CREATE INDEX events_entity_idx          ON events (entity_type, entity_id);
CREATE INDEX events_namespace_occurred_idx ON events (namespace_id, occurred_at);
CREATE INDEX events_occurred_idx        ON events (occurred_at);
CREATE INDEX events_metadata_gin_idx    ON events USING GIN (metadata jsonb_path_ops);
-- Partial backlog index for the hot fan-out "undispatched events" query (#76)
CREATE INDEX events_fanout_backlog_idx  ON events (occurred_at) WHERE dispatched_at IS NULL;

-- Append-only guard: reject normal UPDATE/DELETE.
-- Only the fan-out claim fields (fanout_locked_until, fanout_claim_token) and
-- dispatched_at may be updated. DELETE is permitted only by the privileged
-- retention purge path, which sets a transaction-local guard first.
CREATE OR REPLACE FUNCTION enforce_events_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF current_setting('events.allow_purge', true) IS DISTINCT FROM 'on' THEN
            RAISE EXCEPTION 'events table is append-only: DELETE is not permitted';
        END IF;
        RETURN OLD;
    END IF;

    -- UPDATE: reject if any column other than the fan-out claim fields or
    -- dispatched_at changed. Those mutable columns are intentionally absent
    -- from the checks below.
    IF NEW.id               IS DISTINCT FROM OLD.id
       OR NEW.event_id      IS DISTINCT FROM OLD.event_id
       OR NEW.occurred_at   IS DISTINCT FROM OLD.occurred_at
       OR NEW.entity_type   IS DISTINCT FROM OLD.entity_type
       OR NEW.entity_id     IS DISTINCT FROM OLD.entity_id
       OR NEW.entity_name   IS DISTINCT FROM OLD.entity_name
       OR NEW.namespace_id  IS DISTINCT FROM OLD.namespace_id
       OR NEW.action        IS DISTINCT FROM OLD.action
       OR NEW.actor_user_id IS DISTINCT FROM OLD.actor_user_id
       OR NEW.actor_kind    IS DISTINCT FROM OLD.actor_kind
       OR NEW.request_id    IS DISTINCT FROM OLD.request_id
       OR NEW.correlation_id IS DISTINCT FROM OLD.correlation_id
       OR NEW.summary       IS DISTINCT FROM OLD.summary
       OR NEW.before        IS DISTINCT FROM OLD.before
       OR NEW.after         IS DISTINCT FROM OLD.after
       OR NEW.metadata      IS DISTINCT FROM OLD.metadata
       OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    THEN
        RAISE EXCEPTION 'events table is append-only: only fan-out claim fields and dispatched_at may be updated';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_append_only
BEFORE UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION enforce_events_append_only();

-- Consolidate task lifecycle history into the unified events stream (#87).
--
-- Existing task_events rows are backfilled into events, preserving the public
-- task-history response fields as:
--   event_type -> events.action
--   message    -> events.summary
--   data       -> events.metadata.data
CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO events (
    event_id,
    occurred_at,
    entity_type,
    entity_id,
    action,
    actor_user_id,
    actor_kind,
    summary,
    metadata,
    schema_version
)
SELECT
    gen_random_uuid(),
    task_events.created_at,
    'task',
    task_events.task_id,
    task_events.event_type,
    CASE WHEN task_events.event_type = 'queued' THEN tasks.submitted_by ELSE NULL END,
    CASE
        WHEN task_events.event_type = 'queued' THEN 'user'
        WHEN task_events.event_type = 'cleanup' THEN 'system'
        ELSE 'worker'
    END,
    task_events.message,
    jsonb_strip_nulls(jsonb_build_object(
        'task_id', task_events.task_id,
        'task_kind', tasks.kind,
        'data', task_events.data
    )),
    1
FROM task_events
JOIN tasks ON tasks.id = task_events.task_id;

DROP TABLE task_events;

ALTER TABLE permissions
    ADD COLUMN has_read_audit BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_manage_event_subscription BOOLEAN NOT NULL DEFAULT FALSE;

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
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
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
    UNIQUE (namespace_id, name),
    CHECK (jsonb_typeof(entity_types) = 'array'),
    CHECK (jsonb_array_length(entity_types) > 0),
    CHECK (jsonb_typeof(actions) = 'array'),
    CHECK (jsonb_array_length(actions) > 0),
    CHECK (jsonb_typeof(filter) = 'object'),
    CHECK (jsonb_typeof(routing) = 'object')
);

CREATE INDEX idx_event_sinks_enabled ON event_sinks(enabled);
CREATE INDEX idx_event_subscriptions_namespace_id ON event_subscriptions(namespace_id);
CREATE INDEX idx_event_subscriptions_sink_id ON event_subscriptions(sink_id);
CREATE INDEX idx_event_subscriptions_enabled ON event_subscriptions(enabled);

CREATE TRIGGER update_event_sinks_updated_at
BEFORE UPDATE ON event_sinks
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_event_subscriptions_updated_at
BEFORE UPDATE ON event_subscriptions
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

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

CREATE INDEX idx_event_deliveries_event_id ON event_deliveries(event_id);
CREATE INDEX idx_event_deliveries_subscription_id ON event_deliveries(subscription_id);
CREATE INDEX idx_event_deliveries_pending
    ON event_deliveries(next_attempt_at, id)
    WHERE status IN ('pending', 'failed');
CREATE INDEX idx_event_deliveries_in_flight_locks
    ON event_deliveries(locked_until)
    WHERE status = 'in_flight';

CREATE TRIGGER update_event_deliveries_updated_at
BEFORE UPDATE ON event_deliveries
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE OR REPLACE FUNCTION notify_events_fanout()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('hubuum_events_fanout', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_fanout_notify
AFTER INSERT ON events
FOR EACH ROW
EXECUTE FUNCTION notify_events_fanout();
