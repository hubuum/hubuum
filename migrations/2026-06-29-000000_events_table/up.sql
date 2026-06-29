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
