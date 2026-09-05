CREATE OR REPLACE FUNCTION enforce_events_append_only()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        -- Runtime has no direct DELETE grant. Its only path here is the
        -- allowlisted retention function, which sets this transaction-local
        -- flag after validating and locking a durable bounded claim.
        IF pg_catalog.current_setting('events.allow_purge', true) IS DISTINCT FROM 'on' THEN
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
$function$;

ALTER TABLE events
    DROP CONSTRAINT events_correlation_id_bounded,
    DROP CONSTRAINT events_trace_link_valid,
    DROP CONSTRAINT events_trace_link_complete,
    DROP COLUMN trace_context_version,
    DROP COLUMN trace_flags,
    DROP COLUMN trace_span_id,
    DROP COLUMN trace_id;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_trace_link_valid,
    DROP CONSTRAINT tasks_trace_link_complete,
    DROP COLUMN trace_context_version,
    DROP COLUMN trace_flags,
    DROP COLUMN trace_span_id,
    DROP COLUMN trace_id;
