-- Make temporal history writes possible without granting the runtime role
-- direct mutation authority. The trusted restore bypass is available only to
-- a session whose login role is a member of this function's owning role.
CREATE OR REPLACE FUNCTION hubuum_record_history()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
  history_relation text := pg_catalog.format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME || '_history');
  history_sequence text := pg_catalog.format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME || '_history_seq');
  changed_at timestamptz := pg_catalog.clock_timestamp();
  actor int := nullif(pg_catalog.current_setting('hubuum.actor_id', true), '')::int;
  actor_kind_value text := nullif(pg_catalog.current_setting('hubuum.actor_kind', true), '');
  initiator int := nullif(pg_catalog.current_setting('hubuum.initiator_user_id', true), '')::int;
  provenance_task_id int := nullif(pg_catalog.current_setting('hubuum.task_id', true), '')::int;
  base_columns text;
  history_columns text;
  trusted_restore boolean :=
    pg_catalog.current_setting('hubuum.restore_history', true) IS NOT DISTINCT FROM 'on'
    AND pg_catalog.pg_has_role(session_user, current_user, 'MEMBER');
BEGIN
  IF actor_kind_value IS NULL AND actor IS NOT NULL THEN
    actor_kind_value := 'user';
  END IF;

  IF trusted_restore THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NEW;
  END IF;

  SELECT pg_catalog.string_agg(pg_catalog.format('($1).%1$I', attribute.attname), ', ' ORDER BY attribute.attnum),
         pg_catalog.string_agg(pg_catalog.format('%1$I', attribute.attname), ', ' ORDER BY attribute.attnum)
    INTO base_columns, history_columns
  FROM pg_catalog.pg_attribute attribute
  WHERE attribute.attrelid = TG_RELID
    AND attribute.attnum > 0
    AND NOT attribute.attisdropped;

  IF TG_OP = 'INSERT' THEN
    EXECUTE pg_catalog.format(
      'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id,
                       actor_kind, initiator_user_id, task_id)
       SELECT %s, %L, $2, NULL, $3, pg_catalog.nextval(%L::pg_catalog.regclass), $4, $5, $6',
      history_relation, history_columns, base_columns, 'I', history_sequence)
      USING NEW, changed_at, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE pg_catalog.format(
      'UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL',
      history_relation)
      USING changed_at, OLD.id;
    EXECUTE pg_catalog.format(
      'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id,
                       actor_kind, initiator_user_id, task_id)
       SELECT %s, %L, $2, NULL, $3, pg_catalog.nextval(%L::pg_catalog.regclass), $4, $5, $6',
      history_relation, history_columns, base_columns, 'U', history_sequence)
      USING NEW, changed_at, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN NEW;
  ELSE
    EXECUTE pg_catalog.format(
      'UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL',
      history_relation)
      USING changed_at, OLD.id;
    EXECUTE pg_catalog.format(
      'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id,
                       actor_kind, initiator_user_id, task_id)
       SELECT %s, %L, $2, $2, $3, pg_catalog.nextval(%L::pg_catalog.regclass), $4, $5, $6',
      history_relation, history_columns, base_columns, 'D', history_sequence)
      USING OLD, changed_at, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN OLD;
  END IF;
END;
$function$;

-- A caller may opt into restore revisions only when its login role belongs to
-- the table owner. Internal aggregate bumps run from the owner-controlled
-- SECURITY DEFINER trigger below and are distinguished by current_user.
CREATE OR REPLACE FUNCTION hubuum_manage_resource_revision()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    relation_owner oid := (
        SELECT relation.relowner
        FROM pg_catalog.pg_class relation
        WHERE relation.oid = TG_RELID
    );
    restoring BOOLEAN :=
        current_setting('hubuum.restore_revisions', true) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user, relation_owner, 'MEMBER');
    internal_bump BOOLEAN :=
        current_setting('hubuum.internal_revision_bump', true) IS NOT DISTINCT FROM 'on'
        AND current_user::regrole::oid = relation_owner;
    has_updated_at BOOLEAN := TG_ARGV[0] = 'updated_at';
    coalesced BOOLEAN := TG_ARGV[1] = 'coalesce';
    return_unchanged BOOLEAN := TG_ARGV[1] = 'direct_return_unchanged';
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

    owner_key := public.hubuum_revision_owner_key(TG_TABLE_NAME, to_jsonb(NEW));
    IF TG_OP = 'INSERT' THEN
        IF NEW.revision <> 1 THEN
            RAISE EXCEPTION 'caller-supplied resource revision is not permitted';
        END IF;
        IF coalesced THEN
            PERFORM public.hubuum_revision_owner_first(owner_key);
        END IF;
        RETURN NEW;
    END IF;

    PERFORM public.hubuum_assert_revision_precondition(owner_key, OLD.revision);
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
        operational_old := to_jsonb(OLD) - ARRAY['revision', 'created_at', 'updated_at'];
        operational_new := to_jsonb(NEW) - ARRAY['revision', 'created_at', 'updated_at'];
        IF operational_old = operational_new THEN
            IF return_unchanged THEN
                NEW.revision := OLD.revision;
                IF has_updated_at THEN
                    NEW.updated_at := OLD.updated_at;
                END IF;
                RETURN NEW;
            END IF;
            RETURN NULL;
        END IF;
        NEW.revision := OLD.revision;
        IF has_updated_at THEN
            NEW.updated_at := OLD.updated_at;
        END IF;
        RETURN NEW;
    END IF;

    IF coalesced THEN
        first_change := public.hubuum_revision_owner_first(owner_key);
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
$function$;

CREATE OR REPLACE FUNCTION hubuum_bump_revision_owner()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    owner_table TEXT := TG_ARGV[0];
    owner_id TEXT;
    owner_key TEXT;
    trusted_restore BOOLEAN :=
        pg_catalog.current_setting('hubuum.restore_revisions', true) IS NOT DISTINCT FROM 'on'
        AND pg_catalog.pg_has_role(session_user, current_user, 'MEMBER');
BEGIN
    IF trusted_restore THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF TG_ARGV[1] = 'membership' THEN
        owner_id := coalesce(pg_catalog.to_jsonb(NEW)->>'principal_id', pg_catalog.to_jsonb(OLD)->>'principal_id')
            || ':' || coalesce(pg_catalog.to_jsonb(NEW)->>'group_id', pg_catalog.to_jsonb(OLD)->>'group_id');
    ELSE
        owner_id := coalesce(pg_catalog.to_jsonb(NEW)->>TG_ARGV[1], pg_catalog.to_jsonb(OLD)->>TG_ARGV[1]);
    END IF;
    owner_key := owner_table || ':' || owner_id;
    IF NOT public.hubuum_revision_owner_first(owner_key) THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    PERFORM pg_catalog.set_config('hubuum.internal_revision_bump', 'on', true);
    IF owner_table = 'principals' THEN
        UPDATE public.principals SET revision = revision + 1 WHERE id = owner_id::INT;
    ELSIF owner_table = 'group_memberships' THEN
        UPDATE public.group_memberships SET revision = revision + 1
         WHERE principal_id = pg_catalog.split_part(owner_id, ':', 1)::INT
           AND group_id = pg_catalog.split_part(owner_id, ':', 2)::INT;
    ELSIF owner_table = 'tokens' THEN
        UPDATE public.tokens SET revision = revision + 1 WHERE id = owner_id::INT;
    ELSIF owner_table = 'collection_authorization_state' THEN
        UPDATE public.collection_authorization_state SET revision = revision + 1
         WHERE collection_id = owner_id::INT;
    ELSE
        RAISE EXCEPTION 'unknown revision owner table %', owner_table;
    END IF;
    PERFORM pg_catalog.set_config('hubuum.internal_revision_bump', 'off', true);
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION hubuum_check_delete_revision()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    relation_owner oid := (
        SELECT relation.relowner
        FROM pg_catalog.pg_class relation
        WHERE relation.oid = TG_RELID
    );
    trusted_restore BOOLEAN :=
        current_setting('hubuum.restore_revisions', true) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user, relation_owner, 'MEMBER');
BEGIN
    IF NOT trusted_restore THEN
        PERFORM public.hubuum_assert_revision_precondition(
            public.hubuum_revision_owner_key(TG_TABLE_NAME, to_jsonb(OLD)),
            OLD.revision
        );
    END IF;
    RETURN OLD;
END;
$function$;

CREATE OR REPLACE FUNCTION hubuum_fill_event_revisions()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    trusted_restore BOOLEAN :=
        pg_catalog.current_setting('hubuum.restore_events', true) IS NOT DISTINCT FROM 'on'
        AND pg_catalog.pg_has_role(session_user, current_user, 'MEMBER');
BEGIN
    IF trusted_restore THEN RETURN NEW; END IF;
    IF NEW.before_revision IS NULL
       AND pg_catalog.jsonb_typeof((NEW."before"::jsonb)->'revision') = 'number'
    THEN
        NEW.before_revision := ((NEW."before"::jsonb)->>'revision')::BIGINT;
    END IF;
    IF NEW.after_revision IS NULL
       AND pg_catalog.jsonb_typeof((NEW."after"::jsonb)->'revision') = 'number'
    THEN
        NEW.after_revision := ((NEW."after"::jsonb)->>'revision')::BIGINT;
    END IF;
    IF NEW.before_revision IS NOT NULL OR NEW.after_revision IS NOT NULL THEN
        NEW.schema_version := 2;
    END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION notify_events_fanout()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF pg_catalog.current_setting('hubuum.restore_events', true) IS NOT DISTINCT FROM 'on'
       AND pg_catalog.pg_has_role(session_user, current_user, 'MEMBER')
    THEN
        RETURN NEW;
    END IF;
    PERFORM pg_catalog.pg_notify('hubuum_events_fanout', NEW.id::text);
    RETURN NEW;
END;
$function$;

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

-- Runtime callers can complete only a durable, bounded retention claim. They
-- cannot supply identifiers, cutoffs, predicates, or SQL fragments directly.
CREATE OR REPLACE FUNCTION hubuum_complete_event_retention_purge(requested_claim_id uuid)
RETURNS TABLE (purged_events bigint, purged_terminal_deliveries bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    claimed_event_ids bigint[];
    claimed_delivery_cutoff timestamp;
    claimed_delivery_batch_size bigint;
    deleted_events bigint;
    deleted_deliveries bigint;
BEGIN
    SELECT retention.event_ids, retention.delivery_cutoff, retention.delivery_batch_size
      INTO claimed_event_ids, claimed_delivery_cutoff, claimed_delivery_batch_size
      FROM public.event_retention_batches retention
     WHERE retention.claim_id = requested_claim_id
       AND retention.completed_at IS NULL
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'event retention claim is not pending';
    END IF;
    IF pg_catalog.cardinality(claimed_event_ids) > 10000
       OR claimed_delivery_batch_size < 1
       OR claimed_delivery_batch_size > 10000
    THEN
        RAISE EXCEPTION 'event retention claim exceeds the bounded purge limit';
    END IF;

    WITH candidates AS (
        SELECT delivery.id
          FROM public.event_deliveries delivery
         WHERE delivery.updated_at < claimed_delivery_cutoff
           AND delivery.status IN ('succeeded', 'dead')
         ORDER BY delivery.updated_at ASC, delivery.id ASC
         LIMIT claimed_delivery_batch_size
         FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.event_deliveries delivery
    USING candidates
    WHERE delivery.id = candidates.id;
    GET DIAGNOSTICS deleted_deliveries = ROW_COUNT;

    PERFORM pg_catalog.set_config('events.allow_purge', 'on', true);
    DELETE FROM public.events event
     WHERE event.id = ANY(claimed_event_ids)
       AND event.occurred_at < (pg_catalog.clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '1 day'
       AND event.dispatched_at IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
             FROM public.event_deliveries delivery
            WHERE delivery.event_id = event.id
              AND delivery.status IN ('pending', 'failed', 'in_flight')
       );
    GET DIAGNOSTICS deleted_events = ROW_COUNT;
    IF deleted_events <> pg_catalog.cardinality(claimed_event_ids) THEN
        RAISE EXCEPTION 'event retention claim no longer matches purgeable events';
    END IF;
    RETURN QUERY SELECT deleted_events, deleted_deliveries;
END;
$function$;

REVOKE ALL ON FUNCTION hubuum_record_history() FROM PUBLIC;
REVOKE ALL ON FUNCTION hubuum_bump_revision_owner() FROM PUBLIC;
REVOKE ALL ON FUNCTION hubuum_fill_event_revisions() FROM PUBLIC;
REVOKE ALL ON FUNCTION notify_events_fanout() FROM PUBLIC;
REVOKE ALL ON FUNCTION enforce_events_append_only() FROM PUBLIC;
REVOKE ALL ON FUNCTION hubuum_complete_event_retention_purge(uuid) FROM PUBLIC;
