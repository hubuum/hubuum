DROP INDEX IF EXISTS events_task_queued_initiator_fallback_idx;
DROP INDEX IF EXISTS events_initiator_user_id_idx;

DROP TRIGGER IF EXISTS hubuum_fill_task_initiator_trg ON tasks;
DROP FUNCTION IF EXISTS hubuum_fill_task_initiator();

ALTER TABLE events
    DROP COLUMN task_id,
    DROP COLUMN initiator_user_id;

ALTER TABLE tasks
    DROP COLUMN initiator_user_id;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'hubuumclass_history',
        'hubuumobject_history',
        'collections_history',
        'hubuumclass_relation_history',
        'hubuumobject_relation_history',
        'export_templates_history',
        'remote_targets_history'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE %I
                DROP COLUMN task_id,
                DROP COLUMN initiator_user_id,
                DROP COLUMN actor_kind',
            table_name
        );
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist text := quote_ident(TG_TABLE_NAME || '_history');
  seq text := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts timestamptz := clock_timestamp();
  actor int := nullif(current_setting('hubuum.actor_id', true), '')::int;
  base_cols text;
  hist_cols text;
BEGIN
  IF current_setting('hubuum.restore_history', true) = 'on' THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NEW;
  END IF;

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
