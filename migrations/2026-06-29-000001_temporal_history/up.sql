-- Generic history trigger: writes a full-row snapshot into <table>_history on
-- every INSERT/UPDATE/DELETE. The acting user id is read from the transaction
-- local GUC `hubuum.actor_id` (NULL when unset = system/migration/background).
CREATE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist  text        := quote_ident(TG_TABLE_NAME || '_history');
  seq   text        := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts    timestamptz := clock_timestamp();
  actor int         := nullif(current_setting('hubuum.actor_id', true), '')::int;
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
  ELSE  -- DELETE
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

-- Create one history twin + sequence + indexes + trigger per in-scope table.
DO $$
DECLARE
  t text;
  ts timestamptz := transaction_timestamp();
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hubuumclass','hubuumobject','namespaces','hubuumclass_relation',
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
END $$;
