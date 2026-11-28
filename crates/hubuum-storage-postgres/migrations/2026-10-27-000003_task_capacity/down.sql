DROP FUNCTION hubuum_task_capacity_snapshot();
DROP TRIGGER task_capacity_accounting ON tasks;
DO $triggers$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY['export_task_outputs','backup_task_outputs','schema_profile_outputs','schema_repair_reports','remote_call_results'] LOOP
        EXECUTE format('DROP TRIGGER task_output_capacity ON public.%I', relation_name);
    END LOOP;
END
$triggers$;
DROP FUNCTION hubuum_account_task_output();
DROP FUNCTION hubuum_backfill_task_capacity(INTEGER);
DROP FUNCTION hubuum_account_task();
DROP FUNCTION hubuum_task_claimable_tasks();
DROP FUNCTION hubuum_reconcile_task_capacity();
DROP FUNCTION hubuum_task_output_bytes(INTEGER);
DROP FUNCTION hubuum_task_capacity_rejection(hubuum_control.task_accounting,BOOLEAN,BOOLEAN,BOOLEAN);
DROP FUNCTION hubuum_task_collection_identities(JSONB);
DROP FUNCTION hubuum_task_scope_rejection(JSONB,BIGINT,BIGINT,NUMERIC,BIGINT,BIGINT,BIGINT,TEXT,BOOLEAN,BOOLEAN,BOOLEAN);
DROP FUNCTION hubuum_task_policy_initialize(JSONB);
DROP FUNCTION hubuum_task_policy_replace(BIGINT,JSONB);
DROP FUNCTION hubuum_task_policy_read();
DROP FUNCTION hubuum_task_capacity_lock();
DROP FUNCTION hubuum_validate_task_policy(JSONB);
DROP TABLE hubuum_control.task_accounting;
DROP TABLE hubuum_control.task_policy;
