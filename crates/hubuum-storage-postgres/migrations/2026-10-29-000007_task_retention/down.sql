SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';
LOCK TABLE hubuum_control.deletion_authority IN ACCESS EXCLUSIVE MODE;
LOCK TABLE hubuum_control.task_retention_control IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.task_retention_control WHERE activation->>'state'<>'disabled' OR revision<>0)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_receipts)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_purge_batches)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_retention_holds)
       OR EXISTS (SELECT 1 FROM public.tasks WHERE receipt_generation IS NOT NULL OR retention_policy_revision IS NOT NULL OR details_purge_pending OR recovery_unavailable) THEN
        RAISE EXCEPTION 'Cannot downgrade retained task receipts, retention decisions or admission bindings';
    END IF;
END $$;
DROP INDEX public.task_retention_terminal_settlement;
DROP FUNCTION public.hubuum_task_receipt_settlement_pending(INTEGER);
DROP FUNCTION public.hubuum_task_purge_execute(JSONB,JSONB,INTEGER,BIGINT);
ALTER FUNCTION public.hubuum_guard_artifact_attempt() RESET search_path;
ALTER FUNCTION public.hubuum_guard_artifact_chunk() RESET search_path;
DROP FUNCTION public.hubuum_task_purges_pending(INTEGER);
DROP FUNCTION public.hubuum_task_purge_prepare(UUID,TEXT,INTEGER);
DROP FUNCTION public.hubuum_task_purge_block(UUID,TEXT,TIMESTAMPTZ);
DROP FUNCTION public.hubuum_task_receipts_expire(BIGINT, TIMESTAMPTZ, JSONB, BIGINT, UUID, UUID);
DROP FUNCTION public.hubuum_task_receipts_expired(INTEGER);
DROP FUNCTION public.hubuum_task_admissions_pending(INTEGER);
CREATE OR REPLACE FUNCTION public.hubuum_rotate_application_for_restore()
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority JSONB; empty_authority BOOLEAN;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
    UPDATE hubuum_control.deletion_authority SET material=material WHERE id=1;
    empty_authority := authority->'intents' = '{}'::jsonb;
    UPDATE hubuum_control.application_control SET incarnation=gen_random_uuid(), reapplication_revision=NULL,
        released_revision=CASE WHEN empty_authority THEN (authority->>'revision')::bigint END,
        checkpoint=NULL, pending_restore=NOT empty_authority WHERE id=1;
    RETURN empty_authority;
END $$;
DROP TRIGGER task_retention_policy ON public.tasks;
DROP FUNCTION public.hubuum_pin_task_retention_policy();
DROP FUNCTION public.hubuum_task_receipt_activate(INTEGER, UUID, UUID, BIGINT, BIGINT, UUID, UUID, TEXT);
DROP FUNCTION public.hubuum_task_receipt_settle(INTEGER, BIGINT, TEXT);
DROP FUNCTION public.hubuum_task_receipt_admit(INTEGER, BIGINT, TEXT, TEXT);
DROP TRIGGER task_retention_guard ON public.class_computation_state;
DROP TRIGGER task_retention_guard ON public.schema_repair_reports;
DROP TRIGGER task_retention_guard ON public.schema_impact_findings;
DROP TRIGGER task_retention_guard ON public.schema_validation_work;
DROP TRIGGER task_retention_guard ON public.remote_call_results;
DROP TRIGGER task_retention_guard ON public.schema_profile_outputs;
DROP TRIGGER task_retention_guard ON public.backup_task_outputs;
DROP TRIGGER task_retention_guard ON public.export_task_outputs;
DROP TRIGGER task_retention_guard ON public.import_task_results;
DROP FUNCTION public.hubuum_guard_task_detail_write();
DROP FUNCTION public.hubuum_task_admission_allowed(INTEGER);
DROP FUNCTION public.hubuum_task_outputs_visible(INTEGER);
DROP FUNCTION public.hubuum_task_details_visible(INTEGER);
DROP FUNCTION public.hubuum_task_receipt_for_task(UUID);
DROP FUNCTION public.hubuum_task_receipt_read(UUID, TEXT);
DROP FUNCTION public.hubuum_task_artifact_cleanup_allowed(UUID);
DROP FUNCTION public.hubuum_task_retention_lock();
DROP FUNCTION public.hubuum_task_control_capture();
DROP FUNCTION public.hubuum_task_control_read();
DROP FUNCTION public.hubuum_task_retention_read();
DROP INDEX public.import_task_results_purge;
DROP INDEX public.task_stable_identity;
DROP INDEX public.task_retention_eligible;
DROP INDEX public.task_legacy_submission_key;
ALTER TABLE public.tasks ADD CONSTRAINT tasks_submitted_by_idempotency_key_key UNIQUE(submitted_by,idempotency_key);
ALTER TABLE public.tasks DROP CONSTRAINT task_receipt_admission_pair;
ALTER TABLE public.tasks DROP COLUMN recovery_unavailable;
ALTER TABLE public.tasks DROP COLUMN details_purge_pending;
ALTER TABLE public.tasks DROP COLUMN detail_expires_at;
ALTER TABLE public.tasks DROP COLUMN retention_policy_revision;
ALTER TABLE public.tasks DROP COLUMN admission_event;
ALTER TABLE public.tasks DROP COLUMN receipt_checkpoint;
ALTER TABLE public.tasks DROP COLUMN receipt_legacy;
ALTER TABLE public.tasks DROP COLUMN receipt_protection_deadline;
ALTER TABLE public.tasks DROP COLUMN receipt_created_revision;
ALTER TABLE public.tasks DROP COLUMN receipt_incarnation;
ALTER TABLE public.tasks DROP COLUMN receipt_generation;
DROP TABLE hubuum_control.task_purge_targets;
DROP TABLE hubuum_control.task_purge_batches;
DROP TABLE hubuum_control.task_retention_holds;
DROP TABLE hubuum_control.task_receipts;
DROP TABLE hubuum_control.task_retention_policies;
DROP TABLE hubuum_control.task_retention_control;
