//! Privileged, globally quarantined recovery; no recovered work is replayed.

use super::*;
use crate::backend::deletion_control::{read_application_on, require_drained};
use crate::operations::task_execution::{
    append_task_lifecycle_event, database_now, system_provenance,
};

#[derive(QueryableByName)]
struct RecoveryRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = BigInt)]
    bytes: i64,
}

struct Work {
    rows: u32,
    bytes: u64,
    budget: TaskPurgeBudget,
}
impl Work {
    fn charge(&mut self, bytes: i64) -> Result<bool, PostgresStorageError> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| PostgresStorageError::database("Invalid recovery row size"))?;
        if self.rows >= self.budget.max_rows() || self.bytes + bytes > self.budget.max_bytes() {
            return Ok(false);
        }
        self.rows += 1;
        self.bytes += bytes;
        Ok(true)
    }
    async fn more(
        &self,
        connection: &mut PostgresConnection,
    ) -> Result<TaskReapplicationProgress, PostgresStorageError> {
        Ok(TaskReapplicationProgress::More {
            revision: read_on(connection).await?.revision,
            rows: self.rows,
            bytes: self.bytes,
        })
    }
}

async fn settle_recovered_receipt_on(
    connection: &mut PostgresConnection,
    receipt: &TaskReceipt,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), PostgresStorageError> {
    let settled = receipt.settle(TaskReceiptOutcome::RecoveryUnknown { settled_at: now })?;
    let identity = receipt.reference().task();
    let encoded = canonical(&settled)?;
    diesel::sql_query("UPDATE hubuum_control.task_retention_control SET revision=revision+1,receipt_bytes=receipt_bytes+$1-(SELECT encoded_bytes FROM hubuum_control.task_receipts WHERE task_identity=$2) WHERE id=1")
        .bind::<BigInt,_>(encoded.len() as i64).bind::<SqlUuid,_>(Uuid::from_bytes(*identity.as_bytes())).execute(connection).await?;
    diesel::sql_query("UPDATE hubuum_control.task_receipts SET material=$1::jsonb,encoded_bytes=$2,expires_at=$3 WHERE task_identity=$4")
        .bind::<Text,_>(encoded).bind::<Integer,_>(settled.encoded_bytes()? as i32)
        .bind::<Nullable<diesel::sql_types::Timestamptz>,_>(settled.expires_at())
        .bind::<SqlUuid,_>(Uuid::from_bytes(*identity.as_bytes())).execute(connection).await?;
    Ok(())
}

async fn settle_recovered_on(
    connection: &mut PostgresConnection,
    task: TaskRow,
) -> Result<(), PostgresStorageError> {
    let identity = task_identity_on(connection, task.id).await?;
    let receipt = receipt_for_task_on(connection, identity).await?;
    let now = database_now(connection).await?.and_utc();
    let mut anchor = now;
    let mut status = StorageTaskStatus::Failed;
    let mut unavailable = false;
    if let Some(receipt) = receipt {
        match receipt.outcome() {
            TaskReceiptOutcome::MayExecute => {
                settle_recovered_receipt_on(connection, &receipt, now).await?;
            }
            TaskReceiptOutcome::Terminal {
                status: terminal,
                finished_at,
                ..
            } => {
                status = terminal.as_status();
                anchor = *finished_at;
                unavailable = true;
            }
            TaskReceiptOutcome::RecoveryUnknown { settled_at } => {
                anchor = *settled_at;
                unavailable = true;
            }
        }
    }
    diesel::sql_query("UPDATE public.tasks SET status=$2,finished_at=$3,updated_at=$4,summary='Task execution outcome is unknown after recovery; automatic replay is disabled',request_payload=NULL,request_redacted_at=COALESCE(request_redacted_at,$4),lease_token=NULL,lease_expires_at=NULL,admission_event=NULL,recovery_unavailable=$5 WHERE id=$1")
        .bind::<Integer,_>(task.id).bind::<Text,_>(status.as_str()).bind::<diesel::sql_types::Timestamp,_>(anchor.naive_utc())
        .bind::<diesel::sql_types::Timestamp,_>(now.naive_utc()).bind::<Bool,_>(unavailable).execute(connection).await?;
    append_task_lifecycle_event(
        connection,
        &task,
        StorageTaskEventInput::new(
            status.as_str(),
            "Task reconciled without replay after recovery",
        ),
        &system_provenance(&task)?,
    )
    .await?;
    Ok(())
}

pub async fn reapply_task_retention(
    runtime: &PostgresRuntime,
    request: ProtectedTaskReapplication,
    budget: TaskPurgeBudget,
) -> Result<TaskReapplicationProgress, PostgresStorageError> {
    runtime.with_control_transaction(async move |connection| {
        let rc=diesel::select(diesel::dsl::sql::<Bool>("current_setting('transaction_isolation')='read committed'")).get_result::<bool>(connection).await?;
        if !rc { return Err(PostgresStorageError::conflict("Task reapplication requires READ COMMITTED")); }
        diesel::sql_query("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE").execute(connection).await?;
        authority::lock_expected_on(connection,request.expected()).await?;
        let application=read_application_on(connection).await?;
        let exposed=diesel::select(diesel::dsl::sql::<Bool>("public.hubuum_deletion_exposure_allowed()")).get_result::<bool>(connection).await?;
        if exposed || application.fence()!=request.fence() { return Err(PostgresStorageError::conflict("Task reapplication requires the exact quarantined application")); }
        require_drained(connection,request.fence()).await?;
        diesel::sql_query("SELECT public.hubuum_task_capacity_lock()").execute(connection).await?;
        if !request.expected().receipts().is_empty() {
            let unknown = diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM public.principals WHERE logical_identity #>> '{subject,kind}' IS DISTINCT FROM 'principal' OR logical_identity #>> '{subject,identity,state}' IS DISTINCT FROM 'known' OR logical_identity #>> '{subject,identity,identity}' IS NULL)")).get_result::<bool>(connection).await?;
            if unknown { return Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity)); }
            // A reminted legacy owner would otherwise create a new receipt
            // namespace for the same key and could repeat an external effect.
            let derived = diesel::sql_query("SELECT p.logical_identity AS material FROM public.principals p WHERE substring(p.logical_identity #>> '{subject,identity,identity}' FROM 15 FOR 1)='8' AND NOT EXISTS (SELECT 1 FROM hubuum_control.task_receipts r WHERE r.principal_identity::text=p.logical_identity #>> '{subject,identity,identity}') LIMIT 1")
                .get_result::<MaterialRow>(connection).await.optional()?;
            if let Some(row) = derived
                && decode::<StorageIdentityEvidence>(row.material)?.has_unknown_principal_restore_source()
            {
                return Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity));
            }
        }
        if !request.expected().purge_batches().is_empty() {
            let unknown=diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM public.tasks WHERE logical_identity #>> '{subject,kind}' IS DISTINCT FROM 'task' OR logical_identity #>> '{subject,identity,state}' IS DISTINCT FROM 'known' OR logical_identity #>> '{subject,identity,identity}' IS NULL)")).get_result::<bool>(connection).await?;
            if unknown { return Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity)); }
            // Legacy replay assigns a fresh, cohort-derived UUID. Being known
            // within that restore does not prove that the task is unrelated to
            // an older protected decision. Exact retained targets still have a
            // proven correspondence and are reapplied normally below.
            let derived = diesel::sql_query("SELECT t.logical_identity AS material FROM public.tasks t WHERE substring(t.logical_identity #>> '{subject,identity,identity}' FROM 15 FOR 1)='8' AND NOT EXISTS (SELECT 1 FROM hubuum_control.task_purge_targets p WHERE p.task_identity::text=t.logical_identity #>> '{subject,identity,identity}') LIMIT 1")
                .get_result::<MaterialRow>(connection).await.optional()?;
            if let Some(row) = derived
                && decode::<StorageIdentityEvidence>(row.material)?.has_unknown_task_restore_source()
            {
                return Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity));
            }
        }
        let mut work=Work{rows:0,bytes:0,budget};
        let candidates=diesel::sql_query("SELECT t.id,octet_length(to_jsonb(t)::text)::bigint AS bytes FROM public.tasks t WHERE status IN ('queued','validating','running') ORDER BY id LIMIT $1 FOR UPDATE")
            .bind::<BigInt,_>(i64::from(budget.max_rows())).load::<RecoveryRow>(connection).await?;
        for candidate in candidates {
            if !work.charge(candidate.bytes)? { return if work.rows==0 { Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::OversizedRow)) } else { work.more(connection).await }; }
            use crate::schema::tasks::dsl as tasks;
            let row=tasks::tasks.filter(tasks::id.eq(candidate.id)).select(TaskRow::as_select()).first::<TaskRow>(connection).await?;
            settle_recovered_on(connection,row).await?;
        }
        let pending=diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM public.tasks WHERE status IN ('queued','validating','running'))")).get_result::<bool>(connection).await?;
        if pending { return work.more(connection).await; }
        // Logical restore may omit the task while retaining its independently
        // protected admission. Settle its uncertainty without recreating work.
        let orphaned=diesel::sql_query("SELECT material FROM hubuum_control.task_receipts WHERE material #>> '{outcome,state}'='may_execute' ORDER BY task_identity LIMIT $1")
            .bind::<BigInt,_>(i64::from(budget.max_rows())).load::<MaterialRow>(connection).await?;
        for row in orphaned {
            let receipt:TaskReceipt=decode(row.material)?;
            if !work.charge(receipt.encoded_bytes()? as i64)? { return if work.rows==0 { Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::OversizedRow)) } else { work.more(connection).await }; }
            let now=database_now(connection).await?.and_utc();
            settle_recovered_receipt_on(connection,&receipt,now).await?;
        }
        let unsettled=diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM hubuum_control.task_receipts WHERE material #>> '{outcome,state}'='may_execute')")).get_result::<bool>(connection).await?;
        if unsettled { return work.more(connection).await; }
        let mismatched=diesel::sql_query("SELECT t.id,octet_length(to_jsonb(t)::text)::bigint AS bytes FROM public.tasks t JOIN hubuum_control.task_receipts r ON r.task_identity::text=t.logical_identity #>> '{subject,identity,identity}' WHERE NOT t.recovery_unavailable AND t.status NOT IN ('queued','validating','running') AND ((r.material #>> '{outcome,state}'='terminal' AND (t.status IS DISTINCT FROM r.material #>> '{outcome,status}' OR t.finished_at AT TIME ZONE 'UTC' IS DISTINCT FROM (r.material #>> '{outcome,finished_at}')::timestamptz)) OR (r.material #>> '{outcome,state}'='recovery_unknown' AND (t.status<>'failed' OR t.finished_at AT TIME ZONE 'UTC' IS DISTINCT FROM (r.material #>> '{outcome,settled_at}')::timestamptz))) ORDER BY t.id LIMIT $1 FOR UPDATE OF t")
            .bind::<BigInt,_>(i64::from(budget.max_rows())).load::<RecoveryRow>(connection).await?;
        for row in mismatched {
            if !work.charge(row.bytes)? { return if work.rows==0 { Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::OversizedRow)) } else { work.more(connection).await }; }
            diesel::sql_query("UPDATE public.tasks SET recovery_unavailable=TRUE WHERE id=$1").bind::<Integer,_>(row.id).execute(connection).await?;
        }
        if work.rows>=budget.max_rows() { return work.more(connection).await; }
        // Dependencies are settled in separate bounded phases after every old
        // task has been fenced. No class/task cascade or new repair is invoked.
        for (query,update) in [
            ("SELECT w.task_id AS id,octet_length(to_jsonb(w)::text)::bigint AS bytes FROM public.schema_validation_work w JOIN public.tasks t ON t.id=w.task_id WHERE w.active AND t.status NOT IN ('queued','validating','running') ORDER BY w.task_id LIMIT $1 FOR UPDATE OF w",
             "UPDATE public.schema_validation_work SET active=FALSE WHERE task_id=$1"),
            ("SELECT w.class_id AS id,octet_length(to_jsonb(w)::text)::bigint AS bytes FROM public.class_computation_state w JOIN public.tasks t ON t.id=w.active_task_id WHERE t.status NOT IN ('queued','validating','running') ORDER BY w.class_id LIMIT $1 FOR UPDATE OF w",
             "UPDATE public.class_computation_state SET active_task_id=NULL,rebuild_status='failed',last_error='Task execution outcome is unknown after recovery',updated_at=timezone('UTC',clock_timestamp()) WHERE class_id=$1"),
        ] {
            let rows=diesel::sql_query(query).bind::<BigInt,_>(i64::from(budget.max_rows())).load::<RecoveryRow>(connection).await?;
            for row in rows {
                if !work.charge(row.bytes)? { return if work.rows==0 { Ok(TaskReapplicationProgress::Blocked(TaskPurgeBlock::OversizedRow)) } else { work.more(connection).await }; }
                diesel::sql_query(update).bind::<Integer,_>(row.id).execute(connection).await?;
            }
        }
        for decision in request.expected().purge_batches().values() {
            if work.rows>=budget.max_rows() || work.bytes>=budget.max_bytes() { return work.more(connection).await; }
            let remainder=TaskPurgeBudget::new(budget.max_rows()-work.rows,budget.max_bytes()-work.bytes)?;
            let protected=ProtectedTaskPurge::new(decision.ticket().clone(),request.checkpoint().clone())?;
            match super::purge::execute_on(connection,&protected,remainder).await? {
                TaskPurgeProgress::More{rows,bytes}=>{work.rows+=rows;work.bytes+=bytes;return work.more(connection).await;}
                TaskPurgeProgress::Blocked(block)=>return Ok(TaskReapplicationProgress::Blocked(block)),
                TaskPurgeProgress::Removed{rows,bytes,..}=>{work.rows+=rows;work.bytes+=bytes;}
            }
        }
        // The authority UPDATE lock excludes new productive SQL throughout this
        // proof. Safe settlements cannot introduce another nonterminal row.
        let still_pending=diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM public.tasks WHERE status IN ('queued','validating','running')) OR EXISTS(SELECT 1 FROM public.schema_validation_work WHERE active) OR EXISTS(SELECT 1 FROM public.class_computation_state WHERE active_task_id IS NOT NULL)")).get_result::<bool>(connection).await?;
        if still_pending { return work.more(connection).await; }
        diesel::sql_query("UPDATE hubuum_control.task_retention_control SET revision=revision+1,reapplied_incarnation=$1,reapplied_revision=revision+1 WHERE id=1 AND (reapplied_incarnation IS DISTINCT FROM $1 OR reapplied_revision IS DISTINCT FROM revision)")
            .bind::<SqlUuid,_>(request.fence().incarnation()).execute(connection).await?;
        let revision=read_on(connection).await?.revision;
        Ok(TaskReapplicationProgress::Complete(TaskControlReapplication::new(request.fence().incarnation(),revision)?))
    }).await
}
