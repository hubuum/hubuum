use super::*;
use hubuum_domain::PrincipalIdentity;

#[derive(QueryableByName)]
struct CandidateRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = BigInt)]
    bytes: i64,
}

async fn principal_on(
    connection: &mut PostgresConnection,
    task_id: i32,
) -> Result<PrincipalIdentity, PostgresStorageError> {
    let row = diesel::sql_query("SELECT logical_identity AS material FROM tasks WHERE id=$1")
        .bind::<Integer, _>(task_id)
        .get_result::<MaterialRow>(connection)
        .await?;
    let evidence: StorageIdentityEvidence = decode(row.material)?;
    match evidence.references().get("/submitted_by") {
        Some(StorageCapturedIdentity::Principal {
            reference: IdentityReference::Known { identity },
        }) => Ok(*identity),
        _ => Err(PostgresStorageError::conflict(
            "Legacy task submitter identity is unresolved",
        )),
    }
}

pub async fn list_legacy_task_receipts(
    runtime: &PostgresRuntime,
    budget: TaskPurgeBudget,
) -> Result<Vec<TaskLegacyReceiptCandidate>, PostgresStorageError> {
    runtime.with_control_transaction(async move |connection| {
        lock_on(connection).await?;
        let selected=diesel::sql_query("SELECT t.id,octet_length(to_jsonb(t)::text)::bigint AS bytes FROM tasks t WHERE idempotency_key IS NOT NULL AND receipt_generation IS NULL ORDER BY id LIMIT $1")
            .bind::<BigInt,_>(i64::from(budget.max_rows())).load::<CandidateRow>(connection).await?;
        let mut bytes=0u64;
        let mut result=Vec::new();
        for candidate in selected {
            let size=u64::try_from(candidate.bytes).map_err(|_|PostgresStorageError::database("Invalid legacy task byte measurement"))?;
            if size>budget.max_bytes() && result.is_empty() { return Err(PostgresStorageError::unavailable("Legacy task row exceeds the bounded backfill input budget")); }
            if bytes+size>budget.max_bytes() { break; }
            use crate::schema::tasks::dsl as tasks;
            let row=tasks::tasks.filter(tasks::id.eq(candidate.id)).select(TaskRow::as_select()).first::<TaskRow>(connection).await?;
            result.push(TaskLegacyReceiptCandidate::new(row.into_storage()?,task_identity_on(connection,candidate.id).await?,principal_on(connection,candidate.id).await?));
            bytes+=size;
        }
        Ok(result)
    }).await
}

pub async fn backfill_task_receipts(
    runtime: &PostgresRuntime,
    expected: TaskControlRevision,
    bindings: Vec<TaskLegacyReceiptBinding>,
) -> Result<TaskControlRevision, PostgresStorageError> {
    if bindings.is_empty() || bindings.len() > TASK_CONTROL_MAX_PURGE_TASKS {
        return Err(PostgresStorageError::invalid_input(
            "Legacy receipt backfill requires a bounded nonempty batch",
        ));
    }
    runtime.with_control_transaction(async move |connection| {
        let mut header=lock_on(connection).await?;
        if header.revision!=expected || !matches!(header.activation,TaskReceiptActivation::Backfilling { .. }) { return Err(PostgresStorageError::conflict("Legacy receipt activation changed")); }
        diesel::sql_query("SELECT public.hubuum_task_capacity_lock()").execute(connection).await?;
        let policy=header.policy.clone().ok_or_else(||PostgresStorageError::database("Legacy backfill policy is missing"))?;
        let legacy_policy=TaskRetentionPolicy::new(policy.revision(),policy.deduplication_after_terminal(),None)?
            .with_pending_protection_timeout(policy.pending_protection_timeout())?
            .with_receipt_limits(policy.max_receipts(),policy.max_receipt_bytes())?.with_cleanup_budget(policy.cleanup_budget());
        let application=crate::backend::deletion_control::read_application_on(connection).await?;
        let now=super::super::task_execution::database_now(connection).await?.and_utc();
        let deadline=now.checked_add_signed(chrono::Duration::from_std(policy.pending_protection_timeout()).map_err(|_|PostgresStorageError::invalid_input("Legacy admission timeout is out of range"))?)
            .ok_or_else(||PostgresStorageError::invalid_input("Legacy admission timeout overflow"))?;
        let mut seen=std::collections::BTreeSet::new();
        for binding in bindings {
            if !seen.insert(binding.task()) { return Err(PostgresStorageError::invalid_input("Legacy receipt task identities must be unique")); }
            if receipt_for_task_on(connection,binding.task()).await?.is_some() { return Err(PostgresStorageError::conflict("Legacy task already owns a receipt generation")); }
            use crate::schema::tasks::dsl as tasks;
            let task=tasks::tasks.filter(diesel::dsl::sql::<Bool>("logical_identity #>> '{subject,identity,identity}' = ").bind::<Text,_>(binding.task().to_string()))
                .filter(tasks::idempotency_key.is_not_null()).for_update().select(TaskRow::as_select()).first::<TaskRow>(connection).await?;
            if principal_on(connection,task.id).await?!=binding.submission().principal() { return Err(PostgresStorageError::conflict("Legacy receipt does not match captured submitter identity")); }
            if receipt_on(connection,binding.submission()).await?.is_some() { return Err(PostgresStorageError::conflict("Legacy key already belongs to a different stable task")); }
            if header.receipt_count>=policy.max_receipts() || 2+(u64::from(header.receipt_count)+1)*1025>policy.max_receipt_bytes() {
                return Err(PostgresStorageError::unavailable("Legacy receipt capacity is exhausted; protect and expire eligible completed records before continuing"));
            }
            let revision=header.revision.next()?;
            let ticket=TaskAdmissionTicket::new(TaskReceiptReference::new(binding.submission().clone(),TaskReceiptGeneration::generate(),binding.task()),revision,application.fence().incarnation(),deadline)?;
            let kind=StorageTaskKind::from_persisted(&task.kind).ok_or_else(||PostgresStorageError::database("Unknown legacy task kind"))?;
            let status=StorageTaskStatus::from_persisted(&task.status).ok_or_else(||PostgresStorageError::database("Unknown legacy task status"))?;
            let outcome=if status.is_terminal() {
                TaskReceiptOutcome::Terminal { status:StorageTaskTerminalStatus::try_from_status(status)?,
                    finished_at:task.finished_at.ok_or_else(||PostgresStorageError::conflict("Legacy terminal task is missing its original completion clock"))?.and_utc(),
                    remote_side_effect:(kind==StorageTaskKind::RemoteCall).then_some(if task.remote_dispatched_at.is_some() { TaskRemoteSideEffectState::PossiblySent } else { TaskRemoteSideEffectState::LegacyUnknown }) }
            } else { TaskReceiptOutcome::MayExecute };
            let receipt=TaskReceipt::from_parts(ticket.clone(),kind,binding.fingerprint().clone(),task.created_at.and_utc(),&legacy_policy,outcome)?;
            let encoded=canonical(&receipt)?;
            diesel::sql_query("INSERT INTO hubuum_control.task_receipts(task_identity,principal_identity,protected_key,generation,material,encoded_bytes,expires_at) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)")
                .bind::<SqlUuid,_>(Uuid::from_bytes(*binding.task().as_bytes())).bind::<SqlUuid,_>(Uuid::from_bytes(*binding.submission().principal().as_bytes()))
                .bind::<Text,_>(binding.submission().protected_key().as_str()).bind::<SqlUuid,_>(ticket.receipt().generation().value())
                .bind::<Text,_>(&encoded).bind::<Integer,_>(encoded.len() as i32).bind::<Nullable<diesel::sql_types::Timestamptz>,_>(receipt.expires_at()).execute(connection).await?;
            diesel::sql_query("UPDATE public.tasks SET receipt_generation=$2,receipt_incarnation=$3,receipt_created_revision=$4,receipt_protection_deadline=$5,receipt_legacy=TRUE,idempotency_key=NULL,detail_expires_at=NULL WHERE id=$1")
                .bind::<Integer,_>(task.id).bind::<SqlUuid,_>(ticket.receipt().generation().value()).bind::<SqlUuid,_>(ticket.admitted_incarnation())
                .bind::<BigInt,_>(revision.value()).bind::<diesel::sql_types::Timestamp,_>(deadline.naive_utc()).execute(connection).await?;
            diesel::sql_query("UPDATE hubuum_control.task_retention_control SET revision=$1,receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+$2 WHERE id=1")
                .bind::<BigInt,_>(revision.value()).bind::<BigInt,_>(encoded.len() as i64).execute(connection).await?;
            header.revision=revision;
            header.receipt_count+=1;
        }
        Ok(header.revision)
    }).await
}
