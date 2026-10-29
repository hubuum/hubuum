//! Protected task receipts and bounded, exact-identity detail retention.

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Bool, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel_async::RunQueryDsl;
use hubuum_domain::{IdentityReference, TaskIdentity};
use hubuum_storage_core::*;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::task_rows::TaskRow;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

mod admission;
pub(crate) mod authority;
mod backfill;
mod expiry;
mod purge;
mod recovery;
pub use admission::activate_task_submission;
pub(crate) use admission::{capture_invocation_on, emit_invocation_on};
pub use backfill::{backfill_task_receipts, list_legacy_task_receipts};
pub use expiry::{expire_task_receipts, select_expired_task_receipts};
pub use purge::{
    list_pending_task_purges, prepare_expired_task_outputs, prepare_task_details_purge,
    purge_task_details_batch,
};
pub use recovery::reapply_task_retention;

#[derive(QueryableByName)]
pub(crate) struct MaterialRow {
    #[diesel(sql_type = Jsonb)]
    pub(crate) material: Value,
}

#[derive(QueryableByName)]
struct OptionalMaterialRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    material: Option<Value>,
}

#[derive(QueryableByName)]
struct ChangedRow {
    #[diesel(sql_type = Bool)]
    changed: bool,
}

#[derive(Deserialize)]
pub(crate) struct ControlHeader {
    pub(crate) revision: TaskControlRevision,
    pub(crate) activation: TaskReceiptActivation,
    pub(crate) policy: Option<TaskRetentionPolicy>,
    pub(crate) receipt_count: u32,
    pub(crate) lineage: Uuid,
    pub(crate) epoch: Uuid,
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    material: Value,
) -> Result<T, PostgresStorageError> {
    serde_json::from_value(material).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("task retention authority", error)
    })
}

pub(crate) fn canonical<T: serde::Serialize>(value: &T) -> Result<String, PostgresStorageError> {
    serde_json::to_string(value)
        .map_err(|_| PostgresStorageError::internal("Cannot encode task retention authority"))
}

pub(crate) async fn read_on(
    connection: &mut PostgresConnection,
) -> Result<ControlHeader, PostgresStorageError> {
    let row = diesel::sql_query("SELECT public.hubuum_task_retention_read() AS material")
        .get_result::<MaterialRow>(connection)
        .await?;
    decode(row.material)
}

/// Acquire before task capacity, domain/class locks, task rows or child rows.
/// Settlement is allowed while application exposure is quarantined.
pub(crate) async fn lock_on(
    connection: &mut PostgresConnection,
) -> Result<ControlHeader, PostgresStorageError> {
    let row = diesel::sql_query("SELECT public.hubuum_task_retention_lock() AS material")
        .get_result::<MaterialRow>(connection)
        .await?;
    decode(row.material)
}

pub(crate) async fn task_identity_on(
    connection: &mut PostgresConnection,
    task_id: i32,
) -> Result<TaskIdentity, PostgresStorageError> {
    let row =
        diesel::sql_query("SELECT logical_identity AS material FROM public.tasks WHERE id=$1")
            .bind::<Integer, _>(task_id)
            .get_result::<MaterialRow>(connection)
            .await?;
    let evidence: StorageIdentityEvidence = decode(row.material)?;
    match evidence.subject() {
        Some(StorageIdentitySubject::Task {
            identity: IdentityReference::Known { identity },
        }) => Ok(identity),
        _ => Err(PostgresStorageError::conflict(
            "Task retention requires a known stable task identity",
        )),
    }
}

pub(crate) async fn receipt_on(
    connection: &mut PostgresConnection,
    identity: &TaskSubmissionIdentity,
) -> Result<Option<TaskReceipt>, PostgresStorageError> {
    let row = diesel::sql_query("SELECT public.hubuum_task_receipt_read($1,$2) AS material")
        .bind::<SqlUuid, _>(Uuid::from_bytes(*identity.principal().as_bytes()))
        .bind::<Text, _>(identity.protected_key().as_str())
        .get_result::<OptionalMaterialRow>(connection)
        .await?;
    row.material.map(decode).transpose()
}

pub(crate) async fn receipt_for_task_on(
    connection: &mut PostgresConnection,
    identity: TaskIdentity,
) -> Result<Option<TaskReceipt>, PostgresStorageError> {
    let row = diesel::sql_query("SELECT public.hubuum_task_receipt_for_task($1) AS material")
        .bind::<SqlUuid, _>(Uuid::from_bytes(*identity.as_bytes()))
        .get_result::<OptionalMaterialRow>(connection)
        .await?;
    row.material.map(decode).transpose()
}

pub(crate) async fn find_task_identity_on(
    connection: &mut PostgresConnection,
    identity: TaskIdentity,
) -> Result<Option<TaskRow>, PostgresStorageError> {
    use crate::schema::tasks::dsl as tasks;
    tasks::tasks
        .filter(
            diesel::dsl::sql::<Bool>("logical_identity #>> '{subject,identity,identity}' = ")
                .bind::<Text, _>(identity.to_string()),
        )
        .filter(diesel::dsl::sql::<Bool>(
            "public.hubuum_task_details_visible(id)",
        ))
        .select(TaskRow::as_select())
        .first(connection)
        .await
        .optional()
        .map_err(PostgresStorageError::from)
}

pub(crate) async fn resolve_on(
    connection: &mut PostgresConnection,
    header: &ControlHeader,
    lookup: &TaskSubmissionLookup,
) -> Result<TaskSubmissionResolution, PostgresStorageError> {
    match header.activation {
        TaskReceiptActivation::Disabled => return Ok(TaskSubmissionResolution::ProtectionDisabled),
        TaskReceiptActivation::Backfilling { .. } => {
            return Err(PostgresStorageError::unavailable(
                "Protected task admission is being activated",
            ));
        }
        TaskReceiptActivation::Protected { .. } => {}
    }
    let Some(receipt) = receipt_on(connection, lookup.identity()).await? else {
        return Ok(TaskSubmissionResolution::Fresh);
    };
    let now = super::task_execution::database_now(connection)
        .await?
        .and_utc();
    if receipt.expires_at().is_some_and(|expiry| expiry <= now) {
        return Ok(TaskSubmissionResolution::ExpiryProtectionRequired(
            receipt.reference().clone(),
        ));
    }
    if lookup.kind() != receipt.kind() {
        return Err(PostgresStorageError::conflict(
            "Idempotency key is already in use for a different task kind",
        ));
    }
    if matches!(receipt.fingerprint(), TaskFingerprint::LegacyUnverifiable)
        || (matches!(receipt.fingerprint(), TaskFingerprint::LegacyV1(_))
            && lookup.legacy_v1().is_none())
    {
        return Ok(TaskSubmissionResolution::LegacyUnverifiable(
            receipt.expired_projection(),
        ));
    }
    if !lookup.matches(receipt.kind(), receipt.fingerprint()) {
        return Err(PostgresStorageError::conflict(
            "Idempotency key is already in use for a different task submission",
        ));
    }
    let Some(task) = find_task_identity_on(connection, receipt.reference().task()).await? else {
        return Ok(TaskSubmissionResolution::DetailsExpired(
            receipt.expired_projection(),
        ));
    };
    let matches_outcome = match receipt.outcome() {
        TaskReceiptOutcome::MayExecute => StorageTaskStatus::from_persisted(&task.status)
            .is_some_and(|status| !status.is_terminal()),
        TaskReceiptOutcome::Terminal {
            status,
            finished_at,
            ..
        } => {
            task.status == status.as_status().as_str()
                && task.finished_at.map(|value| value.and_utc()) == Some(*finished_at)
        }
        TaskReceiptOutcome::RecoveryUnknown { settled_at } => {
            task.status == StorageTaskStatus::Failed.as_str()
                && task.finished_at.map(|value| value.and_utc()) == Some(*settled_at)
        }
    };
    if !matches_outcome {
        return Ok(TaskSubmissionResolution::DetailsExpired(
            receipt.expired_projection(),
        ));
    }
    let row = diesel::sql_query("SELECT receipt_checkpoint IS NOT NULL AS changed FROM public.tasks WHERE id=$1 AND receipt_generation=$2")
        .bind::<Integer,_>(task.id)
        .bind::<SqlUuid,_>(receipt.reference().generation().value())
        .get_result::<ChangedRow>(connection).await.optional()?;
    if row.is_some_and(|row| !row.changed) && receipt.outcome() == &TaskReceiptOutcome::MayExecute {
        Ok(TaskSubmissionResolution::PendingProtection(
            receipt.ticket().clone(),
        ))
    } else {
        Ok(TaskSubmissionResolution::Available(Box::new(
            task.into_storage()?,
        )))
    }
}

pub async fn lookup_task_submission(
    runtime: &PostgresRuntime,
    lookup: TaskSubmissionLookup,
) -> Result<TaskSubmissionResolution, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            let header = read_on(connection).await?;
            resolve_on(connection, &header, &lookup).await
        })
        .await
}

pub(crate) async fn record_admission_on(
    connection: &mut PostgresConnection,
    header: &ControlHeader,
    task: &TaskRow,
    binding: &TaskReceiptBinding,
) -> Result<TaskAdmissionTicket, PostgresStorageError> {
    let policy = header.policy.as_ref().ok_or_else(|| {
        PostgresStorageError::unavailable("Protected task retention policy is missing")
    })?;
    if header.receipt_count >= policy.max_receipts()
        || 2 + (u64::from(header.receipt_count) + 1) * 1025 > policy.max_receipt_bytes()
    {
        return Err(PostgresStorageError::unavailable(
            "Task receipt capacity is exhausted",
        ));
    }
    if header.activation.key_fingerprint() != Some(binding.key_fingerprint()) {
        return Err(PostgresStorageError::unavailable(
            "Task control signing key does not match",
        ));
    }
    let application = crate::backend::deletion_control::read_application_on(connection).await?;
    let receipt = TaskReceipt::admit(
        TaskReceiptAdmission::new(
            binding.lookup().clone(),
            task_identity_on(connection, task.id).await?,
            application.fence().incarnation(),
            task.created_at.and_utc(),
        )?,
        policy,
        header.revision.next()?,
    )?;
    diesel::sql_query("SELECT public.hubuum_task_receipt_admit($1,$2,$3,$4) AS changed")
        .bind::<Integer, _>(task.id)
        .bind::<BigInt, _>(header.revision.value())
        .bind::<Text, _>(canonical(&receipt)?)
        .bind::<Text, _>(binding.key_fingerprint().as_str())
        .get_result::<ChangedRow>(connection)
        .await?;
    Ok(receipt.ticket().clone())
}

/// The caller already owns the coordinator before acquiring domain/task locks.
pub(crate) async fn settle_on(
    connection: &mut PostgresConnection,
    task: &TaskRow,
) -> Result<(), PostgresStorageError> {
    let header = read_on(connection).await?;
    if matches!(header.activation, TaskReceiptActivation::Disabled) {
        return Ok(());
    }
    let identity = task_identity_on(connection, task.id).await?;
    let Some(receipt) = receipt_for_task_on(connection, identity).await? else {
        return Ok(());
    };
    let status = StorageTaskStatus::from_persisted(&task.status)
        .ok_or_else(|| PostgresStorageError::database("Invalid terminal task status"))?;
    let finished_at = task
        .finished_at
        .ok_or_else(|| {
            PostgresStorageError::database("Terminal task has no original completion clock")
        })?
        .and_utc();
    let outcome = TaskReceiptOutcome::Terminal {
        status: StorageTaskTerminalStatus::try_from_status(status)?,
        finished_at,
        remote_side_effect: (receipt.kind() == StorageTaskKind::RemoteCall).then_some(
            if task.remote_dispatched_at.is_some() {
                TaskRemoteSideEffectState::PossiblySent
            } else if task.execution_deadline_at.is_some() {
                TaskRemoteSideEffectState::NotSent
            } else {
                TaskRemoteSideEffectState::LegacyUnknown
            },
        ),
    };
    let settled = receipt.settle(outcome)?;
    if settled == receipt {
        return Ok(());
    }
    diesel::sql_query("SELECT public.hubuum_task_receipt_settle($1,$2,$3) AS changed")
        .bind::<Integer, _>(task.id)
        .bind::<BigInt, _>(header.revision.value())
        .bind::<Text, _>(canonical(&settled)?)
        .get_result::<ChangedRow>(connection)
        .await?;
    Ok(())
}
