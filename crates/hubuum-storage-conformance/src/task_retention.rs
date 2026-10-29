//! Protected admission invariants shared by every selectable adapter.

use hubuum_storage_core::{
    DeletionCheckpointIdentity, DeletionControlStorage, ProtectedTaskAdmission,
    StorageTaskCreateRequest, StorageTaskEventInput, StorageTaskFailure, StorageTaskLeaseDuration,
    StorageTaskSubmissionOutcome, TaskActivationOutcome, TaskControlCheckpoint,
    TaskExecutionStorage, TaskQueueStorage, TaskReceiptOutcome, TaskRetentionStorage,
    TaskSubmissionResolution,
};

use crate::FixtureError;

/// The caller supplies an isolated, empty task queue with protected receipt
/// admission enabled and a fresh correctly bound keyed request. The synthetic
/// checkpoint identifies fixture evidence; production callers must publish and
/// read back independently durable control evidence before activating work.
pub async fn verify_protected_task_admission(
    backend: &(
         impl TaskQueueStorage + TaskExecutionStorage + TaskRetentionStorage + DeletionControlStorage
     ),
    request: StorageTaskCreateRequest,
    checkpoint_identity: DeletionCheckpointIdentity,
) -> Result<(), FixtureError> {
    let StorageTaskSubmissionOutcome::PendingProtection(ticket) =
        backend.create_task(request.clone()).await?
    else {
        return Err("Fresh protected admission must reserve a pending generation".into());
    };
    let lookup = request
        .receipt_binding()
        .ok_or("Protected fixture request lacks its binding")?
        .lookup()
        .clone();
    if !matches!(backend.lookup_task_submission(lookup.clone()).await?, TaskSubmissionResolution::PendingProtection(ref pending) if pending == &ticket)
    {
        return Err("Protected lookup lost the original pending reservation".into());
    }
    let before = backend.read_task_control().await?;
    let duration =
        StorageTaskLeaseDuration::from_milliseconds(60_000).expect("valid fixture lease duration");
    if backend.claim_next_task(duration).await?.is_some() {
        return Err("An unprotected admission became executable".into());
    }
    if !matches!(backend.create_task(request).await?, StorageTaskSubmissionOutcome::DuplicatePendingProtection(ref duplicate) if duplicate == &ticket)
        || backend.read_task_control().await? != before
    {
        return Err("Duplicate pending submission changed its reservation or authority".into());
    }
    let checkpoint = TaskControlCheckpoint::new(
        before.lineage(),
        before.epoch(),
        before.revision(),
        checkpoint_identity,
    )?;
    let protected = ProtectedTaskAdmission::new(
        ticket.clone(),
        checkpoint,
        backend.read_application_control().await?.fence(),
    )?;
    let TaskActivationOutcome::Activated(task) =
        backend.activate_task_submission(protected.clone()).await?
    else {
        return Err("First protected activation must acknowledge its exact pending task".into());
    };
    if !matches!(backend.activate_task_submission(protected).await?, TaskActivationOutcome::AlreadyActive(ref repeated) if repeated.id() == task.id())
        || backend.read_task_control().await? != before
    {
        return Err("Activation retry changed same-revision signed material".into());
    }
    if !matches!(backend.lookup_task_submission(lookup).await?, TaskSubmissionResolution::Available(ref available) if available.id() == task.id())
    {
        return Err("Protected lookup lost the activated task correspondence".into());
    }
    let claimed = backend
        .claim_next_task(duration)
        .await?
        .ok_or("Protected task was not claimable")?;
    if claimed.task().id() != task.id() {
        return Err("Activation exposed a different task reservation".into());
    }
    backend
        .fail_task(StorageTaskFailure::new(
            claimed.lease().clone(),
            "Conformance fixture settled",
            StorageTaskEventInput::new("failed", "Conformance fixture settled"),
        ))
        .await?;
    let settled = backend.read_task_control().await?;
    let receipt = settled
        .receipt(ticket.receipt().submission())
        .ok_or("Settlement lost its receipt")?;
    if receipt.reference() != ticket.receipt()
        || !matches!(receipt.outcome(), TaskReceiptOutcome::Terminal { .. })
        || receipt.expires_at().is_none()
        || !settled.covers(&before)
    {
        return Err("Terminal settlement failed to preserve a fixed protected receipt".into());
    }
    Ok(())
}
