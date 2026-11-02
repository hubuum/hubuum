use hubuum_storage_core::{
    StorageTask, StorageTaskLeaseDuration, StorageTaskResumeOutcome, StorageTaskResumeRequest,
    StorageTaskStatus, StorageTaskWaitRequest, TaskExecutionStorage, TaskQueueStorage,
};

use crate::FixtureError;

/// Verify durable waiting, fenced execution, and exactly-once wakeup.
///
/// The caller supplies an isolated export or backup with a live admitted lease
/// and an execution deadline long enough for the verifier. No other worker may
/// claim this fixture during verification. The caller owns setup and cleanup;
/// successful verification leaves the same task queued with its checkpoint.
pub async fn verify_task_wait_resume(
    backend: &(impl TaskExecutionStorage + TaskQueueStorage + ?Sized),
    request: StorageTaskWaitRequest,
) -> Result<(), FixtureError> {
    let lease = request.lease().clone();
    let checkpoint = request.checkpoint().clone();
    let (before, _) = backend.get_task_access(lease.task_id()).await?.into_parts();
    if !before.has_lease() || before.control().deadline().is_none() {
        return Err("Task wait fixture requires a live admitted execution lease".into());
    }

    let wait = backend.wait_task(request).await?;
    let (waiting, _) = backend.get_task_access(before.id()).await?.into_parts();
    if wait.task() != &waiting
        || waiting.status() != StorageTaskStatus::Waiting
        || waiting.has_lease()
        || waiting.checkpoint() != Some(&checkpoint)
    {
        return Err("Waiting must durably retain its checkpoint and release its lease".into());
    }
    require_retained_work(&before, &waiting)?;
    if backend
        .renew_task_lease(
            lease,
            StorageTaskLeaseDuration::from_milliseconds(60_000)
                .expect("positive fixture lease duration"),
        )
        .await?
    {
        return Err("Entering a wait must fence the former execution lease".into());
    }

    let notification = StorageTaskResumeRequest::new(before.id(), wait.token().clone());
    let StorageTaskResumeOutcome::Queued(queued) =
        backend.resume_task(notification.clone()).await?
    else {
        return Err("A matching wait notification must queue the same task".into());
    };
    if queued.status() != StorageTaskStatus::Queued
        || queued.has_lease()
        || queued.checkpoint() != Some(&checkpoint)
    {
        return Err("Resuming must retain the checkpoint without acquiring execution".into());
    }
    require_retained_work(&before, &queued)?;
    if !matches!(
        backend.resume_task(notification).await?,
        StorageTaskResumeOutcome::Unchanged
    ) {
        return Err("A consumed wait notification must have no effect".into());
    }
    let (persisted, _) = backend.get_task_access(before.id()).await?.into_parts();
    if persisted != queued {
        return Err("Wakeup must persist once and duplicate notifications must preserve it".into());
    }
    Ok(())
}

fn require_retained_work(before: &StorageTask, after: &StorageTask) -> Result<(), FixtureError> {
    if before.id() != after.id()
        || before.kind() != after.kind()
        || before.request_payload() != after.request_payload()
        || before.control().deadline() != after.control().deadline()
        || before.started_at() != after.started_at()
        || before.progress() != after.progress()
    {
        return Err(
            "Waiting and wakeup must preserve task identity, input, progress and deadline".into(),
        );
    }
    Ok(())
}
