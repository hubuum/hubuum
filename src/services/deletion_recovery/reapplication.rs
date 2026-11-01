use std::path::Path;

use hubuum_storage_core::{
    ApplicationExposureProof, ControlCheckpointCapture, DeletionAuditProtection,
    DeletionCheckpointIdentity, DeletionControlStorage, ProtectedTaskReapplication,
    TaskControlCheckpoint, TaskControlReapplication, TaskReapplicationProgress,
    TaskReceiptActivation, TaskRetentionStorage,
};

use super::cleanup::protect_audit;
use super::repository::{CheckpointRepository, CheckpointSession};
use super::{ControlRecoveryReport, covers_checkpoint, invalid_control, report, unavailable};
use crate::errors::ApiError;
use crate::storage::{StorageContext, storage_handle};

async fn publish(
    mut session: CheckpointSession,
    capture: ControlCheckpointCapture,
) -> Result<CheckpointSession, ApiError> {
    tokio::task::spawn_blocking(move || {
        session.publish_control(&capture)?;
        Ok(session)
    })
    .await
    .map_err(|_| unavailable("Reapplication checkpoint worker failed"))?
}

fn identity(session: &CheckpointSession) -> Result<DeletionCheckpointIdentity, ApiError> {
    DeletionCheckpointIdentity::new(session.checkpoint().identity().to_owned())
        .map_err(|_| unavailable("Invalid independently verified checkpoint identity"))
}

/// Reapply retained decisions, protect the exact completion, then release this
/// application image. Cancellation or failure before the final CAS leaves it
/// quarantined; retry never treats an old Removed receipt as physical proof.
pub(crate) async fn reapply(
    context: &impl StorageContext,
    directory: &Path,
    key_file: &Path,
) -> Result<ControlRecoveryReport, ApiError> {
    let directory = directory.to_owned();
    let key_file = key_file.to_owned();
    let session = tokio::task::spawn_blocking(move || {
        CheckpointRepository::open(&directory, &key_file)?.into_session()
    })
    .await
    .map_err(|_| unavailable("Reapplication repository worker failed"))??;
    let storage = storage_handle(context);
    let capture = storage.capture_control_checkpoint().await?;
    if !covers_checkpoint(&capture, session.checkpoint()) {
        return Err(unavailable(
            "Primary authority is behind independent recovery evidence; run --recover-control before reapplication",
        ));
    }
    // Protect every retained decision before touching its recovered copy.
    let expected = capture.deletion().state().clone();
    // Rotation clears completion proofs without changing retained authority.
    // Reuse its existing durable evidence until replay advances the revision;
    // publishing cleared proofs at the old vector would correctly be rejected.
    let session = if session.checkpoint().state() == &expected
        && session.checkpoint().task_control() == Some(capture.task_control())
    {
        session
    } else {
        publish(session, capture).await?
    };
    let (session, expected) =
        protect_audit(&storage, session, DeletionAuditProtection::Intent).await?;
    let application = storage.read_application_control().await?;
    let mut receipt = storage
        .reapply_deletion_decisions(expected, application.fence(), identity(&session)?)
        .await?;
    let capture = storage.capture_control_checkpoint().await?;
    if capture.deletion().reapplication() != Some(&receipt) {
        return Err(unavailable(
            "Application changed before reapplication read-back; keep the deployment quarantined",
        ));
    }
    let session = publish(session, capture).await?;
    let (session, task_receipt) = reapply_tasks(context, session).await?;
    if session.checkpoint().reapplication() != Some(&receipt) {
        return Err(unavailable(
            "Independent checkpoint does not protect this application reapplication",
        ));
    }
    let (session, completed) =
        protect_audit(&storage, session, DeletionAuditProtection::Completion).await?;
    if completed.revision() != receipt.revision() {
        receipt = session
            .checkpoint()
            .reapplication()
            .filter(|updated| {
                updated.incarnation() == receipt.incarnation()
                    && updated.revision() == completed.revision()
            })
            .cloned()
            .ok_or_else(|| {
                unavailable("Independent completion evidence does not cover this reapplication")
            })?;
    }
    // The session owns publication exclusion across this exact release CAS.
    storage
        .release_application_exposure(
            ApplicationExposureProof::new(receipt, task_receipt).map_err(invalid_control)?,
            identity(&session)?,
        )
        .await?;
    let current = storage.read_deletion_control().await?;
    let application = storage.read_application_control().await?;
    let mut result = report(&current, session.checkpoint().state());
    result.application_quarantine_released = application.is_released_for(&current);
    if !result.application_quarantine_released {
        return Err(unavailable(
            "Application release changed before read-back; retry reapplication",
        ));
    }
    Ok(result)
}

async fn reapply_tasks(
    context: &impl StorageContext,
    mut session: CheckpointSession,
) -> Result<(CheckpointSession, Option<TaskControlReapplication>), ApiError> {
    let storage = storage_handle(context);
    loop {
        let tasks = session.checkpoint().task_control().ok_or_else(|| {
            unavailable("Task reapplication requires composite recovery evidence")
        })?;
        if matches!(tasks.activation(), TaskReceiptActivation::Disabled) {
            return Ok((session, None));
        }
        let checkpoint = TaskControlCheckpoint::new(
            tasks.lineage(),
            tasks.epoch(),
            tasks.revision(),
            identity(&session)?,
        )
        .map_err(invalid_control)?;
        let fence = storage.read_application_control().await?.fence();
        let budget = tasks.policy().cleanup_budget();
        let request = ProtectedTaskReapplication::new(tasks.clone(), fence, checkpoint)
            .map_err(invalid_control)?;
        let progress = storage.reapply_task_retention(request, budget).await?;
        match progress {
            TaskReapplicationProgress::Blocked(reason) => {
                return Err(unavailable(&format!(
                    "Task retention reapplication is blocked ({reason:?}); keep the deployment quarantined",
                )));
            }
            TaskReapplicationProgress::More {
                rows,
                bytes,
                revision,
            } => {
                if rows == 0 && bytes == 0 && revision == tasks.revision() {
                    return Err(unavailable(
                        "Task retention reapplication made no bounded progress; keep the deployment quarantined",
                    ));
                }
                session = publish(session, storage.capture_control_checkpoint().await?).await?;
            }
            TaskReapplicationProgress::Complete(proof) => {
                let capture = storage.capture_control_checkpoint().await?;
                if capture.task_reapplication() != Some(&proof) {
                    return Err(unavailable(
                        "Task reapplication changed before completion read-back",
                    ));
                }
                session = publish(session, capture).await?;
                if session.checkpoint().task_reapplication() != Some(&proof) {
                    return Err(unavailable(
                        "Independent evidence does not protect task reapplication",
                    ));
                }
                return Ok((session, Some(proof)));
            }
        }
    }
}
