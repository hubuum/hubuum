use std::path::Path;

use hubuum_storage_core::{
    DeletionCheckpointIdentity, DeletionControlCapture, DeletionControlStorage,
};

use super::repository::{CheckpointRepository, CheckpointSession};
use super::{ControlRecoveryReport, report, unavailable};
use crate::errors::ApiError;
use crate::storage::{StorageContext, storage_handle};

async fn publish(
    mut session: CheckpointSession,
    capture: DeletionControlCapture,
) -> Result<CheckpointSession, ApiError> {
    tokio::task::spawn_blocking(move || {
        session.publish(&capture)?;
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
    let capture = storage.capture_deletion_control().await?;
    if !capture.state().covers(session.checkpoint().state()) {
        return Err(unavailable(
            "Primary authority is behind independent recovery evidence; run --recover-control before reapplication",
        ));
    }
    // Protect every retained decision before touching its recovered copy.
    let expected = capture.state().clone();
    let session = if session.checkpoint().state() == &expected {
        session
    } else {
        publish(session, capture).await?
    };
    let application = storage.read_application_control().await?;
    let receipt = storage
        .reapply_deletion_decisions(expected, application.fence(), identity(&session)?)
        .await?;
    let capture = storage.capture_deletion_control().await?;
    if capture.reapplication() != Some(&receipt) {
        return Err(unavailable(
            "Application changed before reapplication read-back; keep the deployment quarantined",
        ));
    }
    let session = publish(session, capture).await?;
    if session.checkpoint().reapplication() != Some(&receipt) {
        return Err(unavailable(
            "Independent checkpoint does not protect this application reapplication",
        ));
    }
    // The session owns publication exclusion across this exact release CAS.
    storage
        .release_application_exposure(receipt, identity(&session)?)
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
