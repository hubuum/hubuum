//! Independent recovery evidence for protected deletion authority.
//!
//! Recovery restores only authenticated control material. Retained decisions
//! impose deployment-wide restrictions until lifecycle consumers can reapply
//! narrower restrictions; importing authority never releases those restrictions.

mod cleanup;
mod repository;

pub(crate) use cleanup::cleanup_object_history;

use std::path::Path;

use hubuum_storage_core::{DeletionControlState, DeletionControlStorage};
use serde::Serialize;

use crate::config::ControlRecoveryOptions;
use crate::errors::ApiError;
use crate::storage::{StorageContext, storage_handle};

use repository::{CheckpointRepository, RecoveryEvidence};

/// Startup composition requires a successful authority/exposure check before
/// it may construct services that serve traffic or originate background work.
pub(crate) struct RuntimeRecoveryPermit(());

async fn evidence(directory: &Path, key_file: &Path) -> Result<RecoveryEvidence, ApiError> {
    let directory = directory.to_owned();
    let key_file = key_file.to_owned();
    tokio::task::spawn_blocking(move || {
        CheckpointRepository::open(&directory, &key_file)?.recovery_evidence()
    })
    .await
    .map_err(|_| unavailable("Recovery evidence worker failed"))?
}

pub(crate) async fn require_runtime_permit(
    context: &impl StorageContext,
    options: &ControlRecoveryOptions,
) -> Result<RuntimeRecoveryPermit, ApiError> {
    let required = match options.paths()? {
        Some((directory, key)) => Some(evidence(directory, key).await?),
        None => None,
    };
    let current = storage_handle(context).read_deletion_control().await?;
    if required
        .as_ref()
        .is_some_and(|required| !current.covers(required.state()))
    {
        return Err(unavailable(
            "Recovered control does not cover the latest independent checkpoint; run hubuum-admin --recover-control while all replicas remain stopped",
        ));
    }
    if !current.permits_exposure() {
        return Err(unavailable(
            "Retained deletion decisions restrict application exposure and worker execution; keep the deployment quarantined until lifecycle restrictions can be reapplied",
        ));
    }
    Ok(RuntimeRecoveryPermit(()))
}

pub(crate) async fn recover(
    context: &impl StorageContext,
    directory: &Path,
    key_file: &Path,
) -> Result<ControlRecoveryReport, ApiError> {
    // Hold the independent publication lock through commit and read-back.
    // The exact predecessor fences a concurrent primary transition; any error
    // leaves the independent checkpoint intact and can be retried after restart.
    let required = evidence(directory, key_file).await?;
    let storage = storage_handle(context);
    let before = storage.read_deletion_control().await?;
    let change = before.recover_from(required.state())
        .map_err(|_| unavailable("Control recovery conflicts with local lineage, epoch, or decisions; keep the deployment quarantined"))?;
    storage.commit_deletion_control(change).await?;
    let recovered = storage.read_deletion_control().await?;
    if !recovered.covers(required.state()) {
        return Err(unavailable(
            "Recovered authority read-back did not cover the checkpoint; keep the deployment quarantined",
        ));
    }
    Ok(report(&recovered, required.state()))
}

#[derive(Serialize)]
pub(crate) struct ControlRecoveryReport {
    control_revision: i64,
    checkpoint_revision: i64,
    retained_intents: usize,
    authority_covered: bool,
    application_quarantine_released: bool,
}

pub(crate) async fn checkpoint(
    context: &impl StorageContext,
    directory: &Path,
    key_file: &Path,
    initialize: bool,
) -> Result<ControlRecoveryReport, ApiError> {
    let capture = storage_handle(context).capture_deletion_control().await?;
    let directory = directory.to_owned();
    let key_file = key_file.to_owned();
    tokio::task::spawn_blocking(move || {
        let repository = CheckpointRepository::open(&directory, &key_file)?;
        let checkpoint = repository.publish(&capture, initialize)?;
        Ok(report(capture.state(), checkpoint.state()))
    })
    .await
    .map_err(|_| unavailable("Checkpoint worker failed"))?
}

pub(crate) async fn verify(
    context: &impl StorageContext,
    directory: &Path,
    key_file: &Path,
) -> Result<ControlRecoveryReport, ApiError> {
    let required = evidence(directory, key_file).await?;
    let current = storage_handle(context).read_deletion_control().await?;
    if !current.covers(required.state()) {
        return Err(unavailable(
            "Recovered control does not cover the latest independent checkpoint; keep the deployment quarantined",
        ));
    }
    Ok(report(&current, required.state()))
}

fn report(
    current: &DeletionControlState,
    required: &DeletionControlState,
) -> ControlRecoveryReport {
    ControlRecoveryReport {
        control_revision: current.revision(),
        checkpoint_revision: required.revision(),
        retained_intents: current.intents().len(),
        authority_covered: current.covers(required),
        application_quarantine_released: false,
    }
}

fn unavailable(message: &str) -> ApiError {
    ApiError::ServiceUnavailable(message.into())
}

#[cfg(all(test, unix))]
mod tests;
