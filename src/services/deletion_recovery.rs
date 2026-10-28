//! Independent recovery evidence for protected deletion authority.
//!
//! This first slice deliberately does not release application quarantine or
//! execute deletion. Lifecycle consumers must reapply restrictions before
//! exposing restored data, even when the authority coverage check succeeds.

mod repository;

use std::path::Path;

use hubuum_storage_core::{DeletionControlState, DeletionControlStorage};
use serde::Serialize;

use crate::errors::ApiError;
use crate::storage::{StorageContext, storage_handle};

use repository::CheckpointRepository;

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
    let capture = storage_handle(context).capture_deletion_control().await?;
    let directory = directory.to_owned();
    let key_file = key_file.to_owned();
    tokio::task::spawn_blocking(move || {
        let checkpoint = CheckpointRepository::open(&directory, &key_file)?.latest()?;
        if !capture.state().covers(checkpoint.state()) {
            return Err(unavailable("Recovered control does not cover the latest independent checkpoint; keep the deployment quarantined"));
        }
        Ok(report(capture.state(), checkpoint.state()))
    }).await.map_err(|_| unavailable("Recovery verification worker failed"))?
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
