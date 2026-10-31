use std::path::Path;

use hubuum_storage_core::{
    DeletionCheckpointIdentity, DeletionControlCapture, DeletionControlState,
    DeletionControlStorage, HotObjectHistoryCleanup, HotObjectHistoryDeletion,
    StorageRecordIdentity,
};
use serde::Serialize;
use uuid::Uuid;

use super::repository::{CheckpointRepository, CheckpointSession};
use super::unavailable;
use crate::errors::ApiError;
use crate::storage::{StorageContext, StorageHandle, storage_handle};

/// This proof is local to the trusted administrator service: its owned session
/// holds the independent publication lock and carries durable authenticated
/// read-back. The storage request alone asserts no filesystem durability.
struct ProtectedHotCopy {
    session: CheckpointSession,
    request: HotObjectHistoryCleanup,
}

impl ProtectedHotCopy {
    fn new(
        session: CheckpointSession,
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
    ) -> Result<Self, ApiError> {
        if session.checkpoint().state() != &expected {
            return Err(unavailable(
                "Independent evidence does not protect the exact cleanup state",
            ));
        }
        let checkpoint =
            DeletionCheckpointIdentity::new(session.checkpoint().identity().to_owned())
                .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        let request = HotObjectHistoryCleanup::new(expected, request, checkpoint)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        Ok(Self { session, request })
    }

    async fn remove(
        self,
        storage: &StorageHandle,
    ) -> Result<(CheckpointSession, DeletionControlState), ApiError> {
        // Keep the session alive while the adapter checks quiescence and exact
        // authority, removes the copy, and commits its receipt atomically.
        let removed = storage.cleanup_hot_object_history(self.request).await?;
        Ok((self.session, removed))
    }
}

#[derive(Serialize)]
pub(crate) struct HotHistoryCleanupReport {
    intent: Uuid,
    copy: Uuid,
    target: StorageRecordIdentity,
    removed_from_hot_object_history: bool,
    control_revision: i64,
    completion_checkpoint: String,
    application_quarantine_released: bool,
}

async fn publish(
    mut session: CheckpointSession,
    capture: DeletionControlCapture,
) -> Result<CheckpointSession, ApiError> {
    // Moving ownership into the worker preserves the lock if this future is
    // cancelled while a file write, rename, or synchronization is still running.
    tokio::task::spawn_blocking(move || {
        session.publish(&capture)?;
        Ok(session)
    })
    .await
    .map_err(|_| unavailable("Cleanup checkpoint worker failed"))?
}

pub(crate) async fn cleanup_object_history(
    context: &impl StorageContext,
    directory: &Path,
    key_file: &Path,
    intent: Uuid,
    target: StorageRecordIdentity,
) -> Result<HotHistoryCleanupReport, ApiError> {
    let directory = directory.to_owned();
    let key_file = key_file.to_owned();
    let session = tokio::task::spawn_blocking(move || {
        CheckpointRepository::open(&directory, &key_file)?.into_session()
    })
    .await
    .map_err(|_| unavailable("Cleanup repository worker failed"))??;
    let storage = storage_handle(context);
    let before = storage.read_deletion_control().await?;
    if !before.covers(session.checkpoint().state()) {
        return Err(unavailable(
            "Primary authority does not cover the latest independent checkpoint; recover control before cleanup",
        ));
    }

    let copy = match before.intents().get(&intent) {
        Some(existing) if existing.copies().len() == 1 => {
            *existing.copies().keys().next().expect("one copy")
        }
        Some(_) => {
            return Err(ApiError::BadRequest(
                "This command requires one explicitly bound hot object-history copy".into(),
            ));
        }
        None => Uuid::new_v4(),
    };
    let request = HotObjectHistoryDeletion::new(intent, copy, target)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let prepared = storage
        .prepare_hot_object_history_deletion(before, request.clone())
        .await?;
    let capture = storage.capture_deletion_control().await?;
    if !capture.state().covers(&prepared) {
        return Err(unavailable(
            "Committed cleanup intent could not be read back",
        ));
    }
    let expected = capture.state().clone();
    let session = publish(session, capture).await?;
    let protected = ProtectedHotCopy::new(session, expected, request.clone())?;
    let (session, removed) = protected.remove(&storage).await?;
    acknowledge_completion(&storage, session, removed, request).await
}

async fn acknowledge_completion(
    storage: &StorageHandle,
    session: CheckpointSession,
    removed: DeletionControlState,
    request: HotObjectHistoryDeletion,
) -> Result<HotHistoryCleanupReport, ApiError> {
    let completion = async {
        let capture = storage.capture_deletion_control().await?;
        if !capture.state().covers(&removed) {
            return Err(unavailable("Committed copy removal could not be read back"));
        }
        let session = publish(session, capture).await?;
        if !session.checkpoint().state().covers(&removed) {
            return Err(unavailable(
                "Independent evidence does not cover copy removal",
            ));
        }
        Ok::<_, ApiError>(HotHistoryCleanupReport {
            intent: request.intent(),
            copy: request.copy(),
            target: request.target(),
            removed_from_hot_object_history: true,
            control_revision: session.checkpoint().state().revision(),
            completion_checkpoint: session.checkpoint().identity().to_owned(),
            application_quarantine_released: false,
        })
    }
    .await;
    completion.map_err(|_| {
        unavailable(
            "Hot-copy removal committed but completion acknowledgement is pending independent recovery protection; retry with the same intent identity",
        )
    })
}

#[cfg(all(test, unix))]
mod tests;
