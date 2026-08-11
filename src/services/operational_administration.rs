//! Backend-neutral administrator diagnostics.

use crate::errors::ApiError;
use crate::storage::{
    OperationalStateStorage, OperationalStorageSnapshot, OperationalTaskQueueSnapshot,
    StorageHandle,
};

pub(crate) async fn storage_snapshot(
    storage: &StorageHandle,
) -> Result<OperationalStorageSnapshot, ApiError> {
    Ok(storage.storage_snapshot().await?)
}

pub(crate) async fn task_queue_snapshot(
    storage: &StorageHandle,
) -> Result<OperationalTaskQueueSnapshot, ApiError> {
    Ok(storage.task_queue_snapshot().await?)
}
