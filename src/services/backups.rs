use crate::errors::ApiError;
use crate::storage::{
    BackupSnapshotStorage, StorageBackupBudget, StorageBackupSnapshot, StorageContext,
    storage_handle,
};

pub(crate) async fn capture_backup_snapshot(
    backend: &impl StorageContext,
    include_history: bool,
    budget: StorageBackupBudget,
) -> Result<StorageBackupSnapshot, ApiError> {
    let snapshot = storage_handle(backend)
        .capture_backup_snapshot(include_history, budget)
        .await?;
    if snapshot.includes_history() != include_history {
        return Err(ApiError::InternalServerError(
            "Captured backup history does not match the request".to_string(),
        ));
    }
    Ok(snapshot)
}
