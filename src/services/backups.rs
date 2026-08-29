use crate::errors::ApiError;
use crate::models::{BackupHistory, BackupState};
use crate::storage::{BackupSnapshotStorage, StorageContext, storage_handle};

pub(crate) async fn capture_backup_snapshot(
    backend: &impl StorageContext,
    include_history: bool,
) -> Result<(BackupState, Option<BackupHistory>), ApiError> {
    let (state_sections, history_sections) = storage_handle(backend)
        .capture_backup_snapshot(include_history)
        .await?
        .into_parts();
    Ok((
        BackupState {
            sections: state_sections,
        },
        history_sections.map(|sections| BackupHistory { sections }),
    ))
}
