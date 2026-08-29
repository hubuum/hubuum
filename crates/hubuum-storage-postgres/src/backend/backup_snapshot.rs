use async_trait::async_trait;

use hubuum_storage_core::{BackupSnapshotStorage, StorageBackupSnapshot, StorageError};

use super::PostgresStorage;

#[async_trait]
impl BackupSnapshotStorage for PostgresStorage {
    async fn capture_backup_snapshot(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        crate::operations::backup::capture_backup_snapshot(self.runtime(), include_history)
            .await
            .map_err(StorageError::from)
    }
}
