use async_trait::async_trait;

use crate::storage::{BackupSnapshotStorage, StorageBackupSnapshot, StorageError};

use super::PostgresStorage;

#[async_trait]
impl BackupSnapshotStorage for PostgresStorage {
    async fn snapshot_backup(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        hubuum_storage_postgres::operations::backup::snapshot_backup(
            self.runtime(),
            include_history,
        )
        .await
        .map_err(StorageError::from)
    }
}
