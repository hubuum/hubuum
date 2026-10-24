use async_trait::async_trait;
use hubuum_storage_core::{
    BackupSnapshotStorage, StorageBackupBudget, StorageBackupReplay, StorageError,
};

use super::PostgresStorage;

#[async_trait]
impl BackupSnapshotStorage for PostgresStorage {
    async fn capture_backup_replay(
        &self,
        include_history: bool,
        budget: StorageBackupBudget,
    ) -> Result<StorageBackupReplay, StorageError> {
        crate::operations::backup::capture_backup_replay(self.runtime(), include_history, budget)
            .await
            .map_err(StorageError::from)
    }
}
