use async_trait::async_trait;

use crate::storage::{BackupSnapshotStorage, StorageBackupSnapshot, StorageError};

use super::PostgresStorage;
use super::error::map_postgres_error;
use super::operations::backup::snapshot_backup_db;

#[async_trait]
impl BackupSnapshotStorage for PostgresStorage {
    async fn snapshot_backup(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        snapshot_backup_db(self.pool(), include_history)
            .await
            .map(|(state, history)| {
                StorageBackupSnapshot::new(state.sections, history.map(|value| value.sections))
            })
            .map_err(map_postgres_error)
    }
}
