use super::*;

#[async_trait]
impl RuntimeSettingsStorage for PostgresStorage {
    async fn load_runtime_settings(&self) -> Result<StorageRuntimeSettingsSnapshot, StorageError> {
        crate::operations::runtime_settings::load_runtime_settings(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn update_runtime_settings(
        &self,
        request: StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        crate::operations::runtime_settings::update_runtime_settings(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn list_runtime_settings_changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Result<Vec<StorageRuntimeSettingsChange>, StorageError> {
        crate::operations::runtime_settings::list_runtime_settings_changes(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}
