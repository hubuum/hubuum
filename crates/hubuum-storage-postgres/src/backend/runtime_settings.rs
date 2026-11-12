use super::*;

#[async_trait]
impl RuntimeSettingsStorage for PostgresStorage {
    async fn recover_security_settings(
        &self,
        request: StorageSecurityRecovery,
    ) -> Result<[StorageRuntimeSettingsUpdateOutcome; 2], StorageError> {
        crate::operations::security_candidates::recover(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn purge_security_candidates(&self) -> Result<usize, StorageError> {
        crate::operations::security_candidates::purge(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn authentication_provider_binding(
        &self,
        name: hubuum_domain::IdentityScopeName,
    ) -> Result<Option<StorageProviderIdentityBinding>, StorageError> {
        crate::operations::security_candidates::provider_binding(self.runtime(), name)
            .await
            .map_err(StorageError::from)
    }

    async fn create_security_candidate(
        &self,
        candidate: StorageSecurityCandidate,
    ) -> Result<(), StorageError> {
        crate::operations::security_candidates::create(self.runtime(), candidate)
            .await
            .map_err(StorageError::from)
    }

    async fn get_security_candidate(
        &self,
        id: uuid::Uuid,
        owner: StorageSecurityCandidateOwner,
    ) -> Result<StorageSecurityCandidate, StorageError> {
        crate::operations::security_candidates::get(self.runtime(), id, owner)
            .await
            .map_err(StorageError::from)
    }

    async fn verify_security_candidate(
        &self,
        candidate: StorageSecurityCandidate,
    ) -> Result<(), StorageError> {
        crate::operations::security_candidates::verify(self.runtime(), candidate)
            .await
            .map_err(StorageError::from)
    }

    async fn activate_security_candidate(
        &self,
        request: StorageSecurityCandidateActivation,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        crate::operations::security_candidates::activate(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

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
