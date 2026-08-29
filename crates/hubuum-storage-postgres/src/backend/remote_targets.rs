use async_trait::async_trait;
use hubuum_domain::RemoteTargetId;

use hubuum_storage_core::{
    MutationOutcome, RemoteTargetStorage, StorageError, StoragePage, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetUpdate,
};

use super::PostgresStorage;

#[async_trait]
impl RemoteTargetStorage for PostgresStorage {
    async fn get_remote_target(
        &self,
        target_id: RemoteTargetId,
    ) -> Result<StorageRemoteTarget, StorageError> {
        crate::operations::remote_target::get_remote_target(self.runtime(), target_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StoragePage<StorageRemoteTarget>, StorageError> {
        crate::operations::remote_target::list_remote_targets(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError> {
        crate::operations::remote_target::create_remote_target(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError> {
        crate::operations::remote_target::update_remote_target(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::remote_target::delete_remote_target(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::remote_target::record_remote_target_invocation(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}
