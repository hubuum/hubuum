use super::*;
use hubuum_storage_core::{
    QueryUsageStorage, StorageQueryUsageCreate, StorageQueryUsageDeclaration,
    StorageQueryUsageDelete, StorageQueryUsageReplace, StorageQueryUsageScope,
};

#[async_trait]
impl QueryUsageStorage for PostgresStorage {
    async fn list_query_usage(
        &self,
        request: StorageQueryUsageScope,
    ) -> Result<Vec<StorageQueryUsageDeclaration>, StorageError> {
        crate::operations::query_usage::list_query_usage(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
    async fn create_query_usage(
        &self,
        request: StorageQueryUsageCreate,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError> {
        crate::operations::query_usage::create_query_usage(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
    async fn replace_query_usage(
        &self,
        request: StorageQueryUsageReplace,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError> {
        crate::operations::query_usage::replace_query_usage(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
    async fn delete_query_usage(
        &self,
        request: StorageQueryUsageDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        crate::operations::query_usage::delete_query_usage(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}
