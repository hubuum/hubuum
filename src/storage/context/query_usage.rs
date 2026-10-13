use super::*;
use hubuum_storage_core::{
    QueryUsageStorage, StorageQueryUsageCreate, StorageQueryUsageDeclaration,
    StorageQueryUsageDelete, StorageQueryUsageReplace, StorageQueryUsageScope,
};

#[async_trait]
impl QueryUsageStorage for StorageHandle {
    async fn list_query_usage(
        &self,
        request: StorageQueryUsageScope,
    ) -> Result<Vec<StorageQueryUsageDeclaration>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::QueryUsage,
            "list_query_usage",
            async {
                dispatch_backend!(self, |backend| { backend.list_query_usage(request).await })
            },
        )
        .await
    }
    async fn create_query_usage(
        &self,
        request: StorageQueryUsageCreate,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::QueryUsage,
            "create_query_usage",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_query_usage(request).await
                })
            },
        )
        .await
    }
    async fn replace_query_usage(
        &self,
        request: StorageQueryUsageReplace,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::QueryUsage,
            "replace_query_usage",
            async {
                dispatch_backend!(self, |backend| {
                    backend.replace_query_usage(request).await
                })
            },
        )
        .await
    }
    async fn delete_query_usage(
        &self,
        request: StorageQueryUsageDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::QueryUsage,
            "delete_query_usage",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_query_usage(request).await
                })
            },
        )
        .await
    }
}
