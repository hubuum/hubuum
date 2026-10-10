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

impl StorageHandle {
    pub(crate) async fn analyze_query_usage(
        &self,
        scope: StorageQueryUsageScope,
        proposed: Vec<hubuum_storage_core::StorageQueryUsagePattern>,
    ) -> Result<hubuum_storage_core::StorageQueryUsageAnalysis, StorageError> {
        use hubuum_storage_core::{
            StorageQueryUsageAnalysis, StorageQueryUsageAnalysisRequest,
            StorageQueryUsageAnalysisStatus,
        };
        let request = StorageQueryUsageAnalysisRequest::try_new(
            scope,
            proposed,
            self.inner.query_observations.snapshot(scope.class_id()),
        )
        .map_err(|error| error.into_request_error())?;
        self.observe_storage_call(self.backend_name(), StorageCapability::QueryUsage, "analyze_query_usage", async {
            match &self.inner.query_usage_analysis {
                Some(provider) => provider.analyze_query_usage(request).await,
                None => {
                    // Recheck the authorized class scope even when analysis is unavailable.
                    self.list_query_usage(scope).await?;
                    let mut report = StorageQueryUsageAnalysis::new(StorageQueryUsageAnalysisStatus::Unavailable, request.into_observations());
                    report.limitation("The selected storage backend has no query usage analysis provider. Declarations remain valid and query behavior is unchanged.");
                    Ok(report)
                }
            }
        }).await
    }
}
