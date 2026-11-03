use super::*;
use hubuum_storage_core::{
    ApiRequestAdmissionStorage, ApiRequestClass, StorageApiRequestPermit, StorageApiRequestPolicy,
    StorageApiRequestPolicyUpdate,
};

#[async_trait]
impl ApiRequestAdmissionStorage for StorageHandle {
    async fn api_request_policy(&self) -> Result<StorageApiRequestPolicy, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ApiRequestAdmission,
            "api_request_policy",
            async { dispatch_backend!(self, |backend| backend.api_request_policy().await) },
        )
        .await
    }

    async fn replace_api_request_policy(
        &self,
        update: StorageApiRequestPolicyUpdate,
    ) -> Result<StorageApiRequestPolicy, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ApiRequestAdmission,
            "replace_api_request_policy",
            async {
                dispatch_backend!(self, |backend| backend
                    .replace_api_request_policy(update)
                    .await)
            },
        )
        .await
    }

    async fn admit_api_request(
        &self,
        principal: PrincipalId,
        class: ApiRequestClass,
        input_bytes: u64,
    ) -> Result<StorageApiRequestPermit, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ApiRequestAdmission,
            "admit_api_request",
            async {
                dispatch_backend!(self, |backend| backend
                    .admit_api_request(principal, class, input_bytes)
                    .await)
            },
        )
        .await
    }

    async fn settle_api_request(
        &self,
        permit: StorageApiRequestPermit,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ApiRequestAdmission,
            "settle_api_request",
            async { dispatch_backend!(self, |backend| backend.settle_api_request(permit).await) },
        )
        .await
    }
}
