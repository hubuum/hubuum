use super::*;
use hubuum_domain::{PrincipalIdentity, ResourceIdentity};
use hubuum_storage_core::{
    HistoryArchiveCapture, HistoryArchiveCaptureRequest, HistoryArchiveCatalog,
    HistoryArchiveCatalogGeneration, HistoryArchiveDescriptor, HistoryArchiveStorage,
    StorageExecutionPermit,
};
use uuid::Uuid;

#[async_trait]
impl HistoryArchiveStorage for StorageHandle {
    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "history_archive_catalog",
            async {
                dispatch_backend!(self, |backend| backend
                    .history_archive_catalog(resource, permit)
                    .await)
            },
        )
        .await
    }
    async fn publish_history_archive(
        &self,
        expected: HistoryArchiveCatalogGeneration,
        descriptors: Vec<HistoryArchiveDescriptor>,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "publish_history_archive",
            async {
                dispatch_backend!(self, |backend| backend
                    .publish_history_archive(expected, descriptors, permit)
                    .await)
            },
        )
        .await
    }
    async fn capture_history_archive(
        &self,
        request: HistoryArchiveCaptureRequest,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "capture_history_archive",
            async {
                dispatch_backend!(self, |backend| backend
                    .capture_history_archive(request, permit)
                    .await)
            },
        )
        .await
    }
    async fn history_archive_capture(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        permit: &StorageExecutionPermit,
    ) -> Result<Option<HistoryArchiveCapture>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "history_archive_capture",
            async {
                dispatch_backend!(self, |backend| backend
                    .history_archive_capture(id, principal, permit)
                    .await)
            },
        )
        .await
    }
    async fn reserve_history_archive_read(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        bytes: u64,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "reserve_history_archive_read",
            async {
                dispatch_backend!(self, |backend| backend
                    .reserve_history_archive_read(id, principal, bytes, permit)
                    .await)
            },
        )
        .await
    }
}
