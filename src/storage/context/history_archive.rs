use super::*;
use hubuum_domain::{PrincipalIdentity, ResourceIdentity};
use hubuum_storage_core::{
    ApplicationFence, HistoryArchiveCapture, HistoryArchiveCaptureRequest, HistoryArchiveCatalog,
    HistoryArchiveCatalogGeneration, HistoryArchiveCleanupClaim, HistoryArchiveCleanupRequest,
    HistoryArchiveCleanupSettlement, HistoryArchiveDescriptor, HistoryArchiveExecutionPermit,
    HistoryArchivePinnedCopies, HistoryArchivePublicationClaim, HistoryArchivePublicationRequest,
    HistoryArchiveStorage, StorageRecordIdentity,
};
use uuid::Uuid;

#[async_trait]
impl HistoryArchiveStorage for StorageHandle {
    async fn history_archive_pinned_copies(
        &self,
        target: StorageRecordIdentity,
        fence: ApplicationFence,
    ) -> Result<HistoryArchivePinnedCopies, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "history_archive_pinned_copies",
            async {
                dispatch_backend!(self, |backend| backend
                    .history_archive_pinned_copies(target, fence)
                    .await)
            },
        )
        .await
    }

    async fn load_history_archive_cleanup(
        &self,
        request: &HistoryArchiveCleanupRequest,
    ) -> Result<HistoryArchiveCleanupClaim, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "load_history_archive_cleanup",
            async {
                dispatch_backend!(self, |backend| backend
                    .load_history_archive_cleanup(request)
                    .await)
            },
        )
        .await
    }
    async fn settle_history_archive_cleanup(
        &self,
        settlement: HistoryArchiveCleanupSettlement,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "settle_history_archive_cleanup",
            async {
                dispatch_backend!(self, |backend| backend
                    .settle_history_archive_cleanup(settlement)
                    .await)
            },
        )
        .await
    }
    async fn claim_history_archive_publication(
        &self,
        request: HistoryArchivePublicationRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchivePublicationClaim, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "claim_history_archive_publication",
            async {
                dispatch_backend!(self, |backend| backend
                    .claim_history_archive_publication(request, permit)
                    .await)
            },
        )
        .await
    }

    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &HistoryArchiveExecutionPermit,
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
        permit: &HistoryArchiveExecutionPermit,
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
        permit: &HistoryArchiveExecutionPermit,
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
        permit: &HistoryArchiveExecutionPermit,
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
        permit: &HistoryArchiveExecutionPermit,
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
