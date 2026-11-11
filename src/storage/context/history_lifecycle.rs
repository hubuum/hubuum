use super::*;
use hubuum_storage_core::{
    HistoryArchiveExecutionOwner, HistoryExpiryAdmissionRequest, HistoryExpiryAdmissionResult,
    HistoryExpiryTaskCheckpoint, HistoryExpiryTaskRequest, HistoryLifecycleControlChange,
    HistoryLifecycleRecovery, HistoryLifecycleScan, HistoryLifecycleScanResult,
    HistoryLifecycleSnapshot, HistoryLifecycleStorage, HistoryProducerOwner,
    HistoryProducerSettlement, HistoryPublicationKind, HistoryPublicationOwner,
    HistoryPublicationSettlement, HistoryReadContext, RuntimeIsolationDomain, RuntimeRegistration,
};

#[async_trait]
impl HistoryLifecycleStorage for StorageHandle {
    async fn process_history_expiry_task(
        &self,
        request: HistoryExpiryTaskRequest,
    ) -> Result<HistoryExpiryTaskCheckpoint, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "process_history_expiry_task",
            async {
                dispatch_backend!(self, |backend| backend
                    .process_history_expiry_task(request)
                    .await)
            },
        )
        .await
    }
    async fn admit_history_archive_execution(
        &self,
    ) -> Result<HistoryArchiveExecutionOwner, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "admit_history_archive_execution",
            async {
                dispatch_backend!(self, |backend| backend
                    .admit_history_archive_execution()
                    .await)
            },
        )
        .await
    }
    async fn admit_history_publication(
        &self,
        kind: HistoryPublicationKind,
    ) -> Result<HistoryPublicationOwner, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "admit_history_publication",
            async {
                dispatch_backend!(self, |backend| backend
                    .admit_history_publication(kind)
                    .await)
            },
        )
        .await
    }
    async fn settle_history_publication(
        &self,
        settlement: HistoryPublicationSettlement,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "settle_history_publication",
            async {
                dispatch_backend!(self, |backend| backend
                    .settle_history_publication(settlement)
                    .await)
            },
        )
        .await
    }
    async fn admit_history_producer(
        &self,
        context: HistoryReadContext,
    ) -> Result<HistoryProducerOwner, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "admit_history_producer",
            async {
                dispatch_backend!(self, |backend| backend
                    .admit_history_producer(context)
                    .await)
            },
        )
        .await
    }
    async fn settle_history_producer(
        &self,
        settlement: &HistoryProducerSettlement,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "settle_history_producer",
            async {
                dispatch_backend!(self, |backend| backend
                    .settle_history_producer(settlement)
                    .await)
            },
        )
        .await
    }
    async fn register_history_runtime(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "register_history_runtime",
            async {
                dispatch_backend!(self, |backend| backend
                    .register_history_runtime(isolation)
                    .await)
            },
        )
        .await
    }
    async fn scan_history_lifecycle(
        &self,
        scan: HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "scan_history_lifecycle",
            async { dispatch_backend!(self, |backend| backend.scan_history_lifecycle(scan).await) },
        )
        .await
    }
    async fn admit_history_expiry(
        &self,
        request: HistoryExpiryAdmissionRequest,
    ) -> Result<HistoryExpiryAdmissionResult, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "admit_history_expiry",
            async {
                dispatch_backend!(self, |backend| backend.admit_history_expiry(request).await)
            },
        )
        .await
    }
    async fn capture_history_lifecycle(&self) -> Result<HistoryLifecycleSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "capture_history_lifecycle",
            async { dispatch_backend!(self, |backend| backend.capture_history_lifecycle().await) },
        )
        .await
    }
    async fn commit_history_lifecycle(
        &self,
        change: HistoryLifecycleControlChange,
    ) -> Result<HistoryLifecycleSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "commit_history_lifecycle",
            async {
                dispatch_backend!(self, |backend| backend
                    .commit_history_lifecycle(change)
                    .await)
            },
        )
        .await
    }
    async fn recover_history_lifecycle(
        &self,
        recovery: HistoryLifecycleRecovery,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "recover_history_lifecycle",
            async {
                dispatch_backend!(self, |backend| backend
                    .recover_history_lifecycle(recovery)
                    .await)
            },
        )
        .await
    }
}
