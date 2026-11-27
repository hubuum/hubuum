use super::*;
use hubuum_storage_core::{
    HistoryArchiveExecutionOwner, HistoryExpiryAdmissionRequest, HistoryExpiryAdmissionResult,
    HistoryExpiryReapplication, HistoryExpiryTaskAdmission, HistoryExpiryTaskCheckpoint,
    HistoryExpiryTaskRequest, HistoryExpiryTaskSubmission, HistoryLifecycleControlChange,
    HistoryLifecycleRecovery, HistoryLifecycleScan, HistoryLifecycleScanResult,
    HistoryLifecycleSnapshot, HistoryLifecycleStorage, HistoryPolicyReference,
    HistoryProducerOwner, HistoryProducerSettlement, HistoryPublicationKind,
    HistoryPublicationOwner, HistoryPublicationSettlement, HistoryReadContext,
    HistorySizeObservation, RuntimeIsolationDomain, RuntimeRegistration, StorageTask,
};
use uuid::Uuid;

#[async_trait]
impl HistoryLifecycleStorage for StorageHandle {
    async fn observe_history_size(
        &self,
        policy: HistoryPolicyReference,
        actor: hubuum_domain::PrincipalIdentity,
    ) -> Result<HistorySizeObservation, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "observe_history_size",
            async {
                dispatch_backend!(self, |backend| backend
                    .observe_history_size(policy, actor)
                    .await)
            },
        )
        .await
    }
    async fn reapply_history_expiry(
        &self,
        request: HistoryExpiryReapplication,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "reapply_history_expiry",
            async {
                dispatch_backend!(self, |backend| backend
                    .reapply_history_expiry(request)
                    .await)
            },
        )
        .await
    }
    async fn submit_history_expiry_task(
        &self,
        request: HistoryExpiryTaskSubmission,
    ) -> Result<HistoryExpiryTaskAdmission, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "submit_history_expiry_task",
            async {
                dispatch_backend!(self, |backend| backend
                    .submit_history_expiry_task(request)
                    .await)
            },
        )
        .await
    }
    async fn recover_history_expiry_task(
        &self,
        batch: Uuid,
    ) -> Result<Option<StorageTask>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::History,
            "recover_history_expiry_task",
            async {
                dispatch_backend!(self, |backend| backend
                    .recover_history_expiry_task(batch)
                    .await)
            },
        )
        .await
    }
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
