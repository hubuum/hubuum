use super::*;
use hubuum_storage_core::{
    ApplicationControlState, ApplicationFence, ApplicationReapplication, ApplicationWorkAdmission,
    DeletionCheckpointIdentity, HotObjectHistoryCleanup, HotObjectHistoryDeletion,
    RuntimeIsolationDomain, RuntimeRegistration,
};

#[async_trait]
impl DeletionControlStorage for StorageHandle {
    async fn register_runtime_isolated(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "register_runtime_isolated",
            async {
                dispatch_backend!(self, |backend| backend
                    .register_runtime_isolated(isolation)
                    .await)
            },
        )
        .await
    }

    async fn capture_application_execution(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<StorageExecutionPermit, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "capture_application_execution",
            async {
                dispatch_backend!(self, |backend| backend
                    .capture_application_execution(owner)
                    .await)
            },
        )
        .await
    }

    async fn capture_application_execution_with_maintenance(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<ApplicationWorkAdmission, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "capture_application_execution_with_maintenance",
            async {
                dispatch_backend!(self, |backend| backend
                    .capture_application_execution_with_maintenance(owner)
                    .await)
            },
        )
        .await
    }

    async fn read_application_control(&self) -> Result<ApplicationControlState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "read_application_control",
            async { dispatch_backend!(self, |backend| backend.read_application_control().await) },
        )
        .await
    }

    async fn acknowledge_runtime_drain(
        &self,
        owner: RuntimeOwner,
        fence: ApplicationFence,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "acknowledge_runtime_drain",
            async {
                dispatch_backend!(self, |backend| backend
                    .acknowledge_runtime_drain(owner, fence)
                    .await)
            },
        )
        .await
    }

    async fn reapply_deletion_decisions(
        &self,
        expected: DeletionControlState,
        fence: ApplicationFence,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<ApplicationReapplication, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "reapply_deletion_decisions",
            async {
                dispatch_backend!(self, |backend| backend
                    .reapply_deletion_decisions(expected, fence, checkpoint)
                    .await)
            },
        )
        .await
    }

    async fn release_application_exposure(
        &self,
        receipt: ApplicationReapplication,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "release_application_exposure",
            async {
                dispatch_backend!(self, |backend| backend
                    .release_application_exposure(receipt, checkpoint)
                    .await)
            },
        )
        .await
    }

    async fn recover_application_control(
        &self,
        change: DeletionControlChange,
        isolation: RuntimeIsolationDomain,
    ) -> Result<ApplicationFence, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "recover_application_control",
            async {
                dispatch_backend!(self, |backend| backend
                    .recover_application_control(change, isolation)
                    .await)
            },
        )
        .await
    }

    async fn read_deletion_control(&self) -> Result<DeletionControlState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "read_deletion_control",
            async { dispatch_backend!(self, |backend| backend.read_deletion_control().await) },
        )
        .await
    }

    async fn capture_deletion_control(&self) -> Result<DeletionControlCapture, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "capture_deletion_control",
            async { dispatch_backend!(self, |backend| backend.capture_deletion_control().await) },
        )
        .await
    }

    async fn register_runtime(&self) -> Result<RuntimeRegistration, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "register_runtime",
            async { dispatch_backend!(self, |backend| backend.register_runtime().await) },
        )
        .await
    }

    async fn unregister_runtime(
        &self,
        registration: RuntimeRegistration,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "unregister_runtime",
            async {
                dispatch_backend!(self, |backend| backend
                    .unregister_runtime(registration)
                    .await)
            },
        )
        .await
    }

    async fn prepare_hot_object_history_deletion(
        &self,
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
    ) -> Result<DeletionControlState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "prepare_hot_object_history_deletion",
            async {
                dispatch_backend!(self, |backend| backend
                    .prepare_hot_object_history_deletion(expected, request)
                    .await)
            },
        )
        .await
    }

    async fn cleanup_hot_object_history(
        &self,
        request: HotObjectHistoryCleanup,
    ) -> Result<DeletionControlState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "cleanup_hot_object_history",
            async {
                dispatch_backend!(self, |backend| backend
                    .cleanup_hot_object_history(request)
                    .await)
            },
        )
        .await
    }

    async fn commit_deletion_control(
        &self,
        change: DeletionControlChange,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "commit_deletion_control",
            async {
                dispatch_backend!(self, |backend| backend
                    .commit_deletion_control(change)
                    .await)
            },
        )
        .await
    }
}

#[async_trait]
impl OperationalStateStorage for StorageHandle {
    async fn get_readiness_snapshot(&self) -> Result<StorageReadinessSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "get_readiness_snapshot",
            async { dispatch_backend!(self, |backend| { backend.get_readiness_snapshot().await }) },
        )
        .await
    }

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "get_maintenance_state",
            async { dispatch_backend!(self, |backend| backend.get_maintenance_state().await) },
        )
        .await
    }

    async fn get_task_queue_snapshot(
        &self,
    ) -> Result<StorageOperationalTaskQueueSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "get_task_queue_snapshot",
            async {
                dispatch_backend!(self, |backend| { backend.get_task_queue_snapshot().await })
            },
        )
        .await
    }

    async fn load_export_template_health(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateHealth>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "load_export_template_health",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_export_template_health().await
                })
            },
        )
        .await
    }

    async fn load_export_templates_for_audit(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateAuditEntry>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "load_export_templates_for_audit",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_export_templates_for_audit().await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl MetricsStorage for StorageHandle {
    async fn get_inventory_metrics_snapshot(
        &self,
    ) -> Result<StorageInventoryGaugeSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Metrics,
            "get_inventory_metrics_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_inventory_metrics_snapshot().await
                })
            },
        )
        .await
    }

    async fn get_task_metrics_snapshot(&self) -> Result<StorageTaskGaugeSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Metrics,
            "get_task_metrics_snapshot",
            async { dispatch_backend!(self, |backend| backend.get_task_metrics_snapshot().await) },
        )
        .await
    }

    async fn get_event_metrics_snapshot(
        &self,
    ) -> Result<StorageEventMetricsSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Metrics,
            "get_event_metrics_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_event_metrics_snapshot().await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl TokenRetentionStorage for StorageHandle {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TokenRetention,
            "purge_expired_tokens",
            async {
                dispatch_backend!(self, |backend| {
                    backend.purge_expired_tokens(settings).await
                })
            },
        )
        .await
    }
}
