use super::*;

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
