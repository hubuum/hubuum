use super::super::*;

#[async_trait]
impl MetricsStorage for PostgresStorage {
    async fn get_inventory_metrics_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        crate::operations::metrics::load_inventory_gauge_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn get_task_metrics_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        crate::operations::metrics::load_task_gauge_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn get_event_metrics_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        crate::operations::event_observability::load_event_metrics_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl OperationalStateStorage for PostgresStorage {
    async fn get_readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        crate::operations::probe::load_readiness_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        crate::operations::maintenance::load_maintenance_state(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn get_task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        crate::operations::meta::load_task_queue_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn load_export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        crate::operations::meta::load_export_template_health(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn list_export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        crate::operations::meta::load_export_templates_for_audit(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl TokenRetentionStorage for PostgresStorage {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        crate::operations::token_retention::purge_expired_tokens(self.runtime(), settings)
            .await
            .map_err(StorageError::from)
    }
}
