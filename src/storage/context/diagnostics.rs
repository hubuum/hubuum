use super::*;

#[async_trait]
impl MetricsStorage for StorageHandle {
    async fn get_inventory_metrics_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Metrics,
            "inventory_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_inventory_metrics_snapshot().await
                })
            },
        )
        .await
    }

    async fn get_task_metrics_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Metrics,
            "task_snapshot",
            async { dispatch_backend!(self, |backend| backend.get_task_metrics_snapshot().await) },
        )
        .await
    }

    async fn get_event_metrics_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Metrics,
            "event_snapshot",
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
impl InventoryStorage for StorageHandle {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Inventory,
            "counts",
            async { dispatch_backend!(self, |backend| backend.get_inventory_counts().await) },
        )
        .await
    }
}
