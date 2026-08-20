use super::*;

#[async_trait]
impl MetricsStorage for StorageHandle {
    async fn inventory_metrics_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "metrics",
            "inventory_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.inventory_metrics_snapshot().await
                })
            },
        )
        .await
    }

    async fn task_metrics_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        self.observe_storage_call(self.backend_name(), "metrics", "task_snapshot", async {
            dispatch_backend!(self, |backend| backend.task_metrics_snapshot().await)
        })
        .await
    }

    async fn event_metrics_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        self.observe_storage_call(self.backend_name(), "metrics", "event_snapshot", async {
            dispatch_backend!(self, |backend| { backend.event_metrics_snapshot().await })
        })
        .await
    }
}

#[async_trait]
impl InventoryStorage for StorageHandle {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        self.observe_storage_call(self.backend_name(), "inventory", "counts", async {
            dispatch_backend!(self, |backend| backend.inventory_counts().await)
        })
        .await
    }
}
