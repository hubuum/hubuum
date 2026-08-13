use super::*;

#[async_trait]
impl MetricsStorage for StorageHandle {
    fn metrics_pool_state(&self) -> StoragePoolState {
        dispatch_backend!(self, |backend| backend.metrics_pool_state())
    }

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "metrics",
            "inventory_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.metrics_inventory_snapshot().await
                })
            },
        )
        .await
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "metrics", "task_snapshot", async {
            dispatch_backend!(self, |backend| backend.metrics_task_snapshot().await)
        })
        .await
    }

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "metrics", "event_snapshot", async {
            dispatch_backend!(self, |backend| { backend.metrics_event_snapshot().await })
        })
        .await
    }
}

#[async_trait]
impl InventoryStorage for StorageHandle {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        observe_storage_call(self.backend_name(), "inventory", "counts", async {
            dispatch_backend!(self, |backend| backend.inventory_counts().await)
        })
        .await
    }
}
