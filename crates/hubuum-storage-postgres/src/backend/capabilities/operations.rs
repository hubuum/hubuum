use super::super::*;

#[async_trait]
impl MetricsStorage for PostgresStorage {
    fn metrics_pool_state(&self) -> StoragePoolState {
        crate::operations::metrics::pool_state(self.runtime())
    }

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        crate::operations::metrics::load_inventory_gauge_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        crate::operations::metrics::load_task_gauge_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        crate::operations::event_observability::load_event_metrics_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl InventoryStorage for PostgresStorage {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        crate::operations::inventory::load_inventory_counts(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl OperationalStateStorage for PostgresStorage {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        crate::operations::probe::load_readiness_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        crate::operations::maintenance::load_maintenance_state(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn storage_snapshot(&self) -> Result<OperationalStorageSnapshot, StorageError> {
        crate::operations::meta::load_storage_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        crate::operations::meta::load_task_queue_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        crate::operations::meta::load_export_template_health(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        crate::operations::meta::load_export_templates_for_audit(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventHealthStorage for PostgresStorage {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        crate::operations::event_observability::load_event_delivery_health(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl AuditEventStorage for PostgresStorage {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StorageEventPage<StorageAuditEvent>, StorageError> {
        crate::operations::event_audit::list_audit_events(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventSubscriptionStorage for PostgresStorage {
    async fn enabled_event_sink_count(&self) -> Result<i64, StorageError> {
        crate::operations::event_subscription::enabled_event_sink_count(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StorageEventPage<StorageEventSink>, StorageError> {
        crate::operations::event_subscription::list_event_sinks(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn load_event_sink(&self, sink_id: i32) -> Result<StorageEventSink, StorageError> {
        crate::operations::event_subscription::load_event_sink(self.runtime(), sink_id)
            .await
            .map_err(StorageError::from)
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageEventSink, StorageError> {
        crate::operations::event_subscription::create_event_sink(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageEventSink, StorageError> {
        crate::operations::event_subscription::update_event_sink(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_event_sink(&self, request: StorageEventSinkDelete) -> Result<(), StorageError> {
        crate::operations::event_subscription::delete_event_sink(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StorageEventPage<StorageEventSubscription>, StorageError> {
        crate::operations::event_subscription::list_event_subscriptions(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn load_event_subscription(
        &self,
        collection_id: i32,
        subscription_id: i32,
    ) -> Result<StorageEventSubscription, StorageError> {
        crate::operations::event_subscription::load_event_subscription(
            self.runtime(),
            collection_id,
            subscription_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageEventSubscription, StorageError> {
        crate::operations::event_subscription::create_event_subscription(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageEventSubscription, StorageError> {
        crate::operations::event_subscription::update_event_subscription(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<(), StorageError> {
        crate::operations::event_subscription::delete_event_subscription(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for PostgresStorage {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StorageEventPage<StorageEventDelivery>, StorageError> {
        crate::operations::event_delivery::list_event_deliveries(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn load_event_delivery(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        crate::operations::event_delivery::load_event_delivery(self.runtime(), delivery_id)
            .await
            .map_err(StorageError::from)
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        crate::operations::event_delivery::release_event_delivery_for_retry(
            self.runtime(),
            delivery_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        crate::operations::event_delivery::mark_event_delivery_dead(self.runtime(), delivery_id)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventDeliveryStorage for PostgresStorage {
    async fn claim_event_delivery_batch(
        &self,
        settings: EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        crate::operations::event_delivery::claim_event_delivery_batch(self.runtime(), settings)
            .await
            .map_err(StorageError::from)
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError> {
        crate::operations::event_delivery::mark_event_delivery_succeeded(self.runtime(), claim)
            .await
            .map_err(StorageError::from)
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        crate::operations::event_delivery::mark_event_delivery_failed(
            self.runtime(),
            claim,
            settings,
            error,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventFanoutStorage for PostgresStorage {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError> {
        crate::operations::event_fanout::process_event_fanout_batch(self.runtime(), settings)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventRetentionStorage for PostgresStorage {
    async fn process_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
        archive: &dyn EventArchive,
    ) -> Result<EventRetentionSummary, StorageError> {
        crate::operations::event_retention::process_event_retention_batch(
            self.runtime(),
            settings,
            archive,
        )
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
