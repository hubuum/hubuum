use super::super::*;

#[async_trait]
impl EventHealthStorage for PostgresStorage {
    async fn get_event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
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
    ) -> Result<StoragePage<StorageAuditEvent>, StorageError> {
        crate::operations::event_audit::list_audit_events(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventConfigurationStorage for PostgresStorage {
    async fn count_enabled_event_sinks(&self) -> Result<i64, StorageError> {
        crate::operations::event_subscription::count_enabled_event_sinks(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StoragePage<StorageEventSink>, StorageError> {
        crate::operations::event_subscription::list_event_sinks(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_event_sink(&self, sink_id: EventSinkId) -> Result<StorageEventSink, StorageError> {
        crate::operations::event_subscription::get_event_sink(self.runtime(), sink_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        crate::operations::event_subscription::create_event_sink(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        crate::operations::event_subscription::update_event_sink(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_event_sink(
        &self,
        request: StorageEventSinkDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::event_subscription::delete_event_sink(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StoragePage<StorageEventSubscription>, StorageError> {
        crate::operations::event_subscription::list_event_subscriptions(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        crate::operations::event_subscription::get_event_subscription(
            self.runtime(),
            collection_id.id(),
            subscription_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError> {
        crate::operations::event_subscription::create_event_subscription(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError> {
        crate::operations::event_subscription::update_event_subscription(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
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
    ) -> Result<StoragePage<StorageEventDelivery>, StorageError> {
        crate::operations::event_delivery::list_event_deliveries(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        crate::operations::event_delivery::get_event_delivery(self.runtime(), delivery_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        crate::operations::event_delivery::release_event_delivery_for_retry(
            self.runtime(),
            delivery_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        crate::operations::event_delivery::mark_event_delivery_dead(
            self.runtime(),
            delivery_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventDeliveryWorkerStorage for PostgresStorage {
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
    async fn claim_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
    ) -> Result<Option<EventRetentionBatch>, StorageError> {
        crate::operations::event_retention::claim_event_retention_batch(self.runtime(), settings)
            .await
            .map_err(StorageError::from)
    }

    async fn complete_event_retention_batch(
        &self,
        batch_id: EventRetentionBatchId,
    ) -> Result<EventRetentionSummary, StorageError> {
        crate::operations::event_retention::complete_event_retention_batch(self.runtime(), batch_id)
            .await
            .map_err(StorageError::from)
    }
}
