use super::super::*;

#[async_trait]
impl MetricsStorage for PostgresStorage {
    fn metrics_pool_state(&self) -> StoragePoolState {
        let state = self.pool.state();
        let max_connections = self.pool.config().max_size;
        let in_use_connections = state.connections.saturating_sub(state.idle_connections);
        StoragePoolState::new(
            StoragePoolCapacity::new(
                max_connections,
                state.connections,
                max_connections.saturating_sub(in_use_connections),
                state.idle_connections,
                in_use_connections,
            ),
            StoragePoolAcquisitionState::new(
                state.statistics.pending_gets(),
                state.statistics.get_started,
                state.statistics.get_direct,
                state.statistics.get_waited,
                state.statistics.get_timed_out,
                u64::try_from(state.statistics.get_wait_time.as_millis()).unwrap_or(u64::MAX),
            ),
            StoragePoolConnectionState::new(
                state.statistics.connections_created,
                state.statistics.connections_closed_broken,
                state.statistics.connections_closed_invalid,
                state.statistics.connections_closed_max_lifetime,
                state.statistics.connections_closed_idle_timeout,
            ),
        )
    }

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        hubuum_storage_postgres::operations::metrics::load_inventory_gauge_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        hubuum_storage_postgres::operations::metrics::load_task_gauge_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        hubuum_storage_postgres::operations::event_observability::load_event_metrics_snapshot(
            self.runtime(),
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl InventoryStorage for PostgresStorage {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        hubuum_storage_postgres::operations::inventory::load_inventory_counts(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl OperationalStateStorage for PostgresStorage {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        hubuum_storage_postgres::operations::probe::load_readiness_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        hubuum_storage_postgres::operations::maintenance::load_maintenance_state(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn storage_snapshot(&self) -> Result<OperationalStorageSnapshot, StorageError> {
        hubuum_storage_postgres::operations::meta::load_storage_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        hubuum_storage_postgres::operations::meta::load_task_queue_snapshot(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        hubuum_storage_postgres::operations::meta::load_export_template_health(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        hubuum_storage_postgres::operations::meta::load_export_templates_for_audit(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventHealthStorage for PostgresStorage {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        hubuum_storage_postgres::operations::event_observability::load_event_delivery_health(
            self.runtime(),
        )
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
        operations::event_administration::list_audit_events(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventSubscriptionStorage for PostgresStorage {
    async fn enabled_event_sink_count(&self) -> Result<i64, StorageError> {
        operations::event_administration::enabled_event_sink_count(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StorageEventPage<StorageEventSink>, StorageError> {
        operations::event_administration::list_event_sinks(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_event_sink(&self, sink_id: i32) -> Result<StorageEventSink, StorageError> {
        operations::event_administration::load_event_sink(&self.pool, sink_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageEventSink, StorageError> {
        operations::event_administration::create_event_sink(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageEventSink, StorageError> {
        operations::event_administration::update_event_sink(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_event_sink(&self, request: StorageEventSinkDelete) -> Result<(), StorageError> {
        operations::event_administration::delete_event_sink(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StorageEventPage<StorageEventSubscription>, StorageError> {
        operations::event_administration::list_event_subscriptions(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_event_subscription(
        &self,
        collection_id: i32,
        subscription_id: i32,
    ) -> Result<StorageEventSubscription, StorageError> {
        operations::event_administration::load_event_subscription(
            &self.pool,
            collection_id,
            subscription_id,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageEventSubscription, StorageError> {
        operations::event_administration::create_event_subscription(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageEventSubscription, StorageError> {
        operations::event_administration::update_event_subscription(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<(), StorageError> {
        operations::event_administration::delete_event_subscription(&self.pool, request)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for PostgresStorage {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StorageEventPage<StorageEventDelivery>, StorageError> {
        hubuum_storage_postgres::operations::event_delivery::list_event_deliveries(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn load_event_delivery(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        hubuum_storage_postgres::operations::event_delivery::load_event_delivery(
            self.runtime(),
            delivery_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        hubuum_storage_postgres::operations::event_delivery::release_event_delivery_for_retry(
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
        hubuum_storage_postgres::operations::event_delivery::mark_event_delivery_dead(
            self.runtime(),
            delivery_id,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl EventDeliveryStorage for PostgresStorage {
    async fn claim_event_delivery_batch(
        &self,
        settings: crate::events::EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        hubuum_storage_postgres::operations::event_delivery::claim_event_delivery_batch(
            self.runtime(),
            settings,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::event_delivery::mark_event_delivery_succeeded(
            self.runtime(),
            claim,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: crate::events::EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::event_delivery::mark_event_delivery_failed(
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
        hubuum_storage_postgres::operations::event_fanout::process_event_fanout_batch(
            self.runtime(),
            settings,
        )
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
        hubuum_storage_postgres::operations::event_retention::process_event_retention_batch(
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
        operations::token_retention::purge_expired_token_batch(&self.pool, settings)
            .await
            .map_err(map_postgres_error)
    }
}
