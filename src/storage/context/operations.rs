use super::*;

#[async_trait]
impl OperationalStateStorage for StorageHandle {
    async fn get_readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
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
            "maintenance_state",
            async { dispatch_backend!(self, |backend| backend.get_maintenance_state().await) },
        )
        .await
    }

    async fn get_task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
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

    async fn get_export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "get_export_template_health",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_export_template_health().await
                })
            },
        )
        .await
    }

    async fn list_export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::OperationalState,
            "list_export_templates_for_audit",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_export_templates_for_audit().await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl EventHealthStorage for StorageHandle {
    async fn get_event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventHealth,
            "delivery_health",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_event_delivery_health().await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl AuditEventStorage for StorageHandle {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StoragePage<StorageAuditEvent>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::AuditEvents,
            "list",
            async { dispatch_backend!(self, |backend| { backend.list_audit_events(query).await }) },
        )
        .await
    }
}

#[async_trait]
impl EventConfigurationStorage for StorageHandle {
    async fn count_enabled_event_sinks(&self) -> Result<i64, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "count_enabled_sinks",
            async {
                dispatch_backend!(self, |backend| {
                    backend.count_enabled_event_sinks().await
                })
            },
        )
        .await
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StoragePage<StorageEventSink>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "list_sinks",
            async { dispatch_backend!(self, |backend| { backend.list_event_sinks(query).await }) },
        )
        .await
    }

    async fn get_event_sink(&self, sink_id: EventSinkId) -> Result<StorageEventSink, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "get_sink",
            async { dispatch_backend!(self, |backend| { backend.get_event_sink(sink_id).await }) },
        )
        .await
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "create_sink",
            async {
                dispatch_backend!(self, |backend| { backend.create_event_sink(request).await })
            },
        )
        .await
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "update_sink",
            async {
                dispatch_backend!(self, |backend| { backend.update_event_sink(request).await })
            },
        )
        .await
    }

    async fn delete_event_sink(
        &self,
        request: StorageEventSinkDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "delete_sink",
            async {
                dispatch_backend!(self, |backend| { backend.delete_event_sink(request).await })
            },
        )
        .await
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StoragePage<StorageEventSubscription>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "list_subscriptions",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_event_subscriptions(query).await
                })
            },
        )
        .await
    }

    async fn get_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "get_subscription",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_event_subscription(collection_id, subscription_id)
                        .await
                })
            },
        )
        .await
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "create_subscription",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_event_subscription(request).await
                })
            },
        )
        .await
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "update_subscription",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_event_subscription(request).await
                })
            },
        )
        .await
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventConfiguration,
            "delete_subscription",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_event_subscription(request).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for StorageHandle {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StoragePage<StorageEventDelivery>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryAdministration,
            "list",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_event_deliveries(query).await
                })
            },
        )
        .await
    }

    async fn get_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryAdministration,
            "get",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_event_delivery(delivery_id).await
                })
            },
        )
        .await
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryAdministration,
            "release_for_retry",
            async {
                dispatch_backend!(self, |backend| {
                    backend.release_event_delivery_for_retry(delivery_id).await
                })
            },
        )
        .await
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryAdministration,
            "mark_dead",
            async {
                dispatch_backend!(self, |backend| {
                    backend.mark_event_delivery_dead(delivery_id).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl EventDeliveryWorkerStorage for StorageHandle {
    async fn claim_event_delivery_batch(
        &self,
        settings: EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryWorker,
            "claim_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend.claim_event_delivery_batch(settings).await
                })
            },
        )
        .await
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryWorker,
            "mark_succeeded",
            async {
                dispatch_backend!(self, |backend| {
                    backend.mark_event_delivery_succeeded(claim).await
                })
            },
        )
        .await
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventDeliveryWorker,
            "mark_failed",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .mark_event_delivery_failed(claim, settings, error)
                        .await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl EventFanoutStorage for StorageHandle {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventFanout,
            "process_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend.process_event_fanout_batch(settings).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl EventRetentionStorage for StorageHandle {
    async fn claim_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
    ) -> Result<Option<EventRetentionBatch>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventRetention,
            "claim_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend.claim_event_retention_batch(settings).await
                })
            },
        )
        .await
    }

    async fn complete_event_retention_batch(
        &self,
        batch_id: EventRetentionBatchId,
    ) -> Result<EventRetentionSummary, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::EventRetention,
            "complete_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend.complete_event_retention_batch(batch_id).await
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
            "purge_expired",
            async {
                dispatch_backend!(self, |backend| {
                    backend.purge_expired_tokens(settings).await
                })
            },
        )
        .await
    }
}
