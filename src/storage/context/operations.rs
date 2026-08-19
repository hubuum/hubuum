use super::*;

#[async_trait]
impl OperationalStateStorage for StorageHandle {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "readiness_snapshot",
            async { dispatch_backend!(self, |backend| { backend.readiness_snapshot().await }) },
        )
        .await
    }

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "maintenance_state",
            async { dispatch_backend!(self, |backend| backend.maintenance_state().await) },
        )
        .await
    }

    async fn storage_snapshot(&self) -> Result<OperationalStorageSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "storage_snapshot",
            async { dispatch_backend!(self, |backend| backend.storage_snapshot().await) },
        )
        .await
    }

    async fn task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "task_queue_snapshot",
            async { dispatch_backend!(self, |backend| { backend.task_queue_snapshot().await }) },
        )
        .await
    }

    async fn export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "export_template_health",
            async { dispatch_backend!(self, |backend| { backend.export_template_health().await }) },
        )
        .await
    }

    async fn export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "export_templates_for_audit",
            async {
                dispatch_backend!(self, |backend| {
                    backend.export_templates_for_audit().await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl EventHealthStorage for StorageHandle {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_health",
            "delivery_health",
            async { dispatch_backend!(self, |backend| { backend.event_delivery_health().await }) },
        )
        .await
    }
}

#[async_trait]
impl AuditEventStorage for StorageHandle {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StorageEventPage<StorageAuditEvent>, StorageError> {
        observe_storage_call(self.backend_name(), "audit_events", "list", async {
            dispatch_backend!(self, |backend| { backend.list_audit_events(query).await })
        })
        .await
    }
}

#[async_trait]
impl EventSubscriptionStorage for StorageHandle {
    async fn enabled_event_sink_count(&self) -> Result<i64, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "count_enabled_sinks",
            async {
                dispatch_backend!(self, |backend| { backend.enabled_event_sink_count().await })
            },
        )
        .await
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StorageEventPage<StorageEventSink>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "list_sinks",
            async { dispatch_backend!(self, |backend| { backend.list_event_sinks(query).await }) },
        )
        .await
    }

    async fn load_event_sink(
        &self,
        sink_id: EventSinkId,
    ) -> Result<StorageEventSink, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "load_sink",
            async { dispatch_backend!(self, |backend| { backend.load_event_sink(sink_id).await }) },
        )
        .await
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
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
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
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
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
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
    ) -> Result<StorageEventPage<StorageEventSubscription>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "list_subscriptions",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_event_subscriptions(query).await
                })
            },
        )
        .await
    }

    async fn load_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "load_subscription",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .load_event_subscription(collection_id, subscription_id)
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
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
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
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
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
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
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
    ) -> Result<StorageEventPage<StorageEventDelivery>, StorageError> {
        observe_storage_call(self.backend_name(), "event_delivery", "list", async {
            dispatch_backend!(self, |backend| {
                backend.list_event_deliveries(query).await
            })
        })
        .await
    }

    async fn load_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        observe_storage_call(self.backend_name(), "event_delivery", "load", async {
            dispatch_backend!(self, |backend| {
                backend.load_event_delivery(delivery_id).await
            })
        })
        .await
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
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
        observe_storage_call(self.backend_name(), "event_delivery", "mark_dead", async {
            dispatch_backend!(self, |backend| {
                backend.mark_event_delivery_dead(delivery_id).await
            })
        })
        .await
    }
}

#[async_trait]
impl EventDeliveryStorage for StorageHandle {
    async fn claim_event_delivery_batch(
        &self,
        settings: EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
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
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
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
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
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
        observe_storage_call(
            self.backend_name(),
            "event_fanout",
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
        observe_storage_call(
            self.backend_name(),
            "event_retention",
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
        observe_storage_call(
            self.backend_name(),
            "event_retention",
            "complete_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend.complete_event_retention_batch(batch_id).await
                })
            },
        )
        .await
    }

    async fn process_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
        archive: &dyn EventArchive,
    ) -> Result<EventRetentionSummary, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_retention",
            "process_batch",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .process_event_retention_batch(settings, archive)
                        .await
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
        observe_storage_call(
            self.backend_name(),
            "token_retention",
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
