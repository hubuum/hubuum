use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_operational_state() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let state = backend
            .get_maintenance_state()
            .await
            .expect("certified backend should expose maintenance state");
        let readiness = backend
            .get_readiness_snapshot()
            .await
            .expect("certified backend should expose readiness state");

        assert_eq!(readiness.maintenance_state(), state);
        assert!(readiness.storage_is_ready());
        let task_queue = backend
            .get_task_queue_snapshot()
            .await
            .expect("certified backend should expose task queue diagnostics");
        assert!(task_queue.statuses().total() >= 0);
        assert!(task_queue.total_task_events() >= 0);
        let export_health = backend
            .load_export_template_health()
            .await
            .expect("certified backend should aggregate export-template health");
        assert!(export_health.iter().all(|row| row.runs() > 0));
        let audit_entries = backend
            .load_export_templates_for_audit()
            .await
            .expect("certified backend should supply the template audit set");
        assert!(audit_entries.windows(2).all(|entries| {
            (entries[0].collection_id(), entries[0].id())
                <= (entries[1].collection_id(), entries[1].id())
        }));
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_event_health() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let health = backend
            .get_event_delivery_health()
            .await
            .expect("certified backend should expose event delivery health");
        assert!(health.fanout().pending_events() >= 0);
        assert!(health.delivery().counts().total() >= 0);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_event_administration() {
    let _permit = postgres_permit().await;
    let options = || {
        QueryOptions::new(Vec::new(), Vec::new(), Some(50), None, true)
            .expect("contract query must be valid")
    };
    let fanout_settings = EventFanoutSettings::new(1_000, 30_000)
        .expect("compatibility fan-out settings should be valid");
    let event_admin_collection_id = collection_id(1);

    for backend in available_backends() {
        let event_context = EventContext::system();
        let sink_name = prefix("event_admin_sink");
        let sink = backend
            .create_event_sink(
                StorageEventSinkCreate::builder(sink_name, "webhook", event_context.clone())
                    .configuration(serde_json::json!({}))
                    .enabled(true)
                    .try_build()
                    .unwrap(),
            )
            .await
            .expect("certified backend should create event sinks")
            .into_value();
        let sink_id = sink.id();

        assert!(
            backend
                .count_enabled_event_sinks()
                .await
                .expect("certified backend should count enabled event sinks")
                >= 1
        );
        assert_eq!(
            backend
                .get_event_sink(sink_id)
                .await
                .expect("certified backend should load event sinks")
                .id(),
            sink_id
        );
        let (sinks, sink_total) = backend
            .list_event_sinks(StorageEventSinkListQuery::new(options()))
            .await
            .expect("certified backend should list event sinks")
            .into_parts();
        assert!(!sinks.is_empty());
        assert!(sink_total.is_some_and(|total| total >= 1));
        let updated_sink = backend
            .update_event_sink(
                StorageEventSinkUpdate::builder(sink_id, event_context.clone())
                    .name(Some(prefix("event_admin_sink_updated")))
                    .try_build()
                    .unwrap(),
            )
            .await
            .expect("certified backend should update event sinks")
            .into_value();
        assert!(updated_sink.revision() > sink.revision());

        backend
            .change_event_sink_grant(StorageEventSinkGrantChange::new(
                sink_id,
                event_admin_collection_id,
                EventSinkGrantAction::Grant,
                event_context.clone(),
            ))
            .await
            .expect("fixture grant")
            .into_value();
        assert_eq!(
            backend.list_event_sink_collections(sink_id).await.unwrap(),
            vec![event_admin_collection_id]
        );
        let authorized = backend
            .resolve_event_sink_use(event_admin_collection_id, sink_id)
            .await
            .unwrap();
        assert_eq!(authorized.collection_id(), event_admin_collection_id);
        assert_eq!(authorized.sink().id(), sink_id);
        let subscription = backend
            .create_event_subscription(
                StorageEventSubscriptionCreate::builder(
                    event_admin_collection_id,
                    sink_id,
                    prefix("event_admin_subscription"),
                    event_context.clone(),
                )
                .description("storage compatibility event subscription")
                .entity_types(vec![EntityType::EventSubscription])
                .actions(vec![Action::Created])
                .routing(serde_json::json!({}))
                .enabled(true)
                .try_build()
                .unwrap(),
            )
            .await
            .expect("certified backend should create event subscriptions")
            .into_value();
        let subscription_id = subscription.id();
        assert_eq!(
            backend
                .get_event_subscription(event_admin_collection_id.into(), subscription_id)
                .await
                .expect("certified backend should load scoped subscriptions")
                .scope()
                .collection_id(),
            Some(event_admin_collection_id)
        );
        let (subscriptions, subscription_total) = backend
            .list_event_subscriptions(StorageEventSubscriptionListQuery::new(
                event_admin_collection_id,
                options(),
            ))
            .await
            .expect("certified backend should list scoped subscriptions")
            .into_parts();
        assert!(!subscriptions.is_empty());
        assert!(subscription_total.is_some_and(|total| total >= 1));
        let updated_subscription = backend
            .update_event_subscription(
                StorageEventSubscriptionUpdate::builder(
                    event_admin_collection_id,
                    subscription_id,
                    event_context.clone(),
                )
                .description(Some(
                    "updated storage compatibility subscription".to_string(),
                ))
                .try_build()
                .unwrap(),
            )
            .await
            .expect("certified backend should update event subscriptions")
            .into_value();
        assert!(updated_subscription.revision() > subscription.revision());

        let (audit_events, audit_total) = backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![event_admin_collection_id],
                false,
                StorageAuditEventFilters::new()
                    .entity_type(Some(EntityType::EventSubscription))
                    .entity_id(Some(
                        EventEntityId::new(subscription_id.id())
                            .expect("persisted event-subscription id must be positive"),
                    )),
                options(),
            ))
            .await
            .expect("certified backend should list event audit records")
            .into_parts();
        assert!(audit_events.len() >= 2);
        assert!(audit_total.is_some_and(|total| total >= 2));

        let mut delivery = None;
        for _ in 0..20 {
            let (deliveries, total) = backend
                .list_event_deliveries(
                    StorageEventDeliveryListQuery::new(options())
                        .subscription_id(Some(subscription_id)),
                )
                .await
                .expect("certified backend should list event deliveries")
                .into_parts();
            assert!(total.is_some());
            if let Some(row) = deliveries.into_iter().next() {
                delivery = Some(row);
                break;
            }
            backend
                .process_event_fanout_batch(fanout_settings)
                .await
                .expect("certified backend should fan out lifecycle events");
        }
        let delivery =
            delivery.expect("event-administration compatibility event should produce a delivery");
        let delivery_id = delivery.id();
        let dead = backend
            .mark_event_delivery_dead(delivery_id)
            .await
            .expect("certified backend should dead-letter event deliveries");
        assert_eq!(dead.status(), EventDeliveryStatus::Dead);
        let pending = backend
            .release_event_delivery_for_retry(delivery_id)
            .await
            .expect("certified backend should release event deliveries for retry");
        assert_eq!(pending.status(), EventDeliveryStatus::Pending);
        assert_eq!(
            backend
                .get_event_delivery(delivery_id)
                .await
                .expect("certified backend should load event deliveries")
                .id(),
            delivery_id
        );

        backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                event_admin_collection_id,
                subscription_id,
                event_context.clone(),
            ))
            .await
            .expect("event-subscription compatibility fixture should be removed")
            .into_value();
        backend
            .delete_event_sink(StorageEventSinkDelete::new(sink_id, event_context))
            .await
            .expect("event-sink compatibility fixture should be removed")
            .into_value();
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_fanout() {
    let _permit = postgres_permit().await;
    let settings = EventFanoutSettings::new(10, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for backend in available_backends() {
        let processed = backend
            .process_event_fanout_batch(settings)
            .await
            .expect("certified backend should process event fan-out");
        assert!(processed.processed() <= settings.batch_size());
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_retention() {
    struct DiscardArchive;

    #[async_trait]
    impl EventArchiveSink for DiscardArchive {
        async fn archive(&self, _batch: &StorageEventRetentionBatch) -> Result<(), StorageError> {
            Ok(())
        }
    }

    let _permit = postgres_permit().await;
    let settings = EventRetentionSettings::new(10_000, 10_000, 10)
        .expect("compatibility event-retention settings should be valid");

    for backend in available_backends() {
        let summary =
            crate::storage::execute_event_retention_batch(&backend, settings, &DiscardArchive)
                .await
                .expect("certified backend should process event retention");

        assert!(!summary.did_work());
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_token_retention() {
    let _permit = postgres_permit().await;
    let settings = TokenRetentionSettings::builder()
        .retention_days(1_000_000)
        .token_lifetime_hours(24)
        .batch_size(10)
        .build()
        .expect("compatibility retention settings should be valid");

    for backend in available_backends() {
        let purged = backend
            .purge_expired_tokens(settings)
            .await
            .expect("certified backend should execute token retention");

        assert_eq!(purged, 0);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_metrics_snapshots() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let inventory = backend
            .get_inventory_metrics_snapshot()
            .await
            .expect("certified backend should supply inventory metrics");
        assert!(inventory.counts().collections() >= 1);

        let tasks = backend
            .get_task_metrics_snapshot()
            .await
            .expect("certified backend should supply task metrics");
        assert_eq!(tasks.ages().len(), StorageTaskKind::ALL.len());

        let events = backend
            .get_event_metrics_snapshot()
            .await
            .expect("certified backend should supply event metrics");
        assert!(events.fanout().pending_events() >= 0);
        assert!(events.delivery().counts().total() >= 0);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_consistent_inventory_counts() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let counts = backend
            .get_inventory_counts()
            .await
            .expect("certified backend should supply inventory counts");
        let grouped_objects = counts
            .objects_by_class()
            .iter()
            .map(|row| row.count())
            .sum::<i64>();

        assert_eq!(grouped_objects, counts.total_objects());
        assert!(counts.total_classes() >= counts.objects_by_class().len() as i64);
        assert!(counts.total_collections() >= 1);
        assert!(
            counts
                .objects_by_class()
                .windows(2)
                .all(|rows| rows[0].class_id() < rows[1].class_id()),
            "per-class counts must use stable class-id ordering"
        );
    }
}
