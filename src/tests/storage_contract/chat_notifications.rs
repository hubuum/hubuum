use super::*;
use hubuum_domain::{EventDeliveryPolicy, EventDeliveryPurpose};
use hubuum_events_core::{EventSubscriptionFilter, EventSubscriptionScope};
use hubuum_storage_core::{StorageEventDeliveryDisposition, StorageEventNotificationSelection};
use hubuum_storage_postgres::test_support::claim_event_delivery_by_id;
use rstest::rstest;

async fn setup(
    backend: &StorageHandle,
    name: String,
) -> (
    hubuum_domain::EventSinkId,
    StorageEventNotificationSelection,
) {
    let created = backend
        .create_event_sink(
            StorageEventSinkCreate::builder(name.clone(), "slack", EventContext::system())
                .configuration(serde_json::json!({"transport":"webhook"}))
                .delivery_policy(EventDeliveryPolicy::new(Some(60_000)).unwrap())
                .enabled(false)
                .try_build()
                .unwrap(),
        )
        .await
        .unwrap();
    let event_id = created.audits().unwrap().first().event_id().as_uuid();
    let sink = created.into_value();
    let subscription = backend
        .create_event_subscription(
            StorageEventSubscriptionCreate::builder(
                EventSubscriptionScope::System,
                sink.id(),
                name,
                EventContext::system(),
            )
            .entity_types(vec![EntityType::Task])
            .actions(vec![Action::Failed])
            .filter(EventSubscriptionFilter {
                task_kinds: vec![hubuum_domain::TaskKind::Backup],
                ..Default::default()
            })
            .enabled(false)
            .try_build()
            .unwrap(),
        )
        .await
        .unwrap()
        .into_value();
    (
        sink.id(),
        StorageEventNotificationSelection::new(sink.id(), subscription.id(), event_id),
    )
}

#[actix_web::test]
async fn system_test_delivery_uses_real_event_but_bypasses_filters_and_enabled_flags() {
    let _permit = postgres_permit().await;
    let scope = crate::tests::test_scope();
    for backend in available_backends() {
        let (sink, selection) = setup(&backend, scope.scoped_name("system_test")).await;
        let input = backend.load_event_notification(selection).await.unwrap();
        assert_eq!(input.event().event_id(), selection.event_id());
        let first = backend
            .enqueue_event_notification_test(selection, EventContext::system())
            .await
            .unwrap()
            .into_value();
        let second = backend
            .enqueue_event_notification_test(selection, EventContext::system())
            .await
            .unwrap()
            .into_value();
        assert_ne!(first.id(), second.id());
        assert_eq!(first.purpose(), EventDeliveryPurpose::Test);
        backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                EventSubscriptionScope::System,
                selection.subscription_id(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        backend
            .delete_event_sink(StorageEventSinkDelete::new(sink, EventContext::system()))
            .await
            .unwrap()
            .into_value();
    }
}

#[rstest]
#[case::configured(false)]
#[case::provider(true)]
#[actix_web::test]
async fn shared_sink_deferral_preserves_attempts_and_fences_old_claims(#[case] provider: bool) {
    let _permit = postgres_permit().await;
    let scope = crate::tests::test_scope();
    for backend in available_backends() {
        let (sink, selection) = setup(&backend, scope.scoped_name("sink_spacing")).await;
        let mut ids = Vec::new();
        for _ in 0..2 {
            ids.push(
                backend
                    .enqueue_event_notification_test(selection, EventContext::system())
                    .await
                    .unwrap()
                    .into_value()
                    .id(),
            );
        }
        let policy = EventDeliverySettings::builder().build().unwrap();
        let mut claims = Vec::new();
        for id in ids {
            let item = if backend.descriptor().kind() == StorageBackendKind::Postgres {
                claim_event_delivery_by_id(&scope.pool, id, policy)
                    .await
                    .unwrap()
            } else {
                backend
                    .claim_event_delivery_batch(policy)
                    .await
                    .unwrap()
                    .into_parts()
                    .0
                    .remove(0)
            };
            claims.push(item.into_parts().0);
        }
        let first = backend
            .begin_event_delivery(&claims[0])
            .await
            .unwrap()
            .unwrap();
        if provider {
            backend
                .finish_event_delivery(
                    first.claim(),
                    StorageEventDeliveryDisposition::RateLimited(Duration::from_secs(90)),
                )
                .await
                .unwrap();
        } else {
            backend
                .mark_event_delivery_succeeded(first.claim())
                .await
                .unwrap();
        }
        assert!(
            backend
                .begin_event_delivery(&claims[1])
                .await
                .unwrap()
                .is_none()
        );
        // A stale worker cannot acknowledge the deferred delivery.
        let _ = backend.mark_event_delivery_succeeded(&claims[1]).await;
        let deferred = backend
            .get_event_delivery(claims[1].delivery_id())
            .await
            .unwrap();
        assert_eq!(deferred.status(), EventDeliveryStatus::Pending);
        assert_eq!(deferred.attempts(), 0);
        assert_eq!(
            deferred.deferred_reason(),
            Some(if provider {
                "provider_rate"
            } else {
                "configured_rate"
            })
        );
        backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                EventSubscriptionScope::System,
                selection.subscription_id(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        backend
            .delete_event_sink(StorageEventSinkDelete::new(sink, EventContext::system()))
            .await
            .unwrap()
            .into_value();
    }
}

#[actix_web::test]
async fn notification_selection_rejects_another_sink() {
    let _permit = postgres_permit().await;
    let scope = crate::tests::test_scope();
    for backend in available_backends() {
        let (first, selection) = setup(&backend, scope.scoped_name("first_sink")).await;
        let (second, second_selection) = setup(&backend, scope.scoped_name("second_sink")).await;
        let wrong = StorageEventNotificationSelection::new(
            second,
            selection.subscription_id(),
            selection.event_id(),
        );
        assert!(backend.load_event_notification(wrong).await.is_err());
        for (id, selection) in [(first, selection), (second, second_selection)] {
            backend
                .delete_event_subscription(StorageEventSubscriptionDelete::new(
                    EventSubscriptionScope::System,
                    selection.subscription_id(),
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
            backend
                .delete_event_sink(StorageEventSinkDelete::new(id, EventContext::system()))
                .await
                .unwrap()
                .into_value();
        }
    }
}
