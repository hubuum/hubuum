use std::collections::HashMap;
use std::sync::Mutex;

use hubuum_domain::EventDeliveryStatus;
use hubuum_storage_core::{StorageEventDeliverySink, StorageEventDeliverySubscription};
use hubuum_storage_postgres::test_support::claim_event_delivery_by_id;
use rstest::rstest;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::config::{
    DEFAULT_EVENT_DELIVERY_BATCH_SIZE, DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS,
};
use crate::events::{process_event_delivery_batch_for_test, process_event_delivery_work_item};
use crate::tests::test_scope;

use super::*;

fn settings(lock_ms: u64, transport_ms: u64) -> EventDeliverySettings {
    EventDeliverySettings::builder()
        .batch_size(DEFAULT_EVENT_DELIVERY_BATCH_SIZE)
        .lock_timeout_ms(lock_ms)
        .transport_timeout_ms(transport_ms)
        .retry_backoff_base_ms(1_000)
        .retry_backoff_max_ms(10_000)
        .max_attempts(10)
        .build()
        .unwrap()
}

fn default_settings() -> EventDeliverySettings {
    settings(
        DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
        DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS,
    )
}

struct SlowSink {
    delay: Duration,
    sends: Mutex<HashMap<Uuid, usize>>,
}

impl SlowSink {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            sends: Mutex::new(HashMap::new()),
        }
    }
}

impl SinkResolver for SlowSink {
    fn resolve(&self, _: &str) -> Option<&dyn Sink> {
        Some(self)
    }
}

impl Sink for SlowSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        _: &'a StorageEventDeliverySubscription,
        _: &'a StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            *self
                .sends
                .lock()
                .unwrap()
                .entry(envelope.event_id())
                .or_default() += 1;
            sleep(self.delay).await;
            Ok(())
        }
        .boxed()
    }
}

async fn memory_deliveries(count: usize) -> MemoryAuditContractFixture {
    let fixture = MemoryAuditContractFixture::new(MemoryStorage::new())
        .await
        .unwrap();
    for index in 0..count {
        fixture
            .backend
            .collection_store()
            .update_collection(
                fixture.collection_id,
                StorageCollectionUpdate::new(None, Some(format!("delivery wave event {index}"))),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
    fixture
        .backend
        .process_event_fanout_batch(EventFanoutSettings::new(1_000, 30_000).unwrap())
        .await
        .unwrap();
    fixture
}

async fn drain_worker(
    storage: &StorageHandle,
    policy: EventDeliverySettings,
    sink: &SlowSink,
) -> Result<(), ApiError> {
    while process_event_delivery_batch_for_test(storage, policy, sink).await? > 0 {}
    Ok(())
}

// Four waves at 16 seconds cross the default 30-second lease. The second
// worker starts after the original lease would expire, while the first is
// still running. With the old 100-row claim it reclaims queued/running rows
// and the first worker subsequently sends those same events again.
#[actix_web::test]
async fn slow_multiworker_delivery_spans_default_lease_without_duplicate_sends() {
    let fixture = memory_deliveries(32).await;
    let sink = SlowSink::new(Duration::from_secs(16));
    let policy = default_settings();
    let results = timeout(Duration::from_secs(90), async {
        futures::join!(drain_worker(&fixture.backend, policy, &sink), async {
            sleep(Duration::from_millis(policy.lock_timeout_ms() + 1_000)).await;
            drain_worker(&fixture.backend, policy, &sink).await
        },)
    })
    .await
    .expect("workers must drain the queue");
    let sends = sink.sends.lock().unwrap().clone();
    fixture.cleanup().await.unwrap();
    assert_eq!(sends.len(), 32);
    assert!(
        sends.values().all(|count| *count == 1),
        "each event must start exactly one send: {sends:?}"
    );
    results.0.unwrap();
    results.1.unwrap();
}

#[rstest]
#[case::expired_memory(StorageBackendKind::Memory, false)]
#[case::reclaimed_memory(StorageBackendKind::Memory, true)]
#[case::expired_postgres(StorageBackendKind::Postgres, false)]
#[case::reclaimed_postgres(StorageBackendKind::Postgres, true)]
#[actix_web::test]
async fn stale_delivery_claim_never_starts_transport(
    #[case] backend_kind: StorageBackendKind,
    #[case] reclaim: bool,
) {
    let _permit = postgres_permit().await;
    let scope = test_scope();
    let memory;
    let postgres;
    let (backend, item) = match backend_kind {
        StorageBackendKind::Memory => {
            memory = Some(memory_deliveries(1).await);
            postgres = None;
            let fixture = memory.as_ref().unwrap();
            let (mut work, _) = fixture
                .backend
                .claim_event_delivery_batch(settings(200, 100))
                .await
                .unwrap()
                .into_parts();
            (fixture.backend.clone(), work.remove(0))
        }
        StorageBackendKind::Postgres => {
            memory = None;
            postgres = Some(
                PostgresAuditContractFixture::new(scope.pool.get_ref().clone())
                    .await
                    .unwrap(),
            );
            let fixture = postgres.as_ref().unwrap();
            fixture.committed_mutation().await.unwrap();
            let delivery_id = fixture.fanout_deliveries().await.unwrap()[0].id();
            let item = claim_event_delivery_by_id(&scope.pool, delivery_id, settings(200, 100))
                .await
                .unwrap();
            (fixture.backend.clone(), item)
        }
    };
    let (claim, _, _, _) = item.clone().into_parts();
    sleep(Duration::from_millis(250)).await;
    if reclaim {
        let replacement = if postgres.is_some() {
            claim_event_delivery_by_id(&scope.pool, claim.delivery_id(), default_settings())
                .await
                .unwrap()
        } else {
            backend
                .claim_event_delivery_batch(default_settings())
                .await
                .unwrap()
                .into_parts()
                .0
                .remove(0)
        };
        assert_ne!(replacement.into_parts().0.token(), claim.token());
    }
    let sink = SlowSink::new(Duration::ZERO);
    process_event_delivery_work_item(&backend, settings(200, 100), &sink, item)
        .await
        .unwrap();
    if let Some(fixture) = memory {
        fixture.cleanup().await.unwrap();
    }
    if let Some(fixture) = postgres {
        fixture.cleanup().await.unwrap();
    }
    assert!(sink.sends.lock().unwrap().is_empty());
}

#[rstest]
#[case::immediate(0, EventDeliveryStatus::Succeeded)]
#[case::delayed(400, EventDeliveryStatus::Failed)]
#[actix_web::test]
async fn delivery_queue_wait_consumes_transport_budget_and_preserves_acknowledgement_time(
    #[case] queue_ms: u64,
    #[case] expected: EventDeliveryStatus,
) {
    let fixture = memory_deliveries(1).await;
    let policy = settings(1_000, 700);
    let (mut work, _) = fixture
        .backend
        .claim_event_delivery_batch(policy)
        .await
        .unwrap()
        .into_parts();
    let item = work.remove(0);
    let id = item.clone().into_parts().0.delivery_id();
    sleep(Duration::from_millis(queue_ms)).await;
    let sink = SlowSink::new(Duration::from_millis(500));
    process_event_delivery_work_item(&fixture.backend, policy, &sink, item)
        .await
        .unwrap();
    let delivery = fixture.backend.get_event_delivery(id).await.unwrap();
    fixture.cleanup().await.unwrap();
    assert_eq!(delivery.status(), expected);
}

#[actix_web::test]
async fn postgres_delayed_ownership_response_cannot_start_a_reclaimed_delivery() {
    let _permit = postgres_permit().await;
    let scope = test_scope();
    let fixture = PostgresAuditContractFixture::new(scope.pool.get_ref().clone())
        .await
        .unwrap();
    fixture.committed_mutation().await.unwrap();
    let id = fixture.fanout_deliveries().await.unwrap()[0].id();
    let policy = settings(1_000, 700);
    let item = claim_event_delivery_by_id(&scope.pool, id, policy)
        .await
        .unwrap();
    let old_claim = item.clone().into_parts().0;
    let sink = SlowSink::new(Duration::ZERO);
    let controller =
        PostgresFaultController::pausing(PostgresFaultPoint::EventDeliveryAfterOwnershipCheck);
    let (result, ()) = timeout(Duration::from_secs(10), async {
        futures::join!(
            controller.run(process_event_delivery_work_item(
                &fixture.backend,
                policy,
                &sink,
                item
            )),
            async {
                controller.wait_until_reached().await;
                sleep(Duration::from_millis(1_100)).await;
                let replacement = claim_event_delivery_by_id(&scope.pool, id, default_settings())
                    .await
                    .unwrap();
                assert_ne!(replacement.into_parts().0.token(), old_claim.token());
                controller.resume();
            },
        )
    })
    .await
    .expect("paused ownership check must finish");
    result.unwrap();
    fixture.cleanup().await.unwrap();
    assert!(sink.sends.lock().unwrap().is_empty());
}

#[actix_web::test]
async fn postgres_delivery_acknowledgement_is_bounded_by_the_lease() {
    let _permit = postgres_permit().await;
    let scope = test_scope();
    let fixture = PostgresAuditContractFixture::new(scope.pool.get_ref().clone())
        .await
        .unwrap();
    fixture.committed_mutation().await.unwrap();
    let id = fixture.fanout_deliveries().await.unwrap()[0].id();
    let policy = settings(1_000, 700);
    let item = claim_event_delivery_by_id(&scope.pool, id, policy)
        .await
        .unwrap();
    let sink = SlowSink::new(Duration::ZERO);
    let controller =
        PostgresFaultController::pausing(PostgresFaultPoint::EventDeliveryBeforeAcknowledge);
    // The acknowledgement is deliberately never resumed. Lease expiry must
    // cancel it and release the transaction so another worker can recover.
    let result = timeout(
        Duration::from_secs(5),
        controller.run(process_event_delivery_work_item(
            &fixture.backend,
            policy,
            &sink,
            item,
        )),
    )
    .await
    .expect("acknowledgement must not outlive its lease");
    assert!(
        matches!(result, Err(ApiError::ServiceUnavailable(message)) if message.contains("lease expired"))
    );
    // The conservative local deadline can precede storage expiry by a query
    // round trip. Allow the original storage lease to expire before recovery.
    sleep(Duration::from_millis(policy.lock_timeout_ms())).await;
    let replacement = claim_event_delivery_by_id(&scope.pool, id, default_settings())
        .await
        .unwrap();
    fixture
        .backend
        .mark_event_delivery_succeeded(&replacement.into_parts().0)
        .await
        .unwrap();
    fixture.cleanup().await.unwrap();
}
