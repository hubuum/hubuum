use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;

use actix_rt::time::sleep;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::config::{
    DEFAULT_EVENT_DELIVERY_BATCH_SIZE, DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS, DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS,
    DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS, DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS,
    DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS, DEFAULT_EVENT_DELIVERY_WORKERS, get_config,
};
use crate::errors::ApiError;
use crate::events::sink::{
    DefaultSinkResolver, EventEnvelope, SinkError, SinkResolver, event_envelope_with_names,
};
use crate::events::{EntityType, EventDeliverySettings, PrincipalNames};
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::models::{EventSink, EventSubscription, EventWorkerWakeupStats};
use crate::observability::metrics;
use crate::restores::MaintenanceActivityGuard;
use crate::storage::StorageContext;
use crate::storage::postgres::operations::event_delivery::{
    ClaimedEventDelivery, claim_event_delivery_batch, mark_event_delivery_failed,
    mark_event_delivery_succeeded,
};
use crate::storage::postgres::operations::events::load_queued_task_initiators;
use crate::storage::postgres::operations::history::resolve_principal_names;
use crate::storage::postgres::{StorageCallSite, with_storage_call_site};
use crate::storage::{StorageHandle, storage_handle};

static EVENT_DELIVERY_WORKER: Once = Once::new();
static EVENT_DELIVERY_LISTENER: Once = Once::new();
static EVENT_DELIVERY_NOTIFY: OnceLock<Notify> = OnceLock::new();
static EVENT_DELIVERY_NOTIFICATIONS_SENT: AtomicU64 = AtomicU64::new(0);
static EVENT_DELIVERY_NOTIFICATION_WAKEUPS: AtomicU64 = AtomicU64::new(0);
static EVENT_DELIVERY_POLL_WAKEUPS: AtomicU64 = AtomicU64::new(0);
static DEFAULT_SINK_RESOLVER: std::sync::LazyLock<DefaultSinkResolver> =
    std::sync::LazyLock::new(DefaultSinkResolver::default);

const EVENT_DELIVERY_MAX_CONCURRENCY_PER_WORKER: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EventDeliveryBatchOutcome {
    processed: usize,
    next_wakeup_in: Option<Duration>,
}

fn get_event_delivery_notify() -> &'static Notify {
    EVENT_DELIVERY_NOTIFY.get_or_init(Notify::new)
}

fn wake_event_delivery_worker_from_postgres() {
    get_event_delivery_notify().notify_one();
}

fn configured_event_delivery_worker_count() -> usize {
    get_config()
        .map(|config| {
            config
                .runtime_role
                .effective_worker_count(config.event_delivery_workers)
        })
        .unwrap_or(DEFAULT_EVENT_DELIVERY_WORKERS)
}

fn configured_event_delivery_poll_interval() -> Duration {
    let interval_ms = get_config()
        .map(|config| config.event_delivery_poll_interval_ms)
        .unwrap_or(DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS);
    Duration::from_millis(interval_ms)
}

fn configured_event_delivery_settings() -> Result<EventDeliverySettings, ApiError> {
    match get_config() {
        Ok(config) => config.event_delivery_settings(),
        Err(_) => EventDeliverySettings::builder()
            .batch_size(DEFAULT_EVENT_DELIVERY_BATCH_SIZE)
            .lock_timeout_ms(DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS)
            .transport_timeout_ms(DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS)
            .retry_backoff_base_ms(DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS)
            .retry_backoff_max_ms(DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS)
            .max_attempts(DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS)
            .build()
            .map_err(ApiError::BadRequest),
    }
}

async fn process_event_delivery_batch_with_schedule(
    pool: &impl crate::storage::StorageContext,
    settings: EventDeliverySettings,
    resolver: &dyn SinkResolver,
) -> Result<EventDeliveryBatchOutcome, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    let (mut deliveries, next_wakeup_in) = claim_event_delivery_batch(pool, settings)
        .await?
        .into_parts();
    let processed = deliveries.len();
    let legacy_task_ids = deliveries
        .iter()
        .filter(|claimed| {
            claimed.event.entity_type == EntityType::Task.as_str()
                && claimed.event.initiator_user_id.is_none()
        })
        .filter_map(|claimed| claimed.event.entity_id)
        .collect::<Vec<_>>();
    let queued_initiators = load_queued_task_initiators(pool, &legacy_task_ids).await?;
    for claimed in &mut deliveries {
        if claimed.event.entity_type != EntityType::Task.as_str() {
            continue;
        }
        let Some(task_id) = claimed.event.entity_id else {
            continue;
        };
        claimed.event.task_id.get_or_insert(task_id);
        if claimed.event.initiator_user_id.is_none() {
            claimed.event.initiator_user_id = queued_initiators.get(&task_id).copied().flatten();
        }
    }
    let principal_ids = deliveries
        .iter()
        .flat_map(|claimed| [claimed.event.actor_user_id, claimed.event.initiator_user_id])
        .flatten()
        .collect();
    let principal_names = Arc::new(resolve_principal_names(pool, principal_ids).await?);
    let results = futures_util::stream::iter(deliveries)
        .map(|claimed| {
            process_claimed_event_delivery_with_names(
                pool,
                settings,
                resolver,
                claimed,
                Arc::clone(&principal_names),
            )
        })
        .buffer_unordered(EVENT_DELIVERY_MAX_CONCURRENCY_PER_WORKER)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result?;
    }

    Ok(EventDeliveryBatchOutcome {
        processed,
        next_wakeup_in,
    })
}

#[cfg(test)]
pub(crate) async fn process_claimed_event_delivery(
    pool: &impl crate::storage::StorageContext,
    settings: EventDeliverySettings,
    resolver: &dyn SinkResolver,
    mut claimed: ClaimedEventDelivery,
) -> Result<(), ApiError> {
    let task_ids = claimed
        .event
        .entity_id
        .filter(|_| claimed.event.entity_type == EntityType::Task.as_str())
        .into_iter()
        .collect::<Vec<_>>();
    let queued_initiators = load_queued_task_initiators(pool, &task_ids).await?;
    if let Some(task_id) = task_ids.first().copied() {
        claimed.event.task_id.get_or_insert(task_id);
        if claimed.event.initiator_user_id.is_none() {
            claimed.event.initiator_user_id = queued_initiators.get(&task_id).copied().flatten();
        }
    }
    let principal_ids = [claimed.event.actor_user_id, claimed.event.initiator_user_id]
        .into_iter()
        .flatten()
        .collect();
    let principal_names = Arc::new(resolve_principal_names(pool, principal_ids).await?);
    process_claimed_event_delivery_with_names(pool, settings, resolver, claimed, principal_names)
        .await
}

async fn process_claimed_event_delivery_with_names(
    pool: &impl crate::storage::StorageContext,
    settings: EventDeliverySettings,
    resolver: &dyn SinkResolver,
    claimed: ClaimedEventDelivery,
    principal_names: Arc<PrincipalNames>,
) -> Result<(), ApiError> {
    let delivery = claimed.delivery;
    let envelope = event_envelope_with_names(claimed.event, &principal_names);
    let subscription = EventSubscription::try_from(claimed.subscription)?;
    let sink = EventSink::try_from(claimed.sink)?;
    let result = tokio::time::timeout(
        settings.transport_timeout(),
        deliver_one(resolver, &envelope, &subscription, &sink),
    )
    .await
    .map_err(|_| {
        SinkError::new(format!(
            "Event delivery transport timed out after {} ms",
            settings.transport_timeout_ms()
        ))
    })
    .and_then(|result| result);

    match result {
        Ok(()) => {
            let claim_token = delivery.claim_token.ok_or_else(|| {
                ApiError::InternalServerError(
                    "claimed event delivery is missing claim_token".to_string(),
                )
            })?;
            mark_event_delivery_succeeded(pool, delivery.id, claim_token).await?;
        }
        Err(error) => {
            warn!(
                message = "Event sink delivery failed",
                event_delivery_id = delivery.id,
                event_id = %envelope.event_id,
                event_sink_id = sink.id,
                event_subscription_id = subscription.id,
                sink_kind = sink.kind.as_str(),
                error = %error,
            );
            mark_event_delivery_failed(pool, &delivery, settings, &error.to_string()).await?;
        }
    }

    Ok(())
}

async fn deliver_one(
    resolver: &dyn SinkResolver,
    envelope: &EventEnvelope,
    subscription: &EventSubscription,
    sink: &EventSink,
) -> Result<(), SinkError> {
    let Some(transport) = resolver.resolve(sink.kind) else {
        return Err(SinkError::new(format!(
            "No event sink transport is registered for kind '{}'",
            sink.kind.as_str()
        )));
    };

    transport.deliver(envelope, subscription, sink).await
}

fn delivery_worker_should_continue(result: &Result<EventDeliveryBatchOutcome, ApiError>) -> bool {
    match result {
        Ok(outcome) => outcome.processed > 0,
        Err(error) => {
            error!(message = "Event delivery worker iteration failed", error = %error);
            false
        }
    }
}

fn event_delivery_wait_duration(
    poll_interval: Duration,
    next_wakeup_in: Option<Duration>,
) -> Duration {
    next_wakeup_in
        .map(|retry_wait| retry_wait.min(poll_interval))
        .unwrap_or(poll_interval)
}

async fn wait_for_event_delivery_wakeup(
    poll_interval: Duration,
    next_wakeup_in: Option<Duration>,
    shutdown: &ShutdownSignal,
) -> bool {
    let wait_duration = event_delivery_wait_duration(poll_interval, next_wakeup_in);
    tokio::select! {
        biased;
        _ = shutdown.requested() => false,
        _ = sleep(wait_duration) => {
            EVENT_DELIVERY_POLL_WAKEUPS.fetch_add(1, Ordering::Relaxed);
            metrics::event_worker_wakeup("delivery", "poll");
            true
        }
        _ = get_event_delivery_notify().notified() => {
            EVENT_DELIVERY_NOTIFICATION_WAKEUPS.fetch_add(1, Ordering::Relaxed);
            metrics::event_worker_wakeup("delivery", "notification");
            true
        }
    }
}

async fn event_delivery_worker_loop(
    pool: StorageHandle,
    settings: EventDeliverySettings,
    poll_interval: Duration,
    resolver: &'static dyn SinkResolver,
    shutdown: ShutdownSignal,
) {
    loop {
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            result = with_storage_call_site(
                StorageCallSite::EventDelivery,
                process_event_delivery_batch_with_schedule(&pool, settings, resolver),
            ) => result,
        };
        let next_wakeup_in = result
            .as_ref()
            .ok()
            .and_then(|outcome| outcome.next_wakeup_in);
        if delivery_worker_should_continue(&result) {
            continue;
        }
        if !wait_for_event_delivery_wakeup(poll_interval, next_wakeup_in, &shutdown).await {
            break;
        }
    }
}

fn spawn_event_delivery_worker_loop(
    pool: StorageHandle,
    settings: EventDeliverySettings,
    poll_interval: Duration,
    worker_index: usize,
    resolver: &'static dyn SinkResolver,
) {
    spawn_background_worker(
        format!("event-delivery-worker-{worker_index}"),
        move |shutdown| {
            info!(
                message = "Starting event delivery worker loop",
                worker_index = worker_index,
                batch_size = settings.batch_size(),
                lock_timeout_ms = settings.lock_timeout_ms(),
                retry_backoff_base_ms = settings.retry_backoff_base_ms(),
                retry_backoff_max_ms = settings.retry_backoff_max_ms(),
                max_attempts = settings.max_attempts(),
                poll_interval = ?poll_interval
            );
            let system = actix_rt::System::new();
            system.block_on(event_delivery_worker_loop(
                pool,
                settings,
                poll_interval,
                resolver,
                shutdown,
            ));
        },
    );
}

pub fn ensure_event_delivery_worker_running<C>(backend: C)
where
    C: StorageContext,
{
    let pool = storage_handle(&backend);
    let worker_count = configured_event_delivery_worker_count();
    if worker_count == 0 {
        return;
    }

    let poll_interval = configured_event_delivery_poll_interval();
    let settings = match configured_event_delivery_settings() {
        Ok(settings) => settings,
        Err(error) => {
            error!(message = "Event delivery settings are invalid", error = %error);
            return;
        }
    };

    EVENT_DELIVERY_LISTENER.call_once(|| {
        super::pg_notify::spawn_postgres_notification_listener(
            super::pg_notify::EVENT_DELIVERY_CHANNEL,
            "event-delivery-pg-listener",
            wake_event_delivery_worker_from_postgres,
        );
    });

    EVENT_DELIVERY_WORKER.call_once(move || {
        info!(
            message = "Initializing event delivery workers",
            worker_count = worker_count,
            batch_size = settings.batch_size(),
            lock_timeout_ms = settings.lock_timeout_ms(),
            poll_interval = ?poll_interval
        );
        for worker_index in 0..worker_count {
            spawn_event_delivery_worker_loop(
                pool.clone(),
                settings,
                poll_interval,
                worker_index,
                &*DEFAULT_SINK_RESOLVER,
            );
        }
    });
}

pub fn kick_event_delivery_worker<C>(backend: C)
where
    C: StorageContext,
{
    ensure_event_delivery_worker_running(backend);
    EVENT_DELIVERY_NOTIFICATIONS_SENT.fetch_add(1, Ordering::Relaxed);
    metrics::event_worker_wakeup("delivery", "notifications_sent");
    get_event_delivery_notify().notify_one();
}

pub fn event_delivery_wakeup_stats() -> EventWorkerWakeupStats {
    EventWorkerWakeupStats {
        notifications_sent: EVENT_DELIVERY_NOTIFICATIONS_SENT.load(Ordering::Relaxed),
        notification_wakeups: EVENT_DELIVERY_NOTIFICATION_WAKEUPS.load(Ordering::Relaxed),
        poll_wakeups: EVENT_DELIVERY_POLL_WAKEUPS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use rstest::rstest;

    use crate::events::sink::{EventEnvelope, NoopSinkResolver, Sink, SinkError};
    use crate::models::EventSinkKind;

    use super::*;

    struct StaticResolver<'a> {
        kind: EventSinkKind,
        sink: &'a dyn Sink,
    }

    impl SinkResolver for StaticResolver<'_> {
        fn resolve(&self, kind: EventSinkKind) -> Option<&dyn Sink> {
            (kind == self.kind).then_some(self.sink)
        }
    }

    struct FailingSink;

    impl Sink for FailingSink {
        fn deliver<'a>(
            &'a self,
            _envelope: &'a EventEnvelope,
            _subscription: &'a EventSubscription,
            _sink: &'a EventSink,
        ) -> futures::future::BoxFuture<'a, Result<(), SinkError>> {
            async { Err(SinkError::new("boom")) }.boxed()
        }
    }

    #[test]
    fn delivery_worker_stops_after_empty_or_error_iteration() {
        assert!(!delivery_worker_should_continue(&Ok(
            EventDeliveryBatchOutcome {
                processed: 0,
                next_wakeup_in: None,
            }
        )));
        assert!(delivery_worker_should_continue(&Ok(
            EventDeliveryBatchOutcome {
                processed: 1,
                next_wakeup_in: None,
            }
        )));
        assert!(!delivery_worker_should_continue(&Err(
            ApiError::InternalServerError("boom".to_string())
        )));
    }

    #[rstest]
    #[case::no_retry(None, Duration::from_secs(5))]
    #[case::earlier_retry(Some(Duration::from_secs(1)), Duration::from_secs(1))]
    #[case::later_retry(Some(Duration::from_secs(10)), Duration::from_secs(5))]
    fn delivery_worker_waits_for_retry_or_safety_poll(
        #[case] next_wakeup_in: Option<Duration>,
        #[case] expected: Duration,
    ) {
        assert_eq!(
            event_delivery_wait_duration(Duration::from_secs(5), next_wakeup_in),
            expected
        );
    }

    #[actix_rt::test]
    async fn resolver_exports_unsupported_sink_kind() {
        let now = chrono::Utc::now().naive_utc();
        let envelope = EventEnvelope {
            id: 1,
            event_id: uuid::Uuid::new_v4(),
            occurred_at: now,
            entity_type: "collection".to_string(),
            entity_id: None,
            entity_name: None,
            collection_id: None,
            action: "created".to_string(),
            actor_user_id: None,
            actor_kind: "system".to_string(),
            provenance: hubuum_events_core::Provenance::default(),
            request_id: None,
            correlation_id: None,
            summary: "summary".to_string(),
            before: None,
            after: None,
            metadata: serde_json::json!({}),
            schema_version: 1,
        };
        let subscription = EventSubscription {
            id: 1,
            collection_id: 1,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["collection".to_string()],
            actions: vec!["created".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter::default(),
            routing: serde_json::json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
            revision: crate::models::ResourceRevision::INITIAL,
        };
        let sink = EventSink {
            id: 1,
            name: "sink".to_string(),
            kind: EventSinkKind::Webhook,
            config: serde_json::json!({}),
            secret_ref: None,
            enabled: true,
            created_at: now,
            updated_at: now,
            revision: crate::models::ResourceRevision::INITIAL,
        };

        let error = deliver_one(&NoopSinkResolver, &envelope, &subscription, &sink)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("webhook"));
    }

    #[actix_rt::test]
    async fn resolver_passes_through_transport_error() {
        let now = chrono::Utc::now().naive_utc();
        let envelope = EventEnvelope {
            id: 1,
            event_id: uuid::Uuid::new_v4(),
            occurred_at: now,
            entity_type: "collection".to_string(),
            entity_id: None,
            entity_name: None,
            collection_id: None,
            action: "created".to_string(),
            actor_user_id: None,
            actor_kind: "system".to_string(),
            provenance: hubuum_events_core::Provenance::default(),
            request_id: None,
            correlation_id: None,
            summary: "summary".to_string(),
            before: None,
            after: None,
            metadata: serde_json::json!({}),
            schema_version: 1,
        };
        let subscription = EventSubscription {
            id: 1,
            collection_id: 1,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["collection".to_string()],
            actions: vec!["created".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter::default(),
            routing: serde_json::json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
            revision: crate::models::ResourceRevision::INITIAL,
        };
        let sink = EventSink {
            id: 1,
            name: "sink".to_string(),
            kind: EventSinkKind::Webhook,
            config: serde_json::json!({}),
            secret_ref: None,
            enabled: true,
            created_at: now,
            updated_at: now,
            revision: crate::models::ResourceRevision::INITIAL,
        };
        let failing = FailingSink;
        let resolver = StaticResolver {
            kind: EventSinkKind::Webhook,
            sink: &failing,
        };

        let error = deliver_one(&resolver, &envelope, &subscription, &sink)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "boom");
    }
}
