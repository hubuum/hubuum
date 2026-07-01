use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Once, OnceLock};
use std::thread;
use std::time::Duration;

use actix_rt::time::sleep;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::config::{
    DEFAULT_EVENT_DELIVERY_BATCH_SIZE, DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS, DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS,
    DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS, DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS,
    DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS, DEFAULT_EVENT_DELIVERY_WORKERS, get_config,
};
use crate::db::DbPool;
use crate::db::traits::event_delivery::{
    ClaimedEventDelivery, EventDeliverySettings, claim_event_deliveries,
    mark_event_delivery_failed, mark_event_delivery_succeeded,
};
use crate::errors::ApiError;
use crate::events::sink::{DefaultSinkResolver, EventEnvelope, SinkResolver};
use crate::models::{EventSink, EventSubscription, EventWorkerWakeupStats};

static EVENT_DELIVERY_WORKER: Once = Once::new();
static EVENT_DELIVERY_NOTIFY: OnceLock<Notify> = OnceLock::new();
static EVENT_DELIVERY_NOTIFICATIONS_SENT: AtomicU64 = AtomicU64::new(0);
static EVENT_DELIVERY_NOTIFICATION_WAKEUPS: AtomicU64 = AtomicU64::new(0);
static EVENT_DELIVERY_POLL_WAKEUPS: AtomicU64 = AtomicU64::new(0);
static DEFAULT_SINK_RESOLVER: std::sync::LazyLock<DefaultSinkResolver> =
    std::sync::LazyLock::new(DefaultSinkResolver::default);

fn get_event_delivery_notify() -> &'static Notify {
    EVENT_DELIVERY_NOTIFY.get_or_init(Notify::new)
}

fn configured_event_delivery_worker_count() -> usize {
    get_config()
        .map(|config| config.event_delivery_workers)
        .unwrap_or(DEFAULT_EVENT_DELIVERY_WORKERS)
}

fn configured_event_delivery_poll_interval() -> Duration {
    let interval_ms = get_config()
        .map(|config| config.event_delivery_poll_interval_ms)
        .unwrap_or(DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS);
    Duration::from_millis(interval_ms)
}

fn configured_event_delivery_settings() -> EventDeliverySettings {
    get_config()
        .map(|config| EventDeliverySettings {
            batch_size: config.event_delivery_batch_size,
            lock_timeout_ms: config.event_delivery_lock_timeout_ms,
            transport_timeout_ms: config.event_delivery_transport_timeout_ms,
            retry_backoff_base_ms: config.event_delivery_retry_backoff_base_ms,
            retry_backoff_max_ms: config.event_delivery_retry_backoff_max_ms,
            max_attempts: config.event_delivery_max_attempts,
        })
        .unwrap_or(EventDeliverySettings {
            batch_size: DEFAULT_EVENT_DELIVERY_BATCH_SIZE,
            lock_timeout_ms: DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
            transport_timeout_ms: DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS,
            retry_backoff_base_ms: DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS,
            retry_backoff_max_ms: DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS,
            max_attempts: DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS,
        })
}

pub async fn process_event_delivery_batch(
    pool: &DbPool,
    settings: EventDeliverySettings,
    resolver: &dyn SinkResolver,
) -> Result<usize, ApiError> {
    let deliveries = claim_event_deliveries(pool, settings).await?;
    let mut processed = 0;

    for claimed in deliveries {
        process_claimed_event_delivery(pool, settings, resolver, claimed).await?;
        processed += 1;
    }

    Ok(processed)
}

pub(crate) async fn process_claimed_event_delivery(
    pool: &DbPool,
    settings: EventDeliverySettings,
    resolver: &dyn SinkResolver,
    claimed: ClaimedEventDelivery,
) -> Result<(), ApiError> {
    let delivery = claimed.delivery;
    let envelope = EventEnvelope::from(claimed.event);
    let subscription = EventSubscription::try_from(claimed.subscription)?;
    let sink = EventSink::try_from(claimed.sink)?;
    let result = tokio::time::timeout(
        Duration::from_millis(settings.transport_timeout_ms),
        deliver_one(resolver, &envelope, &subscription, &sink),
    )
    .await
    .map_err(|_| {
        crate::events::sink::SinkError::new(format!(
            "Event delivery transport timed out after {} ms",
            settings.transport_timeout_ms
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
) -> Result<(), crate::events::sink::SinkError> {
    let Some(transport) = resolver.resolve(sink.kind) else {
        return Err(crate::events::sink::SinkError::new(format!(
            "No event sink transport is registered for kind '{}'",
            sink.kind.as_str()
        )));
    };

    transport.deliver(envelope, subscription, sink).await
}

fn delivery_worker_should_continue(result: &Result<usize, ApiError>) -> bool {
    match result {
        Ok(processed) => *processed > 0,
        Err(error) => {
            error!(message = "Event delivery worker iteration failed", error = %error);
            false
        }
    }
}

async fn wait_for_event_delivery_wakeup(poll_interval: Duration) {
    tokio::select! {
        _ = sleep(poll_interval) => {
            EVENT_DELIVERY_POLL_WAKEUPS.fetch_add(1, Ordering::Relaxed);
        }
        _ = get_event_delivery_notify().notified() => {
            EVENT_DELIVERY_NOTIFICATION_WAKEUPS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn event_delivery_worker_loop(
    pool: DbPool,
    settings: EventDeliverySettings,
    poll_interval: Duration,
    resolver: &'static dyn SinkResolver,
) {
    loop {
        let result = process_event_delivery_batch(&pool, settings, resolver).await;
        if delivery_worker_should_continue(&result) {
            continue;
        }
        wait_for_event_delivery_wakeup(poll_interval).await;
    }
}

fn spawn_event_delivery_worker_loop(
    pool: DbPool,
    settings: EventDeliverySettings,
    poll_interval: Duration,
    worker_index: usize,
    resolver: &'static dyn SinkResolver,
) {
    thread::Builder::new()
        .name(format!("event-delivery-worker-{worker_index}"))
        .spawn(move || {
            info!(
                message = "Starting event delivery worker loop",
                worker_index = worker_index,
                batch_size = settings.batch_size,
                lock_timeout_ms = settings.lock_timeout_ms,
                retry_backoff_base_ms = settings.retry_backoff_base_ms,
                retry_backoff_max_ms = settings.retry_backoff_max_ms,
                max_attempts = settings.max_attempts,
                poll_interval = ?poll_interval
            );
            let system = actix_rt::System::new();
            system.block_on(event_delivery_worker_loop(
                pool,
                settings,
                poll_interval,
                resolver,
            ));
        })
        .expect("failed to spawn event delivery worker thread");
}

pub fn ensure_event_delivery_worker_running(pool: DbPool) {
    let worker_count = configured_event_delivery_worker_count();
    if worker_count == 0 {
        return;
    }

    let poll_interval = configured_event_delivery_poll_interval();
    let settings = configured_event_delivery_settings();

    EVENT_DELIVERY_WORKER.call_once(move || {
        info!(
            message = "Initializing event delivery workers",
            worker_count = worker_count,
            batch_size = settings.batch_size,
            lock_timeout_ms = settings.lock_timeout_ms,
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

pub fn kick_event_delivery_worker(pool: DbPool) {
    ensure_event_delivery_worker_running(pool);
    EVENT_DELIVERY_NOTIFICATIONS_SENT.fetch_add(1, Ordering::Relaxed);
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
        assert!(!delivery_worker_should_continue(&Ok(0)));
        assert!(delivery_worker_should_continue(&Ok(1)));
        assert!(!delivery_worker_should_continue(&Err(
            ApiError::InternalServerError("boom".to_string())
        )));
    }

    #[actix_rt::test]
    async fn resolver_reports_unsupported_sink_kind() {
        let now = chrono::Utc::now().naive_utc();
        let envelope = EventEnvelope {
            id: 1,
            event_id: uuid::Uuid::new_v4(),
            occurred_at: now,
            entity_type: "namespace".to_string(),
            entity_id: None,
            entity_name: None,
            namespace_id: None,
            action: "created".to_string(),
            actor_user_id: None,
            actor_kind: "system".to_string(),
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
            namespace_id: 1,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["namespace".to_string()],
            actions: vec!["created".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter::default(),
            routing: serde_json::json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
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
            entity_type: "namespace".to_string(),
            entity_id: None,
            entity_name: None,
            namespace_id: None,
            action: "created".to_string(),
            actor_user_id: None,
            actor_kind: "system".to_string(),
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
            namespace_id: 1,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["namespace".to_string()],
            actions: vec!["created".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter::default(),
            routing: serde_json::json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
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
