use std::time::{Duration, Instant};

use opentelemetry::KeyValue;

use crate::db::DbPool;
use crate::db::traits::event_observability::load_event_metrics_snapshot;
use crate::db::traits::metrics::EventMetricsSnapshot;
use crate::models::{EventDeliveryStatusCounts, EventWorkerHealth};

use super::scrape::{RefreshOutcome, RefreshSource, record_refresh_attempt};
use super::{Metrics, current};

pub fn event_worker_wakeup(worker: &'static str, kind: &'static str) {
    if let Some(metrics) = current() {
        metrics.event_worker_wakeups.add(
            1,
            &[KeyValue::new("worker", worker), KeyValue::new("kind", kind)],
        );
    }
}

pub(super) async fn refresh_event_gauges(metrics: &Metrics, pool: &DbPool) {
    if let Some(snapshot) = cached_event_snapshot(metrics) {
        record_event_snapshot(metrics, &snapshot);
        return;
    }

    let refresh_started_at = Instant::now();
    match load_event_metrics_snapshot(pool).await {
        Ok(snapshot) => {
            record_refresh_attempt(
                metrics,
                RefreshSource::Events,
                refresh_started_at,
                RefreshOutcome::Succeeded,
            );
            record_event_snapshot(metrics, &snapshot);
            store_event_snapshot(metrics, snapshot);
        }
        Err(_) => {
            record_refresh_attempt(
                metrics,
                RefreshSource::Events,
                refresh_started_at,
                RefreshOutcome::Failed,
            );
            if let Some(snapshot) = stale_event_snapshot(metrics) {
                record_event_snapshot(metrics, &snapshot);
            }
        }
    }
}

fn cached_event_snapshot(metrics: &Metrics) -> Option<EventMetricsSnapshot> {
    let now = Instant::now();
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.events.fresh_value(now))
}

fn stale_event_snapshot(metrics: &Metrics) -> Option<EventMetricsSnapshot> {
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.events.cached_value())
}

fn store_event_snapshot(metrics: &Metrics, snapshot: EventMetricsSnapshot) {
    if let Ok(mut cache) = metrics.scrape_cache.lock() {
        cache.events.store(snapshot, Instant::now());
    }
}

fn record_event_snapshot(metrics: &Metrics, snapshot: &EventMetricsSnapshot) {
    record_queue_item(metrics, "fanout", "pending", snapshot.fanout.pending_events);
    record_queue_item(
        metrics,
        "fanout",
        "in_flight",
        snapshot.fanout.in_flight_events,
    );
    record_delivery_counts(metrics, &snapshot.delivery.counts);
    record_stale_claims(metrics, "fanout", snapshot.fanout.stale_claims);
    record_stale_claims(metrics, "delivery", snapshot.delivery.stale_claims);
    record_oldest_age(
        metrics,
        "fanout",
        snapshot.fanout.oldest_pending_age_seconds,
    );
    record_oldest_age(
        metrics,
        "delivery",
        snapshot.delivery.oldest_due_age_seconds,
    );
    record_worker(metrics, "fanout", &snapshot.fanout.worker);
    record_worker(metrics, "delivery", &snapshot.delivery.worker);
}

fn record_delivery_counts(metrics: &Metrics, counts: &EventDeliveryStatusCounts) {
    for (state, value) in [
        ("total", counts.total),
        ("pending", counts.pending),
        ("in_flight", counts.in_flight),
        ("succeeded", counts.succeeded),
        ("failed", counts.failed),
        ("dead", counts.dead),
        ("retryable", counts.retryable),
    ] {
        record_queue_item(metrics, "delivery", state, value);
    }
}

fn record_queue_item(metrics: &Metrics, queue: &'static str, state: &'static str, value: i64) {
    metrics.event_queue_items.record(
        value,
        &[KeyValue::new("queue", queue), KeyValue::new("state", state)],
    );
}

fn record_stale_claims(metrics: &Metrics, queue: &'static str, value: i64) {
    metrics
        .event_stale_claims
        .record(value, &[KeyValue::new("queue", queue)]);
}

fn record_oldest_age(metrics: &Metrics, queue: &'static str, age: Option<i64>) {
    metrics.event_oldest_age.record(
        age.unwrap_or(0).max(0) as f64,
        &[KeyValue::new("queue", queue)],
    );
}

fn record_worker(metrics: &Metrics, worker: &'static str, health: &EventWorkerHealth) {
    let attrs = [KeyValue::new("worker", worker)];
    metrics.event_workers_configured.record(
        u64::try_from(health.workers_configured).unwrap_or(u64::MAX),
        &attrs,
    );
    metrics
        .event_worker_batch_size
        .record(u64::try_from(health.batch_size).unwrap_or(u64::MAX), &attrs);
    metrics.event_worker_poll_interval.record(
        Duration::from_millis(health.poll_interval_ms).as_secs_f64(),
        &attrs,
    );
    metrics.event_worker_lock_timeout.record(
        Duration::from_millis(health.lock_timeout_ms).as_secs_f64(),
        &attrs,
    );
}
