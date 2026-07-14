use std::collections::HashMap;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;

use crate::db::DbPool;
use crate::db::traits::metrics::{MetricsBackend, TaskMetricsSnapshot};
use crate::models::{TaskKind, TaskStatus};

use super::{Metrics, current};

pub fn task_worker_iteration(outcome: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .task_worker_iterations
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

pub fn task_claimed(kind: &str, queue_wait: Option<Duration>) {
    if let Some(metrics) = current() {
        let attrs = [KeyValue::new("kind", kind.to_string())];
        metrics.task_claims.add(1, &attrs);
        if let Some(queue_wait) = queue_wait {
            metrics
                .task_queue_wait_duration
                .record(queue_wait.as_secs_f64(), &attrs);
        }
    }
}

pub fn task_lease_recovered(kind: &str) {
    if let Some(metrics) = current() {
        metrics
            .task_lease_recoveries
            .add(1, &[KeyValue::new("kind", kind.to_string())]);
    }
}

pub fn task_completed(kind: &str, final_status: &str, execution: Option<Duration>) {
    if let Some(metrics) = current() {
        let attrs = [
            KeyValue::new("kind", kind.to_string()),
            KeyValue::new("final_status", final_status.to_string()),
        ];
        metrics.task_completions.add(1, &attrs);
        if let Some(execution) = execution {
            metrics
                .task_execution_duration
                .record(execution.as_secs_f64(), &attrs);
        }
    }
}

pub fn task_worker_config(worker_count: usize, poll_interval: Duration) {
    if let Some(metrics) = current() {
        metrics.task_config.record(
            u64::try_from(worker_count).unwrap_or(u64::MAX),
            &[KeyValue::new("setting", "workers")],
        );
        metrics.task_config.record(
            u64::try_from(poll_interval.as_millis()).unwrap_or(u64::MAX),
            &[KeyValue::new("setting", "poll_interval_ms")],
        );
    }
}

pub(super) async fn refresh_task_gauges(metrics: &Metrics, pool: &DbPool) {
    if let Some(snapshot) = cached_task_snapshot(metrics) {
        record_task_snapshot(metrics, &snapshot);
        return;
    }

    match pool.metrics_task_snapshot().await {
        Ok(snapshot) => {
            record_task_snapshot(metrics, &snapshot);
            store_task_snapshot(metrics, snapshot);
        }
        Err(_) => {
            metrics
                .refresh_failures
                .add(1, &[KeyValue::new("source", "tasks")]);
            if let Some(snapshot) = stale_task_snapshot(metrics) {
                record_task_snapshot(metrics, &snapshot);
            } else {
                record_empty_task_snapshot(metrics);
            }
        }
    }
}

fn cached_task_snapshot(metrics: &Metrics) -> Option<TaskMetricsSnapshot> {
    let now = Instant::now();
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.tasks.fresh_value(now))
}

fn stale_task_snapshot(metrics: &Metrics) -> Option<TaskMetricsSnapshot> {
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.tasks.cached_value())
}

fn store_task_snapshot(metrics: &Metrics, snapshot: TaskMetricsSnapshot) {
    if let Ok(mut cache) = metrics.scrape_cache.lock() {
        cache.tasks.store(snapshot, Instant::now());
    }
}

fn record_task_snapshot(metrics: &Metrics, snapshot: &TaskMetricsSnapshot) {
    let now = chrono::Utc::now().naive_utc();
    let mut counts = HashMap::new();

    for row in &snapshot.counts {
        counts.insert((row.kind.as_str(), row.status.as_str()), row.count);
    }

    for kind in TaskKind::ALL {
        for status in TaskStatus::ALL {
            let kind = kind.as_str();
            let status = status.as_str();
            let count = counts.get(&(kind, status)).copied().unwrap_or(0);
            metrics.task_counts.record(
                count,
                &[KeyValue::new("kind", kind), KeyValue::new("status", status)],
            );
        }
    }

    metrics.task_oldest_age.record(
        age_seconds(snapshot.oldest_queued_at, now).unwrap_or(0.0),
        &[KeyValue::new("state", "queued")],
    );
    metrics.task_oldest_age.record(
        age_seconds(snapshot.oldest_active_at, now).unwrap_or(0.0),
        &[KeyValue::new("state", "active")],
    );
}

fn record_empty_task_snapshot(metrics: &Metrics) {
    for kind in TaskKind::ALL {
        for status in TaskStatus::ALL {
            metrics.task_counts.record(
                0,
                &[
                    KeyValue::new("kind", kind.as_str()),
                    KeyValue::new("status", status.as_str()),
                ],
            );
        }
    }

    metrics
        .task_oldest_age
        .record(0.0, &[KeyValue::new("state", "queued")]);
    metrics
        .task_oldest_age
        .record(0.0, &[KeyValue::new("state", "active")]);
}

fn age_seconds(
    timestamp: Option<chrono::NaiveDateTime>,
    now: chrono::NaiveDateTime,
) -> Option<f64> {
    timestamp.map(|timestamp| (now - timestamp).num_milliseconds().max(0) as f64 / 1000.0)
}
