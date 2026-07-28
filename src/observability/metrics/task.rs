use std::collections::HashMap;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;

use crate::db::DbPool;
use crate::db::traits::metrics::{MetricsRefreshBackend, TaskGaugeSnapshot};
use crate::models::{TaskKind, TaskStatus};

use super::scrape::{RefreshOutcome, RefreshSource, record_refresh_attempt};
use super::{Metrics, current};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskOutputKind {
    Export,
    Backup,
}

impl TaskOutputKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Export => "export",
            Self::Backup => "backup",
        }
    }
}

#[derive(Clone, Copy)]
enum TaskAgeState {
    Queued,
    Active,
}

impl TaskAgeState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Active => "active",
        }
    }
}

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
        metrics
            .task_workers_configured
            .record(u64::try_from(worker_count).unwrap_or(u64::MAX), &[]);
        metrics
            .task_poll_interval
            .record(poll_interval.as_secs_f64(), &[]);
    }
}

pub fn task_output_cleanup_run(kind: TaskOutputKind) {
    if let Some(metrics) = current() {
        metrics
            .task_output_cleanup_runs
            .add(1, &[KeyValue::new("kind", kind.as_str())]);
    }
}

pub fn task_output_cleanup_failed(kind: TaskOutputKind) {
    if let Some(metrics) = current() {
        metrics
            .task_output_cleanup_failures
            .add(1, &[KeyValue::new("kind", kind.as_str())]);
    }
}

pub fn task_output_cleanup_deleted(kind: TaskOutputKind, count: usize) {
    if let Some(metrics) = current() {
        metrics.task_output_cleanup_deleted.add(
            u64::try_from(count).unwrap_or(u64::MAX),
            &[KeyValue::new("kind", kind.as_str())],
        );
    }
}

pub(super) async fn refresh_task_gauges(metrics: &Metrics, pool: &DbPool) {
    if let Some(snapshot) = cached_task_snapshot(metrics) {
        record_task_snapshot(metrics, &snapshot);
        return;
    }

    let refresh_started_at = Instant::now();
    match pool.metrics_task_gauge_snapshot().await {
        Ok(snapshot) => {
            record_refresh_attempt(
                metrics,
                RefreshSource::Tasks,
                refresh_started_at,
                RefreshOutcome::Succeeded,
            );
            record_task_snapshot(metrics, &snapshot);
            store_task_snapshot(metrics, snapshot);
        }
        Err(_) => {
            record_refresh_attempt(
                metrics,
                RefreshSource::Tasks,
                refresh_started_at,
                RefreshOutcome::Failed,
            );
            if let Some(snapshot) = stale_task_snapshot(metrics) {
                record_task_snapshot(metrics, &snapshot);
            } else {
                record_empty_task_snapshot(metrics);
            }
        }
    }
}

fn cached_task_snapshot(metrics: &Metrics) -> Option<TaskGaugeSnapshot> {
    let now = Instant::now();
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.tasks.fresh_value(now))
}

fn stale_task_snapshot(metrics: &Metrics) -> Option<TaskGaugeSnapshot> {
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.tasks.cached_value())
}

fn store_task_snapshot(metrics: &Metrics, snapshot: TaskGaugeSnapshot) {
    if let Ok(mut cache) = metrics.scrape_cache.lock() {
        cache.tasks.store(snapshot, Instant::now());
    }
}

fn record_task_snapshot(metrics: &Metrics, snapshot: &TaskGaugeSnapshot) {
    let now = chrono::Utc::now().naive_utc();
    let mut counts = HashMap::new();
    let mut last_finished = HashMap::new();

    for row in &snapshot.counts {
        counts.insert((row.kind, row.status), row.count);
    }
    for row in &snapshot.last_finished {
        last_finished.insert((row.kind, row.status), row.timestamp);
    }

    for kind in TaskKind::ALL {
        for status in TaskStatus::ALL {
            let count = counts.get(&(kind, status)).copied().unwrap_or(0);
            record_task_count(metrics, kind, status, count);
        }
        for status in TaskStatus::TERMINAL {
            record_last_terminal_timestamp(
                metrics,
                kind,
                status,
                last_finished
                    .get(&(kind, status))
                    .copied()
                    .flatten()
                    .map(timestamp_seconds)
                    .unwrap_or(0.0),
            );
        }
    }

    for age in &snapshot.ages {
        record_task_age(
            metrics,
            age.kind,
            TaskAgeState::Queued,
            age_seconds(age.oldest_queued_at, now).unwrap_or(0.0),
        );
        record_task_age(
            metrics,
            age.kind,
            TaskAgeState::Active,
            age_seconds(age.oldest_active_at, now).unwrap_or(0.0),
        );
    }
}

fn record_empty_task_snapshot(metrics: &Metrics) {
    for kind in TaskKind::ALL {
        for status in TaskStatus::ALL {
            record_task_count(metrics, kind, status, 0);
        }
        for status in TaskStatus::TERMINAL {
            record_last_terminal_timestamp(metrics, kind, status, 0.0);
        }
        record_task_age(metrics, kind, TaskAgeState::Queued, 0.0);
        record_task_age(metrics, kind, TaskAgeState::Active, 0.0);
    }
}

fn record_task_count(metrics: &Metrics, kind: TaskKind, status: TaskStatus, count: i64) {
    metrics.task_counts.record(
        count,
        &[
            KeyValue::new("kind", kind.as_str()),
            KeyValue::new("status", status.as_str()),
        ],
    );
}

fn record_task_age(metrics: &Metrics, kind: TaskKind, state: TaskAgeState, age: f64) {
    metrics.task_oldest_age.record(
        age,
        &[
            KeyValue::new("kind", kind.as_str()),
            KeyValue::new("state", state.as_str()),
        ],
    );
}

fn record_last_terminal_timestamp(
    metrics: &Metrics,
    kind: TaskKind,
    status: TaskStatus,
    timestamp: f64,
) {
    metrics.task_last_terminal_timestamp.record(
        timestamp,
        &[
            KeyValue::new("kind", kind.as_str()),
            KeyValue::new("status", status.as_str()),
        ],
    );
}

fn timestamp_seconds(timestamp: chrono::NaiveDateTime) -> f64 {
    timestamp.and_utc().timestamp_millis() as f64 / 1000.0
}

fn age_seconds(
    timestamp: Option<chrono::NaiveDateTime>,
    now: chrono::NaiveDateTime,
) -> Option<f64> {
    timestamp.map(|timestamp| (now - timestamp).num_milliseconds().max(0) as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::{TaskAgeState, TaskOutputKind, timestamp_seconds};

    #[test]
    fn task_output_kinds_have_stable_bounded_labels() {
        assert_eq!(
            [TaskOutputKind::Export, TaskOutputKind::Backup].map(TaskOutputKind::as_str),
            ["export", "backup"]
        );
    }

    #[test]
    fn task_age_states_have_stable_bounded_labels() {
        assert_eq!(
            [TaskAgeState::Queued, TaskAgeState::Active].map(TaskAgeState::as_str),
            ["queued", "active"]
        );
    }

    #[test]
    fn terminal_timestamp_preserves_millisecond_precision_in_seconds() {
        let timestamp = NaiveDate::from_ymd_opt(2026, 7, 28)
            .unwrap()
            .and_hms_milli_opt(12, 34, 56, 789)
            .unwrap();

        assert!((timestamp_seconds(timestamp) - 1_785_242_096.789).abs() < f64::EPSILON);
    }
}
