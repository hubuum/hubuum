use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use actix_web::{HttpResponse, Responder, http::header, web};
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider as _, UpDownCounter};
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, Registry, TextEncoder};

use crate::db::DbPool;
use crate::db::traits::metrics::MetricsBackend;
use crate::db::traits::metrics::{InventoryMetricsSnapshot, TaskMetricsSnapshot};
use crate::errors::ApiError;
use crate::middlewares::rate_limit;
use crate::models::{TaskKind, TaskStatus};

static METRICS: OnceLock<Metrics> = OnceLock::new();
const DB_SCRAPE_CACHE_TTL: Duration = Duration::from_secs(30);

struct Metrics {
    registry: Registry,
    _provider: SdkMeterProvider,
    http_requests: Counter<u64>,
    http_request_duration: Histogram<f64>,
    http_in_flight: UpDownCounter<i64>,
    api_errors: Counter<u64>,
    extraction_failures: Counter<u64>,
    db_pool_connections: Gauge<u64>,
    db_connection_acquire_duration: Histogram<f64>,
    db_connection_acquire_failures: Counter<u64>,
    db_operation_duration: Histogram<f64>,
    db_operation_errors: Counter<u64>,
    task_worker_iterations: Counter<u64>,
    task_claims: Counter<u64>,
    task_completions: Counter<u64>,
    task_queue_wait_duration: Histogram<f64>,
    task_execution_duration: Histogram<f64>,
    task_config: Gauge<u64>,
    task_counts: Gauge<i64>,
    task_oldest_age: Gauge<f64>,
    report_output_cleanup_runs: Counter<u64>,
    report_output_cleanup_failures: Counter<u64>,
    report_output_cleanup_deleted: Counter<u64>,
    report_duration: Histogram<f64>,
    report_results: Counter<u64>,
    remote_call_duration: Histogram<f64>,
    remote_call_results: Counter<u64>,
    login_attempts: Counter<u64>,
    login_limiter_entries: Gauge<u64>,
    inventory_entities: Gauge<i64>,
    refresh_failures: Counter<u64>,
    scrape_cache: Mutex<ScrapeCache>,
}

#[derive(Default)]
struct ScrapeCache {
    inventory: CachedSnapshot<InventoryMetricsSnapshot>,
    tasks: CachedSnapshot<TaskMetricsSnapshot>,
}

struct CachedSnapshot<T> {
    value: Option<T>,
    refreshed_at: Option<Instant>,
}

impl<T> Default for CachedSnapshot<T> {
    fn default() -> Self {
        Self {
            value: None,
            refreshed_at: None,
        }
    }
}

impl<T: Clone> CachedSnapshot<T> {
    fn fresh_value(&self, now: Instant) -> Option<T> {
        match (self.value.as_ref(), self.refreshed_at) {
            (Some(value), Some(refreshed_at))
                if now.duration_since(refreshed_at) < DB_SCRAPE_CACHE_TTL =>
            {
                Some(value.clone())
            }
            _ => None,
        }
    }

    fn cached_value(&self) -> Option<T> {
        self.value.clone()
    }

    fn store(&mut self, value: T, now: Instant) {
        self.value = Some(value);
        self.refreshed_at = Some(now);
    }
}

pub fn init() -> Result<(), ApiError> {
    if METRICS.get().is_some() {
        return Ok(());
    }

    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_target_info()
        .build()
        .map_err(|error| {
            ApiError::InternalServerError(format!("Failed to initialize metrics exporter: {error}"))
        })?;
    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    let meter = provider.meter("hubuum");

    let metrics = Metrics {
        registry,
        _provider: provider,
        http_requests: meter
            .u64_counter("hubuum_http_requests")
            .with_description("HTTP requests handled")
            .build(),
        http_request_duration: meter
            .f64_histogram("hubuum_http_request_duration")
            .with_description("HTTP request duration")
            .with_unit("s")
            .build(),
        http_in_flight: meter
            .i64_up_down_counter("hubuum_http_requests_in_flight")
            .with_description("HTTP requests currently in flight")
            .build(),
        api_errors: meter
            .u64_counter("hubuum_api_errors")
            .with_description("API errors by public error class")
            .build(),
        extraction_failures: meter
            .u64_counter("hubuum_extraction_failures")
            .with_description("Request extraction failures")
            .build(),
        db_pool_connections: meter
            .u64_gauge("hubuum_db_pool_connections")
            .with_description("Database pool connections by state")
            .build(),
        db_connection_acquire_duration: meter
            .f64_histogram("hubuum_db_connection_acquire_duration")
            .with_description("Database connection acquisition duration")
            .with_unit("s")
            .build(),
        db_connection_acquire_failures: meter
            .u64_counter("hubuum_db_connection_acquire_failures")
            .with_description("Database connection acquisition failures")
            .build(),
        db_operation_duration: meter
            .f64_histogram("hubuum_db_operation_duration")
            .with_description("Database helper operation duration")
            .with_unit("s")
            .build(),
        db_operation_errors: meter
            .u64_counter("hubuum_db_operation_errors")
            .with_description("Database helper operation failures")
            .build(),
        task_worker_iterations: meter
            .u64_counter("hubuum_task_worker_iterations")
            .with_description("Task worker loop iterations")
            .build(),
        task_claims: meter
            .u64_counter("hubuum_task_claims")
            .with_description("Tasks claimed by workers")
            .build(),
        task_completions: meter
            .u64_counter("hubuum_task_completions")
            .with_description("Tasks completed by terminal status")
            .build(),
        task_queue_wait_duration: meter
            .f64_histogram("hubuum_task_queue_wait_duration")
            .with_description("Task queue wait duration")
            .with_unit("s")
            .build(),
        task_execution_duration: meter
            .f64_histogram("hubuum_task_execution_duration")
            .with_description("Task execution duration")
            .with_unit("s")
            .build(),
        task_config: meter
            .u64_gauge("hubuum_task_worker_config")
            .with_description("Configured task worker settings")
            .build(),
        task_counts: meter
            .i64_gauge("hubuum_tasks")
            .with_description("Tasks by kind and status")
            .build(),
        task_oldest_age: meter
            .f64_gauge("hubuum_task_oldest_age")
            .with_description("Oldest queued or active task age")
            .with_unit("s")
            .build(),
        report_output_cleanup_runs: meter
            .u64_counter("hubuum_report_output_cleanup_runs")
            .with_description("Report output cleanup runs")
            .build(),
        report_output_cleanup_failures: meter
            .u64_counter("hubuum_report_output_cleanup_failures")
            .with_description("Report output cleanup failures")
            .build(),
        report_output_cleanup_deleted: meter
            .u64_counter("hubuum_report_output_cleanup_deleted")
            .with_description("Report outputs deleted by cleanup")
            .build(),
        report_duration: meter
            .f64_histogram("hubuum_report_phase_duration")
            .with_description("Report phase duration")
            .with_unit("s")
            .build(),
        report_results: meter
            .u64_counter("hubuum_report_results")
            .with_description("Report result counters")
            .build(),
        remote_call_duration: meter
            .f64_histogram("hubuum_remote_call_duration")
            .with_description("Remote call duration")
            .with_unit("s")
            .build(),
        remote_call_results: meter
            .u64_counter("hubuum_remote_call_results")
            .with_description("Remote call outcomes")
            .build(),
        login_attempts: meter
            .u64_counter("hubuum_login_attempts")
            .with_description("Login attempts by outcome")
            .build(),
        login_limiter_entries: meter
            .u64_gauge("hubuum_login_limiter_entries")
            .with_description("Login limiter entries")
            .build(),
        inventory_entities: meter
            .i64_gauge("hubuum_inventory_entities")
            .with_description("Low-cardinality domain inventory counts")
            .build(),
        refresh_failures: meter
            .u64_counter("hubuum_metrics_refresh_failures")
            .with_description("Metrics scrape refresh failures by source")
            .build(),
        scrape_cache: Mutex::new(ScrapeCache::default()),
    };

    METRICS
        .set(metrics)
        .map_err(|_| ApiError::InternalServerError("Metrics already initialized".to_string()))
}

pub async fn scrape(pool: web::Data<DbPool>) -> Result<impl Responder, ApiError> {
    let metrics = get()?;
    refresh_scrape_gauges(metrics, &pool).await;

    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut body = Vec::new();
    encoder
        .encode(&metric_families, &mut body)
        .map_err(|error| {
            ApiError::InternalServerError(format!("Failed to encode metrics: {error}"))
        })?;

    Ok(HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, encoder.format_type()))
        .body(body))
}

pub struct HttpInFlightGuard {
    active: bool,
}

impl Drop for HttpInFlightGuard {
    fn drop(&mut self) {
        if self.active
            && let Some(metrics) = METRICS.get()
        {
            metrics.http_in_flight.add(-1, &[]);
        }
    }
}

pub fn http_request_started() -> HttpInFlightGuard {
    if let Some(metrics) = METRICS.get() {
        metrics.http_in_flight.add(1, &[]);
    }
    HttpInFlightGuard {
        active: METRICS.get().is_some(),
    }
}

pub fn http_request_finished(method: &str, route: &str, status_code: u16, duration: Duration) {
    if let Some(metrics) = METRICS.get() {
        let status_family = status_family(status_code);
        let method = Value::from(method.to_owned());
        let route = Value::from(route.to_owned());
        let count_attrs = [
            KeyValue::new("method", method.clone()),
            KeyValue::new("route", route.clone()),
            KeyValue::new("status_code", i64::from(status_code)),
            KeyValue::new("status_family", status_family),
        ];
        let duration_attrs = [
            KeyValue::new("method", method),
            KeyValue::new("route", route),
            KeyValue::new("status_family", status_family),
        ];
        metrics.http_requests.add(1, &count_attrs);
        metrics
            .http_request_duration
            .record(duration.as_secs_f64(), &duration_attrs);
    }
}

pub fn api_error(error_class: &'static str) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .api_errors
            .add(1, &[KeyValue::new("class", error_class)]);
    }
}

pub fn extraction_failure(kind: &'static str) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .extraction_failures
            .add(1, &[KeyValue::new("kind", kind)]);
    }
}

pub fn db_connection_acquired(duration: Duration) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .db_connection_acquire_duration
            .record(duration.as_secs_f64(), &[]);
    }
}

pub fn db_connection_acquire_failed(duration: Duration) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .db_connection_acquire_duration
            .record(duration.as_secs_f64(), &[]);
        metrics.db_connection_acquire_failures.add(1, &[]);
    }
}

pub fn db_operation_finished(operation: &'static str, duration: Duration, result: &ResultKind) {
    if let Some(metrics) = METRICS.get() {
        let attrs = [
            KeyValue::new("operation", operation),
            KeyValue::new("result", result.as_str()),
        ];
        metrics
            .db_operation_duration
            .record(duration.as_secs_f64(), &attrs);
        if matches!(result, ResultKind::Error(_)) {
            metrics.db_operation_errors.add(1, &attrs);
        }
    }
}

pub fn task_worker_iteration(outcome: &'static str) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .task_worker_iterations
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

pub fn task_claimed(kind: &str, queue_wait: Option<Duration>) {
    if let Some(metrics) = METRICS.get() {
        let attrs = [KeyValue::new("kind", kind.to_string())];
        metrics.task_claims.add(1, &attrs);
        if let Some(queue_wait) = queue_wait {
            metrics
                .task_queue_wait_duration
                .record(queue_wait.as_secs_f64(), &attrs);
        }
    }
}

pub fn task_completed(kind: &str, final_status: &str, execution: Option<Duration>) {
    if let Some(metrics) = METRICS.get() {
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
    if let Some(metrics) = METRICS.get() {
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

pub fn report_output_cleanup_run() {
    if let Some(metrics) = METRICS.get() {
        metrics.report_output_cleanup_runs.add(1, &[]);
    }
}

pub fn report_output_cleanup_failed() {
    if let Some(metrics) = METRICS.get() {
        metrics.report_output_cleanup_failures.add(1, &[]);
    }
}

pub fn report_output_cleanup_deleted(count: usize) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .report_output_cleanup_deleted
            .add(u64::try_from(count).unwrap_or(u64::MAX), &[]);
    }
}

pub fn report_phase_duration(phase: &'static str, duration: Duration) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .report_duration
            .record(duration.as_secs_f64(), &[KeyValue::new("phase", phase)]);
    }
}

pub fn report_result(scope: &'static str, content_type: &'static str, outcome: &'static str) {
    if let Some(metrics) = METRICS.get() {
        metrics.report_results.add(
            1,
            &[
                KeyValue::new("scope", scope),
                KeyValue::new("content_type", content_type),
                KeyValue::new("outcome", outcome),
            ],
        );
    }
}

pub fn remote_call_finished(
    method: &str,
    status_family: &'static str,
    outcome: &'static str,
    duration: Duration,
) {
    if let Some(metrics) = METRICS.get() {
        let attrs = [
            KeyValue::new("method", method.to_string()),
            KeyValue::new("status_family", status_family),
            KeyValue::new("outcome", outcome),
        ];
        metrics
            .remote_call_duration
            .record(duration.as_secs_f64(), &attrs);
        metrics.remote_call_results.add(1, &attrs);
    }
}

pub fn login_attempt(outcome: &'static str) {
    if let Some(metrics) = METRICS.get() {
        metrics
            .login_attempts
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

pub enum ResultKind {
    Ok,
    Error(&'static str),
}

impl ResultKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error(class) => class,
        }
    }
}

fn get() -> Result<&'static Metrics, ApiError> {
    METRICS
        .get()
        .ok_or_else(|| ApiError::NotFound("Metrics are disabled".to_string()))
}

#[cfg(test)]
pub(crate) fn clear_scrape_cache_for_tests() {
    if let Some(metrics) = METRICS.get()
        && let Ok(mut cache) = metrics.scrape_cache.lock()
    {
        *cache = ScrapeCache::default();
    }
}

async fn refresh_scrape_gauges(metrics: &Metrics, pool: &DbPool) {
    refresh_pool_gauges(metrics, pool);
    refresh_login_limiter_gauges(metrics).await;
    refresh_inventory_gauges(metrics, pool).await;
    refresh_task_gauges(metrics, pool).await;
}

fn refresh_pool_gauges(metrics: &Metrics, pool: &DbPool) {
    let state = pool.state();
    metrics.db_pool_connections.record(
        u64::from(pool.max_size()),
        &[KeyValue::new("state", "configured")],
    );
    metrics.db_pool_connections.record(
        u64::from(state.connections),
        &[KeyValue::new("state", "open")],
    );
    metrics.db_pool_connections.record(
        u64::from(state.idle_connections),
        &[KeyValue::new("state", "idle")],
    );
    let checked_out = state.connections.saturating_sub(state.idle_connections);
    metrics.db_pool_connections.record(
        u64::from(checked_out),
        &[KeyValue::new("state", "checked_out")],
    );
}

async fn refresh_login_limiter_gauges(metrics: &Metrics) {
    let snapshots = rate_limit::snapshot().await;
    let locked = snapshots.iter().filter(|entry| entry.locked).count();
    metrics.login_limiter_entries.record(
        u64::try_from(snapshots.len()).unwrap_or(u64::MAX),
        &[KeyValue::new("state", "active")],
    );
    metrics.login_limiter_entries.record(
        u64::try_from(locked).unwrap_or(u64::MAX),
        &[KeyValue::new("state", "locked")],
    );
}

async fn refresh_inventory_gauges(metrics: &Metrics, pool: &DbPool) {
    if let Some(row) = cached_inventory_snapshot(metrics) {
        record_inventory_snapshot(metrics, &row);
        return;
    }

    match pool.metrics_inventory_snapshot().await {
        Ok(row) => {
            store_inventory_snapshot(metrics, row);
            record_inventory_snapshot(metrics, &row);
        }
        Err(_) => {
            metrics
                .refresh_failures
                .add(1, &[KeyValue::new("source", "inventory")]);
            if let Some(row) = stale_inventory_snapshot(metrics) {
                record_inventory_snapshot(metrics, &row);
            }
        }
    }
}

fn cached_inventory_snapshot(metrics: &Metrics) -> Option<InventoryMetricsSnapshot> {
    let now = Instant::now();
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.inventory.fresh_value(now))
}

fn stale_inventory_snapshot(metrics: &Metrics) -> Option<InventoryMetricsSnapshot> {
    metrics
        .scrape_cache
        .lock()
        .ok()
        .and_then(|cache| cache.inventory.cached_value())
}

fn store_inventory_snapshot(metrics: &Metrics, snapshot: InventoryMetricsSnapshot) {
    if let Ok(mut cache) = metrics.scrape_cache.lock() {
        cache.inventory.store(snapshot, Instant::now());
    }
}

fn record_inventory_snapshot(metrics: &Metrics, row: &InventoryMetricsSnapshot) {
    record_inventory(metrics, "namespaces", row.namespaces);
    record_inventory(metrics, "classes", row.classes);
    record_inventory(metrics, "objects", row.objects);
    record_inventory(metrics, "users", row.users);
    record_inventory(metrics, "groups", row.groups);
    record_inventory(metrics, "service_accounts", row.service_accounts);
    record_inventory(metrics, "remote_targets", row.remote_targets);
}

fn record_inventory(metrics: &Metrics, entity_type: &'static str, count: i64) {
    metrics
        .inventory_entities
        .record(count, &[KeyValue::new("entity_type", entity_type)]);
}

async fn refresh_task_gauges(metrics: &Metrics, pool: &DbPool) {
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

fn status_family(status_code: u16) -> &'static str {
    match status_code {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}
