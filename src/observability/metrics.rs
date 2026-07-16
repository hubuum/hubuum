mod cache;
mod db;
mod event;
mod export;
mod http;
mod import;
mod inventory;
mod login;
mod registry;
mod remote_call;
mod scrape;
mod security;
mod task;

use std::sync::{Mutex, OnceLock};

use opentelemetry::metrics::{Counter, Gauge, Histogram, UpDownCounter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::Registry;

use crate::errors::ApiError;

use self::cache::ScrapeCache;

pub use self::db::{
    ResultKind, db_connection_acquire_failed, db_connection_acquired, db_operation_finished,
};
pub use self::export::{
    export_completed, export_output_cleanup_deleted, export_output_cleanup_failed,
    export_output_cleanup_run, export_phase_duration, export_truncated, export_warnings,
};
pub use self::http::{api_error, extraction_failure, http_request_finished, http_request_started};
pub use self::import::{import_items, import_phase_duration};
#[cfg(feature = "login-rate-limit-valkey")]
pub use self::login::login_limiter_backend_failure;
pub use self::login::{login_attempt, login_lockout};
pub use self::registry::init;
pub use self::remote_call::remote_call_finished;
pub use self::scrape::scrape;
pub use self::security::client_allowlist_rejected;
pub use self::task::{
    task_claimed, task_completed, task_lease_recovered, task_worker_config, task_worker_iteration,
};

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub struct HttpInFlightGuard {
    active: bool,
}

impl HttpInFlightGuard {
    pub(super) fn new(active: bool) -> Self {
        Self { active }
    }
}

impl Drop for HttpInFlightGuard {
    fn drop(&mut self) {
        if self.active
            && let Some(metrics) = current()
        {
            metrics.http_in_flight.add(-1, &[]);
        }
    }
}

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
    task_lease_recoveries: Counter<u64>,
    task_completions: Counter<u64>,
    task_queue_wait_duration: Histogram<f64>,
    task_execution_duration: Histogram<f64>,
    task_config: Gauge<u64>,
    task_counts: Gauge<i64>,
    task_oldest_age: Gauge<f64>,
    export_output_cleanup_runs: Counter<u64>,
    export_output_cleanup_failures: Counter<u64>,
    export_output_cleanup_deleted: Counter<u64>,
    export_duration: Histogram<f64>,
    export_completions: Counter<u64>,
    export_truncations: Counter<u64>,
    export_warnings: Counter<u64>,
    import_duration: Histogram<f64>,
    import_processed_items: Counter<u64>,
    import_succeeded_items: Counter<u64>,
    import_failed_items: Counter<u64>,
    remote_call_duration: Histogram<f64>,
    remote_call_results: Counter<u64>,
    login_attempts: Counter<u64>,
    login_lockouts: Counter<u64>,
    #[cfg(feature = "login-rate-limit-valkey")]
    login_limiter_backend_failures: Counter<u64>,
    login_limiter_entries: Gauge<u64>,
    client_allowlist_rejections: Counter<u64>,
    event_queue_items: Gauge<i64>,
    event_stale_claims: Gauge<i64>,
    event_oldest_age: Gauge<f64>,
    event_worker_config: Gauge<u64>,
    event_worker_wakeups: Gauge<u64>,
    inventory_entities: Gauge<i64>,
    refresh_failures: Counter<u64>,
    scrape_cache: Mutex<ScrapeCache>,
    db_refresh_lock: tokio::sync::Mutex<()>,
}

fn current() -> Option<&'static Metrics> {
    METRICS.get()
}

fn get() -> Result<&'static Metrics, ApiError> {
    current().ok_or_else(|| ApiError::NotFound("Metrics are disabled".to_string()))
}

#[cfg(feature = "integration-test-support")]
pub(crate) fn clear_scrape_cache_for_tests() {
    if let Some(metrics) = current()
        && let Ok(mut cache) = metrics.scrape_cache.lock()
    {
        *cache = ScrapeCache::default();
    }
}
