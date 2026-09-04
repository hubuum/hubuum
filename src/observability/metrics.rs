mod cache;
mod computed_field;
mod db;
mod event;
mod export;
mod http;
mod import;
mod inventory;
mod login;
mod process;
mod registry;
mod remote_call;
mod scrape;
mod secret;
mod security;
mod storage;
mod task;
mod timer;
mod token;
mod tracing;

use std::sync::{Mutex, OnceLock};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, UpDownCounter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{IntGaugeVec, Registry};

use crate::errors::ApiError;
use crate::operational_contracts::{MetricDefinition, metric_label_value_is_allowed};

use self::cache::ScrapeCache;
use self::process::ProcessMetrics;

pub(crate) use self::computed_field::computed_evaluation_summary;
pub use self::computed_field::{
    computed_evaluation, computed_live_fallback, computed_read_repair, computed_rebuild_batch,
    computed_rebuild_finished,
};
pub(crate) use self::db::{
    ResultKind, db_connection_acquire_failed, db_connection_acquired, db_operation_finished,
};
pub use self::event::event_worker_wakeup;
pub use self::export::{
    ExportMetricOutcome, ExportMetricPhase, export_completed, export_output_cleanup_deleted,
    export_output_cleanup_failed, export_output_cleanup_run, export_phase_duration,
    export_phase_timer, export_truncated, export_warnings,
};
pub use self::http::{
    api_error, extraction_failure, http_request_finished, http_request_started,
    http_request_started_for_route,
};
pub use self::import::{
    ImportMetricOutcome, ImportMetricPhase, import_items, import_phase_duration, import_phase_timer,
};
#[cfg(feature = "login-rate-limit-valkey")]
pub use self::login::login_limiter_backend_failure;
pub use self::login::{login_attempt, login_lockout};
pub use self::registry::{init, runtime_identity};
pub use self::remote_call::remote_call_finished;
pub use self::scrape::scrape;
pub(crate) use self::secret::{secret_resolution_finished, secret_source_identity};
pub use self::security::{client_allowlist_rejected, revision_condition};
pub use self::storage::{storage_backend_identity, storage_operation_finished};
pub use self::task::{
    TaskOutputKind, task_claimed, task_completed, task_lease_recovered,
    task_output_cleanup_deleted, task_output_cleanup_failed, task_output_cleanup_run,
    task_worker_config, task_worker_iteration,
};
pub(crate) use self::token::{token_authentication, token_hash_key_ring};
pub(crate) use self::tracing::{
    trace_export_batch, trace_flush, trace_queue_utilization, trace_span_lifecycle,
    trace_spans_dropped, tracing_config,
};

static METRICS: OnceLock<Metrics> = OnceLock::new();

struct CheckedMetric<T> {
    instrument: T,
    definition: &'static MetricDefinition,
}

impl<T> CheckedMetric<T> {
    fn new(instrument: T, definition: &'static MetricDefinition) -> Self {
        Self {
            instrument,
            definition,
        }
    }

    fn attributes_match(&self, attributes: &[KeyValue]) -> bool {
        let labels_match = attributes.len() == self.definition.labels.len()
            && self.definition.labels.iter().all(|expected| {
                attributes
                    .iter()
                    .any(|attribute| attribute.key.as_str() == *expected)
            });
        let values_match = labels_match
            && attributes.iter().all(|attribute| {
                metric_label_value_is_allowed(
                    self.definition.name,
                    attribute.key.as_str(),
                    attribute.value.as_str().as_ref(),
                )
            });
        let matches = labels_match && values_match;
        if !matches {
            let actual = attributes
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.as_str(),
                        attribute.value.as_str().into_owned(),
                    )
                })
                .collect::<Vec<_>>();
            tracing::error!(
                metric = self.definition.name,
                expected_labels = ?self.definition.labels,
                actual_attributes = ?actual,
                "Metric observation rejected because its labels violate the operational contract"
            );
            debug_assert!(
                matches,
                "metric {} labels and values {:?} do not match {:?}",
                self.definition.name, actual, self.definition.labels
            );
        }
        matches
    }
}

macro_rules! checked_metric_method {
    ($instrument:ty, $method:ident, $value:ty) => {
        impl CheckedMetric<$instrument> {
            fn $method(&self, value: $value, attributes: &[KeyValue]) {
                if self.attributes_match(attributes) {
                    self.instrument.$method(value, attributes);
                }
            }
        }
    };
}

checked_metric_method!(Counter<u64>, add, u64);
checked_metric_method!(Gauge<u64>, record, u64);
checked_metric_method!(Gauge<i64>, record, i64);
checked_metric_method!(Gauge<f64>, record, f64);
checked_metric_method!(Histogram<f64>, record, f64);
checked_metric_method!(UpDownCounter<i64>, add, i64);

type CheckedCounter = CheckedMetric<Counter<u64>>;
type CheckedU64Gauge = CheckedMetric<Gauge<u64>>;
type CheckedI64Gauge = CheckedMetric<Gauge<i64>>;
type CheckedF64Gauge = CheckedMetric<Gauge<f64>>;
type CheckedHistogram = CheckedMetric<Histogram<f64>>;
type CheckedUpDownCounter = CheckedMetric<UpDownCounter<i64>>;

pub struct HttpInFlightGuard {
    route: Option<String>,
}

impl HttpInFlightGuard {
    pub(super) fn new(route: Option<String>) -> Self {
        Self { route }
    }
}

impl Drop for HttpInFlightGuard {
    fn drop(&mut self) {
        if let (Some(metrics), Some(route)) = (current(), self.route.as_ref()) {
            metrics
                .http_in_flight
                .add(-1, &[opentelemetry::KeyValue::new("route", route.clone())]);
        }
    }
}

struct Metrics {
    registry: Registry,
    _provider: SdkMeterProvider,
    build_info: CheckedU64Gauge,
    runtime_info: CheckedU64Gauge,
    process_start_time: CheckedF64Gauge,
    process_metrics: ProcessMetrics,
    http_requests: CheckedCounter,
    http_request_duration: CheckedHistogram,
    http_in_flight: CheckedUpDownCounter,
    api_errors: CheckedCounter,
    extraction_failures: CheckedCounter,
    db_pool_connections: CheckedU64Gauge,
    db_connection_acquire_duration: CheckedHistogram,
    db_connection_acquire_failures: CheckedCounter,
    db_operation_duration: CheckedHistogram,
    db_operation_errors: CheckedCounter,
    storage_backend_info: CheckedU64Gauge,
    storage_operation_duration: CheckedHistogram,
    storage_operation_errors: CheckedCounter,
    secret_source_info: CheckedU64Gauge,
    secret_resolution_duration: CheckedHistogram,
    secret_resolutions: CheckedCounter,
    token_hash_key_info: CheckedU64Gauge,
    token_hash_keys: CheckedU64Gauge,
    token_authentications: CheckedCounter,
    token_hash_stored: CheckedI64Gauge,
    task_worker_iterations: CheckedCounter,
    task_claims: CheckedCounter,
    task_lease_recoveries: CheckedCounter,
    task_completions: CheckedCounter,
    task_queue_wait_duration: CheckedHistogram,
    task_execution_duration: CheckedHistogram,
    task_workers_configured: CheckedU64Gauge,
    task_poll_interval: CheckedF64Gauge,
    task_counts: CheckedI64Gauge,
    task_oldest_age: CheckedF64Gauge,
    task_last_terminal_timestamp: CheckedF64Gauge,
    computed_evaluations: CheckedCounter,
    computed_evaluator_errors: CheckedCounter,
    computed_live_fallbacks: CheckedCounter,
    computed_read_repairs: CheckedCounter,
    computed_rebuild_batches: CheckedCounter,
    computed_rebuild_completions: CheckedCounter,
    computed_rebuild_duration: CheckedHistogram,
    task_output_cleanup_runs: CheckedCounter,
    task_output_cleanup_failures: CheckedCounter,
    task_output_cleanup_deleted: CheckedCounter,
    export_template_info: IntGaugeVec,
    export_phase_duration: CheckedHistogram,
    export_template_duration: CheckedHistogram,
    export_completions: CheckedCounter,
    export_truncations: CheckedCounter,
    export_warnings: CheckedCounter,
    import_phase_duration: CheckedHistogram,
    import_processed_items: CheckedCounter,
    import_succeeded_items: CheckedCounter,
    import_failed_items: CheckedCounter,
    remote_call_duration: CheckedHistogram,
    remote_call_results: CheckedCounter,
    login_attempts: CheckedCounter,
    login_lockouts: CheckedCounter,
    #[cfg(feature = "login-rate-limit-valkey")]
    login_limiter_backend_failures: CheckedCounter,
    login_limiter_entries: CheckedU64Gauge,
    client_allowlist_rejections: CheckedCounter,
    revision_conditions: CheckedCounter,
    event_queue_items: CheckedI64Gauge,
    event_stale_claims: CheckedI64Gauge,
    event_oldest_age: CheckedF64Gauge,
    event_workers_configured: CheckedU64Gauge,
    event_worker_batch_size: CheckedU64Gauge,
    event_worker_poll_interval: CheckedF64Gauge,
    event_worker_lock_timeout: CheckedF64Gauge,
    event_worker_wakeups: CheckedCounter,
    tracing_info: CheckedU64Gauge,
    tracing_sample_ratio: CheckedF64Gauge,
    tracing_queue_capacity: CheckedU64Gauge,
    tracing_queue_utilization: CheckedU64Gauge,
    trace_spans: CheckedCounter,
    trace_spans_dropped: CheckedCounter,
    trace_export_batches: CheckedCounter,
    trace_export_spans: CheckedCounter,
    trace_flushes: CheckedCounter,
    inventory_entities: CheckedI64Gauge,
    refresh_failures: CheckedCounter,
    refresh_duration: CheckedF64Gauge,
    refresh_last_success: CheckedF64Gauge,
    refresh_skipped: CheckedCounter,
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

#[cfg(test)]
mod contract_tests {
    use opentelemetry::global;

    use super::*;
    use crate::operational_contracts::runtime_metric_definition;

    #[test]
    #[should_panic(expected = "labels")]
    fn checked_metrics_reject_labels_from_a_different_contract() {
        let definition = runtime_metric_definition("hubuum_api_errors");
        let metric = CheckedMetric::new(
            global::meter("hubuum-contract-test")
                .u64_counter(definition.runtime_name())
                .build(),
            definition,
        );

        metric.add(1, &[KeyValue::new("route", "/api/v1")]);
    }

    #[test]
    #[should_panic(expected = "labels and values")]
    fn checked_metrics_reject_values_outside_an_enumerated_domain() {
        let definition = runtime_metric_definition("hubuum_task_worker_iterations");
        let metric = CheckedMetric::new(
            global::meter("hubuum-contract-value-test")
                .u64_counter(definition.runtime_name())
                .build(),
            definition,
        );

        metric.add(1, &[KeyValue::new("outcome", "waiting")]);
    }
}
