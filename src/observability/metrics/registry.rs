use std::sync::Mutex;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::Registry;

use crate::errors::ApiError;

use super::cache::ScrapeCache;
use super::{METRICS, Metrics};

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
