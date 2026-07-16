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
        task_lease_recoveries: meter
            .u64_counter("hubuum_task_lease_recoveries")
            .with_description("Tasks failed after worker lease expiry")
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
        computed_evaluations: meter
            .u64_counter("hubuum_computed_field_evaluations")
            .with_description("Computed-field evaluations by scope and outcome")
            .build(),
        computed_evaluator_errors: meter
            .u64_counter("hubuum_computed_field_errors")
            .with_description("Computed-field runtime errors by stable code")
            .build(),
        computed_live_fallbacks: meter
            .u64_counter("hubuum_computed_field_live_fallbacks")
            .with_description("Stale materializations evaluated live during reads")
            .build(),
        computed_read_repairs: meter
            .u64_counter("hubuum_computed_field_read_repairs")
            .with_description("Guarded computed-field read repairs by outcome")
            .build(),
        computed_rebuild_batches: meter
            .u64_counter("hubuum_computed_field_rebuild_batches")
            .with_description("Computed-field rebuild batches")
            .build(),
        computed_rebuild_completions: meter
            .u64_counter("hubuum_computed_field_rebuild_completions")
            .with_description("Computed-field rebuild terminal outcomes")
            .build(),
        computed_rebuild_duration: meter
            .f64_histogram("hubuum_computed_field_rebuild_duration")
            .with_description("Computed-field rebuild duration")
            .with_unit("s")
            .build(),
        export_output_cleanup_runs: meter
            .u64_counter("hubuum_export_output_cleanup_runs")
            .with_description("Stored export and backup output cleanup runs")
            .build(),
        export_output_cleanup_failures: meter
            .u64_counter("hubuum_export_output_cleanup_failures")
            .with_description("Stored export and backup output cleanup failures")
            .build(),
        export_output_cleanup_deleted: meter
            .u64_counter("hubuum_export_output_cleanup_deleted")
            .with_description("Stored export and backup outputs deleted by cleanup")
            .build(),
        export_duration: meter
            .f64_histogram("hubuum_export_phase_duration")
            .with_description("Export phase duration")
            .with_unit("s")
            .build(),
        export_completions: meter
            .u64_counter("hubuum_export_completions")
            .with_description("Successfully persisted export outputs")
            .build(),
        export_truncations: meter
            .u64_counter("hubuum_export_truncations")
            .with_description("Successfully persisted truncated exports")
            .build(),
        export_warnings: meter
            .u64_counter("hubuum_export_warnings")
            .with_description("Warnings on successfully persisted exports")
            .build(),
        import_duration: meter
            .f64_histogram("hubuum_import_phase_duration")
            .with_description("Import phase duration")
            .with_unit("s")
            .build(),
        import_processed_items: meter
            .u64_counter("hubuum_import_processed_items")
            .with_description("Import items processed by terminal tasks")
            .build(),
        import_succeeded_items: meter
            .u64_counter("hubuum_import_succeeded_items")
            .with_description("Import items completed successfully")
            .build(),
        import_failed_items: meter
            .u64_counter("hubuum_import_failed_items")
            .with_description("Import items completed with failure")
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
        login_lockouts: meter
            .u64_counter("hubuum_login_lockouts")
            .with_description("Login limiter lockout transitions by scope kind")
            .build(),
        #[cfg(feature = "login-rate-limit-valkey")]
        login_limiter_backend_failures: meter
            .u64_counter("hubuum_login_limiter_backend_failures")
            .with_description("Login limiter backend failures by operation")
            .build(),
        login_limiter_entries: meter
            .u64_gauge("hubuum_login_limiter_entries")
            .with_description("Login limiter entries")
            .build(),
        client_allowlist_rejections: meter
            .u64_counter("hubuum_client_allowlist_rejections")
            .with_description("Requests rejected by the client IP allowlist")
            .build(),
        event_queue_items: meter
            .i64_gauge("hubuum_event_queue_items")
            .with_description("Event fan-out and delivery queue items by state")
            .build(),
        event_stale_claims: meter
            .i64_gauge("hubuum_event_stale_claims")
            .with_description("Stale event worker claims by queue")
            .build(),
        event_oldest_age: meter
            .f64_gauge("hubuum_event_oldest_age")
            .with_description("Oldest actionable event item age by queue")
            .with_unit("s")
            .build(),
        event_worker_config: meter
            .u64_gauge("hubuum_event_worker_config")
            .with_description("Configured event worker settings")
            .build(),
        event_worker_wakeups: meter
            .u64_gauge("hubuum_event_worker_wakeups")
            .with_description("Event worker wakeup counters")
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
        db_refresh_lock: tokio::sync::Mutex::new(()),
    };

    METRICS
        .set(metrics)
        .map_err(|_| ApiError::InternalServerError("Metrics already initialized".to_string()))
}
