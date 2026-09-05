use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Histogram, Meter, MeterProvider as _};
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use prometheus::{IntGaugeVec, Opts, Registry};

use crate::config::RuntimeRole;
use crate::errors::ApiError;
use crate::logger;

use super::cache::ScrapeCache;
use super::process::ProcessMetrics;
use super::{METRICS, Metrics};

const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];
const OUTBOUND_BUCKETS_SECONDS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];
const BACKGROUND_BUCKETS_SECONDS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
    1800.0, 3600.0,
];

const HTTP_REQUEST_DURATION: &str = "hubuum_http_request_duration";
const DB_CONNECTION_ACQUIRE_DURATION: &str = "hubuum_db_connection_acquire_duration";
const DB_OPERATION_DURATION: &str = "hubuum_db_operation_duration";
const STORAGE_OPERATION_DURATION: &str = "hubuum_storage_operation_duration";
const SECRET_RESOLUTION_DURATION: &str = "hubuum_secret_resolution_duration";
const TEMPLATE_WORKER_DURATION: &str = "hubuum_template_worker_duration";
const REMOTE_CALL_DURATION: &str = "hubuum_remote_call_duration";
const TASK_QUEUE_WAIT_DURATION: &str = "hubuum_task_queue_wait_duration";
const TASK_EXECUTION_DURATION: &str = "hubuum_task_execution_duration";
const COMPUTED_REBUILD_DURATION: &str = "hubuum_computed_field_rebuild_duration";
const EXPORT_PHASE_DURATION: &str = "hubuum_export_phase_duration";
const EXPORT_DURATION: &str = "hubuum_export_duration";
const IMPORT_PHASE_DURATION: &str = "hubuum_import_phase_duration";

fn duration_histogram_view(instrument: &Instrument) -> Option<Stream> {
    let boundaries = match instrument.name() {
        HTTP_REQUEST_DURATION
        | DB_CONNECTION_ACQUIRE_DURATION
        | DB_OPERATION_DURATION
        | STORAGE_OPERATION_DURATION
        | SECRET_RESOLUTION_DURATION
        | TEMPLATE_WORKER_DURATION => LATENCY_BUCKETS_SECONDS,
        REMOTE_CALL_DURATION => OUTBOUND_BUCKETS_SECONDS,
        TASK_QUEUE_WAIT_DURATION
        | TASK_EXECUTION_DURATION
        | COMPUTED_REBUILD_DURATION
        | EXPORT_PHASE_DURATION
        | EXPORT_DURATION
        | IMPORT_PHASE_DURATION => BACKGROUND_BUCKETS_SECONDS,
        _ => return None,
    };

    Some(
        Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: boundaries.to_vec(),
                // The Prometheus classic-histogram format exports buckets,
                // count, and sum, but not exact minima or maxima.
                record_min_max: false,
            })
            .build()
            .expect("hard-coded duration histogram boundaries should be valid"),
    )
}

fn duration_histogram(
    meter: &Meter,
    name: &'static str,
    description: &'static str,
) -> Histogram<f64> {
    meter
        .f64_histogram(name)
        .with_description(description)
        .with_unit("s")
        .build()
}

fn build_provider(registry: &Registry) -> Result<SdkMeterProvider, ApiError> {
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_target_info()
        .build()
        .map_err(|error| {
            ApiError::InternalServerError(format!("Failed to initialize metrics exporter: {error}"))
        })?;

    Ok(SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_view(duration_histogram_view)
        .build())
}

pub fn init() -> Result<(), ApiError> {
    if METRICS.get().is_some() {
        return Ok(());
    }

    let registry = Registry::new();
    let process_metrics = ProcessMetrics::new(&registry)?;
    let provider = build_provider(&registry)?;
    let meter = provider.meter("hubuum");
    let export_template_info = IntGaugeVec::new(
        Opts::new(
            "hubuum_export_template_info",
            "Current stored export template identities from the shared database",
        ),
        &["template_id", "template_name"],
    )
    .map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to create export template info metric: {error}"
        ))
    })?;
    registry
        .register(Box::new(export_template_info.clone()))
        .map_err(|error| {
            ApiError::InternalServerError(format!(
                "Failed to register export template info metric: {error}"
            ))
        })?;

    let metrics = Metrics {
        registry,
        _provider: provider,
        build_info: meter
            .u64_gauge("hubuum_build_info")
            .with_description("Hubuum build identity")
            .build(),
        runtime_info: meter
            .u64_gauge("hubuum_runtime_info")
            .with_description("Hubuum process runtime role")
            .build(),
        process_start_time: meter
            .f64_gauge("hubuum_process_start_time")
            .with_description("Unix timestamp when this Hubuum process initialized metrics")
            .with_unit("s")
            .build(),
        process_metrics,
        http_requests: meter
            .u64_counter("hubuum_http_requests")
            .with_description("HTTP requests handled")
            .build(),
        http_request_duration: duration_histogram(
            &meter,
            HTTP_REQUEST_DURATION,
            "HTTP request duration",
        ),
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
        db_connection_acquire_duration: duration_histogram(
            &meter,
            DB_CONNECTION_ACQUIRE_DURATION,
            "Database connection acquisition duration",
        ),
        db_connection_acquire_failures: meter
            .u64_counter("hubuum_db_connection_acquire_failures")
            .with_description("Database connection acquisition failures")
            .build(),
        db_operation_duration: duration_histogram(
            &meter,
            DB_OPERATION_DURATION,
            "Database helper operation duration",
        ),
        db_operation_errors: meter
            .u64_counter("hubuum_db_operation_errors")
            .with_description("Database helper operation failures")
            .build(),
        storage_backend_info: meter
            .u64_gauge("hubuum_storage_backend_info")
            .with_description("Selected complete storage backend")
            .build(),
        storage_operation_duration: duration_histogram(
            &meter,
            STORAGE_OPERATION_DURATION,
            "Backend-neutral logical storage operation duration",
        ),
        storage_operation_errors: meter
            .u64_counter("hubuum_storage_operation_errors")
            .with_description("Backend-neutral logical storage operation failures")
            .build(),
        secret_source_info: meter
            .u64_gauge("hubuum_secret_source_info")
            .with_description("Selected secret provider")
            .build(),
        secret_resolution_duration: duration_histogram(
            &meter,
            SECRET_RESOLUTION_DURATION,
            "Secret resolution duration by bounded provider and consumer",
        ),
        secret_resolutions: meter
            .u64_counter("hubuum_secret_resolutions")
            .with_description("Secret resolution outcomes by bounded provider and consumer")
            .build(),
        token_hash_key_info: meter
            .u64_gauge("hubuum_token_hash_key_info")
            .with_description("Configured token hash key-ring mode")
            .build(),
        token_hash_keys: meter
            .u64_gauge("hubuum_token_hash_keys")
            .with_description("Configured token hash keys by bounded lifecycle state")
            .build(),
        token_authentications: meter
            .u64_counter("hubuum_token_authentications")
            .with_description("Bearer-token authentication and migration outcomes")
            .build(),
        token_hash_stored: meter
            .i64_gauge("hubuum_token_hash_stored")
            .with_description("Stored bearer tokens by bounded key and lifecycle state")
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
        task_queue_wait_duration: duration_histogram(
            &meter,
            TASK_QUEUE_WAIT_DURATION,
            "Task queue wait duration",
        ),
        task_execution_duration: duration_histogram(
            &meter,
            TASK_EXECUTION_DURATION,
            "Task execution duration",
        ),
        task_workers_configured: meter
            .u64_gauge("hubuum_task_workers_configured")
            .with_description("Configured task workers in this process")
            .build(),
        task_poll_interval: meter
            .f64_gauge("hubuum_task_poll_interval")
            .with_description("Configured task worker poll interval")
            .with_unit("s")
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
        task_last_terminal_timestamp: meter
            .f64_gauge("hubuum_task_last_terminal_timestamp")
            .with_description("Unix timestamp of the most recently finished task")
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
        computed_rebuild_duration: duration_histogram(
            &meter,
            COMPUTED_REBUILD_DURATION,
            "Computed-field rebuild duration",
        ),
        task_output_cleanup_runs: meter
            .u64_counter("hubuum_task_output_cleanup_runs")
            .with_description("Stored task output cleanup runs")
            .build(),
        task_output_cleanup_failures: meter
            .u64_counter("hubuum_task_output_cleanup_failures")
            .with_description("Stored task output cleanup failures")
            .build(),
        task_output_cleanup_deleted: meter
            .u64_counter("hubuum_task_output_cleanup_deleted")
            .with_description("Stored task outputs deleted by cleanup")
            .build(),
        export_template_info,
        export_phase_duration: duration_histogram(
            &meter,
            EXPORT_PHASE_DURATION,
            "Export phase duration",
        ),
        export_template_duration: duration_histogram(
            &meter,
            EXPORT_DURATION,
            "Overall export duration by stored template identity",
        ),
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
        import_phase_duration: duration_histogram(
            &meter,
            IMPORT_PHASE_DURATION,
            "Import phase duration",
        ),
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
        remote_call_duration: duration_histogram(
            &meter,
            REMOTE_CALL_DURATION,
            "Remote call duration",
        ),
        template_worker_events: meter
            .u64_counter("hubuum_template_worker_events")
            .with_description("Template worker lifecycle events by bounded event kind")
            .build(),
        template_worker_duration: duration_histogram(
            &meter,
            TEMPLATE_WORKER_DURATION,
            "Template operation duration including queueing and cleanup",
        ),
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
        revision_conditions: meter
            .u64_counter("hubuum_revision_conditions")
            .with_description("Revision precondition outcomes by bounded kind")
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
        event_workers_configured: meter
            .u64_gauge("hubuum_event_workers_configured")
            .with_description("Configured event workers in this process")
            .build(),
        event_worker_batch_size: meter
            .u64_gauge("hubuum_event_worker_batch_size")
            .with_description("Configured event worker batch size")
            .build(),
        event_worker_poll_interval: meter
            .f64_gauge("hubuum_event_worker_poll_interval")
            .with_description("Configured event worker poll interval")
            .with_unit("s")
            .build(),
        event_worker_lock_timeout: meter
            .f64_gauge("hubuum_event_worker_lock_timeout")
            .with_description("Configured event worker claim lock timeout")
            .with_unit("s")
            .build(),
        event_worker_wakeups: meter
            .u64_counter("hubuum_event_worker_wakeups")
            .with_description("Event worker wakeups")
            .build(),
        tracing_info: meter
            .u64_gauge("hubuum_tracing_info")
            .with_description("Configured OpenTelemetry sampling mode")
            .build(),
        tracing_sample_ratio: meter
            .f64_gauge("hubuum_tracing_sample_ratio")
            .with_description("Configured OpenTelemetry trace sampling ratio")
            .build(),
        tracing_queue_capacity: meter
            .u64_gauge("hubuum_tracing_queue_capacity")
            .with_description("Configured OpenTelemetry export queue capacity")
            .build(),
        tracing_queue_utilization: meter
            .u64_gauge("hubuum_tracing_queue_utilization")
            .with_description("OpenTelemetry spans waiting for export")
            .build(),
        trace_spans: meter
            .u64_counter("hubuum_trace_spans")
            .with_description("Sampled OpenTelemetry spans by closed category and lifecycle state")
            .build(),
        trace_spans_dropped: meter
            .u64_counter("hubuum_trace_spans_dropped")
            .with_description("OpenTelemetry spans dropped by bounded reason")
            .build(),
        trace_export_batches: meter
            .u64_counter("hubuum_trace_export_batches")
            .with_description("OpenTelemetry export batches by outcome")
            .build(),
        trace_export_spans: meter
            .u64_counter("hubuum_trace_export_spans")
            .with_description("OpenTelemetry spans submitted in export batches by outcome")
            .build(),
        trace_flushes: meter
            .u64_counter("hubuum_trace_flushes")
            .with_description("OpenTelemetry shutdown flushes by outcome")
            .build(),
        inventory_entities: meter
            .i64_gauge("hubuum_inventory_entities")
            .with_description("Low-cardinality domain inventory counts")
            .build(),
        refresh_failures: meter
            .u64_counter("hubuum_metrics_refresh_failures")
            .with_description("Metrics scrape refresh failures by source")
            .build(),
        refresh_duration: meter
            .f64_gauge("hubuum_metrics_refresh_duration")
            .with_description("Duration of the latest metrics source refresh attempt")
            .with_unit("s")
            .build(),
        refresh_last_success: meter
            .f64_gauge("hubuum_metrics_refresh_last_success_timestamp")
            .with_description("Unix timestamp of the latest successful metrics source refresh")
            .with_unit("s")
            .build(),
        refresh_skipped: meter
            .u64_counter("hubuum_metrics_refresh_skipped")
            .with_description("Metrics source refreshes skipped by reason")
            .build(),
        scrape_cache: Mutex::new(ScrapeCache::default()),
        db_refresh_lock: tokio::sync::Mutex::new(()),
    };

    metrics.build_info.record(
        1,
        &[
            KeyValue::new("version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("git_sha", logger::build_git_sha()),
        ],
    );
    metrics.process_start_time.record(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        &[],
    );

    METRICS
        .set(metrics)
        .map_err(|_| ApiError::InternalServerError("Metrics already initialized".to_string()))
}

pub fn runtime_identity(role: RuntimeRole) {
    if let Some(metrics) = super::current() {
        metrics
            .runtime_info
            .record(1, &[KeyValue::new("role", role.as_str())]);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::{Encoder, TextEncoder};

    use super::*;

    fn histogram_bucket_bounds(instrument_name: &'static str, value: f64) -> Vec<String> {
        let registry = Registry::new();
        let provider = build_provider(&registry).unwrap();
        let meter = provider.meter("hubuum-test");
        meter
            .f64_histogram(instrument_name)
            .with_unit("s")
            .build()
            .record(value, &[]);

        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut encoded)
            .unwrap();
        let body = String::from_utf8(encoded).unwrap();
        let exported_name = format!("{instrument_name}_seconds_bucket");

        body.lines()
            .filter(|line| line.starts_with(&exported_name))
            .filter_map(|line| {
                line.split_once("le=\"")
                    .and_then(|(_, suffix)| suffix.split_once('"'))
                    .map(|(bound, _)| bound.to_string())
            })
            .collect()
    }

    #[test]
    fn request_histograms_use_subsecond_latency_buckets() {
        let bounds = histogram_bucket_bounds(HTTP_REQUEST_DURATION, 0.004);

        assert_eq!(
            bounds,
            [
                "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25",
                "0.5", "1", "2.5", "5", "10", "30", "+Inf",
            ]
        );
    }

    #[test]
    fn storage_histograms_use_subsecond_latency_buckets() {
        let bounds = histogram_bucket_bounds(STORAGE_OPERATION_DURATION, 0.004);

        assert_eq!(
            bounds,
            [
                "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25",
                "0.5", "1", "2.5", "5", "10", "30", "+Inf",
            ]
        );
    }

    #[test]
    fn task_histograms_cover_long_running_background_work() {
        let bounds = histogram_bucket_bounds(TASK_EXECUTION_DURATION, 75.0);

        assert_eq!(
            bounds,
            [
                "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "60",
                "120", "300", "600", "1800", "3600", "+Inf",
            ]
        );
    }
}
