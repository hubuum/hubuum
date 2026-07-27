use std::sync::Mutex;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use prometheus::Registry;

use crate::errors::ApiError;

use super::cache::ScrapeCache;
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

fn duration_histogram_view(instrument: &Instrument) -> Option<Stream> {
    let boundaries = match instrument.name() {
        "hubuum_http_request_duration"
        | "hubuum_db_connection_acquire_duration"
        | "hubuum_db_operation_duration" => LATENCY_BUCKETS_SECONDS,
        "hubuum_remote_call_duration" => OUTBOUND_BUCKETS_SECONDS,
        "hubuum_task_queue_wait_duration"
        | "hubuum_task_execution_duration"
        | "hubuum_computed_field_rebuild_duration"
        | "hubuum_export_phase_duration"
        | "hubuum_import_phase_duration" => BACKGROUND_BUCKETS_SECONDS,
        _ => return None,
    };

    Some(
        Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: boundaries.to_vec(),
                record_min_max: true,
            })
            .build()
            .expect("hard-coded duration histogram boundaries should be valid"),
    )
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
    let provider = build_provider(&registry)?;
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
        export_template_info: meter
            .u64_gauge("hubuum_export_template_info")
            .with_description("Stored export templates observed by this process")
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

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
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
        let bounds = histogram_bucket_bounds("hubuum_http_request_duration", 0.004);

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
        let bounds = histogram_bucket_bounds("hubuum_task_execution_duration", 75.0);

        assert_eq!(
            bounds,
            [
                "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "60",
                "120", "300", "600", "1800", "3600", "+Inf",
            ]
        );
    }
}
