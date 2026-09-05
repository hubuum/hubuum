use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use prometheus::{IntGaugeVec, Opts, Registry};

use crate::config::RuntimeRole;
use crate::errors::ApiError;
use crate::logger;
use crate::operational_contracts::{MetricKind, metric_definition, runtime_metric_definition};

use super::cache::ScrapeCache;
use super::process::ProcessMetrics;
use super::{
    CheckedCounter, CheckedF64Gauge, CheckedHistogram, CheckedI64Gauge, CheckedMetric,
    CheckedU64Gauge, CheckedUpDownCounter, METRICS, Metrics,
};

fn duration_histogram_view(instrument: &Instrument) -> Option<Stream> {
    let definition = runtime_metric_definition(instrument.name());
    if !matches!(definition.kind, MetricKind::Histogram) {
        return None;
    }

    Some(
        Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: definition.buckets.to_vec(),
                // The Prometheus classic-histogram format exports buckets,
                // count, and sum, but not exact minima or maxima.
                record_min_max: false,
            })
            .build()
            .expect("hard-coded duration histogram boundaries should be valid"),
    )
}

fn u64_counter(meter: &Meter, name: &'static str) -> CheckedCounter {
    let definition = runtime_metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Counter));
    let mut builder = meter
        .u64_counter(definition.runtime_name())
        .with_description(definition.description);
    if let Some(unit) = definition.open_telemetry_unit() {
        builder = builder.with_unit(unit);
    }
    CheckedMetric::new(builder.build(), definition)
}

fn u64_gauge(meter: &Meter, name: &'static str) -> CheckedU64Gauge {
    let definition = runtime_metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Gauge));
    let mut builder = meter
        .u64_gauge(definition.runtime_name())
        .with_description(definition.description);
    if let Some(unit) = definition.open_telemetry_unit() {
        builder = builder.with_unit(unit);
    }
    CheckedMetric::new(builder.build(), definition)
}

fn i64_gauge(meter: &Meter, name: &'static str) -> CheckedI64Gauge {
    let definition = runtime_metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Gauge));
    let mut builder = meter
        .i64_gauge(definition.runtime_name())
        .with_description(definition.description);
    if let Some(unit) = definition.open_telemetry_unit() {
        builder = builder.with_unit(unit);
    }
    CheckedMetric::new(builder.build(), definition)
}

fn f64_gauge(meter: &Meter, name: &'static str) -> CheckedF64Gauge {
    let definition = runtime_metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Gauge));
    let mut builder = meter
        .f64_gauge(definition.runtime_name())
        .with_description(definition.description);
    if let Some(unit) = definition.open_telemetry_unit() {
        builder = builder.with_unit(unit);
    }
    CheckedMetric::new(builder.build(), definition)
}

fn i64_up_down_counter(meter: &Meter, name: &'static str) -> CheckedUpDownCounter {
    let definition = runtime_metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Gauge));
    let mut builder = meter
        .i64_up_down_counter(definition.runtime_name())
        .with_description(definition.description);
    if let Some(unit) = definition.open_telemetry_unit() {
        builder = builder.with_unit(unit);
    }
    CheckedMetric::new(builder.build(), definition)
}

fn duration_histogram(meter: &Meter, name: &'static str) -> CheckedHistogram {
    let definition = runtime_metric_definition(name);
    assert!(matches!(definition.kind, MetricKind::Histogram));
    let mut builder = meter
        .f64_histogram(definition.runtime_name())
        .with_description(definition.description);
    if let Some(unit) = definition.open_telemetry_unit() {
        builder = builder.with_unit(unit);
    }
    CheckedMetric::new(builder.build(), definition)
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
    let export_template_definition = metric_definition("hubuum_export_template_info");
    assert!(matches!(export_template_definition.kind, MetricKind::Gauge));
    let export_template_info = IntGaugeVec::new(
        Opts::new(
            export_template_definition.runtime_name(),
            export_template_definition.description,
        ),
        export_template_definition.labels,
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
        build_info: u64_gauge(&meter, "hubuum_build_info"),
        runtime_info: u64_gauge(&meter, "hubuum_runtime_info"),
        process_start_time: f64_gauge(&meter, "hubuum_process_start_time"),
        process_metrics,
        http_requests: u64_counter(&meter, "hubuum_http_requests"),
        http_request_duration: duration_histogram(&meter, "hubuum_http_request_duration"),
        http_in_flight: i64_up_down_counter(&meter, "hubuum_http_requests_in_flight"),
        api_errors: u64_counter(&meter, "hubuum_api_errors"),
        extraction_failures: u64_counter(&meter, "hubuum_extraction_failures"),
        db_pool_connections: u64_gauge(&meter, "hubuum_db_pool_connections"),
        db_connection_acquire_duration: duration_histogram(
            &meter,
            "hubuum_db_connection_acquire_duration",
        ),
        db_connection_acquire_failures: u64_counter(
            &meter,
            "hubuum_db_connection_acquire_failures",
        ),
        db_operation_duration: duration_histogram(&meter, "hubuum_db_operation_duration"),
        db_operation_errors: u64_counter(&meter, "hubuum_db_operation_errors"),
        storage_backend_info: u64_gauge(&meter, "hubuum_storage_backend_info"),
        storage_operation_duration: duration_histogram(&meter, "hubuum_storage_operation_duration"),
        storage_operation_errors: u64_counter(&meter, "hubuum_storage_operation_errors"),
        secret_source_info: u64_gauge(&meter, "hubuum_secret_source_info"),
        secret_resolution_duration: duration_histogram(&meter, "hubuum_secret_resolution_duration"),
        secret_resolutions: u64_counter(&meter, "hubuum_secret_resolutions"),
        token_hash_key_info: u64_gauge(&meter, "hubuum_token_hash_key_info"),
        token_hash_keys: u64_gauge(&meter, "hubuum_token_hash_keys"),
        token_authentications: u64_counter(&meter, "hubuum_token_authentications"),
        token_hash_stored: i64_gauge(&meter, "hubuum_token_hash_stored"),
        task_worker_iterations: u64_counter(&meter, "hubuum_task_worker_iterations"),
        task_claims: u64_counter(&meter, "hubuum_task_claims"),
        task_lease_recoveries: u64_counter(&meter, "hubuum_task_lease_recoveries"),
        task_completions: u64_counter(&meter, "hubuum_task_completions"),
        task_queue_wait_duration: duration_histogram(&meter, "hubuum_task_queue_wait_duration"),
        task_execution_duration: duration_histogram(&meter, "hubuum_task_execution_duration"),
        task_workers_configured: u64_gauge(&meter, "hubuum_task_workers_configured"),
        task_poll_interval: f64_gauge(&meter, "hubuum_task_poll_interval"),
        task_counts: i64_gauge(&meter, "hubuum_tasks"),
        task_oldest_age: f64_gauge(&meter, "hubuum_task_oldest_age"),
        task_last_terminal_timestamp: f64_gauge(&meter, "hubuum_task_last_terminal_timestamp"),
        computed_evaluations: u64_counter(&meter, "hubuum_computed_field_evaluations"),
        computed_evaluator_errors: u64_counter(&meter, "hubuum_computed_field_errors"),
        computed_live_fallbacks: u64_counter(&meter, "hubuum_computed_field_live_fallbacks"),
        computed_read_repairs: u64_counter(&meter, "hubuum_computed_field_read_repairs"),
        computed_rebuild_batches: u64_counter(&meter, "hubuum_computed_field_rebuild_batches"),
        computed_rebuild_completions: u64_counter(
            &meter,
            "hubuum_computed_field_rebuild_completions",
        ),
        computed_rebuild_duration: duration_histogram(
            &meter,
            "hubuum_computed_field_rebuild_duration",
        ),
        task_output_cleanup_runs: u64_counter(&meter, "hubuum_task_output_cleanup_runs"),
        task_output_cleanup_failures: u64_counter(&meter, "hubuum_task_output_cleanup_failures"),
        task_output_cleanup_deleted: u64_counter(&meter, "hubuum_task_output_cleanup_deleted"),
        export_template_info,
        export_phase_duration: duration_histogram(&meter, "hubuum_export_phase_duration"),
        export_template_duration: duration_histogram(&meter, "hubuum_export_duration"),
        export_completions: u64_counter(&meter, "hubuum_export_completions"),
        export_truncations: u64_counter(&meter, "hubuum_export_truncations"),
        export_warnings: u64_counter(&meter, "hubuum_export_warnings"),
        import_phase_duration: duration_histogram(&meter, "hubuum_import_phase_duration"),
        import_processed_items: u64_counter(&meter, "hubuum_import_processed_items"),
        import_succeeded_items: u64_counter(&meter, "hubuum_import_succeeded_items"),
        import_failed_items: u64_counter(&meter, "hubuum_import_failed_items"),
        remote_call_duration: duration_histogram(&meter, "hubuum_remote_call_duration"),
        remote_call_results: u64_counter(&meter, "hubuum_remote_call_results"),
        login_attempts: u64_counter(&meter, "hubuum_login_attempts"),
        login_lockouts: u64_counter(&meter, "hubuum_login_lockouts"),
        #[cfg(feature = "login-rate-limit-valkey")]
        login_limiter_backend_failures: u64_counter(
            &meter,
            "hubuum_login_limiter_backend_failures",
        ),
        login_limiter_entries: u64_gauge(&meter, "hubuum_login_limiter_entries"),
        client_allowlist_rejections: u64_counter(&meter, "hubuum_client_allowlist_rejections"),
        revision_conditions: u64_counter(&meter, "hubuum_revision_conditions"),
        event_queue_items: i64_gauge(&meter, "hubuum_event_queue_items"),
        event_stale_claims: i64_gauge(&meter, "hubuum_event_stale_claims"),
        event_oldest_age: f64_gauge(&meter, "hubuum_event_oldest_age"),
        event_workers_configured: u64_gauge(&meter, "hubuum_event_workers_configured"),
        event_worker_batch_size: u64_gauge(&meter, "hubuum_event_worker_batch_size"),
        event_worker_poll_interval: f64_gauge(&meter, "hubuum_event_worker_poll_interval"),
        event_worker_lock_timeout: f64_gauge(&meter, "hubuum_event_worker_lock_timeout"),
        event_worker_wakeups: u64_counter(&meter, "hubuum_event_worker_wakeups"),
        tracing_info: u64_gauge(&meter, "hubuum_tracing_info"),
        tracing_sample_ratio: f64_gauge(&meter, "hubuum_tracing_sample_ratio"),
        tracing_queue_capacity: u64_gauge(&meter, "hubuum_tracing_queue_capacity"),
        tracing_queue_utilization: u64_gauge(&meter, "hubuum_tracing_queue_utilization"),
        trace_spans: u64_counter(&meter, "hubuum_trace_spans"),
        trace_spans_dropped: u64_counter(&meter, "hubuum_trace_spans_dropped"),
        trace_export_batches: u64_counter(&meter, "hubuum_trace_export_batches"),
        trace_export_spans: u64_counter(&meter, "hubuum_trace_export_spans"),
        trace_flushes: u64_counter(&meter, "hubuum_trace_flushes"),
        inventory_entities: i64_gauge(&meter, "hubuum_inventory_entities"),
        refresh_failures: u64_counter(&meter, "hubuum_metrics_refresh_failures"),
        refresh_duration: f64_gauge(&meter, "hubuum_metrics_refresh_duration"),
        refresh_last_success: f64_gauge(&meter, "hubuum_metrics_refresh_last_success_timestamp"),
        refresh_skipped: u64_counter(&meter, "hubuum_metrics_refresh_skipped"),
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
    fn storage_histograms_use_subsecond_latency_buckets() {
        let bounds = histogram_bucket_bounds("hubuum_storage_operation_duration", 0.004);

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
