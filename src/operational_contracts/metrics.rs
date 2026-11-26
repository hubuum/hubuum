use serde::Serialize;

pub(super) const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];
pub(super) const OUTBOUND_BUCKETS_SECONDS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];
pub(super) const BACKGROUND_BUCKETS_SECONDS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
    1800.0, 3600.0,
];
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetricScope {
    Process,
    Database,
}

#[derive(Clone, Copy, Serialize)]
pub(crate) struct MetricDefinition {
    pub(crate) name: &'static str,
    pub(crate) kind: MetricKind,
    pub(crate) unit: Option<&'static str>,
    pub(crate) labels: &'static [&'static str],
    pub(crate) buckets: &'static [f64],
    pub(crate) scope: MetricScope,
    pub(crate) description: &'static str,
    pub(crate) feature: Option<&'static str>,
}

macro_rules! metric {
    ($name:literal, $kind:ident, $unit:expr, [$($label:literal),* $(,)?], $buckets:expr, $scope:ident, $description:literal) => {
        MetricDefinition {
            name: $name,
            kind: MetricKind::$kind,
            unit: $unit,
            labels: &[$($label),*],
            buckets: $buckets,
            scope: MetricScope::$scope,
            description: $description,
            feature: None,
        }
    };
    ($name:literal, $kind:ident, $unit:expr, [$($label:literal),* $(,)?], $buckets:expr, $scope:ident, $description:literal, feature = $feature:literal) => {
        MetricDefinition {
            name: $name,
            kind: MetricKind::$kind,
            unit: $unit,
            labels: &[$($label),*],
            buckets: $buckets,
            scope: MetricScope::$scope,
            description: $description,
            feature: Some($feature),
        }
    };
}

#[cfg(test)]
pub(super) use metric;

pub(crate) const METRICS: &[MetricDefinition] = &[
    metric!(
        "hubuum_tracing_info",
        Gauge,
        None,
        ["sampling_mode"],
        &[],
        Process,
        "Configured OpenTelemetry sampling mode"
    ),
    metric!(
        "hubuum_tracing_sample_ratio",
        Gauge,
        None,
        [],
        &[],
        Process,
        "Configured OpenTelemetry trace sampling ratio"
    ),
    metric!(
        "hubuum_tracing_queue_capacity",
        Gauge,
        None,
        [],
        &[],
        Process,
        "Configured OpenTelemetry export queue capacity"
    ),
    metric!(
        "hubuum_tracing_queue_utilization",
        Gauge,
        None,
        [],
        &[],
        Process,
        "OpenTelemetry spans waiting for export"
    ),
    metric!(
        "hubuum_trace_spans_total",
        Counter,
        None,
        ["category", "state"],
        &[],
        Process,
        "Sampled OpenTelemetry spans by closed category and lifecycle state"
    ),
    metric!(
        "hubuum_trace_spans_dropped_total",
        Counter,
        None,
        ["reason"],
        &[],
        Process,
        "OpenTelemetry spans dropped by bounded reason"
    ),
    metric!(
        "hubuum_trace_export_batches_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "OpenTelemetry export batches by outcome"
    ),
    metric!(
        "hubuum_trace_export_spans_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "OpenTelemetry spans submitted in export batches by outcome"
    ),
    metric!(
        "hubuum_trace_flushes_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "OpenTelemetry shutdown flushes by outcome"
    ),
    metric!(
        "hubuum_api_errors_total",
        Counter,
        None,
        ["class"],
        &[],
        Process,
        "API errors by public error class"
    ),
    metric!(
        "hubuum_build_info",
        Gauge,
        None,
        ["version", "git_sha"],
        &[],
        Process,
        "Build identity for the process"
    ),
    metric!(
        "hubuum_client_allowlist_rejections_total",
        Counter,
        None,
        ["reason"],
        &[],
        Process,
        "Requests rejected by the client allowlist"
    ),
    metric!(
        "hubuum_computed_field_errors_total",
        Counter,
        None,
        ["scope", "code"],
        &[],
        Process,
        "Computed-field runtime errors by stable code"
    ),
    metric!(
        "hubuum_computed_field_evaluations_total",
        Counter,
        None,
        ["scope", "outcome"],
        &[],
        Process,
        "Computed-field evaluations"
    ),
    metric!(
        "hubuum_computed_field_live_fallbacks_total",
        Counter,
        None,
        [],
        &[],
        Process,
        "Stale materializations evaluated live"
    ),
    metric!(
        "hubuum_computed_field_read_repairs_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "Guarded computed-field read repairs"
    ),
    metric!(
        "hubuum_schema_dependency_rebuilds_total",
        Counter,
        None,
        [],
        &[],
        Process,
        "Dependent rebuilds queued by explicit schema activation"
    ),
    metric!(
        "hubuum_schema_mutations_total",
        Counter,
        None,
        ["policy", "result"],
        &[],
        Process,
        "Schema staging and activation outcomes"
    ),
    metric!(
        "hubuum_schema_validation_objects_total",
        Counter,
        None,
        ["kind", "result"],
        &[],
        Process,
        "Committed schema inspection outcomes"
    ),
    metric!(
        "hubuum_schema_validation_duration_seconds",
        Histogram,
        Some("seconds"),
        ["kind", "status"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Schema validation work elapsed time"
    ),
    metric!(
        "hubuum_schema_compliance_objects",
        Gauge,
        None,
        ["status"],
        &[],
        Database,
        "Objects by effective active schema compliance"
    ),
    metric!(
        "hubuum_computed_field_rebuild_batches_total",
        Counter,
        None,
        ["items"],
        &[],
        Process,
        "Computed-field rebuild batches"
    ),
    metric!(
        "hubuum_computed_field_rebuild_completions_total",
        Counter,
        None,
        ["status"],
        &[],
        Process,
        "Computed-field rebuild terminal outcomes"
    ),
    metric!(
        "hubuum_computed_field_rebuild_duration_seconds",
        Histogram,
        Some("seconds"),
        ["status"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Computed-field rebuild duration"
    ),
    metric!(
        "hubuum_db_connection_acquire_duration_seconds",
        Histogram,
        Some("seconds"),
        ["caller"],
        LATENCY_BUCKETS_SECONDS,
        Process,
        "Database connection acquisition duration"
    ),
    metric!(
        "hubuum_db_connection_acquire_failures_total",
        Counter,
        None,
        ["caller"],
        &[],
        Process,
        "Database connection acquisition failures"
    ),
    metric!(
        "hubuum_db_operation_duration_seconds",
        Histogram,
        Some("seconds"),
        ["caller", "operation", "result"],
        LATENCY_BUCKETS_SECONDS,
        Process,
        "Database helper operation duration"
    ),
    metric!(
        "hubuum_db_operation_errors_total",
        Counter,
        None,
        ["caller", "operation", "result"],
        &[],
        Process,
        "Database helper operation failures"
    ),
    metric!(
        "hubuum_db_pool_connections",
        Gauge,
        None,
        ["state"],
        &[],
        Process,
        "Database pool connections by state"
    ),
    metric!(
        "hubuum_event_oldest_age_seconds",
        Gauge,
        Some("seconds"),
        ["queue"],
        &[],
        Database,
        "Oldest actionable event-queue item age"
    ),
    metric!(
        "hubuum_event_queue_items",
        Gauge,
        None,
        ["queue", "state"],
        &[],
        Database,
        "Event queue items by state"
    ),
    metric!(
        "hubuum_event_stale_claims",
        Gauge,
        None,
        ["queue"],
        &[],
        Database,
        "Stale event worker claims"
    ),
    metric!(
        "hubuum_event_worker_batch_size",
        Gauge,
        None,
        ["worker"],
        &[],
        Process,
        "Configured event-worker batch size"
    ),
    metric!(
        "hubuum_event_worker_lock_timeout_seconds",
        Gauge,
        Some("seconds"),
        ["worker"],
        &[],
        Process,
        "Configured event-worker claim timeout"
    ),
    metric!(
        "hubuum_event_worker_poll_interval_seconds",
        Gauge,
        Some("seconds"),
        ["worker"],
        &[],
        Process,
        "Configured event-worker poll interval"
    ),
    metric!(
        "hubuum_event_worker_wakeups_total",
        Counter,
        None,
        ["worker", "kind"],
        &[],
        Process,
        "Event-worker wakeups"
    ),
    metric!(
        "hubuum_event_workers_configured",
        Gauge,
        None,
        ["worker"],
        &[],
        Process,
        "Configured event workers"
    ),
    metric!(
        "hubuum_export_completions_total",
        Counter,
        None,
        ["scope", "content_type"],
        &[],
        Process,
        "Persisted export completions"
    ),
    metric!(
        "hubuum_export_duration_seconds",
        Histogram,
        Some("seconds"),
        ["template_id", "outcome"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Total export duration"
    ),
    metric!(
        "hubuum_export_phase_duration_seconds",
        Histogram,
        Some("seconds"),
        ["phase", "outcome"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Export phase duration"
    ),
    metric!(
        "hubuum_export_template_info",
        Gauge,
        None,
        ["template_id", "template_name"],
        &[],
        Database,
        "Current export-template identities"
    ),
    metric!(
        "hubuum_export_truncations_total",
        Counter,
        None,
        ["scope", "content_type"],
        &[],
        Process,
        "Persisted truncated exports"
    ),
    metric!(
        "hubuum_export_warnings_total",
        Counter,
        None,
        ["scope", "content_type"],
        &[],
        Process,
        "Warnings on persisted exports"
    ),
    metric!(
        "hubuum_extraction_failures_total",
        Counter,
        None,
        ["kind"],
        &[],
        Process,
        "Request extraction failures"
    ),
    metric!(
        "hubuum_http_request_duration_seconds",
        Histogram,
        Some("seconds"),
        ["method", "route", "status_family"],
        LATENCY_BUCKETS_SECONDS,
        Process,
        "HTTP request duration"
    ),
    metric!(
        "hubuum_http_requests_in_flight",
        Gauge,
        None,
        ["route"],
        &[],
        Process,
        "HTTP requests currently in flight"
    ),
    metric!(
        "hubuum_http_requests_total",
        Counter,
        None,
        ["method", "route", "status_code", "status_family"],
        &[],
        Process,
        "HTTP requests handled"
    ),
    metric!(
        "hubuum_import_failed_items_total",
        Counter,
        None,
        [],
        &[],
        Process,
        "Import items completed with failure"
    ),
    metric!(
        "hubuum_import_phase_duration_seconds",
        Histogram,
        Some("seconds"),
        ["phase", "outcome"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Import phase duration"
    ),
    metric!(
        "hubuum_import_processed_items_total",
        Counter,
        None,
        [],
        &[],
        Process,
        "Items processed by terminal imports"
    ),
    metric!(
        "hubuum_import_succeeded_items_total",
        Counter,
        None,
        [],
        &[],
        Process,
        "Import items completed successfully"
    ),
    metric!(
        "hubuum_inventory_entities",
        Gauge,
        None,
        ["entity_type"],
        &[],
        Database,
        "Current inventory entity counts"
    ),
    metric!(
        "hubuum_login_attempts_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "Login attempts"
    ),
    metric!(
        "hubuum_login_limiter_backend_failures_total",
        Counter,
        None,
        ["backend", "operation"],
        &[],
        Process,
        "Shared login-limiter failures",
        feature = "login-rate-limit-valkey"
    ),
    metric!(
        "hubuum_login_limiter_entries",
        Gauge,
        None,
        ["state"],
        &[],
        Process,
        "Local login-limiter entries"
    ),
    metric!(
        "hubuum_login_lockouts_total",
        Counter,
        None,
        ["scope"],
        &[],
        Process,
        "Login limiter lockout transitions"
    ),
    metric!(
        "hubuum_metrics_refresh_duration_seconds",
        Gauge,
        Some("seconds"),
        ["source"],
        &[],
        Process,
        "Duration of the latest metrics refresh"
    ),
    metric!(
        "hubuum_metrics_refresh_failures_total",
        Counter,
        None,
        ["source"],
        &[],
        Process,
        "Best-effort metrics refresh failures"
    ),
    metric!(
        "hubuum_metrics_refresh_last_success_timestamp_seconds",
        Gauge,
        Some("seconds"),
        ["source"],
        &[],
        Process,
        "Latest successful metrics refresh timestamp"
    ),
    metric!(
        "hubuum_metrics_refresh_skipped_total",
        Counter,
        None,
        ["source", "reason"],
        &[],
        Process,
        "Skipped concurrent metrics refreshes"
    ),
    metric!(
        "hubuum_process_start_time_seconds",
        Gauge,
        Some("seconds"),
        [],
        &[],
        Process,
        "Time when Hubuum initialized metrics"
    ),
    metric!(
        "hubuum_remote_call_duration_seconds",
        Histogram,
        Some("seconds"),
        ["method", "status_family", "outcome"],
        OUTBOUND_BUCKETS_SECONDS,
        Process,
        "Remote HTTP execution duration"
    ),
    metric!(
        "hubuum_remote_call_results_total",
        Counter,
        None,
        ["method", "status_family", "outcome"],
        &[],
        Process,
        "Remote HTTP execution outcomes"
    ),
    metric!(
        "hubuum_template_worker_events_total",
        Counter,
        None,
        ["event"],
        &[],
        Process,
        "Template worker lifecycle events"
    ),
    metric!(
        "hubuum_template_worker_duration_seconds",
        Histogram,
        Some("seconds"),
        ["event"],
        LATENCY_BUCKETS_SECONDS,
        Process,
        "Template worker terminal operation duration"
    ),
    metric!(
        "hubuum_revision_conditions_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "Conditional-write outcomes"
    ),
    metric!(
        "hubuum_runtime_info",
        Gauge,
        None,
        ["role"],
        &[],
        Process,
        "Runtime role for the process"
    ),
    metric!(
        "hubuum_secret_resolution_duration_seconds",
        Histogram,
        Some("seconds"),
        ["provider", "consumer", "outcome"],
        LATENCY_BUCKETS_SECONDS,
        Process,
        "Secret resolution duration"
    ),
    metric!(
        "hubuum_secret_resolutions_total",
        Counter,
        None,
        ["provider", "consumer", "outcome"],
        &[],
        Process,
        "Secret resolution outcomes"
    ),
    metric!(
        "hubuum_secret_source_info",
        Gauge,
        None,
        ["provider"],
        &[],
        Process,
        "Selected secret provider"
    ),
    metric!(
        "hubuum_storage_backend_info",
        Gauge,
        None,
        ["backend"],
        &[],
        Process,
        "Selected storage backend"
    ),
    metric!(
        "hubuum_storage_operation_duration_seconds",
        Histogram,
        Some("seconds"),
        ["backend", "capability", "operation", "result"],
        LATENCY_BUCKETS_SECONDS,
        Process,
        "Logical storage operation duration"
    ),
    metric!(
        "hubuum_storage_operation_errors_total",
        Counter,
        None,
        ["backend", "capability", "operation", "result"],
        &[],
        Process,
        "Logical storage operation failures"
    ),
    metric!(
        "hubuum_task_cancellation_requests_total",
        Counter,
        None,
        ["kind", "result"],
        &[],
        Process,
        "Durable cancellation requests by task kind and queued, active or unchanged result"
    ),
    metric!(
        "hubuum_task_stop_acknowledgements_total",
        Counter,
        None,
        ["kind", "reason"],
        &[],
        Process,
        "Acknowledged task cancellation and deadline expiration"
    ),
    metric!(
        "hubuum_task_cancellation_acknowledgement_duration_seconds",
        Histogram,
        Some("seconds"),
        ["kind", "reason"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Time from cancellation request to terminal acknowledgement"
    ),
    metric!(
        "hubuum_task_ambiguous_remote_stops_total",
        Counter,
        None,
        ["reason"],
        &[],
        Process,
        "Remote tasks stopped after dispatch may have produced external effects"
    ),
    metric!(
        "hubuum_task_claims_total",
        Counter,
        None,
        ["kind"],
        &[],
        Process,
        "Tasks claimed by workers"
    ),
    metric!(
        "hubuum_task_completions_total",
        Counter,
        None,
        ["kind", "final_status"],
        &[],
        Process,
        "Tasks reaching a terminal status"
    ),
    metric!(
        "hubuum_task_execution_duration_seconds",
        Histogram,
        Some("seconds"),
        ["kind", "final_status"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Task execution duration"
    ),
    metric!(
        "hubuum_task_last_terminal_timestamp_seconds",
        Gauge,
        Some("seconds"),
        ["kind", "status"],
        &[],
        Database,
        "Most recent terminal-task timestamp"
    ),
    metric!(
        "hubuum_task_lease_recoveries_total",
        Counter,
        None,
        ["kind"],
        &[],
        Process,
        "Tasks failed after lease expiry"
    ),
    metric!(
        "hubuum_task_oldest_age_seconds",
        Gauge,
        Some("seconds"),
        ["kind", "state"],
        &[],
        Database,
        "Oldest queued or active task age"
    ),
    metric!(
        "hubuum_task_output_cleanup_deleted_total",
        Counter,
        None,
        ["kind"],
        &[],
        Process,
        "Stored outputs deleted by cleanup"
    ),
    metric!(
        "hubuum_task_output_cleanup_failures_total",
        Counter,
        None,
        ["kind"],
        &[],
        Process,
        "Stored output cleanup failures"
    ),
    metric!(
        "hubuum_task_output_cleanup_runs_total",
        Counter,
        None,
        ["kind"],
        &[],
        Process,
        "Stored output cleanup runs"
    ),
    metric!(
        "hubuum_task_poll_interval_seconds",
        Gauge,
        Some("seconds"),
        [],
        &[],
        Process,
        "Configured task-worker poll interval"
    ),
    metric!(
        "hubuum_task_queue_wait_duration_seconds",
        Histogram,
        Some("seconds"),
        ["kind"],
        BACKGROUND_BUCKETS_SECONDS,
        Process,
        "Task queue wait duration"
    ),
    metric!(
        "hubuum_task_worker_iterations_total",
        Counter,
        None,
        ["outcome"],
        &[],
        Process,
        "Task worker loop iterations"
    ),
    metric!(
        "hubuum_task_workers_configured",
        Gauge,
        None,
        [],
        &[],
        Process,
        "Configured task workers"
    ),
    metric!(
        "hubuum_tasks",
        Gauge,
        None,
        ["kind", "status"],
        &[],
        Database,
        "Current task counts"
    ),
    metric!(
        "hubuum_token_authentications_total",
        Counter,
        None,
        ["format", "key_state", "outcome"],
        &[],
        Process,
        "Bearer-token authentication outcomes"
    ),
    metric!(
        "hubuum_token_hash_key_info",
        Gauge,
        None,
        ["mode", "active_key_id", "ring_identity"],
        &[],
        Process,
        "Token hash key-ring identity"
    ),
    metric!(
        "hubuum_token_hash_keys",
        Gauge,
        None,
        ["state"],
        &[],
        Process,
        "Configured token hash keys"
    ),
    metric!(
        "hubuum_token_hash_stored",
        Gauge,
        None,
        ["key_state", "lifecycle"],
        &[],
        Database,
        "Stored bearer tokens by key lifecycle"
    ),
    metric!(
        "process_cpu_seconds_total",
        Counter,
        Some("seconds"),
        [],
        &[],
        Process,
        "Process user and system CPU time"
    ),
    metric!(
        "process_max_fds",
        Gauge,
        None,
        [],
        &[],
        Process,
        "Process file-descriptor limit"
    ),
    metric!(
        "process_open_fds",
        Gauge,
        None,
        [],
        &[],
        Process,
        "Open file descriptors or handles"
    ),
    metric!(
        "process_resident_memory_bytes",
        Gauge,
        Some("bytes"),
        [],
        &[],
        Process,
        "Resident process memory"
    ),
    metric!(
        "process_start_time_seconds",
        Gauge,
        Some("seconds"),
        [],
        &[],
        Process,
        "Operating-system process start time"
    ),
    metric!(
        "process_virtual_memory_bytes",
        Gauge,
        Some("bytes"),
        [],
        &[],
        Process,
        "Virtual process memory"
    ),
];

impl MetricDefinition {
    pub(crate) fn runtime_name(&self) -> &'static str {
        if self.name.starts_with("process_") || self.name == "hubuum_export_template_info" {
            return self.name;
        }
        if matches!(self.kind, MetricKind::Counter) {
            return self.name.strip_suffix("_total").unwrap_or(self.name);
        }
        if self.unit == Some("seconds") {
            return self.name.strip_suffix("_seconds").unwrap_or(self.name);
        }
        self.name
    }

    pub(crate) fn open_telemetry_unit(&self) -> Option<&'static str> {
        match self.unit {
            Some("seconds") => Some("s"),
            Some(unit) => Some(unit),
            None => None,
        }
    }
}

pub(crate) fn metric_definition(name: &str) -> &'static MetricDefinition {
    METRICS
        .iter()
        .find(|metric| metric.name == name)
        .unwrap_or_else(|| panic!("missing operational metric definition for {name}"))
}

pub(crate) fn runtime_metric_definition(name: &str) -> &'static MetricDefinition {
    METRICS
        .iter()
        .find(|metric| metric.runtime_name() == name)
        .unwrap_or_else(|| {
            panic!("missing operational metric definition for runtime metric {name}")
        })
}
