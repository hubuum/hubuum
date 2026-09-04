use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::OnceLock;

use clap::{Arg, ArgAction, Command};
use hubuum_events_core::{
    AuditDocument, BASE_AUDIT_DOCUMENT_SCHEMA_VERSION, NewEvent,
    REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::config::environment::{
    APP_CONFIG_ENVIRONMENT, DYNAMIC_SECRET_PREFIXES, EnvironmentOwner, EnvironmentVariable,
    Exposure, PROCESS_ENVIRONMENT, configuration_bounds, configuration_constraints,
};
use crate::events::{Action, ActorKind, EntityType, valid_actions};
use crate::models::{
    BackupDocument, BackupManifest, BackupState, CURRENT_BACKUP_VERSION, CURRENT_IMPORT_VERSION,
    ExportContentType, ExportMissingDataPolicy, ExportScopeKind, ExportTemplateKind, ImportGraph,
    ImportRequest, RemoteHttpMethod,
};
use crate::storage::{
    StorageBackupHistorySection, StorageBackupStateSection, StorageCallSite, StorageCapability,
    StorageErrorKind, StorageTaskKind, StorageTaskStatus,
};

const CONTRACT_SCHEMA_VERSION: u32 = 1;
const HTTP_METHOD_LABEL_VALUES: &[&str] = &[
    "CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "OTHER", "PATCH", "POST", "PUT", "TRACE",
];
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
const STORAGE_OPERATION_SOURCES: &[&str] = &[
    include_str!("storage/observed.rs"),
    include_str!("storage/context/api.rs"),
    include_str!("storage/context/computed_fields.rs"),
    include_str!("storage/context/events.rs"),
    include_str!("storage/context/execution.rs"),
    include_str!("storage/context/identity.rs"),
    include_str!("storage/context/identity_queries.rs"),
    include_str!("storage/context/operational.rs"),
    include_str!("storage/context/queries.rs"),
    include_str!("storage/context/relations.rs"),
    include_str!("storage/context/tasks.rs"),
    include_str!("storage/context/transaction.rs"),
    include_str!("storage/context/workflows.rs"),
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

pub(crate) const METRICS: &[MetricDefinition] = &[
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

#[derive(Serialize)]
struct MetricLabelContract {
    name: &'static str,
    bounded_by: &'static str,
    values: Vec<String>,
}

#[derive(Serialize)]
struct MetricContract {
    name: &'static str,
    kind: MetricKind,
    unit: Option<&'static str>,
    labels: Vec<MetricLabelContract>,
    histogram_buckets: &'static [f64],
    scope: MetricScope,
    description: &'static str,
    feature: Option<&'static str>,
}

#[derive(Serialize)]
struct ConfigurationContract {
    name: String,
    owner: String,
    exposure: String,
    value_kind: String,
    default_is_set: bool,
    default: Vec<String>,
    dynamic_default: Option<DynamicDefaultContract>,
    allowed_values: Vec<String>,
    minimum: Option<i64>,
    maximum: Option<i64>,
    runtime_roles: Vec<&'static str>,
    appears_in_running_configuration: bool,
    source: &'static str,
    dynamic_prefix: bool,
}

#[derive(Serialize)]
struct CliOptionContract {
    id: String,
    long: Option<String>,
    short: Option<char>,
    environment: Option<String>,
    value_kind: String,
    value_count: Option<ValueCount>,
    default: Vec<String>,
    dynamic_default: Option<DynamicDefaultContract>,
    allowed_values: Vec<String>,
    required: bool,
    conflicts_with: Vec<String>,
    requires: Vec<String>,
}

#[derive(Serialize)]
struct ValueCount {
    minimum: usize,
    maximum: usize,
}

#[derive(Clone, Copy, Serialize)]
struct DynamicDefaultContract {
    source: &'static str,
    divisor: usize,
    rounding: &'static str,
    minimum: usize,
}

#[derive(Serialize)]
struct CliContract {
    command: &'static str,
    options: Vec<CliOptionContract>,
    stable_output_modes: &'static [&'static str],
    exit_codes: BTreeMap<&'static str, i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FieldContract {
    name: String,
    nullable: bool,
}

#[derive(Serialize)]
struct EventContract {
    schema_version: i32,
    revision_aware_schema_version: i32,
    envelope_fields: Vec<FieldContract>,
    provenance_fields: Vec<FieldContract>,
    sink_payload_fields: Vec<FieldContract>,
    schema_version_semantics: &'static [&'static str],
    actors: Vec<&'static str>,
    entities: Vec<EventEntityContract>,
    redaction_rules: &'static [&'static str],
    audit_document_versions: &'static [i32],
}

#[derive(Serialize)]
struct EventEntityContract {
    name: &'static str,
    actions: Vec<&'static str>,
}

#[derive(Serialize)]
struct VersionedDocumentContract {
    version: Option<i32>,
    required_fields: Vec<String>,
    optional_fields: Vec<String>,
    sections: Vec<String>,
    rejection_policy: &'static str,
}

#[derive(Serialize)]
struct ExportContract {
    scope_kinds: Vec<&'static str>,
    content_types: Vec<&'static str>,
    missing_data_policies: Vec<&'static str>,
    template_kinds: Vec<&'static str>,
}

#[derive(Serialize)]
struct DocumentContracts {
    backup: VersionedDocumentContract,
    import: VersionedDocumentContract,
    export: ExportContract,
}

#[derive(Serialize)]
struct OperationalContract {
    schema_version: u32,
    release: &'static str,
    metrics: Vec<MetricContract>,
    configuration: Vec<ConfigurationContract>,
    configuration_constraints: Vec<String>,
    events: EventContract,
    documents: DocumentContracts,
    cli: Vec<CliContract>,
    compatibility_policy: BTreeMap<&'static str, &'static str>,
}

pub(crate) fn generate_json() -> String {
    let contract = build_contract();
    format!(
        "{}\n",
        serde_json::to_string_pretty(&contract)
            .expect("operational contract contains only serializable values")
    )
}

pub(crate) fn generate_metrics_markdown() -> String {
    let mut output = String::from(
        "# Generated Metric Reference\n\nThis file is generated from the typed operational-contract registry. Do not edit it by hand.\n\n",
    );
    output.push_str("| Metric | Type | Unit | Scope | Labels | Description |\n");
    output.push_str("| --- | --- | --- | --- | --- | --- |\n");
    for metric in metric_contracts() {
        let labels = if metric.labels.is_empty() {
            "none".to_string()
        } else {
            metric
                .labels
                .iter()
                .map(|label| format!("`{}`", label.name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        output.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} |\n",
            metric.name,
            enum_name(metric.kind),
            metric.unit.unwrap_or("none"),
            enum_name(metric.scope),
            labels,
            metric.description,
        ));
    }
    output.push_str("\n## Histogram Buckets\n\n");
    for (name, buckets) in [
        ("latency", LATENCY_BUCKETS_SECONDS),
        ("outbound", OUTBOUND_BUCKETS_SECONDS),
        ("background", BACKGROUND_BUCKETS_SECONDS),
    ] {
        output.push_str(&format!(
            "- **{name}:** {} seconds\n",
            buckets
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    output
}

fn build_contract() -> OperationalContract {
    validate_metrics(METRICS).expect("invalid metric operational contract");
    let mut compatibility_policy = BTreeMap::new();
    compatibility_policy.insert(
        "additive",
        "new optional fields, commands, variables, metrics, or catalog values",
    );
    compatibility_policy.insert(
        "behavioral",
        "changed defaults, descriptions, scopes, or expanded bounded value domains",
    );
    compatibility_policy.insert("breaking", "removed or narrowed items, changed types, labels, buckets, requirements, or unversioned format shapes");

    OperationalContract {
        schema_version: CONTRACT_SCHEMA_VERSION,
        release: env!("CARGO_PKG_VERSION"),
        metrics: metric_contracts(),
        configuration: configuration_contracts(),
        configuration_constraints: configuration_constraints(),
        events: event_contract(),
        documents: document_contracts(),
        cli: vec![
            cli_contract("hubuum-server", crate::config::app_command()),
            cli_contract("hubuum-admin", crate::administration::admin_command()),
        ],
        compatibility_policy,
    }
}

fn metric_contracts() -> Vec<MetricContract> {
    let mut metrics = METRICS
        .iter()
        .map(|metric| MetricContract {
            name: metric.name,
            kind: metric.kind,
            unit: metric.unit,
            labels: metric
                .labels
                .iter()
                .map(|label| metric_label_contract(metric.name, label))
                .collect(),
            histogram_buckets: metric.buckets,
            scope: metric.scope,
            description: metric.description,
            feature: metric.feature,
        })
        .collect::<Vec<_>>();
    metrics.sort_by_key(|metric| metric.name);
    metrics
}

fn metric_label_contract(metric: &str, label: &'static str) -> MetricLabelContract {
    let strings = |values: &[&str]| {
        values
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>()
    };
    let storage_results = || {
        std::iter::once("ok".to_string())
            .chain(
                StorageErrorKind::ALL
                    .iter()
                    .map(|kind| kind.as_str().to_string()),
            )
            .collect::<Vec<_>>()
    };
    let values = match (metric, label) {
        ("hubuum_api_errors_total", "class") => strings(&[
            "unauthorized",
            "internal_server_error",
            "forbidden",
            "not_acceptable",
            "unsupported_media_type",
            "payload_too_large",
            "database_error",
            "conflict",
            "precondition_failed",
            "revision_conflict",
            "too_many_requests",
            "service_unavailable",
            "not_implemented",
            "permission_backend_unavailable",
            "not_found",
            "gone",
            "db_connection_error",
            "hash_error",
            "bad_request",
            "operator_mismatch",
            "invalid_integer_range",
            "validation_error",
        ]),
        ("hubuum_client_allowlist_rejections_total", "reason") => {
            strings(&["disallowed_ip", "missing_ip"])
        }
        (name, "scope") if name.starts_with("hubuum_computed_field") => {
            strings(&["shared", "personal", "preview"])
        }
        ("hubuum_computed_field_errors_total", "code") => strings(&[
            "input_too_large",
            "non_numeric_operand",
            "non_integer_result",
            "result_type_mismatch",
            "numeric_out_of_range",
            "evaluation_limit_exceeded",
            "result_too_large",
        ]),
        ("hubuum_computed_field_evaluations_total", "outcome") => {
            strings(&["success", "field_error"])
        }
        ("hubuum_computed_field_read_repairs_total", "outcome") => strings(&["success", "failure"]),
        ("hubuum_computed_field_rebuild_batches_total", "items") => {
            strings(&["empty", "non_empty"])
        }
        (name, "status") if name.starts_with("hubuum_computed_field_rebuild") => {
            strings(&["succeeded", "failed", "cancelled"])
        }
        (name, "caller") if name.starts_with("hubuum_db_") => StorageCallSite::ALL
            .iter()
            .map(|call_site| call_site.as_str().to_string())
            .collect(),
        (name, "operation") if name.starts_with("hubuum_db_operation") => {
            strings(&["connection", "task_lease_connection", "transaction"])
        }
        (name, "result") if name.starts_with("hubuum_db_operation") => storage_results(),
        ("hubuum_db_pool_connections", "state") => {
            strings(&["configured", "open", "idle", "checked_out"])
        }
        ("hubuum_event_queue_items", "state") => strings(&[
            "total",
            "pending",
            "in_flight",
            "succeeded",
            "failed",
            "dead",
            "retryable",
        ]),
        (name, "scope") if name.starts_with("hubuum_export") => ExportScopeKind::ALL
            .iter()
            .copied()
            .map(ExportScopeKind::as_str)
            .map(str::to_string)
            .collect(),
        (name, "outcome") if name.starts_with("hubuum_export") => {
            strings(&["success", "error", "timeout"])
        }
        (name, "outcome") if name.starts_with("hubuum_import") => {
            strings(&["success", "failed", "partially_succeeded", "error"])
        }
        ("hubuum_extraction_failures_total", "kind") => strings(&["json", "path"]),
        ("hubuum_inventory_entities", "entity_type") => strings(&[
            "collections",
            "classes",
            "objects",
            "users",
            "groups",
            "service_accounts",
            "remote_targets",
        ]),
        ("hubuum_login_attempts_total", "outcome") => strings(&[
            "success",
            "bad_credentials",
            "rate_limited",
            "internal_error",
        ]),
        ("hubuum_login_limiter_backend_failures_total", "backend") => strings(&["valkey"]),
        ("hubuum_login_limiter_backend_failures_total", "operation") => {
            strings(&["begin", "finish", "snapshot", "release", "clear"])
        }
        ("hubuum_login_lockouts_total", "scope") => {
            strings(&["principal_ip", "ip", "subnet", "unknown"])
        }
        ("hubuum_metrics_refresh_skipped_total", "reason") => strings(&["concurrent"]),
        (name, "outcome") if name.starts_with("hubuum_remote_call") => strings(&[
            "success",
            "failure",
            "timeout",
            "private_target_rejected",
            "validation_rejected",
        ]),
        (name, "method") if name.starts_with("hubuum_remote_call") => RemoteHttpMethod::ALL
            .iter()
            .copied()
            .map(RemoteHttpMethod::as_str)
            .map(str::to_string)
            .collect(),
        (name, "consumer") if name.starts_with("hubuum_secret_resolution") => strings(&[
            "database",
            "event_sink",
            "remote_target",
            "ldap",
            "token_hash",
        ]),
        (name, "outcome") if name.starts_with("hubuum_secret_resolution") => strings(&[
            "ok",
            "not_found",
            "permission_denied",
            "too_large",
            "unsafe_path",
            "changed_during_read",
            "invalid",
            "unavailable",
        ]),
        (name, "capability") if name.starts_with("hubuum_storage_operation") => {
            StorageCapability::ALL
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect()
        }
        (name, "operation") if name.starts_with("hubuum_storage_operation") => {
            storage_operation_values()
        }
        (name, "result") if name.starts_with("hubuum_storage_operation") => storage_results(),
        (name, "kind") if name.starts_with("hubuum_task_output_cleanup") => {
            strings(&["export", "backup"])
        }
        (name, "kind") if name.starts_with("hubuum_task") || name == "hubuum_tasks" => {
            StorageTaskKind::ALL
                .iter()
                .map(|kind| kind.as_str().to_string())
                .collect()
        }
        (name, "final_status") if name.starts_with("hubuum_task_") => StorageTaskStatus::TERMINAL
            .iter()
            .map(|status| status.as_str().to_string())
            .collect(),
        ("hubuum_task_last_terminal_timestamp_seconds", "status") => StorageTaskStatus::TERMINAL
            .iter()
            .map(|status| status.as_str().to_string())
            .collect(),
        ("hubuum_tasks", "status") => StorageTaskStatus::ALL
            .iter()
            .map(|status| status.as_str().to_string())
            .collect(),
        ("hubuum_task_oldest_age_seconds", "state") => strings(&["queued", "active"]),
        ("hubuum_token_authentications_total", "format") => {
            strings(&["legacy", "version1", "versioned_unknown"])
        }
        ("hubuum_token_authentications_total", "key_state") => {
            strings(&["active", "previous", "legacy", "unknown"])
        }
        ("hubuum_token_authentications_total", "outcome") => {
            strings(&["success", "migrated", "migration_conflict", "rejected"])
        }
        ("hubuum_token_hash_key_info", "mode") => strings(&["stable", "ephemeral"]),
        ("hubuum_token_hash_keys", "state") => strings(&["active", "previous"]),
        ("hubuum_token_hash_stored", "key_state") => {
            strings(&["active", "previous", "legacy", "unconfigured"])
        }
        ("hubuum_token_hash_stored", "lifecycle") => strings(&["active", "revoked", "expired"]),
        (_, "role") => strings(&["all", "api", "worker"]),
        (_, "backend") if metric.starts_with("hubuum_storage") => {
            strings(&["memory", "postgresql"])
        }
        (_, "provider") => strings(&["environment", "file"]),
        (name, "source") if name.starts_with("hubuum_metrics_refresh") => [
            "database",
            "events",
            "inventory",
            "login_limiter",
            "process",
            "tasks",
            "token_keys",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        (name, "worker") if name.starts_with("hubuum_event_worker") => {
            strings(&["delivery", "fanout"])
        }
        (name, "queue") if name.starts_with("hubuum_event") => strings(&["delivery", "fanout"]),
        ("hubuum_event_worker_wakeups_total", "kind") => {
            strings(&["notification", "notifications_sent", "poll"])
        }
        (name, "phase") if name.starts_with("hubuum_export") => {
            strings(&["total", "query", "hydration", "render"])
        }
        (name, "phase") if name.starts_with("hubuum_import") => {
            strings(&["total", "planning", "execution"])
        }
        (name, "content_type") if name.starts_with("hubuum_export") => {
            strings(&["application/json", "text/plain", "text/html", "text/csv"])
        }
        ("hubuum_revision_conditions_total", "outcome") => strings(&[
            "matched",
            "wildcard",
            "stale",
            "unconditional",
            "malformed",
            "async_stale",
            "invariant_failure",
        ]),
        ("hubuum_login_limiter_entries", "state") => strings(&["active", "locked"]),
        ("hubuum_task_worker_iterations_total", "outcome") => {
            strings(&["claimed", "idle", "error"])
        }
        (_, "status_family") => strings(&["none", "unknown", "1xx", "2xx", "3xx", "4xx", "5xx"]),
        (_, "method") => strings(HTTP_METHOD_LABEL_VALUES),
        _ => Vec::new(),
    };
    let bounded_by = match (metric, label) {
        ("hubuum_export_template_info", "template_id" | "template_name")
        | ("hubuum_export_duration_seconds", "template_id") => {
            "current stored export-template catalog; explicit cardinality exception"
        }
        (_, "route") => "registered route templates and fixed coarse route groups",
        (_, "status_code") => "valid HTTP status-code range",
        (_, "git_sha" | "version" | "active_key_id" | "ring_identity") => {
            "one configured process identity"
        }
        _ => "enumerated values",
    };
    assert!(
        !values.is_empty() || bounded_by != "enumerated values",
        "metric {metric} label {label} claims an enumerated domain without values"
    );
    MetricLabelContract {
        name: label,
        bounded_by,
        values,
    }
}

static ENUMERATED_METRIC_LABEL_DOMAINS: OnceLock<
    BTreeMap<&'static str, BTreeMap<&'static str, BTreeSet<String>>>,
> = OnceLock::new();

pub(crate) fn metric_label_value_is_allowed(metric: &str, label: &str, value: &str) -> bool {
    let domains = ENUMERATED_METRIC_LABEL_DOMAINS.get_or_init(|| {
        let mut domains = BTreeMap::<_, BTreeMap<_, _>>::new();
        for metric in METRICS {
            for label in metric.labels {
                let contract = metric_label_contract(metric.name, label);
                if contract.bounded_by == "enumerated values" {
                    domains
                        .entry(metric.name)
                        .or_default()
                        .insert(*label, contract.values.into_iter().collect::<BTreeSet<_>>());
                }
            }
        }
        domains
    });

    domains
        .get(metric)
        .and_then(|labels| labels.get(label))
        .is_none_or(|values| values.contains(value))
}

pub(crate) fn http_method_metric_label(method: &str) -> &str {
    if HTTP_METHOD_LABEL_VALUES.contains(&method) {
        method
    } else {
        "OTHER"
    }
}

fn storage_operation_values() -> Vec<String> {
    STORAGE_OPERATION_SOURCES
        .iter()
        .flat_map(|source| {
            first_quoted_strings_after(source, "self.observe_storage_call(")
                .chain(first_quoted_strings_after(source, "self.call("))
        })
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn first_quoted_strings_after<'a>(
    source: &'a str,
    marker: &'a str,
) -> impl Iterator<Item = &'a str> {
    source.match_indices(marker).filter_map(move |(index, _)| {
        let remainder = &source[index + marker.len()..];
        let start = remainder.find('"')? + 1;
        remainder[start..].split('"').next()
    })
}

fn validate_metrics(metrics: &[MetricDefinition]) -> Result<(), String> {
    let mut names = BTreeSet::new();
    let forbidden_labels = [
        "client_ip",
        "error",
        "error_message",
        "idempotency_key",
        "object_id",
        "raw_path",
        "task_id",
        "user_id",
        "username",
    ];
    for metric in metrics {
        if !names.insert(metric.name) {
            return Err(format!("duplicate metric contract: {}", metric.name));
        }
        if matches!(metric.kind, MetricKind::Histogram) && metric.buckets.is_empty() {
            return Err(format!(
                "histogram has no explicit buckets: {}",
                metric.name
            ));
        }
        if !matches!(metric.kind, MetricKind::Histogram) && !metric.buckets.is_empty() {
            return Err(format!("non-histogram declares buckets: {}", metric.name));
        }
        for label in metric.labels {
            if forbidden_labels.contains(label) {
                return Err(format!(
                    "metric {} uses forbidden high-cardinality label {label}",
                    metric.name
                ));
            }
        }
    }
    Ok(())
}

fn configuration_contracts() -> Vec<ConfigurationContract> {
    let server = command_environment(crate::config::app_command());
    let admin = command_environment(crate::administration::admin_command());
    let mut contracts = APP_CONFIG_ENVIRONMENT
        .iter()
        .map(|variable| {
            configuration_contract(
                variable,
                server.get(variable.name),
                "server",
                false,
                admin.contains_key(variable.name),
            )
        })
        .chain(PROCESS_ENVIRONMENT.iter().map(|variable| {
            let argument = server
                .get(variable.name)
                .or_else(|| admin.get(variable.name));
            configuration_contract(
                variable,
                argument,
                "process",
                false,
                admin.contains_key(variable.name) || admin_process_environment(variable.name),
            )
        }))
        .collect::<Vec<_>>();
    contracts.extend(
        DYNAMIC_SECRET_PREFIXES
            .iter()
            .map(|(prefix, owner)| ConfigurationContract {
                name: format!("{prefix}*"),
                owner: owner_name(*owner),
                exposure: "secret".to_string(),
                value_kind: "string".to_string(),
                default_is_set: false,
                default: Vec::new(),
                dynamic_default: None,
                allowed_values: Vec::new(),
                minimum: None,
                maximum: None,
                runtime_roles: vec!["all", "api", "worker", "admin"],
                appears_in_running_configuration: false,
                source: "dynamic secret namespace",
                dynamic_prefix: true,
            }),
    );
    contracts.sort_by(|left, right| left.name.cmp(&right.name));
    contracts
}

fn configuration_contract(
    variable: &EnvironmentVariable,
    argument: Option<&CliArgumentMetadata>,
    source: &'static str,
    dynamic_prefix: bool,
    admin_applicable: bool,
) -> ConfigurationContract {
    let (minimum, maximum) = configuration_bounds(variable.name);
    let dynamic_default = dynamic_default(variable.name);
    let metadata = argument
        .cloned()
        .or_else(|| process_environment_metadata(variable.name));
    let parsed_default = metadata
        .as_ref()
        .map(|metadata| metadata.default.clone())
        .unwrap_or_default();
    ConfigurationContract {
        name: variable.name.to_string(),
        owner: owner_name(variable.owner),
        exposure: exposure_name(variable.exposure).to_string(),
        value_kind: metadata
            .as_ref()
            .map(|metadata| metadata.value_kind.clone())
            .unwrap_or_else(|| "string".to_string()),
        default_is_set: dynamic_default.is_some() || !parsed_default.is_empty(),
        default: if matches!(variable.exposure, Exposure::Secret) || dynamic_default.is_some() {
            Vec::new()
        } else {
            parsed_default
        },
        dynamic_default,
        allowed_values: metadata
            .as_ref()
            .map(|metadata| metadata.allowed_values.clone())
            .unwrap_or_default(),
        minimum,
        maximum,
        runtime_roles: runtime_roles(variable.name, admin_applicable),
        appears_in_running_configuration: appears_in_running_configuration(variable.name),
        source,
        dynamic_prefix,
    }
}

#[derive(Clone)]
struct CliArgumentMetadata {
    value_kind: String,
    default: Vec<String>,
    allowed_values: Vec<String>,
}

fn process_environment_metadata(name: &str) -> Option<CliArgumentMetadata> {
    let (value_kind, default, allowed_values): (&str, &[&str], &[&str]) = match name {
        "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY" => {
            ("boolean", &["false"], &["true", "false", "1", "0"])
        }
        "HUBUUM_SECRET_SOURCE" => ("enum", &["environment"], &["environment", "file"]),
        "HUBUUM_SECRET_FILE_ROOT" | "HUBUUM_AUTH_CONFIG_HOST_PATH" => ("path", &[], &[]),
        _ => return None,
    };
    Some(CliArgumentMetadata {
        value_kind: value_kind.to_string(),
        default: default.iter().map(|value| (*value).to_string()).collect(),
        allowed_values: allowed_values
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    })
}

fn command_environment(mut command: Command) -> BTreeMap<String, CliArgumentMetadata> {
    command.build();
    command
        .get_arguments()
        .filter_map(|argument| {
            let environment = argument.get_env()?.to_string_lossy().into_owned();
            Some((
                environment,
                CliArgumentMetadata {
                    value_kind: argument_value_kind(argument).to_string(),
                    default: os_values(argument.get_default_values()),
                    allowed_values: argument
                        .get_possible_values()
                        .iter()
                        .map(|value| value.get_name().to_string())
                        .collect(),
                },
            ))
        })
        .collect()
}

fn cli_contract(command_name: &'static str, mut command: Command) -> CliContract {
    command.build();
    validate_cli_requirements(command_name, &command);
    let options = command
        .get_arguments()
        .filter(|argument| !matches!(argument.get_action(), ArgAction::Help | ArgAction::Version))
        .map(|argument| {
            let value_count = argument.get_num_args().map(|range| ValueCount {
                minimum: range.min_values(),
                maximum: range.max_values(),
            });
            let mut conflicts_with = command
                .get_arg_conflicts_with(argument)
                .iter()
                .map(|conflict| conflict.get_id().to_string())
                .collect::<Vec<_>>();
            conflicts_with.sort();
            let environment = argument
                .get_env()
                .map(|environment| environment.to_string_lossy().into_owned());
            let dynamic_default = environment.as_deref().and_then(dynamic_default);
            CliOptionContract {
                id: argument.get_id().to_string(),
                long: argument.get_long().map(ToString::to_string),
                short: argument.get_short(),
                environment,
                value_kind: argument_value_kind(argument).to_string(),
                value_count,
                default: if dynamic_default.is_some() {
                    Vec::new()
                } else {
                    os_values(argument.get_default_values())
                },
                dynamic_default,
                allowed_values: argument
                    .get_possible_values()
                    .iter()
                    .map(|value| value.get_name().to_string())
                    .collect(),
                required: argument.is_required_set(),
                conflicts_with,
                requires: cli_requirements(command_name, argument.get_id().as_str())
                    .iter()
                    .map(|requirement| (*requirement).to_string())
                    .collect(),
            }
        })
        .collect();
    CliContract {
        command: command_name,
        options,
        stable_output_modes: if command_name == "hubuum-admin" {
            &[
                "--json",
                "--database-role-setup-sql",
                "--database-role-grants-sql",
            ]
        } else {
            &[]
        },
        exit_codes: cli_exit_codes(command_name),
    }
}

fn validate_cli_requirements(command_name: &str, command: &Command) {
    let mut command = command
        .clone()
        .mut_args(|argument| argument.env(None::<&str>));
    command.build();
    let command_requirements = command
        .get_arguments()
        .filter(|argument| argument.is_required_set())
        .flat_map(|argument| {
            let id = argument.get_id().as_str();
            std::iter::once(id).chain(cli_requirement_closure(command_name, id))
        })
        .collect::<BTreeSet<_>>();
    // Disable command-wide requirements only when checking option-specific
    // dependencies, so an always-present option cannot hide registry drift.
    let dependency_command = command
        .clone()
        .mut_args(|argument| argument.required(false));
    for argument in command
        .get_arguments()
        .filter(|argument| !matches!(argument.get_action(), ArgAction::Help | ArgAction::Version))
        .filter(|argument| argument.get_long().is_some() || argument.get_short().is_some())
    {
        let argument_id = argument.get_id().as_str();
        let requirements = cli_requirement_closure(command_name, argument_id);
        let without_requirements = cli_invocation(command_name, &command, [argument_id]);
        let without_requirements_parses = dependency_command
            .clone()
            .try_get_matches_from(without_requirements)
            .is_ok();
        assert_eq!(
            without_requirements_parses,
            requirements.is_empty(),
            "CLI requirement registry disagrees with {command_name} --{}",
            argument.get_long().unwrap_or(argument_id)
        );

        let with_requirements = cli_invocation(
            command_name,
            &command,
            std::iter::once(argument_id).chain(requirements.iter().copied()),
        );
        assert!(
            dependency_command
                .clone()
                .try_get_matches_from(with_requirements)
                .is_ok(),
            "CLI requirement registry is incomplete for {command_name} --{}",
            argument.get_long().unwrap_or(argument_id)
        );

        let invocation = cli_invocation(
            command_name,
            &command,
            std::iter::once(argument_id)
                .chain(requirements.iter().copied())
                .chain(command_requirements.iter().copied()),
        );
        assert!(
            command.clone().try_get_matches_from(invocation).is_ok(),
            "CLI requirement probe failed for {command_name} --{} with command requirements",
            argument.get_long().unwrap_or(argument_id)
        );
    }
}

fn cli_requirement_closure<'a>(command_name: &str, argument: &'a str) -> Vec<&'a str> {
    let mut requirements = Vec::new();
    let mut pending = cli_requirements(command_name, argument).to_vec();
    while let Some(requirement) = pending.pop() {
        if requirements.contains(&requirement) {
            continue;
        }
        requirements.push(requirement);
        pending.extend(cli_requirements(command_name, requirement));
    }
    requirements.sort_unstable();
    requirements
}

fn cli_invocation<'a>(
    command_name: &str,
    command: &Command,
    arguments: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let mut invocation = vec![command_name.to_string()];
    let mut seen = BTreeSet::new();
    for argument_id in arguments {
        if !seen.insert(argument_id) {
            continue;
        }
        let argument = command
            .get_arguments()
            .find(|argument| argument.get_id().as_str() == argument_id)
            .unwrap_or_else(|| panic!("unknown CLI requirement {argument_id} for {command_name}"));
        invocation.extend(cli_argument_tokens(argument));
    }
    invocation
}

fn cli_argument_tokens(argument: &Arg) -> Vec<String> {
    let option = if let Some(long) = argument.get_long() {
        format!("--{long}")
    } else {
        format!(
            "-{}",
            argument
                .get_short()
                .expect("filtered CLI option has a name")
        )
    };
    if !argument.get_action().takes_values() {
        return vec![option];
    }

    let value = argument
        .get_default_values()
        .first()
        .map(|value| value.to_string_lossy().into_owned())
        .or_else(|| {
            argument
                .get_possible_values()
                .first()
                .map(|value| value.get_name().to_string())
        })
        .unwrap_or_else(|| match argument_value_kind(argument) {
            "integer" | "number" => "1".to_string(),
            "path" => "/tmp/hubuum-operational-contract".to_string(),
            _ => "value".to_string(),
        });
    let value_count = argument
        .get_num_args()
        .map(|range| range.min_values().max(1))
        .unwrap_or(1);
    let mut tokens = Vec::with_capacity(value_count + 1);
    if argument.is_require_equals_set() {
        tokens.push(format!("{option}={value}"));
        tokens.extend(std::iter::repeat_n(value, value_count.saturating_sub(1)));
    } else {
        tokens.push(option);
        tokens.extend(std::iter::repeat_n(value, value_count));
    }
    tokens
}

fn dynamic_default(environment: &str) -> Option<DynamicDefaultContract> {
    let divisor = match environment {
        "HUBUUM_ACTIX_WORKERS" => 1,
        "HUBUUM_TASK_WORKERS" => 2,
        _ => return None,
    };
    Some(DynamicDefaultContract {
        source: "available_parallelism",
        divisor,
        rounding: "ceiling",
        minimum: 1,
    })
}

fn cli_exit_codes(command: &str) -> BTreeMap<&'static str, i32> {
    use crate::errors::{
        EXIT_CODE_CONFIG_ERROR, EXIT_CODE_DATABASE_ERROR, EXIT_CODE_GENERIC_ERROR,
        EXIT_CODE_INIT_ERROR, EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_TLS_ERROR,
    };

    let mut exit_codes = BTreeMap::from([
        ("success", 0),
        ("generic", EXIT_CODE_GENERIC_ERROR),
        ("usage_or_configuration", EXIT_CODE_CONFIG_ERROR),
        ("database", EXIT_CODE_DATABASE_ERROR),
    ]);
    if command == "hubuum-server" {
        exit_codes.extend([
            ("initialization", EXIT_CODE_INIT_ERROR),
            ("tls", EXIT_CODE_TLS_ERROR),
            ("permission_backend", EXIT_CODE_PERMISSION_BACKEND_ERROR),
        ]);
    }
    exit_codes
}

fn argument_value_kind(argument: &clap::Arg) -> &'static str {
    if !argument.get_possible_values().is_empty() {
        return "enum";
    }
    match argument.get_action() {
        ArgAction::SetTrue | ArgAction::SetFalse => return "boolean",
        ArgAction::Count => return "count",
        _ if !argument.get_action().takes_values() => return "flag",
        _ => {}
    }
    let type_id = argument.get_value_parser().type_id();
    if [
        TypeId::of::<i8>(),
        TypeId::of::<i16>(),
        TypeId::of::<i32>(),
        TypeId::of::<i64>(),
        TypeId::of::<isize>(),
        TypeId::of::<u8>(),
        TypeId::of::<u16>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<usize>(),
    ]
    .iter()
    .any(|candidate| type_id == *candidate)
    {
        return "integer";
    }
    if [TypeId::of::<f32>(), TypeId::of::<f64>()]
        .iter()
        .any(|candidate| type_id == *candidate)
    {
        return "number";
    }
    if type_id == TypeId::of::<PathBuf>() {
        return "path";
    }
    "string"
}

fn cli_requirements(command: &str, argument: &str) -> &'static [&'static str] {
    match (command, argument) {
        ("hubuum-admin", "backup_without_history") => &["backup"],
        ("hubuum-admin", "restore_test_database_url") => &["verify_backup"],
        ("hubuum-admin", "keep_restore_test_database") => &["restore_test_database_url"],
        ("hubuum-admin", "restore_confirmation") => &["restore"],
        ("hubuum-admin", "legacy_single_role_migration") => &["migrate"],
        ("hubuum-admin", "role") => &["check_database_privileges"],
        _ => &[],
    }
}

fn os_values(values: &[clap::builder::OsStr]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.as_os_str().to_string_lossy().into_owned())
        .collect()
}

fn owner_name(owner: EnvironmentOwner) -> String {
    format!("{owner:?}").to_ascii_lowercase()
}

fn exposure_name(exposure: Exposure) -> &'static str {
    match exposure {
        Exposure::Public => "public",
        Exposure::SensitiveMetadata => "sensitive_metadata",
        Exposure::Secret => "secret",
    }
}

fn admin_process_environment(name: &str) -> bool {
    matches!(
        name,
        "HUBUUM_TOKEN_HASH_KEY"
            | "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID"
            | "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS"
            | "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY"
            | "HUBUUM_SECRET_SOURCE"
            | "HUBUUM_SECRET_FILE_ROOT"
    )
}

fn runtime_roles(name: &str, admin_applicable: bool) -> Vec<&'static str> {
    if name.contains("_TEST_") || name.ends_with("_TESTS") {
        return vec!["test"];
    }
    if name == "HUBUUM_MIGRATION_DATABASE_URL" {
        return vec!["admin"];
    }
    let mut roles = match name {
        "HUBUUM_TASK_WORKERS"
        | "HUBUUM_TASK_POLL_INTERVAL_MS"
        | "HUBUUM_TASK_LEASE_SECONDS"
        | "HUBUUM_TASK_HEARTBEAT_SECONDS"
        | "HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS"
        | "HUBUUM_EVENT_FANOUT_WORKERS"
        | "HUBUUM_EVENT_DELIVERY_WORKERS"
        | "HUBUUM_EVENT_RETENTION_PURGE_ENABLED"
        | "HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS" => vec!["all", "worker"],
        _ => vec!["all", "api", "worker"],
    };
    if admin_applicable {
        roles.push("admin");
    }
    roles
}

fn appears_in_running_configuration(name: &str) -> bool {
    APP_CONFIG_ENVIRONMENT
        .iter()
        .any(|variable| variable.name == name)
        || matches!(
            name,
            "HUBUUM_TOKEN_HASH_KEY"
                | "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID"
                | "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS"
                | "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY"
                | "HUBUUM_SECRET_SOURCE"
                | "HUBUUM_SECRET_FILE_ROOT"
        )
}

fn event_contract() -> EventContract {
    let minimal_envelope = serialized_event_envelope(false);
    let populated_envelope = serialized_event_envelope(true);
    let envelope_fields = direct_field_contracts(&minimal_envelope);
    let provenance_fields = nested_field_contracts(
        "",
        &minimal_envelope["provenance"],
        &populated_envelope["provenance"],
    );
    EventContract {
        schema_version: BASE_AUDIT_DOCUMENT_SCHEMA_VERSION,
        revision_aware_schema_version: REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION,
        sink_payload_fields: envelope_fields.clone(),
        envelope_fields,
        provenance_fields,
        schema_version_semantics: &[
            "positive integer",
            "production events obtain schema_version from their audit document",
            "revision-bearing snapshots use revision_aware_schema_version; other events use schema_version",
            "incompatible envelope, provenance, or sink payload shape changes require both production versions to increase",
        ],
        actors: ActorKind::ALL
            .iter()
            .copied()
            .map(ActorKind::as_str)
            .collect(),
        entities: EntityType::ALL
            .iter()
            .copied()
            .map(|entity| EventEntityContract {
                name: entity.as_str(),
                actions: valid_actions(entity)
                    .iter()
                    .copied()
                    .map(Action::as_str)
                    .collect(),
            })
            .collect(),
        redaction_rules: &[
            "partially visible events replace before and after with null",
            "partially visible events replace metadata with an empty object",
            "event debug output never includes before, after, metadata, or routing secrets",
        ],
        audit_document_versions: &[
            BASE_AUDIT_DOCUMENT_SCHEMA_VERSION,
            REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION,
        ],
    }
}

fn serialized_event_envelope(populated: bool) -> serde_json::Value {
    use crate::events::{
        EventEnvelope, EventSequence, PrincipalId, Provenance, ProvenanceActor,
        ProvenancePrincipal, TaskId,
    };

    let actor_id = PrincipalId::new(1).expect("positive sample principal id");
    let initiator_id = PrincipalId::new(2).expect("positive sample principal id");
    let task_id = TaskId::new(3).expect("positive sample task id");
    let provenance = if populated {
        Provenance {
            actor: ProvenanceActor {
                kind: Some(ActorKind::User.as_str().to_string()),
                principal: Some(ProvenancePrincipal {
                    principal_id: actor_id,
                    name: None,
                }),
            },
            initiator: Some(ProvenancePrincipal {
                principal_id: initiator_id,
                name: None,
            }),
            task_id: Some(task_id),
        }
    } else {
        Provenance::default()
    };
    let event = sample_event(populated);
    let mut builder = EventEnvelope::builder()
        .id(EventSequence::new(1).expect("positive sample event sequence"))
        .event_id(uuid::Uuid::nil())
        .occurred_at(
            chrono::DateTime::from_timestamp(0, 0).expect("Unix epoch is a valid timestamp"),
        )
        .entity_type(EntityType::Collection)
        .action(Action::Created)
        .actor_kind(if populated {
            ActorKind::User
        } else {
            ActorKind::System
        })
        .provenance(provenance)
        .schema_version(event.schema_version())
        .summary(event.summary().to_string());
    if populated {
        builder = builder
            .entity_id(Some(
                crate::events::EventEntityId::new(1).expect("positive sample entity id"),
            ))
            .entity_name(Some("sample".to_string()))
            .collection_id(Some(
                crate::events::CollectionId::new(1).expect("positive sample collection id"),
            ))
            .actor_user_id(Some(actor_id))
            .request_id(Some(uuid::Uuid::nil()))
            .correlation_id(Some("sample".to_string()))
            .before(event.before().cloned())
            .after(event.after().cloned());
    }
    serde_json::to_value(
        builder
            .try_build()
            .expect("sample event envelope must satisfy runtime invariants"),
    )
    .expect("event envelope must serialize")
}

fn sample_event(revision_aware: bool) -> NewEvent {
    let document = AuditDocument::builder("operational contract sample")
        .before_opt(revision_aware.then(|| serde_json::json!({"revision": 1})))
        .after_opt(revision_aware.then(|| serde_json::json!({"revision": 2})))
        .try_build()
        .expect("sample audit document must satisfy runtime invariants");
    NewEvent::from_document(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        document,
    )
    .expect("sample event must satisfy runtime invariants")
}

fn direct_field_contracts(value: &serde_json::Value) -> Vec<FieldContract> {
    let mut fields = value
        .as_object()
        .expect("serialized event envelope must be an object")
        .iter()
        .map(|(name, value)| FieldContract {
            name: name.clone(),
            nullable: value.is_null(),
        })
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    fields
}

fn nested_field_contracts(
    prefix: &str,
    minimal: &serde_json::Value,
    populated: &serde_json::Value,
) -> Vec<FieldContract> {
    let populated = populated
        .as_object()
        .expect("populated event provenance must be an object");
    let minimal = minimal.as_object();
    let mut fields = Vec::new();
    for (name, populated_value) in populated {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let minimal_value = minimal.and_then(|object| object.get(name));
        fields.push(FieldContract {
            name: path.clone(),
            nullable: minimal_value
                .map_or_else(|| populated_value.is_null(), serde_json::Value::is_null),
        });
        if populated_value.is_object() {
            fields.extend(nested_field_contracts(
                &path,
                minimal_value.unwrap_or(&serde_json::Value::Null),
                populated_value,
            ));
        }
    }
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    fields
}

fn document_contracts() -> DocumentContracts {
    let backup_fields = serialized_document_fields(&BackupDocument {
        backup_version: CURRENT_BACKUP_VERSION,
        created_at: chrono::DateTime::UNIX_EPOCH,
        source_version: env!("CARGO_PKG_VERSION").to_string(),
        state: BackupState::default(),
        history: None,
        manifest: BackupManifest::default(),
    });
    let import_fields = serialized_document_fields(&ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: None,
        mode: None,
        graph: ImportGraph::default(),
    });
    DocumentContracts {
        backup: VersionedDocumentContract {
            version: Some(CURRENT_BACKUP_VERSION),
            required_fields: backup_fields.required,
            optional_fields: backup_fields.optional,
            sections: StorageBackupStateSection::ALL
                .iter()
                .copied()
                .map(StorageBackupStateSection::as_str)
                .chain(
                    StorageBackupHistorySection::ALL
                        .iter()
                        .copied()
                        .map(StorageBackupHistorySection::as_str),
                )
                .map(str::to_string)
                .collect(),
            rejection_policy: "reject any backup_version other than the current version",
        },
        import: VersionedDocumentContract {
            version: Some(CURRENT_IMPORT_VERSION),
            required_fields: import_fields.required,
            optional_fields: import_fields.optional,
            sections: serialized_section_names(&ImportGraph::default()),
            rejection_policy: "reject any import version other than the current version",
        },
        export: ExportContract {
            scope_kinds: ExportScopeKind::ALL
                .iter()
                .copied()
                .map(ExportScopeKind::as_str)
                .collect(),
            content_types: ExportContentType::ALL
                .iter()
                .copied()
                .map(ExportContentType::as_mime)
                .collect(),
            missing_data_policies: ExportMissingDataPolicy::ALL
                .iter()
                .copied()
                .map(ExportMissingDataPolicy::as_str)
                .collect(),
            template_kinds: ExportTemplateKind::ALL
                .iter()
                .copied()
                .map(ExportTemplateKind::as_str)
                .collect(),
        },
    }
}

struct SerializedDocumentFields {
    required: Vec<String>,
    optional: Vec<String>,
}

fn serialized_section_names<T: Serialize>(document: &T) -> Vec<String> {
    serde_json::to_value(document)
        .expect("section catalog sample must serialize")
        .as_object()
        .expect("section catalog sample must serialize as an object")
        .keys()
        .cloned()
        .collect()
}

fn serialized_document_fields<T>(document: &T) -> SerializedDocumentFields
where
    T: DeserializeOwned + Serialize,
{
    let value = serde_json::to_value(document).expect("document contract sample must serialize");
    let object = value
        .as_object()
        .expect("document contract sample must serialize as an object");
    let mut required = Vec::new();
    let mut optional = Vec::new();
    for name in object.keys() {
        let mut without_field = object.clone();
        without_field.remove(name);
        if serde_json::from_value::<T>(serde_json::Value::Object(without_field)).is_ok() {
            optional.push(name.clone());
        } else {
            required.push(name.clone());
        }
    }
    required.sort();
    optional.sort();
    SerializedDocumentFields { required, optional }
}

fn enum_name<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .expect("enum value must serialize")
        .as_str()
        .expect("enum value must serialize as a string")
        .to_string()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const METRIC_SOURCES: &[&str] = &[
        include_str!("observability/metrics.rs"),
        include_str!("observability/metrics/computed_field.rs"),
        include_str!("observability/metrics/db.rs"),
        include_str!("observability/metrics/event.rs"),
        include_str!("observability/metrics/export.rs"),
        include_str!("observability/metrics/http.rs"),
        include_str!("observability/metrics/import.rs"),
        include_str!("observability/metrics/inventory.rs"),
        include_str!("observability/metrics/login.rs"),
        include_str!("observability/metrics/process.rs"),
        include_str!("observability/metrics/registry.rs"),
        include_str!("observability/metrics/remote_call.rs"),
        include_str!("observability/metrics/scrape.rs"),
        include_str!("observability/metrics/secret.rs"),
        include_str!("observability/metrics/security.rs"),
        include_str!("observability/metrics/storage.rs"),
        include_str!("observability/metrics/task.rs"),
        include_str!("observability/metrics/token.rs"),
    ];

    #[test]
    fn operational_contract_is_deterministic_and_contains_every_family() {
        let first = generate_json();
        let second = generate_json();
        let value: serde_json::Value = serde_json::from_str(&first).unwrap();

        assert_eq!(first, second);
        assert_eq!(value["schema_version"], CONTRACT_SCHEMA_VERSION);
        assert!(
            value["metrics"]
                .as_array()
                .is_some_and(|metrics| !metrics.is_empty())
        );
        assert!(
            value["configuration"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );
        assert!(value["events"].is_object());
        assert!(value["documents"].is_object());
        assert_eq!(value["cli"].as_array().map(Vec::len), Some(2));
    }

    #[rstest]
    #[case("HUBUUM_ACTIX_WORKERS", 1)]
    #[case("HUBUUM_TASK_WORKERS", 2)]
    fn cpu_derived_defaults_do_not_capture_machine_capacity(
        #[case] environment: &str,
        #[case] divisor: usize,
    ) {
        let configuration = configuration_contracts();
        let configuration_item = configuration
            .iter()
            .find(|item| item.name == environment)
            .unwrap();
        assert!(configuration_item.default_is_set);
        assert!(configuration_item.default.is_empty());
        let configuration_default = configuration_item.dynamic_default.unwrap();
        assert_eq!(configuration_default.source, "available_parallelism");
        assert_eq!(configuration_default.divisor, divisor);
        assert_eq!(configuration_default.rounding, "ceiling");
        assert_eq!(configuration_default.minimum, 1);

        let cli = cli_contract("hubuum-server", crate::config::app_command());
        let cli_option = cli
            .options
            .iter()
            .find(|option| option.environment.as_deref() == Some(environment))
            .unwrap();
        assert!(cli_option.default.is_empty());
        let cli_default = cli_option.dynamic_default.unwrap();
        assert_eq!(cli_default.source, "available_parallelism");
        assert_eq!(cli_default.divisor, divisor);
        assert_eq!(cli_default.rounding, "ceiling");
        assert_eq!(cli_default.minimum, 1);
    }

    #[test]
    #[should_panic(expected = "CLI requirement registry disagrees")]
    fn cli_requirement_validation_detects_an_unregistered_dependency() {
        let mut command = crate::administration::admin_command()
            .mut_arg("reset_password", |argument| argument.requires("backup"));
        command.build();

        validate_cli_requirements("hubuum-admin", &command);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn cli_contract_includes_command_wide_required_options(#[case] takes_value: bool) {
        let command = Command::new("fixture")
            .arg(Arg::new("optional").long("optional"))
            .arg(
                Arg::new("required")
                    .long("required")
                    .required(true)
                    .action(if takes_value {
                        ArgAction::Set
                    } else {
                        ArgAction::SetTrue
                    }),
            );

        let contract = cli_contract("fixture", command);

        assert!(
            contract
                .options
                .iter()
                .find(|option| option.id == "required")
                .unwrap()
                .required
        );
    }

    #[rstest]
    #[case("backup")]
    #[case("backup_without_history")]
    fn cli_dependencies_are_checked_separately_from_command_requirements(#[case] required: &str) {
        let command = Command::new("hubuum-admin")
            .arg(Arg::new("optional").long("optional"))
            .arg(Arg::new("backup").long("backup"))
            .arg(
                Arg::new("backup_without_history")
                    .long("backup-without-history")
                    .action(ArgAction::SetTrue)
                    .requires("backup"),
            )
            .mut_arg(required, |argument| argument.required(true));

        let contract = cli_contract("hubuum-admin", command);

        assert_eq!(
            contract
                .options
                .iter()
                .find(|option| option.id == "backup_without_history")
                .unwrap()
                .requires,
            ["backup"]
        );
    }

    #[test]
    #[should_panic(expected = "CLI requirement registry disagrees")]
    fn cli_required_options_do_not_hide_unregistered_dependencies() {
        let command = Command::new("fixture")
            .arg(Arg::new("optional").long("optional").requires("required"))
            .arg(Arg::new("required").long("required").required(true));

        cli_contract("fixture", command);
    }

    #[test]
    #[should_panic(expected = "CLI requirement registry disagrees")]
    fn cli_required_options_do_not_hide_removed_dependencies() {
        let command = Command::new("hubuum-admin")
            .arg(Arg::new("backup").long("backup").required(true))
            .arg(
                Arg::new("backup_without_history")
                    .long("backup-without-history")
                    .action(ArgAction::SetTrue),
            );

        cli_contract("hubuum-admin", command);
    }

    #[test]
    fn document_fields_follow_serde_names_and_missing_field_behavior() {
        #[derive(Serialize, serde::Deserialize)]
        struct Fixture {
            #[serde(rename = "wire_name")]
            rust_name: u32,
            optional: Option<String>,
        }

        let fields = serialized_document_fields(&Fixture {
            rust_name: 1,
            optional: None,
        });

        assert_eq!(fields.required, ["wire_name"]);
        assert_eq!(fields.optional, ["optional"]);
    }

    #[test]
    fn document_sections_follow_serde_renames_and_omissions() {
        #[derive(Serialize)]
        struct Fixture {
            #[serde(rename = "memberships")]
            group_memberships: Vec<()>,
            #[serde(skip_serializing_if = "Option::is_none")]
            removed: Option<()>,
        }

        let sections = serialized_section_names(&Fixture {
            group_memberships: Vec::new(),
            removed: None,
        });

        assert_eq!(sections, ["memberships"]);
    }

    #[test]
    fn metric_registry_rejects_obvious_high_cardinality_labels() {
        let metric = metric!("hubuum_bad", Gauge, None, ["user_id"], &[], Process, "bad");

        assert!(validate_metrics(&[metric]).is_err());
    }

    #[test]
    fn metric_registry_has_unique_labels_and_names() {
        validate_metrics(METRICS).unwrap();
        for metric in METRICS {
            assert_eq!(
                metric.labels.iter().copied().collect::<BTreeSet<_>>().len(),
                metric.labels.len(),
                "duplicate labels on {}",
                metric.name
            );
        }
    }

    #[test]
    fn metric_registry_matches_runtime_instrument_names() {
        let source_names = METRIC_SOURCES
            .iter()
            .flat_map(|source| quoted_strings(source))
            .filter(|value| value.starts_with("hubuum_") || value.starts_with("process_"))
            .filter(|value| !value.contains(' '))
            .collect::<BTreeSet<_>>();
        let contract_names = METRICS
            .iter()
            .map(MetricDefinition::runtime_name)
            .collect::<BTreeSet<_>>();

        assert_eq!(source_names, contract_names);
    }

    #[test]
    fn metric_registry_covers_every_recorded_label_name() {
        let recorded_labels = METRIC_SOURCES
            .iter()
            .flat_map(|source| string_literals_after_call(source, "KeyValue::new("))
            .chain(["template_id", "template_name"])
            .collect::<BTreeSet<_>>();
        let contract_labels = METRICS
            .iter()
            .flat_map(|metric| metric.labels.iter().copied())
            .collect::<BTreeSet<_>>();

        assert_eq!(recorded_labels, contract_labels);
    }

    #[test]
    fn remote_call_method_labels_use_runtime_method_names() {
        let contract = metric_label_contract("hubuum_remote_call_results_total", "method");
        let runtime_names = RemoteHttpMethod::ALL
            .iter()
            .copied()
            .map(RemoteHttpMethod::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();

        assert_eq!(contract.values, runtime_names);
        assert!(
            contract
                .values
                .iter()
                .all(|value| value == &value.to_lowercase())
        );
    }

    #[test]
    fn configuration_inventory_covers_the_environment_registry() {
        let inventory = configuration_contracts();
        for variable in APP_CONFIG_ENVIRONMENT.iter().chain(PROCESS_ENVIRONMENT) {
            assert!(inventory.iter().any(|item| item.name == variable.name));
        }
    }

    #[test]
    fn token_lifetime_settings_publish_the_runtime_integer_cap() {
        let expected = (Some(1), Some(i64::from(i32::MAX)));

        assert_eq!(
            configuration_bounds("HUBUUM_TOKEN_LIFETIME_HOURS"),
            expected
        );
        assert_eq!(
            configuration_bounds("HUBUUM_MAX_TOKEN_LIFETIME_HOURS"),
            expected
        );
    }

    #[test]
    fn artifact_retention_settings_publish_the_runtime_duration_caps() {
        use crate::models::retention::{MAX_FUTURE_RETENTION_HOURS, MAX_FUTURE_RETENTION_MINUTES};

        for name in [
            "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS",
            "HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS",
        ] {
            assert_eq!(
                configuration_bounds(name),
                (Some(1), Some(MAX_FUTURE_RETENTION_HOURS))
            );
        }
        assert_eq!(
            configuration_bounds("HUBUUM_RESTORE_STAGE_RETENTION_MINUTES"),
            (Some(1), Some(MAX_FUTURE_RETENTION_MINUTES))
        );
    }

    #[test]
    fn process_only_configuration_preserves_typed_metadata() {
        let inventory = configuration_contracts();
        let secret_source = inventory
            .iter()
            .find(|item| item.name == "HUBUUM_SECRET_SOURCE")
            .unwrap();
        assert_eq!(secret_source.value_kind, "enum");
        assert!(secret_source.default_is_set);
        assert_eq!(secret_source.default, ["environment"]);
        assert_eq!(secret_source.allowed_values, ["environment", "file"]);

        let stable_key = inventory
            .iter()
            .find(|item| item.name == "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY")
            .unwrap();
        assert_eq!(stable_key.value_kind, "boolean");
        assert!(stable_key.default_is_set);
        assert_eq!(stable_key.default, ["false"]);
        assert_eq!(stable_key.allowed_values, ["true", "false", "1", "0"]);

        let secret_root = inventory
            .iter()
            .find(|item| item.name == "HUBUUM_SECRET_FILE_ROOT")
            .unwrap();
        assert_eq!(secret_root.value_kind, "path");
        assert!(!secret_root.default_is_set);
    }

    #[test]
    fn configuration_constraints_include_secret_file_root_requirement() {
        assert!(configuration_constraints().iter().any(|constraint| {
            constraint == "HUBUUM_SECRET_SOURCE=file requires HUBUUM_SECRET_FILE_ROOT"
        }));
    }

    #[test]
    fn configuration_constraints_include_token_key_startup_requirements() {
        let constraints = configuration_constraints();
        assert!(constraints.iter().any(|constraint| {
            constraint
                == "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS requires HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID"
        }));
        assert!(constraints.iter().any(|constraint| {
            constraint
                == "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true requires resolvable stable token hash key material"
        }));
    }

    #[test]
    fn configuration_constraints_include_the_treetop_feature_prerequisite() {
        assert!(configuration_constraints().iter().any(|constraint| {
            constraint
                == "HUBUUM_PERMISSION_BACKEND=treetop requires HUBUUM_TREETOP_URL and the compiled permissions-treetop feature"
        }));
    }

    #[test]
    fn page_limit_constraint_expression_comes_from_its_executable_operator() {
        assert!(
            crate::config::environment::constraints::PAGE_LIMITS.ordered_values_satisfy(100, 100)
        );
        assert_eq!(
            crate::config::environment::constraints::PAGE_LIMITS.expression(),
            "HUBUUM_DEFAULT_PAGE_LIMIT must not exceed HUBUUM_MAX_PAGE_LIMIT"
        );
    }

    #[test]
    fn administrator_configuration_includes_shared_database_and_token_settings() {
        let inventory = configuration_contracts();
        for name in [
            "HUBUUM_DATABASE_URL",
            "HUBUUM_DATABASE_ROLE_MODE",
            "HUBUUM_TOKEN_HASH_KEY",
            "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID",
            "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS",
            "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY",
            "HUBUUM_SECRET_SOURCE",
            "HUBUUM_SECRET_FILE_ROOT",
        ] {
            let setting = inventory.iter().find(|item| item.name == name).unwrap();
            assert!(
                setting.runtime_roles.contains(&"admin"),
                "{name} is consumed by hubuum-admin"
            );
        }
    }

    #[test]
    fn closed_metric_label_domains_are_enumerated() {
        for metric in metric_contracts() {
            for label in metric.labels {
                if label.bounded_by == "enumerated values" {
                    assert!(
                        !label.values.is_empty(),
                        "{} label {} has no enumerated values",
                        metric.name,
                        label.name
                    );
                }
            }
        }
    }

    #[test]
    fn event_contract_uses_serialized_envelope_shape() {
        let contract = event_contract();
        let serialized = serialized_event_envelope(false);
        let serialized_fields = serialized
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let contract_fields = contract
            .envelope_fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(contract_fields, serialized_fields);
        assert_eq!(contract.envelope_fields, contract.sink_payload_fields);
        assert!(
            contract
                .provenance_fields
                .iter()
                .any(|field| field.name == "actor.kind" && !field.nullable)
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn event_contract_versions_match_production_events(#[case] revision_aware: bool) {
        let contract = event_contract();
        let version = if revision_aware {
            contract.revision_aware_schema_version
        } else {
            contract.schema_version
        };
        let event = sample_event(revision_aware);

        assert_eq!(version, event.schema_version());
        assert_eq!(
            serialized_event_envelope(revision_aware)["schema_version"],
            version
        );
    }

    #[test]
    fn token_metric_contracts_match_runtime_labels() {
        let metrics = metric_contracts();
        for (name, expected) in [
            (
                "hubuum_token_authentications_total",
                &["format", "key_state", "outcome"][..],
            ),
            ("hubuum_token_hash_keys", &["state"][..]),
            ("hubuum_token_hash_stored", &["key_state", "lifecycle"][..]),
        ] {
            let metric = metrics.iter().find(|metric| metric.name == name).unwrap();
            assert_eq!(
                metric
                    .labels
                    .iter()
                    .map(|label| label.name)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn metric_domains_include_emitted_refresh_and_wakeup_values() {
        let metrics = metric_contracts();
        let refresh_sources = &metrics
            .iter()
            .find(|metric| metric.name == "hubuum_metrics_refresh_duration_seconds")
            .unwrap()
            .labels[0]
            .values;
        assert!(refresh_sources.iter().any(|value| value == "token_keys"));

        let wakeup_kinds = &metrics
            .iter()
            .find(|metric| metric.name == "hubuum_event_worker_wakeups_total")
            .unwrap()
            .labels
            .iter()
            .find(|label| label.name == "kind")
            .unwrap()
            .values;
        assert!(
            wakeup_kinds
                .iter()
                .any(|value| value == "notifications_sent")
        );
        assert!(
            !wakeup_kinds
                .iter()
                .any(|value| value == "notification_send")
        );
    }

    #[test]
    fn secret_configuration_defaults_are_never_serialized() {
        for item in configuration_contracts()
            .into_iter()
            .filter(|item| item.exposure == "secret")
        {
            assert!(item.default.is_empty(), "{} exposes a default", item.name);
        }
    }

    #[test]
    fn generated_metric_reference_is_committed() {
        assert_eq!(
            normalize_line_endings(include_str!("../docs/metrics-reference.md")),
            generate_metrics_markdown()
        );
    }

    #[cfg(feature = "embedded-migrations")]
    #[test]
    fn generated_operational_contract_is_committed() {
        assert_eq!(
            normalize_line_endings(include_str!("../docs/operational-contract.json")),
            generate_json()
        );
    }

    #[rstest]
    #[case("\n")]
    #[case("\r\n")]
    fn generated_artifact_comparison_normalizes_platform_line_endings(#[case] line_ending: &str) {
        let committed = ["first", "second", ""].join(line_ending);

        assert_eq!(normalize_line_endings(&committed), "first\nsecond\n");
    }

    fn normalize_line_endings(text: &str) -> String {
        text.replace("\r\n", "\n")
    }

    fn quoted_strings(source: &str) -> impl Iterator<Item = &str> {
        source.split('"').skip(1).step_by(2)
    }

    fn string_literals_after_call<'a>(
        source: &'a str,
        marker: &'a str,
    ) -> impl Iterator<Item = &'a str> {
        source.match_indices(marker).filter_map(move |(index, _)| {
            source[index + marker.len()..]
                .trim_start()
                .strip_prefix('"')?
                .split('"')
                .next()
        })
    }
}
