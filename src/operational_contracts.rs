use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::OnceLock;

use clap::{Arg, ArgAction, Command, ValueEnum};
use hubuum_events_core::{
    AuditDocument, BASE_AUDIT_DOCUMENT_SCHEMA_VERSION, CorrelationId, NewEvent,
    REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Number;

use crate::config::TracingSamplingMode;
use crate::observability::tracing::SAMPLE_RATIO_BOUNDS;

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
const STORAGE_OPERATION_SOURCES: &[&str] = &[
    include_str!("storage/observed.rs"),
    include_str!("storage/context/schema_evolution.rs"),
    include_str!("storage/context/query_usage.rs"),
    include_str!("storage/context/api.rs"),
    include_str!("storage/context/computed_fields.rs"),
    include_str!("storage/context/events.rs"),
    include_str!("storage/context/execution.rs"),
    include_str!("storage/context/identity.rs"),
    include_str!("storage/context/identity_queries.rs"),
    include_str!("storage/context/operational.rs"),
    include_str!("storage/context/queries.rs"),
    include_str!("storage/context/relations.rs"),
    include_str!("storage/context/runtime_settings.rs"),
    include_str!("storage/context/tasks.rs"),
    include_str!("storage/context/transaction.rs"),
    include_str!("storage/context/workflows.rs"),
];

mod metrics;
use metrics::{
    BACKGROUND_BUCKETS_SECONDS, LATENCY_BUCKETS_SECONDS, METRICS, MetricScope,
    OUTBOUND_BUCKETS_SECONDS,
};
pub(crate) use metrics::{
    MetricDefinition, MetricKind, metric_definition, runtime_metric_definition,
};

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
    minimum: Option<Number>,
    maximum: Option<Number>,
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
    wire_type: &'static str,
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
        ("hubuum_tracing_info", "sampling_mode") => TracingSamplingMode::value_variants()
            .iter()
            .map(|mode| mode.as_str().to_string())
            .collect(),
        ("hubuum_trace_spans_total", "category") => strings(&[
            "http",
            "task",
            "event",
            "outbound",
            "database",
            "storage",
            "authorization",
            "authentication",
        ]),
        ("hubuum_trace_spans_total", "state") => strings(&["started", "ended"]),
        ("hubuum_trace_spans_dropped_total", "reason") => {
            strings(&["classification", "queue_saturation"])
        }
        (
            "hubuum_trace_export_batches_total"
            | "hubuum_trace_export_spans_total"
            | "hubuum_trace_flushes_total",
            "outcome",
        ) => strings(&["success", "failure"]),
        ("hubuum_api_errors_total", "class") => strings(&[
            "interaction_invalid",
            "configuration_changed",
            "interaction_expired",
            "interaction_consumed",
            "interaction_cancelled",
            "access_denied",
            "interaction_failed",
            "interaction_pending",
            "slow_down",
            "capability_unsupported",
            "provider_unavailable",
            "cancel_requested",
            "deadline_exceeded",
            "unauthorized",
            "reauthentication_required",
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
        ("hubuum_computed_field_read_repairs_total", "outcome") => strings(&[
            "queued",
            "coalesced",
            "queue_full",
            "queue_busy",
            "enqueue_failure",
            "enqueue_timeout",
            "success",
            "failure",
        ]),
        ("hubuum_schema_mutations_total", "policy") => {
            strings(&["stage", "reject_incompatible", "allow_pending"])
        }
        ("hubuum_schema_mutations_total", "result") => {
            strings(&["committed", "unchanged", "conflict", "error"])
        }
        ("hubuum_schema_validation_objects_total", "result") => {
            strings(&["valid", "invalid", "not_required", "uninspectable", "stale"])
        }
        (name, "kind") if name.starts_with("hubuum_schema_validation_") => {
            strings(&["impact", "revalidation"])
        }
        ("hubuum_schema_validation_duration_seconds", "status") => {
            strings(&["complete", "cancelled", "superseded"])
        }
        ("hubuum_schema_compliance_objects", "status") => {
            strings(&["valid", "invalid", "pending", "not_required"])
        }
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
            strings(&["principal_ip", "principal", "ip", "subnet", "unknown"])
        }
        ("hubuum_metrics_refresh_skipped_total", "reason") => strings(&["concurrent"]),
        (name, "event") if name.starts_with("hubuum_template_worker") => strings(&[
            "admitted",
            "started",
            "completed",
            "cancelled",
            "shutdown",
            "overloaded",
            "input_limit",
            "admission_timeout",
            "spawn_failed",
            "worker_failed",
            "execution_timeout",
            "render_failed",
        ]),
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
            "oidc",
            "oauth",
            "runtime_settings",
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
        ("hubuum_task_cancellation_requests_total", "result") => {
            strings(&["queued", "active", "unchanged"])
        }
        (name, "reason") if name.starts_with("hubuum_task_") => {
            strings(&["cancel_requested", "deadline_exceeded"])
        }
        (name, "kind") if name.starts_with("hubuum_task_output_cleanup") => {
            strings(&["export", "backup", "schema_profiling"])
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
        ("hubuum_token_authentications_total", "format") => strings(&[
            "legacy",
            "version1",
            "versioned_unknown",
            "session",
            "activity",
        ]),
        ("hubuum_token_authentications_total", "key_state") => {
            strings(&["active", "previous", "legacy", "unknown"])
        }
        ("hubuum_token_authentications_total", "outcome") => strings(&[
            "success",
            "migrated",
            "migration_conflict",
            "rejected",
            "issued",
            "refreshed",
            "replayed",
            "recording_failed",
        ]),
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
            first_quoted_strings_after(source, ".observe_storage_call(")
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
    let (minimum, maximum) = if variable.name == "HUBUUM_TRACING_SAMPLE_RATIO" {
        (
            Some(Number::from_f64(*SAMPLE_RATIO_BOUNDS.start()).expect("finite ratio minimum")),
            Some(Number::from_f64(*SAMPLE_RATIO_BOUNDS.end()).expect("finite ratio maximum")),
        )
    } else {
        (minimum.map(Number::from), maximum.map(Number::from))
    };
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
        "HUBUUM_AUTH_CONFIG_HOST_PATH" => ("path", &[], &[]),
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
    let mut command = command.clone().mut_args(|argument| {
        let argument = argument.env(None::<&str>);
        // Dependency probes exercise clap's argument graph, not value
        // validation. A made-up value cannot satisfy every custom parser.
        if argument.get_action().takes_values() {
            argument.value_parser(clap::builder::StringValueParser::new())
        } else {
            argument
        }
    });
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
    let mut positionals = Vec::new();
    for argument_id in arguments {
        if !seen.insert(argument_id) {
            continue;
        }
        let argument = command
            .get_arguments()
            .find(|argument| argument.get_id().as_str() == argument_id)
            .unwrap_or_else(|| panic!("unknown CLI requirement {argument_id} for {command_name}"));
        if let Some(index) = argument.get_index() {
            positionals.push((index, argument));
        } else {
            invocation.extend(cli_argument_tokens(argument));
        }
    }
    positionals.sort_by_key(|(index, _)| *index);
    if !positionals.is_empty() {
        invocation.push("--".to_string());
    }
    for (_, argument) in positionals {
        invocation.extend(cli_argument_tokens(argument));
    }
    invocation
}

fn cli_argument_tokens(argument: &Arg) -> Vec<String> {
    let option = argument
        .get_long()
        .map(|long| format!("--{long}"))
        .or_else(|| argument.get_short().map(|short| format!("-{short}")));
    if !argument.get_action().takes_values() {
        return option.into_iter().collect();
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
    if let Some(option) = option.as_ref().filter(|_| argument.is_require_equals_set()) {
        tokens.push(format!("{option}={value}"));
        tokens.extend(std::iter::repeat_n(value, value_count.saturating_sub(1)));
    } else {
        tokens.extend(option);
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
        ("hubuum-admin", "query_usage_class") => &["reconcile_query_usage"],
        ("hubuum-admin", "recover_security") => &["recovery_administrator_id", "writes_quiesced"],
        ("hubuum-admin", "recovery_administrator_id" | "writes_quiesced") => &["recover_security"],
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
        )
}

fn event_contract() -> EventContract {
    let minimal_envelope = serialized_event_envelope(false, false);
    let populated_envelope = serialized_event_envelope(true, false);
    let typed_envelope = serialized_event_envelope(true, true);
    let envelope_fields = direct_field_contracts(&minimal_envelope, &typed_envelope);
    let provenance_fields = nested_field_contracts(
        "",
        &minimal_envelope["provenance"],
        &populated_envelope["provenance"],
        &typed_envelope["provenance"],
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

fn serialized_event_envelope(populated: bool, resolve_names: bool) -> serde_json::Value {
    use crate::events::{
        EventEnvelope, EventSequence, PrincipalId, Provenance, ProvenanceActor,
        ProvenancePrincipal, TaskId, TraceLink,
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
                    name: resolve_names.then(|| "actor".to_string()),
                }),
            },
            initiator: Some(ProvenancePrincipal {
                principal_id: initiator_id,
                name: resolve_names.then(|| "initiator".to_string()),
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
            .correlation_id(Some(
                CorrelationId::new("sample").expect("valid fixture correlation ID"),
            ))
            .trace_link(Some(
                TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0)
                    .expect("valid fixture trace link"),
            ))
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

fn field_wire_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => panic!("wire-type fixture must populate every nullable field"),
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn direct_field_contracts(
    value: &serde_json::Value,
    populated: &serde_json::Value,
) -> Vec<FieldContract> {
    let mut fields = value
        .as_object()
        .expect("serialized event envelope must be an object")
        .iter()
        .map(|(name, value)| FieldContract {
            name: name.clone(),
            nullable: value.is_null(),
            wire_type: field_wire_type(&populated[name]),
        })
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    fields
}

fn nested_field_contracts(
    prefix: &str,
    minimal: &serde_json::Value,
    populated: &serde_json::Value,
    typed: &serde_json::Value,
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
            wire_type: field_wire_type(&typed[name]),
        });
        if populated_value.is_object() {
            fields.extend(nested_field_contracts(
                &path,
                minimal_value.unwrap_or(&serde_json::Value::Null),
                populated_value,
                &typed[name],
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
            rejection_policy: "accept backup versions 6 through 10; normalize legacy event defaults, missing query usage declarations and missing shared computed definition history; observe new definition history at the restore boundary",
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
mod tests;
