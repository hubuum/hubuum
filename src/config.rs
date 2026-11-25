use hubuum_storage_core::{StorageBackupBudget, StorageRuntimeSettingsGroup};
pub mod environment;
pub mod running;
pub(crate) mod runtime;

#[cfg(not(test))]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};

use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::errors::ApiError;
use crate::events::{EventDeliverySettings, EventFanoutSettings, EventRetentionSettings};
use crate::models::retention::FutureRetention;
use crate::models::{TokenIssuancePolicy, TokenRetentionSettings};
use crate::pagination::PageLimits;
use crate::storage::StorageBackendKind;
use crate::tasks::TaskWorkerSettings;
use environment::{constraints, validate_configuration_bounds};

mod client_network;
mod defaults;
mod query_observations;
mod schema_validation;
use hubuum_storage_core::StorageQueryObservationSettings;
use query_observations::QueryObservationOptions;
mod secret_source;
use hubuum_domain::JsonSchemaLimits;
pub use schema_validation::SchemaValidationOptions;
mod tls_backend;
mod token_hash;
pub use client_network::{ClientAllowlist, ClientNetworkParseError, TrustedProxies};
pub use defaults::*;
pub use secret_source::SecretSourceOptions;
pub(crate) use secret_source::{CommandLineDatabaseUrl, SecretSourceSettings};
pub use tls_backend::TlsBackend;
pub use token_hash::{
    TokenHashKeyConfigError, TokenHashKeyRing, token_hash_key_bytes, token_hash_key_is_ephemeral,
    token_hash_key_ring,
};

pub use hubuum_storage_core::MetricsPath;

fn detected_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

fn default_actix_workers() -> usize {
    detected_cpu_count()
}

fn default_task_workers() -> usize {
    detected_cpu_count().div_ceil(2).max(1)
}

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PermissionBackendKind {
    #[default]
    Local,
    Treetop,
}

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeRole {
    #[default]
    All,
    Api,
    Worker,
}

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DatabasePrivilegeMode {
    #[default]
    Warn,
    Strict,
}

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseRoleMode {
    #[default]
    Single,
    Split,
}

impl DatabaseRoleMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Split => "split",
        }
    }

    pub const fn uses_split_roles(self) -> bool {
        matches!(self, Self::Split)
    }
}

impl DatabasePrivilegeMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Strict => "strict",
        }
    }
}

impl RuntimeRole {
    pub const fn serves_http(self) -> bool {
        matches!(self, Self::All | Self::Api)
    }

    pub const fn runs_background_workers(self) -> bool {
        matches!(self, Self::All | Self::Worker)
    }

    pub(crate) const fn effective_worker_count(self, configured: usize) -> usize {
        if self.runs_background_workers() {
            configured
        } else {
            0
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Api => "api",
            Self::Worker => "worker",
        }
    }
}

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LoginRateLimitBackendKind {
    #[default]
    Memory,
    Valkey,
}

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TracingSamplingMode {
    Off,
    AlwaysOn,
    #[default]
    ParentBasedRatio,
}

impl TracingSamplingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::AlwaysOn => "always_on",
            Self::ParentBasedRatio => "parent_based_ratio",
        }
    }
}

impl LoginRateLimitBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Valkey => "valkey",
        }
    }
}

#[derive(Parser, Deserialize, Clone)]
#[command(version = env!("CARGO_PKG_VERSION"), about = "Hubuum server", long_about = None)]
pub struct AppConfig {
    // Ownership travels with resolved values across request/job boundaries.
    #[clap(skip)]
    #[serde(skip)]
    runtime_managed_groups: BTreeSet<StorageRuntimeSettingsGroup>,

    #[command(flatten)]
    #[serde(default)]
    pub schema_validation: SchemaValidationOptions,

    #[command(flatten)]
    #[serde(default)]
    pub query_observations: QueryObservationOptions,
    #[clap(skip)]
    #[serde(skip)]
    query_observation_settings: StorageQueryObservationSettings,

    #[clap(skip)]
    #[serde(skip)]
    schema_limits: JsonSchemaLimits,

    #[command(flatten)]
    #[serde(default)]
    pub secrets: SecretSourceOptions,

    #[clap(skip)]
    #[serde(skip)]
    pub(crate) database_url_override: Option<CommandLineDatabaseUrl>,

    /// Runtime role: combined API/workers, API-only, or worker-only.
    #[clap(long, env = "HUBUUM_RUNTIME_ROLE", default_value = "all")]
    pub runtime_role: RuntimeRole,

    /// Storage adapter compiled into this application build.
    #[clap(
        long,
        env = "HUBUUM_STORAGE_BACKEND",
        value_enum,
        default_value = "postgresql"
    )]
    pub storage_backend: StorageBackendKind,

    /// IP address to bind to
    #[clap(long, env = "HUBUUM_BIND_IP", default_value = "127.0.0.1")]
    pub bind_ip: String,

    /// Port to bind to
    #[clap(long, env = "HUBUUM_BIND_PORT", default_value = "8080")]
    pub port: u16,

    /// Logging level
    #[clap(long, env = "HUBUUM_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Enable OpenTelemetry trace export through OTLP/HTTP protobuf.
    #[clap(
        long,
        env = "HUBUUM_TRACING_ENABLED",
        default_value_t = DEFAULT_TRACING_ENABLED
    )]
    pub tracing_enabled: bool,

    /// HTTPS OTLP collector base endpoint. `/v1/traces` is appended by the exporter.
    #[clap(long, env = "HUBUUM_TRACING_OTLP_ENDPOINT", default_value = None)]
    pub tracing_otlp_endpoint: Option<String>,

    /// Comma-separated bounded OTLP headers in `name=value` form.
    #[clap(long, env = "HUBUUM_TRACING_OTLP_HEADERS", default_value = None)]
    pub tracing_otlp_headers: Option<String>,

    /// Optional private CA PEM bundle for the OTLP collector.
    #[clap(long, env = "HUBUUM_TRACING_OTLP_CA_CERT", default_value = None)]
    pub tracing_otlp_ca_cert: Option<String>,

    /// Optional client certificate PEM for OTLP mutual TLS.
    #[clap(long, env = "HUBUUM_TRACING_OTLP_CLIENT_CERT", default_value = None)]
    pub tracing_otlp_client_cert: Option<String>,

    /// Optional client private key PEM for OTLP mutual TLS.
    #[clap(long, env = "HUBUUM_TRACING_OTLP_CLIENT_KEY", default_value = None)]
    pub tracing_otlp_client_key: Option<String>,

    #[clap(
        long,
        env = "HUBUUM_TRACING_CONNECT_TIMEOUT_MS",
        default_value_t = DEFAULT_TRACING_CONNECT_TIMEOUT_MS
    )]
    pub tracing_connect_timeout_ms: u64,

    #[clap(
        long,
        env = "HUBUUM_TRACING_EXPORT_TIMEOUT_MS",
        default_value_t = DEFAULT_TRACING_EXPORT_TIMEOUT_MS
    )]
    pub tracing_export_timeout_ms: u64,

    #[clap(
        long,
        env = "HUBUUM_TRACING_FLUSH_TIMEOUT_MS",
        default_value_t = DEFAULT_TRACING_FLUSH_TIMEOUT_MS
    )]
    pub tracing_flush_timeout_ms: u64,

    #[clap(
        long,
        env = "HUBUUM_TRACING_QUEUE_CAPACITY",
        default_value_t = DEFAULT_TRACING_QUEUE_CAPACITY
    )]
    pub tracing_queue_capacity: usize,

    #[clap(
        long,
        env = "HUBUUM_TRACING_BATCH_SIZE",
        default_value_t = DEFAULT_TRACING_BATCH_SIZE
    )]
    pub tracing_batch_size: usize,

    #[clap(
        long,
        env = "HUBUUM_TRACING_SAMPLING_MODE",
        value_enum,
        default_value = "parent-based-ratio"
    )]
    pub tracing_sampling_mode: TracingSamplingMode,

    #[clap(
        long,
        env = "HUBUUM_TRACING_SAMPLE_RATIO",
        default_value_t = DEFAULT_TRACING_SAMPLE_RATIO
    )]
    pub tracing_sample_ratio: f64,

    #[clap(
        long,
        env = "HUBUUM_TRACING_SERVICE_NAME",
        default_value = DEFAULT_TRACING_SERVICE_NAME
    )]
    pub tracing_service_name: String,

    #[clap(
        long,
        env = "HUBUUM_TRACING_SERVICE_NAMESPACE",
        default_value = DEFAULT_TRACING_SERVICE_NAMESPACE
    )]
    pub tracing_service_namespace: String,

    #[clap(
        long,
        env = "HUBUUM_TRACING_DEPLOYMENT_ENVIRONMENT",
        default_value = DEFAULT_TRACING_DEPLOYMENT_ENVIRONMENT
    )]
    pub tracing_deployment_environment: String,

    /// Let a valid remote sampled flag control parent-based sampling.
    #[clap(
        long,
        env = "HUBUUM_TRACING_TRUST_INCOMING_SAMPLING",
        default_value_t = false
    )]
    pub tracing_trust_incoming_sampling: bool,

    /// Inject W3C trace context into supported outbound integrations.
    #[clap(
        long,
        env = "HUBUUM_TRACING_PROPAGATE_OUTBOUND",
        default_value_t = true
    )]
    pub tracing_propagate_outbound: bool,

    /// Database URL
    #[clap(
        long,
        env = "HUBUUM_DATABASE_URL",
        default_value = "postgres://localhost"
    )]
    pub database_url: String,

    /// Database credential topology: one shared login or split owner/migrator/runtime roles
    #[clap(
        long,
        env = "HUBUUM_DATABASE_ROLE_MODE",
        value_enum,
        default_value = "single"
    )]
    pub database_role_mode: DatabaseRoleMode,

    /// Whether unsafe or incomplete PostgreSQL runtime grants warn or fail startup
    #[clap(
        long,
        env = "HUBUUM_DATABASE_PRIVILEGE_MODE",
        value_enum,
        default_value = "warn"
    )]
    pub database_privilege_mode: DatabasePrivilegeMode,

    /// Non-login PostgreSQL role that owns Hubuum database objects
    #[clap(
        long,
        env = "HUBUUM_DATABASE_OWNER_ROLE",
        default_value = "hubuum_owner"
    )]
    pub database_owner_role: String,

    /// PostgreSQL role used by one-shot migrations and destructive restore
    #[clap(
        long,
        env = "HUBUUM_DATABASE_MIGRATOR_ROLE",
        default_value = "hubuum_migrator"
    )]
    pub database_migrator_role: String,

    /// Non-owning PostgreSQL role used by API and worker processes
    #[clap(
        long,
        env = "HUBUUM_DATABASE_RUNTIME_ROLE",
        default_value = "hubuum_runtime"
    )]
    pub database_runtime_role: String,

    /// Number of Actix workers
    #[clap(
        long,
        env = "HUBUUM_ACTIX_WORKERS",
        default_value_t = default_actix_workers()
    )]
    pub actix_workers: usize,

    /// Number of background task workers
    #[clap(
        long,
        env = "HUBUUM_TASK_WORKERS",
        default_value_t = default_task_workers()
    )]
    pub task_workers: usize,

    /// How long idle task workers sleep between queue polls
    #[clap(
        long,
        env = "HUBUUM_TASK_POLL_INTERVAL_MS",
        default_value_t = DEFAULT_TASK_POLL_INTERVAL_MS
    )]
    pub task_poll_interval_ms: u64,

    /// Duration of a task worker lease before another worker may recover it.
    #[clap(
        long,
        env = "HUBUUM_TASK_LEASE_SECONDS",
        default_value_t = DEFAULT_TASK_LEASE_SECONDS
    )]
    pub task_lease_seconds: u64,

    /// Interval at which an active task lease is renewed.
    #[clap(
        long,
        env = "HUBUUM_TASK_HEARTBEAT_SECONDS",
        default_value_t = DEFAULT_TASK_HEARTBEAT_SECONDS
    )]
    pub task_heartbeat_seconds: u64,

    /// Maximum import execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_IMPORT_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 3600
    )]
    pub task_import_execution_timeout_seconds: u64,

    /// Maximum export execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_EXPORT_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 900
    )]
    pub task_export_execution_timeout_seconds: u64,

    /// Maximum backup execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_BACKUP_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 3600
    )]
    pub task_backup_execution_timeout_seconds: u64,

    /// Maximum reindex execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_REINDEX_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 7200
    )]
    pub task_reindex_execution_timeout_seconds: u64,

    /// Maximum remote call execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_REMOTE_CALL_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 300
    )]
    pub task_remote_call_execution_timeout_seconds: u64,

    /// Maximum schema profiling execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_SCHEMA_PROFILING_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 300
    )]
    pub task_schema_profiling_execution_timeout_seconds: u64,

    /// Maximum schema validation execution time, starting at first claim.
    #[clap(
        long,
        env = "HUBUUM_TASK_SCHEMA_VALIDATION_EXECUTION_TIMEOUT_SECONDS",
        default_value_t = 7200
    )]
    pub task_schema_validation_execution_timeout_seconds: u64,

    /// Minimum interval between stale-task recovery scans in this process.
    #[clap(
        long,
        env = "HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS",
        default_value_t = DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS
    )]
    pub task_recovery_interval_seconds: u64,

    /// Number of objects recomputed in one computed-field rebuild transaction.
    #[clap(
        long,
        env = "HUBUUM_COMPUTED_REINDEX_BATCH_SIZE",
        default_value_t = DEFAULT_COMPUTED_REINDEX_BATCH_SIZE
    )]
    pub computed_reindex_batch_size: usize,

    /// Number of background event fan-out workers
    #[clap(
        long,
        env = "HUBUUM_EVENT_FANOUT_WORKERS",
        default_value_t = DEFAULT_EVENT_FANOUT_WORKERS
    )]
    pub event_fanout_workers: usize,

    /// Number of events an event fan-out worker claims per batch
    #[clap(
        long,
        env = "HUBUUM_EVENT_FANOUT_BATCH_SIZE",
        default_value_t = DEFAULT_EVENT_FANOUT_BATCH_SIZE
    )]
    pub event_fanout_batch_size: usize,

    /// How long idle event fan-out workers sleep between queue polls
    #[clap(
        long,
        env = "HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS",
        default_value_t = DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS
    )]
    pub event_fanout_poll_interval_ms: u64,

    /// How long event fan-out claims remain locked before another worker may retry
    #[clap(
        long,
        env = "HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS",
        default_value_t = DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS
    )]
    pub event_fanout_lock_timeout_ms: u64,

    /// Number of background event delivery workers. Zero disables transport delivery.
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_WORKERS",
        default_value_t = DEFAULT_EVENT_DELIVERY_WORKERS
    )]
    pub event_delivery_workers: usize,

    /// Number of delivery rows an event delivery worker claims per batch
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_BATCH_SIZE",
        default_value_t = DEFAULT_EVENT_DELIVERY_BATCH_SIZE
    )]
    pub event_delivery_batch_size: usize,

    /// How long idle event delivery workers sleep between queue polls
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS",
        default_value_t = DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS
    )]
    pub event_delivery_poll_interval_ms: u64,

    /// How long event delivery claims remain locked before another worker may retry
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS",
        default_value_t = DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS
    )]
    pub event_delivery_lock_timeout_ms: u64,

    /// Wall-clock timeout for one event delivery transport attempt.
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS",
        default_value_t = DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS
    )]
    pub event_delivery_transport_timeout_ms: u64,

    /// Initial event delivery retry backoff
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS",
        default_value_t = DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS
    )]
    pub event_delivery_retry_backoff_base_ms: u64,

    /// Maximum event delivery retry backoff
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS",
        default_value_t = DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS
    )]
    pub event_delivery_retry_backoff_max_ms: u64,

    /// Delivery attempts before a row moves to dead-letter status
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS",
        default_value_t = DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS
    )]
    pub event_delivery_max_attempts: i32,

    /// Enable the destructive event retention purge worker.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_PURGE_ENABLED",
        default_value_t = DEFAULT_EVENT_RETENTION_PURGE_ENABLED
    )]
    pub event_retention_purge_enabled: bool,

    /// How long audit/event rows are retained before purge eligibility.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_DAYS",
        default_value_t = DEFAULT_EVENT_RETENTION_DAYS
    )]
    pub event_retention_days: i64,

    /// How long terminal event delivery rows are retained before purge eligibility.
    #[clap(
        long,
        env = "HUBUUM_EVENT_DELIVERY_RETENTION_DAYS",
        default_value_t = DEFAULT_EVENT_DELIVERY_RETENTION_DAYS
    )]
    pub event_delivery_retention_days: i64,

    /// How often the event retention purge worker wakes up when enabled.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS",
        default_value_t = DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS
    )]
    pub event_retention_purge_interval_seconds: u64,

    /// Maximum events deleted in one retention purge transaction.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE",
        default_value_t = DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE
    )]
    pub event_retention_purge_batch_size: usize,

    /// Enable local JSONL file archive writes before deleting eligible events.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED",
        default_value_t = DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED
    )]
    pub event_retention_file_archive_enabled: bool,

    /// Optional directory for atomic per-claim JSONL event archive files.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
        default_value = None
    )]
    pub event_retention_archive_path: Option<String>,

    /// How long successful stored export outputs remain available for refetch.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS",
        default_value_t = DEFAULT_EXPORT_OUTPUT_RETENTION_HOURS
    )]
    pub export_output_retention_hours: i64,

    /// How often workers attempt cleanup of expired stored export outputs.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS",
        default_value_t = DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS
    )]
    pub export_output_cleanup_interval_seconds: u64,

    /// How long a completed logical backup remains downloadable.
    #[clap(
        long,
        env = "HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS",
        default_value_t = DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS
    )]
    pub backup_output_retention_hours: i64,

    /// Maximum active backup tasks one principal may own.
    #[clap(
        long,
        env = "HUBUUM_BACKUP_MAX_ACTIVE_TASKS_PER_USER",
        default_value_t = DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER
    )]
    pub backup_max_active_tasks_per_user: usize,

    /// Maximum logical backup artifact size accepted by the API.
    #[clap(
        long,
        env = "HUBUUM_BACKUP_MAX_OUTPUT_BYTES",
        default_value_t = DEFAULT_BACKUP_MAX_OUTPUT_BYTES
    )]
    pub backup_max_output_bytes: usize,

    /// Maximum rows enumerated during a backup, including excluded history rows.
    #[clap(long, env = "HUBUUM_BACKUP_MAX_CAPTURE_ROWS", default_value_t = DEFAULT_BACKUP_MAX_CAPTURE_ROWS)]
    pub backup_max_capture_rows: usize,

    /// Minutes a validated restore stage and its capability remain usable.
    #[clap(
        long,
        env = "HUBUUM_RESTORE_STAGE_RETENTION_MINUTES",
        default_value_t = DEFAULT_RESTORE_STAGE_RETENTION_MINUTES
    )]
    pub restore_stage_retention_minutes: i64,

    /// Maximum full-backup document size accepted by the restore API.
    #[clap(
        long,
        env = "HUBUUM_RESTORE_MAX_UPLOAD_BYTES",
        default_value_t = DEFAULT_RESTORE_MAX_UPLOAD_BYTES
    )]
    pub restore_max_upload_bytes: usize,

    /// Maximum queued/validating/running import tasks one user may have at once.
    #[clap(
        long,
        env = "HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER",
        default_value_t = DEFAULT_IMPORT_MAX_ACTIVE_TASKS_PER_USER
    )]
    pub import_max_active_tasks_per_user: usize,

    /// Maximum queued/validating/running export tasks one user may have at once.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER",
        default_value_t = DEFAULT_EXPORT_MAX_ACTIVE_TASKS_PER_USER
    )]
    pub export_max_active_tasks_per_user: usize,

    /// MiniJinja recursion limit for export template rendering.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT
    )]
    pub export_template_recursion_limit: usize,

    /// MiniJinja fuel budget for export template rendering.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_FUEL",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_FUEL
    )]
    pub export_template_fuel: u64,

    /// Maximum number of hydrated relation-aware template objects rendered for one export root.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_MAX_OBJECTS
    )]
    pub export_template_max_objects: usize,

    /// Maximum rendered export output size accepted for storage or response.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_MAX_OUTPUT_BYTES",
        default_value_t = DEFAULT_EXPORT_MAX_OUTPUT_BYTES
    )]
    pub export_max_output_bytes: usize,

    /// Post-completion budget per export execution stage, in milliseconds.
    ///
    /// This is a *rejection* budget, not an in-flight interrupt: an export is
    /// rejected only after a stage (query, hydration, render) has finished if it
    /// exceeded this value. Real in-flight protection comes from minijinja
    /// `export_template_fuel`, `export_template_max_objects`, the output byte
    /// caps, and `db_statement_timeout_ms` (which actually cancels slow queries
    /// server-side).
    #[clap(
        long,
        env = "HUBUUM_EXPORT_STAGE_TIMEOUT_MS",
        default_value_t = DEFAULT_EXPORT_STAGE_TIMEOUT_MS
    )]
    pub export_stage_timeout_ms: u64,

    /// Upper bound (milliseconds) applied to a remote target's per-call `timeout_ms`.
    ///
    /// A target may request a smaller timeout, but never a larger one. This bounds the
    /// wall-clock cost of any single outbound remote call dispatched by the worker.
    #[clap(
        long,
        env = "HUBUUM_REMOTE_CALL_TIMEOUT_MS",
        default_value_t = DEFAULT_REMOTE_CALL_TIMEOUT_MS
    )]
    pub remote_call_timeout_ms: u64,

    /// Maximum number of remote response body bytes read and stored as a preview.
    ///
    /// The worker stops reading the outbound response once this limit is reached, so a
    /// hostile or oversized response cannot exhaust worker memory.
    #[clap(
        long,
        env = "HUBUUM_REMOTE_CALL_MAX_RESPONSE_BYTES",
        default_value_t = DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES
    )]
    pub remote_call_max_response_bytes: usize,

    /// Allow remote targets to resolve to private, loopback, or link-local addresses.
    ///
    /// Disabled by default. When `false`, the worker refuses to call any URL whose host
    /// resolves to a non-global address (loopback, RFC1918, link-local, ULA, cloud
    /// metadata, etc.), mitigating SSRF. Enable only for trusted internal deployments.
    #[clap(
        long,
        env = "HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS",
        default_value_t = DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS
    )]
    pub remote_call_allow_private_targets: bool,

    /// Maximum queued/validating/running remote call tasks one user may have at once.
    #[clap(
        long,
        env = "HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER",
        default_value_t = DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER
    )]
    pub remote_call_max_active_tasks_per_user: usize,

    /// Pool-global Postgres `statement_timeout` in milliseconds (0 = disabled).
    ///
    /// Applied to every connection handed out by the pool, so it bounds *all* DB
    /// work - exports, imports, admin commands, health/auth queries, and
    /// migrations sharing the pool - not just export stages. Postgres cancels any
    /// statement exceeding it server-side, which frees the connection (a genuine
    /// in-flight timeout).
    #[clap(
        long,
        env = "HUBUUM_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    )]
    pub db_statement_timeout_ms: u64,

    /// Export-scoped Postgres `statement_timeout` in milliseconds (0 = disabled).
    ///
    /// Unlike `db_statement_timeout_ms`, this bounds *only* the queries issued
    /// while executing an export (scope query, includes, relation hydration). It
    /// is applied as a transaction-local `SET LOCAL statement_timeout` on those
    /// queries, so it does not affect imports, admin commands, or any other DB
    /// work sharing the pool. This lets operators cap export queries aggressively
    /// without capping legitimately long-running work. When set it should
    /// typically be `<= export_stage_timeout_ms` (the post-completion wall-clock
    /// budget). Disabled by default to preserve existing behavior.
    #[clap(
        long,
        env = "HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_EXPORT_DB_STATEMENT_TIMEOUT_MS
    )]
    pub export_db_statement_timeout_ms: u64,

    /// Maximum time to wait for a free database connection from the pool.
    #[clap(
        long,
        env = "HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS
    )]
    pub db_pool_acquire_timeout_ms: u64,

    /// Number of DB connections in the pool
    #[clap(long, env = "HUBUUM_DB_POOL_SIZE", default_value_t = 10)]
    pub db_pool_size: u32,

    /// Credential source collection policy.
    #[clap(
        long,
        env = "HUBUUM_CREDENTIAL_ACTIVITY_RETENTION_DAYS",
        default_value_t = 30
    )]
    pub credential_activity_retention_days: i64,

    /// Credential source collection policy.
    #[clap(
        long,
        env = "HUBUUM_CREDENTIAL_ACTIVITY_SOURCE_CAP",
        default_value_t = 32
    )]
    pub credential_activity_source_cap: usize,

    /// Credential source collection policy.
    #[clap(
        long,
        env = "HUBUUM_CREDENTIAL_ACTIVITY_ENABLED",
        default_value_t = true
    )]
    pub credential_activity_enabled: bool,

    /// Interactive session policy, in seconds.
    #[clap(long, env = "HUBUUM_SESSION_ACCESS_SECONDS", default_value_t = 900)]
    pub session_access_seconds: i64,

    /// Interactive session policy, in seconds.
    #[clap(long, env = "HUBUUM_SESSION_IDLE_SECONDS", default_value_t = 604800)]
    pub session_idle_seconds: i64,

    /// Interactive session policy, in seconds.
    #[clap(
        long,
        env = "HUBUUM_SESSION_ABSOLUTE_SECONDS",
        default_value_t = 2592000
    )]
    pub session_absolute_seconds: i64,

    /// Interactive session policy, in seconds.
    #[clap(
        long,
        env = "HUBUUM_SESSION_RETENTION_SECONDS",
        default_value_t = 2592000
    )]
    pub session_retention_seconds: i64,

    /// Token lifetime in hours
    #[clap(
        long,
        env = "HUBUUM_TOKEN_LIFETIME_HOURS",
        default_value_t = DEFAULT_TOKEN_LIFETIME_HOURS
    )]
    pub token_lifetime_hours: i64,

    /// Maximum lifetime accepted for an explicitly requested token expiry.
    #[clap(
        long,
        env = "HUBUUM_MAX_TOKEN_LIFETIME_HOURS",
        default_value_t = DEFAULT_MAX_TOKEN_LIFETIME_HOURS
    )]
    pub max_token_lifetime_hours: i64,

    /// Enable removal of token rows after terminal time plus the retention window.
    #[clap(
        long,
        env = "HUBUUM_TOKEN_RETENTION_PURGE_ENABLED",
        default_value_t = DEFAULT_TOKEN_RETENTION_PURGE_ENABLED
    )]
    pub token_retention_purge_enabled: bool,

    /// Days to retain terminal token metadata before permanent deletion.
    #[clap(
        long,
        env = "HUBUUM_TOKEN_RETENTION_DAYS",
        default_value_t = DEFAULT_TOKEN_RETENTION_DAYS
    )]
    pub token_retention_days: i64,

    /// How often the expired-token purge worker wakes up.
    #[clap(
        long,
        env = "HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS",
        default_value_t = DEFAULT_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS
    )]
    pub token_retention_purge_interval_seconds: u64,

    /// Maximum terminal tokens deleted in one transaction. Must be at least 10.
    #[clap(
        long,
        env = "HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE",
        default_value_t = DEFAULT_TOKEN_RETENTION_PURGE_BATCH_SIZE
    )]
    pub token_retention_purge_batch_size: usize,

    /// Master switch for login rate limiting. When false, no login throttling is applied.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_ENABLED",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_ENABLED
    )]
    pub login_rate_limit_enabled: bool,

    /// Maximum failed login attempts per (username, client IP) within the rate-limit window.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS
    )]
    pub login_rate_limit_max_attempts: usize,

    /// Maximum failed attempts per (identity scope, username) across all networks.
    /// `0` disables account-wide throttling.
    #[clap(long, env = "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_ACCOUNT",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_ACCOUNT)]
    pub login_rate_limit_max_attempts_per_account: usize,

    /// Maximum failed login attempts per client IP (across all usernames) within the
    /// rate-limit window. Throttles password spraying from a single host. `0` disables
    /// this scope.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP
    )]
    pub login_rate_limit_max_attempts_per_ip: usize,

    /// Maximum failed login attempts per client subnet within the rate-limit window.
    /// Throttles distributed spraying from one network. `0` disables this scope.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET
    )]
    pub login_rate_limit_max_attempts_per_subnet: usize,

    /// Login rate-limit sliding window in seconds.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS
    )]
    pub login_rate_limit_window_seconds: u64,

    /// Base lockout duration (seconds) applied the first time a scope crosses its
    /// threshold. Each subsequent lockout doubles, capped by the backoff maximum.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS
    )]
    pub login_rate_limit_backoff_base_seconds: u64,

    /// Maximum lockout duration (seconds) for exponential login backoff.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS
    )]
    pub login_rate_limit_backoff_max_seconds: u64,

    /// IPv4 prefix length used to aggregate client IPs into subnets for the per-subnet
    /// failure budget.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4
    )]
    pub login_rate_limit_subnet_prefix_v4: u8,

    /// IPv6 prefix length used to aggregate client IPs into subnets for the per-subnet
    /// failure budget.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6
    )]
    pub login_rate_limit_subnet_prefix_v6: u8,

    /// State backend for login throttling.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_BACKEND",
        default_value = "memory"
    )]
    pub login_rate_limit_backend: LoginRateLimitBackendKind,

    /// Valkey/Redis URL for shared login throttling state.
    #[clap(long, env = "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL")]
    pub login_rate_limit_valkey_url: Option<String>,

    /// Namespace prefix for shared login throttling keys.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_PREFIX",
        default_value = DEFAULT_LOGIN_RATE_LIMIT_VALKEY_PREFIX
    )]
    pub login_rate_limit_valkey_prefix: String,

    /// Valkey/Redis connect, read, and write timeout.
    #[clap(
        long,
        env = "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS",
        default_value_t = DEFAULT_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS
    )]
    pub login_rate_limit_valkey_io_timeout_ms: u64,

    /// Default number of items returned by cursor-paginated list endpoints
    #[clap(
        long,
        env = "HUBUUM_DEFAULT_PAGE_LIMIT",
        default_value_t = DEFAULT_PAGE_LIMIT
    )]
    pub default_page_limit: usize,

    /// Maximum number of items allowed for cursor-paginated list endpoints
    #[clap(
        long,
        env = "HUBUUM_MAX_PAGE_LIMIT",
        default_value_t = MAX_PAGE_LIMIT
    )]
    pub max_page_limit: usize,

    /// Maximum recursion depth for transitive relation graph walks
    #[clap(
        long,
        env = "HUBUUM_MAX_TRANSITIVE_DEPTH",
        default_value_t = DEFAULT_MAX_TRANSITIVE_DEPTH
    )]
    pub max_transitive_depth: i32,

    /// The name of the admin group
    #[clap(long, env = "HUBUUM_ADMIN_GROUPNAME", default_value = "admin")]
    pub admin_groupname: String,

    /// Identity scope that contains the admin group.
    #[clap(long, env = "HUBUUM_ADMIN_IDENTITY_SCOPE", default_value = "local")]
    pub admin_identity_scope: Option<String>,

    /// Optional path to TOML auth provider configuration.
    #[clap(long, env = "HUBUUM_AUTH_CONFIG_PATH", default_value = None)]
    pub auth_config_path: Option<String>,

    /// Secret-source name of the active key for runtime-stored credentials.
    #[clap(
        long,
        env = "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF",
        default_value = None,
        hide_env_values = true
    )]
    pub runtime_settings_encryption_key_ref: Option<String>,

    /// Path to the TLS certificate chain file
    #[clap(long, env = "HUBUUM_TLS_CERT_PATH", default_value = None)]
    pub tls_cert_path: Option<String>,

    /// Path to the TLS private key file
    #[clap(long, env = "HUBUUM_TLS_KEY_PATH", default_value = None)]
    pub tls_key_path: Option<String>,

    /// Optional passphrase to decrypt an encrypted PEM key
    #[clap(long, env = "HUBUUM_TLS_KEY_PASSPHRASE", default_value = None)]
    pub tls_key_passphrase: Option<String>,

    /// Preferred TLS backend when TLS is enabled
    #[clap(
        long,
        env = "HUBUUM_TLS_BACKEND",
        value_enum,
        ignore_case = true,
        default_value = None
    )]
    pub tls_backend: Option<TlsBackend>,

    /// Active permission backend.
    #[clap(
        long,
        env = "HUBUUM_PERMISSION_BACKEND",
        value_enum,
        ignore_case = true,
        default_value = "local"
    )]
    pub permission_backend: PermissionBackendKind,

    /// Treetop server URL (required when HUBUUM_PERMISSION_BACKEND=treetop).
    #[clap(long, env = "HUBUUM_TREETOP_URL", default_value = None)]
    pub treetop_url: Option<String>,

    /// Treetop client connect timeout in milliseconds.
    #[clap(
        long,
        env = "HUBUUM_TREETOP_CONNECT_TIMEOUT_MS",
        default_value_t = 5000
    )]
    pub treetop_connect_timeout_ms: u64,

    /// Treetop client request timeout in milliseconds.
    #[clap(
        long,
        env = "HUBUUM_TREETOP_REQUEST_TIMEOUT_MS",
        default_value_t = 30000
    )]
    pub treetop_request_timeout_ms: u64,

    /// Optional path to a PEM CA certificate bundle to trust for the Treetop client.
    #[clap(long, env = "HUBUUM_TREETOP_CA_CERT", default_value = None)]
    pub treetop_ca_cert: Option<String>,

    /// Accept invalid Treetop server certificates (development only).
    #[clap(
        long,
        env = "HUBUUM_TREETOP_ACCEPT_INVALID_CERTS",
        default_value_t = false
    )]
    pub treetop_accept_invalid_certs: bool,

    /// Enable the Prometheus metrics scrape endpoint.
    #[clap(
        long,
        env = "HUBUUM_METRICS_ENABLED",
        default_value_t = DEFAULT_METRICS_ENABLED
    )]
    pub metrics_enabled: bool,

    /// HTTP path for the Prometheus metrics scrape endpoint.
    #[clap(
        long,
        env = "HUBUUM_METRICS_PATH",
        default_value = DEFAULT_METRICS_PATH
    )]
    pub metrics_path: MetricsPath,

    /// Trust proxy IP headers (X-Forwarded-For). If false, use peer address only.
    ///
    /// When true, the real client IP is resolved from the right of the
    /// `[X-Forwarded-For..., peer]` hop chain using `trusted_proxies` (preferred) or
    /// `trusted_proxy_hops`. If neither is configured, forwarded headers are ignored
    /// and the peer address is used (forwarded values are never trusted blindly).
    #[clap(long, default_value = "false", env = "HUBUUM_TRUST_IP_HEADERS")]
    pub trust_ip_headers: bool,

    /// Trusted reverse-proxy IPs/CIDRs. When `trust_ip_headers` is set, hops in this set
    /// are skipped (from the connection peer inward) and the first untrusted hop is taken
    /// as the client. Comma-separated; empty disables allowlist-based resolution.
    #[clap(long, default_value = "", env = "HUBUUM_TRUSTED_PROXIES")]
    pub trusted_proxies: TrustedProxies,

    /// Number of trusted proxy hops in front of the server. Used only when
    /// `trust_ip_headers` is set and `trusted_proxies` is empty: this many hops are
    /// skipped from the right of the hop chain. `0` means do not trust forwarded headers.
    #[clap(
        long,
        env = "HUBUUM_TRUSTED_PROXY_HOPS",
        default_value_t = DEFAULT_TRUSTED_PROXY_HOPS
    )]
    pub trusted_proxy_hops: usize,

    /// Whitelist of client IPs or CIDRs ("*" allows all)
    #[clap(long, default_value = "127.0.0.1,::1", env = "HUBUUM_CLIENT_ALLOWLIST")]
    pub client_allowlist: ClientAllowlist,
}

pub(crate) fn app_command() -> clap::Command {
    AppConfig::command()
}

impl AppConfig {
    pub fn backup_budget(&self) -> Result<StorageBackupBudget, ApiError> {
        StorageBackupBudget::new(self.backup_max_output_bytes, self.backup_max_capture_rows)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub(crate) fn from_cli_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        let mut config = Self::from_arg_matches(matches)?;
        config.database_url_override =
            CommandLineDatabaseUrl::from_matches(matches, "database_url");
        Ok(config)
    }

    /// Backend-neutral export read budget used by the application boundary.
    ///
    /// The legacy PostgreSQL-named configuration field remains the operator
    /// compatibility input. Only configuration and the selected adapter know
    /// how the budget is implemented.
    #[must_use]
    pub const fn export_storage_query_budget_ms(&self) -> u64 {
        self.export_db_statement_timeout_ms
    }
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("AppConfig")
            .field(&running::RunningConfig::from(self))
            .finish()
    }
}

impl AppConfig {
    pub(crate) fn token_issuance_policy(&self) -> Result<TokenIssuancePolicy, ApiError> {
        Ok(TokenIssuancePolicy::from_hours(
            self.token_lifetime_hours,
            self.max_token_lifetime_hours,
        )?)
    }

    pub fn credential_activity_policy(
        &self,
    ) -> Result<hubuum_storage_core::CredentialActivityPolicy, ApiError> {
        hubuum_storage_core::CredentialActivityPolicy::try_new(
            self.credential_activity_enabled,
            self.credential_activity_retention_days,
            self.credential_activity_source_cap,
        )
        .map_err(|e| ApiError::BadRequest(e.to_string()))
    }
    pub fn session_policy(&self) -> Result<hubuum_storage_core::SessionPolicy, ApiError> {
        hubuum_storage_core::SessionPolicy::try_new(
            self.session_access_seconds,
            self.session_idle_seconds,
            self.session_absolute_seconds,
            self.session_retention_seconds,
        )
        .map_err(|e| ApiError::BadRequest(e.to_string()))
    }

    pub fn token_retention_settings(&self) -> Result<TokenRetentionSettings, ApiError> {
        Ok(TokenRetentionSettings::builder()
            .retention_days(self.token_retention_days)
            .token_lifetime_hours(self.token_lifetime_hours)
            .batch_size(self.token_retention_purge_batch_size)
            .build()?)
    }

    pub fn task_worker_settings(&self) -> Result<TaskWorkerSettings, ApiError> {
        let limits = crate::tasks::TaskExecutionLimits::default()
            .with_seconds(
                crate::models::TaskKind::Import,
                self.task_import_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?
            .with_seconds(
                crate::models::TaskKind::Export,
                self.task_export_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?
            .with_seconds(
                crate::models::TaskKind::Backup,
                self.task_backup_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?
            .with_seconds(
                crate::models::TaskKind::Reindex,
                self.task_reindex_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?
            .with_seconds(
                crate::models::TaskKind::RemoteCall,
                self.task_remote_call_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?
            .with_seconds(
                crate::models::TaskKind::SchemaProfiling,
                self.task_schema_profiling_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?
            .with_seconds(
                crate::models::TaskKind::SchemaValidation,
                self.task_schema_validation_execution_timeout_seconds,
            )
            .map_err(ApiError::BadRequest)?;
        TaskWorkerSettings::builder()
            .execution_limits(limits)
            .worker_count(self.runtime_role.effective_worker_count(self.task_workers))
            .poll_interval(Duration::from_millis(self.task_poll_interval_ms))
            .lease_duration(Duration::from_secs(self.task_lease_seconds))
            .heartbeat_interval(Duration::from_secs(self.task_heartbeat_seconds))
            .recovery_interval(Duration::from_secs(self.task_recovery_interval_seconds))
            .export_output_cleanup_interval(Duration::from_secs(
                self.export_output_cleanup_interval_seconds,
            ))
            .build()
            .map_err(ApiError::BadRequest)
    }

    pub(crate) fn event_fanout_settings(&self) -> Result<EventFanoutSettings, ApiError> {
        Ok(EventFanoutSettings::new(
            self.event_fanout_batch_size,
            self.event_fanout_lock_timeout_ms,
        )?)
    }

    pub(crate) fn event_delivery_settings(&self) -> Result<EventDeliverySettings, ApiError> {
        Ok(EventDeliverySettings::builder()
            .batch_size(self.event_delivery_batch_size)
            .lock_timeout_ms(self.event_delivery_lock_timeout_ms)
            .transport_timeout_ms(self.event_delivery_transport_timeout_ms)
            .retry_backoff_base_ms(self.event_delivery_retry_backoff_base_ms)
            .retry_backoff_max_ms(self.event_delivery_retry_backoff_max_ms)
            .max_attempts(self.event_delivery_max_attempts)
            .build()?)
    }

    pub(crate) fn event_retention_settings(&self) -> Result<EventRetentionSettings, ApiError> {
        Ok(EventRetentionSettings::new(
            self.event_retention_days,
            self.event_delivery_retention_days,
            self.event_retention_purge_batch_size,
        )?)
    }

    pub(crate) const fn schema_limits(&self) -> JsonSchemaLimits {
        self.schema_limits
    }

    pub(crate) const fn query_observation_settings(&self) -> StorageQueryObservationSettings {
        self.query_observation_settings
    }

    fn validate(mut self) -> Result<Self, ApiError> {
        self.query_observation_settings = self.query_observations.validate()?;
        self.schema_limits = self.schema_validation.validate()?;
        if self.actix_workers == 0 {
            return Err(ApiError::BadRequest(
                "actix_workers must be greater than 0".to_string(),
            ));
        }

        let has_background_worker = self.task_workers > 0
            || self.event_fanout_workers > 0
            || self.event_delivery_workers > 0
            || self.event_retention_purge_enabled
            || self.token_retention_purge_enabled;
        if !constraints::WORKER_ROLE.requirement_is_satisfied(
            self.runtime_role == RuntimeRole::Worker,
            has_background_worker,
        ) {
            return Err(ApiError::BadRequest(
                "runtime_role=worker requires at least one enabled task, event fan-out, event delivery, event retention, or token retention worker"
                    .to_string(),
            ));
        }

        self.task_worker_settings()?;

        if self.event_fanout_poll_interval_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_fanout_poll_interval_ms must be greater than 0".to_string(),
            ));
        }

        self.event_fanout_settings()?;

        if self.event_delivery_poll_interval_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_poll_interval_ms must be greater than 0".to_string(),
            ));
        }

        self.event_delivery_settings()?;
        self.event_retention_settings()?;

        if self.event_retention_purge_interval_seconds == 0 {
            return Err(ApiError::BadRequest(
                "event_retention_purge_interval_seconds must be greater than 0".to_string(),
            ));
        }

        if !constraints::RETENTION_ARCHIVE.requirement_is_satisfied(
            self.event_retention_file_archive_enabled,
            self.event_retention_archive_path.is_some(),
        ) {
            return Err(ApiError::BadRequest(
                "event_retention_archive_path is required when event_retention_file_archive_enabled is true".to_string(),
            ));
        }

        FutureRetention::from_hours(
            self.export_output_retention_hours,
            "export_output_retention_hours",
        )
        .map_err(ApiError::BadRequest)?;

        FutureRetention::from_hours(
            self.backup_output_retention_hours,
            "backup_output_retention_hours",
        )
        .map_err(ApiError::BadRequest)?;

        if self.backup_max_active_tasks_per_user == 0 {
            return Err(ApiError::BadRequest(
                "backup_max_active_tasks_per_user must be greater than 0".to_string(),
            ));
        }

        if self.backup_max_capture_rows == 0 {
            return Err(ApiError::BadRequest(
                "backup_max_capture_rows must be greater than 0".to_string(),
            ));
        }

        if self.backup_max_output_bytes == 0 {
            return Err(ApiError::BadRequest(
                "backup_max_output_bytes must be greater than 0".to_string(),
            ));
        }

        FutureRetention::from_minutes(
            self.restore_stage_retention_minutes,
            "restore_stage_retention_minutes",
        )
        .map_err(ApiError::BadRequest)?;

        if self.restore_max_upload_bytes == 0 {
            return Err(ApiError::BadRequest(
                "restore_max_upload_bytes must be greater than 0".to_string(),
            ));
        }

        if self.import_max_active_tasks_per_user == 0 {
            return Err(ApiError::BadRequest(
                "import_max_active_tasks_per_user must be greater than 0".to_string(),
            ));
        }

        if self.export_max_active_tasks_per_user == 0 {
            return Err(ApiError::BadRequest(
                "export_max_active_tasks_per_user must be greater than 0".to_string(),
            ));
        }

        if self.export_template_recursion_limit == 0 {
            return Err(ApiError::BadRequest(
                "export_template_recursion_limit must be greater than 0".to_string(),
            ));
        }

        if self.export_template_fuel == 0 {
            return Err(ApiError::BadRequest(
                "export_template_fuel must be greater than 0".to_string(),
            ));
        }

        if self.export_template_max_objects == 0 {
            return Err(ApiError::BadRequest(
                "export_template_max_objects must be greater than 0".to_string(),
            ));
        }

        if self.export_max_output_bytes == 0 {
            return Err(ApiError::BadRequest(
                "export_max_output_bytes must be greater than 0".to_string(),
            ));
        }

        if self.export_stage_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "export_stage_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.remote_call_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "remote_call_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.remote_call_max_response_bytes == 0 {
            return Err(ApiError::BadRequest(
                "remote_call_max_response_bytes must be greater than 0".to_string(),
            ));
        }

        if self.remote_call_max_active_tasks_per_user == 0 {
            return Err(ApiError::BadRequest(
                "remote_call_max_active_tasks_per_user must be greater than 0".to_string(),
            ));
        }

        if self.db_pool_size == 0 {
            return Err(ApiError::BadRequest(
                "db_pool_size must be greater than 0".to_string(),
            ));
        }

        if self.db_pool_acquire_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "db_pool_acquire_timeout_ms must be greater than 0".to_string(),
            ));
        }

        self.token_issuance_policy()?;
        self.credential_activity_policy()?;
        self.session_policy()?;
        self.token_retention_settings()?;

        if self.token_retention_purge_interval_seconds == 0 {
            return Err(ApiError::BadRequest(
                "token_retention_purge_interval_seconds must be greater than 0".to_string(),
            ));
        }

        if self.login_rate_limit_max_attempts == 0 {
            return Err(ApiError::BadRequest(
                "login_rate_limit_max_attempts must be greater than 0".to_string(),
            ));
        }

        if self.login_rate_limit_window_seconds == 0 {
            return Err(ApiError::BadRequest(
                "login_rate_limit_window_seconds must be greater than 0".to_string(),
            ));
        }

        if self.login_rate_limit_backoff_base_seconds == 0 {
            return Err(ApiError::BadRequest(
                "login_rate_limit_backoff_base_seconds must be greater than 0".to_string(),
            ));
        }

        if !constraints::LOGIN_BACKOFF.ordered_values_satisfy(
            self.login_rate_limit_backoff_base_seconds,
            self.login_rate_limit_backoff_max_seconds,
        ) {
            return Err(ApiError::BadRequest(format!(
                "login_rate_limit_backoff_max_seconds ({}) must be greater than or equal to login_rate_limit_backoff_base_seconds ({})",
                self.login_rate_limit_backoff_max_seconds,
                self.login_rate_limit_backoff_base_seconds
            )));
        }

        if self.login_rate_limit_subnet_prefix_v4 == 0
            || self.login_rate_limit_subnet_prefix_v4 > 32
        {
            return Err(ApiError::BadRequest(
                "login_rate_limit_subnet_prefix_v4 must be between 1 and 32".to_string(),
            ));
        }

        if self.login_rate_limit_subnet_prefix_v6 == 0
            || self.login_rate_limit_subnet_prefix_v6 > 128
        {
            return Err(ApiError::BadRequest(
                "login_rate_limit_subnet_prefix_v6 must be between 1 and 128".to_string(),
            ));
        }

        if self.computed_reindex_batch_size == 0
            || self.computed_reindex_batch_size > MAX_COMPUTED_REINDEX_BATCH_SIZE
        {
            return Err(ApiError::BadRequest(format!(
                "computed_reindex_batch_size must be between 1 and {MAX_COMPUTED_REINDEX_BATCH_SIZE}"
            )));
        }

        if self.login_rate_limit_valkey_io_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "login_rate_limit_valkey_io_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.login_rate_limit_valkey_prefix.trim().is_empty()
            || self.login_rate_limit_valkey_prefix.contains(['{', '}'])
        {
            return Err(ApiError::BadRequest(
                "login_rate_limit_valkey_prefix must not be empty or contain braces".to_string(),
            ));
        }

        if self.login_rate_limit_backend == LoginRateLimitBackendKind::Valkey {
            #[cfg(not(feature = "login-rate-limit-valkey"))]
            if !constraints::VALKEY_URL.requirement_is_satisfied(true, false) {
                return Err(ApiError::BadRequest(
                    "login_rate_limit_backend=valkey requires the login-rate-limit-valkey feature"
                        .to_string(),
                ));
            }

            #[cfg(feature = "login-rate-limit-valkey")]
            if !constraints::VALKEY_URL.requirement_is_satisfied(
                true,
                self.login_rate_limit_valkey_url
                    .as_deref()
                    .is_some_and(|url| !url.trim().is_empty()),
            ) {
                return Err(ApiError::BadRequest(
                    "login_rate_limit_valkey_url is required when login_rate_limit_backend=valkey"
                        .to_string(),
                ));
            }
        }

        if !(1..=hubuum_query::MAX_TRAVERSAL_DEPTH).contains(&self.max_transitive_depth) {
            return Err(ApiError::BadRequest(format!(
                "max_transitive_depth must be between 1 and {}",
                hubuum_query::MAX_TRAVERSAL_DEPTH
            )));
        }

        if self
            .admin_identity_scope
            .as_deref()
            .is_some_and(|scope| scope.trim().is_empty())
        {
            return Err(ApiError::BadRequest(
                "admin_identity_scope must not be empty".to_string(),
            ));
        }

        if self
            .auth_config_path
            .as_deref()
            .is_some_and(|path| path.trim().is_empty())
        {
            return Err(ApiError::BadRequest(
                "auth_config_path must not be empty".to_string(),
            ));
        }

        if let Some(key_ref) = &self.runtime_settings_encryption_key_ref {
            hubuum_secrets::SecretName::new(key_ref.clone()).map_err(|_| {
                ApiError::BadRequest("runtime_settings_encryption_key_ref is invalid".to_string())
            })?;
        }

        PageLimits::new(self.default_page_limit, self.max_page_limit)?;

        if !constraints::TREETOP_BACKEND.requirement_is_satisfied(
            self.permission_backend == PermissionBackendKind::Treetop,
            self.treetop_url.is_some(),
        ) {
            return Err(ApiError::BadRequest(
                "treetop_url is required when permission_backend=treetop".to_string(),
            ));
        }

        crate::observability::tracing::TracingSettings::from_config(&self)
            .map_err(ApiError::BadRequest)?;
        validate_configuration_bounds(&self).map_err(ApiError::BadRequest)?;

        Ok(self)
    }
}

pub fn max_transitive_depth() -> i32 {
    if let Some(config) = runtime::scoped() {
        return config.max_transitive_depth;
    }
    #[cfg(test)]
    let depth = get_config_from_env()
        .map(|config| config.max_transitive_depth)
        .unwrap_or(DEFAULT_MAX_TRANSITIVE_DEPTH);

    #[cfg(not(test))]
    let depth = get_config()
        .map(|config| config.max_transitive_depth)
        .unwrap_or(DEFAULT_MAX_TRANSITIVE_DEPTH);

    depth
}

/// Snapshot of all login rate-limit knobs, resolved once per check so a single request
/// sees a consistent configuration.
#[derive(Debug, Clone, Copy)]
pub struct LoginRateLimitConfig {
    pub enabled: bool,
    pub max_attempts: usize,
    pub max_attempts_per_account: usize,
    pub max_attempts_per_ip: usize,
    pub max_attempts_per_subnet: usize,
    pub window_seconds: u64,
    pub backoff_base_seconds: u64,
    pub backoff_max_seconds: u64,
    pub subnet_prefix_v4: u8,
    pub subnet_prefix_v6: u8,
}

impl Default for LoginRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_LOGIN_RATE_LIMIT_ENABLED,
            max_attempts: DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS,
            max_attempts_per_account: DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_ACCOUNT,
            max_attempts_per_ip: DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP,
            max_attempts_per_subnet: DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET,
            window_seconds: DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS,
            backoff_base_seconds: DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS,
            backoff_max_seconds: DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS,
            subnet_prefix_v4: DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4,
            subnet_prefix_v6: DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6,
        }
    }
}

impl From<&AppConfig> for LoginRateLimitConfig {
    fn from(config: &AppConfig) -> Self {
        Self {
            enabled: config.login_rate_limit_enabled,
            max_attempts: config.login_rate_limit_max_attempts,
            max_attempts_per_account: config.login_rate_limit_max_attempts_per_account,
            max_attempts_per_ip: config.login_rate_limit_max_attempts_per_ip,
            max_attempts_per_subnet: config.login_rate_limit_max_attempts_per_subnet,
            window_seconds: config.login_rate_limit_window_seconds,
            backoff_base_seconds: config.login_rate_limit_backoff_base_seconds,
            backoff_max_seconds: config.login_rate_limit_backoff_max_seconds,
            subnet_prefix_v4: config.login_rate_limit_subnet_prefix_v4,
            subnet_prefix_v6: config.login_rate_limit_subnet_prefix_v6,
        }
    }
}

/// Resolve the active login rate-limit configuration.
pub fn login_rate_limit_config() -> LoginRateLimitConfig {
    if let Some(config) = runtime::scoped() {
        return LoginRateLimitConfig::from(config.as_ref());
    }
    #[cfg(test)]
    let config = get_config_from_env()
        .map(|config| LoginRateLimitConfig::from(&config))
        .unwrap_or_default();

    #[cfg(not(test))]
    let config = get_config()
        .map(LoginRateLimitConfig::from)
        .unwrap_or_default();

    config
}

#[cfg(not(test))]
fn load_config() -> Result<AppConfig, ApiError> {
    environment::validate_registry().map_err(ApiError::BadRequest)?;
    match AppConfig::command()
        .try_get_matches()
        .and_then(|matches| AppConfig::from_cli_matches(&matches))
    {
        Ok(config) => config.validate(),
        Err(error) => match error.kind() {
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => {
                error.exit()
            }
            _ => Err(ApiError::BadRequest(format!(
                "Invalid configuration: {error}"
            ))),
        },
    }
}

#[cfg(not(test))]
static CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Resolve and install the process configuration before starting async work.
///
/// Calling this more than once returns the first installed value. This makes
/// startup composition explicit while retaining a small compatibility accessor
/// for code that has not yet received consumer-owned settings.
#[cfg(not(test))]
pub fn initialize_config() -> Result<&'static AppConfig, ApiError> {
    if let Some(config) = CONFIG.get() {
        return Ok(config);
    }

    let config = load_config()?;
    Ok(CONFIG.get_or_init(|| config))
}

#[cfg(not(test))]
pub fn get_config() -> Result<&'static AppConfig, ApiError> {
    initialize_config()
}

#[cfg(all(not(test), feature = "integration-test-support"))]
pub(crate) fn initialize_integration_test_config() -> Result<&'static AppConfig, ApiError> {
    if let Some(config) = CONFIG.get() {
        return Ok(config);
    }

    let config = get_config_from_env()?;
    Ok(CONFIG.get_or_init(|| config))
}

#[cfg(test)]
static TEST_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[cfg(any(test, feature = "integration-test-support"))]
mod test_environment;
#[cfg(any(test, feature = "integration-test-support"))]
use test_environment::get_config_from_env;

#[cfg(test)]
pub fn get_config() -> Result<AppConfig, ApiError> {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    get_config_from_env()
}

#[cfg(test)]
mod tests;
