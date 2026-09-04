//! Registry for environment variables owned by Hubuum.
//!
//! Configuration consumers receive typed values. Only startup/configuration
//! adapters should map these names to those values. Adding a variable requires
//! declaring its owner and sensitivity here, which prevents a consumer from
//! quietly introducing an ambiguous name such as `HUBUUM_TIMEOUT`.

use hubuum_domain::OperationalConstraint;

use crate::models::retention::{MAX_FUTURE_RETENTION_HOURS, MAX_FUTURE_RETENTION_MINUTES};

use super::AppConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvironmentOwner {
    Server,
    Tracing,
    Database,
    Tasks,
    Events,
    Exports,
    Backups,
    Restores,
    RemoteCalls,
    Authentication,
    Pagination,
    Relations,
    Network,
    Permissions,
    Operations,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exposure {
    Public,
    SensitiveMetadata,
    Secret,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvironmentVariable {
    pub name: &'static str,
    pub owner: EnvironmentOwner,
    pub exposure: Exposure,
}

macro_rules! option {
    ($name:literal, $owner:ident) => {
        EnvironmentVariable {
            name: $name,
            owner: EnvironmentOwner::$owner,
            exposure: Exposure::Public,
        }
    };
    ($name:literal, $owner:ident, sensitive) => {
        EnvironmentVariable {
            name: $name,
            owner: EnvironmentOwner::$owner,
            exposure: Exposure::Secret,
        }
    };
    ($name:literal, $owner:ident, metadata) => {
        EnvironmentVariable {
            name: $name,
            owner: EnvironmentOwner::$owner,
            exposure: Exposure::SensitiveMetadata,
        }
    };
}

/// Variables consumed by `AppConfig` through clap.
pub const APP_CONFIG_ENVIRONMENT: &[EnvironmentVariable] = &[
    option!("HUBUUM_BIND_IP", Server),
    option!("HUBUUM_BIND_PORT", Server),
    option!("HUBUUM_LOG_LEVEL", Server),
    option!("HUBUUM_TRACING_ENABLED", Tracing),
    option!("HUBUUM_TRACING_OTLP_ENDPOINT", Tracing, sensitive),
    option!("HUBUUM_TRACING_OTLP_HEADERS", Tracing, sensitive),
    option!("HUBUUM_TRACING_OTLP_CA_CERT", Tracing),
    option!("HUBUUM_TRACING_OTLP_CLIENT_CERT", Tracing),
    option!("HUBUUM_TRACING_OTLP_CLIENT_KEY", Tracing, sensitive),
    option!("HUBUUM_TRACING_CONNECT_TIMEOUT_MS", Tracing),
    option!("HUBUUM_TRACING_EXPORT_TIMEOUT_MS", Tracing),
    option!("HUBUUM_TRACING_FLUSH_TIMEOUT_MS", Tracing),
    option!("HUBUUM_TRACING_QUEUE_CAPACITY", Tracing),
    option!("HUBUUM_TRACING_BATCH_SIZE", Tracing),
    option!("HUBUUM_TRACING_SAMPLING_MODE", Tracing),
    option!("HUBUUM_TRACING_SAMPLE_RATIO", Tracing),
    option!("HUBUUM_TRACING_SERVICE_NAME", Tracing),
    option!("HUBUUM_TRACING_SERVICE_NAMESPACE", Tracing),
    option!("HUBUUM_TRACING_DEPLOYMENT_ENVIRONMENT", Tracing),
    option!("HUBUUM_TRACING_TRUST_INCOMING_SAMPLING", Tracing),
    option!("HUBUUM_TRACING_PROPAGATE_OUTBOUND", Tracing),
    option!("HUBUUM_ACTIX_WORKERS", Server),
    option!("HUBUUM_RUNTIME_ROLE", Server),
    option!("HUBUUM_STORAGE_BACKEND", Database),
    option!("HUBUUM_DATABASE_URL", Database, sensitive),
    option!("HUBUUM_DATABASE_ROLE_MODE", Database),
    option!("HUBUUM_DATABASE_PRIVILEGE_MODE", Database),
    option!("HUBUUM_DATABASE_OWNER_ROLE", Database),
    option!("HUBUUM_DATABASE_MIGRATOR_ROLE", Database),
    option!("HUBUUM_DATABASE_RUNTIME_ROLE", Database),
    option!("HUBUUM_DB_POOL_SIZE", Database),
    option!("HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS", Database),
    option!("HUBUUM_DB_STATEMENT_TIMEOUT_MS", Database),
    option!("HUBUUM_TASK_WORKERS", Tasks),
    option!("HUBUUM_TASK_POLL_INTERVAL_MS", Tasks),
    option!("HUBUUM_TASK_LEASE_SECONDS", Tasks),
    option!("HUBUUM_TASK_HEARTBEAT_SECONDS", Tasks),
    option!("HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS", Tasks),
    option!("HUBUUM_COMPUTED_REINDEX_BATCH_SIZE", Tasks),
    option!("HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER", Tasks),
    option!("HUBUUM_EVENT_FANOUT_WORKERS", Events),
    option!("HUBUUM_EVENT_FANOUT_BATCH_SIZE", Events),
    option!("HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS", Events),
    option!("HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS", Events),
    option!("HUBUUM_EVENT_DELIVERY_WORKERS", Events),
    option!("HUBUUM_EVENT_DELIVERY_BATCH_SIZE", Events),
    option!("HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS", Events),
    option!("HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS", Events),
    option!("HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS", Events),
    option!("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS", Events),
    option!("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS", Events),
    option!("HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS", Events),
    option!("HUBUUM_EVENT_DELIVERY_RETENTION_DAYS", Events),
    option!("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", Events),
    option!("HUBUUM_EVENT_RETENTION_DAYS", Events),
    option!("HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS", Events),
    option!("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE", Events),
    option!("HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED", Events),
    option!("HUBUUM_EVENT_RETENTION_ARCHIVE_PATH", Events),
    option!("HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS", Exports),
    option!("HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS", Exports),
    option!("HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS", Backups),
    option!("HUBUUM_BACKUP_MAX_ACTIVE_TASKS_PER_USER", Backups),
    option!("HUBUUM_BACKUP_MAX_OUTPUT_BYTES", Backups),
    option!("HUBUUM_RESTORE_STAGE_RETENTION_MINUTES", Restores),
    option!("HUBUUM_RESTORE_MAX_UPLOAD_BYTES", Restores),
    option!("HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER", Exports),
    option!("HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT", Exports),
    option!("HUBUUM_EXPORT_TEMPLATE_FUEL", Exports),
    option!("HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS", Exports),
    option!("HUBUUM_EXPORT_MAX_OUTPUT_BYTES", Exports),
    option!("HUBUUM_EXPORT_STAGE_TIMEOUT_MS", Exports),
    option!("HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS", Exports),
    option!("HUBUUM_REMOTE_CALL_TIMEOUT_MS", RemoteCalls),
    option!("HUBUUM_REMOTE_CALL_MAX_RESPONSE_BYTES", RemoteCalls),
    option!("HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS", RemoteCalls),
    option!("HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER", RemoteCalls),
    option!("HUBUUM_TOKEN_LIFETIME_HOURS", Authentication),
    option!("HUBUUM_MAX_TOKEN_LIFETIME_HOURS", Authentication),
    option!("HUBUUM_TOKEN_RETENTION_PURGE_ENABLED", Authentication),
    option!("HUBUUM_TOKEN_RETENTION_DAYS", Authentication),
    option!(
        "HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS",
        Authentication
    ),
    option!("HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE", Authentication),
    option!("HUBUUM_LOGIN_RATE_LIMIT_ENABLED", Authentication),
    option!("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", Authentication),
    option!(
        "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP",
        Authentication
    ),
    option!(
        "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET",
        Authentication
    ),
    option!("HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS", Authentication),
    option!(
        "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS",
        Authentication
    ),
    option!(
        "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS",
        Authentication
    ),
    option!("HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4", Authentication),
    option!("HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6", Authentication),
    option!("HUBUUM_LOGIN_RATE_LIMIT_BACKEND", Authentication),
    option!(
        "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL",
        Authentication,
        sensitive
    ),
    option!("HUBUUM_LOGIN_RATE_LIMIT_VALKEY_PREFIX", Authentication),
    option!(
        "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS",
        Authentication
    ),
    option!("HUBUUM_ADMIN_GROUPNAME", Authentication),
    option!("HUBUUM_ADMIN_IDENTITY_SCOPE", Authentication),
    option!("HUBUUM_AUTH_CONFIG_PATH", Authentication, metadata),
    option!("HUBUUM_DEFAULT_PAGE_LIMIT", Pagination),
    option!("HUBUUM_MAX_PAGE_LIMIT", Pagination),
    option!("HUBUUM_MAX_TRANSITIVE_DEPTH", Relations),
    option!("HUBUUM_TLS_CERT_PATH", Server),
    option!("HUBUUM_TLS_KEY_PATH", Server, metadata),
    option!("HUBUUM_TLS_KEY_PASSPHRASE", Server, sensitive),
    option!("HUBUUM_TLS_BACKEND", Server),
    option!("HUBUUM_METRICS_ENABLED", Server),
    option!("HUBUUM_METRICS_PATH", Server),
    option!("HUBUUM_TRUST_IP_HEADERS", Network),
    option!("HUBUUM_TRUSTED_PROXIES", Network),
    option!("HUBUUM_TRUSTED_PROXY_HOPS", Network),
    option!("HUBUUM_CLIENT_ALLOWLIST", Network),
    option!("HUBUUM_PERMISSION_BACKEND", Permissions),
    option!("HUBUUM_TREETOP_URL", Permissions, sensitive),
    option!("HUBUUM_TREETOP_CONNECT_TIMEOUT_MS", Permissions),
    option!("HUBUUM_TREETOP_REQUEST_TIMEOUT_MS", Permissions),
    option!("HUBUUM_TREETOP_CA_CERT", Permissions),
    option!("HUBUUM_TREETOP_ACCEPT_INVALID_CERTS", Permissions),
];

/// Exact Hubuum variables resolved outside clap's `AppConfig` adapter.
pub const PROCESS_ENVIRONMENT: &[EnvironmentVariable] = &[
    option!("HUBUUM_MIGRATION_DATABASE_URL", Database, sensitive),
    option!("HUBUUM_DATABASE_ROLE_TESTS", Operations),
    option!("HUBUUM_TOKEN_HASH_KEY", Authentication, sensitive),
    option!("HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID", Authentication),
    option!("HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS", Authentication),
    option!("HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY", Authentication),
    option!("HUBUUM_SECRET_SOURCE", Operations),
    option!("HUBUUM_SECRET_FILE_ROOT", Operations, metadata),
    option!("HUBUUM_BUILD_GIT_SHA", Operations),
    option!("HUBUUM_AUTH_CONFIG_HOST_PATH", Operations, metadata),
    option!("HUBUUM_TREETOP_TEST_URL", Permissions, sensitive),
    option!("HUBUUM_TREETOP_TEST_CONTAINER_NAME", Permissions),
    option!("HUBUUM_TREETOP_TEST_IMAGE", Permissions),
    option!("HUBUUM_TREETOP_TEST_REVISION", Permissions),
    option!("HUBUUM_TREETOP_TLS_TEST_URL", Permissions, sensitive),
    option!("HUBUUM_TREETOP_TEST_CA_CERT", Permissions),
];

/// Registered dynamic secret namespaces. The suffix is a consumer-supplied
/// reference validated by the corresponding secret resolver.
pub const DYNAMIC_SECRET_PREFIXES: &[(&str, EnvironmentOwner)] = &[
    ("HUBUUM_TOKEN_HASH_KEY_", EnvironmentOwner::Authentication),
    ("HUBUUM_REMOTE_SECRET_", EnvironmentOwner::RemoteCalls),
    ("HUBUUM_EVENT_SINK_SECRET_", EnvironmentOwner::Events),
    ("HUBUUM_LDAP_SECRET_", EnvironmentOwner::Authentication),
];

/// A numeric configuration bound together with the runtime value it validates.
pub(crate) struct ConfigurationBound {
    pub(crate) name: &'static str,
    minimum: Option<i64>,
    maximum: Option<i64>,
    value: fn(&AppConfig) -> i128,
}

macro_rules! configuration_bound {
    ($name:literal, $field:ident) => {
        ConfigurationBound {
            name: $name,
            minimum: Some(1),
            maximum: None,
            value: |config| config.$field as i128,
        }
    };
    ($name:literal, $field:ident, minimum = $minimum:expr) => {
        ConfigurationBound {
            name: $name,
            minimum: Some($minimum),
            maximum: None,
            value: |config| config.$field as i128,
        }
    };
    ($name:literal, $field:ident, maximum = $maximum:expr) => {
        ConfigurationBound {
            name: $name,
            minimum: Some(1),
            maximum: Some($maximum),
            value: |config| config.$field as i128,
        }
    };
}

/// Application-enforced numeric bounds for registered environment variables.
/// Runtime validation and the generated operational contract consume these
/// same entries, including the accessor for the value being validated.
pub(crate) const CONFIGURATION_BOUNDS: &[ConfigurationBound] = &[
    configuration_bound!("HUBUUM_ACTIX_WORKERS", actix_workers),
    configuration_bound!("HUBUUM_TASK_POLL_INTERVAL_MS", task_poll_interval_ms),
    configuration_bound!("HUBUUM_TASK_LEASE_SECONDS", task_lease_seconds),
    configuration_bound!("HUBUUM_TASK_HEARTBEAT_SECONDS", task_heartbeat_seconds),
    configuration_bound!(
        "HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS",
        task_recovery_interval_seconds
    ),
    configuration_bound!(
        "HUBUUM_COMPUTED_REINDEX_BATCH_SIZE",
        computed_reindex_batch_size,
        maximum = 1_000
    ),
    configuration_bound!("HUBUUM_EVENT_FANOUT_BATCH_SIZE", event_fanout_batch_size),
    configuration_bound!(
        "HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS",
        event_fanout_poll_interval_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS",
        event_fanout_lock_timeout_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_BATCH_SIZE",
        event_delivery_batch_size
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS",
        event_delivery_poll_interval_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS",
        event_delivery_lock_timeout_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS",
        event_delivery_transport_timeout_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS",
        event_delivery_retry_backoff_base_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS",
        event_delivery_retry_backoff_max_ms
    ),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS",
        event_delivery_max_attempts
    ),
    configuration_bound!("HUBUUM_EVENT_RETENTION_DAYS", event_retention_days),
    configuration_bound!(
        "HUBUUM_EVENT_DELIVERY_RETENTION_DAYS",
        event_delivery_retention_days
    ),
    configuration_bound!(
        "HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS",
        event_retention_purge_interval_seconds
    ),
    configuration_bound!(
        "HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE",
        event_retention_purge_batch_size
    ),
    configuration_bound!(
        "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS",
        export_output_retention_hours,
        maximum = MAX_FUTURE_RETENTION_HOURS
    ),
    configuration_bound!(
        "HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS",
        export_output_cleanup_interval_seconds
    ),
    configuration_bound!(
        "HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS",
        backup_output_retention_hours,
        maximum = MAX_FUTURE_RETENTION_HOURS
    ),
    configuration_bound!(
        "HUBUUM_BACKUP_MAX_ACTIVE_TASKS_PER_USER",
        backup_max_active_tasks_per_user
    ),
    configuration_bound!("HUBUUM_BACKUP_MAX_OUTPUT_BYTES", backup_max_output_bytes),
    configuration_bound!(
        "HUBUUM_RESTORE_STAGE_RETENTION_MINUTES",
        restore_stage_retention_minutes,
        maximum = MAX_FUTURE_RETENTION_MINUTES
    ),
    configuration_bound!("HUBUUM_RESTORE_MAX_UPLOAD_BYTES", restore_max_upload_bytes),
    configuration_bound!(
        "HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER",
        import_max_active_tasks_per_user
    ),
    configuration_bound!(
        "HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER",
        export_max_active_tasks_per_user
    ),
    configuration_bound!(
        "HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT",
        export_template_recursion_limit
    ),
    configuration_bound!("HUBUUM_EXPORT_TEMPLATE_FUEL", export_template_fuel),
    configuration_bound!(
        "HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS",
        export_template_max_objects
    ),
    configuration_bound!("HUBUUM_EXPORT_MAX_OUTPUT_BYTES", export_max_output_bytes),
    configuration_bound!("HUBUUM_EXPORT_STAGE_TIMEOUT_MS", export_stage_timeout_ms),
    configuration_bound!("HUBUUM_REMOTE_CALL_TIMEOUT_MS", remote_call_timeout_ms),
    configuration_bound!(
        "HUBUUM_REMOTE_CALL_MAX_RESPONSE_BYTES",
        remote_call_max_response_bytes
    ),
    configuration_bound!(
        "HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER",
        remote_call_max_active_tasks_per_user
    ),
    configuration_bound!(
        "HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS",
        db_pool_acquire_timeout_ms
    ),
    configuration_bound!("HUBUUM_DB_POOL_SIZE", db_pool_size),
    configuration_bound!(
        "HUBUUM_TOKEN_LIFETIME_HOURS",
        token_lifetime_hours,
        maximum = i32::MAX as i64
    ),
    configuration_bound!(
        "HUBUUM_MAX_TOKEN_LIFETIME_HOURS",
        max_token_lifetime_hours,
        maximum = i32::MAX as i64
    ),
    configuration_bound!("HUBUUM_TOKEN_RETENTION_DAYS", token_retention_days),
    configuration_bound!(
        "HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS",
        token_retention_purge_interval_seconds
    ),
    configuration_bound!(
        "HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE",
        token_retention_purge_batch_size,
        minimum = 10
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS",
        login_rate_limit_max_attempts
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS",
        login_rate_limit_window_seconds
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS",
        login_rate_limit_backoff_base_seconds
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS",
        login_rate_limit_backoff_max_seconds
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4",
        login_rate_limit_subnet_prefix_v4,
        maximum = 32
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6",
        login_rate_limit_subnet_prefix_v6,
        maximum = 128
    ),
    configuration_bound!(
        "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS",
        login_rate_limit_valkey_io_timeout_ms
    ),
    configuration_bound!("HUBUUM_DEFAULT_PAGE_LIMIT", default_page_limit),
    configuration_bound!("HUBUUM_MAX_PAGE_LIMIT", max_page_limit),
    configuration_bound!("HUBUUM_MAX_TRANSITIVE_DEPTH", max_transitive_depth),
];

pub(crate) fn configuration_bounds(name: &str) -> (Option<i64>, Option<i64>) {
    CONFIGURATION_BOUNDS
        .iter()
        .find(|bound| bound.name == name)
        .map_or((None, None), |bound| (bound.minimum, bound.maximum))
}

pub(crate) fn validate_configuration_bounds(config: &AppConfig) -> Result<(), String> {
    for bound in CONFIGURATION_BOUNDS {
        let value = (bound.value)(config);
        if bound
            .minimum
            .is_some_and(|minimum| value < i128::from(minimum))
            || bound
                .maximum
                .is_some_and(|maximum| value > i128::from(maximum))
        {
            return Err(format!(
                "{} must satisfy the registered operational bounds {:?}..={:?}",
                bound.name, bound.minimum, bound.maximum
            ));
        }
    }
    Ok(())
}

pub(crate) mod constraints {
    pub(crate) use hubuum_domain::{
        EVENT_DELIVERY_RETRY_BACKOFF_CONSTRAINT as DELIVERY_RETRY_BACKOFF,
        EVENT_DELIVERY_TRANSPORT_TIMEOUT_CONSTRAINT as DELIVERY_TRANSPORT_TIMEOUT,
        TOKEN_LIFETIME_CONSTRAINT as TOKEN_LIFETIME,
    };

    pub(crate) use crate::tasks::TASK_HEARTBEAT_CONSTRAINT as TASK_HEARTBEAT;

    use hubuum_domain::OperationalConstraint;

    pub(crate) const PAGE_LIMITS: OperationalConstraint = OperationalConstraint::less_than_or_equal(
        "HUBUUM_DEFAULT_PAGE_LIMIT",
        "HUBUUM_MAX_PAGE_LIMIT",
    );
    pub(crate) const WORKER_ROLE: OperationalConstraint = OperationalConstraint::requires(
        "HUBUUM_RUNTIME_ROLE=worker",
        "at least one task, fan-out, delivery, event-retention, or token-retention worker",
    );
    pub(crate) const RETENTION_ARCHIVE: OperationalConstraint = OperationalConstraint::requires(
        "HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED=true",
        "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
    );
    pub(crate) const LOGIN_BACKOFF: OperationalConstraint =
        OperationalConstraint::less_than_or_equal(
            "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS",
            "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS",
        );
    pub(crate) const TLS_KEY_PAIR: OperationalConstraint =
        OperationalConstraint::paired("HUBUUM_TLS_CERT_PATH", "HUBUUM_TLS_KEY_PATH");
    pub(crate) const TREETOP_BACKEND: OperationalConstraint = OperationalConstraint::requires(
        "HUBUUM_PERMISSION_BACKEND=treetop",
        "HUBUUM_TREETOP_URL and the compiled permissions-treetop feature",
    );
    pub(crate) const VALKEY_URL: OperationalConstraint = OperationalConstraint::requires(
        "HUBUUM_LOGIN_RATE_LIMIT_BACKEND=valkey",
        "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL and the compiled valkey feature",
    );
    pub(crate) const SECRET_FILE_ROOT: OperationalConstraint =
        OperationalConstraint::requires("HUBUUM_SECRET_SOURCE=file", "HUBUUM_SECRET_FILE_ROOT");
    pub(crate) const TOKEN_PREVIOUS_KEY_IDS: OperationalConstraint =
        OperationalConstraint::requires(
            "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS",
            "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID",
        );
    pub(crate) const STABLE_TOKEN_HASH_KEY: OperationalConstraint = OperationalConstraint::requires(
        "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true",
        "resolvable stable token hash key material",
    );
}

/// Cross-field configuration contracts shared with their runtime validators.
pub(crate) const CONFIGURATION_CONSTRAINTS: &[OperationalConstraint] = &[
    constraints::PAGE_LIMITS,
    constraints::TASK_HEARTBEAT,
    constraints::WORKER_ROLE,
    constraints::DELIVERY_TRANSPORT_TIMEOUT,
    constraints::DELIVERY_RETRY_BACKOFF,
    constraints::RETENTION_ARCHIVE,
    constraints::LOGIN_BACKOFF,
    constraints::TOKEN_LIFETIME,
    constraints::TLS_KEY_PAIR,
    constraints::TREETOP_BACKEND,
    constraints::VALKEY_URL,
    constraints::SECRET_FILE_ROOT,
    constraints::TOKEN_PREVIOUS_KEY_IDS,
    constraints::STABLE_TOKEN_HASH_KEY,
];

pub(crate) fn configuration_constraints() -> Vec<String> {
    CONFIGURATION_CONSTRAINTS
        .iter()
        .map(|constraint| constraint.expression())
        .collect()
}

/// Files allowed to translate Hubuum-owned environment values. This list is
/// intentionally small and reviewable. The application secret adapter owns
/// dynamic namespace mapping and injects resolved values into consumers.
pub const ENVIRONMENT_ADAPTER_PATHS: &[&str] = &[
    "src/config.rs",
    "src/config/token_hash.rs",
    "src/administration.rs",
    "src/logger.rs",
    "src/secrets.rs",
    "src/test_support.rs",
    "src/tests/temporal/mod.rs",
    "src/tests/permissions/live_treetop_parity.rs",
    "crates/hubuum-storage-postgres/src/test_support.rs",
];

pub fn declared(name: &str) -> bool {
    APP_CONFIG_ENVIRONMENT
        .iter()
        .chain(PROCESS_ENVIRONMENT)
        .any(|variable| variable.name == name)
        || DYNAMIC_SECRET_PREFIXES
            .iter()
            .any(|(prefix, _)| name.starts_with(prefix) && name.len() > prefix.len())
}

/// Check registry invariants before configuration parsing. These are also
/// covered by tests, but keeping the registry executable avoids it becoming a
/// test-only manifest that can silently drift in unusual build targets.
pub fn validate_registry() -> Result<(), String> {
    let mut names = std::collections::BTreeSet::new();
    for variable in APP_CONFIG_ENVIRONMENT.iter().chain(PROCESS_ENVIRONMENT) {
        if !variable.name.starts_with("HUBUUM_") {
            return Err(format!(
                "environment variable is outside the HUBUUM namespace: {}",
                variable.name
            ));
        }
        if !names.insert(variable.name) {
            return Err(format!(
                "environment variable is declared more than once: {}",
                variable.name
            ));
        }
        if !declared(variable.name) {
            return Err(format!(
                "registry entry cannot be resolved as declared: {}",
                variable.name
            ));
        }
        let duration_name = ["_TIMEOUT", "_INTERVAL", "_WINDOW", "_BACKOFF", "_LIFETIME"]
            .iter()
            .any(|marker| variable.name.contains(marker));
        let explicit_unit = ["_MS", "_SECONDS", "_MINUTES", "_HOURS", "_DAYS"]
            .iter()
            .any(|unit| variable.name.ends_with(unit));
        if duration_name && !explicit_unit {
            return Err(format!(
                "duration environment variable must declare its unit: {}",
                variable.name
            ));
        }

        // Reading both classifications here is intentional: every entry must
        // carry ownership and exposure metadata even when parsing needs only
        // the variable name.
        let _classification = (variable.owner, variable.exposure);
    }

    for (prefix, owner) in DYNAMIC_SECRET_PREFIXES {
        if !prefix.starts_with("HUBUUM_") || !prefix.ends_with('_') {
            return Err(format!("invalid dynamic secret prefix: {prefix}"));
        }
        let _owner = owner;
    }
    if ENVIRONMENT_ADAPTER_PATHS.is_empty() {
        return Err("at least one environment adapter must be declared".to_string());
    }
    Ok(())
}

#[cfg(test)]
fn declared_reference(name: &str) -> bool {
    declared(name)
        || DYNAMIC_SECRET_PREFIXES
            .iter()
            .any(|(prefix, _)| name == *prefix)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    use clap::CommandFactory;
    use regex::Regex;

    use super::*;
    use crate::config::AppConfig;

    #[test]
    fn clap_environment_is_exactly_the_declared_app_config_set() {
        let clap_names = AppConfig::command()
            .get_arguments()
            .filter_map(|argument| argument.get_env())
            .map(|name| name.to_string_lossy().into_owned())
            .collect::<BTreeSet<_>>();
        let declared_names = APP_CONFIG_ENVIRONMENT
            .iter()
            .map(|variable| variable.name.to_string())
            .collect::<BTreeSet<_>>();

        assert_eq!(clap_names, declared_names);
    }

    #[test]
    fn every_declared_variable_has_one_owner_and_unique_name() {
        let all = APP_CONFIG_ENVIRONMENT.iter().chain(PROCESS_ENVIRONMENT);
        let names = all
            .clone()
            .map(|variable| variable.name)
            .collect::<Vec<_>>();
        let unique = names.iter().copied().collect::<BTreeSet<_>>();

        assert_eq!(names.len(), unique.len());
        assert!(names.iter().all(|name| name.starts_with("HUBUUM_")));
    }

    #[test]
    fn undeclared_or_ambiguous_names_are_rejected() {
        assert!(!declared("HUBUUM_TIMEOUT"));
        assert!(!declared("HUBUUM_REMOTE_SECRET_"));
        assert!(declared("HUBUUM_REMOTE_SECRET_EXAMPLE"));
    }

    fn rust_sources(directory: &Path) -> Vec<PathBuf> {
        let mut sources = Vec::new();
        let mut pending = vec![directory.to_path_buf()];
        while let Some(path) = pending.pop() {
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    sources.push(path);
                }
            }
        }
        sources
    }

    fn workspace_sources() -> Vec<PathBuf> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        [root.join("src"), root.join("crates")]
            .iter()
            .flat_map(|directory| rust_sources(directory))
            .collect()
    }

    #[test]
    fn every_hubuum_environment_reference_in_every_crate_is_declared() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let registry = root.join("src/config/environment.rs");
        let variable = Regex::new(r"HUBUUM_[A-Z][A-Z0-9_]*").unwrap();
        let mut unknown = Vec::new();

        for path in workspace_sources()
            .into_iter()
            .filter(|path| path != &registry)
        {
            let source = fs::read_to_string(&path).unwrap();
            for name in variable.find_iter(&source).map(|matched| matched.as_str()) {
                if !declared_reference(name) {
                    unknown.push(format!(
                        "{}: {name}",
                        path.strip_prefix(root).unwrap().display()
                    ));
                }
            }
        }

        assert!(
            unknown.is_empty(),
            "undeclared Hubuum environment variables:\n{}",
            unknown.join("\n")
        );
    }

    #[test]
    fn only_declared_adapters_read_hubuum_environment_variables() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let registry = Path::new("src/config/environment.rs");
        let adapters = ENVIRONMENT_ADAPTER_PATHS
            .iter()
            .map(Path::new)
            .collect::<BTreeSet<_>>();
        let mut unexpected = Vec::new();

        for path in workspace_sources() {
            let relative = path.strip_prefix(root).unwrap();
            if relative == registry {
                continue;
            }
            let source = fs::read_to_string(&path).unwrap();
            let reads_environment = source.contains("env::var(")
                || source.contains("std::env::var(")
                || source.contains("env::var_os(")
                || source.contains("std::env::var_os(");
            if source.contains("HUBUUM_") && reads_environment && !adapters.contains(relative) {
                unexpected.push(relative.display().to_string());
            }
        }

        assert!(
            unexpected.is_empty(),
            "Hubuum environment access outside declared adapters:\n{}",
            unexpected.join("\n")
        );
    }

    #[test]
    fn administration_environment_access_stays_behind_the_library_boundary() {
        assert!(ENVIRONMENT_ADAPTER_PATHS.contains(&"src/administration.rs"));
        assert!(!ENVIRONMENT_ADAPTER_PATHS.contains(&"src/bin/admin.rs"));
    }
}
