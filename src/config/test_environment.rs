use super::*;
#[cfg(test)]
use std::{env, ffi::OsString};

pub(super) fn get_config_from_env() -> Result<AppConfig, ApiError> {
    use std::env;

    environment::validate_registry().map_err(ApiError::BadRequest)?;

    fn execution_seconds(key: &str, default: u64) -> Result<u64, ApiError> {
        match env::var(key) {
            Ok(value) => value.parse().map_err(|_| {
                ApiError::BadRequest(format!("{key} must be a positive whole number of seconds"))
            }),
            Err(env::VarError::NotPresent) => Ok(default),
            Err(_) => Err(ApiError::BadRequest(format!("{key} must be valid UTF-8"))),
        }
    }

    // Helper function to read an environment variable or return a default value
    fn env_or_default(key: &str, default: &str) -> String {
        env::var(key).unwrap_or_else(|_| default.to_string())
    }

    fn env_or_default_opt(key: &str, default: Option<&str>) -> Option<String> {
        env::var(key).ok().or(default.map(String::from))
    }

    fn env_or_default_tls_backend(key: &str) -> Option<TlsBackend> {
        env::var(key).ok().map(|value| {
            TlsBackend::from_str(&value, true)
                .unwrap_or_else(|err| panic!("Invalid TLS backend in {key}: {value} ({err})"))
        })
    }

    fn env_or_default_permission_backend(key: &str, default: &str) -> PermissionBackendKind {
        env::var(key)
            .ok()
            .map(|value| {
                PermissionBackendKind::from_str(&value, true).unwrap_or_else(|err| {
                    panic!("Invalid permission backend in {key}: {value} ({err})")
                })
            })
            .unwrap_or_else(|| {
                PermissionBackendKind::from_str(default, true)
                    .expect("Invalid default permission backend")
            })
    }

    fn env_or_default_runtime_role(key: &str, default: RuntimeRole) -> RuntimeRole {
        env::var(key).map_or(default, |value| {
            RuntimeRole::from_str(&value, true)
                .unwrap_or_else(|err| panic!("Invalid runtime role in {key}: {value} ({err})"))
        })
    }

    fn env_or_default_storage_backend(
        key: &str,
        default: StorageBackendKind,
    ) -> StorageBackendKind {
        env::var(key).map_or(default, |value| {
            if value.is_empty() {
                default
            } else {
                StorageBackendKind::from_str(&value, true).unwrap_or_else(|err| {
                    panic!("Invalid storage backend in {key}: {value} ({err})")
                })
            }
        })
    }

    fn env_or_default_login_rate_limit_backend(
        key: &str,
        default: LoginRateLimitBackendKind,
    ) -> LoginRateLimitBackendKind {
        env::var(key).map_or(default, |value| {
            LoginRateLimitBackendKind::from_str(&value, true).unwrap_or_else(|err| {
                panic!("Invalid login rate-limit backend in {key}: {value} ({err})")
            })
        })
    }

    fn env_or_default_client_allowlist(key: &str, default: &str) -> ClientAllowlist {
        env::var(key)
            .unwrap_or_else(|_| default.to_string())
            .parse()
            .unwrap_or_else(|error| panic!("Invalid client allowlist in {key}: {error}"))
    }

    fn env_or_default_trusted_proxies(key: &str, default: &str) -> TrustedProxies {
        env::var(key)
            .unwrap_or_else(|_| default.to_string())
            .parse()
            .unwrap_or_else(|error| panic!("Invalid trusted proxies in {key}: {error}"))
    }

    let config = AppConfig {
        control_recovery: ControlRecoveryOptions::from_environment(),
        runtime_managed_groups: BTreeSet::new(),
        schema_validation: SchemaValidationOptions::from_environment()?,
        query_observations: QueryObservationOptions::from_environment()?,
        artifacts: artifacts::ArtifactOptions::from_environment()?,
        history_archive_filesystem: match env::var("HUBUUM_HISTORY_ARCHIVE_FILESYSTEM") {
            Ok(value) => Some(
                value
                    .parse()
                    .map_err(|error: &str| ApiError::BadRequest(error.into()))?,
            ),
            Err(env::VarError::NotPresent) => None,
            Err(_) => {
                return Err(ApiError::BadRequest(
                    "Archive destination must be valid UTF-8".into(),
                ));
            }
        },
        query_observation_settings: StorageQueryObservationSettings::default(),
        schema_limits: JsonSchemaLimits::default(),
        secrets: SecretSourceOptions::from_environment()
            .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        database_url_override: None,
        runtime_role: env_or_default_runtime_role("HUBUUM_RUNTIME_ROLE", RuntimeRole::All),
        storage_backend: env_or_default_storage_backend(
            "HUBUUM_STORAGE_BACKEND",
            StorageBackendKind::Postgres,
        ),
        bind_ip: env_or_default("HUBUUM_BIND_IP", "127.0.0.1"),
        port: env_or_default("HUBUUM_BIND_PORT", "8080")
            .parse()
            .unwrap_or(8080),
        log_level: env_or_default("HUBUUM_LOG_LEVEL", "debug"),
        tracing_enabled: env_or_default("HUBUUM_TRACING_ENABLED", "false")
            .parse()
            .unwrap_or(DEFAULT_TRACING_ENABLED),
        tracing_otlp_endpoint: env_or_default_opt("HUBUUM_TRACING_OTLP_ENDPOINT", None),
        tracing_otlp_headers: env_or_default_opt("HUBUUM_TRACING_OTLP_HEADERS", None),
        tracing_otlp_ca_cert: env_or_default_opt("HUBUUM_TRACING_OTLP_CA_CERT", None),
        tracing_otlp_client_cert: env_or_default_opt("HUBUUM_TRACING_OTLP_CLIENT_CERT", None),
        tracing_otlp_client_key: env_or_default_opt("HUBUUM_TRACING_OTLP_CLIENT_KEY", None),
        tracing_connect_timeout_ms: env_or_default("HUBUUM_TRACING_CONNECT_TIMEOUT_MS", "2000")
            .parse()
            .unwrap_or(DEFAULT_TRACING_CONNECT_TIMEOUT_MS),
        tracing_export_timeout_ms: env_or_default("HUBUUM_TRACING_EXPORT_TIMEOUT_MS", "5000")
            .parse()
            .unwrap_or(DEFAULT_TRACING_EXPORT_TIMEOUT_MS),
        tracing_flush_timeout_ms: env_or_default("HUBUUM_TRACING_FLUSH_TIMEOUT_MS", "5000")
            .parse()
            .unwrap_or(DEFAULT_TRACING_FLUSH_TIMEOUT_MS),
        tracing_queue_capacity: env_or_default("HUBUUM_TRACING_QUEUE_CAPACITY", "2048")
            .parse()
            .unwrap_or(DEFAULT_TRACING_QUEUE_CAPACITY),
        tracing_batch_size: env_or_default("HUBUUM_TRACING_BATCH_SIZE", "512")
            .parse()
            .unwrap_or(DEFAULT_TRACING_BATCH_SIZE),
        tracing_sampling_mode: env::var("HUBUUM_TRACING_SAMPLING_MODE")
            .ok()
            .and_then(|value| TracingSamplingMode::from_str(&value, true).ok())
            .unwrap_or_default(),
        tracing_sample_ratio: env_or_default("HUBUUM_TRACING_SAMPLE_RATIO", "0.1")
            .parse()
            .unwrap_or(DEFAULT_TRACING_SAMPLE_RATIO),
        tracing_service_name: env_or_default(
            "HUBUUM_TRACING_SERVICE_NAME",
            DEFAULT_TRACING_SERVICE_NAME,
        ),
        tracing_service_namespace: env_or_default(
            "HUBUUM_TRACING_SERVICE_NAMESPACE",
            DEFAULT_TRACING_SERVICE_NAMESPACE,
        ),
        tracing_deployment_environment: env_or_default(
            "HUBUUM_TRACING_DEPLOYMENT_ENVIRONMENT",
            DEFAULT_TRACING_DEPLOYMENT_ENVIRONMENT,
        ),
        tracing_trust_incoming_sampling: env_or_default(
            "HUBUUM_TRACING_TRUST_INCOMING_SAMPLING",
            "false",
        )
        .parse()
        .unwrap_or(false),
        tracing_propagate_outbound: env_or_default("HUBUUM_TRACING_PROPAGATE_OUTBOUND", "true")
            .parse()
            .unwrap_or(true),
        database_url: env_or_default("HUBUUM_DATABASE_URL", "postgres://test"),
        database_role_mode: env::var("HUBUUM_DATABASE_ROLE_MODE")
            .ok()
            .and_then(|value| DatabaseRoleMode::from_str(&value, true).ok())
            .unwrap_or_default(),
        database_privilege_mode: env::var("HUBUUM_DATABASE_PRIVILEGE_MODE")
            .ok()
            .and_then(|value| DatabasePrivilegeMode::from_str(&value, true).ok())
            .unwrap_or_default(),
        database_owner_role: env_or_default("HUBUUM_DATABASE_OWNER_ROLE", "hubuum_owner"),
        database_migrator_role: env_or_default("HUBUUM_DATABASE_MIGRATOR_ROLE", "hubuum_migrator"),
        database_runtime_role: env_or_default("HUBUUM_DATABASE_RUNTIME_ROLE", "hubuum_runtime"),
        actix_workers: env::var("HUBUUM_ACTIX_WORKERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(default_actix_workers),
        task_workers: env::var("HUBUUM_TASK_WORKERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(default_task_workers),
        task_poll_interval_ms: env::var("HUBUUM_TASK_POLL_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_TASK_POLL_INTERVAL_MS),
        task_lease_seconds: env::var("HUBUUM_TASK_LEASE_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_TASK_LEASE_SECONDS),
        task_heartbeat_seconds: env::var("HUBUUM_TASK_HEARTBEAT_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_TASK_HEARTBEAT_SECONDS),
        task_import_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_IMPORT_EXECUTION_TIMEOUT_SECONDS",
            3600,
        )?,
        task_export_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_EXPORT_EXECUTION_TIMEOUT_SECONDS",
            900,
        )?,
        task_backup_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_BACKUP_EXECUTION_TIMEOUT_SECONDS",
            3600,
        )?,
        task_reindex_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_REINDEX_EXECUTION_TIMEOUT_SECONDS",
            7200,
        )?,
        task_remote_call_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_REMOTE_CALL_EXECUTION_TIMEOUT_SECONDS",
            300,
        )?,
        task_schema_profiling_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_SCHEMA_PROFILING_EXECUTION_TIMEOUT_SECONDS",
            300,
        )?,
        task_schema_validation_execution_timeout_seconds: execution_seconds(
            "HUBUUM_TASK_SCHEMA_VALIDATION_EXECUTION_TIMEOUT_SECONDS",
            7200,
        )?,
        task_recovery_interval_seconds: env::var("HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_TASK_RECOVERY_INTERVAL_SECONDS),
        computed_reindex_batch_size: env::var("HUBUUM_COMPUTED_REINDEX_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_COMPUTED_REINDEX_BATCH_SIZE),
        event_fanout_workers: env::var("HUBUUM_EVENT_FANOUT_WORKERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_FANOUT_WORKERS),
        event_fanout_batch_size: env::var("HUBUUM_EVENT_FANOUT_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_FANOUT_BATCH_SIZE),
        event_fanout_poll_interval_ms: env::var("HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS),
        event_fanout_lock_timeout_ms: env::var("HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS),
        event_delivery_workers: env::var("HUBUUM_EVENT_DELIVERY_WORKERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_WORKERS),
        event_delivery_batch_size: env::var("HUBUUM_EVENT_DELIVERY_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_BATCH_SIZE),
        event_delivery_poll_interval_ms: env::var("HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS),
        event_delivery_lock_timeout_ms: env::var("HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS),
        event_delivery_transport_timeout_ms: env::var("HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS),
        event_delivery_retry_backoff_base_ms: env::var(
            "HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS",
        )
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS),
        event_delivery_retry_backoff_max_ms: env::var("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS),
        event_delivery_max_attempts: env::var("HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS),
        event_retention_purge_enabled: env::var("HUBUUM_EVENT_RETENTION_PURGE_ENABLED")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_RETENTION_PURGE_ENABLED),
        event_retention_days: env::var("HUBUUM_EVENT_RETENTION_DAYS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_RETENTION_DAYS),
        event_delivery_retention_days: env::var("HUBUUM_EVENT_DELIVERY_RETENTION_DAYS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_DELIVERY_RETENTION_DAYS),
        event_retention_purge_interval_seconds: env::var(
            "HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS",
        )
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS),
        event_retention_purge_batch_size: env::var("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE),
        event_retention_file_archive_enabled: env::var(
            "HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED",
        )
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED),
        event_retention_archive_path: env_or_default_opt(
            "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
            None,
        ),
        export_output_retention_hours: env::var("HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_OUTPUT_RETENTION_HOURS),
        export_output_cleanup_interval_seconds: env::var(
            "HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS",
        )
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS),
        backup_output_retention_hours: env::var("HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS),
        backup_max_active_tasks_per_user: env::var("HUBUUM_BACKUP_MAX_ACTIVE_TASKS_PER_USER")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER),
        backup_max_capture_rows: env::var("HUBUUM_BACKUP_MAX_CAPTURE_ROWS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_BACKUP_MAX_CAPTURE_ROWS),
        backup_max_output_bytes: env::var("HUBUUM_BACKUP_MAX_OUTPUT_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_BACKUP_MAX_OUTPUT_BYTES),
        restore_stage_retention_minutes: env::var("HUBUUM_RESTORE_STAGE_RETENTION_MINUTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_RESTORE_STAGE_RETENTION_MINUTES),
        restore_max_upload_bytes: env::var("HUBUUM_RESTORE_MAX_UPLOAD_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_RESTORE_MAX_UPLOAD_BYTES),
        import_max_active_tasks_per_user: env::var("HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_IMPORT_MAX_ACTIVE_TASKS_PER_USER),
        export_max_active_tasks_per_user: env::var("HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_MAX_ACTIVE_TASKS_PER_USER),
        export_template_recursion_limit: env::var("HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT),
        export_template_fuel: env::var("HUBUUM_EXPORT_TEMPLATE_FUEL")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_TEMPLATE_FUEL),
        export_template_max_objects: env::var("HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_TEMPLATE_MAX_OBJECTS),
        export_max_output_bytes: env::var("HUBUUM_EXPORT_MAX_OUTPUT_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_MAX_OUTPUT_BYTES),
        export_stage_timeout_ms: env::var("HUBUUM_EXPORT_STAGE_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_STAGE_TIMEOUT_MS),
        remote_call_timeout_ms: env::var("HUBUUM_REMOTE_CALL_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REMOTE_CALL_TIMEOUT_MS),
        remote_call_max_response_bytes: env::var("HUBUUM_REMOTE_CALL_MAX_RESPONSE_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES),
        remote_call_allow_private_targets: env::var("HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS),
        remote_call_max_active_tasks_per_user: env::var(
            "HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER",
        )
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER),
        db_statement_timeout_ms: env::var("HUBUUM_DB_STATEMENT_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_DB_STATEMENT_TIMEOUT_MS),
        export_db_statement_timeout_ms: env::var("HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EXPORT_DB_STATEMENT_TIMEOUT_MS),
        db_pool_acquire_timeout_ms: env::var("HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS),
        db_pool_size: env_or_default("HUBUUM_DB_POOL_SIZE", "2")
            .parse()
            .unwrap_or(5),
        credential_activity_retention_days: env_or_default(
            "HUBUUM_CREDENTIAL_ACTIVITY_RETENTION_DAYS",
            "30",
        )
        .parse()
        .unwrap_or(30),
        credential_activity_source_cap: env_or_default(
            "HUBUUM_CREDENTIAL_ACTIVITY_SOURCE_CAP",
            "32",
        )
        .parse()
        .unwrap_or(32),
        credential_activity_enabled: env_or_default("HUBUUM_CREDENTIAL_ACTIVITY_ENABLED", "true")
            .parse()
            .unwrap_or(true),
        session_access_seconds: env_or_default("HUBUUM_SESSION_ACCESS_SECONDS", "900")
            .parse()
            .unwrap_or(0),
        session_idle_seconds: env_or_default("HUBUUM_SESSION_IDLE_SECONDS", "604800")
            .parse()
            .unwrap_or(0),
        session_absolute_seconds: env_or_default("HUBUUM_SESSION_ABSOLUTE_SECONDS", "2592000")
            .parse()
            .unwrap_or(0),
        session_retention_seconds: env_or_default("HUBUUM_SESSION_RETENTION_SECONDS", "2592000")
            .parse()
            .unwrap_or(0),
        token_lifetime_hours: env_or_default("HUBUUM_TOKEN_LIFETIME_HOURS", "24")
            .parse()
            .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS),
        max_token_lifetime_hours: env_or_default("HUBUUM_MAX_TOKEN_LIFETIME_HOURS", "8760")
            .parse()
            .unwrap_or(DEFAULT_MAX_TOKEN_LIFETIME_HOURS),
        token_retention_purge_enabled: env_or_default(
            "HUBUUM_TOKEN_RETENTION_PURGE_ENABLED",
            "true",
        )
        .parse()
        .unwrap_or(DEFAULT_TOKEN_RETENTION_PURGE_ENABLED),
        token_retention_days: env_or_default("HUBUUM_TOKEN_RETENTION_DAYS", "30")
            .parse()
            .unwrap_or(DEFAULT_TOKEN_RETENTION_DAYS),
        token_retention_purge_interval_seconds: env_or_default(
            "HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS",
            "3600",
        )
        .parse()
        .unwrap_or(DEFAULT_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS),
        token_retention_purge_batch_size: env_or_default(
            "HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE",
            "1000",
        )
        .parse()
        .unwrap_or(DEFAULT_TOKEN_RETENTION_PURGE_BATCH_SIZE),
        login_rate_limit_enabled: env_or_default("HUBUUM_LOGIN_RATE_LIMIT_ENABLED", "true")
            .parse()
            .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_ENABLED),
        login_rate_limit_max_attempts: env_or_default("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "5")
            .parse()
            .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS),
        login_rate_limit_max_attempts_per_account: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_ACCOUNT",
            "20",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_ACCOUNT),
        login_rate_limit_max_attempts_per_ip: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP",
            "20",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP),
        login_rate_limit_max_attempts_per_subnet: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET",
            "100",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET),
        login_rate_limit_window_seconds: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS",
            "300",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS),
        login_rate_limit_backoff_base_seconds: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS",
            "300",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS),
        login_rate_limit_backoff_max_seconds: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS",
            "86400",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS),
        login_rate_limit_subnet_prefix_v4: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4",
            "24",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4),
        login_rate_limit_subnet_prefix_v6: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6",
            "64",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6),
        login_rate_limit_backend: env_or_default_login_rate_limit_backend(
            "HUBUUM_LOGIN_RATE_LIMIT_BACKEND",
            LoginRateLimitBackendKind::Memory,
        ),
        login_rate_limit_valkey_url: env_or_default_opt("HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL", None),
        login_rate_limit_valkey_prefix: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_PREFIX",
            DEFAULT_LOGIN_RATE_LIMIT_VALKEY_PREFIX,
        ),
        login_rate_limit_valkey_io_timeout_ms: env_or_default(
            "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS",
            "1000",
        )
        .parse()
        .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS),
        default_page_limit: env_or_default("HUBUUM_DEFAULT_PAGE_LIMIT", "100")
            .parse()
            .unwrap_or(DEFAULT_PAGE_LIMIT),
        max_page_limit: env_or_default("HUBUUM_MAX_PAGE_LIMIT", "250")
            .parse()
            .unwrap_or(MAX_PAGE_LIMIT),
        max_transitive_depth: env_or_default("HUBUUM_MAX_TRANSITIVE_DEPTH", "100")
            .parse()
            .unwrap_or(DEFAULT_MAX_TRANSITIVE_DEPTH),
        admin_groupname: env_or_default("HUBUUM_ADMIN_GROUPNAME", "admin"),
        admin_identity_scope: Some(env_or_default("HUBUUM_ADMIN_IDENTITY_SCOPE", "local")),
        auth_config_path: env_or_default_opt("HUBUUM_AUTH_CONFIG_PATH", None),
        runtime_settings_encryption_key_ref: env_or_default_opt(
            "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF",
            None,
        ),
        tls_cert_path: env_or_default_opt("HUBUUM_TLS_CERT_PATH", None),
        tls_key_path: env_or_default_opt("HUBUUM_TLS_KEY_PATH", None),
        tls_key_passphrase: env_or_default_opt("HUBUUM_TLS_KEY_PASSPHRASE", None),
        tls_backend: env_or_default_tls_backend("HUBUUM_TLS_BACKEND"),
        permission_backend: env_or_default_permission_backend("HUBUUM_PERMISSION_BACKEND", "local"),
        treetop_url: env_or_default_opt("HUBUUM_TREETOP_URL", None),
        treetop_connect_timeout_ms: env_or_default("HUBUUM_TREETOP_CONNECT_TIMEOUT_MS", "5000")
            .parse()
            .unwrap_or(5000),
        treetop_request_timeout_ms: env_or_default("HUBUUM_TREETOP_REQUEST_TIMEOUT_MS", "30000")
            .parse()
            .unwrap_or(30000),
        treetop_ca_cert: env_or_default_opt("HUBUUM_TREETOP_CA_CERT", None),
        treetop_accept_invalid_certs: env_or_default(
            "HUBUUM_TREETOP_ACCEPT_INVALID_CERTS",
            "false",
        )
        .parse()
        .unwrap_or(false),
        metrics_enabled: env_or_default("HUBUUM_METRICS_ENABLED", "true")
            .parse()
            .unwrap_or(DEFAULT_METRICS_ENABLED),
        metrics_path: MetricsPath::new(env_or_default("HUBUUM_METRICS_PATH", DEFAULT_METRICS_PATH))
            .map_err(ApiError::BadRequest)?,
        trust_ip_headers: env_or_default("HUBUUM_TRUST_IP_HEADERS", "false")
            .parse()
            .unwrap_or(false),
        trusted_proxies: env_or_default_trusted_proxies("HUBUUM_TRUSTED_PROXIES", ""),
        trusted_proxy_hops: env_or_default("HUBUUM_TRUSTED_PROXY_HOPS", "0")
            .parse()
            .unwrap_or(DEFAULT_TRUSTED_PROXY_HOPS),
        client_allowlist: env_or_default_client_allowlist(
            "HUBUUM_CLIENT_ALLOWLIST",
            "127.0.0.1,::1",
        ),
    };

    config.validate()
}

#[cfg(test)]
pub(super) struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

#[cfg(test)]
impl EnvVarGuard {
    pub(super) fn set(key: &'static str, value: Option<&str>) -> Self {
        let original = env::var_os(key);

        match value {
            Some(value) => unsafe { env::set_var(key, value) },
            None => unsafe { env::remove_var(key) },
        }

        Self { key, original }
    }
}

#[cfg(test)]
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => unsafe { env::set_var(self.key, value) },
            None => unsafe { env::remove_var(self.key) },
        }
    }
}
