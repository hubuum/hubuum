use super::test_environment::EnvVarGuard;
use std::collections::BTreeSet;

use clap::Parser;
use hubuum_domain::ResourceRevision;
use hubuum_events_core::EventContext;
use rstest::rstest;

use crate::models::runtime_settings::PaginationSettingsRequest;
use crate::pagination::PageLimits;

#[cfg(feature = "login-rate-limit-valkey")]
use super::LoginRateLimitBackendKind;
use super::{
    AppConfig, DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS, DEFAULT_EVENT_DELIVERY_BATCH_SIZE,
    DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS, DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS,
    DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS, DEFAULT_EVENT_DELIVERY_RETENTION_DAYS,
    DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS, DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS,
    DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS, DEFAULT_EVENT_DELIVERY_WORKERS,
    DEFAULT_EVENT_FANOUT_BATCH_SIZE, DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS, DEFAULT_EVENT_FANOUT_WORKERS,
    DEFAULT_EVENT_RETENTION_DAYS, DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED,
    DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE, DEFAULT_EVENT_RETENTION_PURGE_ENABLED,
    DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS, DEFAULT_EXPORT_MAX_ACTIVE_TASKS_PER_USER,
    DEFAULT_EXPORT_MAX_OUTPUT_BYTES, DEFAULT_IMPORT_MAX_ACTIVE_TASKS_PER_USER,
    DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS, DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS,
    DEFAULT_MAX_TOKEN_LIFETIME_HOURS, DEFAULT_PAGE_LIMIT,
    DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER, DEFAULT_TASK_POLL_INTERVAL_MS,
    DEFAULT_TOKEN_LIFETIME_HOURS, DEFAULT_TOKEN_RETENTION_DAYS,
    DEFAULT_TOKEN_RETENTION_PURGE_BATCH_SIZE, DEFAULT_TOKEN_RETENTION_PURGE_ENABLED,
    DEFAULT_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS, DatabaseRoleMode, MAX_PAGE_LIMIT, RuntimeRole,
    StorageBackendKind, TEST_ENV_LOCK, TlsBackend, default_actix_workers, default_task_workers,
    get_config_from_env, token_hash_key_bytes, token_hash_key_is_ephemeral,
};

#[test]
fn server_preserves_explicit_database_url_provenance() {
    use clap::CommandFactory;
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let matches = AppConfig::command()
        .try_get_matches_from(["hubuum-server", "--database-url", "postgres://cli/db"])
        .unwrap();
    let parsed = AppConfig::from_cli_matches(&matches).unwrap();
    assert_eq!(
        parsed.database_url_override.unwrap().as_str(),
        "postgres://cli/db"
    );
}

#[test]
fn operational_numeric_bounds_reference_registered_runtime_values() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let config = AppConfig::parse_from(["hubuum"]);
    super::environment::validate_configuration_bounds(&config).unwrap();

    let bounded_names = super::environment::CONFIGURATION_BOUNDS
        .iter()
        .map(|bound| bound.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        bounded_names.len(),
        super::environment::CONFIGURATION_BOUNDS.len(),
        "numeric configuration bounds must be unique"
    );

    let registered_names = super::environment::APP_CONFIG_ENVIRONMENT
        .iter()
        .map(|variable| variable.name)
        .collect::<BTreeSet<_>>();

    assert!(bounded_names.is_subset(&registered_names));
}

#[test]
fn operational_numeric_bounds_validate_the_registered_field_accessor() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let mut config = AppConfig::parse_from(["hubuum"]);
    config.computed_reindex_batch_size = 1_001;

    assert_eq!(
        super::environment::validate_configuration_bounds(&config),
        Err(
            "HUBUUM_COMPUTED_REINDEX_BATCH_SIZE must satisfy the registered operational bounds Some(1)..=Some(1000)"
                .to_string()
        )
    );
}

#[test]
fn tls_backend_env_var_is_parsed_by_clap_and_test_config_loader() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TLS_BACKEND", Some("OpEnSsL"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.tls_backend, Some(TlsBackend::Openssl));
    assert_eq!(loaded.tls_backend, Some(TlsBackend::Openssl));
}

#[test]
fn tls_backend_defaults_to_none_when_env_var_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TLS_BACKEND", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.tls_backend, None);
    assert_eq!(loaded.tls_backend, None);
}

#[test]
fn tls_backend_invalid_env_var_is_rejected_by_clap_parser() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TLS_BACKEND", Some("bogus"));

    let error = AppConfig::try_parse_from(["hubuum-server"]).unwrap_err();

    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
    assert!(error.to_string().contains("bogus"));
    assert!(error.to_string().contains("rustls"));
    assert!(error.to_string().contains("openssl"));
}

#[test]
fn page_limits_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _default_guard = EnvVarGuard::set("HUBUUM_DEFAULT_PAGE_LIMIT", Some("100"));
    let _max_guard = EnvVarGuard::set("HUBUUM_MAX_PAGE_LIMIT", Some("500"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.default_page_limit, 100);
    assert_eq!(parsed.max_page_limit, 500);
    assert_eq!(loaded.default_page_limit, 100);
    assert_eq!(loaded.max_page_limit, 500);
}

#[test]
fn page_limits_default_when_env_vars_are_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _default_guard = EnvVarGuard::set("HUBUUM_DEFAULT_PAGE_LIMIT", None);
    let _max_guard = EnvVarGuard::set("HUBUUM_MAX_PAGE_LIMIT", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.default_page_limit, DEFAULT_PAGE_LIMIT);
    assert_eq!(parsed.max_page_limit, MAX_PAGE_LIMIT);
    assert_eq!(loaded.default_page_limit, DEFAULT_PAGE_LIMIT);
    assert_eq!(loaded.max_page_limit, MAX_PAGE_LIMIT);
}

#[test]
fn page_limits_are_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _default_guard = EnvVarGuard::set("HUBUUM_DEFAULT_PAGE_LIMIT", Some("80"));
    let _max_guard = EnvVarGuard::set("HUBUUM_MAX_PAGE_LIMIT", Some("40"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "default_page_limit (80) must be less than or equal to max_page_limit (40)"
    );
}

#[rstest]
#[case::minimum(1, 250, true)]
#[case::ceiling(250, 250, true)]
#[case::zero(0, 250, false)]
#[case::above_ceiling(251, 250, false)]
#[case::storage_maximum(u64::from(u32::MAX), u64::from(u32::MAX), true)]
#[case::above_storage_maximum(u64::from(u32::MAX) + 1, u64::from(u32::MAX) + 1, false)]
fn environment_and_runtime_share_pagination_validation(
    #[case] default: u64,
    #[case] maximum: u64,
    #[case] accepted: bool,
) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _default_guard = EnvVarGuard::set("HUBUUM_DEFAULT_PAGE_LIMIT", Some(&default.to_string()));
    let _max_guard = EnvVarGuard::set("HUBUUM_MAX_PAGE_LIMIT", Some(&maximum.to_string()));
    let environment = get_config_from_env()
        .map(|_| ())
        .map_err(|error| error.to_string());
    let runtime = PaginationSettingsRequest {
        expected_revision: ResourceRevision::INITIAL,
        default_page_limit: default,
        max_page_limit: None,
    }
    .into_update(
        PageLimits::new(1, usize::try_from(maximum).unwrap()).unwrap(),
        EventContext::system(),
    )
    .map(|_| ())
    .map_err(|error| error.to_string());

    assert_eq!(environment.is_ok(), accepted);
    assert_eq!(environment, runtime);
}

#[test]
fn task_worker_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _workers_guard = EnvVarGuard::set("HUBUUM_TASK_WORKERS", Some("3"));
    let _interval_guard = EnvVarGuard::set("HUBUUM_TASK_POLL_INTERVAL_MS", Some("750"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.task_workers, 3);
    assert_eq!(parsed.task_poll_interval_ms, 750);
    assert_eq!(loaded.task_workers, 3);
    assert_eq!(loaded.task_poll_interval_ms, 750);
}

#[test]
fn runtime_role_is_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _role_guard = EnvVarGuard::set("HUBUUM_RUNTIME_ROLE", Some("worker"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.runtime_role, RuntimeRole::Worker);
    assert_eq!(loaded.runtime_role, RuntimeRole::Worker);
    assert!(!loaded.runtime_role.serves_http());
    assert!(loaded.runtime_role.runs_background_workers());
}

#[test]
fn database_role_mode_defaults_to_single_and_accepts_split_opt_in() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _role_mode_guard = EnvVarGuard::set("HUBUUM_DATABASE_ROLE_MODE", None);

    let default = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let split =
        AppConfig::try_parse_from(["hubuum-server", "--database-role-mode", "split"]).unwrap();
    let loaded = {
        let _split_guard = EnvVarGuard::set("HUBUUM_DATABASE_ROLE_MODE", Some("split"));
        get_config_from_env().unwrap()
    };

    assert_eq!(default.database_role_mode, DatabaseRoleMode::Single);
    assert_eq!(split.database_role_mode, DatabaseRoleMode::Split);
    assert_eq!(loaded.database_role_mode, DatabaseRoleMode::Split);
}

#[test]
fn storage_backend_is_explicit_and_strict() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _unset_guard = EnvVarGuard::set("HUBUUM_STORAGE_BACKEND", None);
    let default = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let selected =
        AppConfig::try_parse_from(["hubuum-server", "--storage-backend", "postgresql"]).unwrap();
    let empty = {
        let _empty_guard = EnvVarGuard::set("HUBUUM_STORAGE_BACKEND", Some(""));
        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();
        assert_eq!(loaded.storage_backend, StorageBackendKind::Postgres);
        parsed
    };
    let error = AppConfig::try_parse_from(["hubuum-server", "--storage-backend", "unsupported"])
        .expect_err("an unregistered storage backend must be rejected");
    let whitespace_error = {
        let _whitespace_guard = EnvVarGuard::set("HUBUUM_STORAGE_BACKEND", Some(" "));
        AppConfig::try_parse_from(["hubuum-server"])
            .expect_err("a non-empty invalid storage backend must be rejected")
    };

    assert_eq!(default.storage_backend, StorageBackendKind::Postgres);
    assert_eq!(selected.storage_backend, StorageBackendKind::Postgres);
    assert_eq!(empty.storage_backend, StorageBackendKind::Postgres);
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
    assert_eq!(
        whitespace_error.kind(),
        clap::error::ErrorKind::InvalidValue
    );
}

#[rstest]
#[case::all(RuntimeRole::All, 3)]
#[case::api(RuntimeRole::Api, 0)]
#[case::worker(RuntimeRole::Worker, 3)]
fn runtime_role_controls_effective_worker_count(
    #[case] runtime_role: RuntimeRole,
    #[case] expected: usize,
) {
    assert_eq!(runtime_role.effective_worker_count(3), expected);
}

#[test]
fn worker_runtime_role_requires_at_least_one_enabled_worker() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _role_guard = EnvVarGuard::set("HUBUUM_RUNTIME_ROLE", Some("worker"));
    let _task_guard = EnvVarGuard::set("HUBUUM_TASK_WORKERS", Some("0"));
    let _fanout_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_WORKERS", Some("0"));
    let _delivery_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_WORKERS", Some("0"));
    let _retention_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", Some("false"));
    let _token_retention_guard =
        EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_ENABLED", Some("false"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "runtime_role=worker requires at least one enabled task, event fan-out, event delivery, event retention, or token retention worker"
    );
}

#[test]
fn task_lease_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _lease_guard = EnvVarGuard::set("HUBUUM_TASK_LEASE_SECONDS", Some("90"));
    let _heartbeat_guard = EnvVarGuard::set("HUBUUM_TASK_HEARTBEAT_SECONDS", Some("15"));
    let _recovery_guard = EnvVarGuard::set("HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS", Some("45"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    for config in [&parsed, &loaded] {
        assert_eq!(config.task_lease_seconds, 90);
        assert_eq!(config.task_heartbeat_seconds, 15);
        assert_eq!(config.task_recovery_interval_seconds, 45);
    }
}

#[test]
fn task_heartbeat_must_be_shorter_than_lease() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _lease_guard = EnvVarGuard::set("HUBUUM_TASK_LEASE_SECONDS", Some("30"));
    let _heartbeat_guard = EnvVarGuard::set("HUBUUM_TASK_HEARTBEAT_SECONDS", Some("30"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "task worker heartbeat interval must be greater than zero and shorter than the lease"
    );
}

#[test]
fn task_lease_duration_must_fit_database_timestamps() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _lease_guard = EnvVarGuard::set("HUBUUM_TASK_LEASE_SECONDS", Some("18446744073709551615"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "task worker lease duration is too large for database timestamps"
    );
}

#[test]
fn event_fanout_worker_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _workers_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_WORKERS", Some("2"));
    let _batch_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_BATCH_SIZE", Some("50"));
    let _poll_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS", Some("500"));
    let _lock_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS", Some("45000"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.event_fanout_workers, 2);
    assert_eq!(parsed.event_fanout_batch_size, 50);
    assert_eq!(parsed.event_fanout_poll_interval_ms, 500);
    assert_eq!(parsed.event_fanout_lock_timeout_ms, 45000);
    assert_eq!(loaded.event_fanout_workers, 2);
    assert_eq!(loaded.event_fanout_batch_size, 50);
    assert_eq!(loaded.event_fanout_poll_interval_ms, 500);
    assert_eq!(loaded.event_fanout_lock_timeout_ms, 45000);
}

#[test]
fn event_delivery_worker_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _workers_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_WORKERS", Some("2"));
    let _batch_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_BATCH_SIZE", Some("25"));
    let _poll_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS", Some("750"));
    let _lock_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS", Some("45000"));
    let _transport_timeout_guard =
        EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS", Some("20000"));
    let _base_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS", Some("100"));
    let _max_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS", Some("5000"));
    let _attempts_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS", Some("4"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.event_delivery_workers, 2);
    assert_eq!(parsed.event_delivery_batch_size, 25);
    assert_eq!(parsed.event_delivery_poll_interval_ms, 750);
    assert_eq!(parsed.event_delivery_lock_timeout_ms, 45000);
    assert_eq!(parsed.event_delivery_transport_timeout_ms, 20000);
    assert_eq!(parsed.event_delivery_retry_backoff_base_ms, 100);
    assert_eq!(parsed.event_delivery_retry_backoff_max_ms, 5000);
    assert_eq!(parsed.event_delivery_max_attempts, 4);
    assert_eq!(loaded.event_delivery_workers, 2);
    assert_eq!(loaded.event_delivery_batch_size, 25);
    assert_eq!(loaded.event_delivery_poll_interval_ms, 750);
    assert_eq!(loaded.event_delivery_lock_timeout_ms, 45000);
    assert_eq!(loaded.event_delivery_transport_timeout_ms, 20000);
    assert_eq!(loaded.event_delivery_retry_backoff_base_ms, 100);
    assert_eq!(loaded.event_delivery_retry_backoff_max_ms, 5000);
    assert_eq!(loaded.event_delivery_max_attempts, 4);
}

#[test]
fn event_retention_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _enabled_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", Some("true"));
    let _event_days_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_DAYS", Some("90"));
    let _delivery_days_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETENTION_DAYS", Some("14"));
    let _interval_guard =
        EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS", Some("600"));
    let _batch_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE", Some("250"));
    let _file_archive_guard =
        EnvVarGuard::set("HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED", Some("true"));
    let _archive_guard = EnvVarGuard::set(
        "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
        Some("/tmp/hubuum-event-archive"),
    );

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert!(parsed.event_retention_purge_enabled);
    assert_eq!(parsed.event_retention_days, 90);
    assert_eq!(parsed.event_delivery_retention_days, 14);
    assert_eq!(parsed.event_retention_purge_interval_seconds, 600);
    assert_eq!(parsed.event_retention_purge_batch_size, 250);
    assert!(parsed.event_retention_file_archive_enabled);
    assert_eq!(
        parsed.event_retention_archive_path.as_deref(),
        Some("/tmp/hubuum-event-archive")
    );
    assert!(loaded.event_retention_purge_enabled);
    assert_eq!(loaded.event_retention_days, 90);
    assert_eq!(loaded.event_delivery_retention_days, 14);
    assert_eq!(loaded.event_retention_purge_interval_seconds, 600);
    assert_eq!(loaded.event_retention_purge_batch_size, 250);
    assert!(loaded.event_retention_file_archive_enabled);
    assert_eq!(
        loaded.event_retention_archive_path.as_deref(),
        Some("/tmp/hubuum-event-archive")
    );
}

#[test]
fn event_retention_settings_are_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "event_retention_purge_batch_size must be greater than 0"
    );
}

#[test]
fn event_retention_file_archive_requires_path() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _enabled_guard =
        EnvVarGuard::set("HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED", Some("true"));
    let _path_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_ARCHIVE_PATH", None);

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "event_retention_archive_path is required when event_retention_file_archive_enabled is true"
    );
}

#[test]
fn event_delivery_transport_timeout_must_be_shorter_than_lock_timeout() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _lock_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS", Some("1000"));
    let _transport_guard =
        EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS", Some("1000"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "event_delivery_transport_timeout_ms (1000) must be less than event_delivery_lock_timeout_ms (1000)"
    );
}

#[rstest::rstest]
#[case::fanout_timeout(
    "HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS",
    "18446744073709551615",
    "event_fanout_lock_timeout_ms is too large for timestamps"
)]
#[case::delivery_timeout(
    "HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS",
    "18446744073709551615",
    "event_delivery_lock_timeout_ms is too large for timestamps"
)]
#[case::delivery_retry(
    "HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS",
    "18446744073709551615",
    "event_delivery_retry_backoff_max_ms is too large for timestamps"
)]
#[case::retention_days(
    "HUBUUM_EVENT_RETENTION_DAYS",
    "9223372036854775807",
    "event_retention_days is too large for timestamps"
)]
fn event_worker_durations_must_be_representable(
    #[case] variable: &'static str,
    #[case] value: &str,
    #[case] expected: &str,
) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set(variable, Some(value));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(error.to_string(), expected);
}

#[test]
fn worker_defaults_scale_from_detected_cpu_count() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _actix_guard = EnvVarGuard::set("HUBUUM_ACTIX_WORKERS", None);
    let _task_guard = EnvVarGuard::set("HUBUUM_TASK_WORKERS", None);
    let _interval_guard = EnvVarGuard::set("HUBUUM_TASK_POLL_INTERVAL_MS", None);
    let _fanout_workers_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_WORKERS", None);
    let _fanout_batch_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_BATCH_SIZE", None);
    let _fanout_poll_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS", None);
    let _fanout_lock_guard = EnvVarGuard::set("HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS", None);
    let _delivery_workers_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_WORKERS", None);
    let _delivery_batch_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_BATCH_SIZE", None);
    let _delivery_poll_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS", None);
    let _delivery_lock_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS", None);
    let _delivery_transport_timeout_guard =
        EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS", None);
    let _delivery_base_guard =
        EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS", None);
    let _delivery_max_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS", None);
    let _delivery_attempts_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS", None);
    let _retention_enabled_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", None);
    let _retention_days_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_DAYS", None);
    let _retention_delivery_days_guard =
        EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETENTION_DAYS", None);
    let _retention_interval_guard =
        EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS", None);
    let _retention_batch_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE", None);
    let _retention_file_archive_guard =
        EnvVarGuard::set("HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED", None);
    let _retention_archive_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_ARCHIVE_PATH", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.actix_workers, default_actix_workers());
    assert_eq!(parsed.task_workers, default_task_workers());
    assert_eq!(parsed.task_poll_interval_ms, DEFAULT_TASK_POLL_INTERVAL_MS);
    assert_eq!(parsed.event_fanout_workers, DEFAULT_EVENT_FANOUT_WORKERS);
    assert_eq!(
        parsed.event_fanout_batch_size,
        DEFAULT_EVENT_FANOUT_BATCH_SIZE
    );
    assert_eq!(
        parsed.event_fanout_poll_interval_ms,
        DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS
    );
    assert_eq!(
        parsed.event_fanout_lock_timeout_ms,
        DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS
    );
    assert_eq!(
        parsed.event_delivery_workers,
        DEFAULT_EVENT_DELIVERY_WORKERS
    );
    assert_eq!(
        parsed.event_delivery_batch_size,
        DEFAULT_EVENT_DELIVERY_BATCH_SIZE
    );
    assert_eq!(
        parsed.event_delivery_poll_interval_ms,
        DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS
    );
    assert_eq!(
        parsed.event_delivery_lock_timeout_ms,
        DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS
    );
    assert_eq!(
        parsed.event_delivery_transport_timeout_ms,
        DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS
    );
    assert_eq!(
        parsed.event_delivery_retry_backoff_base_ms,
        DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS
    );
    assert_eq!(
        parsed.event_delivery_retry_backoff_max_ms,
        DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS
    );
    assert_eq!(
        parsed.event_delivery_max_attempts,
        DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS
    );
    assert_eq!(
        parsed.event_retention_purge_enabled,
        DEFAULT_EVENT_RETENTION_PURGE_ENABLED
    );
    assert_eq!(parsed.event_retention_days, DEFAULT_EVENT_RETENTION_DAYS);
    assert_eq!(
        parsed.event_delivery_retention_days,
        DEFAULT_EVENT_DELIVERY_RETENTION_DAYS
    );
    assert_eq!(
        parsed.event_retention_purge_interval_seconds,
        DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS
    );
    assert_eq!(
        parsed.event_retention_purge_batch_size,
        DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE
    );
    assert_eq!(
        parsed.event_retention_file_archive_enabled,
        DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED
    );
    assert_eq!(parsed.event_retention_archive_path, None);
    assert_eq!(loaded.actix_workers, default_actix_workers());
    assert_eq!(loaded.task_workers, default_task_workers());
    assert_eq!(loaded.task_poll_interval_ms, DEFAULT_TASK_POLL_INTERVAL_MS);
    assert_eq!(loaded.event_fanout_workers, DEFAULT_EVENT_FANOUT_WORKERS);
    assert_eq!(
        loaded.event_fanout_batch_size,
        DEFAULT_EVENT_FANOUT_BATCH_SIZE
    );
    assert_eq!(
        loaded.event_fanout_poll_interval_ms,
        DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS
    );
    assert_eq!(
        loaded.event_fanout_lock_timeout_ms,
        DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS
    );
    assert_eq!(
        loaded.event_delivery_workers,
        DEFAULT_EVENT_DELIVERY_WORKERS
    );
    assert_eq!(
        loaded.event_delivery_batch_size,
        DEFAULT_EVENT_DELIVERY_BATCH_SIZE
    );
    assert_eq!(
        loaded.event_delivery_poll_interval_ms,
        DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS
    );
    assert_eq!(
        loaded.event_delivery_lock_timeout_ms,
        DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS
    );
    assert_eq!(
        loaded.event_delivery_transport_timeout_ms,
        DEFAULT_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS
    );
    assert_eq!(
        loaded.event_delivery_retry_backoff_base_ms,
        DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS
    );
    assert_eq!(
        loaded.event_delivery_retry_backoff_max_ms,
        DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS
    );
    assert_eq!(
        loaded.event_delivery_max_attempts,
        DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS
    );
    assert_eq!(
        loaded.event_retention_purge_enabled,
        DEFAULT_EVENT_RETENTION_PURGE_ENABLED
    );
    assert_eq!(loaded.event_retention_days, DEFAULT_EVENT_RETENTION_DAYS);
    assert_eq!(
        loaded.event_delivery_retention_days,
        DEFAULT_EVENT_DELIVERY_RETENTION_DAYS
    );
    assert_eq!(
        loaded.event_retention_purge_interval_seconds,
        DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS
    );
    assert_eq!(
        loaded.event_retention_purge_batch_size,
        DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE
    );
    assert_eq!(
        loaded.event_retention_file_archive_enabled,
        DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED
    );
    assert_eq!(loaded.event_retention_archive_path, None);
}

#[test]
fn export_max_output_bytes_is_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EXPORT_MAX_OUTPUT_BYTES", Some("4096"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.export_max_output_bytes, 4096);
    assert_eq!(loaded.export_max_output_bytes, 4096);
}

#[test]
fn export_max_output_bytes_defaults_when_env_var_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EXPORT_MAX_OUTPUT_BYTES", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(
        parsed.export_max_output_bytes,
        DEFAULT_EXPORT_MAX_OUTPUT_BYTES
    );
    assert_eq!(
        loaded.export_max_output_bytes,
        DEFAULT_EXPORT_MAX_OUTPUT_BYTES
    );
}

#[test]
fn export_max_output_bytes_is_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EXPORT_MAX_OUTPUT_BYTES", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "export_max_output_bytes must be greater than 0"
    );
}

#[rstest]
#[case(
    "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS",
    "export_output_retention_hours"
)]
#[case(
    "HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS",
    "backup_output_retention_hours"
)]
#[case(
    "HUBUUM_RESTORE_STAGE_RETENTION_MINUTES",
    "restore_stage_retention_minutes"
)]
fn artifact_retention_horizons_must_be_representable(
    #[case] key: &'static str,
    #[case] field: &str,
) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set(key, Some("9223372036854775807"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("{field} is outside the supported duration range")
    );
}

#[test]
fn export_max_active_tasks_per_user_is_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER", Some("7"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.export_max_active_tasks_per_user, 7);
    assert_eq!(loaded.export_max_active_tasks_per_user, 7);
}

#[test]
fn import_max_active_tasks_per_user_is_loaded_and_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER", Some("7"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();
    assert_eq!(parsed.import_max_active_tasks_per_user, 7);
    assert_eq!(loaded.import_max_active_tasks_per_user, 7);

    drop(_guard);
    let _guard = EnvVarGuard::set("HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER", None);
    assert_eq!(
        get_config_from_env()
            .unwrap()
            .import_max_active_tasks_per_user,
        DEFAULT_IMPORT_MAX_ACTIVE_TASKS_PER_USER
    );

    drop(_guard);
    let _guard = EnvVarGuard::set("HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER", Some("0"));
    assert_eq!(
        get_config_from_env().unwrap_err().to_string(),
        "import_max_active_tasks_per_user must be greater than 0"
    );
}

#[test]
fn db_pool_acquire_timeout_is_loaded_and_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS", Some("750"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();
    assert_eq!(parsed.db_pool_acquire_timeout_ms, 750);
    assert_eq!(loaded.db_pool_acquire_timeout_ms, 750);

    drop(_guard);
    let _guard = EnvVarGuard::set("HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS", None);
    assert_eq!(
        get_config_from_env().unwrap().db_pool_acquire_timeout_ms,
        DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS
    );

    drop(_guard);
    let _guard = EnvVarGuard::set("HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS", Some("0"));
    assert_eq!(
        get_config_from_env().unwrap_err().to_string(),
        "db_pool_acquire_timeout_ms must be greater than 0"
    );
}

#[test]
fn export_max_active_tasks_per_user_defaults_when_env_var_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(
        parsed.export_max_active_tasks_per_user,
        DEFAULT_EXPORT_MAX_ACTIVE_TASKS_PER_USER
    );
    assert_eq!(
        loaded.export_max_active_tasks_per_user,
        DEFAULT_EXPORT_MAX_ACTIVE_TASKS_PER_USER
    );
}

#[test]
fn export_max_active_tasks_per_user_is_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "export_max_active_tasks_per_user must be greater than 0"
    );
}

#[test]
fn remote_call_max_active_tasks_per_user_is_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER", Some("9"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.remote_call_max_active_tasks_per_user, 9);
    assert_eq!(loaded.remote_call_max_active_tasks_per_user, 9);
}

#[test]
fn remote_call_max_active_tasks_per_user_defaults_when_env_var_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(
        parsed.remote_call_max_active_tasks_per_user,
        DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER
    );
    assert_eq!(
        loaded.remote_call_max_active_tasks_per_user,
        DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER
    );
}

#[test]
fn remote_call_max_active_tasks_per_user_is_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "remote_call_max_active_tasks_per_user must be greater than 0"
    );
}

#[test]
fn token_lifetime_hours_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TOKEN_LIFETIME_HOURS", Some("48"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.token_lifetime_hours, 48);
    assert_eq!(loaded.token_lifetime_hours, 48);
}

#[test]
fn token_lifetime_hours_defaults_when_env_var_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TOKEN_LIFETIME_HOURS", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.token_lifetime_hours, DEFAULT_TOKEN_LIFETIME_HOURS);
    assert_eq!(loaded.token_lifetime_hours, DEFAULT_TOKEN_LIFETIME_HOURS);
}

#[test]
fn token_lifetime_hours_are_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TOKEN_LIFETIME_HOURS", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "token_lifetime_hours must be greater than 0"
    );
}

#[test]
fn negative_token_lifetime_hours_are_rejected() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TOKEN_LIFETIME_HOURS", Some("-1"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "token_lifetime_hours must be greater than 0"
    );
}

#[test]
fn token_lifetime_hours_above_the_supported_range_are_rejected() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_TOKEN_LIFETIME_HOURS", Some("2147483648"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "token_lifetime_hours must not exceed 2147483647"
    );
}

#[test]
fn max_token_lifetime_hours_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_MAX_TOKEN_LIFETIME_HOURS", Some("720"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.max_token_lifetime_hours, 720);
    assert_eq!(loaded.max_token_lifetime_hours, 720);
}

#[test]
fn max_token_lifetime_hours_default_when_env_var_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_MAX_TOKEN_LIFETIME_HOURS", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(
        parsed.max_token_lifetime_hours,
        DEFAULT_MAX_TOKEN_LIFETIME_HOURS
    );
    assert_eq!(
        loaded.max_token_lifetime_hours,
        DEFAULT_MAX_TOKEN_LIFETIME_HOURS
    );
}

#[test]
fn max_token_lifetime_hours_must_cover_the_default_lifetime() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _default_guard = EnvVarGuard::set("HUBUUM_TOKEN_LIFETIME_HOURS", Some("48"));
    let _maximum_guard = EnvVarGuard::set("HUBUUM_MAX_TOKEN_LIFETIME_HOURS", Some("24"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "token_lifetime_hours must not exceed max_token_lifetime_hours"
    );
}

#[test]
fn max_token_lifetime_hours_are_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set("HUBUUM_MAX_TOKEN_LIFETIME_HOURS", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "max_token_lifetime_hours must be greater than 0"
    );
}

#[test]
fn token_retention_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _enabled_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_ENABLED", Some("false"));
    let _days_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_DAYS", Some("45"));
    let _interval_guard =
        EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS", Some("600"));
    let _batch_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE", Some("250"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    for config in [&parsed, &loaded] {
        assert!(!config.token_retention_purge_enabled);
        assert_eq!(config.token_retention_days, 45);
        assert_eq!(config.token_retention_purge_interval_seconds, 600);
        assert_eq!(config.token_retention_purge_batch_size, 250);
    }
}

#[test]
fn token_retention_settings_default_when_env_is_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _enabled_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_ENABLED", None);
    let _days_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_DAYS", None);
    let _interval_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS", None);
    let _batch_guard = EnvVarGuard::set("HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    for config in [&parsed, &loaded] {
        assert_eq!(
            config.token_retention_purge_enabled,
            DEFAULT_TOKEN_RETENTION_PURGE_ENABLED
        );
        assert_eq!(config.token_retention_days, DEFAULT_TOKEN_RETENTION_DAYS);
        assert_eq!(
            config.token_retention_purge_interval_seconds,
            DEFAULT_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS
        );
        assert_eq!(
            config.token_retention_purge_batch_size,
            DEFAULT_TOKEN_RETENTION_PURGE_BATCH_SIZE
        );
    }
}

#[rstest]
#[case(
    "HUBUUM_TOKEN_RETENTION_DAYS",
    "0",
    "token_retention_days must be greater than 0"
)]
#[case(
    "HUBUUM_TOKEN_RETENTION_PURGE_INTERVAL_SECONDS",
    "0",
    "token_retention_purge_interval_seconds must be greater than 0"
)]
#[case(
    "HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE",
    "1",
    "token_retention_purge_batch_size must be at least 10"
)]
fn token_retention_settings_are_validated(
    #[case] variable: &'static str,
    #[case] value: &'static str,
    #[case] expected: &'static str,
) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _guard = EnvVarGuard::set(variable, Some(value));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(error.to_string(), expected);
}

#[test]
fn token_hash_key_is_initialized_once() {
    let key = token_hash_key_bytes();
    assert!(!key.is_empty());
    let _ = token_hash_key_is_ephemeral();
}

#[test]
fn login_rate_limit_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _attempts_guard = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", Some("9"));
    let _window_guard = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS", Some("120"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(parsed.login_rate_limit_max_attempts, 9);
    assert_eq!(parsed.login_rate_limit_window_seconds, 120);
    assert_eq!(loaded.login_rate_limit_max_attempts, 9);
    assert_eq!(loaded.login_rate_limit_window_seconds, 120);
}

#[cfg(feature = "login-rate-limit-valkey")]
#[test]
fn shared_login_rate_limit_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _backend = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_BACKEND", Some("valkey"));
    let _url = EnvVarGuard::set(
        "HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL",
        Some("redis://valkey.internal:6379/"),
    );
    let _prefix = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_VALKEY_PREFIX", Some("hubuum:test"));
    let _timeout = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS", Some("250"));

    let loaded = get_config_from_env().unwrap();

    assert_eq!(
        loaded.login_rate_limit_backend,
        LoginRateLimitBackendKind::Valkey
    );
    assert_eq!(
        loaded.login_rate_limit_valkey_url.as_deref(),
        Some("redis://valkey.internal:6379/")
    );
    assert_eq!(loaded.login_rate_limit_valkey_prefix, "hubuum:test");
    assert_eq!(loaded.login_rate_limit_valkey_io_timeout_ms, 250);
}

#[cfg(feature = "login-rate-limit-valkey")]
#[test]
fn shared_login_rate_limit_requires_a_url() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _backend = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_BACKEND", Some("valkey"));
    let _url = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL", None);

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "login_rate_limit_valkey_url is required when login_rate_limit_backend=valkey"
    );
}

#[test]
fn login_rate_limit_settings_default_when_env_vars_are_unset() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _attempts_guard = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", None);
    let _window_guard = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert_eq!(
        parsed.login_rate_limit_max_attempts,
        DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS
    );
    assert_eq!(
        parsed.login_rate_limit_window_seconds,
        DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS
    );
    assert_eq!(
        loaded.login_rate_limit_max_attempts,
        DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS
    );
    assert_eq!(
        loaded.login_rate_limit_window_seconds,
        DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS
    );
}

#[test]
fn login_rate_limit_settings_are_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _attempts_guard = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", Some("0"));
    let _window_guard = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS", Some("0"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "login_rate_limit_max_attempts must be greater than 0"
    );
}

#[test]
fn extended_login_rate_limit_settings_are_parsed_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _enabled = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_ENABLED", Some("false"));
    let _per_ip = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP", Some("33"));
    let _per_subnet = EnvVarGuard::set(
        "HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET",
        Some("77"),
    );
    let _base = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", Some("60"));
    let _max = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", Some("3600"));
    let _v4 = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4", Some("16"));
    let _v6 = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6", Some("48"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    for config in [&parsed, &loaded] {
        assert!(!config.login_rate_limit_enabled);
        assert_eq!(config.login_rate_limit_max_attempts_per_ip, 33);
        assert_eq!(config.login_rate_limit_max_attempts_per_subnet, 77);
        assert_eq!(config.login_rate_limit_backoff_base_seconds, 60);
        assert_eq!(config.login_rate_limit_backoff_max_seconds, 3600);
        assert_eq!(config.login_rate_limit_subnet_prefix_v4, 16);
        assert_eq!(config.login_rate_limit_subnet_prefix_v6, 48);
    }
}

#[test]
fn per_scope_thresholds_may_be_zero_to_disable() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _per_ip = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP", Some("0"));
    let _per_subnet =
        EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET", Some("0"));

    let loaded = get_config_from_env().unwrap();

    assert_eq!(loaded.login_rate_limit_max_attempts_per_ip, 0);
    assert_eq!(loaded.login_rate_limit_max_attempts_per_subnet, 0);
}

#[test]
fn backoff_max_must_not_be_below_base() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _base = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", Some("600"));
    let _max = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", Some("300"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "login_rate_limit_backoff_max_seconds (300) must be greater than or equal to login_rate_limit_backoff_base_seconds (600)"
    );
}

#[test]
fn subnet_prefixes_are_validated() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _v4 = EnvVarGuard::set("HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4", Some("33"));

    let error = get_config_from_env().unwrap_err();

    assert_eq!(
        error.to_string(),
        "login_rate_limit_subnet_prefix_v4 must be between 1 and 32"
    );
}

#[test]
fn trusted_proxies_parse_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _proxies = EnvVarGuard::set("HUBUUM_TRUSTED_PROXIES", Some("10.0.0.0/8, 192.168.1.1"));
    let _hops = EnvVarGuard::set("HUBUUM_TRUSTED_PROXY_HOPS", Some("2"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    for config in [&parsed, &loaded] {
        assert_eq!(config.trusted_proxies.nets().len(), 2);
        assert_eq!(config.trusted_proxy_hops, 2);
    }
}

#[test]
fn trusted_proxies_default_to_empty() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _proxies = EnvVarGuard::set("HUBUUM_TRUSTED_PROXIES", None);

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    assert!(parsed.trusted_proxies.nets().is_empty());
    assert!(loaded.trusted_proxies.nets().is_empty());
}

#[test]
fn metrics_config_parses_from_env() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _enabled = EnvVarGuard::set("HUBUUM_METRICS_ENABLED", Some("false"));
    let _path = EnvVarGuard::set("HUBUUM_METRICS_PATH", Some("/internal/metrics"));

    let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    let loaded = get_config_from_env().unwrap();

    for config in [&parsed, &loaded] {
        assert!(!config.metrics_enabled);
        assert_eq!(config.metrics_path.as_str(), "/internal/metrics");
    }
}

#[test]
fn metrics_path_must_be_absolute_non_root_path() {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _path = EnvVarGuard::set("HUBUUM_METRICS_PATH", Some("metrics"));

    let error = get_config_from_env().unwrap_err();
    assert_eq!(
        error.to_string(),
        "metrics_path must be an absolute non-root path"
    );
}

#[rstest]
#[case("/api")]
#[case("/api/v1/metrics")]
#[case("/healthz")]
#[case("/readyz")]
#[case("/api-doc/openapi.json")]
#[case("/swagger-ui")]
#[case("/swagger-ui/index.html")]
fn metrics_path_must_not_collide_with_existing_routes(#[case] path: &str) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _path = EnvVarGuard::set("HUBUUM_METRICS_PATH", Some(path));
    let error = get_config_from_env().unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("metrics_path '{path}' collides with an existing application route")
    );
}

#[rstest]
#[case("/{tail}*")]
#[case("/metrics/{id}")]
#[case("/metrics?format=text")]
#[case("/metrics#fragment")]
#[case("/metrics path")]
fn metrics_path_must_be_literal(#[case] path: &str) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _path = EnvVarGuard::set("HUBUUM_METRICS_PATH", Some(path));
    assert_eq!(
        get_config_from_env().unwrap_err().to_string(),
        "metrics_path must be a literal HTTP path"
    );
}

#[rstest]
#[case("/internal//metrics")]
#[case("/metrics/")]
fn metrics_path_must_not_have_empty_segments(#[case] path: &str) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let _path = EnvVarGuard::set("HUBUUM_METRICS_PATH", Some(path));
    assert_eq!(
        get_config_from_env().unwrap_err().to_string(),
        "metrics_path must not contain empty path segments"
    );
}

#[rstest]
#[case(None, None, Ok(None))]
#[case(Some("server.crt"), Some("server.key"), Ok(Some(("server.crt", "server.key"))))]
#[case(
    Some("server.crt"),
    None,
    Err("TLS certificate specified but key is missing")
)]
#[case(
    None,
    Some("server.key"),
    Err("TLS key specified but certificate is missing")
)]
fn both_listeners_resolve_the_same_tls_pair(
    #[case] certificate: Option<&str>,
    #[case] private_key: Option<&str>,
    #[case] expected: Result<Option<(&str, &str)>, &str>,
) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let mut config = AppConfig::parse_from(["hubuum"]);
    config.tls_cert_path = certificate.map(str::to_owned);
    config.tls_key_path = private_key.map(str::to_owned);
    assert_eq!(config.tls_key_pair(), expected);
}

#[rstest::rstest]
#[case::export(300, 1)]
#[case::backup(1, 300)]
fn runtime_output_limits_must_fit_the_worker_reservation(
    #[case] export_mib: usize,
    #[case] backup_mib: usize,
) {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    let mut candidate = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
    candidate.artifacts.worker_resident_bytes = 512 << 20;
    candidate.export_max_output_bytes = export_mib << 20;
    candidate.backup_max_output_bytes = backup_mib << 20;
    let error = candidate.validate_runtime_policy().unwrap_err();
    assert!(
        error.to_string().contains("worker resident bytes"),
        "{error}"
    );
}
