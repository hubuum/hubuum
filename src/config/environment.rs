//! Registry for environment variables owned by Hubuum.
//!
//! Configuration consumers receive typed values. Only startup/configuration
//! adapters should map these names to those values. Adding a variable requires
//! declaring its owner and sensitivity here, which prevents a consumer from
//! quietly introducing an ambiguous name such as `HUBUUM_TIMEOUT`.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvironmentOwner {
    Server,
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
    Sensitive,
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
            exposure: Exposure::Sensitive,
        }
    };
}

/// Variables consumed by `AppConfig` through clap.
pub const APP_CONFIG_ENVIRONMENT: &[EnvironmentVariable] = &[
    option!("HUBUUM_BIND_IP", Server),
    option!("HUBUUM_BIND_PORT", Server),
    option!("HUBUUM_LOG_LEVEL", Server),
    option!("HUBUUM_ACTIX_WORKERS", Server),
    option!("HUBUUM_RUNTIME_ROLE", Server),
    option!("HUBUUM_DATABASE_URL", Database, sensitive),
    option!("HUBUUM_DB_POOL_SIZE", Database),
    option!("HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS", Database),
    option!("HUBUUM_DB_STATEMENT_TIMEOUT_MS", Database),
    option!("HUBUUM_TASK_WORKERS", Tasks),
    option!("HUBUUM_TASK_POLL_INTERVAL_MS", Tasks),
    option!("HUBUUM_TASK_LEASE_SECONDS", Tasks),
    option!("HUBUUM_TASK_HEARTBEAT_SECONDS", Tasks),
    option!("HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS", Tasks),
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
    option!("HUBUUM_AUTH_CONFIG_PATH", Authentication, sensitive),
    option!("HUBUUM_DEFAULT_PAGE_LIMIT", Pagination),
    option!("HUBUUM_MAX_PAGE_LIMIT", Pagination),
    option!("HUBUUM_MAX_TRANSITIVE_DEPTH", Relations),
    option!("HUBUUM_TLS_CERT_PATH", Server),
    option!("HUBUUM_TLS_KEY_PATH", Server, sensitive),
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
    option!("HUBUUM_TOKEN_HASH_KEY", Authentication, sensitive),
    option!("HUBUUM_BUILD_GIT_SHA", Operations),
    option!("HUBUUM_SKIP_MIGRATIONS", Operations),
    option!("HUBUUM_AUTH_CONFIG_HOST_PATH", Operations, sensitive),
    option!("HUBUUM_TREETOP_TEST_URL", Permissions, sensitive),
];

/// Registered dynamic secret namespaces. The suffix is a consumer-supplied
/// reference validated by the corresponding secret resolver.
pub const DYNAMIC_SECRET_PREFIXES: &[(&str, EnvironmentOwner)] = &[
    ("HUBUUM_REMOTE_SECRET_", EnvironmentOwner::RemoteCalls),
    ("HUBUUM_EVENT_SINK_SECRET_", EnvironmentOwner::Events),
];

/// Files allowed to translate Hubuum-owned environment values. This list is
/// intentionally small and reviewable. Dynamic secret resolvers remain here
/// until they can receive an injected secret-provider trait.
pub const ENVIRONMENT_ADAPTER_PATHS: &[&str] = &[
    "src/config.rs",
    "src/config/token_hash.rs",
    "src/bin/admin.rs",
    "src/logger.rs",
    "src/tasks/remote_call.rs",
    "src/tests/permissions/live_treetop_parity.rs",
    "crates/hubuum-events-core/src/lib.rs",
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
}
