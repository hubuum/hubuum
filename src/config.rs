#[cfg(test)]
use std::sync::{LazyLock, Mutex};
#[cfg(not(test))]
use std::sync::{RwLock, RwLockReadGuard};

use clap::{Parser, ValueEnum};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
#[cfg(not(test))]
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;

use crate::errors::ApiError;

pub const DEFAULT_PAGE_LIMIT: usize = 100;
pub const MAX_PAGE_LIMIT: usize = 250;
pub const DEFAULT_TASK_POLL_INTERVAL_MS: u64 = 200;
pub const DEFAULT_TOKEN_LIFETIME_HOURS: i64 = 24;

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

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TlsBackend {
    Rustls,
    Openssl,
}

impl TlsBackend {
    #[cfg(any(
        not(any(feature = "tls-rustls", feature = "tls-openssl")),
        all(feature = "tls-rustls", not(feature = "tls-openssl")),
        all(feature = "tls-openssl", not(feature = "tls-rustls"))
    ))]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rustls => "rustls",
            Self::Openssl => "openssl",
        }
    }
}

#[derive(Parser, Debug, Deserialize, Serialize, Clone)]
#[command(version = env!("CARGO_PKG_VERSION"), about = "Hubuum server", long_about = None)]
pub struct AppConfig {
    /// IP address to bind to
    #[clap(long, env = "HUBUUM_BIND_IP", default_value = "127.0.0.1")]
    pub bind_ip: String,

    /// Port to bind to
    #[clap(long, env = "HUBUUM_BIND_PORT", default_value = "8080")]
    pub port: u16,

    /// Logging level
    #[clap(long, env = "HUBUUM_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Database URL
    #[clap(
        long,
        env = "HUBUUM_DATABASE_URL",
        default_value = "postgres://localhost"
    )]
    pub database_url: String,

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

    /// Number of DB connections in the pool
    #[clap(long, env = "HUBUUM_DB_POOL_SIZE", default_value_t = 10)]
    pub db_pool_size: u32,

    /// Token lifetime in hours
    #[clap(
        long,
        env = "HUBUUM_TOKEN_LIFETIME_HOURS",
        default_value_t = DEFAULT_TOKEN_LIFETIME_HOURS
    )]
    pub token_lifetime_hours: i64,

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

    /// The name of the admin group
    #[clap(long, env = "HUBUUM_ADMIN_GROUPNAME", default_value = "admin")]
    pub admin_groupname: String,

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

    /// Trust proxy IP headers (X-Forwarded-For/Forwarded). If false, use peer address only.
    #[clap(long, default_value = "false", env = "HUBUUM_TRUST_IP_HEADERS")]
    pub trust_ip_headers: bool,

    /// Whitelist of client IPs or CIDRs ("*" allows all)
    #[clap(long, default_value = "127.0.0.1,::1", env = "HUBUUM_CLIENT_ALLOWLIST", value_parser = parse_client_allowlist)]
    pub client_allowlist: ClientAllowlist,
}

fn parse_client_allowlist(s: &str) -> Result<ClientAllowlist, String> {
    ClientAllowlist::from_str(s).map_err(|e| e.to_string())
}

impl AppConfig {
    fn validate(self) -> Result<Self, ApiError> {
        if self.actix_workers == 0 {
            return Err(ApiError::BadRequest(
                "actix_workers must be greater than 0".to_string(),
            ));
        }

        if self.task_workers == 0 {
            return Err(ApiError::BadRequest(
                "task_workers must be greater than 0".to_string(),
            ));
        }

        if self.task_poll_interval_ms == 0 {
            return Err(ApiError::BadRequest(
                "task_poll_interval_ms must be greater than 0".to_string(),
            ));
        }

        if self.db_pool_size == 0 {
            return Err(ApiError::BadRequest(
                "db_pool_size must be greater than 0".to_string(),
            ));
        }

        if self.token_lifetime_hours == 0 {
            return Err(ApiError::BadRequest(
                "token_lifetime_hours must be greater than 0".to_string(),
            ));
        }

        if self.default_page_limit == 0 {
            return Err(ApiError::BadRequest(
                "default_page_limit must be greater than 0".to_string(),
            ));
        }

        if self.max_page_limit == 0 {
            return Err(ApiError::BadRequest(
                "max_page_limit must be greater than 0".to_string(),
            ));
        }

        if self.default_page_limit > self.max_page_limit {
            return Err(ApiError::BadRequest(format!(
                "default_page_limit ({}) must be less than or equal to max_page_limit ({})",
                self.default_page_limit, self.max_page_limit
            )));
        }

        Ok(self)
    }
}

#[cfg(not(test))]
fn load_config() -> Result<AppConfig, ApiError> {
    match AppConfig::try_parse() {
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
pub static CONFIG: Lazy<RwLock<AppConfig>> = Lazy::new(|| {
    let config = load_config().unwrap_or_else(|e| panic!("Invalid application configuration: {e}"));
    RwLock::new(config)
});

#[cfg(not(test))]
pub fn get_config() -> Result<RwLockReadGuard<'static, AppConfig>, ApiError> {
    CONFIG
        .read()
        .map_err(|e| ApiError::InternalServerError(format!("Failed to read config: {e}")))
}

#[cfg(test)]
static TEST_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[cfg(test)]
fn get_config_from_env() -> Result<AppConfig, ApiError> {
    use std::env;

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

    fn env_or_default_client_allowlist(key: &str, default: &str) -> ClientAllowlist {
        env::var(key)
            .unwrap_or_else(|_| default.to_string())
            .parse()
            .unwrap_or_else(|e: ApiError| panic!("Invalid client allowlist in {key}: {e}"))
    }

    let config = AppConfig {
        bind_ip: env_or_default("HUBUUM_BIND_IP", "127.0.0.1"),
        port: env_or_default("HUBUUM_BIND_PORT", "8080")
            .parse()
            .unwrap_or(8080),
        log_level: env_or_default("HUBUUM_LOG_LEVEL", "debug"),
        database_url: env_or_default("HUBUUM_DATABASE_URL", "postgres://test"),
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
        db_pool_size: env_or_default("HUBUUM_DB_POOL_SIZE", "2")
            .parse()
            .unwrap_or(5),
        token_lifetime_hours: env_or_default("HUBUUM_TOKEN_LIFETIME_HOURS", "24")
            .parse()
            .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS),
        default_page_limit: env_or_default("HUBUUM_DEFAULT_PAGE_LIMIT", "100")
            .parse()
            .unwrap_or(DEFAULT_PAGE_LIMIT),
        max_page_limit: env_or_default("HUBUUM_MAX_PAGE_LIMIT", "250")
            .parse()
            .unwrap_or(MAX_PAGE_LIMIT),
        admin_groupname: env_or_default("HUBUUM_ADMIN_GROUPNAME", "admin"),
        tls_cert_path: env_or_default_opt("HUBUUM_TLS_CERT_PATH", None),
        tls_key_path: env_or_default_opt("HUBUUM_TLS_KEY_PATH", None),
        tls_key_passphrase: env_or_default_opt("HUBUUM_TLS_KEY_PASSPHRASE", None),
        tls_backend: env_or_default_tls_backend("HUBUUM_TLS_BACKEND"),
        trust_ip_headers: env_or_default("HUBUUM_TRUST_IP_HEADERS", "false")
            .parse()
            .unwrap_or(false),
        client_allowlist: env_or_default_client_allowlist(
            "HUBUUM_CLIENT_ALLOWLIST",
            "127.0.0.1,::1",
        ),
    };

    config.validate()
}

/// Client IP allowlist - either allow all (`*`) or specific IPs/CIDRs
#[derive(Debug, Clone)]
pub enum ClientAllowlist {
    Any,
    Nets(Vec<IpNet>),
}

impl Serialize for ClientAllowlist {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClientAllowlist::Any => serializer.serialize_str("*"),
            ClientAllowlist::Nets(nets) => {
                let s = nets
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                serializer.serialize_str(&s)
            }
        }
    }
}

impl<'de> Deserialize<'de> for ClientAllowlist {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ClientAllowlist::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl ClientAllowlist {
    /// Parse a CLI/env string into a ClientAllowlist
    pub fn parse_cli(input: &str) -> Result<Self, ApiError> {
        let trimmed = input.trim();

        if trimmed == "*" {
            return Ok(Self::Any);
        }

        let nets: Vec<IpNet> = trimmed
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(Self::parse_net)
            .collect::<Result<_, _>>()?;

        if nets.is_empty() {
            return Err(ApiError::BadRequest(
                "client allowlist cannot be empty".into(),
            ));
        }

        Ok(Self::Nets(nets))
    }

    /// Check if an IP address is allowed
    pub fn allows(&self, ip: IpAddr) -> bool {
        match self {
            ClientAllowlist::Any => true,
            ClientAllowlist::Nets(nets) => nets.iter().any(|net| match (net, ip) {
                (IpNet::V4(net), IpAddr::V4(addr)) => net.contains(&addr),
                (IpNet::V6(net), IpAddr::V6(addr)) => net.contains(&addr),
                _ => false,
            }),
        }
    }

    /// Parse a network CIDR or single IP
    fn parse_net(raw: &str) -> Result<IpNet, ApiError> {
        IpNet::from_str(raw)
            .or_else(|_| Self::ip_to_host_net(raw))
            .map_err(|_| ApiError::BadRequest(format!("Invalid IP/CIDR: {}", raw)))
    }

    /// Convert a single IP address to a /32 or /128 network
    fn ip_to_host_net(raw: &str) -> Result<IpNet, ()> {
        let ip: IpAddr = raw.parse().map_err(|_| ())?;
        match ip {
            IpAddr::V4(addr) => Ipv4Net::new(addr, 32).map(IpNet::from).map_err(|_| ()),
            IpAddr::V6(addr) => Ipv6Net::new(addr, 128).map(IpNet::from).map_err(|_| ()),
        }
    }
}

impl FromStr for ClientAllowlist {
    type Err = ApiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_cli(s)
    }
}

#[cfg(test)]
pub fn get_config() -> Result<AppConfig, ApiError> {
    let _lock = TEST_ENV_LOCK.lock().unwrap();
    get_config_from_env()
}

#[cfg(test)]
mod tests {
    use std::{env, ffi::OsString};

    use clap::Parser;

    use super::{
        AppConfig, DEFAULT_PAGE_LIMIT, DEFAULT_TASK_POLL_INTERVAL_MS, DEFAULT_TOKEN_LIFETIME_HOURS,
        MAX_PAGE_LIMIT, TEST_ENV_LOCK, TlsBackend, default_actix_workers, default_task_workers,
        get_config_from_env,
    };

    struct EnvVarGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: Option<&str>) -> Self {
            let original = env::var_os(key);

            match value {
                Some(value) => unsafe { env::set_var(key, value) },
                None => unsafe { env::remove_var(key) },
            }

            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe { env::set_var(self.key, value) },
                None => unsafe { env::remove_var(self.key) },
            }
        }
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
        let _default_guard = EnvVarGuard::set("HUBUUM_DEFAULT_PAGE_LIMIT", Some("25"));
        let _max_guard = EnvVarGuard::set("HUBUUM_MAX_PAGE_LIMIT", Some("75"));

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(parsed.default_page_limit, 25);
        assert_eq!(parsed.max_page_limit, 75);
        assert_eq!(loaded.default_page_limit, 25);
        assert_eq!(loaded.max_page_limit, 75);
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
    fn worker_defaults_scale_from_detected_cpu_count() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _actix_guard = EnvVarGuard::set("HUBUUM_ACTIX_WORKERS", None);
        let _task_guard = EnvVarGuard::set("HUBUUM_TASK_WORKERS", None);
        let _interval_guard = EnvVarGuard::set("HUBUUM_TASK_POLL_INTERVAL_MS", None);

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(parsed.actix_workers, default_actix_workers());
        assert_eq!(parsed.task_workers, default_task_workers());
        assert_eq!(parsed.task_poll_interval_ms, DEFAULT_TASK_POLL_INTERVAL_MS);
        assert_eq!(loaded.actix_workers, default_actix_workers());
        assert_eq!(loaded.task_workers, default_task_workers());
        assert_eq!(loaded.task_poll_interval_ms, DEFAULT_TASK_POLL_INTERVAL_MS);
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
}
