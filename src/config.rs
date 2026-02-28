#[cfg(not(test))]
use std::sync::{RwLock, RwLockReadGuard};

use clap::{Parser, ValueEnum};
#[cfg(not(test))]
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::errors::ApiError;

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
    #[clap(long, env = "HUBUUM_ACTIX_WORKERS", default_value_t = 4)]
    pub actix_workers: usize,

    /// Number of DB connections in the pool
    #[clap(long, env = "HUBUUM_DB_POOL_SIZE", default_value_t = 10)]
    pub db_pool_size: u32,

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
}

#[cfg(not(test))]
pub static CONFIG: Lazy<RwLock<AppConfig>> = Lazy::new(|| RwLock::new(AppConfig::parse()));

#[cfg(not(test))]
pub fn get_config() -> Result<RwLockReadGuard<'static, AppConfig>, ApiError> {
    CONFIG
        .read()
        .map_err(|e| ApiError::InternalServerError(format!("Failed to read config: {e}")))
}

#[cfg(test)]
pub fn get_config() -> Result<AppConfig, ApiError> {
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

    Ok(AppConfig {
        bind_ip: env_or_default("HUBUUM_BIND_IP", "127.0.0.1"),
        port: env_or_default("HUBUUM_BIND_PORT", "8080")
            .parse()
            .unwrap_or(8080),
        log_level: env_or_default("HUBUUM_LOG_LEVEL", "debug"),
        database_url: env_or_default("HUBUUM_DATABASE_URL", "postgres://test"),
        actix_workers: env_or_default("HUBUUM_ACTIX_WORKERS", "2")
            .parse()
            .unwrap_or(2),
        db_pool_size: env_or_default("HUBUUM_DB_POOL_SIZE", "2")
            .parse()
            .unwrap_or(5),
        admin_groupname: env_or_default("HUBUUM_ADMIN_GROUPNAME", "admin"),
        tls_cert_path: env_or_default_opt("HUBUUM_TLS_CERT_PATH", None),
        tls_key_path: env_or_default_opt("HUBUUM_TLS_KEY_PATH", None),
        tls_key_passphrase: env_or_default_opt("HUBUUM_TLS_KEY_PASSPHRASE", None),
        tls_backend: env_or_default_tls_backend("HUBUUM_TLS_BACKEND"),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        ffi::OsString,
        sync::{LazyLock, Mutex},
    };

    use clap::Parser;

    use super::{get_config, AppConfig, TlsBackend};

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_TLS_BACKEND", Some("OpEnSsL"));

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config().unwrap();

        assert_eq!(parsed.tls_backend, Some(TlsBackend::Openssl));
        assert_eq!(loaded.tls_backend, Some(TlsBackend::Openssl));
    }

    #[test]
    fn tls_backend_defaults_to_none_when_env_var_is_unset() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_TLS_BACKEND", None);

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config().unwrap();

        assert_eq!(parsed.tls_backend, None);
        assert_eq!(loaded.tls_backend, None);
    }

    #[test]
    fn tls_backend_invalid_env_var_is_rejected_by_clap_parser() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_TLS_BACKEND", Some("bogus"));

        let error = AppConfig::try_parse_from(["hubuum-server"]).unwrap_err();

        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
        assert!(error.to_string().contains("bogus"));
        assert!(error.to_string().contains("rustls"));
        assert!(error.to_string().contains("openssl"));
    }
}
