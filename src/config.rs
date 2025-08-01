#[cfg(not(test))]
use std::sync::{RwLock, RwLockReadGuard};

use clap::Parser;
#[cfg(not(test))]
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::errors::ApiError;

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
    })
}
