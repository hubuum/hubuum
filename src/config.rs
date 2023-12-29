use clap::Parser;
use serde::{Deserialize, Serialize};

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
}
