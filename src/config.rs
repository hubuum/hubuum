use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(not(test))]
use std::sync::{RwLock, RwLockReadGuard};

use clap::{Parser, ValueEnum};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use uuid::Uuid;

use crate::errors::ApiError;

pub const DEFAULT_PAGE_LIMIT: usize = 100;
pub const MAX_PAGE_LIMIT: usize = 250;
pub const DEFAULT_TASK_POLL_INTERVAL_MS: u64 = 200;
pub const DEFAULT_EVENT_FANOUT_WORKERS: usize = 1;
pub const DEFAULT_EVENT_FANOUT_BATCH_SIZE: usize = 100;
pub const DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS: u64 = 250;
pub const DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_EVENT_DELIVERY_WORKERS: usize = 0;
pub const DEFAULT_EVENT_DELIVERY_BATCH_SIZE: usize = 100;
pub const DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS: u64 = 500;
pub const DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS: u64 = 1_000;
pub const DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS: u64 = 300_000;
pub const DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS: i32 = 10;
pub const DEFAULT_EVENT_RETENTION_PURGE_ENABLED: bool = false;
pub const DEFAULT_EVENT_RETENTION_DAYS: i64 = 365;
pub const DEFAULT_EVENT_DELIVERY_RETENTION_DAYS: i64 = 30;
pub const DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS: u64 = 3_600;
pub const DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE: usize = 1_000;
pub const DEFAULT_REPORT_OUTPUT_RETENTION_HOURS: i64 = 24 * 7;
pub const DEFAULT_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS: u64 = 300;
pub const DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER: usize = 100;
pub const DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER: usize = 100;
pub const DEFAULT_REPORT_TEMPLATE_RECURSION_LIMIT: usize = 64;
pub const DEFAULT_REPORT_TEMPLATE_FUEL: u64 = 50_000;
pub const DEFAULT_REPORT_TEMPLATE_MAX_OBJECTS: usize = 2_000;
pub const DEFAULT_REPORT_MAX_OUTPUT_BYTES: usize = 262_144;
pub const DEFAULT_REPORT_STAGE_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_REMOTE_CALL_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES: usize = 262_144;
pub const DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS: bool = false;
pub const DEFAULT_DB_STATEMENT_TIMEOUT_MS: u64 = 0;
pub const DEFAULT_REPORT_DB_STATEMENT_TIMEOUT_MS: u64 = 0;
pub const DEFAULT_TOKEN_LIFETIME_HOURS: i64 = 24;
pub const DEFAULT_LOGIN_RATE_LIMIT_ENABLED: bool = true;
pub const DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS: usize = 5;
pub const DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP: usize = 20;
pub const DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET: usize = 100;
pub const DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS: u64 = 300;
pub const DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS: u64 = 300;
pub const DEFAULT_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS: u64 = 86_400;
pub const DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4: u8 = 24;
pub const DEFAULT_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6: u8 = 64;
pub const DEFAULT_TRUSTED_PROXY_HOPS: usize = 0;
pub const DEFAULT_MAX_TRANSITIVE_DEPTH: i32 = 100;

struct TokenHashKeyConfig {
    key: Vec<u8>,
    is_ephemeral: bool,
}

static TOKEN_HASH_KEY_CONFIG: LazyLock<TokenHashKeyConfig> = LazyLock::new(|| {
    if let Ok(env_key) = std::env::var("HUBUUM_TOKEN_HASH_KEY") {
        let trimmed = env_key.trim();
        if !trimmed.is_empty() {
            return TokenHashKeyConfig {
                key: trimmed.as_bytes().to_vec(),
                is_ephemeral: false,
            };
        }
    }

    let generated = format!("{}{}", Uuid::new_v4(), Uuid::new_v4());
    TokenHashKeyConfig {
        key: generated.into_bytes(),
        is_ephemeral: true,
    }
});

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

    /// Optional JSONL archive path for events selected by the retention purge.
    #[clap(
        long,
        env = "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
        default_value = None
    )]
    pub event_retention_archive_path: Option<String>,

    /// How long successful stored report outputs remain available for refetch.
    #[clap(
        long,
        env = "HUBUUM_REPORT_OUTPUT_RETENTION_HOURS",
        default_value_t = DEFAULT_REPORT_OUTPUT_RETENTION_HOURS
    )]
    pub report_output_retention_hours: i64,

    /// How often workers attempt cleanup of expired stored report outputs.
    #[clap(
        long,
        env = "HUBUUM_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS",
        default_value_t = DEFAULT_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS
    )]
    pub report_output_cleanup_interval_seconds: u64,

    /// Maximum queued/validating/running report tasks one user may have at once.
    #[clap(
        long,
        env = "HUBUUM_REPORT_MAX_ACTIVE_TASKS_PER_USER",
        default_value_t = DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER
    )]
    pub report_max_active_tasks_per_user: usize,

    /// MiniJinja recursion limit for report template rendering.
    #[clap(
        long,
        env = "HUBUUM_REPORT_TEMPLATE_RECURSION_LIMIT",
        default_value_t = DEFAULT_REPORT_TEMPLATE_RECURSION_LIMIT
    )]
    pub report_template_recursion_limit: usize,

    /// MiniJinja fuel budget for report template rendering.
    #[clap(
        long,
        env = "HUBUUM_REPORT_TEMPLATE_FUEL",
        default_value_t = DEFAULT_REPORT_TEMPLATE_FUEL
    )]
    pub report_template_fuel: u64,

    /// Maximum number of hydrated relation-aware template objects rendered for one report root.
    #[clap(
        long,
        env = "HUBUUM_REPORT_TEMPLATE_MAX_OBJECTS",
        default_value_t = DEFAULT_REPORT_TEMPLATE_MAX_OBJECTS
    )]
    pub report_template_max_objects: usize,

    /// Maximum rendered report output size accepted for storage or response.
    #[clap(
        long,
        env = "HUBUUM_REPORT_MAX_OUTPUT_BYTES",
        default_value_t = DEFAULT_REPORT_MAX_OUTPUT_BYTES
    )]
    pub report_max_output_bytes: usize,

    /// Post-completion budget per report execution stage, in milliseconds.
    ///
    /// This is a *rejection* budget, not an in-flight interrupt: a report is
    /// rejected only after a stage (query, hydration, render) has finished if it
    /// exceeded this value. Real in-flight protection comes from minijinja
    /// `report_template_fuel`, `report_template_max_objects`, the output byte
    /// caps, and `db_statement_timeout_ms` (which actually cancels slow queries
    /// server-side).
    #[clap(
        long,
        env = "HUBUUM_REPORT_STAGE_TIMEOUT_MS",
        default_value_t = DEFAULT_REPORT_STAGE_TIMEOUT_MS
    )]
    pub report_stage_timeout_ms: u64,

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
    /// work - reports, imports, admin commands, health/auth queries, and
    /// migrations sharing the pool - not just report stages. Postgres cancels any
    /// statement exceeding it server-side, which frees the connection (a genuine
    /// in-flight timeout). Disabled by default to preserve existing behavior.
    #[clap(
        long,
        env = "HUBUUM_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    )]
    pub db_statement_timeout_ms: u64,

    /// Report-scoped Postgres `statement_timeout` in milliseconds (0 = disabled).
    ///
    /// Unlike `db_statement_timeout_ms`, this bounds *only* the queries issued
    /// while executing a report (scope query, includes, relation hydration). It
    /// is applied as a transaction-local `SET LOCAL statement_timeout` on those
    /// queries, so it does not affect imports, admin commands, or any other DB
    /// work sharing the pool. This lets operators cap report queries aggressively
    /// without capping legitimately long-running work. When set it should
    /// typically be `<= report_stage_timeout_ms` (the post-completion wall-clock
    /// budget). Disabled by default to preserve existing behavior.
    #[clap(
        long,
        env = "HUBUUM_REPORT_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_REPORT_DB_STATEMENT_TIMEOUT_MS
    )]
    pub report_db_statement_timeout_ms: u64,

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
    #[clap(long, default_value = "", env = "HUBUUM_TRUSTED_PROXIES", value_parser = parse_trusted_proxies)]
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
    #[clap(long, default_value = "127.0.0.1,::1", env = "HUBUUM_CLIENT_ALLOWLIST", value_parser = parse_client_allowlist)]
    pub client_allowlist: ClientAllowlist,
}

fn parse_client_allowlist(s: &str) -> Result<ClientAllowlist, String> {
    ClientAllowlist::from_str(s).map_err(|e| e.to_string())
}

fn parse_trusted_proxies(s: &str) -> Result<TrustedProxies, String> {
    TrustedProxies::from_str(s).map_err(|e| e.to_string())
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

        if self.event_fanout_workers == 0 {
            return Err(ApiError::BadRequest(
                "event_fanout_workers must be greater than 0".to_string(),
            ));
        }

        if self.event_fanout_batch_size == 0 {
            return Err(ApiError::BadRequest(
                "event_fanout_batch_size must be greater than 0".to_string(),
            ));
        }

        if self.event_fanout_poll_interval_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_fanout_poll_interval_ms must be greater than 0".to_string(),
            ));
        }

        if self.event_fanout_lock_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_fanout_lock_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_batch_size == 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_batch_size must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_poll_interval_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_poll_interval_ms must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_lock_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_lock_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_retry_backoff_base_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_retry_backoff_base_ms must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_retry_backoff_max_ms == 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_retry_backoff_max_ms must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_retry_backoff_base_ms > self.event_delivery_retry_backoff_max_ms {
            return Err(ApiError::BadRequest(format!(
                "event_delivery_retry_backoff_base_ms ({}) must be less than or equal to event_delivery_retry_backoff_max_ms ({})",
                self.event_delivery_retry_backoff_base_ms, self.event_delivery_retry_backoff_max_ms
            )));
        }

        if self.event_delivery_max_attempts <= 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_max_attempts must be greater than 0".to_string(),
            ));
        }

        if self.event_retention_days <= 0 {
            return Err(ApiError::BadRequest(
                "event_retention_days must be greater than 0".to_string(),
            ));
        }

        if self.event_delivery_retention_days <= 0 {
            return Err(ApiError::BadRequest(
                "event_delivery_retention_days must be greater than 0".to_string(),
            ));
        }

        if self.event_retention_purge_interval_seconds == 0 {
            return Err(ApiError::BadRequest(
                "event_retention_purge_interval_seconds must be greater than 0".to_string(),
            ));
        }

        if self.event_retention_purge_batch_size == 0 {
            return Err(ApiError::BadRequest(
                "event_retention_purge_batch_size must be greater than 0".to_string(),
            ));
        }

        if self.report_output_retention_hours <= 0 {
            return Err(ApiError::BadRequest(
                "report_output_retention_hours must be greater than 0".to_string(),
            ));
        }

        if self.report_output_cleanup_interval_seconds == 0 {
            return Err(ApiError::BadRequest(
                "report_output_cleanup_interval_seconds must be greater than 0".to_string(),
            ));
        }

        if self.report_max_active_tasks_per_user == 0 {
            return Err(ApiError::BadRequest(
                "report_max_active_tasks_per_user must be greater than 0".to_string(),
            ));
        }

        if self.report_template_recursion_limit == 0 {
            return Err(ApiError::BadRequest(
                "report_template_recursion_limit must be greater than 0".to_string(),
            ));
        }

        if self.report_template_fuel == 0 {
            return Err(ApiError::BadRequest(
                "report_template_fuel must be greater than 0".to_string(),
            ));
        }

        if self.report_template_max_objects == 0 {
            return Err(ApiError::BadRequest(
                "report_template_max_objects must be greater than 0".to_string(),
            ));
        }

        if self.report_max_output_bytes == 0 {
            return Err(ApiError::BadRequest(
                "report_max_output_bytes must be greater than 0".to_string(),
            ));
        }

        if self.report_stage_timeout_ms == 0 {
            return Err(ApiError::BadRequest(
                "report_stage_timeout_ms must be greater than 0".to_string(),
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

        if self.token_lifetime_hours <= 0 {
            return Err(ApiError::BadRequest(
                "token_lifetime_hours must be greater than 0".to_string(),
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

        if self.login_rate_limit_backoff_max_seconds < self.login_rate_limit_backoff_base_seconds {
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

        if self.max_transitive_depth <= 0 {
            return Err(ApiError::BadRequest(
                "max_transitive_depth must be greater than 0".to_string(),
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

pub fn token_lifetime_hours_i32() -> i32 {
    #[cfg(test)]
    let hours = get_config_from_env()
        .map(|config| config.token_lifetime_hours)
        .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS);

    #[cfg(not(test))]
    let hours = get_config()
        .map(|config| config.token_lifetime_hours)
        .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS);

    hours.clamp(1, i32::MAX as i64) as i32
}

pub fn max_transitive_depth() -> i32 {
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

pub fn token_hash_key_bytes() -> &'static [u8] {
    &TOKEN_HASH_KEY_CONFIG.key
}

pub fn token_hash_key_is_ephemeral() -> bool {
    TOKEN_HASH_KEY_CONFIG.is_ephemeral
}

/// Snapshot of all login rate-limit knobs, resolved once per check so a single request
/// sees a consistent configuration.
#[derive(Debug, Clone, Copy)]
pub struct LoginRateLimitConfig {
    pub enabled: bool,
    pub max_attempts: usize,
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
    #[cfg(test)]
    let config = get_config_from_env()
        .map(|config| LoginRateLimitConfig::from(&config))
        .unwrap_or_default();

    #[cfg(not(test))]
    let config = get_config()
        .map(|config| LoginRateLimitConfig::from(&*config))
        .unwrap_or_default();

    config
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
pub static CONFIG: LazyLock<RwLock<AppConfig>> = LazyLock::new(|| {
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

    fn env_or_default_trusted_proxies(key: &str, default: &str) -> TrustedProxies {
        env::var(key)
            .unwrap_or_else(|_| default.to_string())
            .parse()
            .unwrap_or_else(|e: ApiError| panic!("Invalid trusted proxies in {key}: {e}"))
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
        event_retention_archive_path: env_or_default_opt(
            "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
            None,
        ),
        report_output_retention_hours: env::var("HUBUUM_REPORT_OUTPUT_RETENTION_HOURS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_OUTPUT_RETENTION_HOURS),
        report_output_cleanup_interval_seconds: env::var(
            "HUBUUM_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS",
        )
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS),
        report_max_active_tasks_per_user: env::var("HUBUUM_REPORT_MAX_ACTIVE_TASKS_PER_USER")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER),
        report_template_recursion_limit: env::var("HUBUUM_REPORT_TEMPLATE_RECURSION_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_TEMPLATE_RECURSION_LIMIT),
        report_template_fuel: env::var("HUBUUM_REPORT_TEMPLATE_FUEL")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_TEMPLATE_FUEL),
        report_template_max_objects: env::var("HUBUUM_REPORT_TEMPLATE_MAX_OBJECTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_TEMPLATE_MAX_OBJECTS),
        report_max_output_bytes: env::var("HUBUUM_REPORT_MAX_OUTPUT_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_MAX_OUTPUT_BYTES),
        report_stage_timeout_ms: env::var("HUBUUM_REPORT_STAGE_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_STAGE_TIMEOUT_MS),
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
        report_db_statement_timeout_ms: env::var("HUBUUM_REPORT_DB_STATEMENT_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REPORT_DB_STATEMENT_TIMEOUT_MS),
        db_pool_size: env_or_default("HUBUUM_DB_POOL_SIZE", "2")
            .parse()
            .unwrap_or(5),
        token_lifetime_hours: env_or_default("HUBUUM_TOKEN_LIFETIME_HOURS", "24")
            .parse()
            .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS),
        login_rate_limit_enabled: env_or_default("HUBUUM_LOGIN_RATE_LIMIT_ENABLED", "true")
            .parse()
            .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_ENABLED),
        login_rate_limit_max_attempts: env_or_default("HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "5")
            .parse()
            .unwrap_or(DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS),
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
        tls_cert_path: env_or_default_opt("HUBUUM_TLS_CERT_PATH", None),
        tls_key_path: env_or_default_opt("HUBUUM_TLS_KEY_PATH", None),
        tls_key_passphrase: env_or_default_opt("HUBUUM_TLS_KEY_PASSPHRASE", None),
        tls_backend: env_or_default_tls_backend("HUBUUM_TLS_BACKEND"),
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

/// Trusted reverse-proxy networks used to resolve the real client IP from a forwarded
/// hop chain. Unlike [`ClientAllowlist`], an empty set is valid and means "no trusted
/// proxies configured".
#[derive(Debug, Clone, Default)]
pub struct TrustedProxies(Vec<IpNet>);

impl TrustedProxies {
    /// The configured trusted-proxy networks.
    pub fn nets(&self) -> &[IpNet] {
        &self.0
    }
}

impl FromStr for TrustedProxies {
    type Err = ApiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let nets = s
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ClientAllowlist::parse_net)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TrustedProxies(nets))
    }
}

impl Serialize for TrustedProxies {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = self
            .0
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for TrustedProxies {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TrustedProxies::from_str(&s).map_err(serde::de::Error::custom)
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
        AppConfig, DEFAULT_EVENT_DELIVERY_BATCH_SIZE, DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
        DEFAULT_EVENT_DELIVERY_MAX_ATTEMPTS, DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS,
        DEFAULT_EVENT_DELIVERY_RETENTION_DAYS, DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS,
        DEFAULT_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS, DEFAULT_EVENT_DELIVERY_WORKERS,
        DEFAULT_EVENT_FANOUT_BATCH_SIZE, DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
        DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS, DEFAULT_EVENT_FANOUT_WORKERS,
        DEFAULT_EVENT_RETENTION_DAYS, DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE,
        DEFAULT_EVENT_RETENTION_PURGE_ENABLED, DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS,
        DEFAULT_LOGIN_RATE_LIMIT_MAX_ATTEMPTS, DEFAULT_LOGIN_RATE_LIMIT_WINDOW_SECONDS,
        DEFAULT_PAGE_LIMIT, DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER,
        DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER, DEFAULT_REPORT_MAX_OUTPUT_BYTES,
        DEFAULT_TASK_POLL_INTERVAL_MS, DEFAULT_TOKEN_LIFETIME_HOURS, MAX_PAGE_LIMIT, TEST_ENV_LOCK,
        TlsBackend, default_actix_workers, default_task_workers, get_config_from_env,
        token_hash_key_bytes, token_hash_key_is_ephemeral,
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
        let _base_guard =
            EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS", Some("100"));
        let _max_guard =
            EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS", Some("5000"));
        let _attempts_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS", Some("4"));

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(parsed.event_delivery_workers, 2);
        assert_eq!(parsed.event_delivery_batch_size, 25);
        assert_eq!(parsed.event_delivery_poll_interval_ms, 750);
        assert_eq!(parsed.event_delivery_lock_timeout_ms, 45000);
        assert_eq!(parsed.event_delivery_retry_backoff_base_ms, 100);
        assert_eq!(parsed.event_delivery_retry_backoff_max_ms, 5000);
        assert_eq!(parsed.event_delivery_max_attempts, 4);
        assert_eq!(loaded.event_delivery_workers, 2);
        assert_eq!(loaded.event_delivery_batch_size, 25);
        assert_eq!(loaded.event_delivery_poll_interval_ms, 750);
        assert_eq!(loaded.event_delivery_lock_timeout_ms, 45000);
        assert_eq!(loaded.event_delivery_retry_backoff_base_ms, 100);
        assert_eq!(loaded.event_delivery_retry_backoff_max_ms, 5000);
        assert_eq!(loaded.event_delivery_max_attempts, 4);
    }

    #[test]
    fn event_retention_settings_are_parsed_from_env() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _enabled_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", Some("true"));
        let _event_days_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_DAYS", Some("90"));
        let _delivery_days_guard =
            EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETENTION_DAYS", Some("14"));
        let _interval_guard =
            EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS", Some("600"));
        let _batch_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE", Some("250"));
        let _archive_guard = EnvVarGuard::set(
            "HUBUUM_EVENT_RETENTION_ARCHIVE_PATH",
            Some("/tmp/hubuum-events.jsonl"),
        );

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert!(parsed.event_retention_purge_enabled);
        assert_eq!(parsed.event_retention_days, 90);
        assert_eq!(parsed.event_delivery_retention_days, 14);
        assert_eq!(parsed.event_retention_purge_interval_seconds, 600);
        assert_eq!(parsed.event_retention_purge_batch_size, 250);
        assert_eq!(
            parsed.event_retention_archive_path.as_deref(),
            Some("/tmp/hubuum-events.jsonl")
        );
        assert!(loaded.event_retention_purge_enabled);
        assert_eq!(loaded.event_retention_days, 90);
        assert_eq!(loaded.event_delivery_retention_days, 14);
        assert_eq!(loaded.event_retention_purge_interval_seconds, 600);
        assert_eq!(loaded.event_retention_purge_batch_size, 250);
        assert_eq!(
            loaded.event_retention_archive_path.as_deref(),
            Some("/tmp/hubuum-events.jsonl")
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
        let _delivery_base_guard =
            EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS", None);
        let _delivery_max_guard =
            EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS", None);
        let _delivery_attempts_guard = EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS", None);
        let _retention_enabled_guard =
            EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_ENABLED", None);
        let _retention_days_guard = EnvVarGuard::set("HUBUUM_EVENT_RETENTION_DAYS", None);
        let _retention_delivery_days_guard =
            EnvVarGuard::set("HUBUUM_EVENT_DELIVERY_RETENTION_DAYS", None);
        let _retention_interval_guard =
            EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS", None);
        let _retention_batch_guard =
            EnvVarGuard::set("HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE", None);
        let _retention_archive_guard =
            EnvVarGuard::set("HUBUUM_EVENT_RETENTION_ARCHIVE_PATH", None);

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
        assert_eq!(loaded.event_retention_archive_path, None);
    }

    #[test]
    fn report_max_output_bytes_is_parsed_from_env() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_REPORT_MAX_OUTPUT_BYTES", Some("4096"));

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(parsed.report_max_output_bytes, 4096);
        assert_eq!(loaded.report_max_output_bytes, 4096);
    }

    #[test]
    fn report_max_output_bytes_defaults_when_env_var_is_unset() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_REPORT_MAX_OUTPUT_BYTES", None);

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(
            parsed.report_max_output_bytes,
            DEFAULT_REPORT_MAX_OUTPUT_BYTES
        );
        assert_eq!(
            loaded.report_max_output_bytes,
            DEFAULT_REPORT_MAX_OUTPUT_BYTES
        );
    }

    #[test]
    fn report_max_output_bytes_is_validated() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_REPORT_MAX_OUTPUT_BYTES", Some("0"));

        let error = get_config_from_env().unwrap_err();

        assert_eq!(
            error.to_string(),
            "report_max_output_bytes must be greater than 0"
        );
    }

    #[test]
    fn report_max_active_tasks_per_user_is_parsed_from_env() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_REPORT_MAX_ACTIVE_TASKS_PER_USER", Some("7"));

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(parsed.report_max_active_tasks_per_user, 7);
        assert_eq!(loaded.report_max_active_tasks_per_user, 7);
    }

    #[test]
    fn report_max_active_tasks_per_user_defaults_when_env_var_is_unset() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_REPORT_MAX_ACTIVE_TASKS_PER_USER", None);

        let parsed = AppConfig::try_parse_from(["hubuum-server"]).unwrap();
        let loaded = get_config_from_env().unwrap();

        assert_eq!(
            parsed.report_max_active_tasks_per_user,
            DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER
        );
        assert_eq!(
            loaded.report_max_active_tasks_per_user,
            DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER
        );
    }

    #[test]
    fn report_max_active_tasks_per_user_is_validated() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set("HUBUUM_REPORT_MAX_ACTIVE_TASKS_PER_USER", Some("0"));

        let error = get_config_from_env().unwrap_err();

        assert_eq!(
            error.to_string(),
            "report_max_active_tasks_per_user must be greater than 0"
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
}
