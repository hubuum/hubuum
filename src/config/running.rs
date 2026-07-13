//! Explicit, safe projection of the effective process configuration.
//!
//! This is deliberately not implemented by serializing `AppConfig`: newly
//! added options are hidden until they are consciously classified here.

use serde::Serialize;
use utoipa::ToSchema;

use super::{AppConfig, ClientAllowlist, token_hash_key_is_ephemeral};

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RunningConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub tasks: TaskConfig,
    pub events: EventConfig,
    pub exports: ExportConfig,
    pub remote_calls: RemoteCallConfig,
    pub authentication: AuthenticationConfig,
    pub permissions: PermissionConfig,
    pub pagination: PaginationConfig,
    pub network: NetworkConfig,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct SecretStatus {
    /// Whether a value is configured. The value itself is never returned.
    pub configured: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServerConfig {
    pub runtime_role: String,
    pub bind_ip: String,
    pub bind_port: u16,
    pub log_level: String,
    pub actix_workers: usize,
    pub metrics_enabled: bool,
    pub metrics_path: String,
    pub tls: TlsConfig,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TlsConfig {
    pub enabled: bool,
    pub backend: Option<String>,
    pub certificate_path_configured: bool,
    pub private_key_path: SecretStatus,
    pub private_key_passphrase: SecretStatus,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DatabaseConfig {
    pub url: SecretStatus,
    pub pool_size: u32,
    pub pool_acquire_timeout_ms: u64,
    pub statement_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TaskConfig {
    pub workers: usize,
    pub poll_interval_ms: u64,
    pub lease_seconds: u64,
    pub heartbeat_seconds: u64,
    pub recovery_interval_seconds: u64,
    pub import_max_active_per_user: usize,
    pub export_max_active_per_user: usize,
    pub remote_call_max_active_per_user: usize,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct EventConfig {
    pub fanout_workers: usize,
    pub fanout_batch_size: usize,
    pub fanout_poll_interval_ms: u64,
    pub fanout_lock_timeout_ms: u64,
    pub delivery_workers: usize,
    pub delivery_batch_size: usize,
    pub delivery_poll_interval_ms: u64,
    pub delivery_lock_timeout_ms: u64,
    pub delivery_transport_timeout_ms: u64,
    pub delivery_retry_backoff_base_ms: u64,
    pub delivery_retry_backoff_max_ms: u64,
    pub delivery_max_attempts: i32,
    pub retention_purge_enabled: bool,
    pub retention_days: i64,
    pub delivery_retention_days: i64,
    pub retention_purge_interval_seconds: u64,
    pub retention_purge_batch_size: usize,
    pub retention_file_archive_enabled: bool,
    pub retention_archive_path_configured: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ExportConfig {
    pub output_retention_hours: i64,
    pub output_cleanup_interval_seconds: u64,
    pub template_recursion_limit: usize,
    pub template_fuel: u64,
    pub template_max_objects: usize,
    pub max_output_bytes: usize,
    pub stage_timeout_ms: u64,
    pub database_statement_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RemoteCallConfig {
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
    pub allow_private_targets: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct AuthenticationConfig {
    pub token_lifetime_hours: i64,
    pub stable_token_hash_key_configured: bool,
    pub admin_groupname: String,
    pub admin_identity_scope: Option<String>,
    pub provider_config_path: SecretStatus,
    pub login_rate_limit: LoginRateLimitConfig,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
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
    pub backend: String,
    pub valkey_url: SecretStatus,
    pub valkey_prefix: String,
    pub valkey_io_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct PaginationConfig {
    pub default_page_limit: usize,
    pub max_page_limit: usize,
    pub max_transitive_depth: i32,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct PermissionConfig {
    pub backend: String,
    pub treetop_url: SecretStatus,
    pub treetop_connect_timeout_ms: u64,
    pub treetop_request_timeout_ms: u64,
    pub treetop_ca_certificate_configured: bool,
    pub treetop_accept_invalid_certificates: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct NetworkConfig {
    pub trust_ip_headers: bool,
    pub trusted_proxy_hops: usize,
    pub trusted_proxy_networks: usize,
    pub client_allowlist: ClientAllowlistStatus,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ClientAllowlistStatus {
    pub allows_any: bool,
    pub network_count: usize,
}

impl From<&AppConfig> for RunningConfig {
    fn from(config: &AppConfig) -> Self {
        let client_allowlist = match &config.client_allowlist {
            ClientAllowlist::Any => ClientAllowlistStatus {
                allows_any: true,
                network_count: 0,
            },
            ClientAllowlist::Nets(networks) => ClientAllowlistStatus {
                allows_any: false,
                network_count: networks.len(),
            },
        };

        Self {
            server: ServerConfig {
                runtime_role: config.runtime_role.as_str().to_string(),
                bind_ip: config.bind_ip.clone(),
                bind_port: config.port,
                log_level: config.log_level.clone(),
                actix_workers: config.actix_workers,
                metrics_enabled: config.metrics_enabled,
                metrics_path: config.metrics_path.as_str().to_string(),
                tls: TlsConfig {
                    enabled: config.tls_cert_path.is_some() && config.tls_key_path.is_some(),
                    backend: config.tls_backend.map(|backend| match backend {
                        super::TlsBackend::Rustls => "rustls".to_string(),
                        super::TlsBackend::Openssl => "openssl".to_string(),
                    }),
                    certificate_path_configured: config.tls_cert_path.is_some(),
                    private_key_path: SecretStatus {
                        configured: config.tls_key_path.is_some(),
                    },
                    private_key_passphrase: SecretStatus {
                        configured: config.tls_key_passphrase.is_some(),
                    },
                },
            },
            database: DatabaseConfig {
                url: SecretStatus {
                    configured: !config.database_url.trim().is_empty(),
                },
                pool_size: config.db_pool_size,
                pool_acquire_timeout_ms: config.db_pool_acquire_timeout_ms,
                statement_timeout_ms: config.db_statement_timeout_ms,
            },
            tasks: TaskConfig {
                workers: config.task_workers,
                poll_interval_ms: config.task_poll_interval_ms,
                lease_seconds: config.task_lease_seconds,
                heartbeat_seconds: config.task_heartbeat_seconds,
                recovery_interval_seconds: config.task_recovery_interval_seconds,
                import_max_active_per_user: config.import_max_active_tasks_per_user,
                export_max_active_per_user: config.export_max_active_tasks_per_user,
                remote_call_max_active_per_user: config.remote_call_max_active_tasks_per_user,
            },
            events: EventConfig {
                fanout_workers: config.event_fanout_workers,
                fanout_batch_size: config.event_fanout_batch_size,
                fanout_poll_interval_ms: config.event_fanout_poll_interval_ms,
                fanout_lock_timeout_ms: config.event_fanout_lock_timeout_ms,
                delivery_workers: config.event_delivery_workers,
                delivery_batch_size: config.event_delivery_batch_size,
                delivery_poll_interval_ms: config.event_delivery_poll_interval_ms,
                delivery_lock_timeout_ms: config.event_delivery_lock_timeout_ms,
                delivery_transport_timeout_ms: config.event_delivery_transport_timeout_ms,
                delivery_retry_backoff_base_ms: config.event_delivery_retry_backoff_base_ms,
                delivery_retry_backoff_max_ms: config.event_delivery_retry_backoff_max_ms,
                delivery_max_attempts: config.event_delivery_max_attempts,
                retention_purge_enabled: config.event_retention_purge_enabled,
                retention_days: config.event_retention_days,
                delivery_retention_days: config.event_delivery_retention_days,
                retention_purge_interval_seconds: config.event_retention_purge_interval_seconds,
                retention_purge_batch_size: config.event_retention_purge_batch_size,
                retention_file_archive_enabled: config.event_retention_file_archive_enabled,
                retention_archive_path_configured: config.event_retention_archive_path.is_some(),
            },
            exports: ExportConfig {
                output_retention_hours: config.export_output_retention_hours,
                output_cleanup_interval_seconds: config.export_output_cleanup_interval_seconds,
                template_recursion_limit: config.export_template_recursion_limit,
                template_fuel: config.export_template_fuel,
                template_max_objects: config.export_template_max_objects,
                max_output_bytes: config.export_max_output_bytes,
                stage_timeout_ms: config.export_stage_timeout_ms,
                database_statement_timeout_ms: config.export_db_statement_timeout_ms,
            },
            remote_calls: RemoteCallConfig {
                timeout_ms: config.remote_call_timeout_ms,
                max_response_bytes: config.remote_call_max_response_bytes,
                allow_private_targets: config.remote_call_allow_private_targets,
            },
            authentication: AuthenticationConfig {
                token_lifetime_hours: config.token_lifetime_hours,
                stable_token_hash_key_configured: !token_hash_key_is_ephemeral(),
                admin_groupname: config.admin_groupname.clone(),
                admin_identity_scope: config.admin_identity_scope.clone(),
                provider_config_path: SecretStatus {
                    configured: config.auth_config_path.is_some(),
                },
                login_rate_limit: LoginRateLimitConfig {
                    enabled: config.login_rate_limit_enabled,
                    max_attempts: config.login_rate_limit_max_attempts,
                    max_attempts_per_ip: config.login_rate_limit_max_attempts_per_ip,
                    max_attempts_per_subnet: config.login_rate_limit_max_attempts_per_subnet,
                    window_seconds: config.login_rate_limit_window_seconds,
                    backoff_base_seconds: config.login_rate_limit_backoff_base_seconds,
                    backoff_max_seconds: config.login_rate_limit_backoff_max_seconds,
                    subnet_prefix_v4: config.login_rate_limit_subnet_prefix_v4,
                    subnet_prefix_v6: config.login_rate_limit_subnet_prefix_v6,
                    backend: config.login_rate_limit_backend.as_str().to_string(),
                    valkey_url: SecretStatus {
                        configured: config.login_rate_limit_valkey_url.is_some(),
                    },
                    valkey_prefix: config.login_rate_limit_valkey_prefix.clone(),
                    valkey_io_timeout_ms: config.login_rate_limit_valkey_io_timeout_ms,
                },
            },
            permissions: PermissionConfig {
                backend: match config.permission_backend {
                    super::PermissionBackendKind::Local => "local".to_string(),
                    super::PermissionBackendKind::Treetop => "treetop".to_string(),
                },
                treetop_url: SecretStatus {
                    configured: config.treetop_url.is_some(),
                },
                treetop_connect_timeout_ms: config.treetop_connect_timeout_ms,
                treetop_request_timeout_ms: config.treetop_request_timeout_ms,
                treetop_ca_certificate_configured: config.treetop_ca_cert.is_some(),
                treetop_accept_invalid_certificates: config.treetop_accept_invalid_certs,
            },
            pagination: PaginationConfig {
                default_page_limit: config.default_page_limit,
                max_page_limit: config.max_page_limit,
                max_transitive_depth: config.max_transitive_depth,
            },
            network: NetworkConfig {
                trust_ip_headers: config.trust_ip_headers,
                trusted_proxy_hops: config.trusted_proxy_hops,
                trusted_proxy_networks: config.trusted_proxies.nets().len(),
                client_allowlist,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn running_config_is_an_explicit_redacted_projection() {
        let mut config = AppConfig::parse_from(["hubuum"]);
        config.database_url = "postgres://secret-user:secret-password@example/db".to_string();
        config.tls_key_path = Some("/secret/private-key.pem".to_string());
        config.tls_key_passphrase = Some("correct horse battery staple".to_string());
        config.auth_config_path = Some("/secret/providers.toml".to_string());
        config.login_rate_limit_valkey_url =
            Some("redis://secret-user:secret-password@valkey.example/".to_string());
        config.treetop_url = Some("https://treetop-token@example.invalid".to_string());

        let json = serde_json::to_string(&RunningConfig::from(&config)).unwrap();
        let debug = format!("{config:?}");

        assert!(!json.contains("secret-user"));
        assert!(!json.contains("secret-password"));
        assert!(!json.contains("private-key.pem"));
        assert!(!json.contains("correct horse battery staple"));
        assert!(!json.contains("providers.toml"));
        assert!(!json.contains("valkey.example"));
        assert!(!json.contains("treetop-token"));
        assert!(json.contains("\"configured\":true"));
        assert!(!debug.contains("secret-password"));
        assert!(!debug.contains("correct horse battery staple"));
    }
}
