//! Explicit, safe projection of the effective process configuration.
//!
//! This is deliberately not implemented by serializing `AppConfig`: newly
//! added options are hidden until they are consciously classified here.

use serde::Serialize;
use utoipa::ToSchema;

use super::{AppConfig, ClientAllowlist, token_hash_key_ring};

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RunningConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub tasks: TaskConfig,
    pub events: EventConfig,
    pub exports: ExportConfig,
    pub backups: BackupConfig,
    pub restores: RestoreConfig,
    pub remote_calls: RemoteCallConfig,
    pub secrets: SecretSourceConfig,
    pub authentication: AuthenticationConfig,
    pub permissions: PermissionConfig,
    pub pagination: PaginationConfig,
    pub network: NetworkConfig,
    pub tracing: TracingConfig,
}

/// Public configuration values that API consumers need to use the service correctly.
///
/// Keep this projection deliberately small. Adding a value here makes it available without
/// authentication through the client configuration endpoint.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ClientConfig {
    pub pagination: ClientPaginationConfig,
    pub authentication: ClientAuthenticationConfig,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ClientPaginationConfig {
    /// Number of items returned when a paginated request omits its limit.
    #[schema(minimum = 1)]
    pub default_page_limit: usize,
    /// Largest effective page size used for a client request.
    #[schema(minimum = 1)]
    pub max_page_limit: usize,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ClientAuthenticationConfig {
    /// Lifetime applied to newly issued tokens when the mint request omits an
    /// explicit expiry.
    #[schema(minimum = 1)]
    pub default_token_lifetime_hours: i64,
    /// Largest lifetime accepted for an explicitly requested token expiry.
    #[schema(minimum = 1)]
    pub max_token_lifetime_hours: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct SecretStatus {
    /// Whether a value is configured. The value itself is never returned.
    pub configured: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct SecretSourceConfig {
    /// Selected process-wide provider. Values are `environment`, `file`, or
    /// `invalid` when startup configuration cannot be parsed.
    pub provider: String,
    /// Whether a mounted-secret root was configured. The path is never returned.
    pub file_root_configured: bool,
    pub cache_capacity_per_consumer: usize,
    pub cache_total_bytes_per_consumer: usize,
    pub cache_ttl_seconds: u64,
    pub stale_values_allowed: bool,
    pub projected_symlinks_confined_to_root: bool,
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
    /// Complete storage backend selected by the application composition root.
    pub backend: String,
    pub url: SecretStatus,
    pub pool_size: u32,
    pub pool_acquire_timeout_ms: u64,
    pub statement_timeout_ms: u64,
    pub role_mode: String,
    pub privilege_mode: String,
    pub owner_role: String,
    pub migrator_role: String,
    pub runtime_role: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TaskConfig {
    pub workers: usize,
    pub poll_interval_ms: u64,
    pub lease_seconds: u64,
    pub heartbeat_seconds: u64,
    pub recovery_interval_seconds: u64,
    pub computed_reindex_batch_size: usize,
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
    /// Backend-neutral budget applied to each export storage read stage.
    pub storage_query_budget_ms: u64,
    /// Deprecated compatibility alias for `storage_query_budget_ms`.
    ///
    /// The value is identical and remains present so administrator clients do
    /// not break while migrating away from the PostgreSQL-shaped field name.
    pub database_statement_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct BackupConfig {
    pub output_retention_hours: i64,
    pub max_active_tasks_per_user: usize,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RestoreConfig {
    pub stage_retention_minutes: i64,
    pub max_upload_bytes: usize,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RemoteCallConfig {
    pub credential_policy_count: usize,
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
    pub allow_private_targets: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct AuthenticationConfig {
    pub token_lifetime_hours: i64,
    pub max_token_lifetime_hours: i64,
    pub token_retention_purge_enabled: bool,
    pub token_retention_days: i64,
    pub token_retention_purge_interval_seconds: u64,
    #[schema(minimum = 10)]
    pub token_retention_purge_batch_size: usize,
    pub stable_token_hash_key_configured: bool,
    pub token_hash_key_mode: String,
    pub active_token_hash_key_id: String,
    pub previous_token_hash_key_ids: Vec<String>,
    pub token_hash_key_ring_identity: String,
    pub require_stable_token_hash_key: bool,
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
    pub max_traversal_work_rows: i32,
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
pub struct TracingConfig {
    pub enabled: bool,
    pub protocol: String,
    pub endpoint: SecretStatus,
    pub static_headers: SecretStatus,
    pub ca_certificate_configured: bool,
    pub client_certificate_configured: bool,
    pub client_private_key: SecretStatus,
    pub connect_timeout_ms: u64,
    pub export_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub queue_capacity: usize,
    pub batch_size: usize,
    pub sampling_mode: String,
    pub sampling_ratio: f64,
    pub service_name: String,
    pub service_namespace: String,
    pub deployment_environment: String,
    pub trust_incoming_sampling: bool,
    pub propagate_outbound: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ClientAllowlistStatus {
    pub allows_any: bool,
    pub network_count: usize,
}

impl From<&RunningConfig> for ClientConfig {
    fn from(config: &RunningConfig) -> Self {
        Self {
            pagination: ClientPaginationConfig {
                default_page_limit: config.pagination.default_page_limit,
                max_page_limit: config.pagination.max_page_limit,
            },
            authentication: ClientAuthenticationConfig {
                default_token_lifetime_hours: config.authentication.token_lifetime_hours,
                max_token_lifetime_hours: config.authentication.max_token_lifetime_hours,
            },
        }
    }
}

impl RunningConfig {
    pub(crate) fn from_app_config_and_storage(
        config: &AppConfig,
        storage: crate::storage::StorageBackendDescriptor,
    ) -> Self {
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
        let (secret_provider, secret_file_root_configured) =
            crate::secrets::running_source_configuration();
        let secret_cache_policy = hubuum_secrets::CachePolicy::default();
        let token_hash_keys = token_hash_key_ring()
            .expect("token hash key-ring configuration must be validated before serving config");

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
                backend: storage.kind().as_str().to_string(),
                url: SecretStatus {
                    configured: !config.database_url.trim().is_empty(),
                },
                pool_size: config.db_pool_size,
                pool_acquire_timeout_ms: config.db_pool_acquire_timeout_ms,
                statement_timeout_ms: config.db_statement_timeout_ms,
                role_mode: config.database_role_mode.as_str().to_string(),
                privilege_mode: config.database_privilege_mode.as_str().to_string(),
                owner_role: config.database_owner_role.clone(),
                migrator_role: config.database_migrator_role.clone(),
                runtime_role: config.database_runtime_role.clone(),
            },
            tasks: TaskConfig {
                workers: config.task_workers,
                poll_interval_ms: config.task_poll_interval_ms,
                lease_seconds: config.task_lease_seconds,
                heartbeat_seconds: config.task_heartbeat_seconds,
                recovery_interval_seconds: config.task_recovery_interval_seconds,
                computed_reindex_batch_size: config.computed_reindex_batch_size,
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
                storage_query_budget_ms: config.export_storage_query_budget_ms(),
                database_statement_timeout_ms: config.export_storage_query_budget_ms(),
            },
            backups: BackupConfig {
                output_retention_hours: config.backup_output_retention_hours,
                max_active_tasks_per_user: config.backup_max_active_tasks_per_user,
                max_output_bytes: config.backup_max_output_bytes,
            },
            restores: RestoreConfig {
                stage_retention_minutes: config.restore_stage_retention_minutes,
                max_upload_bytes: config.restore_max_upload_bytes,
            },
            remote_calls: RemoteCallConfig {
                credential_policy_count: config.remote_credential_policies.len(),
                timeout_ms: config.remote_call_timeout_ms,
                max_response_bytes: config.remote_call_max_response_bytes,
                allow_private_targets: config.remote_call_allow_private_targets,
            },
            secrets: SecretSourceConfig {
                provider: secret_provider.to_string(),
                file_root_configured: secret_file_root_configured,
                cache_capacity_per_consumer: secret_cache_policy.capacity().get(),
                cache_total_bytes_per_consumer: secret_cache_policy.total_byte_limit().get(),
                cache_ttl_seconds: secret_cache_policy.ttl().as_secs(),
                stale_values_allowed: false,
                projected_symlinks_confined_to_root: true,
            },
            authentication: AuthenticationConfig {
                token_lifetime_hours: config.token_lifetime_hours,
                max_token_lifetime_hours: config.max_token_lifetime_hours,
                token_retention_purge_enabled: config.token_retention_purge_enabled,
                token_retention_days: config.token_retention_days,
                token_retention_purge_interval_seconds: config
                    .token_retention_purge_interval_seconds,
                token_retention_purge_batch_size: config.token_retention_purge_batch_size,
                stable_token_hash_key_configured: token_hash_keys.is_stable(),
                token_hash_key_mode: if token_hash_keys.is_stable() {
                    "stable".to_string()
                } else {
                    "ephemeral".to_string()
                },
                active_token_hash_key_id: token_hash_keys.active_key_id().to_string(),
                previous_token_hash_key_ids: token_hash_keys
                    .previous_key_ids()
                    .map(ToString::to_string)
                    .collect(),
                token_hash_key_ring_identity: token_hash_keys.identity().to_string(),
                require_stable_token_hash_key: token_hash_keys.requires_stable_key(),
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
                max_traversal_work_rows: hubuum_query::MAX_TRAVERSAL_WORK_ROWS,
            },
            network: NetworkConfig {
                trust_ip_headers: config.trust_ip_headers,
                trusted_proxy_hops: config.trusted_proxy_hops,
                trusted_proxy_networks: config.trusted_proxies.nets().len(),
                client_allowlist,
            },
            tracing: TracingConfig {
                enabled: config.tracing_enabled,
                protocol: "http/protobuf".to_string(),
                endpoint: SecretStatus {
                    configured: config.tracing_otlp_endpoint.is_some(),
                },
                static_headers: SecretStatus {
                    configured: config.tracing_otlp_headers.is_some(),
                },
                ca_certificate_configured: config.tracing_otlp_ca_cert.is_some(),
                client_certificate_configured: config.tracing_otlp_client_cert.is_some(),
                client_private_key: SecretStatus {
                    configured: config.tracing_otlp_client_key.is_some(),
                },
                connect_timeout_ms: config.tracing_connect_timeout_ms,
                export_timeout_ms: config.tracing_export_timeout_ms,
                flush_timeout_ms: config.tracing_flush_timeout_ms,
                queue_capacity: config.tracing_queue_capacity,
                batch_size: config.tracing_batch_size,
                sampling_mode: config.tracing_sampling_mode.as_str().to_string(),
                sampling_ratio: config.tracing_sample_ratio,
                service_name: config.tracing_service_name.clone(),
                service_namespace: config.tracing_service_namespace.clone(),
                deployment_environment: config.tracing_deployment_environment.clone(),
                trust_incoming_sampling: config.tracing_trust_incoming_sampling,
                propagate_outbound: config.tracing_propagate_outbound,
            },
        }
    }
}

impl From<&AppConfig> for RunningConfig {
    fn from(config: &AppConfig) -> Self {
        Self::from_app_config_and_storage(
            config,
            crate::storage::StorageBackendDescriptor::new(config.storage_backend),
        )
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
        config.tracing_otlp_endpoint =
            Some("https://collector-token@collector.example.invalid".to_string());
        config.tracing_otlp_headers = Some("authorization=collector-secret".to_string());
        config.tracing_otlp_ca_cert = Some("/secret/collector-ca.pem".to_string());
        config.tracing_otlp_client_cert = Some("/secret/collector-client.pem".to_string());
        config.tracing_otlp_client_key = Some("/secret/collector-client-key.pem".to_string());

        let json = serde_json::to_string(&RunningConfig::from(&config)).unwrap();
        let debug = format!("{config:?}");

        assert!(!json.contains("secret-user"));
        assert!(!json.contains("secret-password"));
        assert!(!json.contains("private-key.pem"));
        assert!(!json.contains("correct horse battery staple"));
        assert!(!json.contains("providers.toml"));
        assert!(!json.contains("valkey.example"));
        assert!(!json.contains("treetop-token"));
        assert!(!json.contains("collector-token"));
        assert!(!json.contains("collector-secret"));
        assert!(!json.contains("collector.example.invalid"));
        assert!(!json.contains("collector-ca.pem"));
        assert!(!json.contains("collector-client.pem"));
        assert!(!json.contains("collector-client-key.pem"));
        assert!(json.contains("\"configured\":true"));
        assert!(json.contains("\"backend\":\"postgresql\""));
        assert!(json.contains(&format!(
            "\"role_mode\":\"{}\"",
            config.database_role_mode.as_str()
        )));
        assert!(json.contains("\"secrets\":{\"provider\":\"environment\""));
        assert!(json.contains("\"stale_values_allowed\":false"));
        assert!(!json.contains("contract_version"));
        assert!(!json.contains("capabilities"));
        assert!(!debug.contains("secret-password"));
        assert!(!debug.contains("correct horse battery staple"));
        assert!(!debug.contains("collector-secret"));
        assert!(!debug.contains("collector-client-key.pem"));
    }
}
