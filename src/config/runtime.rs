//! Resolve runtime policies once at an operation boundary.
use super::AppConfig;
use crate::{errors::ApiError, storage::StorageHandle};
use hubuum_storage_core::{
    StorageRuntimeSettingsGroup, StorageRuntimeSettingsSnapshot, StorageRuntimeSettingsValue,
};
use serde_json::{Value, json};
use std::{future::Future, sync::Arc};

tokio::task_local! { static ACTIVE_CONFIG: Arc<AppConfig>; }

pub(crate) fn scoped() -> Option<Arc<AppConfig>> {
    ACTIVE_CONFIG.try_with(Arc::clone).ok()
}

pub(crate) fn current() -> Result<Arc<AppConfig>, ApiError> {
    ACTIVE_CONFIG
        .try_with(Arc::clone)
        .or_else(|_| super::get_config().map(|config| Arc::new(config.clone())))
}

pub(crate) async fn scope<T>(config: Arc<AppConfig>, future: impl Future<Output = T>) -> T {
    ACTIVE_CONFIG.scope(config, future).await
}

pub(crate) fn for_storage(storage: &StorageHandle) -> Result<Arc<AppConfig>, ApiError> {
    for_snapshot(storage.applied_runtime_settings().snapshot())
}

pub(crate) fn for_snapshot(
    snapshot: Option<&StorageRuntimeSettingsSnapshot>,
) -> Result<Arc<AppConfig>, ApiError> {
    Ok(Arc::new(effective(super::get_config()?.clone(), snapshot)))
}

pub(crate) fn resolve(
    config: AppConfig,
    snapshot: Option<&StorageRuntimeSettingsSnapshot>,
) -> Result<AppConfig, ApiError> {
    let config = effective(config, snapshot);
    validate(&config)?;
    Ok(config)
}

fn effective(
    mut config: AppConfig,
    snapshot: Option<&StorageRuntimeSettingsSnapshot>,
) -> AppConfig {
    config.runtime_managed_groups.clear();
    if let Some(snapshot) = snapshot {
        for record in snapshot.records() {
            if let Some(value) = record.assignment().value() {
                apply(&mut config, value);
            }
        }
    }
    config
}

pub(crate) fn apply(config: &mut AppConfig, value: &StorageRuntimeSettingsValue) {
    config.runtime_managed_groups.insert(value.group());
    match value {
        StorageRuntimeSettingsValue::Pagination(policy) => {
            let pages = crate::pagination::PageLimits::new(
                config.default_page_limit,
                config.max_page_limit,
            )
            .expect("validated deployment pagination")
            .with_runtime_policy(Some(*policy));
            config.default_page_limit = pages.default_limit();
            config.max_page_limit = pages.maximum_limit();
        }
        StorageRuntimeSettingsValue::Observability(policy) => {
            config.log_level = policy.log_level().clone();
            config.metrics_enabled = *policy.metrics_enabled();
            config.metrics_path = policy.metrics_path().clone();
        }
        StorageRuntimeSettingsValue::Network(policy) => {
            config.trust_ip_headers = *policy.trust_ip_headers();
            config.trusted_proxies = policy.trusted_proxies().clone();
            config.trusted_proxy_hops = *policy.trusted_proxy_hops();
            config.client_allowlist = policy.client_allowlist().clone();
        }
        StorageRuntimeSettingsValue::QueryLimits(policy) => {
            config.max_transitive_depth = *policy.max_transitive_depth();
        }
        StorageRuntimeSettingsValue::Tasks(policy) => {
            config.task_poll_interval_ms = *policy.poll_interval_ms();
            config.task_lease_seconds = *policy.lease_seconds();
            config.task_heartbeat_seconds = *policy.heartbeat_seconds();
            config.task_recovery_interval_seconds = *policy.recovery_interval_seconds();
            config.computed_reindex_batch_size = *policy.computed_reindex_batch_size();
            config.import_max_active_tasks_per_user = *policy.imports_per_user();
            config.export_max_active_tasks_per_user = *policy.exports_per_user();
            config.remote_call_max_active_tasks_per_user = *policy.remote_calls_per_user();
        }
        StorageRuntimeSettingsValue::RemoteCalls(policy) => {
            config.remote_call_timeout_ms = *policy.timeout_ms();
            config.remote_call_max_response_bytes = *policy.max_response_bytes();
            config.remote_call_allow_private_targets =
                config.remote_call_allow_private_targets && *policy.allow_private_targets();
        }
        StorageRuntimeSettingsValue::Tokens(policy) => {
            config.token_lifetime_hours =
                (*policy.lifetime_hours()).min(config.max_token_lifetime_hours);
            config.token_retention_days = *policy.retention_days();
            config.token_retention_purge_enabled = *policy.purge_enabled();
            config.token_retention_purge_interval_seconds = *policy.purge_interval_seconds();
            config.token_retention_purge_batch_size = *policy.purge_batch_size();
        }
        StorageRuntimeSettingsValue::LoginRateLimit(policy) => {
            config.login_rate_limit_enabled = *policy.enabled();
            config.login_rate_limit_max_attempts = *policy.attempts_per_identity();
            config.login_rate_limit_max_attempts_per_account = *policy.attempts_per_account();
            config.login_rate_limit_max_attempts_per_ip = *policy.attempts_per_ip();
            config.login_rate_limit_max_attempts_per_subnet = *policy.attempts_per_subnet();
            config.login_rate_limit_window_seconds = *policy.window_seconds();
            config.login_rate_limit_backoff_base_seconds = *policy.backoff_base_seconds();
            config.login_rate_limit_backoff_max_seconds = *policy.backoff_max_seconds();
        }
        StorageRuntimeSettingsValue::Events(policy) => {
            config.event_fanout_batch_size = *policy.fanout_batch_size();
            config.event_fanout_poll_interval_ms = *policy.fanout_poll_interval_ms();
            config.event_delivery_batch_size = *policy.delivery_batch_size();
            config.event_delivery_poll_interval_ms = *policy.delivery_poll_interval_ms();
            config.event_delivery_max_attempts = *policy.delivery_max_attempts();
            config.event_delivery_transport_timeout_ms = *policy.delivery_transport_timeout_ms();
            config.event_retention_days = *policy.retention_days();
            config.event_delivery_retention_days = *policy.delivery_retention_days();
            config.event_retention_purge_enabled = *policy.purge_enabled();
            config.event_retention_purge_interval_seconds = *policy.purge_interval_seconds();
            config.event_retention_purge_batch_size = *policy.purge_batch_size();
            config.event_retention_file_archive_enabled = *policy.file_archive_enabled();
        }
        StorageRuntimeSettingsValue::Exports(policy) => {
            config.export_output_retention_hours = *policy.output_retention_hours();
            config.export_output_cleanup_interval_seconds = *policy.cleanup_interval_seconds();
            config.export_max_output_bytes = *policy.max_output_bytes();
            config.export_stage_timeout_ms = *policy.stage_timeout_ms();
            config.export_db_statement_timeout_ms = *policy.storage_query_budget_ms();
            config.export_template_recursion_limit = *policy.template_recursion_limit();
            config.export_template_max_objects = *policy.template_max_objects();
            config.export_template_fuel = *policy.template_fuel();
        }
        StorageRuntimeSettingsValue::Backups(policy) => {
            config.backup_max_active_tasks_per_user = *policy.max_active_per_user();
            config.backup_max_output_bytes = *policy.max_output_bytes();
            config.backup_max_capture_rows = *policy.max_capture_rows();
            config.backup_output_retention_hours = *policy.retention_hours();
        }
        StorageRuntimeSettingsValue::Restores(policy) => {
            config.restore_max_upload_bytes = *policy.max_upload_bytes();
            config.restore_stage_retention_minutes = *policy.staging_retention_minutes();
        }
        _ => {}
    }
}

impl AppConfig {
    pub(crate) fn runtime_manages(&self, group: StorageRuntimeSettingsGroup) -> bool {
        self.runtime_managed_groups.contains(&group)
    }
}

pub(crate) fn validate(config: &AppConfig) -> Result<(), ApiError> {
    config.task_worker_settings()?;
    config.event_fanout_settings()?;
    config.event_delivery_settings()?;
    config.event_retention_settings()?;
    config.token_issuance_policy()?;
    config.token_retention_settings()?;
    config.backup_budget()?;
    if config.event_retention_file_archive_enabled
        && config
            .event_retention_archive_path
            .as_ref()
            .is_none_or(|path| path.trim().is_empty())
    {
        return Err(ApiError::BadRequest(
            "File archiving requires a deployment archive path".into(),
        ));
    }
    tracing_subscriber::EnvFilter::try_new(&config.log_level)
        .map_err(|error| ApiError::BadRequest(format!("Invalid log filter: {error}")))?;
    Ok(())
}

pub(crate) fn deployment_document(
    config: &AppConfig,
    group: StorageRuntimeSettingsGroup,
) -> Option<Value> {
    Some(match group {
        StorageRuntimeSettingsGroup::Observability => json!({
            "log_level": config.log_level,
            "metrics_enabled": config.metrics_enabled,
            "metrics_path": config.metrics_path,
        }),
        StorageRuntimeSettingsGroup::Network => json!({
            "trust_ip_headers": config.trust_ip_headers,
            "trusted_proxies": config.trusted_proxies,
            "trusted_proxy_hops": config.trusted_proxy_hops,
            "client_allowlist": config.client_allowlist,
        }),
        StorageRuntimeSettingsGroup::QueryLimits => json!({
            "max_transitive_depth": config.max_transitive_depth,
        }),
        StorageRuntimeSettingsGroup::Tasks => json!({
            "poll_interval_ms": config.task_poll_interval_ms,
            "lease_seconds": config.task_lease_seconds,
            "heartbeat_seconds": config.task_heartbeat_seconds,
            "recovery_interval_seconds": config.task_recovery_interval_seconds,
            "computed_reindex_batch_size": config.computed_reindex_batch_size,
            "imports_per_user": config.import_max_active_tasks_per_user,
            "exports_per_user": config.export_max_active_tasks_per_user,
            "remote_calls_per_user": config.remote_call_max_active_tasks_per_user,
        }),
        StorageRuntimeSettingsGroup::RemoteCalls => json!({
            "timeout_ms": config.remote_call_timeout_ms,
            "max_response_bytes": config.remote_call_max_response_bytes,
            "allow_private_targets": config.remote_call_allow_private_targets,
        }),
        StorageRuntimeSettingsGroup::Tokens => json!({
            "lifetime_hours": config.token_lifetime_hours,
            "retention_days": config.token_retention_days,
            "purge_enabled": config.token_retention_purge_enabled,
            "purge_interval_seconds": config.token_retention_purge_interval_seconds,
            "purge_batch_size": config.token_retention_purge_batch_size,
        }),
        StorageRuntimeSettingsGroup::LoginRateLimit => json!({
            "enabled": config.login_rate_limit_enabled,
            "attempts_per_identity": config.login_rate_limit_max_attempts,
            "attempts_per_account": config.login_rate_limit_max_attempts_per_account,
            "attempts_per_ip": config.login_rate_limit_max_attempts_per_ip,
            "attempts_per_subnet": config.login_rate_limit_max_attempts_per_subnet,
            "window_seconds": config.login_rate_limit_window_seconds,
            "backoff_base_seconds": config.login_rate_limit_backoff_base_seconds,
            "backoff_max_seconds": config.login_rate_limit_backoff_max_seconds,
        }),
        StorageRuntimeSettingsGroup::Events => json!({
            "fanout_batch_size": config.event_fanout_batch_size,
            "fanout_poll_interval_ms": config.event_fanout_poll_interval_ms,
            "delivery_batch_size": config.event_delivery_batch_size,
            "delivery_poll_interval_ms": config.event_delivery_poll_interval_ms,
            "delivery_max_attempts": config.event_delivery_max_attempts,
            "delivery_transport_timeout_ms": config.event_delivery_transport_timeout_ms,
            "retention_days": config.event_retention_days,
            "delivery_retention_days": config.event_delivery_retention_days,
            "purge_enabled": config.event_retention_purge_enabled,
            "purge_interval_seconds": config.event_retention_purge_interval_seconds,
            "purge_batch_size": config.event_retention_purge_batch_size,
            "file_archive_enabled": config.event_retention_file_archive_enabled,
        }),
        StorageRuntimeSettingsGroup::Exports => json!({
            "output_retention_hours": config.export_output_retention_hours,
            "cleanup_interval_seconds": config.export_output_cleanup_interval_seconds,
            "max_output_bytes": config.export_max_output_bytes,
            "stage_timeout_ms": config.export_stage_timeout_ms,
            "storage_query_budget_ms": config.export_db_statement_timeout_ms,
            "template_recursion_limit": config.export_template_recursion_limit,
            "template_max_objects": config.export_template_max_objects,
            "template_fuel": config.export_template_fuel,
        }),
        StorageRuntimeSettingsGroup::Backups => json!({
            "max_active_per_user": config.backup_max_active_tasks_per_user,
            "max_output_bytes": config.backup_max_output_bytes,
            "max_capture_rows": config.backup_max_capture_rows,
            "retention_hours": config.backup_output_retention_hours,
        }),
        StorageRuntimeSettingsGroup::Restores => json!({
            "max_upload_bytes": config.restore_max_upload_bytes,
            "staging_retention_minutes": config.restore_stage_retention_minutes,
        }),
        _ => return None,
    })
}

pub(crate) fn policy_schema(group: StorageRuntimeSettingsGroup) -> Option<Value> {
    use hubuum_storage_core::*;
    Some(match group {
        StorageRuntimeSettingsGroup::Observability => StorageObservabilityPolicy::schema(),
        StorageRuntimeSettingsGroup::Network => StorageNetworkPolicy::schema(),
        StorageRuntimeSettingsGroup::QueryLimits => StorageQueryLimitsPolicy::schema(),
        StorageRuntimeSettingsGroup::Tasks => StorageTasksPolicy::schema(),
        StorageRuntimeSettingsGroup::RemoteCalls => StorageRemoteCallsPolicy::schema(),
        StorageRuntimeSettingsGroup::Tokens => StorageTokensPolicy::schema(),
        StorageRuntimeSettingsGroup::LoginRateLimit => StorageLoginRateLimitPolicy::schema(),
        StorageRuntimeSettingsGroup::Events => StorageEventsPolicy::schema(),
        StorageRuntimeSettingsGroup::Exports => StorageExportsPolicy::schema(),
        StorageRuntimeSettingsGroup::Backups => StorageBackupsPolicy::schema(),
        StorageRuntimeSettingsGroup::Restores => StorageRestoresPolicy::schema(),
        _ => return None,
    })
}
