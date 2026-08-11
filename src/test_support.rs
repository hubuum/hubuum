//! Explicit hooks needed by the grouped request-level integration tests.
//!
//! This module is available only with the non-default
//! `integration-test-support` feature. Production builds do not include these
//! process-global reset and capture facilities.

use hubuum_auth_core::AuthenticatedExternalUser;
use std::sync::OnceLock;
use tracing::Dispatch;
use tracing_subscriber::layer::SubscriberExt;

use crate::auth::ConfiguredLdapScope;
use crate::errors::ApiError;
use crate::models::user::User;
use crate::models::{
    CollectionID, NewEventSink, NewEventSinkRow, NewEventSubscription, NewEventSubscriptionRow,
    TaskKind, validate_sink_parts, validate_subscription_parts,
};
use crate::services::Services;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::event_subscription::{
    SaveEventSinkRecord, SaveEventSubscriptionRecord,
};
use crate::storage::{DynLifecycleStorage, PostgresStorage};

pub use crate::logger::test_support::JsonLogWriter;
pub use crate::middlewares::rate_limit::LOGIN_RATE_LIMIT_TEST_LOCK;

static TEST_TRACING_BASELINE: OnceLock<()> = OnceLock::new();

fn ensure_test_tracing_baseline() {
    TEST_TRACING_BASELINE.get_or_init(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

pub fn tracing_middleware_with_log_capture()
-> (crate::middlewares::TracingMiddleware, JsonLogWriter) {
    ensure_test_tracing_baseline();
    let writer = JsonLogWriter::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(writer.clone())
            .event_format(crate::logger::HubuumLoggingFormat),
    );
    let dispatch = Dispatch::new(subscriber);
    let middleware =
        crate::middlewares::TracingMiddleware::new_with_capture_dispatch(dispatch.clone());
    (middleware, writer)
}

#[cfg(not(test))]
pub fn integration_test_config() -> Result<&'static crate::config::AppConfig, ApiError> {
    crate::config::initialize_integration_test_config()
}

#[cfg(test)]
pub fn integration_test_config() -> Result<crate::config::AppConfig, ApiError> {
    crate::config::get_config()
}

pub struct LocalRemoteTargetGuard;

impl Drop for LocalRemoteTargetGuard {
    fn drop(&mut self) {
        crate::tasks::exit_local_remote_target_test();
    }
}

pub fn allow_local_remote_target() -> LocalRemoteTargetGuard {
    crate::tasks::enter_local_remote_target_test();
    LocalRemoteTargetGuard
}

pub async fn record_login_failure(
    identity_scope: &str,
    username: &str,
    client_ip: Option<std::net::IpAddr>,
) {
    crate::middlewares::rate_limit::record_login_failure(identity_scope, username, client_ip).await;
}

pub async fn reset_login_rate_limit() {
    crate::middlewares::rate_limit::reset_login_rate_limit_for_tests().await;
}

pub fn clear_metrics_scrape_cache() {
    crate::observability::metrics::clear_scrape_cache_for_tests();
}

pub fn record_principal_on_current_span(principal_id: i32) {
    crate::middlewares::tracing::record_principal_on_current_span(principal_id);
}

pub fn executable_task_kind_values() -> [&'static str; 4] {
    [
        TaskKind::Import.as_str(),
        TaskKind::Export.as_str(),
        TaskKind::Backup.as_str(),
        TaskKind::RemoteCall.as_str(),
    ]
}

/// Build lifecycle services around the PostgreSQL adapter for integration tests.
pub fn services_for_postgres(pool: PostgresPool) -> Services {
    Services::from_lifecycle_storage(DynLifecycleStorage::from_backend(PostgresStorage::new(
        pool,
    )))
}

pub async fn save_event_sink(pool: &PostgresPool, sink: NewEventSink) -> Result<i32, ApiError> {
    validate_sink_parts(sink.kind, &sink.config, sink.secret_ref.as_deref())?;
    let sink = NewEventSinkRow {
        name: sink.name,
        kind: sink.kind.as_str().to_string(),
        config: sink.config,
        secret_ref: sink
            .secret_ref
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        enabled: sink.enabled,
    }
    .save_event_sink_record_without_events(pool)
    .await?;
    Ok(sink.id)
}

pub async fn save_event_subscription(
    pool: &PostgresPool,
    subscription: NewEventSubscription,
    collection_id: CollectionID,
) -> Result<i32, ApiError> {
    validate_subscription_parts(
        &subscription.entity_types,
        &subscription.actions,
        &subscription.filter,
        &subscription.routing,
    )?;
    let subscription = NewEventSubscriptionRow {
        collection_id: collection_id.id(),
        sink_id: subscription.sink_id.id(),
        name: subscription.name,
        description: subscription.description,
        entity_types: serde_json::to_value(subscription.entity_types)?,
        actions: serde_json::to_value(subscription.actions)?,
        filter: serde_json::to_value(subscription.filter)?,
        routing: subscription.routing,
        enabled: subscription.enabled,
    }
    .save_event_subscription_record_without_events(pool)
    .await?;
    Ok(subscription.id)
}

pub async fn sync_external_user(
    pool: &PostgresPool,
    configured: &ConfiguredLdapScope,
    authenticated: AuthenticatedExternalUser,
) -> Result<User, ApiError> {
    crate::auth::sync_external_user(pool, configured, authenticated).await
}
