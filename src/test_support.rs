//! Explicit hooks needed by the grouped request-level integration tests.
//!
//! This module is available only with the non-default
//! `integration-test-support` feature. Production builds do not include these
//! process-global reset and capture facilities.

use hubuum_auth_core::AuthenticatedExternalUser;

use crate::auth::ConfiguredLdapScope;
use crate::db::DbPool;
use crate::db::traits::event_subscription::{SaveEventSinkRecord, SaveEventSubscriptionRecord};
use crate::errors::ApiError;
use crate::models::user::User;
use crate::models::{CollectionID, NewEventSink, NewEventSubscription, TaskKind};

pub use crate::logger::test_support::JsonLogWriter;
pub use crate::middlewares::rate_limit::LOGIN_RATE_LIMIT_TEST_LOCK;

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

pub async fn save_event_sink(pool: &DbPool, sink: NewEventSink) -> Result<i32, ApiError> {
    let sink = sink
        .into_row()?
        .save_event_sink_record_without_events(pool)
        .await?;
    Ok(sink.id)
}

pub async fn save_event_subscription(
    pool: &DbPool,
    subscription: NewEventSubscription,
    collection_id: CollectionID,
) -> Result<i32, ApiError> {
    let subscription = subscription
        .into_row(collection_id)?
        .save_event_subscription_record_without_events(pool)
        .await?;
    Ok(subscription.id)
}

pub async fn sync_external_user(
    pool: &DbPool,
    configured: &ConfiguredLdapScope,
    authenticated: AuthenticatedExternalUser,
) -> Result<User, ApiError> {
    crate::auth::sync_external_user(pool, configured, authenticated).await
}
