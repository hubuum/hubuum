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
use crate::events::{Action, ActorKind, EntityType, EventResponse, NewEvent};
use crate::models::user::User;
use crate::models::{
    CollectionID, EventDeliveryResponse, EventDeliveryStatus, NewEventSink, NewEventSubscription,
    RemoteCallResult, TaskKind, validate_sink_parts, validate_subscription_parts,
};
use crate::services::Services;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::event_delivery::{
    load_event_delivery_for_event, set_event_delivery_claim_token_for_test,
    set_event_delivery_status_for_test,
};
use crate::storage::postgres::operations::event_fanout::fanout_event;
use crate::storage::postgres::operations::event_record::{
    count_events_for_test, emit_event, list_events_for_test,
};
use crate::storage::postgres::operations::remote_target::load_remote_call_result_for_task;
use crate::storage::{StorageEventSinkCreate, StorageEventSubscriptionCreate, StorageHandle};

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

/// Build resource-family services around the PostgreSQL adapter for integration tests.
pub fn services_for_postgres(pool: PostgresPool) -> Services {
    Services::from_storage(StorageHandle::postgres(pool))
}

/// Load the adapter-owned remote-call result projection for request-level tests.
pub async fn remote_call_result(
    pool: &PostgresPool,
    task_id: i32,
) -> Result<RemoteCallResult, ApiError> {
    load_remote_call_result_for_task(pool, task_id).await
}

/// Count audit events through the PostgreSQL test adapter boundary.
pub async fn audit_event_count(
    pool: &PostgresPool,
    entity_type_value: crate::events::EntityType,
    action_value: crate::events::Action,
    entity_id_value: i32,
) -> Result<i64, ApiError> {
    count_events_for_test(pool, entity_type_value, entity_id_value, Some(action_value)).await
}

/// Count all audit events for one typed entity through test support.
pub async fn audit_event_total(
    pool: &PostgresPool,
    entity_type: EntityType,
    entity_id: i32,
) -> Result<i64, ApiError> {
    count_events_for_test(pool, entity_type, entity_id, None).await
}

/// Load typed audit responses for one entity through test support.
pub async fn audit_events(
    pool: &PostgresPool,
    entity_type: EntityType,
    entity_id: i32,
    action: Option<Action>,
) -> Result<Vec<EventResponse>, ApiError> {
    list_events_for_test(pool, entity_type, entity_id, action)
        .await
        .map(|events| events.into_iter().map(EventResponse::from).collect())
}

/// Test-fixture capability for appending one validated audit event.
///
/// Request-level tests depend on this narrow operation instead of naming the
/// PostgreSQL pool used by the current integration harness.
pub trait AuditEventFixture: Send + Sync {
    async fn create_audit_event(&self, event: &NewEvent) -> Result<EventResponse, ApiError>;
}

impl AuditEventFixture for PostgresPool {
    async fn create_audit_event(&self, event: &NewEvent) -> Result<EventResponse, ApiError> {
        use crate::storage::postgres::with_connection;

        with_connection(self, async |conn| emit_event(conn, event).await)
            .await
            .map(EventResponse::from)
    }
}

pub async fn create_audit_event(
    fixture: &impl AuditEventFixture,
    event: &NewEvent,
) -> Result<EventResponse, ApiError> {
    fixture.create_audit_event(event).await
}

/// Emit, fan out, and load one collection event delivery for request tests.
pub async fn create_collection_event_delivery(
    pool: &PostgresPool,
    collection_id: i32,
    entity_name: &str,
) -> Result<EventDeliveryResponse, ApiError> {
    use crate::storage::postgres::with_connection;

    let event = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "delivery api test",
    )?
    .with_collection_id(collection_id)
    .with_entity_id(collection_id)
    .with_entity_name(entity_name);
    let event = with_connection(pool, async |conn| emit_event(conn, &event).await).await?;
    fanout_event(pool, event.id).await?;
    load_event_delivery_for_event(pool, event.id).await
}

/// Set a delivery status through adapter-owned test support.
pub async fn set_event_delivery_status(
    pool: &PostgresPool,
    delivery_id: i64,
    status: EventDeliveryStatus,
) -> Result<(), ApiError> {
    set_event_delivery_status_for_test(pool, delivery_id, status).await
}

/// Set a delivery claim token through adapter-owned test support.
pub async fn set_event_delivery_claim_token(
    pool: &PostgresPool,
    delivery_id: i64,
    claim_token: uuid::Uuid,
) -> Result<(), ApiError> {
    set_event_delivery_claim_token_for_test(pool, delivery_id, claim_token).await
}

pub async fn save_event_sink(pool: &PostgresPool, sink: NewEventSink) -> Result<i32, ApiError> {
    validate_sink_parts(sink.kind, &sink.config, sink.secret_ref.as_deref())?;
    let request = StorageEventSinkCreate::builder(
        sink.name,
        sink.kind.as_str(),
        hubuum_events_core::EventContext::system(),
    )
    .configuration(sink.config)
    .secret_ref(
        sink.secret_ref
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    )
    .enabled(sink.enabled)
    .build();
    hubuum_storage_postgres::operations::event_subscription::create_event_sink(
        &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
        request,
    )
    .await
    .map(|sink| sink.id())
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
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
    let request = StorageEventSubscriptionCreate::builder(
        collection_id.id(),
        subscription.sink_id.id(),
        subscription.name,
        hubuum_events_core::EventContext::system(),
    )
    .description(subscription.description)
    .entity_types(subscription.entity_types)
    .actions(subscription.actions)
    .filter(subscription.filter)
    .routing(subscription.routing)
    .enabled(subscription.enabled)
    .build();
    hubuum_storage_postgres::operations::event_subscription::create_event_subscription(
        &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
        request,
    )
    .await
    .map(|subscription| subscription.id())
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
}

pub async fn sync_external_user(
    pool: &PostgresPool,
    configured: &ConfiguredLdapScope,
    authenticated: AuthenticatedExternalUser,
) -> Result<User, ApiError> {
    crate::auth::sync_external_user(pool, configured, authenticated).await
}
