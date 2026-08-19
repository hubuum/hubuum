//! Explicit hooks needed by the grouped request-level integration tests.
//!
//! This module is available only with the non-default
//! `integration-test-support` feature. Production builds do not include these
//! process-global reset and capture facilities.

use hubuum_auth_core::AuthenticatedExternalUser;
use hubuum_domain::EventSinkId;
use std::sync::OnceLock;
use tracing::Dispatch;
use tracing_subscriber::layer::SubscriberExt;

use crate::auth::ConfiguredLdapScope;
use crate::errors::ApiError;
use crate::events::{
    Action, ActorKind, CollectionId, EntityType, EventEntityId, EventResponse, NewEvent,
};
use crate::models::user::User;
use crate::models::{
    CollectionID, EventDeliveryResponse, EventDeliveryStatus, NewEventSink, NewEventSubscription,
    RemoteCallResult, TaskKind, validate_sink_parts, validate_subscription_parts,
};
use crate::services::Services;
use crate::storage::{StorageEventSinkCreate, StorageEventSubscriptionCreate, StorageHandle};
use crate::traits::PrincipalIdAccessor;
use hubuum_storage_postgres::PostgresPool;

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

/// Build the validated PostgreSQL pool used by request-level integration tests.
#[must_use]
pub fn postgres_test_pool(database_url: &str, max_connections: u32) -> PostgresPool {
    let config = integration_test_config().expect("integration test configuration must be valid");
    postgres_test_pool_with_timeout(
        database_url,
        max_connections,
        config.db_statement_timeout_ms,
    )
}

/// Build a validated PostgreSQL test pool with an explicit statement timeout.
#[must_use]
pub fn postgres_test_pool_with_timeout(
    database_url: &str,
    max_connections: u32,
    statement_timeout_ms: u64,
) -> PostgresPool {
    let config = integration_test_config().expect("integration test configuration must be valid");
    let settings = hubuum_storage_postgres::PostgresPoolSettings::builder(database_url)
        .max_size(max_connections)
        .statement_timeout_ms(statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .expect("PostgreSQL test pool settings must be valid");
    hubuum_storage_postgres::build_postgres_pool(&settings)
        .expect("PostgreSQL test pool must be constructible")
}

/// Load the adapter-owned remote-call result projection for request-level tests.
pub async fn remote_call_result(
    pool: &PostgresPool,
    task_id: i32,
) -> Result<RemoteCallResult, ApiError> {
    let row = hubuum_storage_postgres::test_support::load_remote_call_result(
        pool,
        hubuum_domain::TaskId::new(task_id)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?,
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)?;
    Ok(RemoteCallResult {
        id: row.id(),
        task_id: row.task_id(),
        target_id: row.target_id(),
        subject_type: row.subject_type().to_string(),
        subject_id: row.subject_id(),
        method: row.method().to_string(),
        rendered_url: row.rendered_url().to_string(),
        response_status: row.response_status(),
        response_headers: row.response_headers().cloned(),
        response_body_preview: row.response_body_preview().map(ToString::to_string),
        duration_ms: row.duration_ms(),
        success: row.success(),
        error: row.error().map(ToString::to_string),
        created_at: row.created_at(),
    })
}

/// Count audit events through the PostgreSQL test adapter boundary.
pub async fn audit_event_count(
    pool: &PostgresPool,
    entity_type_value: crate::events::EntityType,
    action_value: crate::events::Action,
    entity_id_value: i32,
) -> Result<i64, ApiError> {
    hubuum_storage_postgres::test_support::count_events(
        pool,
        entity_type_value,
        EventEntityId::new(entity_id_value)?,
        Some(action_value),
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
}

/// Count all audit events for one typed entity through test support.
pub async fn audit_event_total(
    pool: &PostgresPool,
    entity_type: EntityType,
    entity_id: i32,
) -> Result<i64, ApiError> {
    hubuum_storage_postgres::test_support::count_events(
        pool,
        entity_type,
        EventEntityId::new(entity_id)?,
        None,
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
}

/// Load typed audit responses for one entity through test support.
pub async fn audit_events(
    pool: &PostgresPool,
    entity_type: EntityType,
    entity_id: i32,
    action: Option<Action>,
) -> Result<Vec<EventResponse>, ApiError> {
    hubuum_storage_postgres::test_support::list_events(
        pool,
        entity_type,
        EventEntityId::new(entity_id)?,
        action,
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
    .map(|events| events.into_iter().map(event_response).collect())
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
        hubuum_storage_postgres::test_support::append_event(self, event)
            .await
            .map_err(hubuum_storage_core::StorageError::from)
            .map_err(ApiError::from)
            .map(event_response)
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
    let event = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "delivery api test",
    )?
    .with_collection_id(CollectionId::new(collection_id)?)
    .with_entity_id(EventEntityId::new(collection_id)?)
    .with_entity_name(entity_name);
    let event = hubuum_storage_postgres::test_support::append_event(pool, &event)
        .await
        .map_err(hubuum_storage_core::StorageError::from)?;
    let event_sequence = event.clone().into_parts().0.id;
    hubuum_storage_postgres::test_support::fanout_event(pool, event_sequence)
        .await
        .map_err(hubuum_storage_core::StorageError::from)?;
    hubuum_storage_postgres::test_support::load_event_delivery_for_event(pool, event_sequence)
        .await
        .map(event_delivery_response)
        .map_err(hubuum_storage_core::StorageError::from)
        .map_err(ApiError::from)
}

/// Set a delivery status through adapter-owned test support.
pub async fn set_event_delivery_status(
    pool: &PostgresPool,
    delivery_id: i64,
    status: EventDeliveryStatus,
) -> Result<(), ApiError> {
    hubuum_storage_postgres::test_support::set_event_delivery_status(
        pool,
        hubuum_domain::EventDeliveryId::new(delivery_id)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        status,
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
}

/// Set a delivery claim token through adapter-owned test support.
pub async fn set_event_delivery_claim_token(
    pool: &PostgresPool,
    delivery_id: i64,
    claim_token: uuid::Uuid,
) -> Result<(), ApiError> {
    hubuum_storage_postgres::test_support::set_event_delivery_claim_token(
        pool,
        hubuum_domain::EventDeliveryId::new(delivery_id)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        claim_token,
    )
    .await
    .map_err(hubuum_storage_core::StorageError::from)
    .map_err(ApiError::from)
}

fn event_response(event: hubuum_storage_core::StorageRecordedEvent) -> EventResponse {
    let (event, before_revision, after_revision) = event.into_parts();
    EventResponse {
        id: event.id.get(),
        event_id: event.event_id,
        occurred_at: event.occurred_at,
        entity_type: event.entity_type,
        entity_id: event.entity_id.map(EventEntityId::get),
        entity_name: event.entity_name,
        collection_id: event.collection_id.map(CollectionId::id),
        action: event.action,
        actor_user_id: event.actor_user_id.map(hubuum_domain::PrincipalId::id),
        actor_kind: event.actor_kind,
        provenance: event.provenance,
        request_id: event.request_id,
        correlation_id: event.correlation_id,
        summary: event.summary,
        before: event.before,
        after: event.after,
        metadata: event.metadata,
        schema_version: event.schema_version,
        before_revision,
        after_revision,
    }
}

fn event_delivery_response(
    delivery: hubuum_storage_core::StorageEventDelivery,
) -> EventDeliveryResponse {
    EventDeliveryResponse {
        id: delivery.id().id(),
        event_id: delivery.event_id().get(),
        subscription_id: delivery.subscription_id().id(),
        status: delivery.status().to_string(),
        attempts: delivery.attempts(),
        next_attempt_at: delivery.next_attempt_at(),
        last_error: delivery.last_error().map(ToString::to_string),
        locked_until: delivery.locked_until(),
        created_at: delivery.created_at(),
        updated_at: delivery.updated_at(),
    }
}

/// Create one invariant-preserving PostgreSQL task fixture through adapter-owned test support.
pub async fn create_persisted_test_task(
    pool: &PostgresPool,
    request: hubuum_storage_postgres::test_support::TestTaskCreate,
) -> Result<crate::models::TaskRecord, ApiError> {
    crate::services::tasks::task_from_storage(
        hubuum_storage_postgres::test_support::create_task(pool, request)
            .await
            .map_err(hubuum_storage_core::StorageError::from)?,
    )
}

/// Claim one exact persisted task without exposing adapter claim representation.
pub async fn claim_persisted_test_task(
    pool: &PostgresPool,
    task_id: i32,
) -> Result<crate::services::tasks::ClaimedTask, ApiError> {
    crate::services::tasks::ClaimedTask::from_storage(
        hubuum_storage_postgres::test_support::claim_task_by_id(
            pool,
            hubuum_domain::TaskId::new(task_id)?,
        )
        .await
        .map_err(hubuum_storage_core::StorageError::from)?,
    )
}

/// Test-only active-token projection without exposing adapter rows or SQL.
#[async_trait::async_trait]
pub trait TestActiveTokens {
    async fn tokens(
        &self,
        pool: &PostgresPool,
    ) -> Result<Vec<crate::models::PrincipalToken>, ApiError>;
}

#[async_trait::async_trait]
impl<T> TestActiveTokens for T
where
    T: PrincipalIdAccessor + Sync,
{
    async fn tokens(
        &self,
        pool: &PostgresPool,
    ) -> Result<Vec<crate::models::PrincipalToken>, ApiError> {
        let observed_at = chrono::Utc::now().naive_utc();
        let legacy_valid_after =
            crate::models::configured_token_lifetime()?.cutoff_from(observed_at)?;
        hubuum_storage_postgres::test_support::load_active_tokens_for_principal(
            pool,
            hubuum_domain::PrincipalId::new(self.principal_id())?,
            observed_at,
            legacy_valid_after,
        )
        .await
        .map_err(hubuum_storage_core::StorageError::from)
        .map_err(ApiError::from)
        .map(|tokens| tokens.into_iter().map(principal_token_from_test).collect())
    }
}

fn principal_token_from_test(
    row: hubuum_storage_postgres::test_support::PersistedTestToken,
) -> crate::models::PrincipalToken {
    crate::models::PrincipalToken {
        id: row.id(),
        token: row.token_hash().to_string(),
        principal_id: row.principal_id(),
        name: row.name().map(ToString::to_string),
        description: row.description().map(ToString::to_string),
        issued: row.issued(),
        expires_at: row.expires_at(),
        last_used_at: row.last_used_at(),
        revoked_at: row.revoked_at(),
        permission_scoped: row.permission_scoped(),
        resource_scoped: row.resource_scoped(),
        revision: row.revision(),
    }
}

/// Build an adapter-owned task fixture request from application task enums.
pub fn persisted_test_task_request(
    kind: crate::models::TaskKind,
    status: crate::models::TaskStatus,
    submitted_by: i32,
) -> Result<hubuum_storage_postgres::test_support::TestTaskCreate, ApiError> {
    let kind = match kind {
        crate::models::TaskKind::Import => hubuum_storage_core::StorageTaskKind::Import,
        crate::models::TaskKind::Export => hubuum_storage_core::StorageTaskKind::Export,
        crate::models::TaskKind::Backup => hubuum_storage_core::StorageTaskKind::Backup,
        crate::models::TaskKind::Reindex => hubuum_storage_core::StorageTaskKind::Reindex,
        crate::models::TaskKind::RemoteCall => hubuum_storage_core::StorageTaskKind::RemoteCall,
    };
    let status = match status {
        crate::models::TaskStatus::Queued => hubuum_storage_core::StorageTaskStatus::Queued,
        crate::models::TaskStatus::Validating => hubuum_storage_core::StorageTaskStatus::Validating,
        crate::models::TaskStatus::Running => hubuum_storage_core::StorageTaskStatus::Running,
        crate::models::TaskStatus::Succeeded => hubuum_storage_core::StorageTaskStatus::Succeeded,
        crate::models::TaskStatus::Failed => hubuum_storage_core::StorageTaskStatus::Failed,
        crate::models::TaskStatus::PartiallySucceeded => {
            hubuum_storage_core::StorageTaskStatus::PartiallySucceeded
        }
        crate::models::TaskStatus::Cancelled => hubuum_storage_core::StorageTaskStatus::Cancelled,
    };
    Ok(hubuum_storage_postgres::test_support::TestTaskCreate::new(
        kind,
        status,
        hubuum_domain::PrincipalId::new(submitted_by)?,
    ))
}

/// Build a validated internal reindex task fixture.
pub fn persisted_internal_reindex_task_request(
    status: crate::models::TaskStatus,
) -> hubuum_storage_postgres::test_support::TestTaskCreate {
    let status = match status {
        crate::models::TaskStatus::Queued => hubuum_storage_core::StorageTaskStatus::Queued,
        crate::models::TaskStatus::Validating => hubuum_storage_core::StorageTaskStatus::Validating,
        crate::models::TaskStatus::Running => hubuum_storage_core::StorageTaskStatus::Running,
        crate::models::TaskStatus::Succeeded => hubuum_storage_core::StorageTaskStatus::Succeeded,
        crate::models::TaskStatus::Failed => hubuum_storage_core::StorageTaskStatus::Failed,
        crate::models::TaskStatus::PartiallySucceeded => {
            hubuum_storage_core::StorageTaskStatus::PartiallySucceeded
        }
        crate::models::TaskStatus::Cancelled => hubuum_storage_core::StorageTaskStatus::Cancelled,
    };
    hubuum_storage_postgres::test_support::TestTaskCreate::internal_reindex(status)
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
        &hubuum_storage_postgres::PostgresRuntime::unobserved(pool.clone()),
        request,
    )
    .await
    .map(|outcome| outcome.into_value().id().id())
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
        CollectionId::new(collection_id.id()).expect("persisted collection id must be positive"),
        EventSinkId::new(subscription.sink_id.id())
            .expect("persisted event-sink id must be positive"),
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
        &hubuum_storage_postgres::PostgresRuntime::unobserved(pool.clone()),
        request,
    )
    .await
    .map(|outcome| outcome.into_value().id().id())
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
