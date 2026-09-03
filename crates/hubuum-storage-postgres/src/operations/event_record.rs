use std::collections::HashMap;

#[cfg(feature = "integration-test-support")]
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{Insertable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_events_core::NewEvent;
#[cfg(feature = "integration-test-support")]
use hubuum_events_core::{Action, EntityType, EventEntityId};
use hubuum_storage_core::StorageRecordedEvent;
use uuid::Uuid;

#[cfg(feature = "integration-test-support")]
use crate::PostgresRuntime;
use crate::{PostgresConnection, PostgresStorageError};

use super::event_rows::StoredEventProjection;

#[derive(Insertable)]
#[diesel(table_name = crate::schema::events)]
struct NewEventRow<'event> {
    event_id: Uuid,
    entity_type: &'static str,
    entity_id: Option<i32>,
    entity_name: Option<&'event str>,
    collection_id: Option<i32>,
    action: &'static str,
    actor_user_id: Option<i32>,
    actor_kind: &'static str,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    request_id: Option<Uuid>,
    correlation_id: Option<&'event str>,
    summary: &'event str,
    before: Option<&'event serde_json::Value>,
    after: Option<&'event serde_json::Value>,
    metadata: &'event serde_json::Value,
    schema_version: i32,
    trace_id: Option<&'event str>,
    trace_span_id: Option<&'event str>,
    trace_flags: Option<i16>,
    trace_context_version: Option<i16>,
}

impl<'event> From<&'event NewEvent> for NewEventRow<'event> {
    fn from(event: &'event NewEvent) -> Self {
        Self {
            event_id: event.event_id().as_uuid(),
            entity_type: event.entity_type().as_str(),
            entity_id: event.entity_id().map(Into::into),
            entity_name: event.entity_name(),
            collection_id: event.collection_id().map(Into::into),
            action: event.action().as_str(),
            actor_user_id: event.actor_user_id().map(Into::into),
            actor_kind: event.actor_kind().as_str(),
            initiator_user_id: event.initiator_user_id().map(Into::into),
            task_id: event.task_id().map(Into::into),
            request_id: event.request_id(),
            correlation_id: event.correlation_id(),
            summary: event.summary(),
            before: event.before(),
            after: event.after(),
            metadata: event.metadata(),
            schema_version: event.schema_version(),
            trace_id: event.trace_link().map(|link| link.trace_id()),
            trace_span_id: event.trace_link().map(|link| link.span_id()),
            trace_flags: event.trace_link().map(|link| i16::from(link.trace_flags())),
            trace_context_version: event.trace_link().map(|link| i16::from(link.version())),
        }
    }
}

/// Append one validated event on the caller-owned PostgreSQL transaction.
pub async fn append_event(
    connection: &mut PostgresConnection,
    event: &NewEvent,
) -> Result<StorageRecordedEvent, PostgresStorageError> {
    let row = diesel::insert_into(crate::schema::events::table)
        .values(NewEventRow::from(event))
        .returning(StoredEventProjection::as_returning())
        .get_result::<StoredEventProjection>(connection)
        .await?;
    log_event_append(&row);
    row.into_audit_event(&HashMap::new(), false)
}

/// Append a bounded batch of validated events in one PostgreSQL statement.
pub async fn append_events(
    connection: &mut PostgresConnection,
    events: &[NewEvent],
) -> Result<Vec<StorageRecordedEvent>, PostgresStorageError> {
    if events.is_empty() {
        return Ok(Vec::new());
    }
    let rows = events.iter().map(NewEventRow::from).collect::<Vec<_>>();
    let persisted = diesel::insert_into(crate::schema::events::table)
        .values(rows)
        .returning(StoredEventProjection::as_returning())
        .get_results::<StoredEventProjection>(connection)
        .await?;
    for event in &persisted {
        log_event_append(event);
    }
    persisted
        .into_iter()
        .map(|event| event.into_audit_event(&HashMap::new(), false))
        .collect()
}

fn log_event_append(event: &StoredEventProjection) {
    tracing::debug!(
        backend = "postgresql",
        operation = "append_event",
        event_id = event.id,
        entity_type = event.entity_type,
        entity_id = event.entity_id,
        actor_user_id = event.actor_user_id,
        "PostgreSQL event append completed"
    );
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn list_events_for_test(
    runtime: &PostgresRuntime,
    entity_type_value: EntityType,
    entity_id_value: EventEntityId,
    action_value: Option<Action>,
) -> Result<Vec<StorageRecordedEvent>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::events::dsl::{action, entity_id, entity_type, events, id};

            let mut query = events
                .filter(entity_type.eq(entity_type_value.as_str()))
                .filter(entity_id.eq(entity_id_value.get()))
                .into_boxed();
            if let Some(action_value) = action_value {
                query = query.filter(action.eq(action_value.as_str()));
            }
            query
                .order(id.asc())
                .select(StoredEventProjection::as_select())
                .load::<StoredEventProjection>(connection)
                .await?
                .into_iter()
                .map(|event| event.into_audit_event(&HashMap::new(), false))
                .collect()
        })
        .await
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn count_events_for_test(
    runtime: &PostgresRuntime,
    entity_type_value: EntityType,
    entity_id_value: EventEntityId,
    action_value: Option<Action>,
) -> Result<i64, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::events::dsl::{action, entity_id, entity_type, events};

            let mut query = events
                .filter(entity_type.eq(entity_type_value.as_str()))
                .filter(entity_id.eq(entity_id_value.get()))
                .into_boxed();
            if let Some(action_value) = action_value {
                query = query.filter(action.eq(action_value.as_str()));
            }
            Ok::<_, PostgresStorageError>(query.count().get_result::<i64>(connection).await?)
        })
        .await
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn list_events_by_type_for_test(
    runtime: &PostgresRuntime,
    entity_type_value: EntityType,
) -> Result<Vec<StorageRecordedEvent>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::events::dsl::{entity_type, events, id};

            events
                .filter(entity_type.eq(entity_type_value.as_str()))
                .order(id.asc())
                .select(StoredEventProjection::as_select())
                .load::<StoredEventProjection>(connection)
                .await?
                .into_iter()
                .map(|event| event.into_audit_event(&HashMap::new(), false))
                .collect()
        })
        .await
}
