use std::collections::HashMap;

use diesel::{Insertable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_events_core::NewEvent;
use hubuum_storage_core::StorageRecordedEvent;
use uuid::Uuid;

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
}

impl<'event> From<&'event NewEvent> for NewEventRow<'event> {
    fn from(event: &'event NewEvent) -> Self {
        Self {
            event_id: event.event_id().as_uuid(),
            entity_type: event.entity_type().as_str(),
            entity_id: event.entity_id(),
            entity_name: event.entity_name(),
            collection_id: event.collection_id(),
            action: event.action().as_str(),
            actor_user_id: event.actor_user_id(),
            actor_kind: event.actor_kind().as_str(),
            initiator_user_id: event.initiator_user_id(),
            task_id: event.task_id(),
            request_id: event.request_id(),
            correlation_id: event.correlation_id(),
            summary: event.summary(),
            before: event.before(),
            after: event.after(),
            metadata: event.metadata(),
            schema_version: event.schema_version(),
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
    Ok(row.into_audit_event(&HashMap::new(), false))
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
    Ok(persisted
        .into_iter()
        .map(|event| event.into_audit_event(&HashMap::new(), false))
        .collect())
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
