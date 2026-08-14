use std::fmt;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::events::{Event, NewEvent};
use crate::models::search::{FilterField, SortParam};
use crate::models::{REDACTED_DEBUG_VALUE, ResourceRevision, redacted_debug_option};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::storage::postgres::prelude::*;

/// PostgreSQL representation of one stored event.
///
/// Dispatch coordination fields never cross the adapter boundary. The row is
/// serializable only because the retention adapter preserves the established
/// JSONL archive format before deleting an event.
#[derive(Clone, Serialize, Deserialize, Queryable, Selectable)]
#[diesel(table_name = crate::schema::events)]
pub(crate) struct EventRow {
    pub(crate) id: i64,
    pub(crate) event_id: Uuid,
    pub(crate) occurred_at: NaiveDateTime,
    pub(crate) entity_type: String,
    pub(crate) entity_id: Option<i32>,
    pub(crate) entity_name: Option<String>,
    pub(crate) collection_id: Option<i32>,
    pub(crate) action: String,
    pub(crate) actor_user_id: Option<i32>,
    pub(crate) actor_kind: String,
    pub(crate) request_id: Option<Uuid>,
    pub(crate) correlation_id: Option<String>,
    pub(crate) summary: String,
    pub(crate) before: Option<serde_json::Value>,
    pub(crate) after: Option<serde_json::Value>,
    pub(crate) metadata: serde_json::Value,
    pub(crate) schema_version: i32,
    pub(crate) dispatched_at: Option<NaiveDateTime>,
    pub(crate) fanout_locked_until: Option<NaiveDateTime>,
    pub(crate) fanout_claim_token: Option<Uuid>,
    pub(crate) initiator_user_id: Option<i32>,
    pub(crate) task_id: Option<i32>,
    pub(crate) before_revision: Option<PostgresRevision>,
    pub(crate) after_revision: Option<PostgresRevision>,
}

impl fmt::Debug for EventRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventRow")
            .field("id", &self.id)
            .field("event_id", &self.event_id)
            .field("occurred_at", &self.occurred_at)
            .field("entity_type", &self.entity_type)
            .field("entity_id", &self.entity_id)
            .field("entity_name", &self.entity_name)
            .field("collection_id", &self.collection_id)
            .field("action", &self.action)
            .field("actor_user_id", &self.actor_user_id)
            .field("actor_kind", &self.actor_kind)
            .field("request_id", &self.request_id)
            .field("correlation_id", &self.correlation_id)
            .field("summary", &self.summary)
            .field("before", &redacted_debug_option(&self.before))
            .field("after", &redacted_debug_option(&self.after))
            .field("metadata", &REDACTED_DEBUG_VALUE)
            .field("schema_version", &self.schema_version)
            .field("dispatched_at", &self.dispatched_at)
            .field("fanout_locked_until", &self.fanout_locked_until)
            .field(
                "fanout_claim_token",
                &redacted_debug_option(&self.fanout_claim_token),
            )
            .field("initiator_user_id", &self.initiator_user_id)
            .field("task_id", &self.task_id)
            .field("before_revision", &self.before_revision)
            .field("after_revision", &self.after_revision)
            .finish()
    }
}

impl From<EventRow> for Event {
    fn from(row: EventRow) -> Self {
        Self {
            id: row.id,
            event_id: row.event_id,
            occurred_at: row.occurred_at,
            entity_type: row.entity_type,
            entity_id: row.entity_id,
            entity_name: row.entity_name,
            collection_id: row.collection_id,
            action: row.action,
            actor_user_id: row.actor_user_id,
            actor_kind: row.actor_kind,
            request_id: row.request_id,
            correlation_id: row.correlation_id,
            summary: row.summary,
            before: row.before,
            after: row.after,
            metadata: row.metadata,
            schema_version: row.schema_version,
            initiator_user_id: row.initiator_user_id,
            task_id: row.task_id,
            before_revision: row.before_revision.map(PostgresRevision::into_domain),
            after_revision: row.after_revision.map(PostgresRevision::into_domain),
        }
    }
}

impl CursorPaginated for EventRow {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id | FilterField::OccurredAt)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, crate::errors::ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            FilterField::OccurredAt => Ok(CursorValue::DateTime(self.occurred_at)),
            _ => Err(crate::errors::ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for events",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::OccurredAt,
            descending: true,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: true,
        }]
    }
}

impl CursorSqlMapping for EventRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, crate::errors::ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "events.id",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            FilterField::OccurredAt => CursorSqlField {
                column: "events.occurred_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(crate::errors::ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for events",
                    field
                )));
            }
        })
    }
}

/// Append one event on the caller-owned PostgreSQL transaction connection.
pub(crate) async fn emit_event(
    conn: &mut crate::storage::postgres::PostgresConnection,
    event: &NewEvent,
) -> Result<Event, ApiError> {
    let event = event_from_storage(
        hubuum_storage_postgres::operations::event_record::append_event(conn, event)
            .await
            .map_err(hubuum_storage_core::StorageError::from)?,
    )?;
    log_event_mutation(&event);
    Ok(event)
}

/// Append a bounded batch of events in one PostgreSQL statement.
pub(crate) async fn emit_events(
    conn: &mut crate::storage::postgres::PostgresConnection,
    events: &[NewEvent],
) -> Result<Vec<Event>, ApiError> {
    let persisted = hubuum_storage_postgres::operations::event_record::append_events(conn, events)
        .await
        .map_err(hubuum_storage_core::StorageError::from)?
        .into_iter()
        .map(event_from_storage)
        .collect::<Result<Vec<_>, _>>()?;
    for event in &persisted {
        log_event_mutation(event);
    }
    Ok(persisted)
}

fn event_from_storage(event: hubuum_storage_core::StorageRecordedEvent) -> Result<Event, ApiError> {
    let (event, before_revision, after_revision) = event.into_parts();
    Ok(Event {
        id: event.id,
        event_id: event.event_id,
        occurred_at: event.occurred_at,
        entity_type: event.entity_type,
        entity_id: event.entity_id,
        entity_name: event.entity_name,
        collection_id: event.collection_id,
        action: event.action,
        actor_user_id: event.actor_user_id,
        actor_kind: event.actor_kind,
        request_id: event.request_id,
        correlation_id: event.correlation_id,
        summary: event.summary,
        before: event.before,
        after: event.after,
        metadata: event.metadata,
        schema_version: event.schema_version,
        initiator_user_id: event
            .provenance
            .initiator
            .map(|principal| principal.principal_id),
        task_id: event.provenance.task_id,
        before_revision: before_revision.map(ResourceRevision::new).transpose()?,
        after_revision: after_revision.map(ResourceRevision::new).transpose()?,
    })
}

fn log_event_mutation(event: &Event) {
    if let (Ok(entity_type), Ok(action)) = (event.entity_type(), event.action()) {
        crate::logger::log_operation_mutation(
            entity_type,
            action,
            event.entity_id,
            event.actor_user_id,
            event.request_id,
            event.correlation_id.as_deref(),
        );
    }
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn list_events_for_test(
    pool: &crate::storage::postgres::PostgresPool,
    entity_type_value: crate::events::EntityType,
    entity_id_value: i32,
    action_value: Option<crate::events::Action>,
) -> Result<Vec<Event>, crate::errors::ApiError> {
    use crate::schema::events::dsl::{action, entity_id, entity_type, events, id};
    use crate::storage::postgres::with_connection;

    with_connection(pool, async |conn| {
        let mut query = events
            .filter(entity_type.eq(entity_type_value.as_str()))
            .filter(entity_id.eq(entity_id_value))
            .into_boxed();
        if let Some(action_value) = action_value {
            query = query.filter(action.eq(action_value.as_str()));
        }
        query.order(id.asc()).load::<EventRow>(conn).await
    })
    .await
    .map(|rows| rows.into_iter().map(Event::from).collect())
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn count_events_for_test(
    pool: &crate::storage::postgres::PostgresPool,
    entity_type_value: crate::events::EntityType,
    entity_id_value: i32,
    action_value: Option<crate::events::Action>,
) -> Result<i64, crate::errors::ApiError> {
    use crate::schema::events::dsl::{action, entity_id, entity_type, events};
    use crate::storage::postgres::with_connection;

    with_connection(pool, async |conn| {
        let mut query = events
            .filter(entity_type.eq(entity_type_value.as_str()))
            .filter(entity_id.eq(entity_id_value))
            .into_boxed();
        if let Some(action_value) = action_value {
            query = query.filter(action.eq(action_value.as_str()));
        }
        query.count().get_result::<i64>(conn).await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Action, ActorKind, EntityType};

    #[test]
    fn event_row_debug_redacts_payloads_and_fanout_claim_token() {
        let claim_token = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let row = EventRow {
            id: 1,
            event_id: Uuid::new_v4(),
            occurred_at: timestamp,
            entity_type: EntityType::Collection.as_str().to_string(),
            entity_id: Some(2),
            entity_name: Some("collection".to_string()),
            collection_id: Some(2),
            action: Action::Created.as_str().to_string(),
            actor_user_id: Some(3),
            actor_kind: ActorKind::User.as_str().to_string(),
            request_id: None,
            correlation_id: None,
            summary: "created".to_string(),
            before: Some(serde_json::json!({"token": "stored-before-secret"})),
            after: Some(serde_json::json!({"token": "stored-after-secret"})),
            metadata: serde_json::json!({"token": "stored-metadata-secret"}),
            schema_version: 1,
            dispatched_at: None,
            fanout_locked_until: Some(timestamp),
            fanout_claim_token: Some(claim_token),
            initiator_user_id: Some(3),
            task_id: None,
            before_revision: None,
            after_revision: None,
        };

        let debug = format!("{row:?}");

        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!debug.contains("stored-before-secret"));
        assert!(!debug.contains("stored-after-secret"));
        assert!(!debug.contains("stored-metadata-secret"));
        assert!(!debug.contains(&claim_token.to_string()));
    }
}
