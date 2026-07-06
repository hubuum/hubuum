//! `Event` / `NewEvent` models for the unified `events` stream (#71).
//!
//! `NewEvent` is a validating builder: the `(entity_type, action)` pair is
//! checked against the authoritative catalog at construction, so invalid
//! ccombinations (e.g. `object_relation.updated`) can never reach
//! [`super::emit_event`]. The struct holds validated `String` snapshots of the
//! catalog enums at the Diesel boundary while exposing typed builders; the
//! [`Event`] read model converts back to the typed enums on demand.

use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::events;

use super::{Action, ActorKind, EntityType, EventCatalogError, EventContext, is_valid_pair};

/// Typed wrapper for the canonical, client-dedupable event identity
/// (`events.event_id`). Flows to sinks as the idempotency key (#78) and to the
/// audit API (#74).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Uuid> for EventId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<EventId> for Uuid {
    fn from(event_id: EventId) -> Uuid {
        event_id.0
    }
}

/// A committed event row — the read model for the audit log (#74) and delivery.
#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Selectable)]
#[diesel(table_name = events)]
pub struct Event {
    pub id: i64,
    pub event_id: Uuid,
    pub occurred_at: NaiveDateTime,
    pub entity_type: String,
    pub entity_id: Option<i32>,
    pub entity_name: Option<String>,
    pub collection_id: Option<i32>,
    pub action: String,
    pub actor_user_id: Option<i32>,
    pub actor_kind: String,
    pub request_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub summary: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub schema_version: i32,
    pub dispatched_at: Option<NaiveDateTime>,
    pub fanout_locked_until: Option<NaiveDateTime>,
    pub fanout_claim_token: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventResponse {
    pub id: i64,
    pub event_id: Uuid,
    pub occurred_at: NaiveDateTime,
    pub entity_type: String,
    pub entity_id: Option<i32>,
    pub entity_name: Option<String>,
    pub collection_id: Option<i32>,
    pub action: String,
    pub actor_user_id: Option<i32>,
    pub actor_kind: String,
    pub request_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub summary: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub schema_version: i32,
}

impl Event {
    /// Parse the stored `entity_type` text back into the typed catalog enum.
    pub fn entity_type(&self) -> Result<EntityType, EventCatalogError> {
        EntityType::from_db(&self.entity_type)
    }

    /// Parse the stored `action` text back into the typed catalog enum.
    pub fn action(&self) -> Result<Action, EventCatalogError> {
        Action::from_db(&self.action)
    }

    /// Parse the stored `actor_kind` text back into the typed enum.
    pub fn actor_kind(&self) -> Result<ActorKind, EventCatalogError> {
        ActorKind::from_db(&self.actor_kind)
    }
}

impl From<Event> for EventResponse {
    fn from(value: Event) -> Self {
        Self {
            id: value.id,
            event_id: value.event_id,
            occurred_at: value.occurred_at,
            entity_type: value.entity_type,
            entity_id: value.entity_id,
            entity_name: value.entity_name,
            collection_id: value.collection_id,
            action: value.action,
            actor_user_id: value.actor_user_id,
            actor_kind: value.actor_kind,
            request_id: value.request_id,
            correlation_id: value.correlation_id,
            summary: value.summary,
            before: value.before,
            after: value.after,
            metadata: value.metadata,
            schema_version: value.schema_version,
        }
    }
}

impl EventResponse {
    pub fn redact_indirect_audit_payloads(mut self) -> Self {
        self.before = None;
        self.after = None;
        self
    }
}

impl CursorPaginated for EventResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id | FilterField::OccurredAt)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            FilterField::OccurredAt => Ok(CursorValue::DateTime(self.occurred_at)),
            _ => Err(ApiError::BadRequest(format!(
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

impl CursorSqlMapping for EventResponse {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "events.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::OccurredAt => CursorSqlField {
                column: "events.occurred_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for events",
                    field
                )));
            }
        })
    }
}

/// A validated, not-yet-persisted event. Built by mutation code inside a
/// `with_transaction` block and appended by [`super::emit_event`].
///
/// Required identity is provided to [`NewEvent::new`], which validates the
/// `(entity_type, action)` pair against the catalog; optional provenance and
/// snapshot fields are added with the `with_*` builders. Columns owned by the
/// database (`id`, `occurred_at`, `dispatched_at`, fan-out claim fields) are
/// intentionally absent so the row uses their defaults on insert.
#[derive(Debug, Insertable)]
#[diesel(table_name = events)]
pub struct NewEvent {
    event_id: Uuid,
    entity_type: String,
    entity_id: Option<i32>,
    entity_name: Option<String>,
    collection_id: Option<i32>,
    action: String,
    actor_user_id: Option<i32>,
    actor_kind: String,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: serde_json::Value,
    schema_version: i32,
}

impl NewEvent {
    /// Create a validated event. The `(entity_type, action)` pair is checked
    /// against the authoritative catalog; an invalid pair (e.g.
    /// `object_relation.updated`) is rejected at the boundary, before any
    /// database work. `event_id` defaults to a fresh UUID; `metadata` defaults
    /// to an empty object; `schema_version` defaults to `1`.
    pub fn new(
        entity_type: EntityType,
        action: Action,
        actor_kind: ActorKind,
        summary: impl Into<String>,
    ) -> Result<Self, ApiError> {
        if !is_valid_pair(entity_type, action) {
            return Err(ApiError::ValidationError(format!(
                "action '{}' is not valid for entity_type '{}'",
                action.as_str(),
                entity_type.as_str()
            )));
        }

        Ok(Self {
            event_id: EventId::new().into(),
            entity_type: entity_type.as_str().to_string(),
            entity_id: None,
            entity_name: None,
            collection_id: None,
            action: action.as_str().to_string(),
            actor_user_id: None,
            actor_kind: actor_kind.as_str().to_string(),
            request_id: None,
            correlation_id: None,
            summary: summary.into(),
            before: None,
            after: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            schema_version: 1,
        })
    }

    pub fn with_entity_id(mut self, entity_id: i32) -> Self {
        self.entity_id = Some(entity_id);
        self
    }

    pub fn with_entity_name(mut self, entity_name: impl Into<String>) -> Self {
        self.entity_name = Some(entity_name.into());
        self
    }

    pub fn with_collection_id(mut self, collection_id: i32) -> Self {
        self.collection_id = Some(collection_id);
        self
    }

    pub fn with_actor_user_id(mut self, actor_user_id: i32) -> Self {
        self.actor_user_id = Some(actor_user_id);
        self
    }

    pub fn with_context(mut self, context: &EventContext) -> Self {
        self.actor_kind = context.actor_kind().as_str().to_string();
        self.actor_user_id = context.actor_user_id();
        self.request_id = context.request_id();
        self.correlation_id = context.correlation_id().map(ToOwned::to_owned);
        self
    }

    pub fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Curated "before" snapshot for update/delete events. Must be captured
    /// inside the same transaction before the row changes (#73).
    pub fn with_before(mut self, before: serde_json::Value) -> Self {
        self.before = Some(before);
        self
    }

    pub fn with_before_opt(mut self, before: Option<serde_json::Value>) -> Self {
        self.before = before;
        self
    }

    /// Curated "after" snapshot for create/update events.
    pub fn with_after(mut self, after: serde_json::Value) -> Self {
        self.after = Some(after);
        self
    }

    pub fn with_after_opt(mut self, after: Option<serde_json::Value>) -> Self {
        self.after = after;
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// The canonical, client-dedupable event identity for this row.
    pub fn event_id(&self) -> Uuid {
        self.event_id
    }

    pub fn actor_kind(&self) -> ActorKind {
        ActorKind::from_db(&self.actor_kind)
            .expect("NewEvent actor_kind is constructed from the ActorKind enum")
    }

    pub fn actor_user_id(&self) -> Option<i32> {
        self.actor_user_id
    }

    pub fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    /// The caller-provided correlation id, if any.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }
}
