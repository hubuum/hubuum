//! `Event` / `NewEvent` models for the unified `events` stream (#71).
//!
//! `NewEvent` is a validating builder: the `(entity_type, action)` pair is
//! checked against the authoritative catalog at construction, so invalid
//! combinations (e.g. `object_relation.updated`) can never reach
//! [`super::emit_event`]. The struct holds validated `String` snapshots of the
//! catalog enums at the Diesel boundary while exposing typed builders; the
//! [`Event`] read model converts back to the typed enums on demand.

use crate::db::prelude::*;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::models::ResourceRevision;
use crate::models::search::{FilterField, SortParam};
use crate::models::{REDACTED_DEBUG_VALUE, redacted_debug_option};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::events;

use super::{
    Action, ActorKind, EntityType, EventCatalogError, EventContext, MutationProvenance, Provenance,
    ProvenanceActor, ProvenancePrincipal, is_valid_pair,
};

/// Principal names resolved in one database query for provenance responses.
///
/// Keeping the map private prevents response builders from duplicating lookup
/// and cloning behavior or treating the resolved names as general-purpose
/// principal records.
#[derive(Debug, Clone, Default)]
pub(crate) struct PrincipalNames(HashMap<i32, String>);

impl PrincipalNames {
    pub(crate) fn name(&self, principal_id: i32) -> Option<&str> {
        self.0.get(&principal_id).map(String::as_str)
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, principal_id: i32) -> bool {
        self.0.contains_key(&principal_id)
    }

    fn principal(&self, principal_id: i32) -> ProvenancePrincipal {
        ProvenancePrincipal {
            principal_id,
            name: self.name(principal_id).map(ToOwned::to_owned),
        }
    }
}

impl FromIterator<(i32, String)> for PrincipalNames {
    fn from_iter<T: IntoIterator<Item = (i32, String)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Stored provenance columns before principal names are resolved.
///
/// Named fields make it difficult to transpose actor, initiator, and task ids
/// while translating persistence models into the public provenance shape.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StoredProvenance<'a> {
    actor_kind: Option<&'a str>,
    actor_user_id: Option<i32>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
}

impl<'a> StoredProvenance<'a> {
    pub(crate) fn from_actor_kind(actor_kind: Option<&'a str>) -> Self {
        Self {
            actor_kind,
            actor_user_id: None,
            initiator_user_id: None,
            task_id: None,
        }
    }

    pub(crate) fn with_actor_user_id(mut self, actor_user_id: Option<i32>) -> Self {
        self.actor_user_id = actor_user_id;
        self
    }

    pub(crate) fn with_initiator_user_id(mut self, initiator_user_id: Option<i32>) -> Self {
        self.initiator_user_id = initiator_user_id;
        self
    }

    pub(crate) fn with_task_id(mut self, task_id: Option<i32>) -> Self {
        self.task_id = task_id;
        self
    }

    pub(crate) fn resolve(self, principal_names: &PrincipalNames) -> Provenance {
        Provenance {
            actor: ProvenanceActor {
                kind: self.actor_kind.map(ToOwned::to_owned),
                principal: self
                    .actor_user_id
                    .map(|principal_id| principal_names.principal(principal_id)),
            },
            initiator: self
                .initiator_user_id
                .map(|principal_id| principal_names.principal(principal_id)),
            task_id: self.task_id,
        }
    }
}

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
#[derive(Clone, Serialize, Deserialize, Queryable, Selectable)]
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
    pub initiator_user_id: Option<i32>,
    pub task_id: Option<i32>,
    pub before_revision: Option<ResourceRevision>,
    pub after_revision: Option<ResourceRevision>,
}

impl fmt::Debug for Event {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Event")
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
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
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
    pub provenance: Provenance,
    pub request_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub summary: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub schema_version: i32,
    pub before_revision: Option<ResourceRevision>,
    pub after_revision: Option<ResourceRevision>,
}

impl fmt::Debug for EventResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventResponse")
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
            .field("provenance", &self.provenance)
            .field("request_id", &self.request_id)
            .field("correlation_id", &self.correlation_id)
            .field("summary", &self.summary)
            .field("before", &redacted_debug_option(&self.before))
            .field("after", &redacted_debug_option(&self.after))
            .field("metadata", &REDACTED_DEBUG_VALUE)
            .field("schema_version", &self.schema_version)
            .finish()
    }
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

    pub(crate) fn resolved_provenance(&self, principal_names: &PrincipalNames) -> Provenance {
        StoredProvenance::from_actor_kind(Some(&self.actor_kind))
            .with_actor_user_id(self.actor_user_id)
            .with_initiator_user_id(self.initiator_user_id)
            .with_task_id(self.task_id)
            .resolve(principal_names)
    }
}

impl EventResponse {
    pub(crate) fn from_event_with_names(value: Event, principal_names: &PrincipalNames) -> Self {
        let provenance = value.resolved_provenance(principal_names);
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
            provenance,
            request_id: value.request_id,
            correlation_id: value.correlation_id,
            summary: value.summary,
            before: value.before,
            after: value.after,
            metadata: value.metadata,
            schema_version: value.schema_version,
            before_revision: value.before_revision,
            after_revision: value.after_revision,
        }
    }
}

impl From<Event> for EventResponse {
    fn from(value: Event) -> Self {
        Self::from_event_with_names(value, &PrincipalNames::default())
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
                sql_type: CursorSqlType::BigInt,
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
#[derive(Insertable)]
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
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: serde_json::Value,
    schema_version: i32,
}

impl fmt::Debug for NewEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewEvent")
            .field("event_id", &self.event_id)
            .field("entity_type", &self.entity_type)
            .field("entity_id", &self.entity_id)
            .field("entity_name", &self.entity_name)
            .field("collection_id", &self.collection_id)
            .field("action", &self.action)
            .field("actor_user_id", &self.actor_user_id)
            .field("actor_kind", &self.actor_kind)
            .field("initiator_user_id", &self.initiator_user_id)
            .field("task_id", &self.task_id)
            .field("request_id", &self.request_id)
            .field("correlation_id", &self.correlation_id)
            .field("summary", &self.summary)
            .field("before", &redacted_debug_option(&self.before))
            .field("after", &redacted_debug_option(&self.after))
            .field("metadata", &REDACTED_DEBUG_VALUE)
            .field("schema_version", &self.schema_version)
            .finish()
    }
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
            initiator_user_id: None,
            task_id: None,
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
        self.initiator_user_id = context.initiator_user_id();
        self.task_id = context.task_id();
        self.request_id = context.request_id();
        self.correlation_id = context.correlation_id().map(ToOwned::to_owned);
        self
    }

    pub fn with_mutation_provenance(mut self, provenance: &MutationProvenance) -> Self {
        self.actor_kind = provenance.actor_kind().as_str().to_string();
        self.actor_user_id = provenance.actor_user_id();
        self.initiator_user_id = provenance.initiator_user_id();
        self.task_id = provenance.task_id();
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

    pub fn initiator_user_id(&self) -> Option<i32> {
        self.initiator_user_id
    }

    pub fn task_id(&self) -> Option<i32> {
        self.task_id
    }

    pub fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    /// The caller-provided correlation id, if any.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn new_event_debug_redacts_payload_snapshots() {
        let event = NewEvent::new(
            EntityType::Collection,
            Action::Created,
            ActorKind::User,
            "created",
        )
        .unwrap()
        .with_before(serde_json::json!({"token": "before-secret"}))
        .with_after(serde_json::json!({"token": "after-secret"}))
        .with_metadata(serde_json::json!({"token": "metadata-secret"}));

        let debug = format!("{event:?}");

        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!debug.contains("before-secret"));
        assert!(!debug.contains("after-secret"));
        assert!(!debug.contains("metadata-secret"));
    }

    #[test]
    fn stored_event_debug_redacts_payloads_and_fanout_claim_token() {
        let claim_token = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let event = Event {
            id: 1,
            event_id: Uuid::new_v4(),
            occurred_at: timestamp(),
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
            before_revision: None,
            after_revision: None,
            metadata: serde_json::json!({"token": "stored-metadata-secret"}),
            schema_version: 1,
            dispatched_at: None,
            fanout_locked_until: Some(timestamp()),
            fanout_claim_token: Some(claim_token),
            initiator_user_id: Some(3),
            task_id: None,
        };

        let debug = format!("{event:?}");

        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!debug.contains("stored-before-secret"));
        assert!(!debug.contains("stored-after-secret"));
        assert!(!debug.contains("stored-metadata-secret"));
        assert!(!debug.contains(&claim_token.to_string()));
    }
}
