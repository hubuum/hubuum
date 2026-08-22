//! Backend-neutral event models for the unified event stream (#71).
//!
//! `NewEvent` is a validating builder: the `(entity_type, action)` pair is
//! checked against the authoritative catalog at construction, so invalid
//! combinations (e.g. `object_relation.updated`) can never reach a storage
//! adapter. The struct holds validated `String` snapshots of the catalog enums
//! while exposing typed builders; [`Event`] converts them back to typed enums
//! on demand. Database rows, fan-out claims, and query mappings belong to each
//! storage adapter.

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
use crate::pagination::{CursorPaginated, CursorValue};

use super::{
    Action, ActorKind, CollectionId, EntityType, EventCatalogError, EventEntityId, EventEnvelope,
    EventSequence, PrincipalId, Provenance, ProvenanceActor, ProvenancePrincipal, TaskId,
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
            principal_id: PrincipalId::new(principal_id)
                .expect("persisted provenance principal id must be positive"),
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
            task_id: self.task_id.map(|task_id| {
                TaskId::new(task_id).expect("persisted provenance task id must be positive")
            }),
        }
    }
}

/// A committed, backend-neutral event used by audit and delivery workflows.
#[derive(Clone, Serialize, Deserialize)]
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
            .field("initiator_user_id", &self.initiator_user_id)
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl Event {
    pub(crate) fn into_envelope(self, principal_names: &PrincipalNames) -> EventEnvelope {
        let provenance = self.resolved_provenance(principal_names);
        EventEnvelope {
            id: EventSequence::new(self.id).expect("persisted event sequence must be positive"),
            event_id: self.event_id,
            occurred_at: self.occurred_at,
            entity_type: self.entity_type,
            entity_id: self.entity_id.map(|entity_id| {
                EventEntityId::new(entity_id).expect("persisted event entity id must be positive")
            }),
            entity_name: self.entity_name,
            collection_id: self.collection_id.map(|collection_id| {
                CollectionId::new(collection_id)
                    .expect("persisted event collection id must be positive")
            }),
            action: self.action,
            actor_user_id: self.actor_user_id.map(|actor_user_id| {
                PrincipalId::new(actor_user_id)
                    .expect("persisted event actor principal id must be positive")
            }),
            actor_kind: self.actor_kind,
            provenance,
            request_id: self.request_id,
            correlation_id: self.correlation_id,
            summary: self.summary,
            before: self.before,
            after: self.after,
            metadata: self.metadata,
            schema_version: self.schema_version,
        }
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
        EntityType::parse(&self.entity_type)
    }

    /// Parse the stored `action` text back into the typed catalog enum.
    pub fn action(&self) -> Result<Action, EventCatalogError> {
        Action::parse(&self.action)
    }

    /// Parse the stored `actor_kind` text back into the typed enum.
    pub fn actor_kind(&self) -> Result<ActorKind, EventCatalogError> {
        ActorKind::parse(&self.actor_kind)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_event_debug_redacts_payload_snapshots() {
        let event = hubuum_events_core::NewEvent::new(
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
}
