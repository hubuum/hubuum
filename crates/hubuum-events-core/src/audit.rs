use super::{
    Action, ActorKind, CollectionId, CorrelationId, EntityType, EventCatalogError, EventContext,
    EventEntityId, EventEnvelope, EventId, MutationProvenance, PrincipalId, TaskId, TraceLink,
    is_valid_pair,
};
use std::fmt;
use uuid::Uuid;

/// Schema version for audit documents without revision-bearing snapshots.
pub const BASE_AUDIT_DOCUMENT_SCHEMA_VERSION: i32 = 1;

/// Schema version for audit documents with a revision-bearing snapshot.
pub const REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION: i32 = 2;

/// The backend-independent, permission-scoped body of a durable audit event.
///
/// This value owns every field whose representation must be identical across
/// storage adapters. Native event rows add persistence coordinates such as a
/// sequence and occurrence timestamp, while [`NewEvent`] adds the logical
/// entity and mutation provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct AuditDocument {
    summary: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: serde_json::Value,
    schema_version: i32,
}

impl AuditDocument {
    /// Start a canonical audit document with an empty metadata object.
    #[must_use]
    pub fn builder(summary: impl Into<String>) -> AuditDocumentBuilder {
        AuditDocumentBuilder {
            summary: summary.into(),
            before: None,
            after: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Construct a summary-only canonical audit document.
    #[must_use]
    pub fn summary(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            before: None,
            after: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            schema_version: BASE_AUDIT_DOCUMENT_SCHEMA_VERSION,
        }
    }

    /// Construct and validate a canonical audit document in one call.
    pub fn try_new(
        summary: impl Into<String>,
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        metadata: serde_json::Value,
    ) -> Result<Self, AuditDocumentError> {
        Self::builder(summary)
            .before_opt(before)
            .after_opt(after)
            .metadata(metadata)
            .try_build()
    }

    #[must_use]
    pub fn summary_text(&self) -> &str {
        &self.summary
    }

    #[must_use]
    pub const fn before(&self) -> Option<&serde_json::Value> {
        self.before.as_ref()
    }

    #[must_use]
    pub const fn after(&self) -> Option<&serde_json::Value> {
        self.after.as_ref()
    }

    #[must_use]
    pub const fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    #[must_use]
    pub const fn schema_version(&self) -> i32 {
        self.schema_version
    }
}

impl fmt::Debug for AuditDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditDocument")
            .field("summary", &self.summary)
            .field("before", &self.before.as_ref().map(|_| "<redacted>"))
            .field("after", &self.after.as_ref().map(|_| "<redacted>"))
            .field("metadata", &"<redacted>")
            .field("schema_version", &self.schema_version)
            .finish()
    }
}

/// Validating builder for a canonical [`AuditDocument`].
pub struct AuditDocumentBuilder {
    summary: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: serde_json::Value,
}

impl AuditDocumentBuilder {
    #[must_use]
    pub fn before(mut self, before: serde_json::Value) -> Self {
        self.before = Some(before);
        self
    }

    #[must_use]
    pub fn before_opt(mut self, before: Option<serde_json::Value>) -> Self {
        self.before = before;
        self
    }

    #[must_use]
    pub fn after(mut self, after: serde_json::Value) -> Self {
        self.after = Some(after);
        self
    }

    #[must_use]
    pub fn after_opt(mut self, after: Option<serde_json::Value>) -> Self {
        self.after = after;
        self
    }

    #[must_use]
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn try_build(self) -> Result<AuditDocument, AuditDocumentError> {
        for (field, snapshot) in [("before", &self.before), ("after", &self.after)] {
            if snapshot.as_ref().is_some_and(|value| !value.is_object()) {
                return Err(AuditDocumentError::new(format!(
                    "audit document {field} snapshot must be a JSON object"
                )));
            }
        }
        if !self.metadata.is_object() {
            return Err(AuditDocumentError::new(
                "audit document metadata must be a JSON object",
            ));
        }
        let mut revision_aware = false;
        for (field, snapshot) in [("before", &self.before), ("after", &self.after)] {
            let Some(revision) = snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get("revision"))
                .filter(|revision| revision.is_number())
            else {
                continue;
            };
            if revision.as_i64().is_none_or(|revision| revision <= 0) {
                return Err(AuditDocumentError::new(format!(
                    "audit document {field} revision must be a positive integer"
                )));
            }
            revision_aware = true;
        }
        let schema_version = if revision_aware {
            REVISION_AWARE_AUDIT_DOCUMENT_SCHEMA_VERSION
        } else {
            BASE_AUDIT_DOCUMENT_SCHEMA_VERSION
        };
        Ok(AuditDocument {
            summary: self.summary,
            before: self.before,
            after: self.after,
            metadata: self.metadata,
            schema_version,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditDocumentError {
    message: String,
}

impl AuditDocumentError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for AuditDocumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AuditDocumentError {}

/// A validated event mutation ready to be appended by a storage adapter.
///
/// Storage-owned columns such as the database id, occurrence timestamp, and
/// dispatch claim state are intentionally absent. Debug output never exposes
/// snapshots or metadata because those values can contain credentials.
pub struct NewEvent {
    event_id: EventId,
    entity_type: EntityType,
    entity_id: Option<EventEntityId>,
    entity_name: Option<String>,
    collection_id: Option<CollectionId>,
    action: Action,
    actor_user_id: Option<PrincipalId>,
    actor_kind: ActorKind,
    initiator_user_id: Option<PrincipalId>,
    task_id: Option<TaskId>,
    request_id: Option<Uuid>,
    correlation_id: Option<CorrelationId>,
    trace_link: Option<TraceLink>,
    document: AuditDocument,
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
            .field("has_trace_link", &self.trace_link.is_some())
            .field("document", &self.document)
            .finish()
    }
}

impl NewEvent {
    /// Validate the event catalog pair and initialize a new event mutation.
    pub fn new(
        entity_type: EntityType,
        action: Action,
        actor_kind: ActorKind,
        summary: impl Into<String>,
    ) -> Result<Self, EventCatalogError> {
        Self::from_document(
            entity_type,
            action,
            actor_kind,
            AuditDocument::summary(summary),
        )
    }

    /// Construct an event from a validated backend-independent document.
    pub fn from_document(
        entity_type: EntityType,
        action: Action,
        actor_kind: ActorKind,
        document: AuditDocument,
    ) -> Result<Self, EventCatalogError> {
        if !is_valid_pair(entity_type, action) {
            return Err(EventCatalogError::InvalidActionForType {
                entity_type,
                action,
            });
        }
        Ok(Self {
            event_id: EventId::new(),
            entity_type,
            entity_id: None,
            entity_name: None,
            collection_id: None,
            action,
            actor_user_id: None,
            actor_kind,
            initiator_user_id: None,
            task_id: None,
            request_id: None,
            correlation_id: None,
            trace_link: None,
            document,
        })
    }

    #[must_use]
    pub fn with_entity_id(mut self, entity_id: EventEntityId) -> Self {
        self.entity_id = Some(entity_id);
        self
    }

    #[must_use]
    pub fn with_entity_name(mut self, entity_name: impl Into<String>) -> Self {
        self.entity_name = Some(entity_name.into());
        self
    }

    #[must_use]
    pub fn with_collection_id(mut self, collection_id: CollectionId) -> Self {
        self.collection_id = Some(collection_id);
        self
    }

    #[must_use]
    pub fn with_actor_user_id(mut self, actor_user_id: PrincipalId) -> Self {
        self.actor_user_id = Some(actor_user_id);
        self
    }

    #[must_use]
    pub fn with_context(mut self, context: &EventContext) -> Self {
        self.actor_kind = context.actor_kind();
        self.actor_user_id = context.actor_user_id();
        self.initiator_user_id = context.initiator_user_id();
        self.task_id = context.task_id();
        self.request_id = context.request_id();
        self.correlation_id = context.correlation_id.clone();
        self.trace_link = context.trace_link().cloned();
        self
    }

    #[must_use]
    pub fn with_mutation_provenance(mut self, provenance: &MutationProvenance) -> Self {
        self.actor_kind = provenance.actor_kind();
        self.actor_user_id = provenance.actor_user_id();
        self.initiator_user_id = provenance.initiator_user_id();
        self.task_id = provenance.task_id();
        self.trace_link = provenance.trace_link().cloned();
        self
    }

    #[must_use]
    pub fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    #[must_use]
    pub fn with_trace_link(mut self, trace_link: TraceLink) -> Self {
        self.trace_link = Some(trace_link);
        self
    }

    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    #[must_use]
    pub const fn entity_type(&self) -> EntityType {
        self.entity_type
    }

    #[must_use]
    pub const fn entity_id(&self) -> Option<EventEntityId> {
        self.entity_id
    }

    #[must_use]
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    #[must_use]
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }

    #[must_use]
    pub const fn action(&self) -> Action {
        self.action
    }

    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    #[must_use]
    pub const fn actor_user_id(&self) -> Option<PrincipalId> {
        self.actor_user_id
    }

    #[must_use]
    pub const fn initiator_user_id(&self) -> Option<PrincipalId> {
        self.initiator_user_id
    }

    #[must_use]
    pub const fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    #[must_use]
    pub fn correlation_id(&self) -> Option<&CorrelationId> {
        self.correlation_id.as_ref()
    }

    #[must_use]
    pub fn trace_link(&self) -> Option<&TraceLink> {
        self.trace_link.as_ref()
    }

    #[must_use]
    pub fn summary(&self) -> &str {
        self.document.summary_text()
    }

    #[must_use]
    pub const fn before(&self) -> Option<&serde_json::Value> {
        self.document.before()
    }

    #[must_use]
    pub const fn after(&self) -> Option<&serde_json::Value> {
        self.document.after()
    }

    #[must_use]
    pub const fn metadata(&self) -> &serde_json::Value {
        self.document.metadata()
    }

    #[must_use]
    pub const fn schema_version(&self) -> i32 {
        self.document.schema_version()
    }

    #[must_use]
    pub const fn document(&self) -> &AuditDocument {
        &self.document
    }
}

impl EventEnvelope {
    /// Return the backend-independent document carried by this envelope.
    #[must_use]
    pub fn audit_document(&self) -> AuditDocument {
        AuditDocument {
            summary: self.summary.clone(),
            before: self.before.clone(),
            after: self.after.clone(),
            metadata: self.metadata.clone(),
            schema_version: self.schema_version,
        }
    }
}
