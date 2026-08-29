//! Backend-agnostic event catalog and provenance types for Hubuum's unified
//! event & audit stream (issue #70).
//!
//! This crate owns the **authoritative** `EntityType` / `Action` catalog: the
//! set of valid event kinds and which actions are legal for each entity type.
//! It is intentionally free of Diesel, Actix, app configuration, and Hubuum's
//! `ApiError` so it can be shared by the producer (`emit_event`), the audit
//! read API (filter validation), and the fan-out worker (subscription
//! matching) without leaking backend concerns.
//!
//! The catalog mirrors the "Entity types & actions" table in the epic (#70):
//! `entity_type` is the API/concept name, **not** the table name (`class`, not
//! `hubuumclass`), and actions are **non-uniform** per entity type.

use std::fmt;

use chrono::{DateTime, NaiveDateTime, Utc};
pub use hubuum_domain::{CollectionId, PrincipalId, TaskId};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use utoipa::ToSchema;
use uuid::Uuid;

/// The kind of actor that originated an event.
///
/// Stored as text on the `events.actor_kind` column. System actors cover
/// maintenance and recovery paths; worker actors carry root-task causation
/// through the event's durable initiator and task provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub enum ActorKind {
    User,
    System,
    Worker,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::User => "user",
            ActorKind::System => "system",
            ActorKind::Worker => "worker",
        }
    }

    pub fn parse(value: &str) -> Result<Self, EventCatalogError> {
        match value {
            "user" => Ok(ActorKind::User),
            "system" => Ok(ActorKind::System),
            "worker" => Ok(ActorKind::Worker),
            other => Err(EventCatalogError::UnknownActorKind(other.to_string())),
        }
    }
}

/// Durable principal identity used in provenance responses and sink envelopes.
///
/// Names are resolved at read/delivery time rather than copied into immutable
/// event and history storage. The id therefore remains available even when no
/// current principal name can be resolved.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub struct ProvenancePrincipal {
    pub principal_id: PrincipalId,
    pub name: Option<String>,
}

/// The immediate actor that performed a mutation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub struct ProvenanceActor {
    pub kind: Option<String>,
    pub principal: Option<ProvenancePrincipal>,
}

/// Shared provenance returned by audit/history APIs and serialized to sinks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub struct Provenance {
    pub actor: ProvenanceActor,
    pub initiator: Option<ProvenancePrincipal>,
    pub task_id: Option<TaskId>,
}

/// Typed mutation attribution propagated through database task-local state.
///
/// The actor is the user, worker, or system process that performed the write.
/// The initiator is the durable principal that submitted the root task, if the
/// write runs asynchronously on that principal's behalf.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationProvenance {
    actor_kind: ActorKind,
    actor_user_id: Option<PrincipalId>,
    initiator_user_id: Option<PrincipalId>,
    task_id: Option<TaskId>,
}

impl MutationProvenance {
    pub fn user(actor_user_id: PrincipalId) -> Self {
        Self::new(ActorKind::User, Some(actor_user_id), None, None)
    }

    pub fn user_for_task(
        actor_user_id: PrincipalId,
        initiator_user_id: Option<PrincipalId>,
        task_id: TaskId,
    ) -> Self {
        Self::new(
            ActorKind::User,
            Some(actor_user_id),
            initiator_user_id,
            Some(task_id),
        )
    }

    pub fn system() -> Self {
        Self::new(ActorKind::System, None, None, None)
    }

    pub fn system_for_task(initiator_user_id: Option<PrincipalId>, task_id: TaskId) -> Self {
        Self::new(ActorKind::System, None, initiator_user_id, Some(task_id))
    }

    pub fn worker(initiator_user_id: Option<PrincipalId>, task_id: TaskId) -> Self {
        Self::new(ActorKind::Worker, None, initiator_user_id, Some(task_id))
    }

    pub fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    pub fn actor_user_id(&self) -> Option<PrincipalId> {
        self.actor_user_id
    }

    pub fn initiator_user_id(&self) -> Option<PrincipalId> {
        self.initiator_user_id
    }

    pub fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    fn new(
        actor_kind: ActorKind,
        actor_user_id: Option<PrincipalId>,
        initiator_user_id: Option<PrincipalId>,
        task_id: Option<TaskId>,
    ) -> Self {
        Self {
            actor_kind,
            actor_user_id,
            initiator_user_id,
            task_id,
        }
    }
}

/// Actor + request provenance attached to an event-producing mutation.
///
/// This type intentionally has no Actix, Diesel, or application-model
/// dependencies so producer code can pass it across future crate boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventContext {
    mutation: MutationProvenance,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
}

impl EventContext {
    /// Build an event context from already-typed mutation provenance.
    pub fn from_mutation(mutation: MutationProvenance) -> Self {
        Self::new(mutation, None, None)
    }

    pub fn user(
        actor_user_id: PrincipalId,
        request_id: Option<Uuid>,
        correlation_id: Option<String>,
    ) -> Self {
        Self::new(
            MutationProvenance::user(actor_user_id),
            request_id,
            correlation_id,
        )
    }

    pub fn system() -> Self {
        Self::new(MutationProvenance::system(), None, None)
    }

    pub fn actor_kind(&self) -> ActorKind {
        self.mutation.actor_kind()
    }

    pub fn actor_user_id(&self) -> Option<PrincipalId> {
        self.mutation.actor_user_id()
    }

    pub fn initiator_user_id(&self) -> Option<PrincipalId> {
        self.mutation.initiator_user_id()
    }

    pub fn task_id(&self) -> Option<TaskId> {
        self.mutation.task_id()
    }

    pub fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    fn new(
        mutation: MutationProvenance,
        request_id: Option<Uuid>,
        correlation_id: Option<String>,
    ) -> Self {
        Self {
            mutation,
            request_id,
            correlation_id,
        }
    }
}

/// The conceptual entity type an event is about.
///
/// This is the API/concept name, **not** the table name (`class`, not
/// `hubuumclass`). Stored as text on `events.entity_type` and validated
/// against the catalog at emit time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub enum EntityType {
    Collection,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    User,
    Group,
    UserGroup,
    Permission,
    Token,
    RemoteTarget,
    ExportTemplate,
    Task,
    ServiceAccount,
    EventSink,
    EventSubscription,
    ComputedFieldDefinition,
    ExternalIdentitySync,
    Restore,
}

impl EntityType {
    pub fn as_str(self) -> &'static str {
        match self {
            EntityType::Collection => "collection",
            EntityType::Class => "class",
            EntityType::Object => "object",
            EntityType::ClassRelation => "class_relation",
            EntityType::ObjectRelation => "object_relation",
            EntityType::User => "user",
            EntityType::Group => "group",
            EntityType::UserGroup => "user_group",
            EntityType::Permission => "permission",
            EntityType::Token => "token",
            EntityType::RemoteTarget => "remote_target",
            EntityType::ExportTemplate => "export_template",
            EntityType::Task => "task",
            EntityType::ServiceAccount => "service_account",
            EntityType::EventSink => "event_sink",
            EntityType::EventSubscription => "event_subscription",
            EntityType::ComputedFieldDefinition => "computed_field_definition",
            EntityType::ExternalIdentitySync => "external_identity_sync",
            EntityType::Restore => "restore",
        }
    }

    pub fn parse(value: &str) -> Result<Self, EventCatalogError> {
        match value {
            "collection" => Ok(EntityType::Collection),
            "class" => Ok(EntityType::Class),
            "object" => Ok(EntityType::Object),
            "class_relation" => Ok(EntityType::ClassRelation),
            "object_relation" => Ok(EntityType::ObjectRelation),
            "user" => Ok(EntityType::User),
            "group" => Ok(EntityType::Group),
            "user_group" => Ok(EntityType::UserGroup),
            "permission" => Ok(EntityType::Permission),
            "token" => Ok(EntityType::Token),
            "remote_target" => Ok(EntityType::RemoteTarget),
            "export_template" => Ok(EntityType::ExportTemplate),
            "task" => Ok(EntityType::Task),
            "service_account" => Ok(EntityType::ServiceAccount),
            "event_sink" => Ok(EntityType::EventSink),
            "event_subscription" => Ok(EntityType::EventSubscription),
            "computed_field_definition" => Ok(EntityType::ComputedFieldDefinition),
            "external_identity_sync" => Ok(EntityType::ExternalIdentitySync),
            "restore" => Ok(EntityType::Restore),
            other => Err(EventCatalogError::UnknownEntityType(other.to_string())),
        }
    }
}

/// The action an event records. Actions are **non-uniform** per entity type:
/// relations have no `Updated`; `permission` is grant/revoke; `user_group` is
/// add/remove; `token` is created/revoked/purged; `remote_target` adds `Invoked`;
/// `task` is lifecycle-only (see #87).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub enum Action {
    Created,
    Updated,
    Deleted,
    Added,
    Removed,
    Granted,
    Revoked,
    Purged,
    Invoked,
    // task lifecycle (#87)
    Queued,
    Started,
    Validating,
    Running,
    Succeeded,
    Failed,
    PartiallySucceeded,
    Cancelled,
    Cleanup,
    Disabled,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Created => "created",
            Action::Updated => "updated",
            Action::Deleted => "deleted",
            Action::Added => "added",
            Action::Removed => "removed",
            Action::Granted => "granted",
            Action::Revoked => "revoked",
            Action::Purged => "purged",
            Action::Invoked => "invoked",
            Action::Queued => "queued",
            Action::Started => "started",
            Action::Validating => "validating",
            Action::Running => "running",
            Action::Succeeded => "succeeded",
            Action::Failed => "failed",
            Action::PartiallySucceeded => "partially_succeeded",
            Action::Cancelled => "cancelled",
            Action::Cleanup => "cleanup",
            Action::Disabled => "disabled",
        }
    }

    pub fn parse(value: &str) -> Result<Self, EventCatalogError> {
        match value {
            "created" => Ok(Action::Created),
            "updated" => Ok(Action::Updated),
            "deleted" => Ok(Action::Deleted),
            "added" => Ok(Action::Added),
            "removed" => Ok(Action::Removed),
            "granted" => Ok(Action::Granted),
            "revoked" => Ok(Action::Revoked),
            "purged" => Ok(Action::Purged),
            "invoked" => Ok(Action::Invoked),
            "queued" => Ok(Action::Queued),
            "started" => Ok(Action::Started),
            "validating" => Ok(Action::Validating),
            "running" => Ok(Action::Running),
            "succeeded" => Ok(Action::Succeeded),
            "failed" => Ok(Action::Failed),
            "partially_succeeded" => Ok(Action::PartiallySucceeded),
            "cancelled" => Ok(Action::Cancelled),
            "cleanup" => Ok(Action::Cleanup),
            "disabled" => Ok(Action::Disabled),
            other => Err(EventCatalogError::UnknownAction(other.to_string())),
        }
    }
}

/// Returns the actions valid for `entity_type`, per the authoritative catalog.
///
/// This drives both audit-row emission (#73) and subscription/filter validation
/// (#74/#75): an `(entity_type, action)` pair outside this mapping is invalid.
pub fn valid_actions(entity_type: EntityType) -> &'static [Action] {
    use Action as A;
    use EntityType as E;
    match entity_type {
        E::Collection | E::Class | E::Object | E::User | E::Group | E::ExportTemplate => {
            &[A::Created, A::Updated, A::Deleted]
        }
        E::ServiceAccount => &[A::Created, A::Updated, A::Disabled, A::Deleted],
        E::EventSink | E::EventSubscription | E::ComputedFieldDefinition => {
            &[A::Created, A::Updated, A::Deleted]
        }
        E::ExternalIdentitySync => &[A::Succeeded, A::Failed],
        E::Restore => &[A::Succeeded],
        E::RemoteTarget => &[A::Created, A::Updated, A::Deleted, A::Invoked],
        E::ClassRelation | E::ObjectRelation => &[A::Created, A::Deleted],
        E::UserGroup => &[A::Added, A::Removed],
        E::Permission => &[A::Granted, A::Revoked],
        E::Token => &[A::Created, A::Revoked, A::Purged],
        E::Task => &[
            A::Queued,
            A::Started,
            A::Validating,
            A::Running,
            A::Succeeded,
            A::Failed,
            A::PartiallySucceeded,
            A::Cancelled,
            A::Cleanup,
        ],
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventIdentifierError;

impl fmt::Display for EventIdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("event identifiers must be positive")
    }
}

impl std::error::Error for EventIdentifierError {}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "i64", into = "i64")]
pub struct EventSequence(i64);

impl EventSequence {
    pub const fn new(value: i64) -> Result<Self, EventIdentifierError> {
        if value <= 0 {
            return Err(EventIdentifierError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for EventSequence {
    type Error = EventIdentifierError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<EventSequence> for i64 {
    fn from(value: EventSequence) -> Self {
        value.get()
    }
}

#[cfg(feature = "schema")]
impl utoipa::PartialSchema for EventSequence {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{SchemaFormat, Type};
        use utoipa::openapi::{KnownFormat, ObjectBuilder};

        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
            .minimum(Some(1))
            .description(Some("Validated positive event sequence."))
            .into()
    }
}

#[cfg(feature = "schema")]
impl utoipa::ToSchema for EventSequence {}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "i32", into = "i32")]
pub struct EventEntityId(i32);

impl EventEntityId {
    pub const fn new(value: i32) -> Result<Self, EventIdentifierError> {
        if value <= 0 {
            return Err(EventIdentifierError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for EventEntityId {
    type Error = EventIdentifierError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<EventEntityId> for i32 {
    fn from(value: EventEntityId) -> Self {
        value.get()
    }
}

#[cfg(feature = "schema")]
impl utoipa::PartialSchema for EventEntityId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{SchemaFormat, Type};
        use utoipa::openapi::{KnownFormat, ObjectBuilder};

        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
            .minimum(Some(1))
            .description(Some("Validated positive event entity id."))
            .into()
    }
}

#[cfg(feature = "schema")]
impl utoipa::ToSchema for EventEntityId {}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "EventEnvelopeWire")]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub struct EventEnvelope {
    id: EventSequence,
    event_id: Uuid,
    #[serde(serialize_with = "serialize_utc_as_naive")]
    #[cfg_attr(feature = "schema", schema(value_type = NaiveDateTime))]
    occurred_at: DateTime<Utc>,
    entity_type: EntityType,
    entity_id: Option<EventEntityId>,
    entity_name: Option<String>,
    collection_id: Option<CollectionId>,
    action: Action,
    actor_user_id: Option<PrincipalId>,
    actor_kind: ActorKind,
    provenance: Provenance,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: serde_json::Value,
    schema_version: i32,
}

fn serialize_utc_as_naive<S>(timestamp: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    timestamp.naive_utc().serialize(serializer)
}

#[derive(Deserialize)]
struct EventEnvelopeWire {
    id: EventSequence,
    event_id: Uuid,
    occurred_at: NaiveDateTime,
    entity_type: EntityType,
    entity_id: Option<EventEntityId>,
    entity_name: Option<String>,
    collection_id: Option<CollectionId>,
    action: Action,
    actor_user_id: Option<PrincipalId>,
    actor_kind: ActorKind,
    provenance: Provenance,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: serde_json::Value,
    schema_version: i32,
}

impl TryFrom<EventEnvelopeWire> for EventEnvelope {
    type Error = EventEnvelopeError;

    fn try_from(wire: EventEnvelopeWire) -> Result<Self, Self::Error> {
        EventEnvelope::builder()
            .id(wire.id)
            .event_id(wire.event_id)
            .occurred_at(wire.occurred_at.and_utc())
            .entity_type(wire.entity_type)
            .entity_id(wire.entity_id)
            .entity_name(wire.entity_name)
            .collection_id(wire.collection_id)
            .action(wire.action)
            .actor_user_id(wire.actor_user_id)
            .actor_kind(wire.actor_kind)
            .provenance(wire.provenance)
            .request_id(wire.request_id)
            .correlation_id(wire.correlation_id)
            .summary(wire.summary)
            .before(wire.before)
            .after(wire.after)
            .metadata(wire.metadata)
            .schema_version(wire.schema_version)
            .try_build()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEnvelopeError {
    message: String,
}

impl EventEnvelopeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for EventEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for EventEnvelopeError {}

#[derive(Default)]
pub struct EventEnvelopeBuilder {
    id: Option<EventSequence>,
    event_id: Option<Uuid>,
    occurred_at: Option<DateTime<Utc>>,
    entity_type: Option<EntityType>,
    entity_id: Option<EventEntityId>,
    entity_name: Option<String>,
    collection_id: Option<CollectionId>,
    action: Option<Action>,
    actor_user_id: Option<PrincipalId>,
    actor_kind: Option<ActorKind>,
    provenance: Provenance,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: Option<String>,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    metadata: Option<serde_json::Value>,
    schema_version: Option<i32>,
}

macro_rules! event_envelope_builder_required_setter {
    ($name:ident, $field:ident, $type:ty) => {
        #[must_use]
        pub fn $name(mut self, value: $type) -> Self {
            self.$field = Some(value);
            self
        }
    };
}

macro_rules! event_envelope_builder_optional_setter {
    ($name:ident, $field:ident, $type:ty) => {
        #[must_use]
        pub fn $name(mut self, value: Option<$type>) -> Self {
            self.$field = value;
            self
        }
    };
}

impl EventEnvelopeBuilder {
    event_envelope_builder_required_setter!(id, id, EventSequence);
    event_envelope_builder_required_setter!(event_id, event_id, Uuid);
    event_envelope_builder_required_setter!(occurred_at, occurred_at, DateTime<Utc>);
    event_envelope_builder_required_setter!(entity_type, entity_type, EntityType);
    event_envelope_builder_optional_setter!(entity_id, entity_id, EventEntityId);
    event_envelope_builder_optional_setter!(entity_name, entity_name, String);
    event_envelope_builder_optional_setter!(collection_id, collection_id, CollectionId);
    event_envelope_builder_required_setter!(action, action, Action);
    event_envelope_builder_optional_setter!(actor_user_id, actor_user_id, PrincipalId);
    event_envelope_builder_required_setter!(actor_kind, actor_kind, ActorKind);
    event_envelope_builder_optional_setter!(request_id, request_id, Uuid);
    event_envelope_builder_optional_setter!(correlation_id, correlation_id, String);
    event_envelope_builder_required_setter!(summary, summary, String);
    event_envelope_builder_optional_setter!(before, before, serde_json::Value);
    event_envelope_builder_optional_setter!(after, after, serde_json::Value);
    event_envelope_builder_required_setter!(metadata, metadata, serde_json::Value);
    event_envelope_builder_required_setter!(schema_version, schema_version, i32);

    #[must_use]
    pub fn provenance(mut self, value: Provenance) -> Self {
        self.provenance = value;
        self
    }

    pub fn try_build(self) -> Result<EventEnvelope, EventEnvelopeError> {
        let entity_type = self
            .entity_type
            .ok_or_else(|| EventEnvelopeError::new("event envelope is missing entity_type"))?;
        let action = self
            .action
            .ok_or_else(|| EventEnvelopeError::new("event envelope is missing action"))?;
        if !is_valid_pair(entity_type, action) {
            return Err(EventEnvelopeError::new(format!(
                "action '{}' is not valid for entity type '{}'",
                action.as_str(),
                entity_type.as_str()
            )));
        }

        let actor_kind = self
            .actor_kind
            .ok_or_else(|| EventEnvelopeError::new("event envelope is missing actor_kind"))?;
        let actor_user_id = self.actor_user_id;
        let mut provenance = self.provenance;
        if let Some(provenance_kind) = provenance.actor.kind.as_deref() {
            let provenance_kind = ActorKind::parse(provenance_kind)
                .map_err(|error| EventEnvelopeError::new(error.to_string()))?;
            if provenance_kind != actor_kind {
                return Err(EventEnvelopeError::new(
                    "event envelope actor kind does not match its provenance actor kind",
                ));
            }
        } else {
            provenance.actor.kind = Some(actor_kind.as_str().to_string());
        }
        match (actor_user_id, provenance.actor.principal.as_ref()) {
            (Some(actor_user_id), Some(principal)) if principal.principal_id != actor_user_id => {
                return Err(EventEnvelopeError::new(
                    "event envelope actor id does not match its provenance actor principal",
                ));
            }
            (Some(actor_user_id), None) => {
                provenance.actor.principal = Some(ProvenancePrincipal {
                    principal_id: actor_user_id,
                    name: None,
                });
            }
            (None, Some(_)) => {
                return Err(EventEnvelopeError::new(
                    "event envelope provenance actor principal has no matching actor id",
                ));
            }
            _ => {}
        }

        let metadata = self
            .metadata
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
        if !metadata.is_object() {
            return Err(EventEnvelopeError::new(
                "event envelope metadata must be a JSON object",
            ));
        }
        for (field, snapshot) in [("before", &self.before), ("after", &self.after)] {
            if snapshot.as_ref().is_some_and(|value| !value.is_object()) {
                return Err(EventEnvelopeError::new(format!(
                    "event envelope {field} snapshot must be a JSON object"
                )));
            }
        }

        let schema_version = self.schema_version.unwrap_or(1);
        if schema_version <= 0 {
            return Err(EventEnvelopeError::new(
                "event envelope schema_version must be positive",
            ));
        }

        Ok(EventEnvelope {
            id: self
                .id
                .ok_or_else(|| EventEnvelopeError::new("event envelope is missing id"))?,
            event_id: self
                .event_id
                .ok_or_else(|| EventEnvelopeError::new("event envelope is missing event_id"))?,
            occurred_at: self
                .occurred_at
                .ok_or_else(|| EventEnvelopeError::new("event envelope is missing occurred_at"))?,
            entity_type,
            entity_id: self.entity_id,
            entity_name: self.entity_name,
            collection_id: self.collection_id,
            action,
            actor_user_id,
            actor_kind,
            provenance,
            request_id: self.request_id,
            correlation_id: self.correlation_id,
            summary: self
                .summary
                .ok_or_else(|| EventEnvelopeError::new("event envelope is missing summary"))?,
            before: self.before,
            after: self.after,
            metadata,
            schema_version,
        })
    }
}

impl fmt::Debug for EventEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventEnvelope")
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
            .field("before", &self.before.as_ref().map(|_| "<redacted>"))
            .field("after", &self.after.as_ref().map(|_| "<redacted>"))
            .field("metadata", &"<redacted>")
            .field("schema_version", &self.schema_version)
            .finish()
    }
}

impl EventEnvelope {
    #[must_use]
    pub fn builder() -> EventEnvelopeBuilder {
        EventEnvelopeBuilder::default()
    }

    #[must_use]
    pub const fn id(&self) -> EventSequence {
        self.id
    }

    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    #[must_use]
    pub const fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
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
    pub const fn actor_user_id(&self) -> Option<PrincipalId> {
        self.actor_user_id
    }

    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    #[must_use]
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    #[must_use]
    pub const fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    #[must_use]
    pub fn summary(&self) -> &str {
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

    #[must_use]
    pub fn without_payloads(mut self) -> Self {
        self.before = None;
        self.after = None;
        self
    }

    pub fn related_collection_ids(&self) -> Vec<CollectionId> {
        self.metadata
            .get("related_collection_ids")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| {
                        value
                            .as_i64()
                            .and_then(|value| i32::try_from(value).ok())
                            .or_else(|| value.as_str().and_then(|value| value.parse::<i32>().ok()))
                            .and_then(|value| CollectionId::new(value).ok())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Optional additional fan-out filter for an event subscription.
///
/// `entity_types` and `actions` remain first-class subscription fields because
/// they drive catalog validation and coarse fan-out selection. This filter
/// narrows those matches by stable event-envelope fields. Empty or omitted
/// fields match all events for that dimension.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
pub struct EventSubscriptionFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collection_ids: Vec<CollectionId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_collection_ids: Vec<CollectionId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entity_ids: Vec<EventEntityId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entity_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actor_kinds: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actor_user_ids: Vec<PrincipalId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initiator_user_ids: Vec<PrincipalId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub correlation_ids: Vec<String>,
}

impl EventSubscriptionFilter {
    pub fn matches(&self, event: &EventEnvelope) -> bool {
        matches_optional(&self.collection_ids, event.collection_id())
            && matches_any(
                &self.related_collection_ids,
                &event.related_collection_ids(),
            )
            && matches_optional(&self.entity_ids, event.entity_id())
            && matches_optional_str(&self.entity_names, event.entity_name())
            && matches_str(&self.actor_kinds, event.actor_kind().as_str())
            && matches_optional(&self.actor_user_ids, event.actor_user_id())
            && matches_optional(
                &self.initiator_user_ids,
                event
                    .provenance()
                    .initiator
                    .as_ref()
                    .map(|principal| principal.principal_id),
            )
            && matches_optional_uuid(&self.request_ids, event.request_id())
            && matches_optional_str(&self.correlation_ids, event.correlation_id())
    }

    pub fn validate(&self) -> Result<(), EventFilterError> {
        ensure_unique("collection_ids", &self.collection_ids)?;
        ensure_unique("related_collection_ids", &self.related_collection_ids)?;
        ensure_unique("entity_ids", &self.entity_ids)?;
        ensure_unique_str("entity_names", &self.entity_names)?;
        ensure_unique_str("actor_kinds", &self.actor_kinds)?;
        ensure_unique("actor_user_ids", &self.actor_user_ids)?;
        ensure_unique("initiator_user_ids", &self.initiator_user_ids)?;
        ensure_unique_uuid("request_ids", &self.request_ids)?;
        ensure_unique_str("correlation_ids", &self.correlation_ids)?;

        for value in &self.entity_names {
            ensure_non_empty("entity_names", value)?;
        }
        for value in &self.actor_kinds {
            ensure_non_empty("actor_kinds", value)?;
            ActorKind::parse(value).map_err(|_| EventFilterError::InvalidActorKind {
                value: value.clone(),
            })?;
        }
        for value in &self.correlation_ids {
            ensure_non_empty("correlation_ids", value)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventFilterError {
    DuplicateValue { field: &'static str, value: String },
    EmptyString { field: &'static str },
    InvalidActorKind { value: String },
}

impl fmt::Display for EventFilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateValue { field, value } => {
                write!(f, "filter.{field} contains duplicate '{value}'")
            }
            Self::EmptyString { field } => {
                write!(f, "filter.{field} contains an empty string")
            }
            Self::InvalidActorKind { value } => {
                write!(
                    f,
                    "filter.actor_kinds contains invalid actor kind '{value}'"
                )
            }
        }
    }
}

impl std::error::Error for EventFilterError {}

fn matches_optional<T: PartialEq>(filter_values: &[T], event_value: Option<T>) -> bool {
    filter_values.is_empty() || event_value.is_some_and(|value| filter_values.contains(&value))
}

fn matches_any<T: PartialEq>(filter_values: &[T], event_values: &[T]) -> bool {
    filter_values.is_empty()
        || event_values
            .iter()
            .any(|event_value| filter_values.contains(event_value))
}

fn matches_str(filter_values: &[String], event_value: &str) -> bool {
    filter_values.is_empty() || filter_values.iter().any(|value| value == event_value)
}

fn matches_optional_str(filter_values: &[String], event_value: Option<&str>) -> bool {
    filter_values.is_empty()
        || event_value
            .is_some_and(|event_value| filter_values.iter().any(|value| value == event_value))
}

fn matches_optional_uuid(filter_values: &[Uuid], event_value: Option<Uuid>) -> bool {
    filter_values.is_empty() || event_value.is_some_and(|value| filter_values.contains(&value))
}

fn ensure_non_empty(field: &'static str, value: &str) -> Result<(), EventFilterError> {
    if value.trim().is_empty() {
        return Err(EventFilterError::EmptyString { field });
    }
    Ok(())
}

fn ensure_unique<T>(field: &'static str, values: &[T]) -> Result<(), EventFilterError>
where
    T: Copy + Eq + std::hash::Hash + fmt::Debug,
{
    let mut seen = std::collections::HashSet::new();
    for value in values {
        if !seen.insert(*value) {
            return Err(EventFilterError::DuplicateValue {
                field,
                value: format!("{value:?}"),
            });
        }
    }
    Ok(())
}

fn ensure_unique_str(field: &'static str, values: &[String]) -> Result<(), EventFilterError> {
    let mut seen = std::collections::HashSet::new();
    for value in values {
        if !seen.insert(value.as_str()) {
            return Err(EventFilterError::DuplicateValue {
                field,
                value: value.clone(),
            });
        }
    }
    Ok(())
}

fn ensure_unique_uuid(field: &'static str, values: &[Uuid]) -> Result<(), EventFilterError> {
    let mut seen = std::collections::HashSet::new();
    for value in values {
        if !seen.insert(*value) {
            return Err(EventFilterError::DuplicateValue {
                field,
                value: value.to_string(),
            });
        }
    }
    Ok(())
}

pub fn resolve_event_sink_secret(secret_ref: &str) -> Result<String, EventSinkSecretError> {
    let key = format!(
        "HUBUUM_EVENT_SINK_SECRET_{}",
        secret_ref.to_ascii_uppercase()
    );
    std::env::var(&key).map_err(|_| EventSinkSecretError::MissingSecret {
        secret_ref: secret_ref.to_string(),
    })
}

pub fn resolve_event_sink_secret_uri(
    uri: &str,
    secret_ref: Option<&str>,
    sink_label: &str,
) -> Result<String, EventSinkSecretError> {
    let contains_secret_placeholder = uri.contains("{secret}");
    match secret_ref {
        Some(secret_ref) => {
            if !contains_secret_placeholder {
                return Err(EventSinkSecretError::MissingSecretPlaceholder {
                    sink_label: sink_label.to_string(),
                });
            }
            let secret = resolve_event_sink_secret(secret_ref)?;
            let encoded = utf8_percent_encode(&secret, NON_ALPHANUMERIC).to_string();
            Ok(uri.replace("{secret}", &encoded))
        }
        None if contains_secret_placeholder => {
            Err(EventSinkSecretError::UnexpectedSecretPlaceholder {
                sink_label: sink_label.to_string(),
            })
        }
        None => Ok(uri.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventSinkSecretError {
    MissingSecret { secret_ref: String },
    MissingSecretPlaceholder { sink_label: String },
    UnexpectedSecretPlaceholder { sink_label: String },
}

impl fmt::Display for EventSinkSecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSecret { secret_ref } => write!(
                f,
                "Event sink secret reference '{secret_ref}' is not configured"
            ),
            Self::MissingSecretPlaceholder { sink_label } => write!(
                f,
                "Invalid {sink_label} config: uri must include {{secret}} when secret_ref is set"
            ),
            Self::UnexpectedSecretPlaceholder { sink_label } => write!(
                f,
                "Invalid {sink_label} config: uri includes {{secret}} without secret_ref"
            ),
        }
    }
}

impl std::error::Error for EventSinkSecretError {}

/// Validates that `action` is legal for `entity_type`.
pub fn is_valid_pair(entity_type: EntityType, action: Action) -> bool {
    valid_actions(entity_type).contains(&action)
}

/// Canonical, client-deduplicable identity for an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
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
    fn from(event_id: EventId) -> Self {
        event_id.0
    }
}

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
            .field("before", &self.before.as_ref().map(|_| "<redacted>"))
            .field("after", &self.after.as_ref().map(|_| "<redacted>"))
            .field("metadata", &"<redacted>")
            .field("schema_version", &self.schema_version)
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
            summary: summary.into(),
            before: None,
            after: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            schema_version: 1,
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
        self.correlation_id = context.correlation_id().map(ToOwned::to_owned);
        self
    }

    #[must_use]
    pub fn with_mutation_provenance(mut self, provenance: &MutationProvenance) -> Self {
        self.actor_kind = provenance.actor_kind();
        self.actor_user_id = provenance.actor_user_id();
        self.initiator_user_id = provenance.initiator_user_id();
        self.task_id = provenance.task_id();
        self
    }

    #[must_use]
    pub fn with_request_id(mut self, request_id: Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    #[must_use]
    pub fn with_before(mut self, before: serde_json::Value) -> Self {
        self.before = Some(before);
        self
    }

    #[must_use]
    pub fn with_before_opt(mut self, before: Option<serde_json::Value>) -> Self {
        self.before = before;
        self
    }

    #[must_use]
    pub fn with_after(mut self, after: serde_json::Value) -> Self {
        self.after = Some(after);
        self
    }

    #[must_use]
    pub fn with_after_opt(mut self, after: Option<serde_json::Value>) -> Self {
        self.after = after;
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
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
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    #[must_use]
    pub fn summary(&self) -> &str {
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

/// Redact credentials from persisted sink configuration and routing values.
#[must_use]
pub fn redact_event_sink_config(config: &serde_json::Value) -> serde_json::Value {
    match config {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let redacted = if is_sensitive_config_key(key) {
                        serde_json::Value::String("[redacted]".to_string())
                    } else if key.eq_ignore_ascii_case("uri") || key.eq_ignore_ascii_case("url") {
                        redact_uri_value(value)
                    } else {
                        redact_event_sink_config(value)
                    };
                    (key.clone(), redacted)
                })
                .collect(),
        ),
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(redact_event_sink_config)
                .collect::<Vec<_>>(),
        ),
        value => value.clone(),
    }
}

fn is_sensitive_config_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    let compact = lower
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    if [
        "password",
        "passwd",
        "token",
        "secret",
        "authorization",
        "auth",
        "credential",
        "credentials",
        "apikey",
        "privatekey",
        "accesskey",
    ]
    .iter()
    .any(|suffix| compact.ends_with(suffix))
    {
        return true;
    }

    let mut previous = None;
    for segment in lower
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
    {
        if matches!(
            segment,
            "password"
                | "passwd"
                | "token"
                | "secret"
                | "authorization"
                | "credential"
                | "credentials"
                | "apikey"
        ) || (segment == "key" && matches!(previous, Some("api" | "private" | "access")))
        {
            return true;
        }
        previous = Some(segment);
    }
    false
}

fn redact_uri_value(value: &serde_json::Value) -> serde_json::Value {
    let Some(uri) = value.as_str() else {
        return redact_event_sink_config(value);
    };
    serde_json::Value::String(redact_uri_userinfo(uri))
}

fn redact_uri_userinfo(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_string();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some((_, host)) = authority.rsplit_once('@') else {
        return uri.to_string();
    };
    format!("{scheme}://[redacted]@{host}{}", &rest[authority_end..])
}

/// Catalog-level validation errors. Callers map these into their public error
/// surface (e.g. Hubuum's `ApiError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventCatalogError {
    UnknownEntityType(String),
    UnknownAction(String),
    InvalidActionForType {
        entity_type: EntityType,
        action: Action,
    },
    UnknownActorKind(String),
}

impl fmt::Display for EventCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownEntityType(value) => {
                write!(f, "unknown event entity_type '{value}'")
            }
            Self::UnknownAction(value) => write!(f, "unknown event action '{value}'"),
            Self::InvalidActionForType {
                entity_type,
                action,
            } => write!(
                f,
                "action '{}' is not valid for entity_type '{}'",
                action.as_str(),
                entity_type.as_str()
            ),
            Self::UnknownActorKind(value) => write!(f, "unknown event actor_kind '{value}'"),
        }
    }
}

impl std::error::Error for EventCatalogError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_type_round_trips() {
        let all = [
            EntityType::Collection,
            EntityType::Class,
            EntityType::Object,
            EntityType::ClassRelation,
            EntityType::ObjectRelation,
            EntityType::User,
            EntityType::Group,
            EntityType::UserGroup,
            EntityType::Permission,
            EntityType::Token,
            EntityType::RemoteTarget,
            EntityType::ExportTemplate,
            EntityType::Task,
            EntityType::ServiceAccount,
            EntityType::EventSink,
            EntityType::EventSubscription,
            EntityType::ExternalIdentitySync,
            EntityType::Restore,
        ];
        for t in all {
            assert_eq!(EntityType::parse(t.as_str()).unwrap(), t);
        }
        assert!(EntityType::parse("hubuumclass").is_err());
    }

    #[test]
    fn action_round_trips() {
        let all = [
            Action::Created,
            Action::Updated,
            Action::Deleted,
            Action::Added,
            Action::Removed,
            Action::Granted,
            Action::Revoked,
            Action::Purged,
            Action::Invoked,
            Action::Queued,
            Action::Started,
            Action::Validating,
            Action::Running,
            Action::Succeeded,
            Action::Failed,
            Action::PartiallySucceeded,
            Action::Cancelled,
            Action::Cleanup,
            Action::Disabled,
        ];
        for a in all {
            assert_eq!(Action::parse(a.as_str()).unwrap(), a);
        }
        assert!(Action::parse("patched").is_err());
    }

    #[test]
    fn relations_have_no_updated() {
        assert!(is_valid_pair(EntityType::ObjectRelation, Action::Created));
        assert!(is_valid_pair(EntityType::ObjectRelation, Action::Deleted));
        assert!(!is_valid_pair(EntityType::ObjectRelation, Action::Updated));
        assert!(!is_valid_pair(EntityType::ClassRelation, Action::Updated));
    }

    #[test]
    fn permission_is_grant_revoke() {
        assert!(is_valid_pair(EntityType::Permission, Action::Granted));
        assert!(is_valid_pair(EntityType::Permission, Action::Revoked));
        assert!(!is_valid_pair(EntityType::Permission, Action::Created));
        assert!(!is_valid_pair(EntityType::Permission, Action::Updated));
    }

    #[test]
    fn token_has_lifecycle_actions_but_no_updated_or_deleted() {
        assert!(is_valid_pair(EntityType::Token, Action::Created));
        assert!(is_valid_pair(EntityType::Token, Action::Revoked));
        assert!(is_valid_pair(EntityType::Token, Action::Purged));
        assert!(!is_valid_pair(EntityType::Token, Action::Updated));
        assert!(!is_valid_pair(EntityType::Token, Action::Deleted));
    }

    #[test]
    fn remote_target_has_invoked() {
        assert!(is_valid_pair(EntityType::RemoteTarget, Action::Invoked));
        assert!(!is_valid_pair(EntityType::Object, Action::Invoked));
    }

    #[test]
    fn user_group_is_add_remove() {
        assert!(is_valid_pair(EntityType::UserGroup, Action::Added));
        assert!(is_valid_pair(EntityType::UserGroup, Action::Removed));
        assert!(!is_valid_pair(EntityType::UserGroup, Action::Created));
    }

    #[test]
    fn task_is_lifecycle_only() {
        assert!(is_valid_pair(EntityType::Task, Action::Queued));
        assert!(is_valid_pair(EntityType::Task, Action::Succeeded));
        assert!(is_valid_pair(EntityType::Task, Action::Cleanup));
        assert!(!is_valid_pair(EntityType::Task, Action::Created));
        assert!(!is_valid_pair(EntityType::Task, Action::Updated));
    }

    #[test]
    fn restore_records_success_only() {
        assert_eq!(
            (
                is_valid_pair(EntityType::Restore, Action::Succeeded),
                is_valid_pair(EntityType::Restore, Action::Failed),
                is_valid_pair(EntityType::Restore, Action::Created),
            ),
            (true, false, false)
        );
    }

    #[test]
    fn actor_kind_round_trips() {
        for k in [ActorKind::User, ActorKind::System, ActorKind::Worker] {
            assert_eq!(ActorKind::parse(k.as_str()).unwrap(), k);
        }
        assert!(ActorKind::parse("anonymous").is_err());
    }

    #[test]
    fn empty_subscription_filter_matches_any_event() {
        assert!(EventSubscriptionFilter::default().matches(&envelope()));
    }

    #[test]
    fn subscription_filter_matches_selected_dimensions() {
        let request_id = Uuid::new_v4();
        let event = envelope_builder()
            .request_id(Some(request_id))
            .try_build()
            .unwrap();
        let filter = EventSubscriptionFilter {
            collection_ids: vec![CollectionId::new(10).unwrap()],
            related_collection_ids: vec![CollectionId::new(20).unwrap()],
            entity_ids: vec![EventEntityId::new(30).unwrap()],
            entity_names: vec!["test entity".to_string()],
            actor_kinds: vec!["user".to_string()],
            actor_user_ids: vec![PrincipalId::new(40).unwrap()],
            initiator_user_ids: vec![],
            request_ids: vec![request_id],
            correlation_ids: vec!["correlation".to_string()],
        };

        assert!(filter.matches(&event));
    }

    #[test]
    fn subscription_filter_rejects_non_matching_dimension() {
        let filter = EventSubscriptionFilter {
            actor_user_ids: vec![PrincipalId::new(999).unwrap()],
            ..EventSubscriptionFilter::default()
        };

        assert!(!filter.matches(&envelope()));
    }

    #[test]
    fn subscription_filter_matches_task_initiator() {
        let event = envelope_builder()
            .provenance(Provenance {
                initiator: Some(ProvenancePrincipal {
                    principal_id: PrincipalId::new(77).unwrap(),
                    name: Some("submitter".to_string()),
                }),
                ..Provenance::default()
            })
            .try_build()
            .unwrap();
        let filter = EventSubscriptionFilter {
            initiator_user_ids: vec![PrincipalId::new(77).unwrap()],
            ..EventSubscriptionFilter::default()
        };

        assert!(filter.matches(&event));
    }

    #[test]
    fn subscription_filter_validates_values() {
        let filter = EventSubscriptionFilter {
            actor_kinds: vec!["anonymous".to_string()],
            ..EventSubscriptionFilter::default()
        };

        assert!(matches!(
            filter.validate(),
            Err(EventFilterError::InvalidActorKind { .. })
        ));

        let filter = EventSubscriptionFilter {
            collection_ids: vec![
                CollectionId::new(10).unwrap(),
                CollectionId::new(10).unwrap(),
            ],
            ..EventSubscriptionFilter::default()
        };

        assert!(matches!(
            filter.validate(),
            Err(EventFilterError::DuplicateValue { field, .. }) if field == "collection_ids"
        ));
    }

    fn envelope_builder() -> EventEnvelopeBuilder {
        EventEnvelope::builder()
            .id(EventSequence::new(1).unwrap())
            .event_id(Uuid::new_v4())
            .occurred_at(
                chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc(),
            )
            .entity_type(EntityType::Collection)
            .entity_id(Some(EventEntityId::new(30).unwrap()))
            .entity_name(Some("test entity".to_string()))
            .collection_id(Some(CollectionId::new(10).unwrap()))
            .action(Action::Created)
            .actor_user_id(Some(PrincipalId::new(40).unwrap()))
            .actor_kind(ActorKind::User)
            .correlation_id(Some("correlation".to_string()))
            .summary("summary".to_string())
            .metadata(serde_json::json!({"related_collection_ids": [20, "21"]}))
            .schema_version(1)
    }

    fn envelope() -> EventEnvelope {
        envelope_builder().try_build().unwrap()
    }

    #[test]
    fn event_envelope_debug_redacts_payload_snapshots() {
        let event = envelope_builder()
            .before(Some(serde_json::json!({"token": "before-secret"})))
            .after(Some(serde_json::json!({"token": "after-secret"})))
            .metadata(serde_json::json!({"token": "metadata-secret"}))
            .try_build()
            .unwrap();

        let debug = format!("{event:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("before-secret"));
        assert!(!debug.contains("after-secret"));
        assert!(!debug.contains("metadata-secret"));
    }

    #[test]
    fn event_envelope_rejects_invalid_catalog_and_payload_values() {
        let invalid_pair = envelope_builder()
            .entity_type(EntityType::ObjectRelation)
            .action(Action::Updated)
            .try_build();
        assert!(invalid_pair.is_err());

        assert!(
            envelope_builder()
                .metadata(serde_json::json!([]))
                .try_build()
                .is_err()
        );
        assert!(
            envelope_builder()
                .before(Some(serde_json::json!("not an object")))
                .try_build()
                .is_err()
        );
        assert!(envelope_builder().schema_version(0).try_build().is_err());
    }

    #[test]
    fn event_envelope_rejects_invalid_or_mismatched_provenance_actor_kind() {
        let invalid = envelope_builder()
            .provenance(Provenance {
                actor: ProvenanceActor {
                    kind: Some("anonymous".to_string()),
                    principal: None,
                },
                ..Provenance::default()
            })
            .try_build();
        assert!(invalid.is_err());

        let mismatched = envelope_builder()
            .provenance(Provenance {
                actor: ProvenanceActor {
                    kind: Some(ActorKind::Worker.as_str().to_string()),
                    principal: None,
                },
                ..Provenance::default()
            })
            .try_build();
        assert!(mismatched.is_err());

        let mismatched_principal = envelope_builder()
            .provenance(Provenance {
                actor: ProvenanceActor {
                    kind: Some(ActorKind::User.as_str().to_string()),
                    principal: Some(ProvenancePrincipal {
                        principal_id: PrincipalId::new(41).unwrap(),
                        name: None,
                    }),
                },
                ..Provenance::default()
            })
            .try_build();
        assert!(mismatched_principal.is_err());
    }

    #[test]
    fn event_envelope_canonicalizes_missing_actor_provenance() {
        let event = envelope();

        assert_eq!(event.provenance().actor.kind.as_deref(), Some("user"));
        assert_eq!(
            event
                .provenance()
                .actor
                .principal
                .as_ref()
                .map(|principal| principal.principal_id),
            event.actor_user_id()
        );
    }

    #[test]
    fn event_envelope_serialization_preserves_naive_utc_wire_format() {
        let event = envelope();
        let value = serde_json::to_value(&event).unwrap();

        assert_eq!(value["occurred_at"], "2026-01-01T00:00:00");
        let decoded: EventEnvelope = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.occurred_at(), event.occurred_at());
    }

    #[test]
    fn new_event_validates_catalog_pairs_and_redacts_debug_payloads() {
        assert!(matches!(
            NewEvent::new(
                EntityType::ObjectRelation,
                Action::Updated,
                ActorKind::System,
                "invalid"
            ),
            Err(EventCatalogError::InvalidActionForType { .. })
        ));
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

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("before-secret"));
        assert!(!debug.contains("after-secret"));
        assert!(!debug.contains("metadata-secret"));
    }

    #[test]
    fn sink_redaction_covers_nested_keys_and_uri_userinfo() {
        let redacted = redact_event_sink_config(&serde_json::json!({
            "url": "https://user:password@example.invalid/events",
            "headers": {"X-API-Key": "secret", "routing_key": "visible"}
        }));

        assert_eq!(redacted["url"], "https://[redacted]@example.invalid/events");
        assert_eq!(redacted["headers"]["X-API-Key"], "[redacted]");
        assert_eq!(redacted["headers"]["routing_key"], "visible");
    }
}
