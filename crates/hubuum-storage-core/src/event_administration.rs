use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    CollectionId, EventDeliveryId, EventDeliveryStatus, EventSinkId, EventSubscriptionId,
    PrincipalId, ResourceRevision,
};
use hubuum_events_core::{
    Action, ActorKind, EntityType, EventContext, EventEntityId, EventSequence,
    EventSubscriptionFilter, is_valid_pair,
};
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{MutationOutcome, StorageError, StoragePage};

fn validate_non_empty(field: &str, value: &str) -> Result<(), StorageError> {
    if value.trim().is_empty() {
        return Err(StorageError::invalid_input(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_optional_non_empty(field: &str, value: Option<&str>) -> Result<(), StorageError> {
    if let Some(value) = value {
        validate_non_empty(field, value)?;
    }
    Ok(())
}

fn validate_json_object(field: &str, value: &Value) -> Result<(), StorageError> {
    if !value.is_object() {
        return Err(StorageError::invalid_input(format!(
            "{field} must be a JSON object"
        )));
    }
    Ok(())
}

fn validate_timestamps(
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    if updated_at < created_at {
        return Err(StorageError::invalid_input(
            "updated_at must not be earlier than created_at",
        ));
    }
    Ok(())
}

fn validate_catalog_values<T>(field: &str, values: &[T]) -> Result<(), StorageError>
where
    T: Copy + Eq + std::hash::Hash,
{
    if values.is_empty() {
        return Err(StorageError::invalid_input(format!(
            "{field} must include at least one value"
        )));
    }
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    if values.iter().any(|value| !seen.insert(*value)) {
        return Err(StorageError::invalid_input(format!(
            "{field} must not contain duplicates"
        )));
    }
    Ok(())
}

fn validate_subscription_parts(
    name: &str,
    entity_types: &[EntityType],
    actions: &[Action],
    filter: &EventSubscriptionFilter,
    routing: &Value,
) -> Result<(), StorageError> {
    validate_non_empty("name", name)?;
    validate_catalog_values("entity_types", entity_types)?;
    validate_catalog_values("actions", actions)?;
    for entity_type in entity_types {
        for action in actions {
            if !is_valid_pair(*entity_type, *action) {
                return Err(StorageError::invalid_input(format!(
                    "action '{}' is not valid for entity_type '{}'",
                    action.as_str(),
                    entity_type.as_str()
                )));
            }
        }
    }
    filter
        .validate()
        .map_err(|error| StorageError::invalid_input(error.to_string()))?;
    validate_json_object("routing", routing)
}

/// Typed audit filters interpreted by the selected backend.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StorageAuditEventFilters {
    entity_type: Option<EntityType>,
    entity_id: Option<EventEntityId>,
    action: Option<Action>,
    actor_kind: Option<ActorKind>,
    actor_user_id: Option<PrincipalId>,
    initiator_user_id: Option<PrincipalId>,
    collection_id: Option<CollectionId>,
    occurred_after: Option<DateTime<Utc>>,
    occurred_before: Option<DateTime<Utc>>,
}

impl StorageAuditEventFilters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entity_type: None,
            entity_id: None,
            action: None,
            actor_kind: None,
            actor_user_id: None,
            initiator_user_id: None,
            collection_id: None,
            occurred_after: None,
            occurred_before: None,
        }
    }

    #[must_use]
    pub const fn entity_type(mut self, value: Option<EntityType>) -> Self {
        self.entity_type = value;
        self
    }

    #[must_use]
    pub const fn entity_id(mut self, value: Option<EventEntityId>) -> Self {
        self.entity_id = value;
        self
    }

    #[must_use]
    pub const fn action(mut self, value: Option<Action>) -> Self {
        self.action = value;
        self
    }

    #[must_use]
    pub const fn actor_kind(mut self, value: Option<ActorKind>) -> Self {
        self.actor_kind = value;
        self
    }

    #[must_use]
    pub const fn actor_user_id(mut self, value: Option<PrincipalId>) -> Self {
        self.actor_user_id = value;
        self
    }

    #[must_use]
    pub const fn initiator_user_id(mut self, value: Option<PrincipalId>) -> Self {
        self.initiator_user_id = value;
        self
    }

    #[must_use]
    pub const fn collection_id(mut self, value: Option<CollectionId>) -> Self {
        self.collection_id = value;
        self
    }

    #[must_use]
    pub const fn occurred_after(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.occurred_after = value;
        self
    }

    #[must_use]
    pub const fn occurred_before(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.occurred_before = value;
        self
    }

    #[must_use]
    pub const fn entity_type_value(&self) -> Option<EntityType> {
        self.entity_type
    }

    #[must_use]
    pub const fn entity_id_value(&self) -> Option<EventEntityId> {
        self.entity_id
    }

    #[must_use]
    pub const fn action_value(&self) -> Option<Action> {
        self.action
    }

    #[must_use]
    pub const fn actor_kind_value(&self) -> Option<ActorKind> {
        self.actor_kind
    }

    #[must_use]
    pub const fn actor_user_id_value(&self) -> Option<PrincipalId> {
        self.actor_user_id
    }

    #[must_use]
    pub const fn initiator_user_id_value(&self) -> Option<PrincipalId> {
        self.initiator_user_id
    }

    #[must_use]
    pub const fn collection_id_value(&self) -> Option<CollectionId> {
        self.collection_id
    }

    #[must_use]
    pub const fn occurred_after_value(&self) -> Option<DateTime<Utc>> {
        self.occurred_after
    }

    #[must_use]
    pub const fn occurred_before_value(&self) -> Option<DateTime<Utc>> {
        self.occurred_before
    }
}

impl fmt::Debug for StorageAuditEventFilters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuditEventFilters")
            .field("has_entity_type", &self.entity_type.is_some())
            .field("has_entity_id", &self.entity_id.is_some())
            .field("has_action", &self.action.is_some())
            .field("has_actor_kind", &self.actor_kind.is_some())
            .field("has_actor_user_id", &self.actor_user_id.is_some())
            .field("has_initiator_user_id", &self.initiator_user_id.is_some())
            .field("has_collection_id", &self.collection_id.is_some())
            .field("has_occurred_after", &self.occurred_after.is_some())
            .field("has_occurred_before", &self.occurred_before.is_some())
            .finish()
    }
}

/// Visibility-scoped, cursor-paginated audit query.
#[derive(Clone, PartialEq)]
pub struct StorageAuditEventListQuery {
    accessible_collection_ids: Vec<CollectionId>,
    include_collection_less: bool,
    filters: StorageAuditEventFilters,
    options: QueryOptions,
}

impl StorageAuditEventListQuery {
    #[must_use]
    pub fn new(
        mut accessible_collection_ids: Vec<CollectionId>,
        include_collection_less: bool,
        filters: StorageAuditEventFilters,
        options: QueryOptions,
    ) -> Self {
        accessible_collection_ids.sort_unstable();
        accessible_collection_ids.dedup();
        Self {
            accessible_collection_ids,
            include_collection_less,
            filters,
            options,
        }
    }

    #[must_use]
    pub fn accessible_collection_ids(&self) -> &[CollectionId] {
        &self.accessible_collection_ids
    }

    #[must_use]
    pub const fn include_collection_less(&self) -> bool {
        self.include_collection_less
    }

    #[must_use]
    pub const fn filters(&self) -> &StorageAuditEventFilters {
        &self.filters
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }
}

impl fmt::Debug for StorageAuditEventListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuditEventListQuery")
            .field(
                "accessible_collection_count",
                &self.accessible_collection_ids.len(),
            )
            .field("include_collection_less", &self.include_collection_less)
            .field("filters", &self.filters)
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Audit-facing name for the shared committed-event boundary DTO.
pub type StorageAuditEvent = crate::events::StorageRecordedEvent;

/// Read-only audit stream behavior required from every selectable backend.
#[async_trait]
pub trait AuditEventStorage: Send + Sync {
    /// Apply visibility before filtering, counting, and paging; enrich durable
    /// provenance; redact indirectly visible payloads; and return an optional
    /// exact total from the same predicate as the page.
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StoragePage<StorageAuditEvent>, StorageError>;
}

/// Backend-neutral persisted event sink.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSink {
    id: EventSinkId,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageEventSink {
    #[must_use]
    pub fn builder(
        id: EventSinkId,
        name: impl Into<String>,
        kind: impl Into<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        revision: ResourceRevision,
    ) -> StorageEventSinkBuilder {
        StorageEventSinkBuilder {
            id,
            name: name.into(),
            kind: kind.into(),
            configuration: serde_json::json!({}),
            secret_ref: None,
            enabled: false,
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn id(&self) -> EventSinkId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub const fn configuration(&self) -> &Value {
        &self.configuration
    }

    #[must_use]
    pub fn secret_ref(&self) -> Option<&str> {
        self.secret_ref.as_deref()
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

/// Builder for persisted event-sink projections.
pub struct StorageEventSinkBuilder {
    id: EventSinkId,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageEventSinkBuilder {
    #[must_use]
    pub fn configuration(mut self, value: Value) -> Self {
        self.configuration = value;
        self
    }

    #[must_use]
    pub fn secret_ref(mut self, value: Option<String>) -> Self {
        self.secret_ref = value;
        self
    }

    #[must_use]
    pub const fn enabled(mut self, value: bool) -> Self {
        self.enabled = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventSink, StorageError> {
        validate_non_empty("name", &self.name)?;
        validate_non_empty("kind", &self.kind)?;
        validate_json_object("configuration", &self.configuration)?;
        validate_optional_non_empty("secret_ref", self.secret_ref.as_deref())?;
        validate_timestamps(self.created_at, self.updated_at)?;
        Ok(StorageEventSink {
            id: self.id,
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            secret_ref: self.secret_ref,
            enabled: self.enabled,
            created_at: self.created_at,
            updated_at: self.updated_at,
            revision: self.revision,
        })
    }
}

/// Cursor-paginated event sink query.
#[derive(Clone, PartialEq)]
pub struct StorageEventSinkListQuery {
    options: QueryOptions,
}

impl StorageEventSinkListQuery {
    #[must_use]
    pub const fn new(options: QueryOptions) -> Self {
        Self { options }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }
}

impl fmt::Debug for StorageEventSinkListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSinkListQuery")
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Validated event sink creation request.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSinkCreate {
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSinkCreate {
    #[must_use]
    pub fn builder(
        name: impl Into<String>,
        kind: impl Into<String>,
        event_context: EventContext,
    ) -> StorageEventSinkCreateBuilder {
        StorageEventSinkCreateBuilder {
            name: name.into(),
            kind: kind.into(),
            configuration: serde_json::json!({}),
            secret_ref: None,
            enabled: false,
            event_context,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub const fn configuration(&self) -> &Value {
        &self.configuration
    }

    #[must_use]
    pub fn secret_ref(&self) -> Option<&str> {
        self.secret_ref.as_deref()
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

/// Builder for validated event-sink creation requests.
pub struct StorageEventSinkCreateBuilder {
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSinkCreateBuilder {
    #[must_use]
    pub fn configuration(mut self, value: Value) -> Self {
        self.configuration = value;
        self
    }

    #[must_use]
    pub fn secret_ref(mut self, value: Option<String>) -> Self {
        self.secret_ref = value;
        self
    }

    #[must_use]
    pub const fn enabled(mut self, value: bool) -> Self {
        self.enabled = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventSinkCreate, StorageError> {
        validate_non_empty("name", &self.name)?;
        validate_non_empty("kind", &self.kind)?;
        validate_json_object("configuration", &self.configuration)?;
        validate_optional_non_empty("secret_ref", self.secret_ref.as_deref())?;
        Ok(StorageEventSinkCreate {
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            secret_ref: self.secret_ref,
            enabled: self.enabled,
            event_context: self.event_context,
        })
    }
}

impl fmt::Debug for StorageEventSinkCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSinkCreate")
            .field("name", &"<redacted>")
            .field("kind", &self.kind)
            .field("configuration", &"<redacted>")
            .field("has_secret_ref", &self.secret_ref.is_some())
            .field("enabled", &self.enabled)
            .finish()
    }
}

/// Validated event sink patch.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSinkUpdate {
    id: EventSinkId,
    name: Option<String>,
    kind: Option<String>,
    configuration: Option<Value>,
    secret_ref: Option<Option<String>>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSinkUpdate {
    #[must_use]
    pub fn builder(id: EventSinkId, event_context: EventContext) -> StorageEventSinkUpdateBuilder {
        StorageEventSinkUpdateBuilder {
            id,
            name: None,
            kind: None,
            configuration: None,
            secret_ref: None,
            enabled: None,
            event_context,
        }
    }

    #[must_use]
    pub const fn id(&self) -> EventSinkId {
        self.id
    }

    #[must_use]
    pub fn name_value(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn kind_value(&self) -> Option<&str> {
        self.kind.as_deref()
    }

    #[must_use]
    pub const fn configuration_value(&self) -> Option<&Value> {
        self.configuration.as_ref()
    }

    #[must_use]
    pub fn secret_ref_value(&self) -> Option<Option<&str>> {
        self.secret_ref.as_ref().map(|value| value.as_deref())
    }

    #[must_use]
    pub const fn enabled_value(&self) -> Option<bool> {
        self.enabled
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

/// Builder for validated event-sink patches.
pub struct StorageEventSinkUpdateBuilder {
    id: EventSinkId,
    name: Option<String>,
    kind: Option<String>,
    configuration: Option<Value>,
    secret_ref: Option<Option<String>>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSinkUpdateBuilder {
    #[must_use]
    pub fn name(mut self, value: Option<String>) -> Self {
        self.name = value;
        self
    }

    #[must_use]
    pub fn kind(mut self, value: Option<String>) -> Self {
        self.kind = value;
        self
    }

    #[must_use]
    pub fn configuration(mut self, value: Option<Value>) -> Self {
        self.configuration = value;
        self
    }

    #[must_use]
    pub fn secret_ref(mut self, value: Option<Option<String>>) -> Self {
        self.secret_ref = value;
        self
    }

    #[must_use]
    pub const fn enabled(mut self, value: Option<bool>) -> Self {
        self.enabled = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventSinkUpdate, StorageError> {
        validate_optional_non_empty("name", self.name.as_deref())?;
        validate_optional_non_empty("kind", self.kind.as_deref())?;
        if let Some(configuration) = &self.configuration {
            validate_json_object("configuration", configuration)?;
        }
        if let Some(secret_ref) = &self.secret_ref {
            validate_optional_non_empty("secret_ref", secret_ref.as_deref())?;
        }
        Ok(StorageEventSinkUpdate {
            id: self.id,
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            secret_ref: self.secret_ref,
            enabled: self.enabled,
            event_context: self.event_context,
        })
    }
}

impl fmt::Debug for StorageEventSinkUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSinkUpdate")
            .field("id", &"<redacted>")
            .field("has_name", &self.name.is_some())
            .field("has_kind", &self.kind.is_some())
            .field("has_configuration", &self.configuration.is_some())
            .field("has_secret_ref_change", &self.secret_ref.is_some())
            .field("has_enabled", &self.enabled.is_some())
            .finish()
    }
}

/// Event sink deletion request with mandatory audit context.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSinkDelete {
    id: EventSinkId,
    event_context: EventContext,
}

impl StorageEventSinkDelete {
    #[must_use]
    pub const fn new(id: EventSinkId, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub const fn id(&self) -> EventSinkId {
        self.id
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

impl fmt::Debug for StorageEventSinkDelete {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSinkDelete")
            .field("id", &"<redacted>")
            .finish()
    }
}

/// Backend-neutral persisted event subscription.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSubscription {
    id: EventSubscriptionId,
    collection_id: CollectionId,
    sink_id: EventSinkId,
    name: String,
    description: String,
    entity_types: Vec<EntityType>,
    actions: Vec<Action>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageEventSubscription {
    #[must_use]
    pub fn builder(
        id: EventSubscriptionId,
        collection_id: CollectionId,
        sink_id: EventSinkId,
        name: impl Into<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        revision: ResourceRevision,
    ) -> StorageEventSubscriptionBuilder {
        StorageEventSubscriptionBuilder {
            id,
            collection_id,
            sink_id,
            name: name.into(),
            description: String::new(),
            entity_types: Vec::new(),
            actions: Vec::new(),
            filter: EventSubscriptionFilter::default(),
            routing: serde_json::json!({}),
            enabled: false,
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn id(&self) -> EventSubscriptionId {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn sink_id(&self) -> EventSinkId {
        self.sink_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub fn entity_types(&self) -> &[EntityType] {
        &self.entity_types
    }

    #[must_use]
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    #[must_use]
    pub const fn filter(&self) -> &EventSubscriptionFilter {
        &self.filter
    }

    #[must_use]
    pub const fn routing(&self) -> &Value {
        &self.routing
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

/// Builder for persisted event subscription projections.
pub struct StorageEventSubscriptionBuilder {
    id: EventSubscriptionId,
    collection_id: CollectionId,
    sink_id: EventSinkId,
    name: String,
    description: String,
    entity_types: Vec<EntityType>,
    actions: Vec<Action>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageEventSubscriptionBuilder {
    #[must_use]
    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.description = value.into();
        self
    }

    #[must_use]
    pub fn entity_types(mut self, value: Vec<EntityType>) -> Self {
        self.entity_types = value;
        self
    }

    #[must_use]
    pub fn actions(mut self, value: Vec<Action>) -> Self {
        self.actions = value;
        self
    }

    #[must_use]
    pub fn filter(mut self, value: EventSubscriptionFilter) -> Self {
        self.filter = value;
        self
    }

    #[must_use]
    pub fn routing(mut self, value: Value) -> Self {
        self.routing = value;
        self
    }

    #[must_use]
    pub const fn enabled(mut self, value: bool) -> Self {
        self.enabled = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventSubscription, StorageError> {
        validate_subscription_parts(
            &self.name,
            &self.entity_types,
            &self.actions,
            &self.filter,
            &self.routing,
        )?;
        validate_timestamps(self.created_at, self.updated_at)?;
        Ok(StorageEventSubscription {
            id: self.id,
            collection_id: self.collection_id,
            sink_id: self.sink_id,
            name: self.name,
            description: self.description,
            entity_types: self.entity_types,
            actions: self.actions,
            filter: self.filter,
            routing: self.routing,
            enabled: self.enabled,
            created_at: self.created_at,
            updated_at: self.updated_at,
            revision: self.revision,
        })
    }
}

/// Collection-scoped event subscription list query.
#[derive(Clone, PartialEq)]
pub struct StorageEventSubscriptionListQuery {
    collection_id: CollectionId,
    options: QueryOptions,
}

impl StorageEventSubscriptionListQuery {
    #[must_use]
    pub const fn new(collection_id: CollectionId, options: QueryOptions) -> Self {
        Self {
            collection_id,
            options,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }
}

impl fmt::Debug for StorageEventSubscriptionListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSubscriptionListQuery")
            .field("collection_id", &"<redacted>")
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Validated event subscription creation request.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSubscriptionCreate {
    collection_id: CollectionId,
    sink_id: EventSinkId,
    name: String,
    description: String,
    entity_types: Vec<EntityType>,
    actions: Vec<Action>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSubscriptionCreate {
    #[must_use]
    pub fn builder(
        collection_id: CollectionId,
        sink_id: EventSinkId,
        name: impl Into<String>,
        event_context: EventContext,
    ) -> StorageEventSubscriptionCreateBuilder {
        StorageEventSubscriptionCreateBuilder {
            collection_id,
            sink_id,
            name: name.into(),
            description: String::new(),
            entity_types: Vec::new(),
            actions: Vec::new(),
            filter: EventSubscriptionFilter::default(),
            routing: serde_json::json!({}),
            enabled: false,
            event_context,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn sink_id(&self) -> EventSinkId {
        self.sink_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub fn entity_types(&self) -> &[EntityType] {
        &self.entity_types
    }

    #[must_use]
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    #[must_use]
    pub const fn filter(&self) -> &EventSubscriptionFilter {
        &self.filter
    }

    #[must_use]
    pub const fn routing(&self) -> &Value {
        &self.routing
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

impl fmt::Debug for StorageEventSubscriptionCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSubscriptionCreate")
            .field("collection_id", &"<redacted>")
            .field("sink_id", &"<redacted>")
            .field("name", &"<redacted>")
            .field("description", &"<redacted>")
            .field("entity_type_count", &self.entity_types.len())
            .field("action_count", &self.actions.len())
            .field("filter", &"<redacted>")
            .field("routing", &"<redacted>")
            .field("enabled", &self.enabled)
            .finish()
    }
}

/// Builder for event subscription creation requests.
pub struct StorageEventSubscriptionCreateBuilder {
    collection_id: CollectionId,
    sink_id: EventSinkId,
    name: String,
    description: String,
    entity_types: Vec<EntityType>,
    actions: Vec<Action>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSubscriptionCreateBuilder {
    #[must_use]
    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.description = value.into();
        self
    }

    #[must_use]
    pub fn entity_types(mut self, value: Vec<EntityType>) -> Self {
        self.entity_types = value;
        self
    }

    #[must_use]
    pub fn actions(mut self, value: Vec<Action>) -> Self {
        self.actions = value;
        self
    }

    #[must_use]
    pub fn filter(mut self, value: EventSubscriptionFilter) -> Self {
        self.filter = value;
        self
    }

    #[must_use]
    pub fn routing(mut self, value: Value) -> Self {
        self.routing = value;
        self
    }

    #[must_use]
    pub const fn enabled(mut self, value: bool) -> Self {
        self.enabled = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventSubscriptionCreate, StorageError> {
        validate_subscription_parts(
            &self.name,
            &self.entity_types,
            &self.actions,
            &self.filter,
            &self.routing,
        )?;
        Ok(StorageEventSubscriptionCreate {
            collection_id: self.collection_id,
            sink_id: self.sink_id,
            name: self.name,
            description: self.description,
            entity_types: self.entity_types,
            actions: self.actions,
            filter: self.filter,
            routing: self.routing,
            enabled: self.enabled,
            event_context: self.event_context,
        })
    }
}

/// Validated collection-scoped event subscription patch.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSubscriptionUpdate {
    collection_id: CollectionId,
    id: EventSubscriptionId,
    sink_id: Option<EventSinkId>,
    name: Option<String>,
    description: Option<String>,
    entity_types: Option<Vec<EntityType>>,
    actions: Option<Vec<Action>>,
    filter: Option<EventSubscriptionFilter>,
    routing: Option<Value>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSubscriptionUpdate {
    #[must_use]
    pub fn builder(
        collection_id: CollectionId,
        id: EventSubscriptionId,
        event_context: EventContext,
    ) -> StorageEventSubscriptionUpdateBuilder {
        StorageEventSubscriptionUpdateBuilder {
            collection_id,
            id,
            sink_id: None,
            name: None,
            description: None,
            entity_types: None,
            actions: None,
            filter: None,
            routing: None,
            enabled: None,
            event_context,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn id(&self) -> EventSubscriptionId {
        self.id
    }

    #[must_use]
    pub const fn sink_id_value(&self) -> Option<EventSinkId> {
        self.sink_id
    }

    #[must_use]
    pub fn name_value(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn description_value(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub fn entity_types_value(&self) -> Option<&[EntityType]> {
        self.entity_types.as_deref()
    }

    #[must_use]
    pub fn actions_value(&self) -> Option<&[Action]> {
        self.actions.as_deref()
    }

    #[must_use]
    pub const fn filter_value(&self) -> Option<&EventSubscriptionFilter> {
        self.filter.as_ref()
    }

    #[must_use]
    pub const fn routing_value(&self) -> Option<&Value> {
        self.routing.as_ref()
    }

    #[must_use]
    pub const fn enabled_value(&self) -> Option<bool> {
        self.enabled
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

/// Builder for validated collection-scoped event-subscription patches.
pub struct StorageEventSubscriptionUpdateBuilder {
    collection_id: CollectionId,
    id: EventSubscriptionId,
    sink_id: Option<EventSinkId>,
    name: Option<String>,
    description: Option<String>,
    entity_types: Option<Vec<EntityType>>,
    actions: Option<Vec<Action>>,
    filter: Option<EventSubscriptionFilter>,
    routing: Option<Value>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSubscriptionUpdateBuilder {
    #[must_use]
    pub const fn sink_id(mut self, value: Option<EventSinkId>) -> Self {
        self.sink_id = value;
        self
    }

    #[must_use]
    pub fn name(mut self, value: Option<String>) -> Self {
        self.name = value;
        self
    }

    #[must_use]
    pub fn description(mut self, value: Option<String>) -> Self {
        self.description = value;
        self
    }

    #[must_use]
    pub fn entity_types(mut self, value: Option<Vec<EntityType>>) -> Self {
        self.entity_types = value;
        self
    }

    #[must_use]
    pub fn actions(mut self, value: Option<Vec<Action>>) -> Self {
        self.actions = value;
        self
    }

    #[must_use]
    pub fn filter(mut self, value: Option<EventSubscriptionFilter>) -> Self {
        self.filter = value;
        self
    }

    #[must_use]
    pub fn routing(mut self, value: Option<Value>) -> Self {
        self.routing = value;
        self
    }

    #[must_use]
    pub const fn enabled(mut self, value: Option<bool>) -> Self {
        self.enabled = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventSubscriptionUpdate, StorageError> {
        validate_optional_non_empty("name", self.name.as_deref())?;
        if let Some(entity_types) = &self.entity_types {
            validate_catalog_values("entity_types", entity_types)?;
        }
        if let Some(actions) = &self.actions {
            validate_catalog_values("actions", actions)?;
        }
        if let (Some(entity_types), Some(actions)) = (&self.entity_types, &self.actions) {
            for entity_type in entity_types {
                for action in actions {
                    if !is_valid_pair(*entity_type, *action) {
                        return Err(StorageError::invalid_input(format!(
                            "action '{}' is not valid for entity_type '{}'",
                            action.as_str(),
                            entity_type.as_str()
                        )));
                    }
                }
            }
        }
        if let Some(filter) = &self.filter {
            filter
                .validate()
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        if let Some(routing) = &self.routing {
            validate_json_object("routing", routing)?;
        }
        Ok(StorageEventSubscriptionUpdate {
            collection_id: self.collection_id,
            id: self.id,
            sink_id: self.sink_id,
            name: self.name,
            description: self.description,
            entity_types: self.entity_types,
            actions: self.actions,
            filter: self.filter,
            routing: self.routing,
            enabled: self.enabled,
            event_context: self.event_context,
        })
    }
}

impl fmt::Debug for StorageEventSubscriptionUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSubscriptionUpdate")
            .field("collection_id", &"<redacted>")
            .field("id", &"<redacted>")
            .field("has_sink_id", &self.sink_id.is_some())
            .field("has_name", &self.name.is_some())
            .field("has_description", &self.description.is_some())
            .field("has_entity_types", &self.entity_types.is_some())
            .field("has_actions", &self.actions.is_some())
            .field("has_filter", &self.filter.is_some())
            .field("has_routing", &self.routing.is_some())
            .field("has_enabled", &self.enabled.is_some())
            .finish()
    }
}

/// Collection-scoped event subscription deletion request.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSubscriptionDelete {
    collection_id: CollectionId,
    id: EventSubscriptionId,
    event_context: EventContext,
}

impl StorageEventSubscriptionDelete {
    #[must_use]
    pub const fn new(
        collection_id: CollectionId,
        id: EventSubscriptionId,
        event_context: EventContext,
    ) -> Self {
        Self {
            collection_id,
            id,
            event_context,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn id(&self) -> EventSubscriptionId {
        self.id
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

impl fmt::Debug for StorageEventSubscriptionDelete {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventSubscriptionDelete")
            .field("collection_id", &"<redacted>")
            .field("id", &"<redacted>")
            .finish()
    }
}

/// Complete event sink and subscription administration behavior.
///
/// Adapters must construct every returned projection through its fallible
/// terminal builder and classify invalid persisted state as a backend failure.
/// Patch implementations must validate the merged projection before commit,
/// including when only one half of a subscription catalog pair is changed.
#[async_trait]
pub trait EventConfigurationStorage: Send + Sync {
    /// Return the number of enabled sinks used to decide whether fan-out
    /// workers need to run.
    async fn count_enabled_event_sinks(&self) -> Result<i64, StorageError>;

    /// List sinks with backend filtering, stable cursor paging, and optional
    /// exact count.
    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StoragePage<StorageEventSink>, StorageError>;

    /// Load one event sink by ID.
    async fn get_event_sink(&self, sink_id: EventSinkId) -> Result<StorageEventSink, StorageError>;

    /// Atomically create an event sink and its lifecycle event.
    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError>;

    /// Atomically patch an event sink and its lifecycle event, preserving
    /// no-op revision behavior and validating the merged projection.
    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<MutationOutcome<StorageEventSink>, StorageError>;

    /// Atomically delete an eligible event sink and emit its lifecycle event.
    async fn delete_event_sink(
        &self,
        request: StorageEventSinkDelete,
    ) -> Result<MutationOutcome<()>, StorageError>;

    /// List subscriptions inside one collection with backend filtering, stable
    /// cursor paging, and optional exact count.
    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StoragePage<StorageEventSubscription>, StorageError>;

    /// Load a subscription only when it belongs to the named collection.
    async fn get_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError>;

    /// Atomically create a validated subscription and its lifecycle event.
    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError>;

    /// Atomically patch a collection-scoped subscription and its lifecycle
    /// event, preserving no-op revision behavior and validating the merged
    /// projection.
    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<MutationOutcome<StorageEventSubscription>, StorageError>;

    /// Atomically delete a collection-scoped subscription and emit its
    /// lifecycle event.
    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<MutationOutcome<()>, StorageError>;
}

/// Claim-free event delivery projection for administrator APIs.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventDelivery {
    id: EventDeliveryId,
    event_id: EventSequence,
    subscription_id: EventSubscriptionId,
    status: EventDeliveryStatus,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
    last_error: Option<String>,
    locked_until: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl StorageEventDelivery {
    #[must_use]
    pub fn builder(
        id: EventDeliveryId,
        event_id: EventSequence,
        subscription_id: EventSubscriptionId,
        status: EventDeliveryStatus,
        next_attempt_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> StorageEventDeliveryBuilder {
        StorageEventDeliveryBuilder {
            id,
            event_id,
            subscription_id,
            status,
            attempts: 0,
            next_attempt_at,
            last_error: None,
            locked_until: None,
            created_at,
            updated_at,
        }
    }

    #[must_use]
    pub const fn id(&self) -> EventDeliveryId {
        self.id
    }

    #[must_use]
    pub const fn event_id(&self) -> EventSequence {
        self.event_id
    }

    #[must_use]
    pub const fn subscription_id(&self) -> EventSubscriptionId {
        self.subscription_id
    }

    #[must_use]
    pub const fn status(&self) -> EventDeliveryStatus {
        self.status
    }

    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    #[must_use]
    pub const fn next_attempt_at(&self) -> DateTime<Utc> {
        self.next_attempt_at
    }

    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    #[must_use]
    pub const fn locked_until(&self) -> Option<DateTime<Utc>> {
        self.locked_until
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

/// Builder for claim-free delivery projections.
pub struct StorageEventDeliveryBuilder {
    id: EventDeliveryId,
    event_id: EventSequence,
    subscription_id: EventSubscriptionId,
    status: EventDeliveryStatus,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
    last_error: Option<String>,
    locked_until: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl StorageEventDeliveryBuilder {
    #[must_use]
    pub const fn attempts(mut self, value: i32) -> Self {
        self.attempts = value;
        self
    }

    #[must_use]
    pub fn last_error(mut self, value: Option<String>) -> Self {
        self.last_error = value;
        self
    }

    #[must_use]
    pub const fn locked_until(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.locked_until = value;
        self
    }

    pub fn try_build(self) -> Result<StorageEventDelivery, StorageError> {
        if self.attempts < 0 {
            return Err(StorageError::invalid_input(
                "Event delivery attempts must not be negative",
            ));
        }
        if self.updated_at < self.created_at {
            return Err(StorageError::invalid_input(
                "Event delivery updated_at must not precede created_at",
            ));
        }
        Ok(StorageEventDelivery {
            id: self.id,
            event_id: self.event_id,
            subscription_id: self.subscription_id,
            status: self.status,
            attempts: self.attempts,
            next_attempt_at: self.next_attempt_at,
            last_error: self.last_error,
            locked_until: self.locked_until,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

/// Cursor-paginated administrator delivery query.
#[derive(Clone, PartialEq)]
pub struct StorageEventDeliveryListQuery {
    subscription_id: Option<EventSubscriptionId>,
    options: QueryOptions,
}

impl StorageEventDeliveryListQuery {
    #[must_use]
    pub const fn new(options: QueryOptions) -> Self {
        Self {
            subscription_id: None,
            options,
        }
    }

    #[must_use]
    pub const fn subscription_id(mut self, value: Option<EventSubscriptionId>) -> Self {
        self.subscription_id = value;
        self
    }

    #[must_use]
    pub const fn subscription_id_value(&self) -> Option<EventSubscriptionId> {
        self.subscription_id
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }
}

impl fmt::Debug for StorageEventDeliveryListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageEventDeliveryListQuery")
            .field("has_subscription_id", &self.subscription_id.is_some())
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Administrator delivery inspection and intervention behavior.
#[async_trait]
pub trait EventDeliveryAdministrationStorage: Send + Sync {
    /// List claim-free delivery projections with backend filtering, stable
    /// cursor paging, and optional exact count.
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StoragePage<StorageEventDelivery>, StorageError>;

    /// Load one claim-free delivery projection.
    async fn get_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError>;

    /// Release a failed or dead delivery for immediate retry and notify native
    /// workers atomically with the state change.
    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError>;

    /// Mark any non-succeeded delivery dead while clearing opaque claim state.
    async fn mark_event_delivery_dead(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_delivery_builder_rejects_negative_attempts_and_reversed_timestamps() {
        let created_at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let earlier = created_at - chrono::Duration::seconds(1);
        let delivery = |updated_at| {
            StorageEventDelivery::builder(
                EventDeliveryId::new(1).unwrap(),
                EventSequence::new(2).unwrap(),
                EventSubscriptionId::new(3).unwrap(),
                EventDeliveryStatus::Pending,
                created_at,
                created_at,
                updated_at,
            )
        };

        assert_eq!(
            delivery(created_at)
                .attempts(-1)
                .try_build()
                .err()
                .unwrap()
                .kind(),
            crate::StorageErrorKind::InvalidInput
        );
        assert_eq!(
            delivery(earlier).try_build().err().unwrap().kind(),
            crate::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn event_administration_query_debug_is_bounded_and_redacted() {
        let options = QueryOptions::new(
            Vec::new(),
            Vec::new(),
            Some(20),
            Some("secret-cursor".to_string()),
            true,
        )
        .unwrap();
        let query = StorageAuditEventListQuery::new(
            vec![
                CollectionId::new(99).unwrap(),
                CollectionId::new(42).unwrap(),
                CollectionId::new(99).unwrap(),
            ],
            false,
            StorageAuditEventFilters::new()
                .entity_id(Some(EventEntityId::new(123).unwrap()))
                .actor_user_id(Some(PrincipalId::new(456).unwrap())),
            options,
        );
        let debug = format!("{query:?}");

        assert_eq!(
            query.accessible_collection_ids(),
            [
                CollectionId::new(42).unwrap(),
                CollectionId::new(99).unwrap()
            ]
        );
        assert!(!debug.contains("123"));
        assert!(!debug.contains("456"));
        assert!(!debug.contains("secret-cursor"));
        assert!(debug.contains("accessible_collection_count: 2"));
        assert!(debug.contains("has_cursor: true"));
    }

    #[test]
    fn sink_and_subscription_debug_redacts_transport_and_identity_values() {
        let event_context = EventContext::system();
        let sink = StorageEventSinkCreate::builder("secret-name", "webhook", event_context.clone())
            .configuration(serde_json::json!({"url": "https://secret.invalid"}))
            .secret_ref(Some("secret-reference".to_string()))
            .enabled(true)
            .try_build()
            .unwrap();
        let subscription = StorageEventSubscriptionCreate::builder(
            CollectionId::new(42).unwrap(),
            EventSinkId::new(43).unwrap(),
            "secret-subscription",
            event_context,
        )
        .entity_types(vec![EntityType::Collection])
        .actions(vec![Action::Created])
        .routing(serde_json::json!({"key": "secret-routing"}))
        .try_build()
        .unwrap();
        let debug = format!("{sink:?} {subscription:?}");

        assert!(!debug.contains("secret-name"));
        assert!(!debug.contains("secret.invalid"));
        assert!(!debug.contains("secret-reference"));
        assert!(!debug.contains("secret-subscription"));
        assert!(!debug.contains("secret-routing"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("43"));
    }

    #[test]
    fn event_administration_builders_reject_invalid_requests() {
        let context = EventContext::system();
        assert!(
            StorageEventSinkCreate::builder("", "webhook", context.clone())
                .try_build()
                .is_err()
        );
        assert!(
            StorageEventSinkUpdate::builder(EventSinkId::new(1).unwrap(), context.clone())
                .configuration(Some(serde_json::json!([])))
                .try_build()
                .is_err()
        );
        assert!(
            StorageEventSubscriptionCreate::builder(
                CollectionId::new(1).unwrap(),
                EventSinkId::new(1).unwrap(),
                "subscription",
                context.clone(),
            )
            .try_build()
            .is_err()
        );
        assert!(
            StorageEventSubscriptionUpdate::builder(
                CollectionId::new(1).unwrap(),
                EventSubscriptionId::new(1).unwrap(),
                context,
            )
            .routing(Some(serde_json::json!([])))
            .try_build()
            .is_err()
        );
    }

    #[test]
    fn persisted_event_administration_builders_reject_corrupt_projections() {
        let created_at = Utc::now();
        let updated_at = created_at - chrono::Duration::seconds(1);
        assert!(
            StorageEventSink::builder(
                EventSinkId::new(1).unwrap(),
                "sink",
                "webhook",
                created_at,
                updated_at,
                ResourceRevision::INITIAL,
            )
            .try_build()
            .is_err()
        );
        assert!(
            StorageEventSubscription::builder(
                EventSubscriptionId::new(1).unwrap(),
                CollectionId::new(1).unwrap(),
                EventSinkId::new(1).unwrap(),
                "subscription",
                created_at,
                created_at,
                ResourceRevision::INITIAL,
            )
            .entity_types(vec![EntityType::ObjectRelation])
            .actions(vec![Action::Updated])
            .try_build()
            .is_err()
        );
    }
}
