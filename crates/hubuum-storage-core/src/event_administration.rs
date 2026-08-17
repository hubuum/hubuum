use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::{Action, ActorKind, EntityType, EventContext, EventSubscriptionFilter};
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::StorageError;

/// One backend-selected event-administration page and optional exact total.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventPage<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> StorageEventPage<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, total: Option<i64>) -> Self {
        Self { rows, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Option<i64>) {
        (self.rows, self.total)
    }
}

/// Typed audit filters interpreted by the selected backend.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StorageAuditEventFilters {
    entity_type: Option<EntityType>,
    entity_id: Option<i32>,
    action: Option<Action>,
    actor_kind: Option<ActorKind>,
    actor_user_id: Option<i32>,
    initiator_user_id: Option<i32>,
    collection_id: Option<i32>,
    occurred_after: Option<NaiveDateTime>,
    occurred_before: Option<NaiveDateTime>,
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
    pub const fn entity_id(mut self, value: Option<i32>) -> Self {
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
    pub const fn actor_user_id(mut self, value: Option<i32>) -> Self {
        self.actor_user_id = value;
        self
    }

    #[must_use]
    pub const fn initiator_user_id(mut self, value: Option<i32>) -> Self {
        self.initiator_user_id = value;
        self
    }

    #[must_use]
    pub const fn collection_id(mut self, value: Option<i32>) -> Self {
        self.collection_id = value;
        self
    }

    #[must_use]
    pub const fn occurred_after(mut self, value: Option<NaiveDateTime>) -> Self {
        self.occurred_after = value;
        self
    }

    #[must_use]
    pub const fn occurred_before(mut self, value: Option<NaiveDateTime>) -> Self {
        self.occurred_before = value;
        self
    }

    #[must_use]
    pub const fn entity_type_value(&self) -> Option<EntityType> {
        self.entity_type
    }

    #[must_use]
    pub const fn entity_id_value(&self) -> Option<i32> {
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
    pub const fn actor_user_id_value(&self) -> Option<i32> {
        self.actor_user_id
    }

    #[must_use]
    pub const fn initiator_user_id_value(&self) -> Option<i32> {
        self.initiator_user_id
    }

    #[must_use]
    pub const fn collection_id_value(&self) -> Option<i32> {
        self.collection_id
    }

    #[must_use]
    pub const fn occurred_after_value(&self) -> Option<NaiveDateTime> {
        self.occurred_after
    }

    #[must_use]
    pub const fn occurred_before_value(&self) -> Option<NaiveDateTime> {
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
    accessible_collection_ids: Vec<i32>,
    include_collection_less: bool,
    filters: StorageAuditEventFilters,
    options: QueryOptions,
}

impl StorageAuditEventListQuery {
    #[must_use]
    pub fn new(
        mut accessible_collection_ids: Vec<i32>,
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
    pub fn accessible_collection_ids(&self) -> &[i32] {
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
    ) -> Result<StorageEventPage<StorageAuditEvent>, StorageError>;
}

/// Backend-neutral persisted event sink.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSink {
    id: i32,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageEventSink {
    #[must_use]
    pub fn builder(
        id: i32,
        name: impl Into<String>,
        kind: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
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
    pub const fn id(&self) -> i32 {
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
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

/// Builder for persisted event-sink projections.
pub struct StorageEventSinkBuilder {
    id: i32,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
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

    #[must_use]
    pub fn build(self) -> StorageEventSink {
        StorageEventSink {
            id: self.id,
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            secret_ref: self.secret_ref,
            enabled: self.enabled,
            created_at: self.created_at,
            updated_at: self.updated_at,
            revision: self.revision,
        }
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

    #[must_use]
    pub fn build(self) -> StorageEventSinkCreate {
        StorageEventSinkCreate {
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            secret_ref: self.secret_ref,
            enabled: self.enabled,
            event_context: self.event_context,
        }
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
    id: i32,
    name: Option<String>,
    kind: Option<String>,
    configuration: Option<Value>,
    secret_ref: Option<Option<String>>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSinkUpdate {
    #[must_use]
    pub fn new(id: i32, event_context: EventContext) -> Self {
        Self {
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

    #[must_use]
    pub const fn id(&self) -> i32 {
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
    id: i32,
    event_context: EventContext,
}

impl StorageEventSinkDelete {
    #[must_use]
    pub const fn new(id: i32, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
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
    id: i32,
    collection_id: i32,
    sink_id: i32,
    name: String,
    description: String,
    entity_types: Vec<String>,
    actions: Vec<String>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageEventSubscription {
    #[must_use]
    pub fn builder(
        id: i32,
        collection_id: i32,
        sink_id: i32,
        name: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
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
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub const fn sink_id(&self) -> i32 {
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
    pub fn entity_types(&self) -> &[String] {
        &self.entity_types
    }

    #[must_use]
    pub fn actions(&self) -> &[String] {
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
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

/// Builder for persisted event subscription projections.
pub struct StorageEventSubscriptionBuilder {
    id: i32,
    collection_id: i32,
    sink_id: i32,
    name: String,
    description: String,
    entity_types: Vec<String>,
    actions: Vec<String>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageEventSubscriptionBuilder {
    #[must_use]
    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.description = value.into();
        self
    }

    #[must_use]
    pub fn entity_types(mut self, value: Vec<String>) -> Self {
        self.entity_types = value;
        self
    }

    #[must_use]
    pub fn actions(mut self, value: Vec<String>) -> Self {
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

    #[must_use]
    pub fn build(self) -> StorageEventSubscription {
        StorageEventSubscription {
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
        }
    }
}

/// Collection-scoped event subscription list query.
#[derive(Clone, PartialEq)]
pub struct StorageEventSubscriptionListQuery {
    collection_id: i32,
    options: QueryOptions,
}

impl StorageEventSubscriptionListQuery {
    #[must_use]
    pub const fn new(collection_id: i32, options: QueryOptions) -> Self {
        Self {
            collection_id,
            options,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
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
    collection_id: i32,
    sink_id: i32,
    name: String,
    description: String,
    entity_types: Vec<String>,
    actions: Vec<String>,
    filter: EventSubscriptionFilter,
    routing: Value,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSubscriptionCreate {
    #[must_use]
    pub fn builder(
        collection_id: i32,
        sink_id: i32,
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
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub const fn sink_id(&self) -> i32 {
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
    pub fn entity_types(&self) -> &[String] {
        &self.entity_types
    }

    #[must_use]
    pub fn actions(&self) -> &[String] {
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
    collection_id: i32,
    sink_id: i32,
    name: String,
    description: String,
    entity_types: Vec<String>,
    actions: Vec<String>,
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
    pub fn entity_types(mut self, value: Vec<String>) -> Self {
        self.entity_types = value;
        self
    }

    #[must_use]
    pub fn actions(mut self, value: Vec<String>) -> Self {
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

    #[must_use]
    pub fn build(self) -> StorageEventSubscriptionCreate {
        StorageEventSubscriptionCreate {
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
        }
    }
}

/// Validated collection-scoped event subscription patch.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSubscriptionUpdate {
    collection_id: i32,
    id: i32,
    sink_id: Option<i32>,
    name: Option<String>,
    description: Option<String>,
    entity_types: Option<Vec<String>>,
    actions: Option<Vec<String>>,
    filter: Option<EventSubscriptionFilter>,
    routing: Option<Value>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSubscriptionUpdate {
    #[must_use]
    pub fn new(collection_id: i32, id: i32, event_context: EventContext) -> Self {
        Self {
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
    pub const fn sink_id(mut self, value: Option<i32>) -> Self {
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
    pub fn entity_types(mut self, value: Option<Vec<String>>) -> Self {
        self.entity_types = value;
        self
    }

    #[must_use]
    pub fn actions(mut self, value: Option<Vec<String>>) -> Self {
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

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn sink_id_value(&self) -> Option<i32> {
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
    pub fn entity_types_value(&self) -> Option<&[String]> {
        self.entity_types.as_deref()
    }

    #[must_use]
    pub fn actions_value(&self) -> Option<&[String]> {
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
    collection_id: i32,
    id: i32,
    event_context: EventContext,
}

impl StorageEventSubscriptionDelete {
    #[must_use]
    pub const fn new(collection_id: i32, id: i32, event_context: EventContext) -> Self {
        Self {
            collection_id,
            id,
            event_context,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
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
#[async_trait]
pub trait EventSubscriptionStorage: Send + Sync {
    /// Return the number of enabled sinks used to decide whether fan-out
    /// workers need to run.
    async fn enabled_event_sink_count(&self) -> Result<i64, StorageError>;

    /// List sinks with backend filtering, stable cursor paging, and optional
    /// exact count.
    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StorageEventPage<StorageEventSink>, StorageError>;

    /// Load one event sink by ID.
    async fn load_event_sink(&self, sink_id: i32) -> Result<StorageEventSink, StorageError>;

    /// Atomically create an event sink and its lifecycle event.
    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageEventSink, StorageError>;

    /// Atomically patch an event sink and its lifecycle event, preserving
    /// no-op revision behavior.
    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageEventSink, StorageError>;

    /// Atomically delete an eligible event sink and emit its lifecycle event.
    async fn delete_event_sink(&self, request: StorageEventSinkDelete) -> Result<(), StorageError>;

    /// List subscriptions inside one collection with backend filtering, stable
    /// cursor paging, and optional exact count.
    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StorageEventPage<StorageEventSubscription>, StorageError>;

    /// Load a subscription only when it belongs to the named collection.
    async fn load_event_subscription(
        &self,
        collection_id: i32,
        subscription_id: i32,
    ) -> Result<StorageEventSubscription, StorageError>;

    /// Atomically create a validated subscription and its lifecycle event.
    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageEventSubscription, StorageError>;

    /// Atomically patch a collection-scoped subscription and its lifecycle
    /// event, preserving no-op revision behavior.
    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageEventSubscription, StorageError>;

    /// Atomically delete a collection-scoped subscription and emit its
    /// lifecycle event.
    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<(), StorageError>;
}

/// Claim-free event delivery projection for administrator APIs.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventDelivery {
    id: i64,
    event_id: i64,
    subscription_id: i32,
    status: String,
    attempts: i32,
    next_attempt_at: NaiveDateTime,
    last_error: Option<String>,
    locked_until: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl StorageEventDelivery {
    #[must_use]
    pub fn builder(
        id: i64,
        event_id: i64,
        subscription_id: i32,
        status: impl Into<String>,
        next_attempt_at: NaiveDateTime,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> StorageEventDeliveryBuilder {
        StorageEventDeliveryBuilder {
            id,
            event_id,
            subscription_id,
            status: status.into(),
            attempts: 0,
            next_attempt_at,
            last_error: None,
            locked_until: None,
            created_at,
            updated_at,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    #[must_use]
    pub const fn event_id(&self) -> i64 {
        self.event_id
    }

    #[must_use]
    pub const fn subscription_id(&self) -> i32 {
        self.subscription_id
    }

    #[must_use]
    pub fn status(&self) -> &str {
        &self.status
    }

    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    #[must_use]
    pub const fn next_attempt_at(&self) -> NaiveDateTime {
        self.next_attempt_at
    }

    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    #[must_use]
    pub const fn locked_until(&self) -> Option<NaiveDateTime> {
        self.locked_until
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }
}

/// Builder for claim-free delivery projections.
pub struct StorageEventDeliveryBuilder {
    id: i64,
    event_id: i64,
    subscription_id: i32,
    status: String,
    attempts: i32,
    next_attempt_at: NaiveDateTime,
    last_error: Option<String>,
    locked_until: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
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
    pub const fn locked_until(mut self, value: Option<NaiveDateTime>) -> Self {
        self.locked_until = value;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageEventDelivery {
        StorageEventDelivery {
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
        }
    }
}

/// Cursor-paginated administrator delivery query.
#[derive(Clone, PartialEq)]
pub struct StorageEventDeliveryListQuery {
    subscription_id: Option<i32>,
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
    pub const fn subscription_id(mut self, value: Option<i32>) -> Self {
        self.subscription_id = value;
        self
    }

    #[must_use]
    pub const fn subscription_id_value(&self) -> Option<i32> {
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
    ) -> Result<StorageEventPage<StorageEventDelivery>, StorageError>;

    /// Load one claim-free delivery projection.
    async fn load_event_delivery(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError>;

    /// Release a failed or dead delivery for immediate retry and notify native
    /// workers atomically with the state change.
    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError>;

    /// Mark any non-succeeded delivery dead while clearing opaque claim state.
    async fn mark_event_delivery_dead(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

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
            vec![99, 42, 99],
            false,
            StorageAuditEventFilters::new()
                .entity_id(Some(123))
                .actor_user_id(Some(456)),
            options,
        );
        let debug = format!("{query:?}");

        assert_eq!(query.accessible_collection_ids(), &[42, 99]);
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
            .build();
        let subscription =
            StorageEventSubscriptionCreate::builder(42, 43, "secret-subscription", event_context)
                .routing(serde_json::json!({"key": "secret-routing"}))
                .build();
        let debug = format!("{sink:?} {subscription:?}");

        assert!(!debug.contains("secret-name"));
        assert!(!debug.contains("secret.invalid"));
        assert!(!debug.contains("secret-reference"));
        assert!(!debug.contains("secret-subscription"));
        assert!(!debug.contains("secret-routing"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("43"));
    }
}
