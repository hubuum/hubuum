//! Event sink configuration, ownership and collection grants.
use super::{
    validate_json_object, validate_non_empty, validate_optional_non_empty, validate_timestamps,
};
use crate::{StorageError, StorageValidationError};
use chrono::{DateTime, Utc};
use hubuum_domain::{CollectionId, EventDeliveryPolicy, EventSinkId, ResourceRevision};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use serde_json::Value;
use std::fmt;

/// Backend-neutral persisted event sink.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSink {
    collection_id: Option<CollectionId>,
    id: EventSinkId,
    name: String,
    kind: String,
    configuration: Value,
    delivery_policy: EventDeliveryPolicy,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageEventSink {
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }

    pub const fn delivery_policy(&self) -> EventDeliveryPolicy {
        self.delivery_policy
    }

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
            collection_id: None,
            id,
            name: name.into(),
            kind: kind.into(),
            configuration: serde_json::json!({}),
            delivery_policy: EventDeliveryPolicy::default(),
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
    collection_id: Option<CollectionId>,
    id: EventSinkId,
    name: String,
    kind: String,
    configuration: Value,
    delivery_policy: EventDeliveryPolicy,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageEventSinkBuilder {
    pub fn collection_id(mut self, value: Option<CollectionId>) -> Self {
        self.collection_id = value;
        self
    }

    pub fn delivery_policy(mut self, value: EventDeliveryPolicy) -> Self {
        self.delivery_policy = value;
        self
    }

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

    pub fn try_build(self) -> Result<StorageEventSink, StorageValidationError> {
        validate_non_empty("name", &self.name)?;
        validate_non_empty("kind", &self.kind)?;
        validate_json_object("configuration", &self.configuration)?;
        validate_optional_non_empty("secret_ref", self.secret_ref.as_deref())?;
        validate_collection_sink(
            self.collection_id,
            &self.kind,
            &self.configuration,
            self.secret_ref.as_deref(),
        )?;
        validate_timestamps(self.created_at, self.updated_at)?;
        Ok(StorageEventSink {
            collection_id: self.collection_id,
            id: self.id,
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            delivery_policy: self.delivery_policy,
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
    collection_id: Option<CollectionId>,
    options: QueryOptions,
}

impl StorageEventSinkListQuery {
    pub fn for_collection(mut self, collection_id: CollectionId) -> Self {
        self.collection_id = Some(collection_id);
        self
    }
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }
    #[must_use]
    pub const fn new(options: QueryOptions) -> Self {
        Self {
            options,
            collection_id: None,
        }
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
            .field("has_cursor", &self.options.has_cursor())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Validated event sink creation request.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventSinkCreate {
    collection_id: Option<CollectionId>,
    name: String,
    kind: String,
    configuration: Value,
    delivery_policy: EventDeliveryPolicy,
    secret_ref: Option<String>,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSinkCreate {
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }

    pub const fn delivery_policy(&self) -> EventDeliveryPolicy {
        self.delivery_policy
    }

    #[must_use]
    pub fn builder(
        name: impl Into<String>,
        kind: impl Into<String>,
        event_context: EventContext,
    ) -> StorageEventSinkCreateBuilder {
        StorageEventSinkCreateBuilder {
            collection_id: None,
            name: name.into(),
            kind: kind.into(),
            configuration: serde_json::json!({}),
            delivery_policy: EventDeliveryPolicy::default(),
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
    collection_id: Option<CollectionId>,
    name: String,
    kind: String,
    configuration: Value,
    delivery_policy: EventDeliveryPolicy,
    secret_ref: Option<String>,
    enabled: bool,
    event_context: EventContext,
}

impl StorageEventSinkCreateBuilder {
    pub fn collection_id(mut self, value: Option<CollectionId>) -> Self {
        self.collection_id = value;
        self
    }

    pub fn delivery_policy(mut self, value: EventDeliveryPolicy) -> Self {
        self.delivery_policy = value;
        self
    }

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
        validate_non_empty("name", &self.name)
            .map_err(StorageValidationError::into_request_error)?;
        validate_non_empty("kind", &self.kind)
            .map_err(StorageValidationError::into_request_error)?;
        validate_json_object("configuration", &self.configuration)
            .map_err(StorageValidationError::into_request_error)?;
        validate_optional_non_empty("secret_ref", self.secret_ref.as_deref())
            .map_err(StorageValidationError::into_request_error)?;
        validate_collection_sink(
            self.collection_id,
            &self.kind,
            &self.configuration,
            self.secret_ref.as_deref(),
        )
        .map_err(StorageValidationError::into_request_error)?;
        Ok(StorageEventSinkCreate {
            collection_id: self.collection_id,
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            delivery_policy: self.delivery_policy,
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
    delivery_policy: Option<EventDeliveryPolicy>,
    secret_ref: Option<Option<String>>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSinkUpdate {
    pub const fn delivery_policy(&self) -> Option<EventDeliveryPolicy> {
        self.delivery_policy
    }

    #[must_use]
    pub fn builder(id: EventSinkId, event_context: EventContext) -> StorageEventSinkUpdateBuilder {
        StorageEventSinkUpdateBuilder {
            id,
            name: None,
            kind: None,
            configuration: None,
            delivery_policy: None,
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
    delivery_policy: Option<EventDeliveryPolicy>,
    secret_ref: Option<Option<String>>,
    enabled: Option<bool>,
    event_context: EventContext,
}

impl StorageEventSinkUpdateBuilder {
    pub fn delivery_policy(mut self, value: Option<EventDeliveryPolicy>) -> Self {
        self.delivery_policy = value;
        self
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

    pub fn try_build(self) -> Result<StorageEventSinkUpdate, StorageError> {
        validate_optional_non_empty("name", self.name.as_deref())
            .map_err(StorageValidationError::into_request_error)?;
        validate_optional_non_empty("kind", self.kind.as_deref())
            .map_err(StorageValidationError::into_request_error)?;
        if let Some(configuration) = &self.configuration {
            validate_json_object("configuration", configuration)
                .map_err(StorageValidationError::into_request_error)?;
        }
        if let Some(secret_ref) = &self.secret_ref {
            validate_optional_non_empty("secret_ref", secret_ref.as_deref())
                .map_err(StorageValidationError::into_request_error)?;
        }
        Ok(StorageEventSinkUpdate {
            id: self.id,
            name: self.name,
            kind: self.kind,
            configuration: self.configuration,
            delivery_policy: self.delivery_policy,
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

/// A sink resolved for one collection. Storage rechecks the grant under its
/// mutation/admission lock because authorization can change concurrently.
#[derive(Clone)]
pub struct StorageAuthorizedEventSink {
    collection_id: CollectionId,
    sink: StorageEventSink,
}

impl StorageAuthorizedEventSink {
    pub fn try_new(
        collection_id: CollectionId,
        sink: StorageEventSink,
        directly_granted: bool,
    ) -> Result<Self, StorageError> {
        if sink.collection_id() != Some(collection_id)
            && !(sink.collection_id().is_none() && directly_granted)
        {
            return Err(StorageError::permission_denied(
                "Sink is not available to this collection",
            ));
        }
        Ok(Self {
            collection_id,
            sink,
        })
    }

    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    pub const fn sink(&self) -> &StorageEventSink {
        &self.sink
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventSinkGrantAction {
    Grant,
    Revoke,
}

/// An explicit administrator change to a global sink's collection grants.
#[derive(Clone)]
pub struct StorageEventSinkGrantChange {
    sink_id: EventSinkId,
    collection_id: CollectionId,
    action: EventSinkGrantAction,
    event_context: EventContext,
}

impl StorageEventSinkGrantChange {
    pub const fn new(
        sink_id: EventSinkId,
        collection_id: CollectionId,
        action: EventSinkGrantAction,
        event_context: EventContext,
    ) -> Self {
        Self {
            sink_id,
            collection_id,
            action,
            event_context,
        }
    }
    pub const fn sink_id(&self) -> EventSinkId {
        self.sink_id
    }
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    pub const fn action(&self) -> EventSinkGrantAction {
        self.action
    }
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

fn validate_collection_sink(
    collection_id: Option<CollectionId>,
    kind: &str,
    configuration: &Value,
    secret_ref: Option<&str>,
) -> Result<(), StorageValidationError> {
    if collection_id.is_some()
        && (kind != "webhook"
            || secret_ref.is_some()
            || configuration.get("url_secret_ref").is_some()
            || configuration
                .get("destination_url")
                .and_then(Value::as_str)
                .is_none_or(|url| url.trim().is_empty()))
    {
        return Err(StorageValidationError::invalid(
            "Collection-owned sinks require a webhook destination_url and cannot reference server secrets",
        ));
    }
    Ok(())
}
