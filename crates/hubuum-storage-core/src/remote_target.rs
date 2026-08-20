use std::fmt;

use async_trait::async_trait;
use hubuum_domain::{ClassId, CollectionId, RemoteTargetId, ResourceId, TaskId};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{MutationOutcome, StorageError, StoragePage, StorageRecordMetadata};

/// HTTP transport configuration for a remote target.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteTargetTransport {
    method: String,
    url_template: String,
    headers_template: Value,
    body_template: Option<String>,
    auth_config: Value,
    timeout_ms: i32,
}

impl StorageRemoteTargetTransport {
    #[must_use]
    pub fn new(
        method: impl Into<String>,
        url_template: impl Into<String>,
        headers_template: Value,
        body_template: Option<String>,
        auth_config: Value,
        timeout_ms: i32,
    ) -> Self {
        Self {
            method: method.into(),
            url_template: url_template.into(),
            headers_template,
            body_template,
            auth_config,
            timeout_ms,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (String, String, Value, Option<String>, Value, i32) {
        (
            self.method,
            self.url_template,
            self.headers_template,
            self.body_template,
            self.auth_config,
            self.timeout_ms,
        )
    }
}

impl fmt::Debug for StorageRemoteTargetTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTargetTransport")
            .field("method", &self.method)
            .field("timeout_ms", &self.timeout_ms)
            .field("has_body_template", &self.body_template.is_some())
            .field("configuration", &"[redacted]")
            .finish()
    }
}

/// Subject and enablement policy for a remote target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRemoteTargetPolicy {
    class_id: Option<ClassId>,
    allowed_subject_types: Vec<String>,
    enabled: bool,
}

impl StorageRemoteTargetPolicy {
    #[must_use]
    pub const fn new(
        class_id: Option<ClassId>,
        allowed_subject_types: Vec<String>,
        enabled: bool,
    ) -> Self {
        Self {
            class_id,
            allowed_subject_types,
            enabled,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<ClassId>, Vec<String>, bool) {
        (self.class_id, self.allowed_subject_types, self.enabled)
    }
}

/// Backend-neutral mutable definition of one remote target.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteTargetDefinition {
    description: String,
    transport: StorageRemoteTargetTransport,
    policy: StorageRemoteTargetPolicy,
}

impl fmt::Debug for StorageRemoteTargetDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTargetDefinition")
            .field("transport", &self.transport)
            .field("policy", &self.policy)
            .field("description", &"[redacted]")
            .finish()
    }
}

impl StorageRemoteTargetDefinition {
    #[must_use]
    pub fn new(
        description: impl Into<String>,
        transport: StorageRemoteTargetTransport,
        policy: StorageRemoteTargetPolicy,
    ) -> Self {
        Self {
            description: description.into(),
            transport,
            policy,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        String,
        StorageRemoteTargetTransport,
        StorageRemoteTargetPolicy,
    ) {
        (self.description, self.transport, self.policy)
    }
}

/// Persisted remote target projected without adapter row types.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteTarget {
    metadata: StorageRecordMetadata,
    collection_id: CollectionId,
    name: String,
    definition: StorageRemoteTargetDefinition,
}

impl StorageRemoteTarget {
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        collection_id: CollectionId,
        name: impl Into<String>,
        definition: StorageRemoteTargetDefinition,
    ) -> Self {
        Self {
            metadata,
            collection_id,
            name: name.into(),
            definition,
        }
    }

    #[must_use]
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageRecordMetadata,
        CollectionId,
        String,
        StorageRemoteTargetDefinition,
    ) {
        (
            self.metadata,
            self.collection_id,
            self.name,
            self.definition,
        )
    }
}

impl fmt::Debug for StorageRemoteTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTarget")
            .field("metadata", &self.metadata)
            .field("collection_id", &self.collection_id)
            .field("definition", &self.definition)
            .field("name", &"[redacted]")
            .finish()
    }
}

/// Filtered, ordered remote-target list request.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteTargetListQuery {
    allowed_collection_ids: Vec<CollectionId>,
    options: QueryOptions,
}

impl StorageRemoteTargetListQuery {
    #[must_use]
    pub const fn new(allowed_collection_ids: Vec<CollectionId>, options: QueryOptions) -> Self {
        Self {
            allowed_collection_ids,
            options,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<CollectionId>, QueryOptions) {
        (self.allowed_collection_ids, self.options)
    }
}

impl fmt::Debug for StorageRemoteTargetListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTargetListQuery")
            .field(
                "allowed_collection_count",
                &self.allowed_collection_ids.len(),
            )
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish()
    }
}

/// Atomic remote-target create command including audit provenance.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteTargetCreate {
    collection_id: CollectionId,
    name: String,
    definition: StorageRemoteTargetDefinition,
    event_context: EventContext,
}

impl StorageRemoteTargetCreate {
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        name: impl Into<String>,
        definition: StorageRemoteTargetDefinition,
        event_context: EventContext,
    ) -> Self {
        Self {
            collection_id,
            name: name.into(),
            definition,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        CollectionId,
        String,
        StorageRemoteTargetDefinition,
        EventContext,
    ) {
        (
            self.collection_id,
            self.name,
            self.definition,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageRemoteTargetCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTargetCreate")
            .field("collection_id", &self.collection_id)
            .field("definition", &self.definition)
            .field("name", &"[redacted]")
            .finish_non_exhaustive()
    }
}

pub struct StorageRemoteTargetPatchParts {
    collection_id: Option<CollectionId>,
    class_id: Option<Option<ClassId>>,
    name: Option<String>,
    description: Option<String>,
    method: Option<String>,
    url_template: Option<String>,
    headers_template: Option<Value>,
    body_template: Option<Option<String>>,
    auth_config: Option<Value>,
    allowed_subject_types: Option<Vec<String>>,
    timeout_ms: Option<i32>,
    enabled: Option<bool>,
}

impl StorageRemoteTargetPatchParts {
    #[must_use]
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }
    #[must_use]
    pub const fn class_id(&self) -> Option<Option<ClassId>> {
        self.class_id
    }
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
    #[must_use]
    pub fn method(&self) -> Option<&str> {
        self.method.as_deref()
    }
    #[must_use]
    pub fn url_template(&self) -> Option<&str> {
        self.url_template.as_deref()
    }
    #[must_use]
    pub const fn headers_template(&self) -> Option<&Value> {
        self.headers_template.as_ref()
    }
    #[must_use]
    pub fn body_template(&self) -> Option<Option<&str>> {
        self.body_template.as_ref().map(|value| value.as_deref())
    }
    #[must_use]
    pub const fn auth_config(&self) -> Option<&Value> {
        self.auth_config.as_ref()
    }
    #[must_use]
    pub fn allowed_subject_types(&self) -> Option<&[String]> {
        self.allowed_subject_types.as_deref()
    }
    #[must_use]
    pub const fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
    #[must_use]
    pub const fn enabled(&self) -> Option<bool> {
        self.enabled
    }
}

/// Sparse, already validated remote-target update.
#[derive(Clone, Default, PartialEq)]
pub struct StorageRemoteTargetPatch {
    collection_id: Option<CollectionId>,
    class_id: Option<Option<ClassId>>,
    name: Option<String>,
    description: Option<String>,
    method: Option<String>,
    url_template: Option<String>,
    headers_template: Option<Value>,
    body_template: Option<Option<String>>,
    auth_config: Option<Value>,
    allowed_subject_types: Option<Vec<String>>,
    timeout_ms: Option<i32>,
    enabled: Option<bool>,
}

impl StorageRemoteTargetPatch {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            collection_id: None,
            class_id: None,
            name: None,
            description: None,
            method: None,
            url_template: None,
            headers_template: None,
            body_template: None,
            auth_config: None,
            allowed_subject_types: None,
            timeout_ms: None,
            enabled: None,
        }
    }

    #[must_use]
    pub const fn with_collection_id(mut self, value: Option<CollectionId>) -> Self {
        self.collection_id = value;
        self
    }

    #[must_use]
    pub const fn with_class_id(mut self, value: Option<Option<ClassId>>) -> Self {
        self.class_id = value;
        self
    }

    #[must_use]
    pub fn with_name(mut self, value: Option<String>) -> Self {
        self.name = value;
        self
    }

    #[must_use]
    pub fn with_description(mut self, value: Option<String>) -> Self {
        self.description = value;
        self
    }

    #[must_use]
    pub fn with_method(mut self, value: Option<String>) -> Self {
        self.method = value;
        self
    }

    #[must_use]
    pub fn with_url_template(mut self, value: Option<String>) -> Self {
        self.url_template = value;
        self
    }

    #[must_use]
    pub fn with_headers_template(mut self, value: Option<Value>) -> Self {
        self.headers_template = value;
        self
    }

    #[must_use]
    pub fn with_body_template(mut self, value: Option<Option<String>>) -> Self {
        self.body_template = value;
        self
    }

    #[must_use]
    pub fn with_auth_config(mut self, value: Option<Value>) -> Self {
        self.auth_config = value;
        self
    }

    #[must_use]
    pub fn with_allowed_subject_types(mut self, value: Option<Vec<String>>) -> Self {
        self.allowed_subject_types = value;
        self
    }

    #[must_use]
    pub const fn with_timeout_ms(mut self, value: Option<i32>) -> Self {
        self.timeout_ms = value;
        self
    }

    #[must_use]
    pub const fn with_enabled(mut self, value: Option<bool>) -> Self {
        self.enabled = value;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> StorageRemoteTargetPatchParts {
        StorageRemoteTargetPatchParts {
            collection_id: self.collection_id,
            class_id: self.class_id,
            name: self.name,
            description: self.description,
            method: self.method,
            url_template: self.url_template,
            headers_template: self.headers_template,
            body_template: self.body_template,
            auth_config: self.auth_config,
            allowed_subject_types: self.allowed_subject_types,
            timeout_ms: self.timeout_ms,
            enabled: self.enabled,
        }
    }
}

impl fmt::Debug for StorageRemoteTargetPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTargetPatch")
            .field("changes_collection_id", &self.collection_id.is_some())
            .field("changes_class_id", &self.class_id.is_some())
            .field("changes_name", &self.name.is_some())
            .field("changes_description", &self.description.is_some())
            .field("changes_method", &self.method.is_some())
            .field("changes_url_template", &self.url_template.is_some())
            .field("changes_headers_template", &self.headers_template.is_some())
            .field("changes_body_template", &self.body_template.is_some())
            .field("changes_auth_config", &self.auth_config.is_some())
            .field(
                "changes_allowed_subject_types",
                &self.allowed_subject_types.is_some(),
            )
            .field("changes_timeout_ms", &self.timeout_ms.is_some())
            .field("changes_enabled", &self.enabled.is_some())
            .finish()
    }
}

/// Atomic remote-target update command including audit provenance.
#[derive(Clone, PartialEq)]
pub struct StorageRemoteTargetUpdate {
    target_id: RemoteTargetId,
    patch: StorageRemoteTargetPatch,
    event_context: EventContext,
}

impl StorageRemoteTargetUpdate {
    #[must_use]
    pub const fn new(
        target_id: RemoteTargetId,
        patch: StorageRemoteTargetPatch,
        event_context: EventContext,
    ) -> Self {
        Self {
            target_id,
            patch,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (RemoteTargetId, StorageRemoteTargetPatch, EventContext) {
        (self.target_id, self.patch, self.event_context)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageRemoteTargetDelete {
    target_id: RemoteTargetId,
    event_context: EventContext,
}

impl StorageRemoteTargetDelete {
    #[must_use]
    pub const fn new(target_id: RemoteTargetId, event_context: EventContext) -> Self {
        Self {
            target_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (RemoteTargetId, EventContext) {
        (self.target_id, self.event_context)
    }
}

/// Invocation audit command. The backend resolves target name and collection
/// from the same durable target record used for the event.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRemoteTargetInvocation {
    target_id: RemoteTargetId,
    task_id: TaskId,
    subject_type: String,
    subject_id: ResourceId,
    event_context: EventContext,
}

impl StorageRemoteTargetInvocation {
    #[must_use]
    pub fn new(
        target_id: RemoteTargetId,
        task_id: TaskId,
        subject_type: impl Into<String>,
        subject_id: ResourceId,
        event_context: EventContext,
    ) -> Self {
        Self {
            target_id,
            task_id,
            subject_type: subject_type.into(),
            subject_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (RemoteTargetId, TaskId, String, ResourceId, EventContext) {
        (
            self.target_id,
            self.task_id,
            self.subject_type,
            self.subject_id,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageRemoteTargetInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteTargetInvocation")
            .field("target_id", &self.target_id)
            .field("task_id", &self.task_id)
            .field("subject_type", &self.subject_type)
            .field("subject_id", &self.subject_id)
            .finish_non_exhaustive()
    }
}

/// Complete remote-target lifecycle required from every selectable backend.
#[async_trait]
pub trait RemoteTargetStorage: Send + Sync {
    async fn get_remote_target(
        &self,
        target_id: RemoteTargetId,
    ) -> Result<StorageRemoteTarget, StorageError>;

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StoragePage<StorageRemoteTarget>, StorageError>;

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError>;

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError>;

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<MutationOutcome<()>, StorageError>;

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<MutationOutcome<()>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition() -> StorageRemoteTargetDefinition {
        StorageRemoteTargetDefinition::new(
            "secret description",
            StorageRemoteTargetTransport::new(
                "post",
                "https://secret.invalid/{{ secret }}",
                serde_json::json!({"x-secret": "{{ secret }}"}),
                Some("{{ secret }}".to_string()),
                serde_json::json!({"type": "bearer_secret", "secret": "secret-ref"}),
                1_000,
            ),
            StorageRemoteTargetPolicy::new(None, vec!["collection".to_string()], true),
        )
    }

    #[test]
    fn debug_output_redacts_target_configuration() {
        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("valid timestamp")
            .naive_utc();
        let target = StorageRemoteTarget::new(
            StorageRecordMetadata::new(
                hubuum_domain::ResourceId::new(1).unwrap(),
                now,
                now,
                hubuum_domain::ResourceRevision::new(1).unwrap(),
            ),
            CollectionId::new(2).unwrap(),
            "secret target name",
            definition(),
        );
        let request = StorageRemoteTargetCreate::new(
            CollectionId::new(2).unwrap(),
            "secret create name",
            definition(),
            EventContext::user(hubuum_domain::PrincipalId::new(3).unwrap(), None, None),
        );

        let debug = format!("{target:?} {request:?}");

        for secret in [
            "secret target name",
            "secret create name",
            "secret description",
            "secret.invalid",
            "x-secret",
            "secret-ref",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
