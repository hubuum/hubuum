use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{StorageError, StorageRecordMetadata};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StorageComputedFieldVisibility {
    Shared,
    Personal { owner_id: i32 },
}

impl fmt::Debug for StorageComputedFieldVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Shared => "Shared",
            Self::Personal { .. } => "Personal { owner_id: [redacted] }",
        })
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageComputedFieldDefinitionInput {
    key: String,
    label: String,
    description: String,
    operation: Value,
    result_type: String,
    enabled: bool,
}

impl StorageComputedFieldDefinitionInput {
    #[must_use]
    pub fn new(key: String, label: String, operation: Value, result_type: String) -> Self {
        Self {
            key,
            label,
            description: String::new(),
            operation,
            result_type,
            enabled: true,
        }
    }

    #[must_use]
    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }

    #[must_use]
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn operation(&self) -> &Value {
        &self.operation
    }

    #[must_use]
    pub fn result_type(&self) -> &str {
        &self.result_type
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
}

impl fmt::Debug for StorageComputedFieldDefinitionInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageComputedFieldDefinitionInput")
            .field("result_type", &self.result_type)
            .field("enabled", &self.enabled)
            .field("operation", &"[redacted]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default, PartialEq)]
pub struct StorageComputedFieldDefinitionPatch {
    key: Option<String>,
    label: Option<String>,
    description: Option<String>,
    operation: Option<Value>,
    result_type: Option<String>,
    enabled: Option<bool>,
}

impl StorageComputedFieldDefinitionPatch {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            key: None,
            label: None,
            description: None,
            operation: None,
            result_type: None,
            enabled: None,
        }
    }

    #[must_use]
    pub fn with_key(mut self, key: Option<String>) -> Self {
        self.key = key;
        self
    }

    #[must_use]
    pub fn with_label(mut self, label: Option<String>) -> Self {
        self.label = label;
        self
    }

    #[must_use]
    pub fn with_description(mut self, description: Option<String>) -> Self {
        self.description = description;
        self
    }

    #[must_use]
    pub fn with_operation(mut self, operation: Option<Value>) -> Self {
        self.operation = operation;
        self
    }

    #[must_use]
    pub fn with_result_type(mut self, result_type: Option<String>) -> Self {
        self.result_type = result_type;
        self
    }

    #[must_use]
    pub const fn with_enabled(mut self, enabled: Option<bool>) -> Self {
        self.enabled = enabled;
        self
    }

    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    #[must_use]
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub const fn operation(&self) -> Option<&Value> {
        self.operation.as_ref()
    }

    #[must_use]
    pub fn result_type(&self) -> Option<&str> {
        self.result_type.as_deref()
    }

    #[must_use]
    pub const fn enabled(&self) -> Option<bool> {
        self.enabled
    }
}

impl fmt::Debug for StorageComputedFieldDefinitionPatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageComputedFieldDefinitionPatch")
            .field("changes_key", &self.key.is_some())
            .field("changes_label", &self.label.is_some())
            .field("changes_description", &self.description.is_some())
            .field("changes_operation", &self.operation.is_some())
            .field("result_type", &self.result_type)
            .field("enabled", &self.enabled)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageComputedFieldDefinitionContent {
    key: String,
    label: String,
    description: String,
    operation: Value,
    result_type: String,
    enabled: bool,
    semantics_version: i16,
}

impl StorageComputedFieldDefinitionContent {
    #[must_use]
    pub fn new(input: StorageComputedFieldDefinitionInput, semantics_version: i16) -> Self {
        Self {
            key: input.key,
            label: input.label,
            description: input.description,
            operation: input.operation,
            result_type: input.result_type,
            enabled: input.enabled,
            semantics_version,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageComputedFieldProvenance {
    created_by: Option<i32>,
    updated_by: Option<i32>,
}

impl StorageComputedFieldProvenance {
    #[must_use]
    pub const fn new(created_by: Option<i32>, updated_by: Option<i32>) -> Self {
        Self {
            created_by,
            updated_by,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageComputedFieldDefinition {
    metadata: StorageRecordMetadata,
    class_id: i32,
    visibility: StorageComputedFieldVisibility,
    content: StorageComputedFieldDefinitionContent,
    provenance: StorageComputedFieldProvenance,
}

impl StorageComputedFieldDefinition {
    #[must_use]
    pub const fn new(
        metadata: StorageRecordMetadata,
        class_id: i32,
        visibility: StorageComputedFieldVisibility,
        content: StorageComputedFieldDefinitionContent,
        provenance: StorageComputedFieldProvenance,
    ) -> Self {
        Self {
            metadata,
            class_id,
            visibility,
            content,
            provenance,
        }
    }

    #[must_use]
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn class_id(&self) -> i32 {
        self.class_id
    }

    #[must_use]
    pub const fn visibility(&self) -> StorageComputedFieldVisibility {
        self.visibility
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.content.key
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.content.label
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.content.description
    }

    #[must_use]
    pub const fn operation(&self) -> &Value {
        &self.content.operation
    }

    #[must_use]
    pub fn result_type(&self) -> &str {
        &self.content.result_type
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.content.enabled
    }

    #[must_use]
    pub const fn semantics_version(&self) -> i16 {
        self.content.semantics_version
    }

    #[must_use]
    pub const fn created_by(&self) -> Option<i32> {
        self.provenance.created_by
    }

    #[must_use]
    pub const fn updated_by(&self) -> Option<i32> {
        self.provenance.updated_by
    }
}

impl fmt::Debug for StorageComputedFieldDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let visibility = match self.visibility {
            StorageComputedFieldVisibility::Shared => "shared",
            StorageComputedFieldVisibility::Personal { .. } => "personal",
        };
        formatter
            .debug_struct("StorageComputedFieldDefinition")
            .field("visibility", &visibility)
            .field("result_type", &self.content.result_type)
            .field("enabled", &self.content.enabled)
            .field("semantics_version", &self.content.semantics_version)
            .field("content", &"[redacted]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageClassComputationState {
    class_id: i32,
    evaluation_revision: i64,
    rebuild_status: String,
    active_task_id: Option<i32>,
    last_error: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl StorageClassComputationState {
    #[must_use]
    pub fn new(
        class_id: i32,
        evaluation_revision: i64,
        rebuild_status: String,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> Self {
        Self {
            class_id,
            evaluation_revision,
            rebuild_status,
            active_task_id: None,
            last_error: None,
            created_at,
            updated_at,
        }
    }

    #[must_use]
    pub const fn active_task(mut self, active_task_id: Option<i32>) -> Self {
        self.active_task_id = active_task_id;
        self
    }

    #[must_use]
    pub fn last_error(mut self, last_error: Option<String>) -> Self {
        self.last_error = last_error;
        self
    }

    #[must_use]
    pub const fn class_id(&self) -> i32 {
        self.class_id
    }

    #[must_use]
    pub const fn evaluation_revision(&self) -> i64 {
        self.evaluation_revision
    }

    #[must_use]
    pub fn rebuild_status(&self) -> &str {
        &self.rebuild_status
    }

    #[must_use]
    pub const fn active_task_id(&self) -> Option<i32> {
        self.active_task_id
    }

    #[must_use]
    pub fn last_error_message(&self) -> Option<&str> {
        self.last_error.as_deref()
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

impl fmt::Debug for StorageClassComputationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageClassComputationState")
            .field("evaluation_revision", &self.evaluation_revision)
            .field("rebuild_status", &self.rebuild_status)
            .field("has_active_task", &self.active_task_id.is_some())
            .field("has_last_error", &self.last_error.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageComputedFieldMutation {
    definition: StorageComputedFieldDefinition,
    state: StorageClassComputationState,
}

impl StorageComputedFieldMutation {
    #[must_use]
    pub const fn new(
        definition: StorageComputedFieldDefinition,
        state: StorageClassComputationState,
    ) -> Self {
        Self { definition, state }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageComputedFieldDefinition, StorageClassComputationState) {
        (self.definition, self.state)
    }
}

#[derive(Clone, PartialEq)]
pub struct StoragePersonalComputedFieldListQuery {
    owner_id: i32,
    class_id: Option<i32>,
    options: QueryOptions,
}

impl StoragePersonalComputedFieldListQuery {
    #[must_use]
    pub const fn new(owner_id: i32, class_id: Option<i32>, options: QueryOptions) -> Self {
        Self {
            owner_id,
            class_id,
            options,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, Option<i32>, QueryOptions) {
        (self.owner_id, self.class_id, self.options)
    }
}

impl fmt::Debug for StoragePersonalComputedFieldListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoragePersonalComputedFieldListQuery")
            .field("has_class_filter", &self.class_id.is_some())
            .field("filter_count", &self.options.filters.len())
            .field("sort_count", &self.options.sort.len())
            .field("limit", &self.options.limit)
            .field("has_cursor", &self.options.cursor.is_some())
            .field("include_total", &self.options.include_total)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageComputedFieldPage {
    definitions: Vec<StorageComputedFieldDefinition>,
    total: Option<i64>,
}

impl StorageComputedFieldPage {
    #[must_use]
    pub const fn new(definitions: Vec<StorageComputedFieldDefinition>, total: Option<i64>) -> Self {
        Self { definitions, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageComputedFieldDefinition>, Option<i64>) {
        (self.definitions, self.total)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageSharedComputedFieldCreate {
    class_id: i32,
    authorized_collection_id: i32,
    actor_id: i32,
    definition: StorageComputedFieldDefinitionInput,
    event_context: EventContext,
}

impl StorageSharedComputedFieldCreate {
    #[must_use]
    pub const fn new(
        class_id: i32,
        authorized_collection_id: i32,
        actor_id: i32,
        definition: StorageComputedFieldDefinitionInput,
        event_context: EventContext,
    ) -> Self {
        Self {
            class_id,
            authorized_collection_id,
            actor_id,
            definition,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        i32,
        i32,
        StorageComputedFieldDefinitionInput,
        EventContext,
    ) {
        (
            self.class_id,
            self.authorized_collection_id,
            self.actor_id,
            self.definition,
            self.event_context,
        )
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageSharedComputedFieldUpdate {
    class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    patch: StorageComputedFieldDefinitionPatch,
    event_context: EventContext,
}

impl StorageSharedComputedFieldUpdate {
    #[must_use]
    pub const fn new(
        class_id: i32,
        authorized_collection_id: i32,
        definition_id: i32,
        actor_id: i32,
        patch: StorageComputedFieldDefinitionPatch,
        event_context: EventContext,
    ) -> Self {
        Self {
            class_id,
            authorized_collection_id,
            definition_id,
            actor_id,
            patch,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        i32,
        i32,
        i32,
        StorageComputedFieldDefinitionPatch,
        EventContext,
    ) {
        (
            self.class_id,
            self.authorized_collection_id,
            self.definition_id,
            self.actor_id,
            self.patch,
            self.event_context,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageSharedComputedFieldDelete {
    class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    event_context: EventContext,
}

impl StorageSharedComputedFieldDelete {
    #[must_use]
    pub const fn new(
        class_id: i32,
        authorized_collection_id: i32,
        definition_id: i32,
        actor_id: i32,
        event_context: EventContext,
    ) -> Self {
        Self {
            class_id,
            authorized_collection_id,
            definition_id,
            actor_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i32, i32, i32, EventContext) {
        (
            self.class_id,
            self.authorized_collection_id,
            self.definition_id,
            self.actor_id,
            self.event_context,
        )
    }
}

#[derive(Clone, PartialEq)]
pub struct StoragePersonalComputedFieldCreate {
    class_id: i32,
    owner_id: i32,
    definition: StorageComputedFieldDefinitionInput,
}

impl StoragePersonalComputedFieldCreate {
    #[must_use]
    pub const fn new(
        class_id: i32,
        owner_id: i32,
        definition: StorageComputedFieldDefinitionInput,
    ) -> Self {
        Self {
            class_id,
            owner_id,
            definition,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i32, StorageComputedFieldDefinitionInput) {
        (self.class_id, self.owner_id, self.definition)
    }
}

#[derive(Clone, PartialEq)]
pub struct StoragePersonalComputedFieldUpdate {
    owner_id: i32,
    definition_id: i32,
    patch: StorageComputedFieldDefinitionPatch,
}

impl StoragePersonalComputedFieldUpdate {
    #[must_use]
    pub const fn new(
        owner_id: i32,
        definition_id: i32,
        patch: StorageComputedFieldDefinitionPatch,
    ) -> Self {
        Self {
            owner_id,
            definition_id,
            patch,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i32, StorageComputedFieldDefinitionPatch) {
        (self.owner_id, self.definition_id, self.patch)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StoragePersonalComputedFieldDelete {
    owner_id: i32,
    definition_id: i32,
}

impl StoragePersonalComputedFieldDelete {
    #[must_use]
    pub const fn new(owner_id: i32, definition_id: i32) -> Self {
        Self {
            owner_id,
            definition_id,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, i32) {
        (self.owner_id, self.definition_id)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageComputedFieldRebuildRequest {
    class_id: i32,
    authorized_collection_id: i32,
    actor_id: Option<i32>,
}

impl StorageComputedFieldRebuildRequest {
    #[must_use]
    pub const fn new(class_id: i32, authorized_collection_id: i32, actor_id: Option<i32>) -> Self {
        Self {
            class_id,
            authorized_collection_id,
            actor_id,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, i32, Option<i32>) {
        (self.class_id, self.authorized_collection_id, self.actor_id)
    }
}

/// Mandatory backend contract for computed-field definitions and rebuild state.
#[async_trait]
pub trait ComputedFieldLifecycleStorage: Send + Sync {
    async fn computed_field_state(
        &self,
        class_id: i32,
    ) -> Result<StorageClassComputationState, StorageError>;

    async fn list_shared_computed_fields(
        &self,
        class_id: i32,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError>;

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StorageComputedFieldPage, StorageError>;

    async fn get_computed_field(
        &self,
        definition_id: i32,
    ) -> Result<StorageComputedFieldDefinition, StorageError>;

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageComputedFieldMutation, StorageError>;

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageComputedFieldMutation, StorageError>;

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageClassComputationState, StorageError>;

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageComputedFieldDefinition, StorageError>;

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageComputedFieldDefinition, StorageError>;

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<(), StorageError>;

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_debug_redacts_identifiers_and_expression_content() {
        let now = chrono::Utc::now().naive_utc();
        let definition = StorageComputedFieldDefinition::new(
            StorageRecordMetadata::new(71, now, now, 5),
            72,
            StorageComputedFieldVisibility::Personal { owner_id: 73 },
            StorageComputedFieldDefinitionContent::new(
                StorageComputedFieldDefinitionInput::new(
                    "secret_key".to_string(),
                    "secret label".to_string(),
                    serde_json::json!({"secret": "expression"}),
                    "number".to_string(),
                )
                .with_description("secret description".to_string()),
                4,
            ),
            StorageComputedFieldProvenance::new(Some(74), Some(75)),
        );

        let debug = format!("{definition:?}");

        for secret in [
            "71",
            "72",
            "73",
            "74",
            "75",
            "secret_key",
            "secret label",
            "secret description",
            "expression",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn computation_state_debug_redacts_resource_task_and_error_details() {
        let now = chrono::Utc::now().naive_utc();
        let state = StorageClassComputationState::new(98_765, 7, "failed".to_string(), now, now)
            .active_task(Some(87_654))
            .last_error(Some("secret database detail".to_string()));

        let debug = format!("{state:?}");

        assert!(!debug.contains("98765"));
        assert!(!debug.contains("87654"));
        assert!(!debug.contains("secret database detail"));
        assert!(debug.contains("failed"));
        assert!(debug.contains("has_active_task: true"));
        assert!(debug.contains("has_last_error: true"));
    }
}
