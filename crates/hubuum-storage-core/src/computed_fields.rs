use std::{fmt, str::FromStr};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_computed_fields::{Definition, Operation, ResultType};
use hubuum_domain::{ClassId, CollectionId, ComputedFieldDefinitionId, PrincipalId, TaskId};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use serde_json::Value;

use crate::{
    StorageError, StorageMutationOutcome, StoragePage, StorageRecordMetadata, StorageTask,
    StorageTaskLease, StorageValidationError,
};

/// Non-negative generation of the materialized computed-field state.
///
/// Generation zero means that no shared definition has been evaluated yet;
/// unlike a resource revision, it is therefore a valid initial value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageComputationRevision(i64);

impl StorageComputationRevision {
    pub fn try_new(value: i64) -> Result<Self, StorageValidationError> {
        if value < 0 {
            return Err(StorageValidationError::invalid(
                "computation revision must not be negative",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StorageComputedFieldVisibility {
    Shared,
    Personal { owner_id: PrincipalId },
}

impl fmt::Debug for StorageComputedFieldVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Shared => "Shared",
            Self::Personal { .. } => "Personal { owner_id: [redacted] }",
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageComputedFieldDefinitionInput {
    definition: Definition,
}

impl StorageComputedFieldDefinitionInput {
    #[must_use]
    pub const fn new(definition: Definition) -> Self {
        Self { definition }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        self.definition.key().as_str()
    }

    #[must_use]
    pub fn label(&self) -> &str {
        self.definition.label()
    }

    #[must_use]
    pub fn description(&self) -> &str {
        self.definition.description()
    }

    #[must_use]
    pub fn operation(&self) -> &Operation {
        self.definition.operation()
    }

    #[must_use]
    pub const fn result_type(&self) -> ResultType {
        self.definition.result_type()
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.definition.enabled()
    }

    #[must_use]
    pub const fn semantics_version(&self) -> i16 {
        self.definition.semantics_version()
    }

    #[must_use]
    pub const fn definition(&self) -> &Definition {
        &self.definition
    }

    #[must_use]
    pub fn into_definition(self) -> Definition {
        self.definition
    }
}

impl fmt::Debug for StorageComputedFieldDefinitionInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageComputedFieldDefinitionInput")
            .field("result_type", &self.result_type())
            .field("enabled", &self.enabled())
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

#[derive(Clone, PartialEq, Eq)]
pub struct StorageComputedFieldDefinitionContent {
    definition: Definition,
}

impl StorageComputedFieldDefinitionContent {
    #[must_use]
    pub fn new(input: StorageComputedFieldDefinitionInput) -> Self {
        Self {
            definition: input.into_definition(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageComputedFieldProvenance {
    created_by: Option<PrincipalId>,
    updated_by: Option<PrincipalId>,
}

impl StorageComputedFieldProvenance {
    #[must_use]
    pub const fn new(created_by: Option<PrincipalId>, updated_by: Option<PrincipalId>) -> Self {
        Self {
            created_by,
            updated_by,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageComputedFieldDefinition {
    metadata: StorageRecordMetadata,
    class_id: ClassId,
    visibility: StorageComputedFieldVisibility,
    content: StorageComputedFieldDefinitionContent,
    provenance: StorageComputedFieldProvenance,
}

impl StorageComputedFieldDefinition {
    #[must_use]
    pub const fn new(
        metadata: StorageRecordMetadata,
        class_id: ClassId,
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
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub const fn visibility(&self) -> StorageComputedFieldVisibility {
        self.visibility
    }

    #[must_use]
    pub fn key(&self) -> &str {
        self.content.definition.key().as_str()
    }

    #[must_use]
    pub fn label(&self) -> &str {
        self.content.definition.label()
    }

    #[must_use]
    pub fn description(&self) -> &str {
        self.content.definition.description()
    }

    #[must_use]
    pub fn operation(&self) -> &Operation {
        self.content.definition.operation()
    }

    #[must_use]
    pub const fn result_type(&self) -> ResultType {
        self.content.definition.result_type()
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.content.definition.enabled()
    }

    #[must_use]
    pub const fn semantics_version(&self) -> i16 {
        self.content.definition.semantics_version()
    }

    #[must_use]
    pub const fn evaluator_definition(&self) -> &Definition {
        &self.content.definition
    }

    #[must_use]
    pub const fn created_by(&self) -> Option<PrincipalId> {
        self.provenance.created_by
    }

    #[must_use]
    pub const fn updated_by(&self) -> Option<PrincipalId> {
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
            .field("result_type", &self.content.definition.result_type())
            .field("enabled", &self.content.definition.enabled())
            .field(
                "semantics_version",
                &self.content.definition.semantics_version(),
            )
            .field("content", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// Durable computed-field rebuild lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageComputationRebuildStatus {
    Ready,
    Rebuilding,
    Failed,
}

impl StorageComputationRebuildStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Rebuilding => "rebuilding",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for StorageComputationRebuildStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for StorageComputationRebuildStatus {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ready" => Ok(Self::Ready),
            "rebuilding" => Ok(Self::Rebuilding),
            "failed" => Ok(Self::Failed),
            _ => Err(StorageError::invalid_input(format!(
                "Unsupported computation rebuild status '{value}'"
            ))),
        }
    }
}

/// Correlated computed-field rebuild state.
///
/// Each variant carries exactly the data that is meaningful for that state, so
/// callers cannot construct a rebuilding state without a task or a failed state
/// without an error.
#[derive(Clone, PartialEq, Eq)]
pub enum StorageComputationRebuildState {
    Ready,
    Rebuilding { active_task_id: TaskId },
    Failed { last_error: String },
}

impl StorageComputationRebuildState {
    pub fn try_from_parts(
        status: StorageComputationRebuildStatus,
        active_task_id: Option<TaskId>,
        last_error: Option<String>,
    ) -> Result<Self, StorageValidationError> {
        match (status, active_task_id, last_error) {
            (StorageComputationRebuildStatus::Ready, None, None) => Ok(Self::Ready),
            (StorageComputationRebuildStatus::Rebuilding, Some(active_task_id), None) => {
                Ok(Self::Rebuilding { active_task_id })
            }
            (StorageComputationRebuildStatus::Failed, None, Some(last_error)) => {
                Ok(Self::Failed { last_error })
            }
            _ => Err(StorageValidationError::invalid(
                "Computation rebuild status, active task, and last error are inconsistent",
            )),
        }
    }

    #[must_use]
    pub const fn status(&self) -> StorageComputationRebuildStatus {
        match self {
            Self::Ready => StorageComputationRebuildStatus::Ready,
            Self::Rebuilding { .. } => StorageComputationRebuildStatus::Rebuilding,
            Self::Failed { .. } => StorageComputationRebuildStatus::Failed,
        }
    }

    #[must_use]
    pub const fn active_task_id(&self) -> Option<TaskId> {
        match self {
            Self::Rebuilding { active_task_id } => Some(*active_task_id),
            Self::Ready | Self::Failed { .. } => None,
        }
    }

    #[must_use]
    pub fn last_error_message(&self) -> Option<&str> {
        match self {
            Self::Failed { last_error } => Some(last_error),
            Self::Ready | Self::Rebuilding { .. } => None,
        }
    }
}

impl fmt::Debug for StorageComputationRebuildState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageComputationRebuildState")
            .field("status", &self.status().as_str())
            .field("has_active_task", &self.active_task_id().is_some())
            .field("has_last_error", &self.last_error_message().is_some())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageClassComputationState {
    class_id: ClassId,
    evaluation_revision: StorageComputationRevision,
    rebuild_state: StorageComputationRebuildState,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl StorageClassComputationState {
    pub fn try_new(
        class_id: ClassId,
        evaluation_revision: StorageComputationRevision,
        rebuild_state: StorageComputationRebuildState,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "Computation state updated_at must not precede created_at",
            ));
        }
        Ok(Self {
            class_id,
            evaluation_revision,
            rebuild_state,
            created_at,
            updated_at,
        })
    }

    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub const fn evaluation_revision(&self) -> StorageComputationRevision {
        self.evaluation_revision
    }

    #[must_use]
    pub const fn rebuild_status(&self) -> StorageComputationRebuildStatus {
        self.rebuild_state.status()
    }

    #[must_use]
    pub const fn rebuild_state(&self) -> &StorageComputationRebuildState {
        &self.rebuild_state
    }

    #[must_use]
    pub const fn active_task_id(&self) -> Option<TaskId> {
        self.rebuild_state.active_task_id()
    }

    #[must_use]
    pub fn last_error_message(&self) -> Option<&str> {
        self.rebuild_state.last_error_message()
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

impl fmt::Debug for StorageClassComputationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageClassComputationState")
            .field("evaluation_revision", &self.evaluation_revision)
            .field("rebuild_state", &self.rebuild_state)
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
    owner_id: PrincipalId,
    class_id: Option<ClassId>,
    options: QueryOptions,
}

impl StoragePersonalComputedFieldListQuery {
    #[must_use]
    pub const fn new(
        owner_id: PrincipalId,
        class_id: Option<ClassId>,
        options: QueryOptions,
    ) -> Self {
        Self {
            owner_id,
            class_id,
            options,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (PrincipalId, Option<ClassId>, QueryOptions) {
        (self.owner_id, self.class_id, self.options)
    }
}

impl fmt::Debug for StoragePersonalComputedFieldListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoragePersonalComputedFieldListQuery")
            .field("has_class_filter", &self.class_id.is_some())
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("limit", &self.options.limit())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("include_total", &self.options.include_total())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageSharedComputedFieldCreate {
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    actor_id: PrincipalId,
    definition: StorageComputedFieldDefinitionInput,
    event_context: EventContext,
}

impl StorageSharedComputedFieldCreate {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        authorized_collection_id: CollectionId,
        actor_id: PrincipalId,
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
        ClassId,
        CollectionId,
        PrincipalId,
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
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    definition_id: ComputedFieldDefinitionId,
    actor_id: PrincipalId,
    patch: StorageComputedFieldDefinitionPatch,
    event_context: EventContext,
}

impl StorageSharedComputedFieldUpdate {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        authorized_collection_id: CollectionId,
        definition_id: ComputedFieldDefinitionId,
        actor_id: PrincipalId,
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
        ClassId,
        CollectionId,
        ComputedFieldDefinitionId,
        PrincipalId,
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
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    definition_id: ComputedFieldDefinitionId,
    actor_id: PrincipalId,
    event_context: EventContext,
}

impl StorageSharedComputedFieldDelete {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        authorized_collection_id: CollectionId,
        definition_id: ComputedFieldDefinitionId,
        actor_id: PrincipalId,
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
    pub fn into_parts(
        self,
    ) -> (
        ClassId,
        CollectionId,
        ComputedFieldDefinitionId,
        PrincipalId,
        EventContext,
    ) {
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
    class_id: ClassId,
    owner_id: PrincipalId,
    definition: StorageComputedFieldDefinitionInput,
    event_context: EventContext,
}

impl StoragePersonalComputedFieldCreate {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        owner_id: PrincipalId,
        definition: StorageComputedFieldDefinitionInput,
        event_context: EventContext,
    ) -> Self {
        Self {
            class_id,
            owner_id,
            definition,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ClassId,
        PrincipalId,
        StorageComputedFieldDefinitionInput,
        EventContext,
    ) {
        (
            self.class_id,
            self.owner_id,
            self.definition,
            self.event_context,
        )
    }
}

#[derive(Clone, PartialEq)]
pub struct StoragePersonalComputedFieldUpdate {
    owner_id: PrincipalId,
    definition_id: ComputedFieldDefinitionId,
    patch: StorageComputedFieldDefinitionPatch,
    event_context: EventContext,
}

impl StoragePersonalComputedFieldUpdate {
    #[must_use]
    pub const fn new(
        owner_id: PrincipalId,
        definition_id: ComputedFieldDefinitionId,
        patch: StorageComputedFieldDefinitionPatch,
        event_context: EventContext,
    ) -> Self {
        Self {
            owner_id,
            definition_id,
            patch,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        PrincipalId,
        ComputedFieldDefinitionId,
        StorageComputedFieldDefinitionPatch,
        EventContext,
    ) {
        (
            self.owner_id,
            self.definition_id,
            self.patch,
            self.event_context,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StoragePersonalComputedFieldDelete {
    owner_id: PrincipalId,
    definition_id: ComputedFieldDefinitionId,
    event_context: EventContext,
}

impl StoragePersonalComputedFieldDelete {
    #[must_use]
    pub const fn new(
        owner_id: PrincipalId,
        definition_id: ComputedFieldDefinitionId,
        event_context: EventContext,
    ) -> Self {
        Self {
            owner_id,
            definition_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (PrincipalId, ComputedFieldDefinitionId, EventContext) {
        (self.owner_id, self.definition_id, self.event_context)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageComputedFieldRebuildRequest {
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    actor_id: Option<PrincipalId>,
}

impl StorageComputedFieldRebuildRequest {
    #[must_use]
    pub const fn new(
        class_id: ClassId,
        authorized_collection_id: CollectionId,
        actor_id: Option<PrincipalId>,
    ) -> Self {
        Self {
            class_id,
            authorized_collection_id,
            actor_id,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (ClassId, CollectionId, Option<PrincipalId>) {
        (self.class_id, self.authorized_collection_id, self.actor_id)
    }
}

/// Mandatory backend contract for computed-field definitions and rebuild state.
#[async_trait]
pub trait ComputedFieldStorage: Send + Sync {
    async fn get_computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError>;

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError>;

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StoragePage<StorageComputedFieldDefinition>, StorageError>;

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError>;

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError>;

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError>;

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<StorageClassComputationState>, StorageError>;

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError>;

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError>;

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError>;

    /// Execute the backend-owned materialization workflow for a claimed
    /// computed-field rebuild task.
    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_debug_redacts_identifiers_and_expression_content() {
        let now = chrono::Utc::now();
        let definition = StorageComputedFieldDefinition::new(
            StorageRecordMetadata::try_new(
                hubuum_domain::ResourceId::new(71).unwrap(),
                now,
                now,
                hubuum_domain::ResourceRevision::new(5).unwrap(),
            )
            .unwrap(),
            ClassId::new(72).unwrap(),
            StorageComputedFieldVisibility::Personal {
                owner_id: PrincipalId::new(73).unwrap(),
            },
            StorageComputedFieldDefinitionContent::new(StorageComputedFieldDefinitionInput::new(
                Definition::new(
                    hubuum_computed_fields::FieldKey::new("secret_key").unwrap(),
                    "secret label",
                    "secret description",
                    hubuum_computed_fields::Operation::Sum {
                        paths: vec![hubuum_computed_fields::JsonPointer::new("/secret").unwrap()],
                    },
                    ResultType::Number,
                    true,
                )
                .unwrap(),
            )),
            StorageComputedFieldProvenance::new(
                PrincipalId::new(74).ok(),
                PrincipalId::new(75).ok(),
            ),
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
        let now = chrono::Utc::now();
        let failed_state = StorageClassComputationState::try_new(
            ClassId::new(98_765).unwrap(),
            StorageComputationRevision::try_new(7).unwrap(),
            StorageComputationRebuildState::Failed {
                last_error: "secret database detail".to_string(),
            },
            now,
            now,
        )
        .unwrap();
        let rebuilding_state = StorageClassComputationState::try_new(
            ClassId::new(98_765).unwrap(),
            StorageComputationRevision::try_new(7).unwrap(),
            StorageComputationRebuildState::Rebuilding {
                active_task_id: TaskId::new(87_654).unwrap(),
            },
            now,
            now,
        )
        .unwrap();

        let debug = format!("{failed_state:?} {rebuilding_state:?}");

        assert!(!debug.contains("98765"));
        assert!(!debug.contains("87654"));
        assert!(!debug.contains("secret database detail"));
        assert!(debug.contains("failed"));
        assert!(debug.contains("has_active_task: true"));
        assert!(debug.contains("has_last_error: true"));
    }

    #[test]
    fn computation_rebuild_state_rejects_inconsistent_parts() {
        let task_id = TaskId::new(42).unwrap();

        assert!(
            StorageComputationRebuildState::try_from_parts(
                StorageComputationRebuildStatus::Ready,
                Some(task_id),
                None,
            )
            .is_err()
        );
        assert!(
            StorageComputationRebuildState::try_from_parts(
                StorageComputationRebuildStatus::Rebuilding,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            StorageComputationRebuildState::try_from_parts(
                StorageComputationRebuildStatus::Failed,
                None,
                None,
            )
            .is_err()
        );
    }
}
