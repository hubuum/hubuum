use async_trait::async_trait;

use crate::models::{
    Collection, CollectionKey, HubuumClass, HubuumObject, ImportClassInput,
    ImportClassRelationInput, ImportCollectionInput, ImportCollectionPermissionInput,
    ImportComputedFieldInput, ImportEventSinkInput, ImportEventSubscriptionInput,
    ImportExportTemplateInput, ImportGroupInput, ImportGroupMembershipInput,
    ImportIdentityScopeInput, ImportMode, ImportObjectInput, ImportObjectRelationInput,
    ImportPrincipalInput, ImportRemoteTargetInput, RestoreTimestamps,
};

use super::StorageError;

/// Durable import result supplied by the application after planning or apply.
#[derive(Clone, PartialEq)]
pub(crate) struct StorageImportResult {
    task_id: i32,
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    outcome: String,
    error: Option<String>,
    details: Option<serde_json::Value>,
}

impl StorageImportResult {
    pub(crate) fn builder(
        task_id: i32,
        entity_kind: impl Into<String>,
        action: impl Into<String>,
        outcome: impl Into<String>,
    ) -> StorageImportResultBuilder {
        StorageImportResultBuilder {
            result: Self {
                task_id,
                item_ref: None,
                entity_kind: entity_kind.into(),
                action: action.into(),
                identifier: None,
                outcome: outcome.into(),
                error: None,
                details: None,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        i32,
        Option<String>,
        String,
        String,
        Option<String>,
        String,
        Option<String>,
        Option<serde_json::Value>,
    ) {
        (
            self.task_id,
            self.item_ref,
            self.entity_kind,
            self.action,
            self.identifier,
            self.outcome,
            self.error,
            self.details,
        )
    }
}

impl std::fmt::Debug for StorageImportResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageImportResult")
            .field("task_id", &"[redacted]")
            .field("has_item_ref", &self.item_ref.is_some())
            .field("entity_kind", &self.entity_kind)
            .field("action", &self.action)
            .field("outcome", &self.outcome)
            .field("has_identifier", &self.identifier.is_some())
            .field("has_error", &self.error.is_some())
            .field("has_details", &self.details.is_some())
            .finish()
    }
}

pub(crate) struct StorageImportResultBuilder {
    result: StorageImportResult,
}

impl StorageImportResultBuilder {
    pub(crate) fn item_ref(mut self, item_ref: Option<String>) -> Self {
        self.result.item_ref = item_ref;
        self
    }

    pub(crate) fn identifier(mut self, identifier: Option<String>) -> Self {
        self.result.identifier = identifier;
        self
    }

    pub(crate) fn error(mut self, error: Option<String>) -> Self {
        self.result.error = error;
        self
    }

    pub(crate) fn details(mut self, details: Option<serde_json::Value>) -> Self {
        self.result.details = details;
        self
    }

    pub(crate) fn build(self) -> StorageImportResult {
        self.result
    }
}

/// A backend-neutral import command produced by application planning.
///
/// The enum is intentionally exhaustive: a selectable backend cannot claim
/// import compatibility without handling every import entity and mutation.
#[derive(Clone)]
pub(crate) enum StorageImportOperation {
    UpsertIdentityScope {
        input: ImportIdentityScopeInput,
        overwrite: bool,
    },
    UpsertGroup {
        input: ImportGroupInput,
        overwrite: bool,
    },
    UpsertPrincipal {
        input: ImportPrincipalInput,
        overwrite: bool,
    },
    UpsertGroupMembership {
        input: ImportGroupMembershipInput,
        overwrite: bool,
    },
    CreateCollection(ImportCollectionInput),
    UpdateCollection {
        collection_id: i32,
        input: ImportCollectionInput,
    },
    CreateClass(ImportClassInput),
    UpdateClass {
        class_id: i32,
        input: ImportClassInput,
    },
    CreateObject(ImportObjectInput),
    UpdateObject {
        object_id: i32,
        input: ImportObjectInput,
    },
    UpsertComputedField {
        input: ImportComputedFieldInput,
        overwrite: bool,
    },
    CreateClassRelation(ImportClassRelationInput),
    UpdateClassRelationTimestamps {
        input: ImportClassRelationInput,
        timestamps: RestoreTimestamps,
    },
    CheckClassRelationCondition(ImportClassRelationInput),
    CreateObjectRelation(ImportObjectRelationInput),
    UpdateObjectRelationTimestamps {
        input: ImportObjectRelationInput,
        timestamps: RestoreTimestamps,
    },
    CheckObjectRelationCondition(ImportObjectRelationInput),
    ApplyCollectionPermissions {
        input: ImportCollectionPermissionInput,
        overwrite: bool,
    },
    UpsertExportTemplate {
        input: ImportExportTemplateInput,
        overwrite: bool,
    },
    UpsertRemoteTarget {
        input: ImportRemoteTargetInput,
        overwrite: bool,
    },
    UpsertEventSink {
        input: ImportEventSinkInput,
        overwrite: bool,
    },
    UpsertEventSubscription {
        input: ImportEventSubscriptionInput,
        overwrite: bool,
    },
}

impl StorageImportOperation {
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::UpsertIdentityScope { .. } => "upsert_identity_scope",
            Self::UpsertGroup { .. } => "upsert_group",
            Self::UpsertPrincipal { .. } => "upsert_principal",
            Self::UpsertGroupMembership { .. } => "upsert_group_membership",
            Self::CreateCollection(_) => "create_collection",
            Self::UpdateCollection { .. } => "update_collection",
            Self::CreateClass(_) => "create_class",
            Self::UpdateClass { .. } => "update_class",
            Self::CreateObject(_) => "create_object",
            Self::UpdateObject { .. } => "update_object",
            Self::UpsertComputedField { .. } => "upsert_computed_field",
            Self::CreateClassRelation(_) => "create_class_relation",
            Self::UpdateClassRelationTimestamps { .. } => "update_class_relation_timestamps",
            Self::CheckClassRelationCondition(_) => "check_class_relation_condition",
            Self::CreateObjectRelation(_) => "create_object_relation",
            Self::UpdateObjectRelationTimestamps { .. } => "update_object_relation_timestamps",
            Self::CheckObjectRelationCondition(_) => "check_object_relation_condition",
            Self::ApplyCollectionPermissions { .. } => "apply_collection_permissions",
            Self::UpsertExportTemplate { .. } => "upsert_export_template",
            Self::UpsertRemoteTarget { .. } => "upsert_remote_target",
            Self::UpsertEventSink { .. } => "upsert_event_sink",
            Self::UpsertEventSubscription { .. } => "upsert_event_subscription",
        }
    }

    const fn overwrite(&self) -> Option<bool> {
        match self {
            Self::UpsertIdentityScope { overwrite, .. }
            | Self::UpsertGroup { overwrite, .. }
            | Self::UpsertPrincipal { overwrite, .. }
            | Self::UpsertGroupMembership { overwrite, .. }
            | Self::UpsertComputedField { overwrite, .. }
            | Self::ApplyCollectionPermissions { overwrite, .. }
            | Self::UpsertExportTemplate { overwrite, .. }
            | Self::UpsertRemoteTarget { overwrite, .. }
            | Self::UpsertEventSink { overwrite, .. }
            | Self::UpsertEventSubscription { overwrite, .. } => Some(*overwrite),
            Self::CreateCollection(_)
            | Self::UpdateCollection { .. }
            | Self::CreateClass(_)
            | Self::UpdateClass { .. }
            | Self::CreateObject(_)
            | Self::UpdateObject { .. }
            | Self::CreateClassRelation(_)
            | Self::UpdateClassRelationTimestamps { .. }
            | Self::CheckClassRelationCondition(_)
            | Self::CreateObjectRelation(_)
            | Self::UpdateObjectRelationTimestamps { .. }
            | Self::CheckObjectRelationCondition(_) => None,
        }
    }
}

impl std::fmt::Debug for StorageImportOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageImportOperation")
            .field("kind", &self.kind())
            .field("overwrite", &self.overwrite())
            .field("payload", &"[redacted]")
            .finish()
    }
}

/// One indexed operation in an import plan.
#[derive(Clone, Debug)]
pub(crate) struct StorageImportPlanItem {
    index: usize,
    operation: StorageImportOperation,
}

impl StorageImportPlanItem {
    pub(crate) const fn new(index: usize, operation: StorageImportOperation) -> Self {
        Self { index, operation }
    }

    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    pub(crate) const fn operation(&self) -> &StorageImportOperation {
        &self.operation
    }
}

/// Per-item dry-run result returned from a rollback-only backend transaction.
#[derive(Debug)]
pub(crate) struct StorageImportPreflightItem {
    index: usize,
    observed_revision: Option<crate::models::ResourceRevision>,
    error: Option<StorageError>,
}

impl StorageImportPreflightItem {
    pub(crate) const fn success(
        index: usize,
        observed_revision: Option<crate::models::ResourceRevision>,
    ) -> Self {
        Self {
            index,
            observed_revision,
            error: None,
        }
    }

    pub(crate) const fn failure(
        index: usize,
        observed_revision: Option<crate::models::ResourceRevision>,
        error: StorageError,
    ) -> Self {
        Self {
            index,
            observed_revision,
            error: Some(error),
        }
    }

    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        usize,
        Option<crate::models::ResourceRevision>,
        Option<StorageError>,
    ) {
        (self.index, self.observed_revision, self.error)
    }
}

#[derive(Debug)]
pub(crate) struct StorageImportPreflight {
    items: Vec<StorageImportPreflightItem>,
    aborted: bool,
}

impl StorageImportPreflight {
    pub(crate) const fn new(items: Vec<StorageImportPreflightItem>, aborted: bool) -> Self {
        Self { items, aborted }
    }

    pub(crate) fn into_parts(self) -> (Vec<StorageImportPreflightItem>, bool) {
        (self.items, self.aborted)
    }
}

/// Per-item result from a best-effort import transaction.
#[derive(Debug)]
pub(crate) struct StorageImportApplyItem {
    index: usize,
    error: Option<StorageError>,
}

impl StorageImportApplyItem {
    pub(crate) const fn success(index: usize) -> Self {
        Self { index, error: None }
    }

    pub(crate) const fn failure(index: usize, error: StorageError) -> Self {
        Self {
            index,
            error: Some(error),
        }
    }

    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    #[cfg(test)]
    pub(crate) const fn error(&self) -> Option<&StorageError> {
        self.error.as_ref()
    }

    pub(crate) fn into_parts(self) -> (usize, Option<StorageError>) {
        (self.index, self.error)
    }
}

#[derive(Debug)]
pub(crate) struct StorageImportApply {
    items: Vec<StorageImportApplyItem>,
    aborted: bool,
}

impl StorageImportApply {
    pub(crate) const fn new(items: Vec<StorageImportApplyItem>, aborted: bool) -> Self {
        Self { items, aborted }
    }

    pub(crate) fn into_parts(self) -> (Vec<StorageImportApplyItem>, bool) {
        (self.items, self.aborted)
    }
}

/// Complete import capability required from every selectable backend.
///
/// Planning lookups, rollback-only preflight, strict atomic application,
/// best-effort per-item application, and durable result recording form one
/// indivisible capability. Implementing only the lookup subset is not enough
/// to satisfy [`crate::storage::StorageBackend`].
#[async_trait]
pub(crate) trait ImportStorage: Send + Sync {
    async fn import_root_collection(&self) -> Result<Collection, StorageError>;

    async fn import_collection_by_id(
        &self,
        collection_id: i32,
    ) -> Result<Option<Collection>, StorageError>;

    async fn import_collection_by_key(
        &self,
        key: &CollectionKey,
    ) -> Result<Option<Collection>, StorageError>;

    async fn import_collections_by_name(&self, name: &str)
    -> Result<Vec<Collection>, StorageError>;

    async fn import_collection_child_by_name(
        &self,
        parent_collection_id: i32,
        name: &str,
    ) -> Result<Option<Collection>, StorageError>;

    async fn import_class_by_name(
        &self,
        collection_id: i32,
        name: &str,
    ) -> Result<Option<HubuumClass>, StorageError>;

    async fn import_classes_by_names(
        &self,
        collection_id: i32,
        names: &[String],
    ) -> Result<Vec<HubuumClass>, StorageError>;

    async fn import_object_by_name(
        &self,
        class_id: i32,
        name: &str,
    ) -> Result<Option<HubuumObject>, StorageError>;

    async fn import_objects_by_names(
        &self,
        class_id: i32,
        names: &[String],
    ) -> Result<Vec<HubuumObject>, StorageError>;

    async fn import_class_relation_exists(
        &self,
        left_class_id: i32,
        right_class_id: i32,
    ) -> Result<bool, StorageError>;

    async fn import_object_relation_exists(
        &self,
        left_object_id: i32,
        right_object_id: i32,
    ) -> Result<bool, StorageError>;

    async fn import_group_exists(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError>;

    async fn preflight_import(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: ImportMode,
    ) -> Result<StorageImportPreflight, StorageError>;

    async fn apply_import_strict(
        &self,
        items: Vec<StorageImportPlanItem>,
    ) -> Result<(), StorageError>;

    async fn apply_import_best_effort(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: ImportMode,
    ) -> Result<StorageImportApply, StorageError>;

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError>;
}
