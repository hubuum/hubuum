use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde_json::Value;

use crate::{
    AuthorizationPermission, StorageClassRecord, StorageCollection, StorageError, StorageObject,
};

macro_rules! import_dto {
    ($name:ident, $parts:ident { $($field:ident: $field_type:ty),+ $(,)? }) => {
        #[derive(Clone, PartialEq)]
        pub struct $name {
            $($field: $field_type),+
        }

        pub struct $parts {
            $(pub $field: $field_type),+
        }

        impl $name {
            #[must_use]
            pub fn from_parts(parts: $parts) -> Self {
                let $parts { $($field),+ } = parts;
                Self { $($field),+ }
            }

            #[must_use]
            pub fn into_parts(self) -> $parts {
                $parts {
                    $($field: self.$field),+
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("payload", &"[redacted]")
                    .finish()
            }
        }
    };
}

/// Positive resource revision used by an import precondition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageImportRevision(i64);

impl StorageImportRevision {
    pub fn new(value: i64) -> Result<Self, StorageError> {
        if value <= 0 {
            return Err(StorageError::bad_request(
                "import revision must be greater than zero",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Per-item collision and optimistic-concurrency behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageImportWriteCondition {
    CreateOnly,
    Overwrite,
    IfRevision(StorageImportRevision),
}

/// Transaction behavior for one import request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageImportAtomicity {
    Strict,
    BestEffort,
}

/// Default collision behavior for items without an explicit condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageImportCollisionPolicy {
    Abort,
    Overwrite,
}

/// Whether a permission failure aborts a best-effort import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageImportPermissionPolicy {
    Abort,
    Continue,
}

/// Complete import execution policy supplied by the application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageImportMode {
    atomicity: Option<StorageImportAtomicity>,
    collision_policy: Option<StorageImportCollisionPolicy>,
    permission_policy: Option<StorageImportPermissionPolicy>,
}

impl StorageImportMode {
    #[must_use]
    pub const fn new(
        atomicity: Option<StorageImportAtomicity>,
        collision_policy: Option<StorageImportCollisionPolicy>,
        permission_policy: Option<StorageImportPermissionPolicy>,
    ) -> Self {
        Self {
            atomicity,
            collision_policy,
            permission_policy,
        }
    }

    #[must_use]
    pub const fn into_parts(
        self,
    ) -> (
        Option<StorageImportAtomicity>,
        Option<StorageImportCollisionPolicy>,
        Option<StorageImportPermissionPolicy>,
    ) {
        (
            self.atomicity,
            self.collision_policy,
            self.permission_policy,
        )
    }
}

/// Validated imported creation and update timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageImportTimestamps {
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl StorageImportTimestamps {
    pub fn new(created_at: NaiveDateTime, updated_at: NaiveDateTime) -> Result<Self, StorageError> {
        if updated_at < created_at {
            return Err(StorageError::bad_request(
                "import updated_at must not be earlier than created_at",
            ));
        }
        Ok(Self {
            created_at,
            updated_at,
        })
    }

    #[must_use]
    pub const fn into_parts(self) -> (NaiveDateTime, NaiveDateTime) {
        (self.created_at, self.updated_at)
    }
}

import_dto!(
    StorageImportCollectionKey,
    StorageImportCollectionKeyParts {
        name: String,
        path: Option<Vec<String>>,
    }
);

import_dto!(
    StorageImportIdentityScopeKey,
    StorageImportIdentityScopeKeyParts { name: String }
);

import_dto!(
    StorageImportGroupKey,
    StorageImportGroupKeyParts {
        identity_scope: Option<String>,
        name: String,
    }
);

import_dto!(
    StorageImportPrincipalKey,
    StorageImportPrincipalKeyParts {
        identity_scope: Option<String>,
        name: String,
    }
);

import_dto!(
    StorageImportEventSinkKey,
    StorageImportEventSinkKeyParts { name: String }
);

import_dto!(
    StorageImportClassKey,
    StorageImportClassKeyParts {
        name: String,
        collection_ref: Option<String>,
        collection_key: Option<StorageImportCollectionKey>,
    }
);

import_dto!(
    StorageImportObjectKey,
    StorageImportObjectKeyParts {
        name: String,
        class_ref: Option<String>,
        class_key: Option<StorageImportClassKey>,
    }
);

import_dto!(
    StorageImportIdentityScope,
    StorageImportIdentityScopeParts {
        reference: Option<String>,
        name: String,
        provider_kind: String,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportGroup,
    StorageImportGroupParts {
        reference: Option<String>,
        name: String,
        description: String,
        identity_scope_ref: Option<String>,
        identity_scope_key: Option<StorageImportIdentityScopeKey>,
        managed_by: String,
        external_key: Option<String>,
        last_sync_attempted_at: Option<NaiveDateTime>,
        last_sync_success_at: Option<NaiveDateTime>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

/// Principal subtype supplied by a validated import plan.
#[derive(Clone, PartialEq)]
pub enum StorageImportPrincipalSubtype {
    Human {
        password: Option<String>,
        password_hash: Option<String>,
        proper_name: Option<String>,
        email: Option<String>,
        anonymized_at: Option<NaiveDateTime>,
    },
    ServiceAccount {
        description: String,
        owner_group_ref: Option<String>,
        owner_group_key: Option<StorageImportGroupKey>,
        created_by_ref: Option<String>,
        created_by_key: Option<StorageImportPrincipalKey>,
        disabled_at: Option<NaiveDateTime>,
    },
}

impl fmt::Debug for StorageImportPrincipalSubtype {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Human { .. } => "Human { credentials: [redacted] }",
            Self::ServiceAccount { .. } => "ServiceAccount { identity: [redacted] }",
        })
    }
}

import_dto!(
    StorageImportPrincipal,
    StorageImportPrincipalParts {
        reference: Option<String>,
        name: String,
        identity_scope_ref: Option<String>,
        identity_scope_key: Option<StorageImportIdentityScopeKey>,
        provider_managed: bool,
        settings: Value,
        external_subject: Option<String>,
        last_sync_attempted_at: Option<NaiveDateTime>,
        last_sync_success_at: Option<NaiveDateTime>,
        subtype: StorageImportPrincipalSubtype,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportMembershipSource,
    StorageImportMembershipSourceParts {
        source: String,
        source_scope_ref: Option<String>,
        source_scope_key: Option<StorageImportIdentityScopeKey>,
        source_key: String,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportGroupMembership,
    StorageImportGroupMembershipParts {
        reference: Option<String>,
        principal_ref: Option<String>,
        principal_key: Option<StorageImportPrincipalKey>,
        group_ref: Option<String>,
        group_key: Option<StorageImportGroupKey>,
        sources: Vec<StorageImportMembershipSource>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportCollection,
    StorageImportCollectionParts {
        reference: Option<String>,
        name: String,
        description: String,
        parent_collection_ref: Option<String>,
        parent_collection_key: Option<StorageImportCollectionKey>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportClass,
    StorageImportClassParts {
        reference: Option<String>,
        name: String,
        description: String,
        json_schema: Option<Value>,
        validate_schema: Option<bool>,
        collection_ref: Option<String>,
        collection_key: Option<StorageImportCollectionKey>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportObject,
    StorageImportObjectParts {
        reference: Option<String>,
        name: String,
        description: String,
        data: Value,
        class_ref: Option<String>,
        class_key: Option<StorageImportClassKey>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

/// Visibility of one imported computed-field definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageImportComputedFieldVisibility {
    Shared,
    Personal,
}

import_dto!(
    StorageImportComputedField,
    StorageImportComputedFieldParts {
        reference: Option<String>,
        class_ref: Option<String>,
        class_key: Option<StorageImportClassKey>,
        visibility: StorageImportComputedFieldVisibility,
        owner_ref: Option<String>,
        owner_key: Option<StorageImportPrincipalKey>,
        key: String,
        label: String,
        description: String,
        operation: Value,
        result_type: String,
        enabled: bool,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportClassRelation,
    StorageImportClassRelationParts {
        reference: Option<String>,
        from_class_ref: Option<String>,
        from_class_key: Option<StorageImportClassKey>,
        to_class_ref: Option<String>,
        to_class_key: Option<StorageImportClassKey>,
        forward_template_alias: Option<String>,
        reverse_template_alias: Option<String>,
        from_max_relations: Option<i32>,
        to_max_relations: Option<i32>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportObjectRelation,
    StorageImportObjectRelationParts {
        reference: Option<String>,
        from_object_ref: Option<String>,
        from_object_key: Option<StorageImportObjectKey>,
        to_object_ref: Option<String>,
        to_object_key: Option<StorageImportObjectKey>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportCollectionPermission,
    StorageImportCollectionPermissionParts {
        reference: Option<String>,
        collection_ref: Option<String>,
        collection_key: Option<StorageImportCollectionKey>,
        group_key: StorageImportGroupKey,
        permissions: Vec<AuthorizationPermission>,
        replace_existing: Option<bool>,
        condition: Option<StorageImportWriteCondition>,
    }
);

import_dto!(
    StorageImportExportTemplate,
    StorageImportExportTemplateParts {
        reference: Option<String>,
        collection_ref: Option<String>,
        collection_key: Option<StorageImportCollectionKey>,
        class_ref: Option<String>,
        class_key: Option<StorageImportClassKey>,
        name: String,
        description: String,
        content_type: String,
        template: String,
        kind: String,
        scope_kind: Option<String>,
        default_query: Option<String>,
        include: Option<Value>,
        relation_context: Option<Value>,
        default_missing_data_policy: Option<String>,
        default_limits: Option<Value>,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportRemoteTarget,
    StorageImportRemoteTargetParts {
        reference: Option<String>,
        collection_ref: Option<String>,
        collection_key: Option<StorageImportCollectionKey>,
        class_ref: Option<String>,
        class_key: Option<StorageImportClassKey>,
        name: String,
        description: String,
        method: String,
        url_template: String,
        headers_template: Value,
        body_template: Option<String>,
        auth_config: Value,
        allowed_subject_types: Vec<String>,
        timeout_ms: i32,
        enabled: bool,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportEventSink,
    StorageImportEventSinkParts {
        reference: Option<String>,
        name: String,
        kind: String,
        config: Value,
        secret_ref: Option<String>,
        enabled: bool,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

import_dto!(
    StorageImportEventSubscription,
    StorageImportEventSubscriptionParts {
        reference: Option<String>,
        collection_ref: Option<String>,
        collection_key: Option<StorageImportCollectionKey>,
        sink_ref: Option<String>,
        sink_key: Option<StorageImportEventSinkKey>,
        name: String,
        description: String,
        entity_types: Vec<String>,
        actions: Vec<String>,
        filter: Value,
        routing: Value,
        enabled: bool,
        condition: Option<StorageImportWriteCondition>,
        timestamps: Option<StorageImportTimestamps>,
    }
);

/// A backend-neutral import command produced by application planning.
///
/// The enum is exhaustive: every selectable backend must handle every import
/// entity and mutation before it can implement [`crate::StorageBackend`].
#[derive(Clone)]
pub enum StorageImportOperation {
    UpsertIdentityScope {
        input: StorageImportIdentityScope,
        overwrite: bool,
    },
    UpsertGroup {
        input: StorageImportGroup,
        overwrite: bool,
    },
    UpsertPrincipal {
        input: StorageImportPrincipal,
        overwrite: bool,
    },
    UpsertGroupMembership {
        input: StorageImportGroupMembership,
        overwrite: bool,
    },
    CreateCollection(StorageImportCollection),
    UpdateCollection {
        collection_id: i32,
        input: StorageImportCollection,
    },
    CreateClass(StorageImportClass),
    UpdateClass {
        class_id: i32,
        input: StorageImportClass,
    },
    CreateObject(StorageImportObject),
    UpdateObject {
        object_id: i32,
        input: StorageImportObject,
    },
    UpsertComputedField {
        input: StorageImportComputedField,
        overwrite: bool,
    },
    CreateClassRelation(StorageImportClassRelation),
    UpdateClassRelationTimestamps {
        input: StorageImportClassRelation,
        timestamps: StorageImportTimestamps,
    },
    CheckClassRelationCondition(StorageImportClassRelation),
    CreateObjectRelation(StorageImportObjectRelation),
    UpdateObjectRelationTimestamps {
        input: StorageImportObjectRelation,
        timestamps: StorageImportTimestamps,
    },
    CheckObjectRelationCondition(StorageImportObjectRelation),
    ApplyCollectionPermissions {
        input: StorageImportCollectionPermission,
        overwrite: bool,
    },
    UpsertExportTemplate {
        input: StorageImportExportTemplate,
        overwrite: bool,
    },
    UpsertRemoteTarget {
        input: StorageImportRemoteTarget,
        overwrite: bool,
    },
    UpsertEventSink {
        input: StorageImportEventSink,
        overwrite: bool,
    },
    UpsertEventSubscription {
        input: StorageImportEventSubscription,
        overwrite: bool,
    },
}

impl StorageImportOperation {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
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

impl fmt::Debug for StorageImportOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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
pub struct StorageImportPlanItem {
    index: usize,
    operation: StorageImportOperation,
}

impl StorageImportPlanItem {
    #[must_use]
    pub const fn new(index: usize, operation: StorageImportOperation) -> Self {
        Self { index, operation }
    }

    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub const fn operation(&self) -> &StorageImportOperation {
        &self.operation
    }
}

/// Per-item dry-run result from a rollback-only backend transaction.
#[derive(Debug)]
pub struct StorageImportPreflightItem {
    index: usize,
    observed_revision: Option<i64>,
    error: Option<StorageError>,
}

impl StorageImportPreflightItem {
    #[must_use]
    pub const fn success(index: usize, observed_revision: Option<i64>) -> Self {
        Self {
            index,
            observed_revision,
            error: None,
        }
    }

    #[must_use]
    pub const fn failure(
        index: usize,
        observed_revision: Option<i64>,
        error: StorageError,
    ) -> Self {
        Self {
            index,
            observed_revision,
            error: Some(error),
        }
    }

    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub fn into_parts(self) -> (usize, Option<i64>, Option<StorageError>) {
        (self.index, self.observed_revision, self.error)
    }
}

#[derive(Debug)]
pub struct StorageImportPreflight {
    items: Vec<StorageImportPreflightItem>,
    aborted: bool,
}

impl StorageImportPreflight {
    #[must_use]
    pub const fn new(items: Vec<StorageImportPreflightItem>, aborted: bool) -> Self {
        Self { items, aborted }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageImportPreflightItem>, bool) {
        (self.items, self.aborted)
    }
}

/// Per-item result from a best-effort import transaction.
#[derive(Debug)]
pub struct StorageImportApplyItem {
    index: usize,
    error: Option<StorageError>,
}

impl StorageImportApplyItem {
    #[must_use]
    pub const fn success(index: usize) -> Self {
        Self { index, error: None }
    }

    #[must_use]
    pub const fn failure(index: usize, error: StorageError) -> Self {
        Self {
            index,
            error: Some(error),
        }
    }

    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub const fn error(&self) -> Option<&StorageError> {
        self.error.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (usize, Option<StorageError>) {
        (self.index, self.error)
    }
}

#[derive(Debug)]
pub struct StorageImportApply {
    items: Vec<StorageImportApplyItem>,
    aborted: bool,
}

impl StorageImportApply {
    #[must_use]
    pub const fn new(items: Vec<StorageImportApplyItem>, aborted: bool) -> Self {
        Self { items, aborted }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageImportApplyItem>, bool) {
        (self.items, self.aborted)
    }
}

/// Durable import result supplied after planning or application.
#[derive(Clone, PartialEq)]
pub struct StorageImportResult {
    task_id: i32,
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    outcome: String,
    error: Option<String>,
    details: Option<Value>,
}

impl StorageImportResult {
    #[must_use]
    pub fn builder(
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

    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        Option<String>,
        String,
        String,
        Option<String>,
        String,
        Option<String>,
        Option<Value>,
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

impl fmt::Debug for StorageImportResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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

pub struct StorageImportResultBuilder {
    result: StorageImportResult,
}

impl StorageImportResultBuilder {
    #[must_use]
    pub fn item_ref(mut self, item_ref: Option<String>) -> Self {
        self.result.item_ref = item_ref;
        self
    }

    #[must_use]
    pub fn identifier(mut self, identifier: Option<String>) -> Self {
        self.result.identifier = identifier;
        self
    }

    #[must_use]
    pub fn error(mut self, error: Option<String>) -> Self {
        self.result.error = error;
        self
    }

    #[must_use]
    pub fn details(mut self, details: Option<Value>) -> Self {
        self.result.details = details;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageImportResult {
        self.result
    }
}

/// Complete import capability required from every selectable backend.
#[async_trait]
pub trait ImportStorage: Send + Sync {
    async fn import_root_collection(&self) -> Result<StorageCollection, StorageError>;

    async fn import_collection_by_id(
        &self,
        collection_id: i32,
    ) -> Result<Option<StorageCollection>, StorageError>;

    async fn import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError>;

    async fn import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError>;

    async fn import_collection_child_by_name(
        &self,
        parent_collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError>;

    async fn import_class_by_name(
        &self,
        collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageClassRecord>, StorageError>;

    async fn import_classes_by_names(
        &self,
        collection_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageClassRecord>, StorageError>;

    async fn import_object_by_name(
        &self,
        class_id: i32,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError>;

    async fn import_objects_by_names(
        &self,
        class_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError>;

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
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError>;

    async fn apply_import_strict(
        &self,
        items: Vec<StorageImportPlanItem>,
    ) -> Result<(), StorageError>;

    async fn apply_import_best_effort(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError>;

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn revisions_must_be_positive() {
        assert!(StorageImportRevision::new(0).is_err());
        assert_eq!(StorageImportRevision::new(7).unwrap().get(), 7);
    }

    #[test]
    fn timestamps_preserve_chronological_order() {
        let created_at = NaiveDate::from_ymd_opt(2026, 8, 14)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap();
        let updated_at = NaiveDate::from_ymd_opt(2026, 8, 14)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap();

        assert!(StorageImportTimestamps::new(created_at, updated_at).is_err());
    }

    #[test]
    fn import_operation_debug_redacts_payloads() {
        let sensitive = "sensitive-import-value";
        let operation = StorageImportOperation::CreateCollection(
            StorageImportCollection::from_parts(StorageImportCollectionParts {
                reference: Some("collection:private".to_string()),
                name: sensitive.to_string(),
                description: sensitive.to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
                condition: None,
                timestamps: None,
            }),
        );

        let debug = format!("{operation:?}");
        assert!(debug.contains("create_collection"));
        assert!(!debug.contains(sensitive));
    }
}
