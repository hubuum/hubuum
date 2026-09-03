use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_computed_fields::Definition;
use hubuum_domain::{ClassId, CollectionId, ObjectId, ResourceRevision, TaskId};
use serde_json::Value;

use crate::{
    StorageAuthorizationPermission, StorageClass, StorageClassSchemaPolicy, StorageCollection,
    StorageError, StorageObject, StorageRemoteTargetHttpMethod, StorageRemoteTargetSubjectType,
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
    pub fn try_new(value: i64) -> Result<Self, StorageError> {
        if value <= 0 {
            return Err(StorageError::invalid_input(
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

impl StorageImportWriteCondition {
    #[must_use]
    pub const fn expected_revision(self) -> Option<i64> {
        match self {
            Self::IfRevision(revision) => Some(revision.get()),
            Self::CreateOnly | Self::Overwrite => None,
        }
    }

    #[must_use]
    pub const fn requires_existing(self) -> bool {
        matches!(self, Self::IfRevision(_))
    }
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
    atomicity: StorageImportAtomicity,
    collision_policy: StorageImportCollisionPolicy,
    permission_policy: StorageImportPermissionPolicy,
}

impl StorageImportMode {
    #[must_use]
    pub const fn new(
        atomicity: StorageImportAtomicity,
        collision_policy: StorageImportCollisionPolicy,
        permission_policy: StorageImportPermissionPolicy,
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
        StorageImportAtomicity,
        StorageImportCollisionPolicy,
        StorageImportPermissionPolicy,
    ) {
        (
            self.atomicity,
            self.collision_policy,
            self.permission_policy,
        )
    }

    #[must_use]
    pub const fn atomicity(&self) -> StorageImportAtomicity {
        self.atomicity
    }

    #[must_use]
    pub const fn collision_policy(&self) -> StorageImportCollisionPolicy {
        self.collision_policy
    }

    #[must_use]
    pub const fn permission_policy(&self) -> StorageImportPermissionPolicy {
        self.permission_policy
    }
}

/// Validated imported creation and update timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageImportTimestamps {
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl StorageImportTimestamps {
    pub fn try_new(
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        if updated_at < created_at {
            return Err(StorageError::invalid_input(
                "import updated_at must not be earlier than created_at",
            ));
        }
        Ok(Self {
            created_at,
            updated_at,
        })
    }

    #[must_use]
    pub const fn into_parts(self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.created_at, self.updated_at)
    }

    #[must_use]
    pub const fn as_pair(self) -> (DateTime<Utc>, DateTime<Utc>) {
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
        identity_scope: String,
        name: String,
    }
);

import_dto!(
    StorageImportPrincipalKey,
    StorageImportPrincipalKeyParts {
        identity_scope: String,
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
        last_sync_attempted_at: Option<DateTime<Utc>>,
        last_sync_success_at: Option<DateTime<Utc>>,
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
        anonymized_at: Option<DateTime<Utc>>,
    },
    ServiceAccount {
        description: String,
        owner_group_ref: Option<String>,
        owner_group_key: Option<StorageImportGroupKey>,
        created_by_ref: Option<String>,
        created_by_key: Option<StorageImportPrincipalKey>,
        disabled_at: Option<DateTime<Utc>>,
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
        last_sync_attempted_at: Option<DateTime<Utc>>,
        last_sync_success_at: Option<DateTime<Utc>>,
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
        schema_policy: StorageClassSchemaPolicy,
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
        definition: Definition,
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
        permissions: Vec<StorageAuthorizationPermission>,
        replace_existing: bool,
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
        method: StorageRemoteTargetHttpMethod,
        url_template: String,
        headers_template: Value,
        body_template: Option<String>,
        auth_config: Value,
        allowed_subject_types: Vec<StorageRemoteTargetSubjectType>,
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
        collection_id: CollectionId,
        input: StorageImportCollection,
    },
    CreateClass(StorageImportClass),
    UpdateClass {
        class_id: ClassId,
        input: StorageImportClass,
    },
    CreateObject(StorageImportObject),
    UpdateObject {
        object_id: ObjectId,
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

    #[must_use]
    pub fn into_parts(self) -> (usize, StorageImportOperation) {
        (self.index, self.operation)
    }
}

/// Structurally validated import operations ready for backend execution.
///
/// The application may leave gaps in item indexes when planning rejected an
/// earlier source item, but indexes must remain strictly increasing. The plan
/// also guarantees that every operation has unambiguous selectors and valid
/// identifiers before any backend begins a transaction.
#[derive(Clone, Debug)]
pub struct StorageImportPlan {
    items: Vec<StorageImportPlanItem>,
}

impl StorageImportPlan {
    pub fn try_new(items: Vec<StorageImportPlanItem>) -> Result<Self, StorageError> {
        let mut previous_index = None;
        for item in &items {
            if previous_index.is_some_and(|previous| item.index() <= previous) {
                return Err(StorageError::invalid_input(
                    "import plan indexes must be strictly increasing",
                ));
            }
            item.operation().validate()?;
            previous_index = Some(item.index());
        }
        Ok(Self { items })
    }

    #[must_use]
    pub fn items(&self) -> &[StorageImportPlanItem] {
        &self.items
    }

    #[must_use]
    pub fn into_items(self) -> Vec<StorageImportPlanItem> {
        self.items
    }
}

impl StorageImportOperation {
    fn validate(&self) -> Result<(), StorageError> {
        match self {
            Self::UpsertIdentityScope { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "identity scope ref")?;
                validate_text(&parts.name, "identity scope name")?;
                validate_text(&parts.provider_kind, "identity scope provider kind")
            }
            Self::UpsertGroup { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "group ref")?;
                validate_text(&parts.name, "group name")?;
                validate_selector(
                    parts.identity_scope_ref.as_deref(),
                    parts.identity_scope_key.as_ref(),
                    "group identity scope",
                    true,
                )?;
                if let Some(key) = &parts.identity_scope_key {
                    validate_identity_scope_key(key)?;
                }
                validate_text(&parts.managed_by, "group provider")
            }
            Self::UpsertPrincipal { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "principal ref")?;
                validate_text(&parts.name, "principal name")?;
                validate_selector(
                    parts.identity_scope_ref.as_deref(),
                    parts.identity_scope_key.as_ref(),
                    "principal identity scope",
                    true,
                )?;
                if let Some(key) = &parts.identity_scope_key {
                    validate_identity_scope_key(key)?;
                }
                match &parts.subtype {
                    StorageImportPrincipalSubtype::Human { .. } => Ok(()),
                    StorageImportPrincipalSubtype::ServiceAccount {
                        owner_group_ref,
                        owner_group_key,
                        created_by_ref,
                        created_by_key,
                        ..
                    } => {
                        validate_selector(
                            owner_group_ref.as_deref(),
                            owner_group_key.as_ref(),
                            "service-account owner group",
                            true,
                        )?;
                        validate_selector(
                            created_by_ref.as_deref(),
                            created_by_key.as_ref(),
                            "service-account creator",
                            false,
                        )?;
                        if let Some(key) = owner_group_key {
                            validate_group_key(key)?;
                        }
                        if let Some(key) = created_by_key {
                            validate_principal_key(key)?;
                        }
                        Ok(())
                    }
                }
            }
            Self::UpsertGroupMembership { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "membership ref")?;
                validate_selector(
                    parts.principal_ref.as_deref(),
                    parts.principal_key.as_ref(),
                    "membership principal",
                    true,
                )?;
                validate_selector(
                    parts.group_ref.as_deref(),
                    parts.group_key.as_ref(),
                    "membership group",
                    true,
                )?;
                if let Some(key) = &parts.principal_key {
                    validate_principal_key(key)?;
                }
                if let Some(key) = &parts.group_key {
                    validate_group_key(key)?;
                }
                for source in parts.sources {
                    let source = source.into_parts();
                    validate_text(&source.source, "membership source")?;
                    validate_text(&source.source_key, "membership source key")?;
                    validate_selector(
                        source.source_scope_ref.as_deref(),
                        source.source_scope_key.as_ref(),
                        "membership source scope",
                        true,
                    )?;
                    if let Some(key) = &source.source_scope_key {
                        validate_identity_scope_key(key)?;
                    }
                }
                Ok(())
            }
            Self::CreateCollection(input) | Self::UpdateCollection { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "collection ref")?;
                validate_text(&parts.name, "collection name")?;
                validate_selector(
                    parts.parent_collection_ref.as_deref(),
                    parts.parent_collection_key.as_ref(),
                    "parent collection",
                    false,
                )?;
                if let Some(key) = &parts.parent_collection_key {
                    validate_collection_key(key)?;
                }
                Ok(())
            }
            Self::CreateClass(input) | Self::UpdateClass { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "class ref")?;
                validate_text(&parts.name, "class name")?;
                validate_selector(
                    parts.collection_ref.as_deref(),
                    parts.collection_key.as_ref(),
                    "class collection",
                    true,
                )?;
                if let Some(key) = &parts.collection_key {
                    validate_collection_key(key)?;
                }
                Ok(())
            }
            Self::CreateObject(input) | Self::UpdateObject { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "object ref")?;
                validate_text(&parts.name, "object name")?;
                validate_selector(
                    parts.class_ref.as_deref(),
                    parts.class_key.as_ref(),
                    "object class",
                    true,
                )?;
                if let Some(key) = &parts.class_key {
                    validate_class_key(key)?;
                }
                Ok(())
            }
            Self::UpsertComputedField { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "computed-field ref")?;
                validate_selector(
                    parts.class_ref.as_deref(),
                    parts.class_key.as_ref(),
                    "computed-field class",
                    true,
                )?;
                if let Some(key) = &parts.class_key {
                    validate_class_key(key)?;
                }
                Ok(())
            }
            Self::CreateClassRelation(input)
            | Self::UpdateClassRelationTimestamps { input, .. }
            | Self::CheckClassRelationCondition(input) => validate_class_relation(input),
            Self::CreateObjectRelation(input)
            | Self::UpdateObjectRelationTimestamps { input, .. }
            | Self::CheckObjectRelationCondition(input) => validate_object_relation(input),
            Self::ApplyCollectionPermissions { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "permission ref")?;
                validate_selector(
                    parts.collection_ref.as_deref(),
                    parts.collection_key.as_ref(),
                    "permission collection",
                    true,
                )?;
                if let Some(key) = &parts.collection_key {
                    validate_collection_key(key)?;
                }
                validate_group_key(&parts.group_key)
            }
            Self::UpsertExportTemplate { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "export-template ref")?;
                validate_collection_and_optional_class(
                    parts.collection_ref.as_deref(),
                    parts.collection_key.as_ref(),
                    parts.class_ref.as_deref(),
                    parts.class_key.as_ref(),
                    "export template",
                )?;
                validate_text(&parts.name, "export-template name")?;
                validate_text(&parts.content_type, "export-template content type")?;
                validate_text(&parts.kind, "export-template kind")
            }
            Self::UpsertRemoteTarget { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "remote-target ref")?;
                validate_collection_and_optional_class(
                    parts.collection_ref.as_deref(),
                    parts.collection_key.as_ref(),
                    parts.class_ref.as_deref(),
                    parts.class_key.as_ref(),
                    "remote target",
                )?;
                validate_text(&parts.name, "remote-target name")?;
                validate_text(&parts.url_template, "remote-target URL template")?;
                if parts.allowed_subject_types.is_empty() {
                    return Err(StorageError::invalid_input(
                        "remote-target policy must allow at least one subject type",
                    ));
                }
                let unique_subject_types = parts
                    .allowed_subject_types
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>();
                if unique_subject_types.len() != parts.allowed_subject_types.len() {
                    return Err(StorageError::invalid_input(
                        "remote-target policy contains duplicate subject types",
                    ));
                }
                let has_class_scope = parts.class_ref.is_some() || parts.class_key.is_some();
                let allows_objects =
                    unique_subject_types.contains(&StorageRemoteTargetSubjectType::Object);
                if has_class_scope != allows_objects {
                    return Err(StorageError::invalid_input(
                        "remote-target class scope must be present exactly when object subjects are allowed",
                    ));
                }
                if parts.timeout_ms <= 0 {
                    return Err(StorageError::invalid_input(
                        "remote-target timeout must be greater than zero",
                    ));
                }
                Ok(())
            }
            Self::UpsertEventSink { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "event-sink ref")?;
                validate_text(&parts.name, "event-sink name")?;
                validate_text(&parts.kind, "event-sink kind")
            }
            Self::UpsertEventSubscription { input, .. } => {
                let parts = input.clone().into_parts();
                validate_optional_reference(parts.reference.as_deref(), "event-subscription ref")?;
                validate_selector(
                    parts.collection_ref.as_deref(),
                    parts.collection_key.as_ref(),
                    "event-subscription collection",
                    true,
                )?;
                validate_selector(
                    parts.sink_ref.as_deref(),
                    parts.sink_key.as_ref(),
                    "event-subscription sink",
                    true,
                )?;
                if let Some(key) = &parts.collection_key {
                    validate_collection_key(key)?;
                }
                if let Some(key) = &parts.sink_key {
                    validate_event_sink_key(key)?;
                }
                validate_text(&parts.name, "event-subscription name")
            }
        }
    }
}

fn validate_text(value: &str, field: &str) -> Result<(), StorageError> {
    if value.trim().is_empty() {
        Err(StorageError::invalid_input(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn validate_optional_reference(value: Option<&str>, field: &str) -> Result<(), StorageError> {
    value.map_or(Ok(()), |value| validate_text(value, field))
}

fn validate_selector<T>(
    reference: Option<&str>,
    key: Option<&T>,
    field: &str,
    required: bool,
) -> Result<(), StorageError> {
    validate_optional_reference(reference, field)?;
    match (reference.is_some(), key.is_some(), required) {
        (true, true, _) => Err(StorageError::invalid_input(format!(
            "{field} must use either a ref or a key, not both"
        ))),
        (false, false, true) => Err(StorageError::invalid_input(format!(
            "{field} requires either a ref or a key"
        ))),
        _ => Ok(()),
    }
}

fn validate_identity_scope_key(key: &StorageImportIdentityScopeKey) -> Result<(), StorageError> {
    validate_text(&key.clone().into_parts().name, "identity scope key name")
}

fn validate_group_key(key: &StorageImportGroupKey) -> Result<(), StorageError> {
    let parts = key.clone().into_parts();
    validate_text(&parts.identity_scope, "group key identity scope")?;
    validate_text(&parts.name, "group key name")
}

fn validate_principal_key(key: &StorageImportPrincipalKey) -> Result<(), StorageError> {
    let parts = key.clone().into_parts();
    validate_text(&parts.identity_scope, "principal key identity scope")?;
    validate_text(&parts.name, "principal key name")
}

fn validate_collection_key(key: &StorageImportCollectionKey) -> Result<(), StorageError> {
    let parts = key.clone().into_parts();
    validate_text(&parts.name, "collection key name")?;
    if parts
        .path
        .as_ref()
        .is_some_and(|path| path.iter().any(|segment| segment.trim().is_empty()))
    {
        return Err(StorageError::invalid_input(
            "collection key path segments must not be empty",
        ));
    }
    Ok(())
}

fn validate_class_key(key: &StorageImportClassKey) -> Result<(), StorageError> {
    let parts = key.clone().into_parts();
    validate_text(&parts.name, "class key name")?;
    validate_selector(
        parts.collection_ref.as_deref(),
        parts.collection_key.as_ref(),
        "class key collection",
        true,
    )?;
    if let Some(key) = &parts.collection_key {
        validate_collection_key(key)?;
    }
    Ok(())
}

fn validate_object_key(key: &StorageImportObjectKey) -> Result<(), StorageError> {
    let parts = key.clone().into_parts();
    validate_text(&parts.name, "object key name")?;
    validate_selector(
        parts.class_ref.as_deref(),
        parts.class_key.as_ref(),
        "object key class",
        true,
    )?;
    if let Some(key) = &parts.class_key {
        validate_class_key(key)?;
    }
    Ok(())
}

fn validate_event_sink_key(key: &StorageImportEventSinkKey) -> Result<(), StorageError> {
    validate_text(&key.clone().into_parts().name, "event-sink key name")
}

fn validate_class_relation(input: &StorageImportClassRelation) -> Result<(), StorageError> {
    let parts = input.clone().into_parts();
    validate_optional_reference(parts.reference.as_deref(), "class-relation ref")?;
    validate_selector(
        parts.from_class_ref.as_deref(),
        parts.from_class_key.as_ref(),
        "class-relation source",
        true,
    )?;
    validate_selector(
        parts.to_class_ref.as_deref(),
        parts.to_class_key.as_ref(),
        "class-relation target",
        true,
    )?;
    if let Some(key) = &parts.from_class_key {
        validate_class_key(key)?;
    }
    if let Some(key) = &parts.to_class_key {
        validate_class_key(key)?;
    }
    Ok(())
}

fn validate_object_relation(input: &StorageImportObjectRelation) -> Result<(), StorageError> {
    let parts = input.clone().into_parts();
    validate_optional_reference(parts.reference.as_deref(), "object-relation ref")?;
    validate_selector(
        parts.from_object_ref.as_deref(),
        parts.from_object_key.as_ref(),
        "object-relation source",
        true,
    )?;
    validate_selector(
        parts.to_object_ref.as_deref(),
        parts.to_object_key.as_ref(),
        "object-relation target",
        true,
    )?;
    if let Some(key) = &parts.from_object_key {
        validate_object_key(key)?;
    }
    if let Some(key) = &parts.to_object_key {
        validate_object_key(key)?;
    }
    Ok(())
}

fn validate_collection_and_optional_class(
    collection_ref: Option<&str>,
    collection_key: Option<&StorageImportCollectionKey>,
    class_ref: Option<&str>,
    class_key: Option<&StorageImportClassKey>,
    field: &str,
) -> Result<(), StorageError> {
    validate_selector(collection_ref, collection_key, field, true)?;
    validate_selector(class_ref, class_key, field, false)?;
    if let Some(key) = collection_key {
        validate_collection_key(key)?;
    }
    if let Some(key) = class_key {
        validate_class_key(key)?;
    }
    Ok(())
}

/// Per-item dry-run result from a rollback-only backend transaction.
#[derive(Debug)]
pub struct StorageImportPreflightItem {
    index: usize,
    observed_revision: Option<ResourceRevision>,
    error: Option<StorageError>,
}

impl StorageImportPreflightItem {
    #[must_use]
    pub const fn success(index: usize, observed_revision: Option<ResourceRevision>) -> Self {
        Self {
            index,
            observed_revision,
            error: None,
        }
    }

    #[must_use]
    pub const fn failure(
        index: usize,
        observed_revision: Option<ResourceRevision>,
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
    pub fn into_parts(self) -> (usize, Option<ResourceRevision>, Option<StorageError>) {
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
    task_id: TaskId,
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
        task_id: TaskId,
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
        TaskId,
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
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError>;

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError>;

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError>;

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError>;

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError>;

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClass>, StorageError>;

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClass>, StorageError>;

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError>;

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError>;

    /// Reports whether a relation exists between two persisted class IDs.
    ///
    /// Both IDs are positive storage identifiers. Planner-local virtual IDs
    /// remain in the application layer and are never passed to a backend.
    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError>;

    /// Reports whether a relation exists between two persisted object IDs.
    ///
    /// Both IDs are positive storage identifiers. Planner-local virtual IDs
    /// remain in the application layer and are never passed to a backend.
    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError>;

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError>;

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError>;

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError>;

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
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
        assert!(StorageImportRevision::try_new(0).is_err());
        assert_eq!(StorageImportRevision::try_new(7).unwrap().get(), 7);
    }

    #[test]
    fn timestamps_preserve_chronological_order() {
        let created_at = NaiveDate::from_ymd_opt(2026, 8, 14)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let updated_at = NaiveDate::from_ymd_opt(2026, 8, 14)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();

        assert!(StorageImportTimestamps::try_new(created_at, updated_at).is_err());
    }

    #[test]
    fn overwrite_condition_allows_create_or_update() {
        assert!(!StorageImportWriteCondition::Overwrite.requires_existing());
        assert!(
            StorageImportWriteCondition::IfRevision(StorageImportRevision::try_new(1).unwrap())
                .requires_existing()
        );
    }

    #[test]
    fn import_mode_exposes_one_complete_policy() {
        let mode = StorageImportMode::new(
            StorageImportAtomicity::BestEffort,
            StorageImportCollisionPolicy::Overwrite,
            StorageImportPermissionPolicy::Continue,
        );

        assert_eq!(
            mode.into_parts(),
            (
                StorageImportAtomicity::BestEffort,
                StorageImportCollisionPolicy::Overwrite,
                StorageImportPermissionPolicy::Continue,
            )
        );
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

    #[test]
    fn import_plan_rejects_non_increasing_indexes() {
        let operation = || StorageImportOperation::UpsertIdentityScope {
            input: StorageImportIdentityScope::from_parts(StorageImportIdentityScopeParts {
                reference: None,
                name: "local".to_string(),
                provider_kind: "local".to_string(),
                condition: None,
                timestamps: None,
            }),
            overwrite: false,
        };

        let result = StorageImportPlan::try_new(vec![
            StorageImportPlanItem::new(1, operation()),
            StorageImportPlanItem::new(1, operation()),
        ]);

        assert!(result.is_err_and(|error| {
            error
                .to_string()
                .contains("indexes must be strictly increasing")
        }));
    }

    #[test]
    fn import_plan_rejects_invalid_operation_shape() {
        let result = StorageImportPlan::try_new(vec![StorageImportPlanItem::new(
            0,
            StorageImportOperation::UpsertIdentityScope {
                input: StorageImportIdentityScope::from_parts(StorageImportIdentityScopeParts {
                    reference: None,
                    name: " ".to_string(),
                    provider_kind: "local".to_string(),
                    condition: None,
                    timestamps: None,
                }),
                overwrite: false,
            },
        )]);

        assert!(result.is_err_and(|error| {
            error
                .to_string()
                .contains("identity scope name must not be empty")
        }));
    }
}
