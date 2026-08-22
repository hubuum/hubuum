use crate::models::{
    ImportClassInput, ImportClassRelationInput, ImportCollectionInput,
    ImportCollectionPermissionInput, ImportComputedFieldInput, ImportEventSinkInput,
    ImportEventSubscriptionInput, ImportExportTemplateInput, ImportGroupInput,
    ImportGroupMembershipInput, ImportIdentityScopeInput, ImportObjectInput,
    ImportObjectRelationInput, ImportPrincipalInput, ImportRemoteTargetInput, RestoreTimestamps,
};

/// Application planning representation before the storage-boundary conversion.
///
/// This type is deliberately not a backend contract. Public import request
/// models stay in the application, while `hubuum-storage-core` owns the DTOs
/// and exhaustive operation enum consumed by adapters.
#[derive(Clone)]
pub(crate) enum ApplicationImportOperation {
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

impl ApplicationImportOperation {
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
}

impl std::fmt::Debug for ApplicationImportOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApplicationImportOperation")
            .field("kind", &self.kind())
            .field("payload", &"[redacted]")
            .finish()
    }
}
