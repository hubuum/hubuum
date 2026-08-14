use std::str::FromStr;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::errors::ApiError;
use crate::models::{
    ClassKey, CollectionKey, ComputedResultType, EventSinkKey, EventSinkKind, ExportContentType,
    ExportInclude, ExportLimits, ExportMissingDataPolicy, ExportRelationContext, ExportScopeKind,
    ExportTemplateKind, GroupKey, IdentityScopeKey, ImportAtomicity, ImportClassInput,
    ImportClassRelationInput, ImportCollectionInput, ImportCollectionPermissionInput,
    ImportCollisionPolicy, ImportComputedFieldInput, ImportComputedFieldVisibility,
    ImportEventSinkInput, ImportEventSubscriptionInput, ImportExportTemplateInput,
    ImportGroupInput, ImportGroupMembershipInput, ImportIdentityScopeInput,
    ImportMembershipSourceInput, ImportMode, ImportObjectInput, ImportObjectRelationInput,
    ImportPermissionPolicy, ImportPrincipalInput, ImportPrincipalSubtype, ImportRemoteTargetInput,
    ImportWriteCondition, ObjectKey, ObjectRelationLimit, PrincipalKey, RemoteAuthConfig,
    RemoteHttpMethod, RemoteTargetSubjectType, RestoreTimestamps,
};
use crate::permissions::{permission_from_storage, permission_to_storage};
use crate::storage::ApplicationImportOperation;
use hubuum_storage_core::{
    StorageImportAtomicity, StorageImportClass, StorageImportClassKey, StorageImportClassKeyParts,
    StorageImportClassParts, StorageImportClassRelation, StorageImportClassRelationParts,
    StorageImportCollection, StorageImportCollectionKey, StorageImportCollectionKeyParts,
    StorageImportCollectionParts, StorageImportCollectionPermission,
    StorageImportCollectionPermissionParts, StorageImportCollisionPolicy,
    StorageImportComputedField, StorageImportComputedFieldParts,
    StorageImportComputedFieldVisibility, StorageImportEventSink, StorageImportEventSinkKey,
    StorageImportEventSinkKeyParts, StorageImportEventSinkParts, StorageImportEventSubscription,
    StorageImportEventSubscriptionParts, StorageImportExportTemplate,
    StorageImportExportTemplateParts, StorageImportGroup, StorageImportGroupKey,
    StorageImportGroupKeyParts, StorageImportGroupMembership, StorageImportGroupMembershipParts,
    StorageImportGroupParts, StorageImportIdentityScope, StorageImportIdentityScopeKey,
    StorageImportIdentityScopeKeyParts, StorageImportIdentityScopeParts,
    StorageImportMembershipSource, StorageImportMembershipSourceParts, StorageImportMode,
    StorageImportObject, StorageImportObjectKey, StorageImportObjectKeyParts,
    StorageImportObjectParts, StorageImportObjectRelation, StorageImportObjectRelationParts,
    StorageImportOperation, StorageImportPermissionPolicy, StorageImportPrincipal,
    StorageImportPrincipalKey, StorageImportPrincipalKeyParts, StorageImportPrincipalParts,
    StorageImportPrincipalSubtype, StorageImportRemoteTarget, StorageImportRemoteTargetParts,
    StorageImportRevision, StorageImportTimestamps, StorageImportWriteCondition,
};

fn json_to_storage<T: Serialize>(value: T, field: &str) -> Result<Value, ApiError> {
    serde_json::to_value(value).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Validated import field '{field}' could not cross the storage boundary: {error}"
        ))
    })
}

fn json_from_storage<T: DeserializeOwned>(value: Value, field: &str) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|error| {
        ApiError::BadRequest(format!(
            "Storage import field '{field}' has an invalid canonical value: {error}"
        ))
    })
}

fn timestamps_to_storage(
    timestamps: Option<RestoreTimestamps>,
) -> Result<Option<StorageImportTimestamps>, ApiError> {
    timestamps.map(timestamp_to_storage).transpose()
}

fn timestamp_to_storage(
    timestamps: RestoreTimestamps,
) -> Result<StorageImportTimestamps, ApiError> {
    let (created_at, updated_at) = timestamps.as_pair();
    StorageImportTimestamps::new(created_at, updated_at).map_err(ApiError::from)
}

fn timestamps_from_storage(
    timestamps: Option<StorageImportTimestamps>,
) -> Result<Option<RestoreTimestamps>, ApiError> {
    timestamps.map(timestamp_from_storage).transpose()
}

fn timestamp_from_storage(
    timestamps: StorageImportTimestamps,
) -> Result<RestoreTimestamps, ApiError> {
    let (created_at, updated_at) = timestamps.into_parts();
    RestoreTimestamps::new(created_at, updated_at)
}

fn condition_to_storage(
    condition: Option<ImportWriteCondition>,
) -> Result<Option<StorageImportWriteCondition>, ApiError> {
    condition
        .map(|condition| match condition {
            ImportWriteCondition::CreateOnly => Ok(StorageImportWriteCondition::CreateOnly),
            ImportWriteCondition::Overwrite => Ok(StorageImportWriteCondition::Overwrite),
            ImportWriteCondition::IfRevision { expected_revision } => {
                StorageImportRevision::new(expected_revision.get())
                    .map(StorageImportWriteCondition::IfRevision)
                    .map_err(ApiError::from)
            }
        })
        .transpose()
}

fn condition_from_storage(
    condition: Option<StorageImportWriteCondition>,
) -> Result<Option<ImportWriteCondition>, ApiError> {
    condition
        .map(|condition| match condition {
            StorageImportWriteCondition::CreateOnly => Ok(ImportWriteCondition::CreateOnly),
            StorageImportWriteCondition::Overwrite => Ok(ImportWriteCondition::Overwrite),
            StorageImportWriteCondition::IfRevision(expected_revision) => {
                Ok(ImportWriteCondition::IfRevision {
                    expected_revision: crate::models::ResourceRevision::new(
                        expected_revision.get(),
                    )?,
                })
            }
        })
        .transpose()
}

pub(crate) fn import_mode_to_storage(mode: ImportMode) -> StorageImportMode {
    StorageImportMode::new(
        mode.atomicity.map(|value| match value {
            ImportAtomicity::Strict => StorageImportAtomicity::Strict,
            ImportAtomicity::BestEffort => StorageImportAtomicity::BestEffort,
        }),
        mode.collision_policy.map(|value| match value {
            ImportCollisionPolicy::Abort => StorageImportCollisionPolicy::Abort,
            ImportCollisionPolicy::Overwrite => StorageImportCollisionPolicy::Overwrite,
        }),
        mode.permission_policy.map(|value| match value {
            ImportPermissionPolicy::Abort => StorageImportPermissionPolicy::Abort,
            ImportPermissionPolicy::Continue => StorageImportPermissionPolicy::Continue,
        }),
    )
}

pub(crate) fn import_mode_from_storage(mode: StorageImportMode) -> ImportMode {
    let (atomicity, collision_policy, permission_policy) = mode.into_parts();
    ImportMode {
        atomicity: atomicity.map(|value| match value {
            StorageImportAtomicity::Strict => ImportAtomicity::Strict,
            StorageImportAtomicity::BestEffort => ImportAtomicity::BestEffort,
        }),
        collision_policy: collision_policy.map(|value| match value {
            StorageImportCollisionPolicy::Abort => ImportCollisionPolicy::Abort,
            StorageImportCollisionPolicy::Overwrite => ImportCollisionPolicy::Overwrite,
        }),
        permission_policy: permission_policy.map(|value| match value {
            StorageImportPermissionPolicy::Abort => ImportPermissionPolicy::Abort,
            StorageImportPermissionPolicy::Continue => ImportPermissionPolicy::Continue,
        }),
    }
}

pub(crate) fn collection_key_to_storage(key: CollectionKey) -> StorageImportCollectionKey {
    StorageImportCollectionKey::from_parts(StorageImportCollectionKeyParts {
        name: key.name,
        path: key.path,
    })
}

pub(crate) fn collection_key_from_storage(key: StorageImportCollectionKey) -> CollectionKey {
    let parts = key.into_parts();
    CollectionKey {
        name: parts.name,
        path: parts.path,
    }
}

fn identity_scope_key_to_storage(key: IdentityScopeKey) -> StorageImportIdentityScopeKey {
    StorageImportIdentityScopeKey::from_parts(StorageImportIdentityScopeKeyParts { name: key.name })
}

fn identity_scope_key_from_storage(key: StorageImportIdentityScopeKey) -> IdentityScopeKey {
    IdentityScopeKey {
        name: key.into_parts().name,
    }
}

fn group_key_to_storage(key: GroupKey) -> StorageImportGroupKey {
    StorageImportGroupKey::from_parts(StorageImportGroupKeyParts {
        identity_scope: key.identity_scope,
        name: key.groupname,
    })
}

fn group_key_from_storage(key: StorageImportGroupKey) -> GroupKey {
    let parts = key.into_parts();
    GroupKey {
        identity_scope: parts.identity_scope,
        groupname: parts.name,
    }
}

fn principal_key_to_storage(key: PrincipalKey) -> StorageImportPrincipalKey {
    StorageImportPrincipalKey::from_parts(StorageImportPrincipalKeyParts {
        identity_scope: key.identity_scope,
        name: key.name,
    })
}

fn principal_key_from_storage(key: StorageImportPrincipalKey) -> PrincipalKey {
    let parts = key.into_parts();
    PrincipalKey {
        identity_scope: parts.identity_scope,
        name: parts.name,
    }
}

fn event_sink_key_to_storage(key: EventSinkKey) -> StorageImportEventSinkKey {
    StorageImportEventSinkKey::from_parts(StorageImportEventSinkKeyParts { name: key.name })
}

fn event_sink_key_from_storage(key: StorageImportEventSinkKey) -> EventSinkKey {
    EventSinkKey {
        name: key.into_parts().name,
    }
}

fn class_key_to_storage(key: ClassKey) -> StorageImportClassKey {
    StorageImportClassKey::from_parts(StorageImportClassKeyParts {
        name: key.name,
        collection_ref: key.collection_ref,
        collection_key: key.collection_key.map(collection_key_to_storage),
    })
}

fn class_key_from_storage(key: StorageImportClassKey) -> ClassKey {
    let parts = key.into_parts();
    ClassKey {
        name: parts.name,
        collection_ref: parts.collection_ref,
        collection_key: parts.collection_key.map(collection_key_from_storage),
    }
}

fn object_key_to_storage(key: ObjectKey) -> StorageImportObjectKey {
    StorageImportObjectKey::from_parts(StorageImportObjectKeyParts {
        name: key.name,
        class_ref: key.class_ref,
        class_key: key.class_key.map(class_key_to_storage),
    })
}

fn object_key_from_storage(key: StorageImportObjectKey) -> ObjectKey {
    let parts = key.into_parts();
    ObjectKey {
        name: parts.name,
        class_ref: parts.class_ref,
        class_key: parts.class_key.map(class_key_from_storage),
    }
}

fn principal_subtype_to_storage(subtype: ImportPrincipalSubtype) -> StorageImportPrincipalSubtype {
    match subtype {
        ImportPrincipalSubtype::Human {
            password,
            password_hash,
            proper_name,
            email,
            anonymized_at,
        } => StorageImportPrincipalSubtype::Human {
            password,
            password_hash,
            proper_name,
            email,
            anonymized_at,
        },
        ImportPrincipalSubtype::ServiceAccount {
            description,
            owner_group_ref,
            owner_group_key,
            created_by_ref,
            created_by_key,
            disabled_at,
        } => StorageImportPrincipalSubtype::ServiceAccount {
            description,
            owner_group_ref,
            owner_group_key: owner_group_key.map(group_key_to_storage),
            created_by_ref,
            created_by_key: created_by_key.map(principal_key_to_storage),
            disabled_at,
        },
    }
}

fn principal_subtype_from_storage(
    subtype: StorageImportPrincipalSubtype,
) -> ImportPrincipalSubtype {
    match subtype {
        StorageImportPrincipalSubtype::Human {
            password,
            password_hash,
            proper_name,
            email,
            anonymized_at,
        } => ImportPrincipalSubtype::Human {
            password,
            password_hash,
            proper_name,
            email,
            anonymized_at,
        },
        StorageImportPrincipalSubtype::ServiceAccount {
            description,
            owner_group_ref,
            owner_group_key,
            created_by_ref,
            created_by_key,
            disabled_at,
        } => ImportPrincipalSubtype::ServiceAccount {
            description,
            owner_group_ref,
            owner_group_key: owner_group_key.map(group_key_from_storage),
            created_by_ref,
            created_by_key: created_by_key.map(principal_key_from_storage),
            disabled_at,
        },
    }
}

fn membership_source_to_storage(
    input: ImportMembershipSourceInput,
) -> Result<StorageImportMembershipSource, ApiError> {
    Ok(StorageImportMembershipSource::from_parts(
        StorageImportMembershipSourceParts {
            source: input.source,
            source_scope_ref: input.source_scope_ref,
            source_scope_key: input.source_scope_key.map(identity_scope_key_to_storage),
            source_key: input.source_key,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn membership_source_from_storage(
    input: StorageImportMembershipSource,
) -> Result<ImportMembershipSourceInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportMembershipSourceInput {
        source: parts.source,
        source_scope_ref: parts.source_scope_ref,
        source_scope_key: parts.source_scope_key.map(identity_scope_key_from_storage),
        source_key: parts.source_key,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

pub(crate) fn import_operation_to_storage(
    operation: ApplicationImportOperation,
) -> Result<StorageImportOperation, ApiError> {
    Ok(match operation {
        ApplicationImportOperation::UpsertIdentityScope { input, overwrite } => {
            StorageImportOperation::UpsertIdentityScope {
                input: StorageImportIdentityScope::from_parts(StorageImportIdentityScopeParts {
                    reference: input.ref_,
                    name: input.name,
                    provider_kind: input.provider_kind,
                    condition: condition_to_storage(input.condition)?,
                    timestamps: timestamps_to_storage(input.timestamps)?,
                }),
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertGroup { input, overwrite } => {
            StorageImportOperation::UpsertGroup {
                input: StorageImportGroup::from_parts(StorageImportGroupParts {
                    reference: input.ref_,
                    name: input.groupname,
                    description: input.description,
                    identity_scope_ref: input.identity_scope_ref,
                    identity_scope_key: input.identity_scope_key.map(identity_scope_key_to_storage),
                    managed_by: input.managed_by,
                    external_key: input.external_key,
                    last_sync_attempted_at: input.last_sync_attempted_at,
                    last_sync_success_at: input.last_sync_success_at,
                    condition: condition_to_storage(input.condition)?,
                    timestamps: timestamps_to_storage(input.timestamps)?,
                }),
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertPrincipal { input, overwrite } => {
            StorageImportOperation::UpsertPrincipal {
                input: StorageImportPrincipal::from_parts(StorageImportPrincipalParts {
                    reference: input.ref_,
                    name: input.name,
                    identity_scope_ref: input.identity_scope_ref,
                    identity_scope_key: input.identity_scope_key.map(identity_scope_key_to_storage),
                    provider_managed: input.provider_managed,
                    settings: input.settings,
                    external_subject: input.external_subject,
                    last_sync_attempted_at: input.last_sync_attempted_at,
                    last_sync_success_at: input.last_sync_success_at,
                    subtype: principal_subtype_to_storage(input.subtype),
                    condition: condition_to_storage(input.condition)?,
                    timestamps: timestamps_to_storage(input.timestamps)?,
                }),
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertGroupMembership { input, overwrite } => {
            StorageImportOperation::UpsertGroupMembership {
                input: StorageImportGroupMembership::from_parts(
                    StorageImportGroupMembershipParts {
                        reference: input.ref_,
                        principal_ref: input.principal_ref,
                        principal_key: input.principal_key.map(principal_key_to_storage),
                        group_ref: input.group_ref,
                        group_key: input.group_key.map(group_key_to_storage),
                        sources: input
                            .sources
                            .into_iter()
                            .map(membership_source_to_storage)
                            .collect::<Result<_, _>>()?,
                        condition: condition_to_storage(input.condition)?,
                        timestamps: timestamps_to_storage(input.timestamps)?,
                    },
                ),
                overwrite,
            }
        }
        ApplicationImportOperation::CreateCollection(input) => {
            StorageImportOperation::CreateCollection(collection_to_storage(input)?)
        }
        ApplicationImportOperation::UpdateCollection {
            collection_id,
            input,
        } => StorageImportOperation::UpdateCollection {
            collection_id,
            input: collection_to_storage(input)?,
        },
        ApplicationImportOperation::CreateClass(input) => {
            StorageImportOperation::CreateClass(class_to_storage(input)?)
        }
        ApplicationImportOperation::UpdateClass { class_id, input } => {
            StorageImportOperation::UpdateClass {
                class_id,
                input: class_to_storage(input)?,
            }
        }
        ApplicationImportOperation::CreateObject(input) => {
            StorageImportOperation::CreateObject(object_to_storage(input)?)
        }
        ApplicationImportOperation::UpdateObject { object_id, input } => {
            StorageImportOperation::UpdateObject {
                object_id,
                input: object_to_storage(input)?,
            }
        }
        ApplicationImportOperation::UpsertComputedField { input, overwrite } => {
            StorageImportOperation::UpsertComputedField {
                input: computed_field_to_storage(input)?,
                overwrite,
            }
        }
        ApplicationImportOperation::CreateClassRelation(input) => {
            StorageImportOperation::CreateClassRelation(class_relation_to_storage(input)?)
        }
        ApplicationImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
            StorageImportOperation::UpdateClassRelationTimestamps {
                input: class_relation_to_storage(input)?,
                timestamps: timestamp_to_storage(timestamps)?,
            }
        }
        ApplicationImportOperation::CheckClassRelationCondition(input) => {
            StorageImportOperation::CheckClassRelationCondition(class_relation_to_storage(input)?)
        }
        ApplicationImportOperation::CreateObjectRelation(input) => {
            StorageImportOperation::CreateObjectRelation(object_relation_to_storage(input)?)
        }
        ApplicationImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
            StorageImportOperation::UpdateObjectRelationTimestamps {
                input: object_relation_to_storage(input)?,
                timestamps: timestamp_to_storage(timestamps)?,
            }
        }
        ApplicationImportOperation::CheckObjectRelationCondition(input) => {
            StorageImportOperation::CheckObjectRelationCondition(object_relation_to_storage(input)?)
        }
        ApplicationImportOperation::ApplyCollectionPermissions { input, overwrite } => {
            StorageImportOperation::ApplyCollectionPermissions {
                input: collection_permission_to_storage(input)?,
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertExportTemplate { input, overwrite } => {
            StorageImportOperation::UpsertExportTemplate {
                input: export_template_to_storage(input)?,
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertRemoteTarget { input, overwrite } => {
            StorageImportOperation::UpsertRemoteTarget {
                input: remote_target_to_storage(input)?,
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertEventSink { input, overwrite } => {
            StorageImportOperation::UpsertEventSink {
                input: event_sink_to_storage(input)?,
                overwrite,
            }
        }
        ApplicationImportOperation::UpsertEventSubscription { input, overwrite } => {
            StorageImportOperation::UpsertEventSubscription {
                input: event_subscription_to_storage(input)?,
                overwrite,
            }
        }
    })
}

pub(crate) fn import_operation_from_storage(
    operation: StorageImportOperation,
) -> Result<ApplicationImportOperation, ApiError> {
    Ok(match operation {
        StorageImportOperation::UpsertIdentityScope { input, overwrite } => {
            let parts = input.into_parts();
            ApplicationImportOperation::UpsertIdentityScope {
                input: ImportIdentityScopeInput {
                    ref_: parts.reference,
                    name: parts.name,
                    provider_kind: parts.provider_kind,
                    condition: condition_from_storage(parts.condition)?,
                    timestamps: timestamps_from_storage(parts.timestamps)?,
                },
                overwrite,
            }
        }
        StorageImportOperation::UpsertGroup { input, overwrite } => {
            let parts = input.into_parts();
            ApplicationImportOperation::UpsertGroup {
                input: ImportGroupInput {
                    ref_: parts.reference,
                    groupname: parts.name,
                    description: parts.description,
                    identity_scope_ref: parts.identity_scope_ref,
                    identity_scope_key: parts
                        .identity_scope_key
                        .map(identity_scope_key_from_storage),
                    managed_by: parts.managed_by,
                    external_key: parts.external_key,
                    last_sync_attempted_at: parts.last_sync_attempted_at,
                    last_sync_success_at: parts.last_sync_success_at,
                    condition: condition_from_storage(parts.condition)?,
                    timestamps: timestamps_from_storage(parts.timestamps)?,
                },
                overwrite,
            }
        }
        StorageImportOperation::UpsertPrincipal { input, overwrite } => {
            let parts = input.into_parts();
            ApplicationImportOperation::UpsertPrincipal {
                input: ImportPrincipalInput {
                    ref_: parts.reference,
                    name: parts.name,
                    identity_scope_ref: parts.identity_scope_ref,
                    identity_scope_key: parts
                        .identity_scope_key
                        .map(identity_scope_key_from_storage),
                    provider_managed: parts.provider_managed,
                    settings: parts.settings,
                    external_subject: parts.external_subject,
                    last_sync_attempted_at: parts.last_sync_attempted_at,
                    last_sync_success_at: parts.last_sync_success_at,
                    subtype: principal_subtype_from_storage(parts.subtype),
                    condition: condition_from_storage(parts.condition)?,
                    timestamps: timestamps_from_storage(parts.timestamps)?,
                },
                overwrite,
            }
        }
        StorageImportOperation::UpsertGroupMembership { input, overwrite } => {
            let parts = input.into_parts();
            ApplicationImportOperation::UpsertGroupMembership {
                input: ImportGroupMembershipInput {
                    ref_: parts.reference,
                    principal_ref: parts.principal_ref,
                    principal_key: parts.principal_key.map(principal_key_from_storage),
                    group_ref: parts.group_ref,
                    group_key: parts.group_key.map(group_key_from_storage),
                    sources: parts
                        .sources
                        .into_iter()
                        .map(membership_source_from_storage)
                        .collect::<Result<_, _>>()?,
                    condition: condition_from_storage(parts.condition)?,
                    timestamps: timestamps_from_storage(parts.timestamps)?,
                },
                overwrite,
            }
        }
        StorageImportOperation::CreateCollection(input) => {
            ApplicationImportOperation::CreateCollection(collection_from_storage(input)?)
        }
        StorageImportOperation::UpdateCollection {
            collection_id,
            input,
        } => ApplicationImportOperation::UpdateCollection {
            collection_id,
            input: collection_from_storage(input)?,
        },
        StorageImportOperation::CreateClass(input) => {
            ApplicationImportOperation::CreateClass(class_from_storage(input)?)
        }
        StorageImportOperation::UpdateClass { class_id, input } => {
            ApplicationImportOperation::UpdateClass {
                class_id,
                input: class_from_storage(input)?,
            }
        }
        StorageImportOperation::CreateObject(input) => {
            ApplicationImportOperation::CreateObject(object_from_storage(input)?)
        }
        StorageImportOperation::UpdateObject { object_id, input } => {
            ApplicationImportOperation::UpdateObject {
                object_id,
                input: object_from_storage(input)?,
            }
        }
        StorageImportOperation::UpsertComputedField { input, overwrite } => {
            ApplicationImportOperation::UpsertComputedField {
                input: computed_field_from_storage(input)?,
                overwrite,
            }
        }
        StorageImportOperation::CreateClassRelation(input) => {
            ApplicationImportOperation::CreateClassRelation(class_relation_from_storage(input)?)
        }
        StorageImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
            ApplicationImportOperation::UpdateClassRelationTimestamps {
                input: class_relation_from_storage(input)?,
                timestamps: timestamp_from_storage(timestamps)?,
            }
        }
        StorageImportOperation::CheckClassRelationCondition(input) => {
            ApplicationImportOperation::CheckClassRelationCondition(class_relation_from_storage(
                input,
            )?)
        }
        StorageImportOperation::CreateObjectRelation(input) => {
            ApplicationImportOperation::CreateObjectRelation(object_relation_from_storage(input)?)
        }
        StorageImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
            ApplicationImportOperation::UpdateObjectRelationTimestamps {
                input: object_relation_from_storage(input)?,
                timestamps: timestamp_from_storage(timestamps)?,
            }
        }
        StorageImportOperation::CheckObjectRelationCondition(input) => {
            ApplicationImportOperation::CheckObjectRelationCondition(object_relation_from_storage(
                input,
            )?)
        }
        StorageImportOperation::ApplyCollectionPermissions { input, overwrite } => {
            ApplicationImportOperation::ApplyCollectionPermissions {
                input: collection_permission_from_storage(input)?,
                overwrite,
            }
        }
        StorageImportOperation::UpsertExportTemplate { input, overwrite } => {
            ApplicationImportOperation::UpsertExportTemplate {
                input: export_template_from_storage(input)?,
                overwrite,
            }
        }
        StorageImportOperation::UpsertRemoteTarget { input, overwrite } => {
            ApplicationImportOperation::UpsertRemoteTarget {
                input: remote_target_from_storage(input)?,
                overwrite,
            }
        }
        StorageImportOperation::UpsertEventSink { input, overwrite } => {
            ApplicationImportOperation::UpsertEventSink {
                input: event_sink_from_storage(input)?,
                overwrite,
            }
        }
        StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
            ApplicationImportOperation::UpsertEventSubscription {
                input: event_subscription_from_storage(input)?,
                overwrite,
            }
        }
    })
}

fn collection_to_storage(
    input: ImportCollectionInput,
) -> Result<StorageImportCollection, ApiError> {
    Ok(StorageImportCollection::from_parts(
        StorageImportCollectionParts {
            reference: input.ref_,
            name: input.name,
            description: input.description,
            parent_collection_ref: input.parent_collection_ref,
            parent_collection_key: input.parent_collection_key.map(collection_key_to_storage),
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn collection_from_storage(
    input: StorageImportCollection,
) -> Result<ImportCollectionInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportCollectionInput {
        ref_: parts.reference,
        name: parts.name,
        description: parts.description,
        parent_collection_ref: parts.parent_collection_ref,
        parent_collection_key: parts.parent_collection_key.map(collection_key_from_storage),
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn class_to_storage(input: ImportClassInput) -> Result<StorageImportClass, ApiError> {
    Ok(StorageImportClass::from_parts(StorageImportClassParts {
        reference: input.ref_,
        name: input.name,
        description: input.description,
        json_schema: input.json_schema,
        validate_schema: input.validate_schema,
        collection_ref: input.collection_ref,
        collection_key: input.collection_key.map(collection_key_to_storage),
        condition: condition_to_storage(input.condition)?,
        timestamps: timestamps_to_storage(input.timestamps)?,
    }))
}

fn class_from_storage(input: StorageImportClass) -> Result<ImportClassInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportClassInput {
        ref_: parts.reference,
        name: parts.name,
        description: parts.description,
        json_schema: parts.json_schema,
        validate_schema: parts.validate_schema,
        collection_ref: parts.collection_ref,
        collection_key: parts.collection_key.map(collection_key_from_storage),
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn object_to_storage(input: ImportObjectInput) -> Result<StorageImportObject, ApiError> {
    Ok(StorageImportObject::from_parts(StorageImportObjectParts {
        reference: input.ref_,
        name: input.name,
        description: input.description,
        data: input.data,
        class_ref: input.class_ref,
        class_key: input.class_key.map(class_key_to_storage),
        condition: condition_to_storage(input.condition)?,
        timestamps: timestamps_to_storage(input.timestamps)?,
    }))
}

fn object_from_storage(input: StorageImportObject) -> Result<ImportObjectInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportObjectInput {
        ref_: parts.reference,
        name: parts.name,
        description: parts.description,
        data: parts.data,
        class_ref: parts.class_ref,
        class_key: parts.class_key.map(class_key_from_storage),
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn computed_field_to_storage(
    input: ImportComputedFieldInput,
) -> Result<StorageImportComputedField, ApiError> {
    Ok(StorageImportComputedField::from_parts(
        StorageImportComputedFieldParts {
            reference: input.ref_,
            class_ref: input.class_ref,
            class_key: input.class_key.map(class_key_to_storage),
            visibility: match input.visibility {
                ImportComputedFieldVisibility::Shared => {
                    StorageImportComputedFieldVisibility::Shared
                }
                ImportComputedFieldVisibility::Personal => {
                    StorageImportComputedFieldVisibility::Personal
                }
            },
            owner_ref: input.owner_ref,
            owner_key: input.owner_key.map(principal_key_to_storage),
            key: input.key,
            label: input.label,
            description: input.description,
            operation: input.operation,
            result_type: input.result_type.as_str().to_string(),
            enabled: input.enabled,
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn computed_field_from_storage(
    input: StorageImportComputedField,
) -> Result<ImportComputedFieldInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportComputedFieldInput {
        ref_: parts.reference,
        class_ref: parts.class_ref,
        class_key: parts.class_key.map(class_key_from_storage),
        visibility: match parts.visibility {
            StorageImportComputedFieldVisibility::Shared => ImportComputedFieldVisibility::Shared,
            StorageImportComputedFieldVisibility::Personal => {
                ImportComputedFieldVisibility::Personal
            }
        },
        owner_ref: parts.owner_ref,
        owner_key: parts.owner_key.map(principal_key_from_storage),
        key: parts.key,
        label: parts.label,
        description: parts.description,
        operation: parts.operation,
        result_type: ComputedResultType::from_db(&parts.result_type)?,
        enabled: parts.enabled,
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn class_relation_to_storage(
    input: ImportClassRelationInput,
) -> Result<StorageImportClassRelation, ApiError> {
    Ok(StorageImportClassRelation::from_parts(
        StorageImportClassRelationParts {
            reference: input.ref_,
            from_class_ref: input.from_class_ref,
            from_class_key: input.from_class_key.map(class_key_to_storage),
            to_class_ref: input.to_class_ref,
            to_class_key: input.to_class_key.map(class_key_to_storage),
            forward_template_alias: input.forward_template_alias,
            reverse_template_alias: input.reverse_template_alias,
            from_max_relations: input.from_max_relations.map(ObjectRelationLimit::value),
            to_max_relations: input.to_max_relations.map(ObjectRelationLimit::value),
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn class_relation_from_storage(
    input: StorageImportClassRelation,
) -> Result<ImportClassRelationInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportClassRelationInput {
        ref_: parts.reference,
        from_class_ref: parts.from_class_ref,
        from_class_key: parts.from_class_key.map(class_key_from_storage),
        to_class_ref: parts.to_class_ref,
        to_class_key: parts.to_class_key.map(class_key_from_storage),
        forward_template_alias: parts.forward_template_alias,
        reverse_template_alias: parts.reverse_template_alias,
        from_max_relations: parts
            .from_max_relations
            .map(ObjectRelationLimit::new)
            .transpose()?,
        to_max_relations: parts
            .to_max_relations
            .map(ObjectRelationLimit::new)
            .transpose()?,
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn object_relation_to_storage(
    input: ImportObjectRelationInput,
) -> Result<StorageImportObjectRelation, ApiError> {
    Ok(StorageImportObjectRelation::from_parts(
        StorageImportObjectRelationParts {
            reference: input.ref_,
            from_object_ref: input.from_object_ref,
            from_object_key: input.from_object_key.map(object_key_to_storage),
            to_object_ref: input.to_object_ref,
            to_object_key: input.to_object_key.map(object_key_to_storage),
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn object_relation_from_storage(
    input: StorageImportObjectRelation,
) -> Result<ImportObjectRelationInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportObjectRelationInput {
        ref_: parts.reference,
        from_object_ref: parts.from_object_ref,
        from_object_key: parts.from_object_key.map(object_key_from_storage),
        to_object_ref: parts.to_object_ref,
        to_object_key: parts.to_object_key.map(object_key_from_storage),
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn collection_permission_to_storage(
    input: ImportCollectionPermissionInput,
) -> Result<StorageImportCollectionPermission, ApiError> {
    Ok(StorageImportCollectionPermission::from_parts(
        StorageImportCollectionPermissionParts {
            reference: input.ref_,
            collection_ref: input.collection_ref,
            collection_key: input.collection_key.map(collection_key_to_storage),
            group_key: group_key_to_storage(input.group_key),
            permissions: input
                .permissions
                .into_iter()
                .map(permission_to_storage)
                .collect(),
            replace_existing: input.replace_existing,
            condition: condition_to_storage(input.condition)?,
        },
    ))
}

fn collection_permission_from_storage(
    input: StorageImportCollectionPermission,
) -> Result<ImportCollectionPermissionInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportCollectionPermissionInput {
        ref_: parts.reference,
        collection_ref: parts.collection_ref,
        collection_key: parts.collection_key.map(collection_key_from_storage),
        group_key: group_key_from_storage(parts.group_key),
        permissions: parts
            .permissions
            .into_iter()
            .map(permission_from_storage)
            .collect(),
        replace_existing: parts.replace_existing,
        condition: condition_from_storage(parts.condition)?,
    })
}

fn export_template_to_storage(
    input: ImportExportTemplateInput,
) -> Result<StorageImportExportTemplate, ApiError> {
    Ok(StorageImportExportTemplate::from_parts(
        StorageImportExportTemplateParts {
            reference: input.ref_,
            collection_ref: input.collection_ref,
            collection_key: input.collection_key.map(collection_key_to_storage),
            class_ref: input.class_ref,
            class_key: input.class_key.map(class_key_to_storage),
            name: input.name,
            description: input.description,
            content_type: input.content_type.as_mime().to_string(),
            template: input.template,
            kind: input.kind.as_str().to_string(),
            scope_kind: input.scope_kind.map(|kind| kind.as_str().to_string()),
            default_query: input.default_query,
            include: input
                .include
                .map(|value| json_to_storage(value, "include"))
                .transpose()?,
            relation_context: input
                .relation_context
                .map(|value| json_to_storage(value, "relation_context"))
                .transpose()?,
            default_missing_data_policy: input
                .default_missing_data_policy
                .map(|policy| policy.as_str().to_string()),
            default_limits: input
                .default_limits
                .map(|value| json_to_storage(value, "default_limits"))
                .transpose()?,
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn export_template_from_storage(
    input: StorageImportExportTemplate,
) -> Result<ImportExportTemplateInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportExportTemplateInput {
        ref_: parts.reference,
        collection_ref: parts.collection_ref,
        collection_key: parts.collection_key.map(collection_key_from_storage),
        class_ref: parts.class_ref,
        class_key: parts.class_key.map(class_key_from_storage),
        name: parts.name,
        description: parts.description,
        content_type: ExportContentType::from_mime(&parts.content_type)?,
        template: parts.template,
        kind: ExportTemplateKind::from_str(&parts.kind)?,
        scope_kind: parts
            .scope_kind
            .map(|value| ExportScopeKind::from_str(&value))
            .transpose()?,
        default_query: parts.default_query,
        include: parts
            .include
            .map(|value| json_from_storage::<ExportInclude>(value, "include"))
            .transpose()?,
        relation_context: parts
            .relation_context
            .map(|value| json_from_storage::<ExportRelationContext>(value, "relation_context"))
            .transpose()?,
        default_missing_data_policy: parts
            .default_missing_data_policy
            .map(|value| ExportMissingDataPolicy::from_str(&value))
            .transpose()?,
        default_limits: parts
            .default_limits
            .map(|value| json_from_storage::<ExportLimits>(value, "default_limits"))
            .transpose()?,
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn remote_target_to_storage(
    input: ImportRemoteTargetInput,
) -> Result<StorageImportRemoteTarget, ApiError> {
    Ok(StorageImportRemoteTarget::from_parts(
        StorageImportRemoteTargetParts {
            reference: input.ref_,
            collection_ref: input.collection_ref,
            collection_key: input.collection_key.map(collection_key_to_storage),
            class_ref: input.class_ref,
            class_key: input.class_key.map(class_key_to_storage),
            name: input.name,
            description: input.description,
            method: input.method.as_str().to_string(),
            url_template: input.url_template,
            headers_template: input.headers_template,
            body_template: input.body_template,
            auth_config: json_to_storage(input.auth_config, "auth_config")?,
            allowed_subject_types: input
                .allowed_subject_types
                .into_iter()
                .map(|subject| subject.as_str().to_string())
                .collect(),
            timeout_ms: input.timeout_ms,
            enabled: input.enabled,
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn remote_target_from_storage(
    input: StorageImportRemoteTarget,
) -> Result<ImportRemoteTargetInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportRemoteTargetInput {
        ref_: parts.reference,
        collection_ref: parts.collection_ref,
        collection_key: parts.collection_key.map(collection_key_from_storage),
        class_ref: parts.class_ref,
        class_key: parts.class_key.map(class_key_from_storage),
        name: parts.name,
        description: parts.description,
        method: RemoteHttpMethod::from_str(&parts.method)?,
        url_template: parts.url_template,
        headers_template: parts.headers_template,
        body_template: parts.body_template,
        auth_config: json_from_storage::<RemoteAuthConfig>(parts.auth_config, "auth_config")?,
        allowed_subject_types: parts
            .allowed_subject_types
            .into_iter()
            .map(|value| RemoteTargetSubjectType::from_str(&value))
            .collect::<Result<_, _>>()?,
        timeout_ms: parts.timeout_ms,
        enabled: parts.enabled,
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn event_sink_to_storage(input: ImportEventSinkInput) -> Result<StorageImportEventSink, ApiError> {
    Ok(StorageImportEventSink::from_parts(
        StorageImportEventSinkParts {
            reference: input.ref_,
            name: input.name,
            kind: input.kind.as_str().to_string(),
            config: input.config,
            secret_ref: input.secret_ref,
            enabled: input.enabled,
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn event_sink_from_storage(
    input: StorageImportEventSink,
) -> Result<ImportEventSinkInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportEventSinkInput {
        ref_: parts.reference,
        name: parts.name,
        kind: EventSinkKind::from_str(&parts.kind)?,
        config: parts.config,
        secret_ref: parts.secret_ref,
        enabled: parts.enabled,
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}

fn event_subscription_to_storage(
    input: ImportEventSubscriptionInput,
) -> Result<StorageImportEventSubscription, ApiError> {
    Ok(StorageImportEventSubscription::from_parts(
        StorageImportEventSubscriptionParts {
            reference: input.ref_,
            collection_ref: input.collection_ref,
            collection_key: input.collection_key.map(collection_key_to_storage),
            sink_ref: input.sink_ref,
            sink_key: input.sink_key.map(event_sink_key_to_storage),
            name: input.name,
            description: input.description,
            entity_types: input.entity_types,
            actions: input.actions,
            filter: input.filter,
            routing: input.routing,
            enabled: input.enabled,
            condition: condition_to_storage(input.condition)?,
            timestamps: timestamps_to_storage(input.timestamps)?,
        },
    ))
}

fn event_subscription_from_storage(
    input: StorageImportEventSubscription,
) -> Result<ImportEventSubscriptionInput, ApiError> {
    let parts = input.into_parts();
    Ok(ImportEventSubscriptionInput {
        ref_: parts.reference,
        collection_ref: parts.collection_ref,
        collection_key: parts.collection_key.map(collection_key_from_storage),
        sink_ref: parts.sink_ref,
        sink_key: parts.sink_key.map(event_sink_key_from_storage),
        name: parts.name,
        description: parts.description,
        entity_types: parts.entity_types,
        actions: parts.actions,
        filter: parts.filter,
        routing: parts.routing,
        enabled: parts.enabled,
        condition: condition_from_storage(parts.condition)?,
        timestamps: timestamps_from_storage(parts.timestamps)?,
    })
}
