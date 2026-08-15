use serde::Serialize;
use serde_json::Value;

use crate::errors::ApiError;
use crate::models::{
    ClassKey, CollectionKey, EventSinkKey, GroupKey, IdentityScopeKey, ImportAtomicity,
    ImportClassInput, ImportClassRelationInput, ImportCollectionInput,
    ImportCollectionPermissionInput, ImportCollisionPolicy, ImportComputedFieldInput,
    ImportComputedFieldVisibility, ImportEventSinkInput, ImportEventSubscriptionInput,
    ImportExportTemplateInput, ImportMembershipSourceInput, ImportMode, ImportObjectInput,
    ImportObjectRelationInput, ImportPermissionPolicy, ImportPrincipalSubtype,
    ImportRemoteTargetInput, ImportWriteCondition, ObjectKey, ObjectRelationLimit, PrincipalKey,
    RestoreTimestamps,
};
use crate::permissions::permission_to_storage;
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

pub(crate) fn collection_key_to_storage(key: CollectionKey) -> StorageImportCollectionKey {
    StorageImportCollectionKey::from_parts(StorageImportCollectionKeyParts {
        name: key.name,
        path: key.path,
    })
}

fn identity_scope_key_to_storage(key: IdentityScopeKey) -> StorageImportIdentityScopeKey {
    StorageImportIdentityScopeKey::from_parts(StorageImportIdentityScopeKeyParts { name: key.name })
}

fn group_key_to_storage(key: GroupKey) -> StorageImportGroupKey {
    StorageImportGroupKey::from_parts(StorageImportGroupKeyParts {
        identity_scope: key.identity_scope,
        name: key.groupname,
    })
}

fn principal_key_to_storage(key: PrincipalKey) -> StorageImportPrincipalKey {
    StorageImportPrincipalKey::from_parts(StorageImportPrincipalKeyParts {
        identity_scope: key.identity_scope,
        name: key.name,
    })
}

fn event_sink_key_to_storage(key: EventSinkKey) -> StorageImportEventSinkKey {
    StorageImportEventSinkKey::from_parts(StorageImportEventSinkKeyParts { name: key.name })
}

fn class_key_to_storage(key: ClassKey) -> StorageImportClassKey {
    StorageImportClassKey::from_parts(StorageImportClassKeyParts {
        name: key.name,
        collection_ref: key.collection_ref,
        collection_key: key.collection_key.map(collection_key_to_storage),
    })
}

fn object_key_to_storage(key: ObjectKey) -> StorageImportObjectKey {
    StorageImportObjectKey::from_parts(StorageImportObjectKeyParts {
        name: key.name,
        class_ref: key.class_ref,
        class_key: key.class_key.map(class_key_to_storage),
    })
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
