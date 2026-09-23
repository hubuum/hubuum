use crate::errors::ApiError;
use crate::models::{
    ClassSelector, ClassSelectorKind, Collection, Group, HubuumClass, HubuumClassExpanded,
    HubuumClassRelation, HubuumObject, HubuumObjectRelation, NewCollectionWithAssignee, NewGroup,
    NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
    ObjectDataPatchDocument, ObjectRelationCreateSelector, ObjectRelationCreateSelectorKind,
    ObjectRelationEndpoint, ObjectRelationLimit, ObjectRelationSelector,
    ObjectRelationSelectorKind, ObjectSelector, ObjectSelectorKind, PreparedClassRelation,
    PreparedObjectRelation, Principal, PrincipalGroup, PrincipalSettings, PrincipalSettingsPatch,
    PrincipalSettingsResponse, ResolvedClassRelationTarget, ResolvedClassTarget,
    ResolvedObjectRelationTarget, ResolvedObjectTarget, TokenResourceScope, TokenScope,
    UpdateCollection, UpdateGroup, UpdateHubuumClass, UpdateHubuumObject,
};
use crate::permissions::permission_to_storage;
use crate::storage::{
    StorageClass, StorageClassCreate, StorageClassRelation, StorageClassRelationCreate,
    StorageClassSchemaPolicy, StorageClassSelector, StorageClassUpdate, StorageClassWithCollection,
    StorageCollection, StorageCollectionCreate, StorageCollectionUpdate, StorageGroupCreate,
    StorageGroupUpdate, StorageIdentityGroup, StorageObject, StorageObjectCreate,
    StorageObjectDataPatch, StorageObjectRelation, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationEndpoint,
    StorageObjectRelationSelector, StorageObjectSelector, StorageObjectUpdate,
    StoragePreparedClassRelation, StoragePreparedObjectRelation, StoragePrincipal,
    StoragePrincipalGroup, StoragePrincipalSettings, StoragePrincipalSettingsMutation,
    StorageRecordMetadata, StorageResolvedClass, StorageResolvedClassRelation,
    StorageResolvedObject, StorageResolvedObjectRelation, StorageResourceScope, StorageVisibility,
};
use hubuum_domain::{ClassId, ClassRelationId, CollectionId, ObjectId, PrincipalId, ResourceId};

pub(super) fn visibility(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
) -> Result<StorageVisibility, ApiError> {
    let permissions = scope.and_then(TokenScope::permissions).map(|permissions| {
        permissions
            .iter()
            .copied()
            .map(permission_to_storage)
            .collect::<Vec<_>>()
    });
    let resources = scope.map(resource_scope).transpose()?.flatten();
    Ok(StorageVisibility::new(
        PrincipalId::new(principal_id)?,
        is_admin,
        permissions,
        resources,
    ))
}

fn resource_scope(scope: &TokenScope) -> Result<Option<StorageResourceScope>, ApiError> {
    let Some(resources) = scope.resources()? else {
        return Ok(None);
    };
    let mut collection_ids = Vec::new();
    let mut class_ids = Vec::new();
    let mut object_ids = Vec::new();
    for resource in resources {
        match resource {
            TokenResourceScope::Collection(id) => collection_ids.push(id),
            TokenResourceScope::Class(id) => class_ids.push(id),
            TokenResourceScope::Object(id) => object_ids.push(id),
        }
    }
    Ok(Some(StorageResourceScope::new(
        collection_ids,
        class_ids,
        object_ids,
    )))
}

pub(crate) fn collection_from_storage(row: StorageCollection) -> Result<Collection, ApiError> {
    let (id, name, description, created_at, updated_at, parent_collection_id, revision) =
        row.into_parts();
    Ok(Collection {
        id: id.id(),
        name,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        parent_collection_id: parent_collection_id.map(CollectionId::id),
        revision,
    })
}

pub(crate) fn group_from_storage(row: StorageIdentityGroup) -> Result<Group, ApiError> {
    Ok(Group {
        id: row.id().id(),
        groupname: row.name().to_string(),
        description: row.description().to_string(),
        created_at: row.created_at().naive_utc(),
        updated_at: row.updated_at().naive_utc(),
        identity_scope_id: row.identity_scope_id().id(),
        managed_by: row.managed_by().to_string(),
        external_key: row.external_key().map(ToString::to_string),
        last_sync_attempted_at: row
            .last_sync_attempted_at()
            .map(|timestamp| timestamp.naive_utc()),
        last_sync_success_at: row
            .last_sync_success_at()
            .map(|timestamp| timestamp.naive_utc()),
        revision: row.revision(),
    })
}

pub(crate) fn group_create_to_storage(command: &NewGroup) -> StorageGroupCreate {
    StorageGroupCreate::new(
        command.identity_scope.clone(),
        command.groupname.clone(),
        command.description.clone(),
    )
}

pub(crate) fn group_update_to_storage(update: &UpdateGroup) -> StorageGroupUpdate {
    StorageGroupUpdate::new(update.groupname.clone())
}

pub(crate) fn principal_from_storage(row: StoragePrincipal) -> Result<Principal, ApiError> {
    Ok(Principal {
        id: row.id().id(),
        kind: row.kind(),
        name: row.name().to_owned(),
        created_at: row.created_at().naive_utc(),
        updated_at: row.updated_at().naive_utc(),
        identity_scope_id: row.identity_scope_id().id(),
        provider_managed: row.provider_managed(),
        settings: row.settings().clone(),
        external_subject: row.external_subject().map(ToOwned::to_owned),
        last_sync_attempted_at: row
            .last_sync_attempted_at()
            .map(|timestamp| timestamp.naive_utc()),
        last_sync_success_at: row
            .last_sync_success_at()
            .map(|timestamp| timestamp.naive_utc()),
        revision: row.revision(),
    })
}

pub(crate) fn principal_group_from_storage(
    row: StoragePrincipalGroup,
) -> Result<PrincipalGroup, ApiError> {
    Ok(PrincipalGroup {
        principal_id: row.principal_id().id(),
        group_id: row.group_id().id(),
        created_at: row.created_at().naive_utc(),
        updated_at: row.updated_at().naive_utc(),
        revision: row.revision(),
    })
}

pub(crate) fn principal_settings_from_storage(
    row: StoragePrincipalSettings,
) -> Result<PrincipalSettingsResponse, ApiError> {
    let (principal_id, revision, document) = row.into_parts();
    Ok(PrincipalSettingsResponse::new(
        principal_id.id(),
        revision,
        PrincipalSettings::new(document)?,
    ))
}

pub(crate) fn principal_settings_mutation_to_storage(
    mutation: PrincipalSettingsPatch,
) -> Result<StoragePrincipalSettingsMutation, ApiError> {
    match mutation {
        PrincipalSettingsPatch::MergePatch(settings) => Ok(
            StoragePrincipalSettingsMutation::MergePatch(settings.as_value().clone()),
        ),
        PrincipalSettingsPatch::JsonPatch(document) => Ok(
            StoragePrincipalSettingsMutation::JsonPatch(serde_json::to_value(document)?),
        ),
    }
}

#[cfg(test)]
pub(crate) fn collection_to_storage(collection: Collection) -> StorageCollection {
    StorageCollection::try_new(
        StorageRecordMetadata::try_new(
            ResourceId::new(collection.id).expect("stored collection id must be positive"),
            collection.created_at.and_utc(),
            collection.updated_at.and_utc(),
            collection.revision,
        )
        .expect("stored collection timestamps must be ordered"),
        collection.name,
        collection.description,
        collection
            .parent_collection_id
            .map(|id| CollectionId::new(id).expect("stored parent collection id must be positive")),
    )
    .expect("stored collection hierarchy must satisfy the storage contract")
}

pub(crate) fn collection_create_to_storage(
    command: NewCollectionWithAssignee,
) -> StorageCollectionCreate {
    StorageCollectionCreate::new(
        command.name,
        command.description,
        command.group_id,
        command.parent_collection_id,
    )
}

pub(crate) fn collection_update_to_storage(update: UpdateCollection) -> StorageCollectionUpdate {
    StorageCollectionUpdate::new(update.name, update.description)
}

pub(super) fn class_from_storage(
    row: StorageClassWithCollection,
) -> Result<HubuumClassExpanded, ApiError> {
    let (
        id,
        name,
        collection,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
    Ok(HubuumClassExpanded {
        id: id.id(),
        name,
        collection: collection_from_storage(collection)?,
        json_schema,
        validate_schema,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        revision,
    })
}

pub(crate) fn class_record_from_storage(row: StorageClass) -> Result<HubuumClass, ApiError> {
    let (
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
    Ok(HubuumClass {
        id: id.id(),
        name,
        collection_id: collection_id.id(),
        json_schema,
        validate_schema,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        revision,
    })
}

#[cfg(test)]
pub(crate) fn class_record_to_storage(class: HubuumClass) -> Result<StorageClass, ApiError> {
    let schema_policy =
        StorageClassSchemaPolicy::try_from_parts(class.json_schema, class.validate_schema)
            .map_err(|error| ApiError::from(error.into_request_error()))?;
    Ok(StorageClass::builder(
        StorageRecordMetadata::try_new(
            ResourceId::new(class.id)?,
            class.created_at.and_utc(),
            class.updated_at.and_utc(),
            class.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        class.name,
        CollectionId::new(class.collection_id)?,
        class.description,
    )
    .schema_policy(schema_policy)
    .build())
}

pub(crate) fn class_selector_to_storage(selector: ClassSelector) -> StorageClassSelector {
    match selector.kind() {
        ClassSelectorKind::ById(id) => StorageClassSelector::Id(*id),
        ClassSelectorKind::ByName(name) => StorageClassSelector::Name(name.clone()),
    }
}

pub(crate) fn class_selector_from_storage(
    selector: StorageClassSelector,
) -> Result<ClassSelector, ApiError> {
    Ok(match selector {
        StorageClassSelector::Id(id) => ClassSelector::by_id(id),
        StorageClassSelector::Name(name) => ClassSelector::by_name(name),
    })
}

pub(crate) fn resolved_class_to_storage(target: &ResolvedClassTarget) -> &StorageResolvedClass {
    target.as_storage()
}

pub(crate) fn resolved_class_from_storage(
    target: StorageResolvedClass,
) -> Result<ResolvedClassTarget, ApiError> {
    ResolvedClassTarget::from_storage(target)
}

pub(crate) fn class_create_to_storage(
    command: NewHubuumClass,
) -> Result<StorageClassCreate, ApiError> {
    let schema_policy = StorageClassSchemaPolicy::try_from_parts(
        command.json_schema,
        command.validate_schema.unwrap_or(false),
    )
    .map_err(|error| ApiError::from(error.into_request_error()))?;
    Ok(StorageClassCreate::builder(
        command.name,
        CollectionId::new(command.collection_id)?,
        command.description,
    )
    .schema_policy(schema_policy)
    .build())
}

pub(crate) fn class_update_to_storage(
    update: UpdateHubuumClass,
) -> Result<StorageClassUpdate, ApiError> {
    Ok(StorageClassUpdate::builder()
        .name(update.name)
        .collection_id(update.collection_id.map(CollectionId::new).transpose()?)
        .json_schema(update.json_schema)
        .validate_schema(update.validate_schema)
        .description(update.description)
        .build())
}

pub(crate) fn object_from_storage(row: StorageObject) -> Result<HubuumObject, ApiError> {
    let (
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
    Ok(HubuumObject {
        id: id.id(),
        name,
        collection_id: collection_id.id(),
        hubuum_class_id: hubuum_class_id.id(),
        data,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        revision,
    })
}

pub(crate) fn object_to_storage(object: HubuumObject) -> Result<StorageObject, ApiError> {
    Ok(StorageObject::new(
        StorageRecordMetadata::try_new(
            ResourceId::new(object.id)?,
            object.created_at.and_utc(),
            object.updated_at.and_utc(),
            object.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        object.name,
        CollectionId::new(object.collection_id)?,
        ClassId::new(object.hubuum_class_id)?,
        object.data,
        object.description,
    ))
}

pub(crate) fn object_selector_to_storage(selector: ObjectSelector) -> StorageObjectSelector {
    match selector.kind() {
        ObjectSelectorKind::ById {
            class_id,
            object_id,
        } => StorageObjectSelector::Ids {
            class_id: *class_id,
            object_id: *object_id,
        },
        ObjectSelectorKind::ByName {
            class_name,
            object_name,
        } => StorageObjectSelector::Names {
            class_name: class_name.clone(),
            object_name: object_name.clone(),
        },
    }
}

pub(crate) fn object_selector_from_storage(
    selector: StorageObjectSelector,
) -> Result<ObjectSelector, ApiError> {
    Ok(match selector {
        StorageObjectSelector::Ids {
            class_id,
            object_id,
        } => ObjectSelector::by_id(class_id, object_id),
        StorageObjectSelector::Names {
            class_name,
            object_name,
        } => ObjectSelector::by_name(class_name, object_name),
    })
}

pub(crate) fn resolved_object_to_storage(target: &ResolvedObjectTarget) -> &StorageResolvedObject {
    target.as_storage()
}

pub(crate) fn resolved_object_from_storage(
    target: StorageResolvedObject,
) -> Result<ResolvedObjectTarget, ApiError> {
    ResolvedObjectTarget::from_storage(target)
}

pub(crate) fn object_create_to_storage(
    command: NewHubuumObject,
) -> Result<StorageObjectCreate, ApiError> {
    Ok(StorageObjectCreate::new(
        command.name,
        CollectionId::new(command.collection_id)?,
        ClassId::new(command.hubuum_class_id)?,
        command.data,
        command.description,
    ))
}

pub(crate) fn object_update_to_storage(
    update: UpdateHubuumObject,
) -> Result<StorageObjectUpdate, ApiError> {
    Ok(StorageObjectUpdate::builder()
        .name(update.name)
        .collection_id(update.collection_id.map(CollectionId::new).transpose()?)
        .class_id(update.hubuum_class_id.map(ClassId::new).transpose()?)
        .data(update.data)
        .description(update.description)
        .build())
}

pub(crate) fn object_patch_to_storage(
    patch: ObjectDataPatchDocument,
) -> Result<StorageObjectDataPatch, ApiError> {
    Ok(StorageObjectDataPatch::new(patch.into_bounded_patch()))
}

#[cfg(test)]
pub(crate) fn class_relation_to_storage(
    relation: HubuumClassRelation,
) -> Result<StorageClassRelation, ApiError> {
    StorageClassRelation::try_new(
        StorageRecordMetadata::try_new(
            ResourceId::new(relation.id)?,
            relation.created_at.and_utc(),
            relation.updated_at.and_utc(),
            relation.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        ClassId::new(relation.from_hubuum_class_id)?,
        ClassId::new(relation.to_hubuum_class_id)?,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?
    .try_with_template_aliases(
        relation.forward_template_alias,
        relation.reverse_template_alias,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?
    .try_with_relation_limits(
        relation.from_max_relations.map(ObjectRelationLimit::value),
        relation.to_max_relations.map(ObjectRelationLimit::value),
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))
}

pub(crate) fn class_relation_from_storage(
    relation: StorageClassRelation,
) -> Result<HubuumClassRelation, ApiError> {
    let (
        id,
        from_hubuum_class_id,
        to_hubuum_class_id,
        forward_template_alias,
        reverse_template_alias,
        created_at,
        updated_at,
        from_max_relations,
        to_max_relations,
        revision,
    ) = relation.into_parts();
    Ok(HubuumClassRelation {
        id: id.id(),
        from_hubuum_class_id: from_hubuum_class_id.id(),
        to_hubuum_class_id: to_hubuum_class_id.id(),
        forward_template_alias,
        reverse_template_alias,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        from_max_relations: from_max_relations
            .map(ObjectRelationLimit::new)
            .transpose()?,
        to_max_relations: to_max_relations.map(ObjectRelationLimit::new).transpose()?,
        revision,
    })
}

pub(crate) fn class_relation_create_to_storage(
    command: NewHubuumClassRelation,
) -> Result<StorageClassRelationCreate, ApiError> {
    Ok(StorageClassRelationCreate::builder(
        ClassId::new(command.from_hubuum_class_id)?,
        ClassId::new(command.to_hubuum_class_id)?,
    )
    .template_aliases(
        command.forward_template_alias,
        command.reverse_template_alias,
    )
    .relation_limits(
        command.from_max_relations.map(ObjectRelationLimit::value),
        command.to_max_relations.map(ObjectRelationLimit::value),
    )
    .build())
}

pub(crate) fn class_relation_create_from_storage(
    command: &StorageClassRelationCreate,
) -> Result<NewHubuumClassRelation, ApiError> {
    Ok(NewHubuumClassRelation {
        from_hubuum_class_id: command.from_class_id().id(),
        to_hubuum_class_id: command.to_class_id().id(),
        forward_template_alias: command.forward_template_alias().map(str::to_string),
        reverse_template_alias: command.reverse_template_alias().map(str::to_string),
        from_max_relations: command
            .from_max_relations()
            .map(ObjectRelationLimit::new)
            .transpose()?,
        to_max_relations: command
            .to_max_relations()
            .map(ObjectRelationLimit::new)
            .transpose()?,
    })
}

pub(crate) fn prepared_class_relation_to_storage(
    target: &PreparedClassRelation,
) -> &StoragePreparedClassRelation {
    target.as_storage()
}

pub(crate) fn prepared_class_relation_from_storage(
    target: StoragePreparedClassRelation,
) -> Result<PreparedClassRelation, ApiError> {
    PreparedClassRelation::from_storage(target)
}

pub(crate) fn resolved_class_relation_to_storage(
    target: &ResolvedClassRelationTarget,
) -> &StorageResolvedClassRelation {
    target.as_storage()
}

pub(crate) fn resolved_class_relation_from_storage(
    target: StorageResolvedClassRelation,
) -> Result<ResolvedClassRelationTarget, ApiError> {
    ResolvedClassRelationTarget::from_storage(target)
}

#[cfg(test)]
pub(crate) fn object_relation_to_storage(
    relation: HubuumObjectRelation,
) -> Result<StorageObjectRelation, ApiError> {
    StorageObjectRelation::try_new(
        StorageRecordMetadata::try_new(
            ResourceId::new(relation.id)?,
            relation.created_at.and_utc(),
            relation.updated_at.and_utc(),
            relation.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        ObjectId::new(relation.from_hubuum_object_id)?,
        ObjectId::new(relation.to_hubuum_object_id)?,
        ClassRelationId::new(relation.class_relation_id)?,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))
}

pub(crate) fn object_relation_from_storage(
    relation: StorageObjectRelation,
) -> Result<HubuumObjectRelation, ApiError> {
    let (
        id,
        from_hubuum_object_id,
        to_hubuum_object_id,
        class_relation_id,
        created_at,
        updated_at,
        revision,
    ) = relation.into_parts();
    Ok(HubuumObjectRelation {
        id: id.id(),
        from_hubuum_object_id: from_hubuum_object_id.id(),
        to_hubuum_object_id: to_hubuum_object_id.id(),
        class_relation_id: class_relation_id.id(),
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        revision,
    })
}

fn relation_endpoint_to_storage(endpoint: ObjectRelationEndpoint) -> StorageObjectRelationEndpoint {
    StorageObjectRelationEndpoint::new(endpoint.class_id(), endpoint.object_id())
}

#[cfg(test)]
fn relation_endpoint_from_storage(
    endpoint: StorageObjectRelationEndpoint,
) -> Result<ObjectRelationEndpoint, ApiError> {
    Ok(ObjectRelationEndpoint::new(
        endpoint.class_id(),
        endpoint.object_id(),
    ))
}

pub(crate) fn object_relation_create_to_storage(
    command: NewHubuumObjectRelation,
) -> Result<StorageObjectRelationCreate, ApiError> {
    Ok(StorageObjectRelationCreate::new(
        ObjectId::new(command.from_hubuum_object_id)?,
        ObjectId::new(command.to_hubuum_object_id)?,
        ClassRelationId::new(command.class_relation_id)?,
    ))
}

pub(crate) fn object_relation_create_from_storage(
    command: StorageObjectRelationCreate,
) -> NewHubuumObjectRelation {
    NewHubuumObjectRelation {
        from_hubuum_object_id: command.from_object_id().id(),
        to_hubuum_object_id: command.to_object_id().id(),
        class_relation_id: command.class_relation_id().id(),
    }
}

pub(crate) fn object_relation_create_selector_to_storage(
    selector: ObjectRelationCreateSelector,
) -> Result<StorageObjectRelationCreateSelector, ApiError> {
    Ok(match selector.kind() {
        ObjectRelationCreateSelectorKind::Explicit(command) => {
            StorageObjectRelationCreateSelector::Explicit(object_relation_create_to_storage(
                command.clone(),
            )?)
        }
        ObjectRelationCreateSelectorKind::Between { from, to } => {
            StorageObjectRelationCreateSelector::Between {
                from: relation_endpoint_to_storage(*from),
                to: relation_endpoint_to_storage(*to),
            }
        }
    })
}

#[cfg(test)]
pub(crate) fn object_relation_create_selector_from_storage(
    selector: StorageObjectRelationCreateSelector,
) -> Result<ObjectRelationCreateSelector, ApiError> {
    Ok(match selector {
        StorageObjectRelationCreateSelector::Explicit(command) => {
            ObjectRelationCreateSelector::explicit(object_relation_create_from_storage(command))
        }
        StorageObjectRelationCreateSelector::Between { from, to } => {
            ObjectRelationCreateSelector::between(
                relation_endpoint_from_storage(from)?,
                relation_endpoint_from_storage(to)?,
            )
        }
    })
}

pub(crate) fn object_relation_selector_to_storage(
    selector: ObjectRelationSelector,
) -> StorageObjectRelationSelector {
    match selector.kind() {
        ObjectRelationSelectorKind::ById(id) => StorageObjectRelationSelector::Id(*id),
        ObjectRelationSelectorKind::Between { from, to } => {
            StorageObjectRelationSelector::Between {
                from: relation_endpoint_to_storage(*from),
                to: relation_endpoint_to_storage(*to),
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn object_relation_selector_from_storage(
    selector: StorageObjectRelationSelector,
) -> Result<ObjectRelationSelector, ApiError> {
    Ok(match selector {
        StorageObjectRelationSelector::Id(id) => ObjectRelationSelector::by_id(id),
        StorageObjectRelationSelector::Between { from, to } => ObjectRelationSelector::between(
            relation_endpoint_from_storage(from)?,
            relation_endpoint_from_storage(to)?,
        ),
    })
}

pub(crate) fn prepared_object_relation_to_storage(
    target: &PreparedObjectRelation,
) -> &StoragePreparedObjectRelation {
    target.as_storage()
}

pub(crate) fn prepared_object_relation_from_storage(
    target: StoragePreparedObjectRelation,
) -> Result<PreparedObjectRelation, ApiError> {
    PreparedObjectRelation::from_storage(target)
}

pub(crate) fn resolved_object_relation_to_storage(
    target: &ResolvedObjectRelationTarget,
) -> &StorageResolvedObjectRelation {
    target.as_storage()
}

pub(crate) fn resolved_object_relation_from_storage(
    target: StorageResolvedObjectRelation,
) -> Result<ResolvedObjectRelationTarget, ApiError> {
    ResolvedObjectRelationTarget::from_storage(target)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use hubuum_domain::{ResourceId, ResourceRevision};
    use rstest::rstest;

    use super::*;
    use crate::models::{CollectionID, HubuumClassID, Permissions};

    #[rstest]
    #[case(0, 1)]
    #[case(-1, 1)]
    #[case(1, 0)]
    #[case(1, -1)]
    fn object_creation_rejects_raw_invalid_ids(#[case] collection_id: i32, #[case] class_id: i32) {
        let result = object_create_to_storage(NewHubuumObject {
            name: "invalid_ids".into(),
            collection_id,
            hubuum_class_id: class_id,
            data: serde_json::json!({}),
            description: String::new(),
        });
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }

    #[rstest]
    #[case(Some(0), None)]
    #[case(Some(-1), None)]
    #[case(None, Some(0))]
    #[case(None, Some(-1))]
    fn object_updates_reject_raw_invalid_ids(
        #[case] collection_id: Option<i32>,
        #[case] class_id: Option<i32>,
    ) {
        let result = object_update_to_storage(UpdateHubuumObject {
            name: None,
            collection_id,
            hubuum_class_id: class_id,
            data: None,
            description: None,
        });
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }

    fn metadata(id: i32) -> StorageRecordMetadata {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        StorageRecordMetadata::try_new(
            ResourceId::new(id).unwrap(),
            timestamp,
            timestamp,
            ResourceRevision::INITIAL,
        )
        .unwrap()
    }

    fn stored_class(id: i32) -> StorageClass {
        StorageClass::builder(
            metadata(id),
            format!("class_{id}"),
            CollectionID::new(1).unwrap(),
            String::new(),
        )
        .build()
    }

    fn stored_object(id: i32) -> StorageObject {
        StorageObject::new(
            metadata(id),
            format!("object_{id}"),
            CollectionID::new(1).unwrap(),
            ClassId::new(id).unwrap(),
            serde_json::json!({"payload": "retained object data"}),
            String::new(),
        )
    }

    #[derive(Clone, Copy)]
    enum Aggregate {
        Class,
        Object,
        PreparedClassRelation,
        ClassRelation,
        PreparedObjectRelation,
        ObjectRelation,
    }

    #[rstest]
    #[case(Aggregate::Class)]
    #[case(Aggregate::Object)]
    #[case(Aggregate::PreparedClassRelation)]
    #[case(Aggregate::ClassRelation)]
    #[case(Aggregate::PreparedObjectRelation)]
    #[case(Aggregate::ObjectRelation)]
    fn application_wrappers_retain_the_original_aggregate(#[case] aggregate: Aggregate) {
        let from_class = stored_class(1);
        let to_class = stored_class(2);
        let original_name = from_class.name().as_ptr();
        let relation = || {
            StorageClassRelation::try_new(
                metadata(3),
                ClassId::new(1).unwrap(),
                ClassId::new(2).unwrap(),
            )
            .unwrap()
        };

        match aggregate {
            Aggregate::Class => {
                let stored = StorageResolvedClass::try_new(
                    StorageClassSelector::Name("class_1".into()),
                    from_class,
                )
                .unwrap();
                let target = resolved_class_from_storage(stored).unwrap();
                assert_eq!(
                    resolved_class_to_storage(&target).class().name().as_ptr(),
                    original_name
                );
            }
            Aggregate::Object => {
                let stored = StorageResolvedObject::try_new(
                    StorageObjectSelector::Names {
                        class_name: "class_1".into(),
                        object_name: "object_1".into(),
                    },
                    from_class,
                    stored_object(1),
                )
                .unwrap();
                let target = resolved_object_from_storage(stored).unwrap();
                assert_eq!(
                    resolved_object_to_storage(&target).class().name().as_ptr(),
                    original_name
                );
            }
            Aggregate::PreparedClassRelation => {
                let stored = StoragePreparedClassRelation::try_new(
                    StorageClassRelationCreate::builder(
                        ClassId::new(1).unwrap(),
                        ClassId::new(2).unwrap(),
                    )
                    .build(),
                    from_class,
                    to_class,
                )
                .unwrap();
                let target = prepared_class_relation_from_storage(stored).unwrap();
                assert_eq!(
                    prepared_class_relation_to_storage(&target)
                        .from_class()
                        .name()
                        .as_ptr(),
                    original_name
                );
            }
            Aggregate::ClassRelation => {
                let stored =
                    StorageResolvedClassRelation::try_new(relation(), from_class, to_class)
                        .unwrap();
                let target = resolved_class_relation_from_storage(stored).unwrap();
                assert_eq!(
                    resolved_class_relation_to_storage(&target)
                        .from_class()
                        .name()
                        .as_ptr(),
                    original_name
                );
            }
            Aggregate::PreparedObjectRelation => {
                let class_relation =
                    StorageResolvedClassRelation::try_new(relation(), from_class, to_class)
                        .unwrap();
                let stored = StoragePreparedObjectRelation::try_new(
                    StorageObjectRelationCreate::new(
                        ObjectId::new(1).unwrap(),
                        ObjectId::new(2).unwrap(),
                        ClassRelationId::new(3).unwrap(),
                    ),
                    stored_object(1),
                    stored_object(2),
                    class_relation,
                )
                .unwrap();
                let target = prepared_object_relation_from_storage(stored).unwrap();
                assert_eq!(
                    prepared_object_relation_to_storage(&target)
                        .class_relation()
                        .from_class()
                        .name()
                        .as_ptr(),
                    original_name
                );
            }
            Aggregate::ObjectRelation => {
                let class_relation =
                    StorageResolvedClassRelation::try_new(relation(), from_class, to_class)
                        .unwrap();
                let stored = StorageResolvedObjectRelation::try_new(
                    StorageObjectRelation::try_new(
                        metadata(4),
                        ObjectId::new(1).unwrap(),
                        ObjectId::new(2).unwrap(),
                        ClassRelationId::new(3).unwrap(),
                    )
                    .unwrap(),
                    stored_object(1),
                    stored_object(2),
                    class_relation,
                )
                .unwrap();
                let target = resolved_object_relation_from_storage(stored).unwrap();
                assert_eq!(
                    resolved_object_relation_to_storage(&target)
                        .class_relation()
                        .from_class()
                        .name()
                        .as_ptr(),
                    original_name
                );
            }
        }
    }

    #[test]
    fn visibility_preserves_independent_token_dimensions() {
        let scope = TokenScope::from_stored_parts(
            Some(vec![Permissions::ReadCollection, Permissions::ReadClass]),
            Some(vec![
                TokenResourceScope::Collection(CollectionID::new(7).unwrap()),
                TokenResourceScope::Class(HubuumClassID::new(9).unwrap()),
            ]),
        )
        .unwrap();

        let visibility = visibility(42, false, Some(&scope)).unwrap();

        assert!(visibility.allows_permissions(&[
            crate::storage::StorageAuthorizationPermission::ReadCollection,
            crate::storage::StorageAuthorizationPermission::ReadClass,
        ]));
        let resources = visibility.resources().unwrap();
        assert_eq!(resources.collection_ids()[0].id(), 7);
        assert_eq!(resources.class_ids()[0].id(), 9);
        assert!(resources.object_ids().is_empty());
    }
}
