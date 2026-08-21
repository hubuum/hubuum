use crate::errors::ApiError;
use crate::models::{
    ClassSelector, ClassSelectorKind, Collection, Group, HubuumClass, HubuumClassExpanded,
    HubuumClassID, HubuumClassRelation, HubuumObject, HubuumObjectID, HubuumObjectRelation,
    NewCollectionWithAssignee, NewGroup, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    NewHubuumObjectRelation, ObjectDataPatchDocument, ObjectRelationCreateSelector,
    ObjectRelationCreateSelectorKind, ObjectRelationEndpoint, ObjectRelationLimit,
    ObjectRelationSelector, ObjectRelationSelectorKind, ObjectSelector, ObjectSelectorKind,
    PreparedClassRelation, PreparedObjectRelation, Principal, PrincipalGroup, PrincipalSettings,
    PrincipalSettingsPatch, PrincipalSettingsResponse, ResolvedClassRelationTarget,
    ResolvedClassTarget, ResolvedObjectRelationTarget, ResolvedObjectTarget, TokenResourceScope,
    TokenScope, UpdateCollection, UpdateGroup, UpdateHubuumClass, UpdateHubuumObject,
};
use crate::permissions::permission_to_storage;
use crate::storage::{
    StorageClass, StorageClassCreate, StorageClassRecord, StorageClassRelation,
    StorageClassRelationCreate, StorageClassSelector, StorageClassUpdate, StorageCollection,
    StorageCollectionCreate, StorageCollectionUpdate, StorageGroupCreate, StorageGroupUpdate,
    StorageIdentityGroup, StorageObject, StorageObjectCreate, StorageObjectDataPatch,
    StorageObjectRelation, StorageObjectRelationCreate, StorageObjectRelationCreateSelector,
    StorageObjectRelationEndpoint, StorageObjectRelationSelector, StorageObjectSelector,
    StorageObjectUpdate, StoragePreparedClassRelation, StoragePreparedObjectRelation,
    StoragePrincipal, StoragePrincipalGroup, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation, StorageRecordMetadata, StorageResolvedClass,
    StorageResolvedClassRelation, StorageResolvedObject, StorageResolvedObjectRelation,
    StorageResourceScope, StorageVisibility,
};
use crate::traits::SelfAccessors;

pub(crate) fn collection_id_to_storage(id: i32) -> hubuum_domain::CollectionId {
    hubuum_domain::CollectionId::new(id).expect("validated collection id must be positive")
}

pub(crate) fn class_id_to_storage(id: i32) -> hubuum_domain::ClassId {
    hubuum_domain::ClassId::new(id).expect("validated class id must be positive")
}

pub(crate) fn object_id_to_storage(id: i32) -> hubuum_domain::ObjectId {
    hubuum_domain::ObjectId::new(id).expect("validated object id must be positive")
}

pub(crate) fn resource_id_to_storage(id: i32) -> hubuum_domain::ResourceId {
    hubuum_domain::ResourceId::new(id).expect("validated resource id must be positive")
}

pub(crate) fn principal_id_to_storage(id: i32) -> hubuum_domain::PrincipalId {
    hubuum_domain::PrincipalId::new(id).expect("validated principal id must be positive")
}

pub(crate) fn group_id_to_storage(id: i32) -> hubuum_domain::GroupId {
    hubuum_domain::GroupId::new(id).expect("validated group id must be positive")
}

pub(crate) fn class_relation_id_to_storage(id: i32) -> hubuum_domain::ClassRelationId {
    hubuum_domain::ClassRelationId::new(id).expect("validated class relation id must be positive")
}

pub(crate) fn object_relation_id_to_storage(id: i32) -> hubuum_domain::ObjectRelationId {
    hubuum_domain::ObjectRelationId::new(id).expect("validated object relation id must be positive")
}

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
        principal_id_to_storage(principal_id),
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
            TokenResourceScope::Collection(id) => {
                collection_ids.push(collection_id_to_storage(id.id()));
            }
            TokenResourceScope::Class(id) => class_ids.push(class_id_to_storage(id.id())),
            TokenResourceScope::Object(id) => object_ids.push(object_id_to_storage(id.id())),
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
        parent_collection_id: parent_collection_id.map(hubuum_domain::CollectionId::id),
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
        kind: row.kind().to_owned(),
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
    StorageCollection::new(
        StorageRecordMetadata::try_new(
            hubuum_domain::ResourceId::new(collection.id)
                .expect("stored collection id must be positive"),
            collection.created_at.and_utc(),
            collection.updated_at.and_utc(),
            collection.revision,
        )
        .expect("stored collection timestamps must be ordered"),
        collection.name,
        collection.description,
        collection.parent_collection_id.map(|id| {
            hubuum_domain::CollectionId::new(id)
                .expect("stored parent collection id must be positive")
        }),
    )
}

pub(crate) fn collection_create_to_storage(
    command: NewCollectionWithAssignee,
) -> StorageCollectionCreate {
    StorageCollectionCreate::new(
        command.name,
        command.description,
        hubuum_domain::GroupId::new(command.group_id.id())
            .expect("validated group id must be positive"),
        command.parent_collection_id.map(|id| {
            hubuum_domain::CollectionId::new(id.id())
                .expect("validated collection id must be positive")
        }),
    )
}

pub(crate) fn collection_update_to_storage(update: UpdateCollection) -> StorageCollectionUpdate {
    StorageCollectionUpdate::new(update.name, update.description)
}

pub(super) fn class_from_storage(row: StorageClass) -> Result<HubuumClassExpanded, ApiError> {
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

pub(crate) fn class_record_from_storage(row: StorageClassRecord) -> Result<HubuumClass, ApiError> {
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

pub(crate) fn class_record_to_storage(class: HubuumClass) -> StorageClassRecord {
    StorageClassRecord::builder(
        StorageRecordMetadata::try_new(
            hubuum_domain::ResourceId::new(class.id).expect("stored class id must be positive"),
            class.created_at.and_utc(),
            class.updated_at.and_utc(),
            class.revision,
        )
        .expect("stored class timestamps must be ordered"),
        class.name,
        hubuum_domain::CollectionId::new(class.collection_id)
            .expect("stored class collection id must be positive"),
        class.description,
    )
    .json_schema(class.json_schema)
    .validate_schema(class.validate_schema)
    .build()
}

pub(crate) fn class_selector_to_storage(selector: ClassSelector) -> StorageClassSelector {
    match selector.kind() {
        ClassSelectorKind::ById(id) => StorageClassSelector::Id(
            hubuum_domain::ClassId::new(id.id()).expect("validated class id must be positive"),
        ),
        ClassSelectorKind::ByName(name) => StorageClassSelector::Name(name.clone()),
    }
}

pub(super) fn class_selector_from_storage(
    selector: StorageClassSelector,
) -> Result<ClassSelector, ApiError> {
    Ok(match selector {
        StorageClassSelector::Id(id) => ClassSelector::by_id(HubuumClassID::new(id.id())?),
        StorageClassSelector::Name(name) => ClassSelector::by_name(name),
    })
}

pub(crate) fn resolved_class_to_storage(target: &ResolvedClassTarget) -> StorageResolvedClass {
    StorageResolvedClass::new(
        class_selector_to_storage(target.selector().clone()),
        class_record_to_storage(target.class().clone()),
    )
}

pub(crate) fn resolved_class_from_storage(
    target: StorageResolvedClass,
) -> Result<ResolvedClassTarget, ApiError> {
    let (selector, class) = target.into_parts();
    Ok(ResolvedClassTarget::new(
        class_selector_from_storage(selector)?,
        class_record_from_storage(class)?,
    ))
}

pub(crate) fn class_create_to_storage(command: NewHubuumClass) -> StorageClassCreate {
    StorageClassCreate::builder(
        command.name,
        hubuum_domain::CollectionId::new(command.collection_id)
            .expect("validated collection id must be positive"),
        command.description,
    )
    .json_schema(command.json_schema)
    .validate_schema(command.validate_schema.unwrap_or(false))
    .build()
}

pub(crate) fn class_update_to_storage(update: UpdateHubuumClass) -> StorageClassUpdate {
    StorageClassUpdate::builder()
        .name(update.name)
        .collection_id(update.collection_id.map(|id| {
            hubuum_domain::CollectionId::new(id).expect("validated collection id must be positive")
        }))
        .json_schema(update.json_schema)
        .validate_schema(update.validate_schema)
        .description(update.description)
        .build()
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

pub(crate) fn object_to_storage(object: HubuumObject) -> StorageObject {
    StorageObject::new(
        StorageRecordMetadata::try_new(
            hubuum_domain::ResourceId::new(object.id).expect("stored object id must be positive"),
            object.created_at.and_utc(),
            object.updated_at.and_utc(),
            object.revision,
        )
        .expect("stored object timestamps must be ordered"),
        object.name,
        hubuum_domain::CollectionId::new(object.collection_id)
            .expect("stored object collection id must be positive"),
        hubuum_domain::ClassId::new(object.hubuum_class_id)
            .expect("stored object class id must be positive"),
        object.data,
        object.description,
    )
}

pub(crate) fn object_selector_to_storage(selector: ObjectSelector) -> StorageObjectSelector {
    match selector.kind() {
        ObjectSelectorKind::ById {
            class_id,
            object_id,
        } => StorageObjectSelector::Ids {
            class_id: hubuum_domain::ClassId::new(class_id.id())
                .expect("validated class id must be positive"),
            object_id: hubuum_domain::ObjectId::new(object_id.id())
                .expect("validated object id must be positive"),
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

pub(super) fn object_selector_from_storage(
    selector: StorageObjectSelector,
) -> Result<ObjectSelector, ApiError> {
    Ok(match selector {
        StorageObjectSelector::Ids {
            class_id,
            object_id,
        } => ObjectSelector::by_id(
            HubuumClassID::new(class_id.id())?,
            HubuumObjectID::new(object_id.id())?,
        ),
        StorageObjectSelector::Names {
            class_name,
            object_name,
        } => ObjectSelector::by_name(class_name, object_name),
    })
}

pub(crate) fn resolved_object_to_storage(target: &ResolvedObjectTarget) -> StorageResolvedObject {
    StorageResolvedObject::new(
        object_selector_to_storage(target.selector().clone()),
        class_record_to_storage(target.class().clone()),
        object_to_storage(target.object().clone()),
    )
}

pub(crate) fn resolved_object_from_storage(
    target: StorageResolvedObject,
) -> Result<ResolvedObjectTarget, ApiError> {
    let (selector, class, object) = target.into_parts();
    Ok(ResolvedObjectTarget::new(
        object_selector_from_storage(selector)?,
        class_record_from_storage(class)?,
        object_from_storage(object)?,
    ))
}

pub(crate) fn object_create_to_storage(command: NewHubuumObject) -> StorageObjectCreate {
    StorageObjectCreate::new(
        command.name,
        hubuum_domain::CollectionId::new(command.collection_id)
            .expect("validated collection id must be positive"),
        hubuum_domain::ClassId::new(command.hubuum_class_id)
            .expect("validated class id must be positive"),
        command.data,
        command.description,
    )
}

pub(crate) fn object_update_to_storage(update: UpdateHubuumObject) -> StorageObjectUpdate {
    StorageObjectUpdate::builder()
        .name(update.name)
        .collection_id(update.collection_id.map(|id| {
            hubuum_domain::CollectionId::new(id).expect("validated collection id must be positive")
        }))
        .class_id(update.hubuum_class_id.map(|id| {
            hubuum_domain::ClassId::new(id).expect("validated class id must be positive")
        }))
        .data(update.data)
        .description(update.description)
        .build()
}

pub(crate) fn object_patch_to_storage(
    patch: ObjectDataPatchDocument,
) -> Result<StorageObjectDataPatch, ApiError> {
    Ok(StorageObjectDataPatch::new(patch.into_bounded_patch()))
}

pub(crate) fn class_relation_to_storage(relation: HubuumClassRelation) -> StorageClassRelation {
    StorageClassRelation::new(
        StorageRecordMetadata::try_new(
            hubuum_domain::ResourceId::new(relation.id)
                .expect("stored class relation id must be positive"),
            relation.created_at.and_utc(),
            relation.updated_at.and_utc(),
            relation.revision,
        )
        .expect("stored class relation timestamps must be ordered"),
        class_id_to_storage(relation.from_hubuum_class_id),
        class_id_to_storage(relation.to_hubuum_class_id),
    )
    .with_template_aliases(
        relation.forward_template_alias,
        relation.reverse_template_alias,
    )
    .with_relation_limits(
        relation.from_max_relations.map(ObjectRelationLimit::value),
        relation.to_max_relations.map(ObjectRelationLimit::value),
    )
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
) -> StorageClassRelationCreate {
    StorageClassRelationCreate::builder(
        class_id_to_storage(command.from_hubuum_class_id),
        class_id_to_storage(command.to_hubuum_class_id),
    )
    .template_aliases(
        command.forward_template_alias,
        command.reverse_template_alias,
    )
    .relation_limits(
        command.from_max_relations.map(ObjectRelationLimit::value),
        command.to_max_relations.map(ObjectRelationLimit::value),
    )
    .build()
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
    prepared: &PreparedClassRelation,
) -> StoragePreparedClassRelation {
    StoragePreparedClassRelation::new(
        class_relation_create_to_storage(prepared.command().clone()),
        class_record_to_storage(prepared.from_class().clone()),
        class_record_to_storage(prepared.to_class().clone()),
    )
}

pub(crate) fn prepared_class_relation_from_storage(
    prepared: StoragePreparedClassRelation,
) -> Result<PreparedClassRelation, ApiError> {
    let (command, from_class, to_class) = prepared.into_parts();
    PreparedClassRelation::new(
        class_relation_create_from_storage(&command)?,
        class_record_from_storage(from_class)?,
        class_record_from_storage(to_class)?,
    )
}

pub(crate) fn resolved_class_relation_to_storage(
    target: &ResolvedClassRelationTarget,
) -> StorageResolvedClassRelation {
    StorageResolvedClassRelation::new(
        class_relation_to_storage(target.relation().clone()),
        class_record_to_storage(target.from_class().clone()),
        class_record_to_storage(target.to_class().clone()),
    )
}

pub(crate) fn resolved_class_relation_from_storage(
    target: StorageResolvedClassRelation,
) -> Result<ResolvedClassRelationTarget, ApiError> {
    let (relation, from_class, to_class) = target.into_parts();
    ResolvedClassRelationTarget::new(
        class_relation_from_storage(relation)?,
        class_record_from_storage(from_class)?,
        class_record_from_storage(to_class)?,
    )
}

pub(crate) fn object_relation_to_storage(relation: HubuumObjectRelation) -> StorageObjectRelation {
    StorageObjectRelation::new(
        StorageRecordMetadata::try_new(
            hubuum_domain::ResourceId::new(relation.id)
                .expect("stored object relation id must be positive"),
            relation.created_at.and_utc(),
            relation.updated_at.and_utc(),
            relation.revision,
        )
        .expect("stored object relation timestamps must be ordered"),
        object_id_to_storage(relation.from_hubuum_object_id),
        object_id_to_storage(relation.to_hubuum_object_id),
        class_relation_id_to_storage(relation.class_relation_id),
    )
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
    StorageObjectRelationEndpoint::new(
        class_id_to_storage(endpoint.class_id().id()),
        object_id_to_storage(endpoint.object_id().id()),
    )
}

#[cfg(test)]
fn relation_endpoint_from_storage(
    endpoint: StorageObjectRelationEndpoint,
) -> Result<ObjectRelationEndpoint, ApiError> {
    Ok(ObjectRelationEndpoint::new(
        HubuumClassID::new(endpoint.class_id().id())?,
        HubuumObjectID::new(endpoint.object_id().id())?,
    ))
}

pub(crate) fn object_relation_create_to_storage(
    command: NewHubuumObjectRelation,
) -> StorageObjectRelationCreate {
    StorageObjectRelationCreate::new(
        object_id_to_storage(command.from_hubuum_object_id),
        object_id_to_storage(command.to_hubuum_object_id),
        class_relation_id_to_storage(command.class_relation_id),
    )
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
) -> StorageObjectRelationCreateSelector {
    match selector.kind() {
        ObjectRelationCreateSelectorKind::Explicit(command) => {
            StorageObjectRelationCreateSelector::Explicit(object_relation_create_to_storage(
                command.clone(),
            ))
        }
        ObjectRelationCreateSelectorKind::Between { from, to } => {
            StorageObjectRelationCreateSelector::Between {
                from: relation_endpoint_to_storage(*from),
                to: relation_endpoint_to_storage(*to),
            }
        }
    }
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
        ObjectRelationSelectorKind::ById(id) => {
            StorageObjectRelationSelector::Id(object_relation_id_to_storage(id.id()))
        }
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
        StorageObjectRelationSelector::Id(id) => {
            ObjectRelationSelector::by_id(crate::models::HubuumObjectRelationID::new(id.id())?)
        }
        StorageObjectRelationSelector::Between { from, to } => ObjectRelationSelector::between(
            relation_endpoint_from_storage(from)?,
            relation_endpoint_from_storage(to)?,
        ),
    })
}

pub(crate) fn prepared_object_relation_to_storage(
    prepared: &PreparedObjectRelation,
) -> StoragePreparedObjectRelation {
    StoragePreparedObjectRelation::new(
        object_relation_create_to_storage(prepared.command().clone()),
        object_to_storage(prepared.from_object().clone()),
        object_to_storage(prepared.to_object().clone()),
        resolved_class_relation_to_storage(prepared.class_relation()),
    )
}

pub(crate) fn prepared_object_relation_from_storage(
    prepared: StoragePreparedObjectRelation,
) -> Result<PreparedObjectRelation, ApiError> {
    let (command, from_object, to_object, class_relation) = prepared.into_parts();
    PreparedObjectRelation::new(
        object_relation_create_from_storage(command),
        object_from_storage(from_object)?,
        object_from_storage(to_object)?,
        resolved_class_relation_from_storage(class_relation)?,
    )
}

pub(crate) fn resolved_object_relation_to_storage(
    target: &ResolvedObjectRelationTarget,
) -> StorageResolvedObjectRelation {
    StorageResolvedObjectRelation::new(
        object_relation_to_storage(*target.relation()),
        object_to_storage(target.from_object().clone()),
        object_to_storage(target.to_object().clone()),
        resolved_class_relation_to_storage(target.class_relation()),
    )
}

pub(crate) fn resolved_object_relation_from_storage(
    target: StorageResolvedObjectRelation,
) -> Result<ResolvedObjectRelationTarget, ApiError> {
    let (relation, from_object, to_object, class_relation) = target.into_parts();
    ResolvedObjectRelationTarget::new(
        object_relation_from_storage(relation)?,
        object_from_storage(from_object)?,
        object_from_storage(to_object)?,
        resolved_class_relation_from_storage(class_relation)?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CollectionID, HubuumClassID, Permissions};

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
            crate::storage::AuthorizationPermission::ReadCollection,
            crate::storage::AuthorizationPermission::ReadClass,
        ]));
        let resources = visibility.resources().unwrap();
        assert_eq!(resources.collection_ids()[0].id(), 7);
        assert_eq!(resources.class_ids()[0].id(), 9);
        assert!(resources.object_ids().is_empty());
    }
}
