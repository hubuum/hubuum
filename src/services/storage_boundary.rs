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
    ResolvedClassTarget, ResolvedObjectRelationTarget, ResolvedObjectTarget, ResourceRevision,
    TokenResourceScope, TokenScope, UpdateCollection, UpdateGroup, UpdateHubuumClass,
    UpdateHubuumObject,
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
        principal_id,
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
            TokenResourceScope::Collection(id) => collection_ids.push(id.id()),
            TokenResourceScope::Class(id) => class_ids.push(id.id()),
            TokenResourceScope::Object(id) => object_ids.push(id.id()),
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
        id,
        name,
        description,
        created_at,
        updated_at,
        parent_collection_id,
        revision: ResourceRevision::new(revision)?,
    })
}

pub(crate) fn group_from_storage(row: StorageIdentityGroup) -> Result<Group, ApiError> {
    Ok(Group {
        id: row.id(),
        groupname: row.name().to_string(),
        description: row.description().to_string(),
        created_at: row.created_at(),
        updated_at: row.updated_at(),
        identity_scope_id: row.identity_scope_id(),
        managed_by: row.managed_by().to_string(),
        external_key: row.external_key().map(ToString::to_string),
        last_sync_attempted_at: row.last_sync_attempted_at(),
        last_sync_success_at: row.last_sync_success_at(),
        revision: ResourceRevision::new(row.revision())?,
    })
}

pub(crate) fn group_to_storage(group: Group) -> StorageIdentityGroup {
    StorageIdentityGroup::builder(
        StorageRecordMetadata::new(
            group.id,
            group.created_at,
            group.updated_at,
            group.revision.get(),
        ),
        group.groupname,
        group.description,
        group.identity_scope_id,
        group.managed_by,
    )
    .external_key(group.external_key)
    .last_sync_attempted_at(group.last_sync_attempted_at)
    .last_sync_success_at(group.last_sync_success_at)
    .build()
}

pub(crate) fn group_create_to_storage(command: &NewGroup) -> StorageGroupCreate {
    StorageGroupCreate::new(
        command.identity_scope.clone(),
        command.groupname.clone(),
        command.description.clone(),
    )
}

pub(crate) fn group_create_from_storage(command: StorageGroupCreate) -> NewGroup {
    let (identity_scope, groupname, description) = command.into_parts();
    NewGroup {
        identity_scope,
        groupname,
        description,
    }
}

pub(crate) fn group_update_to_storage(update: &UpdateGroup) -> StorageGroupUpdate {
    StorageGroupUpdate::new(update.groupname.clone())
}

pub(crate) fn group_update_from_storage(update: StorageGroupUpdate) -> UpdateGroup {
    UpdateGroup {
        groupname: update.into_name(),
    }
}

pub(crate) fn principal_from_storage(row: StoragePrincipal) -> Result<Principal, ApiError> {
    let row = row.into_parts();
    Ok(Principal {
        id: row.id,
        kind: row.kind,
        name: row.name,
        created_at: row.created_at,
        updated_at: row.updated_at,
        identity_scope_id: row.identity_scope_id,
        provider_managed: row.provider_managed,
        settings: row.settings,
        external_subject: row.external_subject,
        last_sync_attempted_at: row.last_sync_attempted_at,
        last_sync_success_at: row.last_sync_success_at,
        revision: ResourceRevision::new(row.revision)?,
    })
}

pub(crate) fn principal_to_storage(principal: Principal) -> StoragePrincipal {
    StoragePrincipal::builder(
        StorageRecordMetadata::new(
            principal.id,
            principal.created_at,
            principal.updated_at,
            principal.revision.get(),
        ),
        principal.kind,
        principal.name,
        principal.identity_scope_id,
    )
    .provider_managed(principal.provider_managed)
    .settings(principal.settings)
    .external_subject(principal.external_subject)
    .last_sync_attempted_at(principal.last_sync_attempted_at)
    .last_sync_success_at(principal.last_sync_success_at)
    .build()
}

pub(crate) fn principal_group_from_storage(
    row: StoragePrincipalGroup,
) -> Result<PrincipalGroup, ApiError> {
    Ok(PrincipalGroup {
        principal_id: row.principal_id(),
        group_id: row.group_id(),
        created_at: row.created_at(),
        updated_at: row.updated_at(),
        revision: ResourceRevision::new(row.revision())?,
    })
}

pub(crate) fn principal_group_to_storage(row: PrincipalGroup) -> StoragePrincipalGroup {
    StoragePrincipalGroup::new(
        row.principal_id,
        row.group_id,
        row.created_at,
        row.updated_at,
        row.revision.get(),
    )
}

pub(crate) fn principal_settings_from_storage(
    row: StoragePrincipalSettings,
) -> Result<PrincipalSettingsResponse, ApiError> {
    let (principal_id, revision, document) = row.into_parts();
    Ok(PrincipalSettingsResponse::new(
        principal_id,
        ResourceRevision::new(revision)?,
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
        StorageRecordMetadata::new(
            collection.id,
            collection.created_at,
            collection.updated_at,
            collection.revision.get(),
        ),
        collection.name,
        collection.description,
        collection.parent_collection_id,
    )
}

pub(crate) fn collection_create_to_storage(
    command: NewCollectionWithAssignee,
) -> StorageCollectionCreate {
    StorageCollectionCreate::new(
        command.name,
        command.description,
        command.group_id.id(),
        command.parent_collection_id.map(|id| id.id()),
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
        id,
        name,
        collection: collection_from_storage(collection)?,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision: ResourceRevision::new(revision)?,
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
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision: ResourceRevision::new(revision)?,
    })
}

pub(crate) fn class_record_to_storage(class: HubuumClass) -> StorageClassRecord {
    StorageClassRecord::builder(
        StorageRecordMetadata::new(
            class.id,
            class.created_at,
            class.updated_at,
            class.revision.get(),
        ),
        class.name,
        class.collection_id,
        class.description,
    )
    .json_schema(class.json_schema)
    .validate_schema(class.validate_schema)
    .build()
}

pub(crate) fn class_selector_to_storage(selector: ClassSelector) -> StorageClassSelector {
    match selector.kind() {
        ClassSelectorKind::ById(id) => StorageClassSelector::Id(id.id()),
        ClassSelectorKind::ByName(name) => StorageClassSelector::Name(name.clone()),
    }
}

pub(super) fn class_selector_from_storage(
    selector: StorageClassSelector,
) -> Result<ClassSelector, ApiError> {
    Ok(match selector {
        StorageClassSelector::Id(id) => ClassSelector::by_id(HubuumClassID::new(id)?),
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
    StorageClassCreate::builder(command.name, command.collection_id, command.description)
        .json_schema(command.json_schema)
        .validate_schema(command.validate_schema.unwrap_or(false))
        .build()
}

pub(crate) fn class_update_to_storage(update: UpdateHubuumClass) -> StorageClassUpdate {
    StorageClassUpdate::builder()
        .name(update.name)
        .collection_id(update.collection_id)
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
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        revision: ResourceRevision::new(revision)?,
    })
}

pub(crate) fn object_to_storage(object: HubuumObject) -> StorageObject {
    StorageObject::new(
        StorageRecordMetadata::new(
            object.id,
            object.created_at,
            object.updated_at,
            object.revision.get(),
        ),
        object.name,
        object.collection_id,
        object.hubuum_class_id,
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
            class_id: class_id.id(),
            object_id: object_id.id(),
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
            HubuumClassID::new(class_id)?,
            HubuumObjectID::new(object_id)?,
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
        command.collection_id,
        command.hubuum_class_id,
        command.data,
        command.description,
    )
}

pub(crate) fn object_update_to_storage(update: UpdateHubuumObject) -> StorageObjectUpdate {
    StorageObjectUpdate::builder()
        .name(update.name)
        .collection_id(update.collection_id)
        .class_id(update.hubuum_class_id)
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
        StorageRecordMetadata::new(
            relation.id,
            relation.created_at,
            relation.updated_at,
            relation.revision.get(),
        ),
        relation.from_hubuum_class_id,
        relation.to_hubuum_class_id,
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
        id,
        from_hubuum_class_id,
        to_hubuum_class_id,
        forward_template_alias,
        reverse_template_alias,
        created_at,
        updated_at,
        from_max_relations: from_max_relations
            .map(ObjectRelationLimit::new)
            .transpose()?,
        to_max_relations: to_max_relations.map(ObjectRelationLimit::new).transpose()?,
        revision: ResourceRevision::new(revision)?,
    })
}

pub(crate) fn class_relation_create_to_storage(
    command: NewHubuumClassRelation,
) -> StorageClassRelationCreate {
    StorageClassRelationCreate::builder(command.from_hubuum_class_id, command.to_hubuum_class_id)
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
        from_hubuum_class_id: command.from_class_id(),
        to_hubuum_class_id: command.to_class_id(),
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
        StorageRecordMetadata::new(
            relation.id,
            relation.created_at,
            relation.updated_at,
            relation.revision.get(),
        ),
        relation.from_hubuum_object_id,
        relation.to_hubuum_object_id,
        relation.class_relation_id,
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
        id,
        from_hubuum_object_id,
        to_hubuum_object_id,
        class_relation_id,
        created_at,
        updated_at,
        revision: ResourceRevision::new(revision)?,
    })
}

fn relation_endpoint_to_storage(endpoint: ObjectRelationEndpoint) -> StorageObjectRelationEndpoint {
    StorageObjectRelationEndpoint::new(endpoint.class_id().id(), endpoint.object_id().id())
}

#[cfg(test)]
fn relation_endpoint_from_storage(
    endpoint: StorageObjectRelationEndpoint,
) -> Result<ObjectRelationEndpoint, ApiError> {
    Ok(ObjectRelationEndpoint::new(
        HubuumClassID::new(endpoint.class_id())?,
        HubuumObjectID::new(endpoint.object_id())?,
    ))
}

pub(crate) fn object_relation_create_to_storage(
    command: NewHubuumObjectRelation,
) -> StorageObjectRelationCreate {
    StorageObjectRelationCreate::new(
        command.from_hubuum_object_id,
        command.to_hubuum_object_id,
        command.class_relation_id,
    )
}

pub(crate) fn object_relation_create_from_storage(
    command: StorageObjectRelationCreate,
) -> NewHubuumObjectRelation {
    NewHubuumObjectRelation {
        from_hubuum_object_id: command.from_object_id(),
        to_hubuum_object_id: command.to_object_id(),
        class_relation_id: command.class_relation_id(),
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
        ObjectRelationSelectorKind::ById(id) => StorageObjectRelationSelector::Id(id.id()),
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
            ObjectRelationSelector::by_id(crate::models::HubuumObjectRelationID::new(id)?)
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
        assert_eq!(resources.collection_ids(), &[7]);
        assert_eq!(resources.class_ids(), &[9]);
        assert!(resources.object_ids().is_empty());
    }
}
