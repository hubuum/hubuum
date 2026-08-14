use crate::errors::ApiError;
use crate::models::{
    ClassSelector, Collection, HubuumClass, HubuumClassExpanded, HubuumClassID, HubuumObject,
    HubuumObjectID, NewHubuumClass, NewHubuumObject, ObjectDataPatchDocument, ObjectSelector,
    ResolvedClassTarget, ResolvedObjectTarget, UpdateHubuumClass, UpdateHubuumObject,
};
use crate::storage::{
    StorageClass, StorageClassCreate, StorageClassRecord, StorageClassSelector, StorageClassUpdate,
    StorageCollection, StorageObject, StorageObjectCreate, StorageObjectDataPatch,
    StorageObjectSelector, StorageObjectUpdate, StorageRecordMetadata, StorageResolvedClass,
    StorageResolvedObject,
};
use crate::traits::SelfAccessors;
use hubuum_storage_postgres::PostgresRevision;

pub(in crate::storage::postgres) fn collection_to_storage(
    collection: Collection,
) -> StorageCollection {
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

pub(in crate::storage::postgres) fn class_to_storage(class: HubuumClassExpanded) -> StorageClass {
    StorageClass::builder(
        StorageRecordMetadata::new(
            class.id,
            class.created_at,
            class.updated_at,
            class.revision.get(),
        ),
        class.name,
        collection_to_storage(class.collection),
        class.description,
    )
    .json_schema(class.json_schema)
    .validate_schema(class.validate_schema)
    .build()
}

pub(in crate::storage::postgres) fn class_record_to_storage(
    class: HubuumClass,
) -> StorageClassRecord {
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

pub(in crate::storage::postgres) fn class_record_from_storage(
    class: StorageClassRecord,
) -> Result<HubuumClass, ApiError> {
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
    ) = class.into_parts();
    Ok(HubuumClass {
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision: PostgresRevision::new(revision)?.into_domain(),
    })
}

pub(in crate::storage::postgres) fn class_selector_from_storage(
    selector: StorageClassSelector,
) -> Result<ClassSelector, ApiError> {
    Ok(match selector {
        StorageClassSelector::Id(id) => ClassSelector::by_id(HubuumClassID::new(id)?),
        StorageClassSelector::Name(name) => ClassSelector::by_name(name),
    })
}

pub(in crate::storage::postgres) fn class_selector_to_storage(
    selector: &ClassSelector,
) -> StorageClassSelector {
    match selector.kind() {
        crate::models::ClassSelectorKind::ById(id) => StorageClassSelector::Id(id.id()),
        crate::models::ClassSelectorKind::ByName(name) => StorageClassSelector::Name(name.clone()),
    }
}

pub(in crate::storage::postgres) fn resolved_class_from_storage(
    target: &StorageResolvedClass,
) -> Result<ResolvedClassTarget, ApiError> {
    let (selector, class) = target.clone().into_parts();
    Ok(ResolvedClassTarget::new(
        class_selector_from_storage(selector)?,
        class_record_from_storage(class)?,
    ))
}

pub(in crate::storage::postgres) fn resolved_class_to_storage(
    target: ResolvedClassTarget,
) -> StorageResolvedClass {
    StorageResolvedClass::new(
        class_selector_to_storage(target.selector()),
        class_record_to_storage(target.class().clone()),
    )
}

pub(in crate::storage::postgres) fn class_create_from_storage(
    command: &StorageClassCreate,
) -> NewHubuumClass {
    NewHubuumClass {
        name: command.name().to_string(),
        collection_id: command.collection_id(),
        json_schema: command.json_schema().cloned(),
        validate_schema: Some(command.validates_schema()),
        description: command.description().to_string(),
    }
}

pub(in crate::storage::postgres) fn class_update_from_storage(
    update: &StorageClassUpdate,
) -> UpdateHubuumClass {
    UpdateHubuumClass {
        name: update.name().map(str::to_string),
        collection_id: update.collection_id(),
        json_schema: update.json_schema().cloned(),
        validate_schema: update.validate_schema(),
        description: update.description().map(str::to_string),
    }
}

pub(in crate::storage::postgres) fn object_to_storage(object: HubuumObject) -> StorageObject {
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

pub(in crate::storage::postgres) fn object_from_storage(
    object: StorageObject,
) -> Result<HubuumObject, ApiError> {
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
    ) = object.into_parts();
    Ok(HubuumObject {
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        revision: PostgresRevision::new(revision)?.into_domain(),
    })
}

pub(in crate::storage::postgres) fn object_selector_from_storage(
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

pub(in crate::storage::postgres) fn object_selector_to_storage(
    selector: &ObjectSelector,
) -> StorageObjectSelector {
    match selector.kind() {
        crate::models::ObjectSelectorKind::ById {
            class_id,
            object_id,
        } => StorageObjectSelector::Ids {
            class_id: class_id.id(),
            object_id: object_id.id(),
        },
        crate::models::ObjectSelectorKind::ByName {
            class_name,
            object_name,
        } => StorageObjectSelector::Names {
            class_name: class_name.clone(),
            object_name: object_name.clone(),
        },
    }
}

pub(in crate::storage::postgres) fn resolved_object_from_storage(
    target: &StorageResolvedObject,
) -> Result<ResolvedObjectTarget, ApiError> {
    let (selector, class, object) = target.clone().into_parts();
    Ok(ResolvedObjectTarget::new(
        object_selector_from_storage(selector)?,
        class_record_from_storage(class)?,
        object_from_storage(object)?,
    ))
}

pub(in crate::storage::postgres) fn resolved_object_to_storage(
    target: ResolvedObjectTarget,
) -> StorageResolvedObject {
    StorageResolvedObject::new(
        object_selector_to_storage(target.selector()),
        class_record_to_storage(target.class().clone()),
        object_to_storage(target.object().clone()),
    )
}

pub(in crate::storage::postgres) fn object_create_from_storage(
    command: &StorageObjectCreate,
) -> NewHubuumObject {
    NewHubuumObject {
        name: command.name().to_string(),
        collection_id: command.collection_id(),
        hubuum_class_id: command.class_id(),
        data: command.data().clone(),
        description: command.description().to_string(),
    }
}

pub(in crate::storage::postgres) fn object_update_from_storage(
    update: &StorageObjectUpdate,
) -> UpdateHubuumObject {
    UpdateHubuumObject {
        name: update.name().map(str::to_string),
        collection_id: update.collection_id(),
        hubuum_class_id: update.class_id(),
        data: update.data().cloned(),
        description: update.description().map(str::to_string),
    }
}

pub(in crate::storage::postgres) fn object_patch_from_storage(
    patch: &StorageObjectDataPatch,
) -> Result<ObjectDataPatchDocument, ApiError> {
    serde_json::from_value(patch.document().clone()).map_err(ApiError::from)
}
