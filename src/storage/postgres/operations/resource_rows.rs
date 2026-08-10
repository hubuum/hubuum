use crate::errors::ApiError;
use crate::models::{Collection, HubuumClassExpanded, HubuumObject};
use crate::storage::{StorageClass, StorageCollection, StorageObject};

pub(super) fn collection_to_storage(collection: Collection) -> StorageCollection {
    StorageCollection::new(
        collection.id,
        collection.name,
        collection.description,
        collection.created_at,
        collection.updated_at,
        collection.parent_collection_id,
        collection.revision.get(),
    )
}

pub(super) fn class_to_storage(class: HubuumClassExpanded) -> StorageClass {
    StorageClass::new(
        class.id,
        class.name,
        collection_to_storage(class.collection),
        class.json_schema,
        class.validate_schema,
        class.description,
        class.created_at,
        class.updated_at,
        class.revision.get(),
    )
}

pub(super) fn object_to_storage(object: HubuumObject) -> StorageObject {
    StorageObject::new(
        object.id,
        object.name,
        object.collection_id,
        object.hubuum_class_id,
        object.data,
        object.description,
        object.created_at,
        object.updated_at,
        object.revision.get(),
    )
}

pub(super) fn object_from_storage(object: StorageObject) -> Result<HubuumObject, ApiError> {
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
        revision: crate::models::ResourceRevision::new(revision)?,
    })
}
