use crate::errors::ApiError;
use crate::models::{Collection, HubuumClass, HubuumClassExpanded, HubuumObject};
use crate::storage::{
    StorageClass, StorageClassRecord, StorageCollection, StorageObject, StorageRecordMetadata,
};
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
