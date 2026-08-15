use crate::models::{Collection, HubuumClass, HubuumObject};
use crate::storage::{StorageClassRecord, StorageCollection, StorageObject, StorageRecordMetadata};

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
