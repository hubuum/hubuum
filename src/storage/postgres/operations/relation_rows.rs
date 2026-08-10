use crate::models::{
    ClassGraphRow, HubuumClassRelation, HubuumObjectRelation, RelatedObjectForRootRow,
    RelatedObjectGraphRow, RelatedObjectIncludeRow,
};
use crate::storage::{
    StorageClassGraphRow, StorageClassRelation, StorageGraphClass, StorageGraphObject,
    StorageGraphResource, StorageObjectGraphRow, StorageObjectRelation, StorageRecordMetadata,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
};

fn metadata(
    id: i32,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: i64,
) -> StorageRecordMetadata {
    StorageRecordMetadata::new(id, created_at, updated_at, revision)
}

pub(super) fn class_relation_to_storage(row: HubuumClassRelation) -> StorageClassRelation {
    StorageClassRelation::new(
        metadata(row.id, row.created_at, row.updated_at, row.revision.get()),
        row.from_hubuum_class_id,
        row.to_hubuum_class_id,
    )
    .with_template_aliases(row.forward_template_alias, row.reverse_template_alias)
    .with_relation_limits(
        row.from_max_relations.map(|limit| limit.value()),
        row.to_max_relations.map(|limit| limit.value()),
    )
}

pub(super) fn object_relation_to_storage(row: HubuumObjectRelation) -> StorageObjectRelation {
    StorageObjectRelation::new(
        metadata(row.id, row.created_at, row.updated_at, row.revision.get()),
        row.from_hubuum_object_id,
        row.to_hubuum_object_id,
        row.class_relation_id,
    )
}

fn graph_resource(
    id: i32,
    name: String,
    collection_id: i32,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: i64,
) -> StorageGraphResource {
    StorageGraphResource::new(
        metadata(id, created_at, updated_at, revision),
        name,
        collection_id,
        description,
    )
}

pub(super) fn class_graph_to_storage(row: ClassGraphRow) -> StorageClassGraphRow {
    let ancestor = StorageGraphClass::new(
        graph_resource(
            row.ancestor_class_id,
            row.ancestor_name,
            row.ancestor_collection_id,
            row.ancestor_description,
            row.ancestor_created_at,
            row.ancestor_updated_at,
            row.ancestor_revision.get(),
        ),
        row.ancestor_json_schema,
        row.ancestor_validate_schema,
    );
    let descendant = StorageGraphClass::new(
        graph_resource(
            row.descendant_class_id,
            row.descendant_name,
            row.descendant_collection_id,
            row.descendant_description,
            row.descendant_created_at,
            row.descendant_updated_at,
            row.descendant_revision.get(),
        ),
        row.descendant_json_schema,
        row.descendant_validate_schema,
    );
    StorageClassGraphRow::new(ancestor, descendant, row.depth, row.path)
}

fn graph_object(
    resource: StorageGraphResource,
    class_id: i32,
    data: serde_json::Value,
) -> StorageGraphObject {
    StorageGraphObject::new(resource, class_id, data)
}

pub(super) fn object_graph_to_storage(row: RelatedObjectGraphRow) -> StorageObjectGraphRow {
    let ancestor = graph_object(
        graph_resource(
            row.ancestor_object_id,
            row.ancestor_name,
            row.ancestor_collection_id,
            row.ancestor_description,
            row.ancestor_created_at,
            row.ancestor_updated_at,
            row.ancestor_revision.get(),
        ),
        row.ancestor_class_id,
        row.ancestor_data,
    );
    let descendant = graph_object(
        graph_resource(
            row.descendant_object_id,
            row.descendant_name,
            row.descendant_collection_id,
            row.descendant_description,
            row.descendant_created_at,
            row.descendant_updated_at,
            row.descendant_revision.get(),
        ),
        row.descendant_class_id,
        row.descendant_data,
    );
    StorageObjectGraphRow::new(ancestor, descendant, row.depth, row.path)
}

pub(super) fn related_include_to_storage(
    row: RelatedObjectIncludeRow,
) -> StorageRelatedObjectIncludeRow {
    let root_object_id = row.root_object_id;
    StorageRelatedObjectIncludeRow::new(
        root_object_id,
        object_graph_to_storage(RelatedObjectGraphRow {
            ancestor_object_id: row.ancestor_object_id,
            descendant_object_id: row.descendant_object_id,
            depth: row.depth,
            path: row.path,
            ancestor_name: row.ancestor_name,
            descendant_name: row.descendant_name,
            ancestor_collection_id: row.ancestor_collection_id,
            descendant_collection_id: row.descendant_collection_id,
            ancestor_class_id: row.ancestor_class_id,
            descendant_class_id: row.descendant_class_id,
            ancestor_description: row.ancestor_description,
            descendant_description: row.descendant_description,
            ancestor_data: row.ancestor_data,
            descendant_data: row.descendant_data,
            ancestor_created_at: row.ancestor_created_at,
            descendant_created_at: row.descendant_created_at,
            ancestor_updated_at: row.ancestor_updated_at,
            descendant_updated_at: row.descendant_updated_at,
            ancestor_revision: row.ancestor_revision,
            descendant_revision: row.descendant_revision,
        }),
    )
}

pub(super) fn related_for_root_to_storage(
    row: RelatedObjectForRootRow,
) -> StorageRelatedObjectForRootRow {
    let descendant = graph_object(
        graph_resource(
            row.descendant_object_id,
            row.descendant_name,
            row.descendant_collection_id,
            row.descendant_description,
            row.descendant_created_at,
            row.descendant_updated_at,
            row.descendant_revision.get(),
        ),
        row.descendant_class_id,
        row.descendant_data,
    );
    StorageRelatedObjectForRootRow::new(row.root_object_id, descendant, row.depth, row.path)
}
