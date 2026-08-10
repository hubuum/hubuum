use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    ClassGraphRow, ExportIncludeRelatedDirection, ExportIncludeRelatedQuery,
    ExportIncludeRelatedSort, HubuumClassRelation, HubuumObjectRelation, ObjectRelationLimit,
    RelatedObjectForRootRow, RelatedObjectGraphRow, RelatedObjectIncludeRow, ResourceRevision,
    TokenScope,
};
use crate::services::storage_boundary::visibility;
use crate::storage::{
    BidirectionalRelatedObjectsQuery, ObjectRelationsTouchingIdsQuery, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationQueryStorage,
    RelationTouchingQuery, StorageClassGraphRow, StorageClassRelation, StorageContext,
    StorageGraphClass, StorageGraphObject, StorageObjectGraphRow, StorageObjectRelation,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedSort, storage_handle,
};

fn class_relation_from_storage(row: StorageClassRelation) -> Result<HubuumClassRelation, ApiError> {
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
    ) = row.into_parts();
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

fn object_relation_from_storage(
    row: StorageObjectRelation,
) -> Result<HubuumObjectRelation, ApiError> {
    let (
        id,
        from_hubuum_object_id,
        to_hubuum_object_id,
        class_relation_id,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
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

#[allow(clippy::type_complexity)]
fn class_parts(
    row: StorageGraphClass,
) -> Result<
    (
        i32,
        String,
        i32,
        Option<serde_json::Value>,
        bool,
        String,
        chrono::NaiveDateTime,
        chrono::NaiveDateTime,
        ResourceRevision,
    ),
    ApiError,
> {
    let (id, name, collection_id, schema, validate, description, created, updated, revision) =
        row.into_parts();
    Ok((
        id,
        name,
        collection_id,
        schema,
        validate,
        description,
        created,
        updated,
        ResourceRevision::new(revision)?,
    ))
}

fn class_graph_from_storage(row: StorageClassGraphRow) -> Result<ClassGraphRow, ApiError> {
    let (ancestor, descendant, depth, path) = row.into_parts();
    let (
        ancestor_class_id,
        ancestor_name,
        ancestor_collection_id,
        ancestor_json_schema,
        ancestor_validate_schema,
        ancestor_description,
        ancestor_created_at,
        ancestor_updated_at,
        ancestor_revision,
    ) = class_parts(ancestor)?;
    let (
        descendant_class_id,
        descendant_name,
        descendant_collection_id,
        descendant_json_schema,
        descendant_validate_schema,
        descendant_description,
        descendant_created_at,
        descendant_updated_at,
        descendant_revision,
    ) = class_parts(descendant)?;
    Ok(ClassGraphRow {
        ancestor_class_id,
        descendant_class_id,
        depth,
        path,
        ancestor_name,
        descendant_name,
        ancestor_collection_id,
        descendant_collection_id,
        ancestor_json_schema,
        descendant_json_schema,
        ancestor_validate_schema,
        descendant_validate_schema,
        ancestor_description,
        descendant_description,
        ancestor_created_at,
        descendant_created_at,
        ancestor_updated_at,
        descendant_updated_at,
        ancestor_revision,
        descendant_revision,
    })
}

#[allow(clippy::type_complexity)]
fn object_parts(
    row: StorageGraphObject,
) -> Result<
    (
        i32,
        String,
        i32,
        i32,
        String,
        serde_json::Value,
        chrono::NaiveDateTime,
        chrono::NaiveDateTime,
        ResourceRevision,
    ),
    ApiError,
> {
    let (id, name, collection_id, class_id, description, data, created, updated, revision) =
        row.into_parts();
    Ok((
        id,
        name,
        collection_id,
        class_id,
        description,
        data,
        created,
        updated,
        ResourceRevision::new(revision)?,
    ))
}

fn object_graph_from_storage(
    row: StorageObjectGraphRow,
) -> Result<RelatedObjectGraphRow, ApiError> {
    let (ancestor, descendant, depth, path) = row.into_parts();
    let (
        ancestor_object_id,
        ancestor_name,
        ancestor_collection_id,
        ancestor_class_id,
        ancestor_description,
        ancestor_data,
        ancestor_created_at,
        ancestor_updated_at,
        ancestor_revision,
    ) = object_parts(ancestor)?;
    let (
        descendant_object_id,
        descendant_name,
        descendant_collection_id,
        descendant_class_id,
        descendant_description,
        descendant_data,
        descendant_created_at,
        descendant_updated_at,
        descendant_revision,
    ) = object_parts(descendant)?;
    Ok(RelatedObjectGraphRow {
        ancestor_object_id,
        descendant_object_id,
        depth,
        path,
        ancestor_name,
        descendant_name,
        ancestor_collection_id,
        descendant_collection_id,
        ancestor_class_id,
        descendant_class_id,
        ancestor_description,
        descendant_description,
        ancestor_data,
        descendant_data,
        ancestor_created_at,
        descendant_created_at,
        ancestor_updated_at,
        descendant_updated_at,
        ancestor_revision,
        descendant_revision,
    })
}

fn related_include_from_storage(
    row: StorageRelatedObjectIncludeRow,
) -> Result<RelatedObjectIncludeRow, ApiError> {
    let (root_object_id, row) = row.into_parts();
    let row = object_graph_from_storage(row)?;
    Ok(RelatedObjectIncludeRow {
        root_object_id,
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
    })
}

fn related_for_root_from_storage(
    row: StorageRelatedObjectForRootRow,
) -> Result<RelatedObjectForRootRow, ApiError> {
    let (root_object_id, descendant, depth, path) = row.into_parts();
    let (
        descendant_object_id,
        descendant_name,
        descendant_collection_id,
        descendant_class_id,
        descendant_description,
        descendant_data,
        descendant_created_at,
        descendant_updated_at,
        descendant_revision,
    ) = object_parts(descendant)?;
    Ok(RelatedObjectForRootRow {
        root_object_id,
        descendant_object_id,
        depth,
        path,
        descendant_name,
        descendant_collection_id,
        descendant_class_id,
        descendant_description,
        descendant_data,
        descendant_created_at,
        descendant_updated_at,
        descendant_revision,
    })
}

#[derive(Clone, Copy)]
pub(crate) struct RelationAccess<'a> {
    principal_id: i32,
    is_admin: bool,
    scope: Option<&'a TokenScope>,
}

impl<'a> RelationAccess<'a> {
    pub(crate) const fn new(
        principal_id: i32,
        is_admin: bool,
        scope: Option<&'a TokenScope>,
    ) -> Self {
        Self {
            principal_id,
            is_admin,
            scope,
        }
    }

    fn visibility(self) -> Result<crate::storage::StorageVisibility, ApiError> {
        visibility(self.principal_id, self.is_admin, self.scope)
    }
}

pub(crate) async fn list_class_relations(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    options: QueryOptions,
) -> Result<(Vec<HubuumClassRelation>, Option<i64>), ApiError> {
    let query = RelationListQuery::new(options, access.visibility()?);
    let (rows, total) = storage_handle(backend)
        .list_class_relations(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(class_relation_from_storage)
            .collect::<Result<_, _>>()?,
        total,
    ))
}

pub(crate) async fn list_object_relations(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    options: QueryOptions,
) -> Result<(Vec<HubuumObjectRelation>, Option<i64>), ApiError> {
    let query = RelationListQuery::new(options, access.visibility()?);
    let (rows, total) = storage_handle(backend)
        .list_object_relations(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(object_relation_from_storage)
            .collect::<Result<_, _>>()?,
        total,
    ))
}

pub(crate) async fn list_class_relations_touching(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    class_id: i32,
    options: QueryOptions,
) -> Result<(Vec<HubuumClassRelation>, Option<i64>), ApiError> {
    let query = RelationTouchingQuery::new(class_id, options, access.visibility()?);
    let (rows, total) = storage_handle(backend)
        .list_class_relations_touching(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(class_relation_from_storage)
            .collect::<Result<_, _>>()?,
        total,
    ))
}

pub(crate) async fn list_object_relations_touching(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    object_id: i32,
    options: QueryOptions,
) -> Result<(Vec<HubuumObjectRelation>, Option<i64>), ApiError> {
    let query = RelationTouchingQuery::new(object_id, options, access.visibility()?);
    let (rows, total) = storage_handle(backend)
        .list_object_relations_touching(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(object_relation_from_storage)
            .collect::<Result<_, _>>()?,
        total,
    ))
}

pub(crate) async fn class_relations_touching_ids(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    class_ids: &[i32],
) -> Result<Vec<HubuumClassRelation>, ApiError> {
    let query = RelationIdsQuery::new(class_ids.iter().copied(), access.visibility()?);
    storage_handle(backend)
        .class_relations_touching_ids(query)
        .await?
        .into_iter()
        .map(class_relation_from_storage)
        .collect()
}

pub(crate) async fn class_relations_between_ids(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    class_ids: &[i32],
) -> Result<Vec<HubuumClassRelation>, ApiError> {
    let query = RelationIdsQuery::new(class_ids.iter().copied(), access.visibility()?);
    storage_handle(backend)
        .class_relations_between_ids(query)
        .await?
        .into_iter()
        .map(class_relation_from_storage)
        .collect()
}

pub(crate) async fn object_relations_between_ids(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    object_ids: &[i32],
) -> Result<Vec<HubuumObjectRelation>, ApiError> {
    let query = RelationIdsQuery::new(object_ids.iter().copied(), access.visibility()?);
    storage_handle(backend)
        .object_relations_between_ids(query)
        .await?
        .into_iter()
        .map(object_relation_from_storage)
        .collect()
}

pub(crate) async fn object_relations_touching_ids(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    object_ids: &[i32],
    excluded_relation_ids: &[i32],
    max_results: usize,
) -> Result<Vec<HubuumObjectRelation>, ApiError> {
    let query = ObjectRelationsTouchingIdsQuery::new(
        object_ids.iter().copied(),
        max_results,
        access.visibility()?,
    )
    .excluding_relation_ids(excluded_relation_ids.iter().copied());
    storage_handle(backend)
        .object_relations_touching_ids(query)
        .await?
        .into_iter()
        .map(object_relation_from_storage)
        .collect()
}

pub(crate) async fn related_classes(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    class_id: i32,
    options: QueryOptions,
) -> Result<(Vec<ClassGraphRow>, Option<i64>), ApiError> {
    let query = RelationGraphQuery::new(class_id, options, access.visibility()?);
    let (rows, total) = storage_handle(backend)
        .related_classes(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(class_graph_from_storage)
            .collect::<Result<_, _>>()?,
        total,
    ))
}

pub(crate) async fn related_objects(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    object_id: i32,
    options: QueryOptions,
) -> Result<(Vec<RelatedObjectGraphRow>, Option<i64>), ApiError> {
    let query = RelationGraphQuery::new(object_id, options, access.visibility()?);
    let (rows, total) = storage_handle(backend)
        .related_objects(query)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(object_graph_from_storage)
            .collect::<Result<_, _>>()?,
        total,
    ))
}

pub(crate) async fn related_objects_for_roots(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    root_object_ids: &[i32],
    include: ExportIncludeRelatedQuery,
    preserve_alternative_paths: bool,
) -> Result<Vec<RelatedObjectIncludeRow>, ApiError> {
    let direction = match include.direction {
        ExportIncludeRelatedDirection::Any => StorageRelatedDirection::Any,
        ExportIncludeRelatedDirection::Outgoing => StorageRelatedDirection::Outgoing,
        ExportIncludeRelatedDirection::Incoming => StorageRelatedDirection::Incoming,
    };
    let sort = match include.sort {
        ExportIncludeRelatedSort::Path => StorageRelatedSort::Path,
        ExportIncludeRelatedSort::Name => StorageRelatedSort::Name,
        ExportIncludeRelatedSort::CreatedAt => StorageRelatedSort::CreatedAt,
    };
    let query = RelatedObjectsForRootsQuery::new(
        root_object_ids.iter().copied(),
        include.class_id,
        access.visibility()?,
    )
    .class_relation_id(include.class_relation_id)
    .direction(direction)
    .sort(sort)
    .max_depth(include.max_depth)
    .limit(include.limit)
    .preserve_alternative_paths(preserve_alternative_paths);
    storage_handle(backend)
        .related_objects_for_roots(query)
        .await?
        .into_iter()
        .map(related_include_from_storage)
        .collect()
}

pub(crate) async fn bidirectionally_related_objects_for_roots(
    backend: &impl StorageContext,
    access: RelationAccess<'_>,
    root_object_ids: &[i32],
    max_depth: i32,
    per_root_cap: i32,
    preserve_alternative_paths: bool,
) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
    let query = BidirectionalRelatedObjectsQuery::new(
        root_object_ids.iter().copied(),
        max_depth,
        per_root_cap,
        preserve_alternative_paths,
        access.visibility()?,
    );
    storage_handle(backend)
        .bidirectionally_related_objects_for_roots(query)
        .await?
        .into_iter()
        .map(related_for_root_from_storage)
        .collect()
}
