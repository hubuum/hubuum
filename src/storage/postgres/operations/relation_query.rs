use crate::errors::ApiError;
use crate::models::{
    ExportIncludeRelatedDirection, ExportIncludeRelatedQuery, ExportIncludeRelatedSort,
    HubuumClassID, HubuumObjectID,
};
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::relation_rows::{
    class_graph_to_storage, class_relation_to_storage, object_graph_to_storage,
    object_relation_to_storage, related_for_root_to_storage, related_include_to_storage,
};
use crate::storage::postgres::operations::user::UserSearchBackend;
use crate::storage::postgres::operations::visibility::{principal, token_scope};
use crate::storage::{
    BidirectionalRelatedObjectsQuery, RelatedObjectsForRootsQuery, RelationGraphQuery,
    RelationIdsQuery, RelationListQuery, RelationPage, RelationTouchingQuery, StorageClassGraphRow,
    StorageClassRelation, StorageObjectGraphRow, StorageObjectRelation, StorageRelatedDirection,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRelatedSort,
};

pub(crate) async fn list_class_relations(
    pool: &PostgresPool,
    query: RelationListQuery,
) -> Result<RelationPage<StorageClassRelation>, ApiError> {
    let include_total = query.options().include_total;
    let (options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let (rows, total) = principal
        .class_relations_page_from_backend_with_admin_status(
            pool,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(class_relation_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn list_object_relations(
    pool: &PostgresPool,
    query: RelationListQuery,
) -> Result<RelationPage<StorageObjectRelation>, ApiError> {
    let include_total = query.options().include_total;
    let (options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let (rows, total) = principal
        .object_relations_page_from_backend_with_admin_status(
            pool,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(object_relation_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn list_class_relations_touching(
    pool: &PostgresPool,
    query: RelationTouchingQuery,
) -> Result<RelationPage<StorageClassRelation>, ApiError> {
    let include_total = query.options().include_total;
    let (class_id, options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let class_id = HubuumClassID::new(class_id)?;
    let (rows, total) = principal
        .class_relations_touching_page_from_backend_with_admin_status(
            pool,
            class_id,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(class_relation_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn list_object_relations_touching(
    pool: &PostgresPool,
    query: RelationTouchingQuery,
) -> Result<RelationPage<StorageObjectRelation>, ApiError> {
    let include_total = query.options().include_total;
    let (object_id, options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let object_id = HubuumObjectID::new(object_id)?;
    let (rows, total) = principal
        .object_relations_touching_page_from_backend_with_admin_status(
            pool,
            object_id,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(object_relation_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn class_relations_touching_ids(
    pool: &PostgresPool,
    query: RelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, ApiError> {
    let (ids, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    Ok(principal
        .search_class_relations_touching_ids_from_backend_with_admin_status(
            pool,
            &ids,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?
        .into_iter()
        .map(class_relation_to_storage)
        .collect())
}

pub(crate) async fn class_relations_between_ids(
    pool: &PostgresPool,
    query: RelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, ApiError> {
    let (ids, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    Ok(principal
        .search_class_relations_between_ids_from_backend_with_admin_status(
            pool,
            &ids,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?
        .into_iter()
        .map(class_relation_to_storage)
        .collect())
}

pub(crate) async fn object_relations_between_ids(
    pool: &PostgresPool,
    query: RelationIdsQuery,
) -> Result<Vec<StorageObjectRelation>, ApiError> {
    let (ids, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    Ok(principal
        .search_object_relations_between_ids_from_backend_with_admin_status(
            pool,
            &ids,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?
        .into_iter()
        .map(object_relation_to_storage)
        .collect())
}

pub(crate) async fn related_classes(
    pool: &PostgresPool,
    query: RelationGraphQuery,
) -> Result<RelationPage<StorageClassGraphRow>, ApiError> {
    let include_total = query.options().include_total;
    let (class_id, options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let class_id = HubuumClassID::new(class_id)?;
    let (rows, total) = principal
        .classes_related_to_page_from_backend_with_admin_status(
            pool,
            class_id,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(class_graph_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn related_objects(
    pool: &PostgresPool,
    query: RelationGraphQuery,
) -> Result<RelationPage<StorageObjectGraphRow>, ApiError> {
    let include_total = query.options().include_total;
    let (object_id, options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let object_id = HubuumObjectID::new(object_id)?;
    let (rows, total) = principal
        .objects_related_to_page_from_backend_with_admin_status(
            pool,
            object_id,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?;
    Ok(RelationPage::new(
        rows.into_iter().map(object_graph_to_storage).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn related_objects_for_roots(
    pool: &PostgresPool,
    query: RelatedObjectsForRootsQuery,
) -> Result<Vec<StorageRelatedObjectIncludeRow>, ApiError> {
    let (
        root_ids,
        class_id,
        class_relation_id,
        direction,
        sort,
        max_depth,
        limit,
        preserve_alternative_paths,
        visibility,
    ) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let include = ExportIncludeRelatedQuery {
        class_id,
        class_relation_id,
        direction: match direction {
            StorageRelatedDirection::Any => ExportIncludeRelatedDirection::Any,
            StorageRelatedDirection::Outgoing => ExportIncludeRelatedDirection::Outgoing,
            StorageRelatedDirection::Incoming => ExportIncludeRelatedDirection::Incoming,
        },
        sort: match sort {
            StorageRelatedSort::Path => ExportIncludeRelatedSort::Path,
            StorageRelatedSort::Name => ExportIncludeRelatedSort::Name,
            StorageRelatedSort::CreatedAt => ExportIncludeRelatedSort::CreatedAt,
        },
        max_depth,
        limit,
    };
    let rows = if preserve_alternative_paths {
        principal
            .related_objects_for_roots_preserving_paths_from_backend_with_admin_status(
                pool,
                &root_ids,
                include,
                visibility.is_admin(),
                scope.as_ref(),
            )
            .await?
    } else {
        principal
            .related_objects_for_roots_from_backend_with_admin_status(
                pool,
                &root_ids,
                include,
                visibility.is_admin(),
                scope.as_ref(),
            )
            .await?
    };
    Ok(rows.into_iter().map(related_include_to_storage).collect())
}

pub(crate) async fn bidirectionally_related_objects_for_roots(
    pool: &PostgresPool,
    query: BidirectionalRelatedObjectsQuery,
) -> Result<Vec<StorageRelatedObjectForRootRow>, ApiError> {
    let (root_ids, max_depth, per_root_cap, preserve_alternative_paths, visibility) =
        query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let rows = if preserve_alternative_paths {
        principal
            .bidirectionally_related_objects_for_roots_preserving_paths_from_backend_with_admin_status(
                pool,
                &root_ids,
                max_depth,
                per_root_cap,
                visibility.is_admin(),
                scope.as_ref(),
            )
            .await?
    } else {
        principal
            .bidirectionally_related_objects_for_roots_from_backend_with_admin_status(
                pool,
                &root_ids,
                max_depth,
                per_root_cap,
                visibility.is_admin(),
                scope.as_ref(),
            )
            .await?
    };
    Ok(rows.into_iter().map(related_for_root_to_storage).collect())
}
