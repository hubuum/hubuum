use crate::errors::ApiError;
use crate::models::{
    Collection, HubuumClassExpanded, HubuumObject, TokenScope, UnifiedSearchCursorToken,
    UnifiedSearchSpec,
};
use crate::services::storage_boundary::{
    class_from_storage, collection_from_storage, object_from_storage, resource_id_to_storage,
    visibility,
};
use crate::storage::{
    StorageContext, UnifiedSearchCursor, UnifiedSearchQuery, UnifiedSearchStorage, storage_handle,
};

fn cursor(cursor: Option<&UnifiedSearchCursorToken>) -> Option<UnifiedSearchCursor> {
    cursor.map(|cursor| {
        UnifiedSearchCursor::new(
            cursor.rank,
            cursor.name.clone(),
            resource_id_to_storage(cursor.id),
        )
    })
}

fn query(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
    cursor: Option<&UnifiedSearchCursorToken>,
    search_extended_document: bool,
) -> Result<UnifiedSearchQuery, ApiError> {
    Ok(UnifiedSearchQuery::new(
        spec.query.clone(),
        spec.limit_per_kind,
        visibility(principal_id, is_admin, scope)?,
    )
    .search_extended_document(search_extended_document)
    .cursor(self::cursor(cursor)))
}

pub async fn search_collections(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<Vec<Collection>, ApiError> {
    storage_handle(backend)
        .search_unified_collections(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.collection_cursor.as_ref(),
            false,
        )?)
        .await?
        .into_iter()
        .map(collection_from_storage)
        .collect()
}

pub async fn search_classes(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<Vec<HubuumClassExpanded>, ApiError> {
    storage_handle(backend)
        .search_unified_classes(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.class_cursor.as_ref(),
            spec.search_class_schema,
        )?)
        .await?
        .into_iter()
        .map(class_from_storage)
        .collect()
}

pub async fn search_objects(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    spec: &UnifiedSearchSpec,
) -> Result<Vec<HubuumObject>, ApiError> {
    storage_handle(backend)
        .search_unified_objects(query(
            principal_id,
            is_admin,
            scope,
            spec,
            spec.object_cursor.as_ref(),
            spec.search_object_data,
        )?)
        .await?
        .into_iter()
        .map(object_from_storage)
        .collect()
}
