use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{Collection, HubuumClassExpanded, HubuumObject, TokenScope};
use crate::services::storage_boundary::{
    class_from_storage, collection_from_storage, object_from_storage, visibility,
};
use crate::storage::{CatalogListQuery, CatalogStorage, StorageContext, storage_handle};

fn query(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    options: QueryOptions,
) -> Result<CatalogListQuery, ApiError> {
    Ok(CatalogListQuery::new(
        options,
        visibility(principal_id, is_admin, scope)?,
    ))
}

pub(crate) async fn list_collections(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    options: QueryOptions,
) -> Result<(Vec<Collection>, Option<i64>), ApiError> {
    let (rows, total) = storage_handle(backend)
        .list_collections(query(principal_id, is_admin, scope, options)?)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(collection_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total,
    ))
}

pub(crate) async fn list_classes(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    options: QueryOptions,
) -> Result<(Vec<HubuumClassExpanded>, Option<i64>), ApiError> {
    let (rows, total) = storage_handle(backend)
        .list_classes(query(principal_id, is_admin, scope, options)?)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(class_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total,
    ))
}

pub(crate) async fn list_objects(
    backend: &impl StorageContext,
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
    options: QueryOptions,
) -> Result<(Vec<HubuumObject>, Option<i64>), ApiError> {
    let (rows, total) = storage_handle(backend)
        .list_objects(query(principal_id, is_admin, scope, options)?)
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(object_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total,
    ))
}
