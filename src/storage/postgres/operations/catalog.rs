use crate::errors::ApiError;
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::resource_rows::{
    class_to_storage, collection_to_storage, object_to_storage,
};
use crate::storage::postgres::operations::user::UserSearchBackend;
use crate::storage::postgres::operations::visibility::{principal, token_scope};
use crate::storage::{
    AuthorizationPermission, CatalogListQuery, CatalogPage, StorageClass, StorageCollection,
    StorageObject,
};

pub(crate) async fn list_collections(
    pool: &PostgresPool,
    query: CatalogListQuery,
) -> Result<CatalogPage<StorageCollection>, ApiError> {
    let include_total = query.options().include_total;
    if !query
        .visibility()
        .allows_permissions(&[AuthorizationPermission::ReadCollection])
    {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }
    let (options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let total = if include_total {
        Some(
            principal
                .count_collections_from_backend_with_admin_status(
                    pool,
                    options.clone(),
                    visibility.is_admin(),
                    scope.as_ref(),
                )
                .await?,
        )
    } else {
        None
    };
    let rows = principal
        .search_collections_from_backend_with_admin_status(
            pool,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?
        .into_iter()
        .map(collection_to_storage)
        .collect();
    Ok(CatalogPage::new(rows, total))
}

pub(crate) async fn list_classes(
    pool: &PostgresPool,
    query: CatalogListQuery,
) -> Result<CatalogPage<StorageClass>, ApiError> {
    let include_total = query.options().include_total;
    if !query.visibility().allows_permissions(&[
        AuthorizationPermission::ReadCollection,
        AuthorizationPermission::ReadClass,
    ]) {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }
    let (options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let total = if include_total {
        Some(
            principal
                .count_classes_from_backend_with_admin_status(
                    pool,
                    options.clone(),
                    visibility.is_admin(),
                    scope.as_ref(),
                )
                .await?,
        )
    } else {
        None
    };
    let rows = principal
        .search_classes_from_backend_with_admin_status(
            pool,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?
        .into_iter()
        .map(class_to_storage)
        .collect();
    Ok(CatalogPage::new(rows, total))
}

pub(crate) async fn list_objects(
    pool: &PostgresPool,
    query: CatalogListQuery,
) -> Result<CatalogPage<StorageObject>, ApiError> {
    let include_total = query.options().include_total;
    if !query.visibility().allows_permissions(&[
        AuthorizationPermission::ReadCollection,
        AuthorizationPermission::ReadObject,
    ]) {
        return Ok(CatalogPage::new(Vec::new(), include_total.then_some(0)));
    }
    let (options, visibility) = query.into_parts();
    let principal = principal(&visibility)?;
    let scope = token_scope(&visibility)?;
    let total = if include_total {
        Some(
            principal
                .count_objects_from_backend_with_admin_status(
                    pool,
                    options.clone(),
                    visibility.is_admin(),
                    scope.as_ref(),
                )
                .await?,
        )
    } else {
        None
    };
    let rows = principal
        .search_objects_from_backend_with_admin_status(
            pool,
            options,
            visibility.is_admin(),
            scope.as_ref(),
        )
        .await?
        .into_iter()
        .map(object_to_storage)
        .collect();
    Ok(CatalogPage::new(rows, total))
}
