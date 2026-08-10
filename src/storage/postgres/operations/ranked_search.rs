use crate::errors::ApiError;
use crate::models::{UnifiedSearchCursorToken, UnifiedSearchSpec};
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::resource_rows::{
    class_to_storage, collection_to_storage, object_to_storage,
};
use crate::storage::postgres::operations::user::UnifiedSearchBackend;
use crate::storage::postgres::operations::visibility::{principal, token_scope};
use crate::storage::{
    UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchObject, UnifiedSearchQuery,
};

#[derive(Clone, Copy)]
enum SearchKind {
    Collection,
    Class,
    Object,
}

fn cursor(query: &UnifiedSearchQuery) -> Option<UnifiedSearchCursorToken> {
    query
        .search_cursor()
        .map(|cursor| UnifiedSearchCursorToken {
            rank: cursor.rank(),
            name: cursor.normalized_name().to_string(),
            id: cursor.id(),
        })
}

fn spec(query: &UnifiedSearchQuery, kind: SearchKind) -> UnifiedSearchSpec {
    let cursor = cursor(query);
    UnifiedSearchSpec {
        query: query.search_term().to_string(),
        search_class_schema: matches!(kind, SearchKind::Class)
            && query.searches_extended_document(),
        search_object_data: matches!(kind, SearchKind::Object)
            && query.searches_extended_document(),
        limit_per_kind: query.limit(),
        collection_cursor: matches!(kind, SearchKind::Collection)
            .then_some(cursor.clone())
            .flatten(),
        class_cursor: matches!(kind, SearchKind::Class)
            .then_some(cursor.clone())
            .flatten(),
        object_cursor: matches!(kind, SearchKind::Object)
            .then_some(cursor)
            .flatten(),
    }
}

pub(crate) async fn search_collections(
    pool: &PostgresPool,
    query: UnifiedSearchQuery,
) -> Result<Vec<UnifiedSearchCollection>, ApiError> {
    if !query
        .visibility()
        .allows_permissions(&[crate::storage::AuthorizationPermission::ReadCollection])
    {
        return Ok(Vec::new());
    }
    let principal = principal(query.visibility())?;
    let scope = token_scope(query.visibility())?;
    let spec = spec(&query, SearchKind::Collection);
    principal
        .search_unified_collections_from_backend_with_admin_status(
            pool,
            &spec,
            scope.as_ref(),
            query.visibility().is_admin(),
        )
        .await
        .map(|rows| rows.into_iter().map(collection_to_storage).collect())
}

pub(crate) async fn search_classes(
    pool: &PostgresPool,
    query: UnifiedSearchQuery,
) -> Result<Vec<UnifiedSearchClass>, ApiError> {
    if !query.visibility().allows_permissions(&[
        crate::storage::AuthorizationPermission::ReadCollection,
        crate::storage::AuthorizationPermission::ReadClass,
    ]) {
        return Ok(Vec::new());
    }
    let principal = principal(query.visibility())?;
    let scope = token_scope(query.visibility())?;
    let spec = spec(&query, SearchKind::Class);
    principal
        .search_unified_classes_from_backend_with_admin_status(
            pool,
            &spec,
            scope.as_ref(),
            query.visibility().is_admin(),
        )
        .await
        .map(|rows| rows.into_iter().map(class_to_storage).collect())
}

pub(crate) async fn search_objects(
    pool: &PostgresPool,
    query: UnifiedSearchQuery,
) -> Result<Vec<UnifiedSearchObject>, ApiError> {
    if !query.visibility().allows_permissions(&[
        crate::storage::AuthorizationPermission::ReadCollection,
        crate::storage::AuthorizationPermission::ReadObject,
    ]) {
        return Ok(Vec::new());
    }
    let principal = principal(query.visibility())?;
    let scope = token_scope(query.visibility())?;
    let spec = spec(&query, SearchKind::Object);
    principal
        .search_unified_objects_from_backend_with_admin_status(
            pool,
            &spec,
            scope.as_ref(),
            query.visibility().is_admin(),
        )
        .await
        .map(|rows| rows.into_iter().map(object_to_storage).collect())
}
