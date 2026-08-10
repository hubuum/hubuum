use crate::errors::ApiError;
use crate::models::{
    Collection, CollectionID, HubuumClassExpanded, HubuumClassID, HubuumObject, HubuumObjectID,
    TokenResourceScope, TokenScope, UnifiedSearchCursorToken, UnifiedSearchSpec, UserID,
};
use crate::storage::postgres::PostgresPool;
use crate::storage::postgres::operations::authorization::permission_from_storage;
use crate::storage::postgres::operations::user::UnifiedSearchBackend;
use crate::storage::{
    UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchObject, UnifiedSearchQuery,
};

#[derive(Clone, Copy)]
enum SearchKind {
    Collection,
    Class,
    Object,
}

fn collection_to_storage(collection: Collection) -> UnifiedSearchCollection {
    UnifiedSearchCollection::new(
        collection.id,
        collection.name,
        collection.description,
        collection.created_at,
        collection.updated_at,
        collection.parent_collection_id,
        collection.revision.get(),
    )
}

fn class_to_storage(class: HubuumClassExpanded) -> UnifiedSearchClass {
    UnifiedSearchClass::new(
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

fn object_to_storage(object: HubuumObject) -> UnifiedSearchObject {
    UnifiedSearchObject::new(
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

fn token_scope(query: &UnifiedSearchQuery) -> Result<Option<TokenScope>, ApiError> {
    let visibility = query.visibility();
    let permissions = visibility.permissions().map(|permissions| {
        permissions
            .iter()
            .copied()
            .map(permission_from_storage)
            .collect::<Vec<_>>()
    });
    let resources = visibility
        .resources()
        .map(|scope| {
            scope
                .collection_ids()
                .iter()
                .copied()
                .map(|id| CollectionID::new(id).map(TokenResourceScope::Collection))
                .chain(
                    scope
                        .class_ids()
                        .iter()
                        .copied()
                        .map(|id| HubuumClassID::new(id).map(TokenResourceScope::Class)),
                )
                .chain(
                    scope
                        .object_ids()
                        .iter()
                        .copied()
                        .map(|id| HubuumObjectID::new(id).map(TokenResourceScope::Object)),
                )
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    if permissions.is_none() && resources.is_none() {
        Ok(None)
    } else {
        TokenScope::from_stored_parts(permissions, resources).map(Some)
    }
}

fn principal(query: &UnifiedSearchQuery) -> Result<UserID, ApiError> {
    UserID::new(query.visibility().principal_id())
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
    let principal = principal(&query)?;
    let scope = token_scope(&query)?;
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
    let principal = principal(&query)?;
    let scope = token_scope(&query)?;
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
    let principal = principal(&query)?;
    let scope = token_scope(&query)?;
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
