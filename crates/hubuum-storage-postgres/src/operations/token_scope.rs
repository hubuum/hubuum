use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId, ObjectId};
use hubuum_storage_core::{
    StorageAuthenticationResourceScope, StorageAuthenticationTokenScope,
    StorageAuthenticationTokenScopeQuery, StorageAuthorizationPermission,
};

use crate::{PostgresConnection, PostgresStorageError};

pub(super) fn permission_from_persisted(
    value: String,
) -> Result<StorageAuthorizationPermission, PostgresStorageError> {
    StorageAuthorizationPermission::from_name(&value).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("token permission scope", error)
    })
}

pub(super) async fn load_token_scope(
    connection: &mut PostgresConnection,
    query: StorageAuthenticationTokenScopeQuery,
) -> Result<Option<StorageAuthenticationTokenScope>, PostgresStorageError> {
    if !query.is_scoped() {
        return Ok(None);
    }
    let token_id = query.token_id().id();
    let permissions = if query.is_permission_scoped() {
        Some(
            crate::schema::token_scopes::table
                .filter(crate::schema::token_scopes::token_id.eq(token_id))
                .order_by(crate::schema::token_scopes::permission.asc())
                .select(crate::schema::token_scopes::permission)
                .load::<String>(connection)
                .await?
                .into_iter()
                .map(permission_from_persisted)
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        None
    };
    let resources = if query.is_resource_scoped() {
        let collection_ids = crate::schema::token_collection_scopes::table
            .filter(crate::schema::token_collection_scopes::token_id.eq(token_id))
            .order_by(crate::schema::token_collection_scopes::collection_id.asc())
            .select(crate::schema::token_collection_scopes::collection_id)
            .load::<i32>(connection)
            .await?;
        let class_ids = crate::schema::token_class_scopes::table
            .filter(crate::schema::token_class_scopes::token_id.eq(token_id))
            .order_by(crate::schema::token_class_scopes::class_id.asc())
            .select(crate::schema::token_class_scopes::class_id)
            .load::<i32>(connection)
            .await?;
        let object_ids = crate::schema::token_object_scopes::table
            .filter(crate::schema::token_object_scopes::token_id.eq(token_id))
            .order_by(crate::schema::token_object_scopes::object_id.asc())
            .select(crate::schema::token_object_scopes::object_id)
            .load::<i32>(connection)
            .await?;
        Some(StorageAuthenticationResourceScope::new(
            collection_ids
                .into_iter()
                .map(CollectionId::new)
                .collect::<Result<Vec<_>, _>>()?,
            class_ids
                .into_iter()
                .map(ClassId::new)
                .collect::<Result<Vec<_>, _>>()?,
            object_ids
                .into_iter()
                .map(ObjectId::new)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        None
    };
    Ok(Some(StorageAuthenticationTokenScope::new(
        permissions,
        resources,
    )))
}
