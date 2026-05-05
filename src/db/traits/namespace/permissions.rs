//! Compatibility shims. The implementations now live in
//! `crate::permissions::local::queries`. Phase 3 routes call sites through
//! `PermissionBackend`; this file goes away once that lands.

use super::*;

pub async fn total_namespace_count_from_backend(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::namespaces::dsl::*;

    with_connection(pool, |conn| namespaces.count().get_result::<i64>(conn))
}

pub async fn user_on_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::user_on_query(pool, user_id, namespace_ref).await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn user_on_paginated_with_total_count_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::user_on_paginated_query(
            pool,
            user_id,
            namespace_ref,
            query_options,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn user_can_on_any_from_backend<U: SelfAccessors<User> + GroupAccessors>(
    pool: &DbPool,
    user_id: U,
    permission_type: Permissions,
) -> Result<Vec<Namespace>, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::user_can_on_any_query(
            pool,
            user_id.id(),
            std::slice::from_ref(&permission_type),
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn group_can_on_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    gid: i32,
    namespace_ref: T,
    permission_type: Permissions,
) -> Result<bool, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::group_can_on_query(
            pool,
            gid,
            namespace_ref,
            permission_type,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn groups_can_on_from_backend(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::groups_can_on_query(pool, nid, permission_type).await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn groups_can_on_paginated_with_total_count_from_backend(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::groups_can_on_paginated_with_total_count_query(
            pool,
            nid,
            permission_type,
            query_options,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn groups_on_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::groups_on_query(
            pool,
            namespace_ref,
            permissions_filter,
            query_options,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn groups_on_paginated_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::groups_on_paginated_query(
            pool,
            namespace_ref,
            permissions_filter,
            query_options,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn groups_on_paginated_with_total_count_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::groups_on_paginated_with_total_count_query(
            pool,
            namespace_ref,
            permissions_filter,
            query_options,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn count_groups_on_paginated_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<i64, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::count_groups_on_paginated_query(
            pool,
            namespace_ref,
            permissions_filter,
            query_options,
        )
        .await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}

pub async fn group_on_from_backend(
    pool: &DbPool,
    nid: i32,
    gid: i32,
) -> Result<Permission, ApiError> {
    #[cfg(feature = "permissions-local")]
    {
        crate::permissions::local::queries::group_on_query(pool, nid, gid).await
    }
    #[cfg(not(feature = "permissions-local"))]
    {
        Err(ApiError::InternalServerError(
            "permissions-local feature not enabled".to_string(),
        ))
    }
}
