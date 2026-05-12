//! Compatibility shims. The implementations now live in
//! `crate::permissions::local::queries`. Phase 3 routes call sites through
//! `PermissionBackend`; this file goes away once that lands.

use serde::Serialize;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::{Permission, Permissions, PermissionsList, User};
use crate::traits::{GroupAccessors, GroupMemberships, NamespaceAccessors, SelfAccessors};

/// COMPAT: thin wrapper around `crate::permissions::local::queries::*` so
/// existing trait-based call sites keep working until Phase 3 routes them
/// through `PermissionBackend`.
#[allow(dead_code)]
pub trait PermissionControllerBackend: Serialize + NamespaceAccessors {
    async fn user_can_all_from_backend<
        U: SelfAccessors<User> + GroupAccessors + GroupMemberships,
    >(
        &self,
        pool: &DbPool,
        user: U,
        permissions_requested: Vec<Permissions>,
    ) -> Result<bool, ApiError> {
        let nid = self.namespace_id(pool).await?;
        #[cfg(feature = "permissions-local")]
        {
            crate::permissions::local::queries::user_can_all_query(
                pool,
                user.id(),
                nid,
                permissions_requested,
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

    async fn apply_permissions_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        let nid = self.namespace_id(pool).await?;
        #[cfg(feature = "permissions-local")]
        {
            crate::permissions::local::queries::apply_permissions_query(
                pool,
                nid,
                group_id_for_grant,
                permission_list,
                replace_existing,
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

    async fn revoke_permissions_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        let nid = self.namespace_id(pool).await?;
        #[cfg(feature = "permissions-local")]
        {
            crate::permissions::local::queries::revoke_permissions_query(
                pool,
                nid,
                group_id_for_revoke,
                permission_list,
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

    async fn revoke_all_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
    ) -> Result<(), ApiError> {
        let nid = self.namespace_id(pool).await?;
        #[cfg(feature = "permissions-local")]
        {
            crate::permissions::local::queries::revoke_all_query(pool, nid, group_id_for_revoke)
                .await
        }
        #[cfg(not(feature = "permissions-local"))]
        {
            Err(ApiError::InternalServerError(
                "permissions-local feature not enabled".to_string(),
            ))
        }
    }
}

impl<T: ?Sized> PermissionControllerBackend for T where T: Serialize + NamespaceAccessors {}
