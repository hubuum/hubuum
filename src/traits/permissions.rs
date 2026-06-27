use serde::Serialize;

use crate::db::traits::authz::AuthzSubject;
use crate::db::traits::permissions::PermissionControllerBackend;
use crate::errors::ApiError;
use crate::models::{Permission, Permissions, PermissionsList};

use super::{BackendContext, NamespaceAccessors};

#[allow(dead_code)]
pub trait PermissionController: Serialize + NamespaceAccessors {
    /// Check if the user has the given permission on the object.
    ///
    /// - If the trait is called on a namespace, check against self.
    /// - If the trait is called on a HubuumClass or a HubuumObject,
    ///   check against the namespace of the class or object.
    /// - If the trait is called on a HubuumClassID or a HubuumObjectID,
    ///   create a HubuumClass or HubuumObject and check against the namespace
    ///   of the class or object.
    ///
    /// If this is called on a *ID, a full class is created to extract
    /// the namespace_id. To avoid creating the class multiple times during use
    /// do this:
    /// ```ignore
    /// class = class_id.class(backend).await?;
    /// if (class.user_can(backend, subject, Permissions::ReadClass, scopes).await?) {
    ///     return Ok(class);
    /// }
    /// ```
    /// And not this:
    /// ```ignore
    /// if (class_id.user_can(backend, subject, Permissions::ReadClass, scopes).await?) {
    ///    return Ok(class_id.class(backend).await?);
    /// }
    /// ```
    ///
    /// ## Arguments
    ///
    /// * `backend` - The backend context to use for the query.
    /// * `subject` - The principal (impl `AuthzSubject`) to check permissions for.
    /// * `permission` - The permission to check.
    /// * `scopes` - The token scope set as `Option<&[Permissions]>`; `None` = unscoped
    ///   (full authority), `Some(..)` intersects the check fail-closed (even for admins).
    ///
    /// ## Returns
    ///
    /// * `Ok(true)` if the subject has the given permission on this class.
    /// * `Ok(false)` if the subject does not have the given permission on this class.
    /// * `Err(_)` if the lookup fails or the permission is invalid.
    ///
    /// ## Example
    ///
    /// ```ignore
    /// if (hubuum_class_or_classid.user_can(backend, subject, Permissions::ReadClass, scopes).await?) {
    ///     // Do something
    /// }
    async fn user_can<C, S>(
        &self,
        backend: &C,
        subject: S,
        permission: Permissions,
        scopes: Option<&[Permissions]>,
    ) -> Result<bool, ApiError>
    where
        C: BackendContext + ?Sized,
        S: AuthzSubject,
    {
        self.user_can_all(backend, subject, vec![permission], scopes)
            .await
    }

    /// Check if the user has all the given permissions on the object.
    ///
    /// - If the trait is called on a namespace, check against self.
    /// - If the trait is called on a HubuumClass or a HubuumObject,
    ///   check against the namespace of the class or object.
    /// - If the trait is called on a HubuumClassID or a HubuumObjectID,
    ///   create a HubuumClass or HubuumObject and check against the namespace
    ///   of the class or object.
    ///
    /// If this is called on a *ID, a full class is created to extract
    /// the namespace_id. To avoid creating the class multiple times during use
    /// do this:
    /// ```ignore
    /// permissions = vec![Permissions::ReadClass, Permissions::UpdateClass];
    /// class = class_id.class(backend).await?;
    /// if (class.user_can_all(backend, subject, permissions, scopes).await?) {
    ///     return Ok(class);
    /// }
    /// ```
    /// And not this:
    /// ```ignore
    /// permissions = vec![Permissions::ReadClass, Permissions::UpdateClass];
    /// if (class_id.user_can_all(backend, subject, permissions, scopes).await?) {
    ///    return Ok(class_id.class(backend).await?);
    /// }
    /// ```
    ///
    /// ## Arguments
    ///
    /// * `backend` - The backend context to use for the query.
    /// * `subject` - The principal (impl `AuthzSubject`) to check permissions for.
    /// * `permission` - The permissions to check (all must be present).
    /// * `scopes` - The token scope set as `Option<&[Permissions]>`; `None` = unscoped
    ///   (full authority), `Some(..)` intersects the check fail-closed (even for admins).
    ///
    /// ## Returns
    ///
    /// * `Ok(true)` if the subject has all the given permissions on this class.
    /// * `Ok(false)` if the subject does not.
    /// * `Err(_)` if the lookup fails or a permission is invalid.
    ///
    /// ## Example
    ///
    /// ```ignore
    /// if (hubuum_class_or_classid.user_can_all(backend, subject, permissions, scopes).await?) {
    ///     // Do something
    /// }
    async fn user_can_all<C, S>(
        &self,
        backend: &C,
        subject: S,
        permission: Vec<Permissions>,
        scopes: Option<&[Permissions]>,
    ) -> Result<bool, ApiError>
    where
        C: BackendContext + ?Sized,
        S: AuthzSubject,
    {
        self.user_can_all_from_backend(backend.db_pool(), subject, permission, scopes)
            .await
    }

    /// Grant a set of permissions to a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions are added to the existing permission object for
    ///   the group.
    /// - If the group did not have any permissions, a new permission
    ///   object is created for the group, with the requested permissions.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_id_for_grant` - The group ID to grant the permissions to.
    /// - `permission_list` - A list of permissions to grant, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    async fn grant<C>(
        &self,
        backend: &C,
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.apply_permissions(backend, group_id_for_grant, permission_list, false)
            .await
    }

    /// Apply permissions to a group, optionally replacing existing permissions.
    ///
    /// - When `replace_existing` is false, no permissions are removed from the group.
    /// - When `replace_existing` is true, any existing permissions are cleared first.
    async fn apply_permissions<C>(
        &self,
        backend: &C,
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.apply_permissions_from_backend(
            backend.db_pool(),
            group_id_for_grant,
            permission_list,
            replace_existing,
        )
        .await
    }

    /// Revoke a set of permissions from a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions are removed from the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have any permissions, no permissions are modified
    ///   and an ApiError::NotFound is returned.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_id_for_revoke` - The group ID to revoke the permissions from.
    /// - `permission_list` - A list of permissions to revoke, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group. If the group
    /// did not have any permissions, an ApiError::NotFound is returned.
    async fn revoke<C>(
        &self,
        backend: &C,
        group_id_for_revoke: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.revoke_permissions_from_backend(
            backend.db_pool(),
            group_id_for_revoke,
            permission_list,
        )
        .await
    }

    /// Grant a specific permission to a group.
    ///
    /// - If the group previously had the permission, the requested
    ///   permission is added to the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have the permission, a new permission
    ///   object is created for the group, with the requested permission.
    ///
    /// - No permissions are removed from the group.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_identifier` - The group ID to grant the permission to.
    /// - `permission` - The permission to grant.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    async fn grant_one<C>(
        &self,
        backend: &C,
        group_identifier: i32,
        permission: Permissions,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.grant(
            backend,
            group_identifier,
            PermissionsList::new(vec![permission]),
        )
        .await
    }

    /// Revoke a specific permission from a group.
    ///
    /// - If the group previously had the permission, the requested
    ///   permission is removed from the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have the permission, no permissions are modified
    ///   and an ApiError::NotFound is returned.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_identifier` - The group ID to revoke the permission from.
    /// - `permission` - The permission to revoke.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group. If the group
    /// did not have the permission, an ApiError::NotFound is returned.
    async fn revoke_one<C>(
        &self,
        backend: &C,
        group_identifier: i32,
        permission: Permissions,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.revoke(
            backend,
            group_identifier,
            PermissionsList::new(vec![permission]),
        )
        .await
    }

    /// Set the permissions for a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions *replace* the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have any permissions, a new permission
    ///   object is created for the group, with the requested permissions.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_identifier` - The group ID to set the permissions for.
    /// - `permission_list` - A list of permissions to set, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    async fn set_permissions<C>(
        &self,
        backend: &C,
        group_identifier: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.apply_permissions(backend, group_identifier, permission_list, true)
            .await
    }

    /// Revoke all permissions from a group.
    ///
    /// - If the group previously had any permissions, these are removed.
    ///
    /// - If the group did not have any permissions, no action is taken.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_id_for_revoke` - The group ID to revoke the permissions from.
    ///
    /// ## Returns
    ///
    /// An empty result.
    async fn revoke_all<C>(&self, backend: &C, group_id_for_revoke: i32) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.revoke_all_from_backend(backend.db_pool(), group_id_for_revoke)
            .await
    }
}
