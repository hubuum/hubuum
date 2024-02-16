use serde::Serialize;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::class::HubuumClass;
use crate::models::namespace::Namespace;
use crate::models::object::HubuumObject;
use crate::models::permissions::PermissionsList;
use crate::models::user::UserID;

pub trait CanDelete {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError>;
}

pub trait CanSave {
    type Output;
    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError>;
}

pub trait CanUpdate {
    type Output;

    async fn update(&self, pool: &DbPool, entry_id: i32) -> Result<Self::Output, ApiError>;
}

// This trait is used to provide a uniform interface for both EntityID
// and Entity types, ie User and UserID.
pub trait SelfAccessors<T> {
    fn id(&self) -> i32;
    async fn instance(&self, pool: &DbPool) -> Result<T, ApiError>;
}

pub trait NamespaceAccessors {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
}

pub trait ClassAccessors {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
}

pub trait ObjectAccessors {
    async fn object(&self, pool: &DbPool) -> Result<HubuumObject, ApiError>;
    async fn object_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
}

pub trait PermissionInterface: Serialize {
    type PermissionEnum;
    type PermissionType;

    /// Check if a user has a specific permission on the current object.
    ///
    /// - If the trait is called on a namespace, check against self.
    /// - If the trait is called on a HubuumClass or a HubuumObject,
    ///   check against the namespace of the class or object.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `user_id` - The user ID to check.
    ///
    /// ## Returns
    ///
    /// A boolean indicating if the user has the permission.
    ///
    /// ## Example
    ///
    /// ```
    /// if (namespace.user_can(pool, user_id, NamespacePermissions::ReadCollection).await?) {
    ///    // Do something
    /// }
    /// ```
    async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission: Self::PermissionEnum,
    ) -> Result<bool, ApiError>;

    /// Grant a set of permissions to a group.
    ///
    /// - If the group previously had any permissions, the requested
    /// permissions are added to the existing permission object for
    /// the group.
    ///
    /// - If the group did not have any permissions, a new permission
    /// object is created for the group, with the requested permissions.
    ///
    /// - No permissions are removed from the group.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `group_identifier` - The group ID to grant the permissions to.
    /// - `permission_list` - A list of permissions to grant, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    async fn grant(
        &self,
        pool: &DbPool,
        group_identifier: i32,
        permission_list: PermissionsList<Self::PermissionEnum>,
    ) -> Result<Self::PermissionType, ApiError>
    where
        <Self as PermissionInterface>::PermissionEnum: Serialize + PartialEq;

    /// Revoke a set of permissions from a group.
    ///
    /// - If the group previously had any permissions, the requested
    /// permissions are removed from the existing permission object for
    /// the group.
    ///
    /// - If the group did not have any permissions, no permissions are modified
    /// and an ApiError::NotFound is returned.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `group_identifier` - The group ID to revoke the permissions from.
    /// - `permission_list` - A list of permissions to revoke, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group. If the group
    /// did not have any permissions, an ApiError::NotFound is returned.
    async fn revoke(
        &self,
        pool: &DbPool,
        group_identifier: i32,
        permission_list: PermissionsList<Self::PermissionEnum>,
    ) -> Result<Self::PermissionType, ApiError>
    where
        <Self as PermissionInterface>::PermissionEnum: Serialize + PartialEq;

    /// Grant a specific permission to a group.
    ///
    /// - If the group previously had the permission, the requested
    /// permission is added to the existing permission object for
    /// the group.
    ///
    /// - If the group did not have the permission, a new permission
    /// object is created for the group, with the requested permission.
    ///
    /// - No permissions are removed from the group.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `group_identifier` - The group ID to grant the permission to.
    /// - `permission` - The permission to grant.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    async fn grant_one(
        &self,
        pool: &DbPool,
        group_identifier: i32,
        permission: Self::PermissionEnum,
    ) -> Result<Self::PermissionType, ApiError>
    where
        <Self as PermissionInterface>::PermissionEnum: Serialize + PartialEq,
    {
        self.grant(
            pool,
            group_identifier,
            PermissionsList::new(vec![permission]),
        )
        .await
    }

    /// Revoke a specific permission from a group.
    ///
    /// - If the group previously had the permission, the requested
    /// permission is removed from the existing permission object for
    /// the group.
    ///
    /// - If the group did not have the permission, no permissions are modified
    /// and an ApiError::NotFound is returned.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `group_identifier` - The group ID to revoke the permission from.
    /// - `permission` - The permission to revoke.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group. If the group
    /// did not have the permission, an ApiError::NotFound is returned.
    async fn revoke_one(
        &self,
        pool: &DbPool,
        group_identifier: i32,
        permission: Self::PermissionEnum,
    ) -> Result<Self::PermissionType, ApiError>
    where
        <Self as PermissionInterface>::PermissionEnum: Serialize + PartialEq,
    {
        self.revoke(
            pool,
            group_identifier,
            PermissionsList::new(vec![permission]),
        )
        .await
    }

    /// Set the permissions for a group.
    ///
    /// - If the group previously had any permissions, the requested
    /// permissions *replace* the existing permission object for
    /// the group.
    ///
    /// - If the group did not have any permissions, a new permission
    /// object is created for the group, with the requested permissions.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `group_identifier` - The group ID to set the permissions for.
    /// - `permission_list` - A list of permissions to set, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    async fn set_permissions(
        &self,
        pool: &DbPool,
        group_identifier: i32,
        permission_list: PermissionsList<Self::PermissionEnum>,
    ) -> Result<Self::PermissionType, ApiError>
    where
        <Self as PermissionInterface>::PermissionEnum: Serialize + PartialEq;

    /// Revoke all permissions from a group.
    ///
    /// - If the group previously had any permissions, these are removed.
    ///
    /// - If the group did not have any permissions, no action is taken.
    ///
    /// ## Arguments
    ///
    /// - `pool` - A connection pool to the database.
    /// - `group_identifier` - The group ID to revoke the permissions from.
    ///
    /// ## Returns
    ///
    /// An empty result.

    async fn revoke_all(&self, pool: &DbPool, group_identifier: i32) -> Result<(), ApiError>;
}
