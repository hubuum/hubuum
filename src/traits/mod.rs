use diesel::prelude::*;
use serde::Serialize;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::{
    HubuumClass, Namespace, NewPermission, Permission, PermissionFilter, Permissions,
    PermissionsList, UpdatePermission, User,
};

#[allow(unused_imports)]
pub use crate::models::traits::user::{GroupAccessors, Search, UserNamespaceAccessors};

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
#[allow(async_fn_in_trait)]
pub trait SelfAccessors<T> {
    fn id(&self) -> i32;
    async fn instance(&self, pool: &DbPool) -> Result<T, ApiError>;
}

#[allow(async_fn_in_trait)]
pub trait NamespaceAccessors<N = Namespace, I = i32> {
    async fn namespace(&self, pool: &DbPool) -> Result<N, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

pub trait ClassAccessors<C = HubuumClass, I = i32> {
    async fn class(&self, pool: &DbPool) -> Result<C, ApiError>;
    async fn class_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

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
    /// class = class_id.class(pool).await?;
    /// if (class.user_can(pool, userid, Permissions::ReadClass).await?) {
    ///     return Ok(class);
    /// }
    /// ```
    /// And not this:
    /// ```ignore
    /// if (class_id.user_can(pool, userid, Permissions::ReadClass).await?) {
    ///    return Ok(class_id.class(pool).await?);
    /// }
    /// ```
    ///
    /// ## Arguments
    ///
    /// * `pool` - The database pool to use for the query.
    /// * `user_id` - The user to check permissions for.
    /// * `permission` - The permission to check.
    ///
    /// ## Returns
    ///
    /// * `Ok(true)` if the user has the given permission on this class.
    /// * `Ok(false)` if the user does not have the given permission on this class.
    /// * `Err(_)` if the user does not have the given permission on this class, or if the
    ///   permission is invalid.
    ///
    /// ## Example
    ///
    /// ```ignore
    /// if (hubuum_class_or_classid.user_can(pool, userid, ClassPermissions::ReadClass).await?) {
    ///     // Do something
    /// }
    async fn user_can<U: SelfAccessors<User> + GroupAccessors>(
        &self,
        pool: &DbPool,
        user: U,
        permission: Permissions,
    ) -> Result<bool, ApiError> {
        self.user_can_all(pool, user, vec![permission]).await
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
    /// class = class_id.class(pool).await?;
    /// if (class.user_can(pool, userid, permissions).await?) {
    ///     return Ok(class);
    /// }
    /// ```
    /// And not this:
    /// ```ignore
    /// permissions = vec![Permissions::ReadClass, Permissions::UpdateClass];
    /// if (class_id.user_can(pool, userid, permissions).await?) {
    ///    return Ok(class_id.class(pool).await?);
    /// }
    /// ```
    ///
    /// ## Arguments
    ///
    /// * `pool` - The database pool to use for the query.
    /// * `user_id` - The user to check permissions for.
    /// * `permission` - The permission to check.
    ///
    /// ## Returns
    ///
    /// * `Ok(true)` if the user has the given permission on this class.
    /// * `Ok(false)` if the user does not have the given permission on this class.
    /// * `Err(_)` if the user does not have the given permission on this class, or if the
    ///   permission is invalid.
    ///
    /// ## Example
    ///
    /// ```ignore
    /// if (hubuum_class_or_classid.user_can(pool, userid, ClassPermissions::ReadClass).await?) {
    ///     // Do something
    /// }
    async fn user_can_all<U: SelfAccessors<User> + GroupAccessors>(
        &self,
        pool: &DbPool,
        user: U,
        permission: Vec<Permissions>,
    ) -> Result<bool, ApiError> {
        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;

        let mut conn = pool.get()?;
        let group_id_subquery = user.group_ids_subquery();

        // Note that self.namespace_id(pool).await? is only a query if the caller is a HubuumClassID, otherwise
        // it's a simple field access (which ignores the passed pool).
        let mut base_query = lookup_table
            .into_boxed()
            .filter(namespace_id_field.eq(self.namespace_id(pool).await?))
            .filter(group_id_field.eq_any(group_id_subquery));

        for perm in permission {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        let result = base_query.first::<Permission>(&mut conn).optional()?;

        Ok(result.is_some())
    }

    /// Grant a set of permissions to a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions are added to the existing permission object for
    ///   the group.
    /// - If the group did not have any permissions, a new permission
    ///   object is created for the group, with the requested permissions.
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
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        // If the group already has permissions, update the permissions in permissions. Otherwise, insert a new row.
        let mut conn = pool.get()?;

        let nid = self.namespace_id(pool).await?;

        conn.transaction::<_, ApiError, _>(|conn| {
            let existing_entry = permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_grant))
                .first::<Permission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => {
                    let mut update_perm = UpdatePermission::default();
                    for permission in permission_list.into_iter() {
                        match permission {
                            Permissions::ReadCollection => {
                                update_perm.has_read_namespace = Some(true);
                            }
                            Permissions::UpdateCollection => {
                                update_perm.has_update_namespace = Some(true);
                            }
                            Permissions::DeleteCollection => {
                                update_perm.has_delete_namespace = Some(true);
                            }
                            Permissions::DelegateCollection => {
                                update_perm.has_delegate_namespace = Some(true);
                            }
                            Permissions::CreateClass => {
                                update_perm.has_create_class = Some(true);
                            }
                            Permissions::ReadClass => {
                                update_perm.has_read_class = Some(true);
                            }
                            Permissions::UpdateClass => {
                                update_perm.has_update_class = Some(true);
                            }
                            Permissions::DeleteClass => {
                                update_perm.has_delete_class = Some(true);
                            }
                            Permissions::CreateObject => {
                                update_perm.has_create_object = Some(true);
                            }
                            Permissions::ReadObject => {
                                update_perm.has_read_object = Some(true);
                            }
                            Permissions::UpdateObject => {
                                update_perm.has_update_object = Some(true);
                            }
                            Permissions::DeleteObject => {
                                update_perm.has_delete_object = Some(true);
                            }
                            Permissions::CreateClassRelation => {
                                update_perm.has_create_class_relation = Some(true);
                            }
                            Permissions::ReadClassRelation => {
                                update_perm.has_read_class_relation = Some(true);
                            }
                            Permissions::UpdateClassRelation => {
                                update_perm.has_update_class_relation = Some(true);
                            }
                            Permissions::DeleteClassRelation => {
                                update_perm.has_delete_class_relation = Some(true);
                            }
                            Permissions::CreateObjectRelation => {
                                update_perm.has_create_object_relation = Some(true);
                            }
                            Permissions::ReadObjectRelation => {
                                update_perm.has_read_object_relation = Some(true);
                            }
                            Permissions::UpdateObjectRelation => {
                                update_perm.has_update_object_relation = Some(true);
                            }
                            Permissions::DeleteObjectRelation => {
                                update_perm.has_delete_object_relation = Some(true);
                            }
                        }
                    }

                    Ok(diesel::update(permissions)
                        .filter(namespace_id.eq(nid))
                        .filter(group_id.eq(group_id_for_grant))
                        .set(&update_perm)
                        .get_result(conn)?)
                }
                None => {
                    let new_entry = NewPermission {
                        namespace_id: nid,
                        group_id: group_id_for_grant,
                        has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
                        has_update_namespace: permission_list
                            .contains(&Permissions::UpdateCollection),
                        has_delete_namespace: permission_list
                            .contains(&Permissions::DeleteCollection),
                        has_delegate_namespace: permission_list
                            .contains(&Permissions::DelegateCollection),
                        has_create_class: permission_list.contains(&Permissions::CreateClass),
                        has_read_class: permission_list.contains(&Permissions::ReadClass),
                        has_update_class: permission_list.contains(&Permissions::UpdateClass),
                        has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                        has_create_object: permission_list.contains(&Permissions::CreateObject),
                        has_read_object: permission_list.contains(&Permissions::ReadObject),
                        has_update_object: permission_list.contains(&Permissions::UpdateObject),
                        has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                        has_create_class_relation: permission_list
                            .contains(&Permissions::CreateClassRelation),
                        has_read_class_relation: permission_list
                            .contains(&Permissions::ReadClassRelation),
                        has_update_class_relation: permission_list
                            .contains(&Permissions::UpdateClassRelation),
                        has_delete_class_relation: permission_list
                            .contains(&Permissions::DeleteClassRelation),
                        has_create_object_relation: permission_list
                            .contains(&Permissions::CreateObjectRelation),
                        has_read_object_relation: permission_list
                            .contains(&Permissions::ReadObjectRelation),
                        has_update_object_relation: permission_list
                            .contains(&Permissions::UpdateObjectRelation),
                        has_delete_object_relation: permission_list
                            .contains(&Permissions::DeleteObjectRelation),
                    };
                    Ok(diesel::insert_into(permissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
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
        group_id_for_revoke: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        let mut conn = pool.get()?;

        let nid = self.namespace_id(pool).await?;

        conn.transaction::<_, ApiError, _>(|conn| {
            permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<Permission>(conn)?;

            let mut update_perm = UpdatePermission::default();
            for permission in permission_list.into_iter() {
                match permission {
                    Permissions::ReadCollection => {
                        update_perm.has_read_namespace = Some(false);
                    }
                    Permissions::UpdateCollection => {
                        update_perm.has_update_namespace = Some(false);
                    }
                    Permissions::DeleteCollection => {
                        update_perm.has_delete_namespace = Some(false);
                    }
                    Permissions::DelegateCollection => {
                        update_perm.has_delegate_namespace = Some(false);
                    }
                    Permissions::CreateClass => {
                        update_perm.has_create_class = Some(false);
                    }
                    Permissions::ReadClass => {
                        update_perm.has_read_class = Some(false);
                    }
                    Permissions::UpdateClass => {
                        update_perm.has_update_class = Some(false);
                    }
                    Permissions::DeleteClass => {
                        update_perm.has_delete_class = Some(false);
                    }
                    Permissions::CreateObject => {
                        update_perm.has_create_object = Some(false);
                    }
                    Permissions::ReadObject => {
                        update_perm.has_read_object = Some(false);
                    }
                    Permissions::UpdateObject => {
                        update_perm.has_update_object = Some(false);
                    }
                    Permissions::DeleteObject => {
                        update_perm.has_delete_object = Some(false);
                    }
                    Permissions::CreateClassRelation => {
                        update_perm.has_create_class_relation = Some(false);
                    }
                    Permissions::ReadClassRelation => {
                        update_perm.has_read_class_relation = Some(false);
                    }
                    Permissions::UpdateClassRelation => {
                        update_perm.has_update_class_relation = Some(false);
                    }
                    Permissions::DeleteClassRelation => {
                        update_perm.has_delete_class_relation = Some(false);
                    }
                    Permissions::CreateObjectRelation => {
                        update_perm.has_create_object_relation = Some(false);
                    }
                    Permissions::ReadObjectRelation => {
                        update_perm.has_read_object_relation = Some(false);
                    }
                    Permissions::UpdateObjectRelation => {
                        update_perm.has_update_object_relation = Some(false);
                    }
                    Permissions::DeleteObjectRelation => {
                        update_perm.has_delete_object_relation = Some(false);
                    }
                }
            }
            Ok(diesel::update(permissions)
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .set(&update_perm)
                .get_result(conn)?)
        })
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
        permission: Permissions,
    ) -> Result<Permission, ApiError> {
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
    ///   permission is removed from the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have the permission, no permissions are modified
    ///   and an ApiError::NotFound is returned.
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
        permission: Permissions,
    ) -> Result<Permission, ApiError> {
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
    ///   permissions *replace* the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have any permissions, a new permission
    ///   object is created for the group, with the requested permissions.
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
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        let mut conn = pool.get()?;
        let nid = self.namespace_id(pool).await?;

        conn.transaction::<_, ApiError, _>(|conn| {
            let existing_entry = permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_identifier))
                .first::<Permission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => Ok(diesel::update(permissions)
                    .filter(namespace_id.eq(nid))
                    .filter(group_id.eq(group_identifier))
                    .set((
                        has_read_namespace
                            .eq(permission_list.contains(&Permissions::ReadCollection)),
                        has_update_namespace
                            .eq(permission_list.contains(&Permissions::UpdateCollection)),
                        has_delete_namespace
                            .eq(permission_list.contains(&Permissions::DeleteCollection)),
                        has_delegate_namespace
                            .eq(permission_list.contains(&Permissions::DelegateCollection)),
                        has_create_class.eq(permission_list.contains(&Permissions::CreateClass)),
                        has_read_class.eq(permission_list.contains(&Permissions::ReadClass)),
                        has_update_class.eq(permission_list.contains(&Permissions::UpdateClass)),
                        has_delete_class.eq(permission_list.contains(&Permissions::DeleteClass)),
                        has_create_object.eq(permission_list.contains(&Permissions::CreateObject)),
                        has_read_object.eq(permission_list.contains(&Permissions::ReadObject)),
                        has_update_object.eq(permission_list.contains(&Permissions::UpdateObject)),
                        has_delete_object.eq(permission_list.contains(&Permissions::DeleteObject)),
                    ))
                    .get_result(conn)?),
                None => {
                    let new_entry = NewPermission {
                        namespace_id: nid,
                        group_id: group_identifier,
                        has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
                        has_update_namespace: permission_list
                            .contains(&Permissions::UpdateCollection),
                        has_delete_namespace: permission_list
                            .contains(&Permissions::DeleteCollection),
                        has_delegate_namespace: permission_list
                            .contains(&Permissions::DelegateCollection),
                        has_create_class: permission_list.contains(&Permissions::CreateClass),
                        has_read_class: permission_list.contains(&Permissions::ReadClass),
                        has_update_class: permission_list.contains(&Permissions::UpdateClass),
                        has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                        has_create_object: permission_list.contains(&Permissions::CreateObject),
                        has_read_object: permission_list.contains(&Permissions::ReadObject),
                        has_update_object: permission_list.contains(&Permissions::UpdateObject),
                        has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                        has_create_class_relation: permission_list
                            .contains(&Permissions::CreateClassRelation),
                        has_read_class_relation: permission_list
                            .contains(&Permissions::ReadClassRelation),
                        has_update_class_relation: permission_list
                            .contains(&Permissions::UpdateClassRelation),
                        has_delete_class_relation: permission_list
                            .contains(&Permissions::DeleteClassRelation),
                        has_create_object_relation: permission_list
                            .contains(&Permissions::CreateObjectRelation),
                        has_read_object_relation: permission_list
                            .contains(&Permissions::ReadObjectRelation),
                        has_update_object_relation: permission_list
                            .contains(&Permissions::UpdateObjectRelation),
                        has_delete_object_relation: permission_list
                            .contains(&Permissions::DeleteObjectRelation),
                    };
                    Ok(diesel::insert_into(permissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
    }

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
    async fn revoke_all(&self, pool: &DbPool, group_id_for_revoke: i32) -> Result<(), ApiError> {
        use crate::schema::permissions::dsl::*;

        let mut conn = pool.get()?;

        diesel::delete(permissions)
            .filter(namespace_id.eq(self.namespace_id(pool).await?))
            .filter(group_id.eq(group_id_for_revoke))
            .execute(&mut conn)?;

        Ok(())
    }
}
