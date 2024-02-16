use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::traits::user::GroupAccessors;

use crate::models::class::HubuumClass;
use crate::models::namespace::Namespace;
use crate::models::object::{HubuumObject, HubuumObjectID, NewHubuumObject, UpdateHubuumObject};
use crate::models::permissions::{
    NewObjectPermission, ObjectPermission, ObjectPermissions, PermissionsList,
};
use crate::models::user::UserID;
use crate::traits::{
    CanDelete, CanSave, CanUpdate, ClassAccessors, NamespaceAccessors, ObjectAccessors,
    PermissionInterface, SelfAccessors,
};
use diesel::prelude::*;

//
// Save/Update/Delete
//
impl CanSave for HubuumObject {
    type Output = HubuumObject;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_object = UpdateHubuumObject {
            name: Some(self.name.clone()),
            namespace_id: Some(self.namespace_id),
            hubuum_class_id: Some(self.hubuum_class_id),
            data: Some(self.data.clone()),
            description: Some(self.description.clone()),
        };
        updated_object.update(pool, self.id).await
    }
}

impl CanSave for NewHubuumObject {
    type Output = HubuumObject;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumobject)
            .values(self)
            .get_result::<Self::Output>(&mut conn)?;

        Ok(result)
    }
}

impl CanUpdate for UpdateHubuumObject {
    type Output = HubuumObject;

    async fn update(&self, pool: &DbPool, object_id: i32) -> Result<Self::Output, ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::update(hubuumobject)
            .filter(id.eq(object_id))
            .set(self)
            .get_result::<Self::Output>(&mut conn)?;

        Ok(result)
    }
}

impl CanDelete for HubuumObject {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumobject.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

//
// Accessors
//
impl SelfAccessors<HubuumObject> for HubuumObject {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAccessors for HubuumObject {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let mut conn = pool.get()?;
        let namespace = namespaces
            .filter(id.eq(self.namespace_id))
            .first::<Namespace>(&mut conn)?;

        Ok(namespace)
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace_id)
    }
}

impl ClassAccessors for HubuumObject {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.hubuum_class_id))
            .first::<HubuumClass>(&mut conn)?;

        Ok(class)
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.hubuum_class_id)
    }
}

impl ObjectAccessors for HubuumObject {
    async fn object(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }

    async fn object_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }
}

impl SelfAccessors<HubuumObject> for HubuumObjectID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        Ok(object)
    }
}

impl NamespaceAccessors for HubuumObjectID {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

impl ClassAccessors for HubuumObjectID {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.class(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.class(pool).await?.id)
    }
}

impl ObjectAccessors for HubuumObjectID {
    async fn object(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        self.instance(pool).await
    }

    async fn object_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }
}

impl PermissionInterface for HubuumObject {
    type PermissionType = ObjectPermission;
    type PermissionEnum = ObjectPermissions;

    /// Check if the user has the given permission on this object.
    ///
    /// If this is called on a HubuumObjectID, a full HubuumObject is created to extract
    /// the namespace_id. To avoid creating the HubuumObject multiple times during use
    /// do this:
    /// ```
    /// obj = obj_id.Object(pool).await?;
    /// if (obj.user_can(pool, userid, ObjectPermissions::ReadObject).await?) {
    ///     return Ok(obj);
    /// }
    /// ```
    /// And not this:
    /// ```
    /// if (obj_id.user_can(pool, userid, ObjectPermissions::ReadObject).await?) {
    ///    return Ok(obj_id.Object(pool).await?);
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
    /// * `Ok(true)` if the user has the given permission on this object.
    /// * `Ok(false)` if the user does not have the given permission on this object.
    /// * `Err(_)` if the user does not have the given permission on this object, or if the
    ///  permission is invalid.
    ///
    /// ## Example
    ///
    /// ```
    /// if (hubuum_object_or_objectid.user_can(pool, userid, ObjectPermissions::ReadObject).await?) {
    ///     // Do something
    /// }    
    async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission: Self::PermissionEnum,
    ) -> Result<bool, ApiError> {
        use crate::models::permissions::PermissionFilter;
        use crate::schema::objectpermissions::dsl::*;

        let mut conn = pool.get()?;
        let group_id_subquery = user_id.group_ids_subquery();

        // Note that self.namespace_id(pool).await? is only a query if the caller is a HubuumObjectID, otherwise
        // it's a simple field access (which ignores the passed pool).
        let base_query = objectpermissions
            .into_boxed()
            .filter(namespace_id.eq(self.namespace_id(pool).await?))
            .filter(group_id.eq_any(group_id_subquery));

        let result = PermissionFilter::filter(permission, base_query)
            .first::<ObjectPermission>(&mut conn)
            .optional()?;

        Ok(result.is_some())
    }

    async fn grant(
        &self,
        pool: &DbPool,
        group_id_for_grant: i32,
        permissions: PermissionsList<Self::PermissionEnum>,
    ) -> Result<Self::PermissionType, ApiError> {
        use crate::models::permissions::UpdateObjectPermission;
        use crate::schema::objectpermissions::dsl::*;
        use diesel::prelude::*;

        // If the group already has permissions, update the permissions in permissions. Otherwise, insert a new row.
        let mut conn = pool.get()?;

        conn.transaction::<_, ApiError, _>(|conn| {
            let existing_entry = objectpermissions
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_grant))
                .first::<ObjectPermission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => {
                    let mut update_permissions = UpdateObjectPermission::default();
                    for permission in permissions.into_iter() {
                        match permission {
                            ObjectPermissions::ReadObject => {
                                update_permissions.has_read_object = Some(true);
                            }
                            ObjectPermissions::UpdateObject => {
                                update_permissions.has_update_object = Some(true);
                            }
                            ObjectPermissions::DeleteObject => {
                                update_permissions.has_delete_object = Some(true);
                            }
                        }
                    }

                    Ok(diesel::update(objectpermissions)
                        .filter(namespace_id.eq(self.id))
                        .filter(group_id.eq(group_id_for_grant))
                        .set(&update_permissions)
                        .get_result(conn)?)
                }
                None => {
                    let new_entry = NewObjectPermission {
                        namespace_id: self.id,
                        group_id: group_id_for_grant,
                        has_read_object: permissions.contains(&ObjectPermissions::ReadObject),
                        has_update_object: permissions.contains(&ObjectPermissions::UpdateObject),
                        has_delete_object: permissions.contains(&ObjectPermissions::DeleteObject),
                    };
                    Ok(diesel::insert_into(objectpermissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
    }

    // Revoke permissions from a group on a namespace
    // This only revokes the permissions that are passed in the permissions vector.
    async fn revoke(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        permissions: PermissionsList<Self::PermissionEnum>,
    ) -> Result<Self::PermissionType, ApiError> {
        use crate::models::permissions::UpdateObjectPermission;
        use crate::schema::objectpermissions::dsl::*;
        use diesel::prelude::*;

        let mut conn = pool.get()?;

        conn.transaction::<_, ApiError, _>(|conn| {
            objectpermissions
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<ObjectPermission>(conn)?;

            let mut update_permissions = UpdateObjectPermission::default();
            for permission in permissions.into_iter() {
                match permission {
                    ObjectPermissions::ReadObject => {
                        update_permissions.has_read_object = Some(true);
                    }
                    ObjectPermissions::UpdateObject => {
                        update_permissions.has_update_object = Some(true);
                    }
                    ObjectPermissions::DeleteObject => {
                        update_permissions.has_delete_object = Some(true);
                    }
                }
            }
            Ok(diesel::update(objectpermissions)
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_revoke))
                .set(&update_permissions)
                .get_result(conn)?)
        })
    }

    async fn set_permissions(
        &self,
        pool: &DbPool,
        group_id_for_set: i32,
        permissions: PermissionsList<Self::PermissionEnum>,
    ) -> Result<Self::PermissionType, ApiError> {
        use crate::schema::objectpermissions::dsl::*;
        use diesel::prelude::*;

        let mut conn = pool.get()?;

        conn.transaction::<_, ApiError, _>(|conn| {
            let existing_entry = objectpermissions
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_set))
                .first::<ObjectPermission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => Ok(diesel::update(objectpermissions)
                    .filter(namespace_id.eq(self.id))
                    .filter(group_id.eq(group_id_for_set))
                    .set((
                        has_read_object.eq(permissions.contains(&ObjectPermissions::ReadObject)),
                        has_delete_object
                            .eq(permissions.contains(&ObjectPermissions::DeleteObject)),
                        has_update_object
                            .eq(permissions.contains(&ObjectPermissions::UpdateObject)),
                    ))
                    .get_result(conn)?),
                None => {
                    let new_entry = NewObjectPermission {
                        namespace_id: self.id,
                        group_id: group_id_for_set,
                        has_read_object: permissions.contains(&ObjectPermissions::ReadObject),
                        has_update_object: permissions.contains(&ObjectPermissions::UpdateObject),
                        has_delete_object: permissions.contains(&ObjectPermissions::DeleteObject),
                    };
                    Ok(diesel::insert_into(objectpermissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
    }

    async fn revoke_all(&self, pool: &DbPool, group_id_for_revoke: i32) -> Result<(), ApiError> {
        use crate::schema::objectpermissions::dsl::*;
        use diesel::prelude::*;

        let mut conn = pool.get()?;

        diesel::delete(objectpermissions)
            .filter(namespace_id.eq(self.id))
            .filter(group_id.eq(group_id_for_revoke))
            .execute(&mut conn)?;

        Ok(())
    }
}
