use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::GroupID;
use crate::models::namespace::{
    Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee, UpdateNamespace,
};
use crate::models::permissions::{
    NamespacePermission, NamespacePermissions, NewNamespacePermission,
};
use crate::traits::{CanDelete, CanSave, CanUpdate, NamespaceAccessors, SelfAccessors};
use diesel::prelude::*;

impl CanSave for Namespace {
    type Output = Namespace;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_namespace = UpdateNamespace {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        updated_namespace.update(pool, self.id).await
    }
}

impl CanDelete for Namespace {
    /// Delete a namespace
    ///
    /// This does not check for permissions, it only deletes the namespace.
    /// It is assumed that permissions are already checked before calling this method.
    /// See `user_can` for permission checking.
    ///
    /// Note: This will also delete all objects and classes in the namespace, as well
    /// as all permissions related to the namespace.
    ///
    /// ## Arguments
    /// * pool - Database connection pool
    ///
    /// ## Returns
    /// * Ok(usize) - Number of deleted namespaces
    /// * Err(ApiError) - On query errors only.
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(namespaces.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanUpdate for UpdateNamespace {
    type Output = Namespace;

    /// Update a namespace
    ///
    /// This does not check for permissions, it only updates the namespace.
    /// It is assumed that permissions are already checked before calling this method.
    /// See `user_can` for permission checking.
    ///
    /// ## Arguments
    /// * pool - Database connection pool
    /// * new_data - New data to update the namespace with
    ///
    /// ## Returns
    /// * Ok(Namespace) - Updated namespace
    /// * Err(ApiError) - On query errors only.
    async fn update(&self, pool: &DbPool, nid: i32) -> Result<Self::Output, ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        let namespace = diesel::update(namespaces)
            .filter(id.eq(nid))
            .set(self)
            .get_result::<Namespace>(&mut conn)?;

        Ok(namespace)
    }
}

impl CanSave for NewNamespaceWithAssignee {
    type Output = Namespace;

    async fn save(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        let new_namespace = NewNamespace {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            // Insert the new namespace
            let namespace = diesel::insert_into(crate::schema::namespaces::table)
                .values(&new_namespace)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewNamespacePermission {
                namespace_id: namespace.id,
                group_id: self.group_id,
                has_create_object: true,
                has_create_class: true,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
            };

            diesel::insert_into(crate::schema::namespacepermissions::table)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }
}

impl SelfAccessors<Namespace> for Namespace {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAccessors for Namespace {
    async fn namespace(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }
}

impl SelfAccessors<Namespace> for NamespaceID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.namespace(pool).await
    }
}

impl NamespaceAccessors for NamespaceID {
    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }

    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let mut conn = pool.get()?;
        let namespace = namespaces
            .filter(id.eq(self.0))
            .first::<Namespace>(&mut conn)?;

        Ok(namespace)
    }
}

impl NewNamespace {
    pub async fn save_and_grant_all_to(
        self,
        pool: &DbPool,
        assignee: GroupID,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespacepermissions::dsl::namespacepermissions;
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(&self)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewNamespacePermission {
                namespace_id: namespace.id,
                group_id: assignee.0,
                has_create_object: true,
                has_create_class: true,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
            };

            diesel::insert_into(namespacepermissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }

    pub async fn update_with_permissions(
        self,
        pool: &DbPool,
        permissions: NewNamespaceWithAssignee,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespacepermissions::dsl::namespacepermissions;
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(&self)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewNamespacePermission {
                namespace_id: namespace.id,
                group_id: permissions.group_id,
                has_create_object: true,
                has_create_class: true,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
            };

            diesel::insert_into(namespacepermissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }
}

// Grants
impl Namespace {
    /// Grant permissions to a group on a namespace
    /// This only grants the permissions that are passed in the permissions vector.
    /// If the group previously had no permissions, a new entry is created.
    /// If the group already has permissions, the permissions are updated.
    pub async fn grant(
        &self,
        pool: &DbPool,
        group_id_for_grant: i32,
        permissions: Vec<NamespacePermissions>,
    ) -> Result<NamespacePermission, ApiError> {
        use crate::models::permissions::UpdateNamespacePermission;
        use crate::schema::namespacepermissions::dsl::*;
        use diesel::prelude::*;

        // If the group already has permissions, update the permissions in permissions. Otherwise, insert a new row.
        let mut conn = pool.get()?;

        conn.transaction::<_, ApiError, _>(|conn| {
            let existing_entry = namespacepermissions
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_grant))
                .first::<NamespacePermission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => {
                    let mut update_permissions = UpdateNamespacePermission::default();
                    for permission in permissions {
                        match permission {
                            NamespacePermissions::CreateObject => {
                                update_permissions.has_create_object = Some(true);
                            }
                            NamespacePermissions::CreateClass => {
                                update_permissions.has_create_class = Some(true);
                            }
                            NamespacePermissions::ReadCollection => {
                                update_permissions.has_read_namespace = Some(true);
                            }
                            NamespacePermissions::UpdateCollection => {
                                update_permissions.has_update_namespace = Some(true);
                            }
                            NamespacePermissions::DeleteCollection => {
                                update_permissions.has_delete_namespace = Some(true);
                            }
                            NamespacePermissions::DelegateCollection => {
                                update_permissions.has_delegate_namespace = Some(true);
                            }
                        }
                    }

                    Ok(diesel::update(namespacepermissions)
                        .filter(namespace_id.eq(self.id))
                        .filter(group_id.eq(group_id_for_grant))
                        .set(&update_permissions)
                        .get_result(conn)?)
                }
                None => {
                    let new_entry = NewNamespacePermission {
                        namespace_id: self.id,
                        group_id: group_id_for_grant,
                        has_create_object: permissions
                            .contains(&NamespacePermissions::CreateObject),
                        has_create_class: permissions.contains(&NamespacePermissions::CreateClass),
                        has_read_namespace: permissions
                            .contains(&NamespacePermissions::ReadCollection),
                        has_update_namespace: permissions
                            .contains(&NamespacePermissions::UpdateCollection),
                        has_delete_namespace: permissions
                            .contains(&NamespacePermissions::DeleteCollection),
                        has_delegate_namespace: permissions
                            .contains(&NamespacePermissions::DelegateCollection),
                    };
                    Ok(diesel::insert_into(namespacepermissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
    }

    // Revoke permissions from a group on a namespace
    // This only revokes the permissions that are passed in the permissions vector.
    pub async fn revoke(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        permissions: Vec<NamespacePermissions>,
    ) -> Result<NamespacePermission, ApiError> {
        use crate::models::permissions::UpdateNamespacePermission;
        use crate::schema::namespacepermissions::dsl::*;
        use diesel::prelude::*;

        let mut conn = pool.get()?;

        conn.transaction::<_, ApiError, _>(|conn| {
            namespacepermissions
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<NamespacePermission>(conn)?;

            let mut update_permissions = UpdateNamespacePermission::default();
            for permission in permissions {
                match permission {
                    NamespacePermissions::CreateObject => {
                        update_permissions.has_create_object = Some(false);
                    }
                    NamespacePermissions::CreateClass => {
                        update_permissions.has_create_class = Some(false);
                    }
                    NamespacePermissions::ReadCollection => {
                        update_permissions.has_read_namespace = Some(false);
                    }
                    NamespacePermissions::UpdateCollection => {
                        update_permissions.has_update_namespace = Some(false);
                    }
                    NamespacePermissions::DeleteCollection => {
                        update_permissions.has_delete_namespace = Some(false);
                    }
                    NamespacePermissions::DelegateCollection => {
                        update_permissions.has_delegate_namespace = Some(false);
                    }
                }
            }
            Ok(diesel::update(namespacepermissions)
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_revoke))
                .set(&update_permissions)
                .get_result(conn)?)
        })
    }

    pub async fn set_permissions(
        &self,
        pool: &DbPool,
        group_id_for_set: i32,
        permissions: Vec<NamespacePermissions>,
    ) -> Result<NamespacePermission, ApiError> {
        use crate::schema::namespacepermissions::dsl::*;
        use diesel::prelude::*;

        let mut conn = pool.get()?;

        conn.transaction::<_, ApiError, _>(|conn| {
            let existing_entry = namespacepermissions
                .filter(namespace_id.eq(self.id))
                .filter(group_id.eq(group_id_for_set))
                .first::<NamespacePermission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => Ok(diesel::update(namespacepermissions)
                    .filter(namespace_id.eq(self.id))
                    .filter(group_id.eq(group_id_for_set))
                    .set((
                        has_create_object
                            .eq(permissions.contains(&NamespacePermissions::CreateObject)),
                        has_create_class
                            .eq(permissions.contains(&NamespacePermissions::CreateClass)),
                        has_read_namespace
                            .eq(permissions.contains(&NamespacePermissions::ReadCollection)),
                        has_update_namespace
                            .eq(permissions.contains(&NamespacePermissions::UpdateCollection)),
                        has_delete_namespace
                            .eq(permissions.contains(&NamespacePermissions::DeleteCollection)),
                        has_delegate_namespace
                            .eq(permissions.contains(&NamespacePermissions::DelegateCollection)),
                    ))
                    .get_result(conn)?),
                None => {
                    let new_entry = NewNamespacePermission {
                        namespace_id: self.id,
                        group_id: group_id_for_set,
                        has_create_object: permissions
                            .contains(&NamespacePermissions::CreateObject),
                        has_create_class: permissions.contains(&NamespacePermissions::CreateClass),
                        has_read_namespace: permissions
                            .contains(&NamespacePermissions::ReadCollection),
                        has_update_namespace: permissions
                            .contains(&NamespacePermissions::UpdateCollection),
                        has_delete_namespace: permissions
                            .contains(&NamespacePermissions::DeleteCollection),
                        has_delegate_namespace: permissions
                            .contains(&NamespacePermissions::DelegateCollection),
                    };
                    Ok(diesel::insert_into(namespacepermissions)
                        .values(&new_entry)
                        .get_result(conn)?)
                }
            }
        })
    }

    pub async fn revoke_all(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
    ) -> Result<(), ApiError> {
        use crate::schema::namespacepermissions::dsl::*;
        use diesel::prelude::*;

        let mut conn = pool.get()?;

        diesel::delete(namespacepermissions)
            .filter(namespace_id.eq(self.id))
            .filter(group_id.eq(group_id_for_revoke))
            .execute(&mut conn)?;

        Ok(())
    }
}
