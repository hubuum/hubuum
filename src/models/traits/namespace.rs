use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::GroupID;
use crate::models::namespace::{
    Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee, UpdateNamespace,
};
use crate::models::permissions::{NewPermission, Permission, Permissions, PermissionsList};
use crate::models::traits::user::GroupAccessors;
use crate::models::user::User;
use crate::traits::{
    CanDelete, CanSave, CanUpdate, NamespaceAccessors, PermissionController, SelfAccessors,
};
use diesel::prelude::*;
use tracing::debug;

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

            let group_permission = NewPermission {
                namespace_id: namespace.id,
                group_id: self.group_id,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
            };

            diesel::insert_into(crate::schema::permissions::table)
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
        use crate::schema::namespaces::dsl::*;
        use crate::schema::permissions::dsl::permissions;

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(&self)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewPermission {
                namespace_id: namespace.id,
                group_id: assignee.0,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }

    pub async fn update_with_permissions(
        self,
        pool: &DbPool,
        ns_with_assignee: NewNamespaceWithAssignee,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::*;
        use crate::schema::permissions::dsl::permissions;

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(&self)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewPermission {
                namespace_id: namespace.id,
                group_id: ns_with_assignee.group_id,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }
}

impl PermissionController for Namespace {}
impl PermissionController for NamespaceID {}
