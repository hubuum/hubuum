use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::models::user::UserID;

use crate::db::connection::DbPool;

use crate::schema::group_namespacepermissions;
use crate::schema::namespaces;
use crate::schema::user_namespacepermissions;

use crate::models::permissions::{
    user_can_on, NewGroupNamespacePermission, NewUserNamespacePermission,
};

use crate::errors::ApiError;

use crate::models::permissions::{Assignee, NamespacePermissions};

#[derive(Serialize, Deserialize, Queryable, PartialEq)]
#[diesel(table_name = namespaces)]
pub struct Namespace {
    pub id: i32,
    pub name: String,
    pub description: String,
}

impl Namespace {
    /// Check if a user has a specific permission to this namespace
    ///
    /// ## Arguments
    /// * pool - Database connection pool
    /// * user_id - ID of the user to check permissions for
    /// * permission_type - Type of permission to check
    ///
    /// ## Returns
    /// * Ok(Namespace) - Namespace if the user has the requested permission
    /// * Err(ApiError) - Always returns 404 if there is no match (we never do 403/401)
    pub async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission_type: NamespacePermissions,
    ) -> Result<Self, ApiError> {
        user_can_on(pool, user_id, permission_type, NamespaceID(self.id)).await
    }

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
    pub async fn update(&self, pool: &DbPool, new_data: UpdateNamespace) -> Result<Self, ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        let namespace = diesel::update(namespaces.filter(id.eq(self.id)))
            .set(&new_data)
            .get_result::<Namespace>(&mut conn)?;

        Ok(namespace)
    }

    /// Delete a namespace
    ///
    /// This does not check for permissions, it only deletes the namespace.
    /// It is assumed that permissions are already checked before calling this method.
    /// See `user_can` for permission checking.
    ///
    /// ## Arguments
    /// * pool - Database connection pool
    ///
    /// ## Returns
    /// * Ok(usize) - Number of deleted namespaces
    /// * Err(ApiError) - On query errors only.
    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::delete(namespaces.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(result)
    }
}

#[derive(Serialize, Deserialize)]
pub struct NamespaceID(pub i32);

impl NamespaceID {
    pub async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission_type: NamespacePermissions,
    ) -> Result<Namespace, ApiError> {
        user_can_on(pool, user_id, permission_type, NamespaceID(self.0)).await
    }
}

#[derive(Serialize, Deserialize, AsChangeset)]
#[diesel(table_name = namespaces)]
pub struct UpdateNamespace {
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NewNamespaceRequest {
    pub name: String,
    pub description: String,
    pub assign_to_user_id: Option<i32>,
    pub assign_to_group_id: Option<i32>,
}

impl NewNamespaceRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        match (self.assign_to_user_id, self.assign_to_group_id) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            _ => Err(ApiError::BadRequest(
                "Exactly one of assign_to_user_id or assign_to_group_id must be set".to_string(),
            )),
        }
    }

    pub async fn save_and_grant_all(self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.validate()?;

        let new_namespace = NewNamespace {
            name: self.name,
            description: self.description,
        };

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            // Insert the new namespace
            let namespace = diesel::insert_into(crate::schema::namespaces::table)
                .values(&new_namespace)
                .get_result::<Namespace>(conn)?;

            // Grant all permissions to the user or group
            match (self.assign_to_user_id, self.assign_to_group_id) {
                (Some(user_id), None) => {
                    let user_permission = NewUserNamespacePermission {
                        namespace_id: namespace.id,
                        user_id,
                        has_create: true,
                        has_read: true,
                        has_update: true,
                        has_delete: true,
                        has_delegate: true,
                    };
                    diesel::insert_into(crate::schema::user_namespacepermissions::table)
                        .values(&user_permission)
                        .execute(conn)?;
                }
                (None, Some(group_id)) => {
                    let group_permission = NewGroupNamespacePermission {
                        namespace_id: namespace.id,
                        group_id,
                        has_create: true,
                        has_read: true,
                        has_update: true,
                        has_delete: true,
                        has_delegate: true,
                    };
                    diesel::insert_into(crate::schema::group_namespacepermissions::table)
                        .values(&group_permission)
                        .execute(conn)?;
                }
                _ => return Err(ApiError::BadRequest("Invalid assignee".to_string())),
            }

            Ok(namespace)
        })
    }
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = namespaces)]
pub struct NewNamespace {
    pub name: String,
    pub description: String,
}

impl NewNamespace {
    pub async fn save_and_grant_all_to(
        self,
        pool: &DbPool,
        assignee: Assignee,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(&self)
                .get_result::<Namespace>(conn)?;

            match assignee {
                Assignee::Group(group_id) => {
                    let group_permission = NewGroupNamespacePermission {
                        namespace_id: namespace.id,
                        group_id: group_id.0,
                        has_create: true,
                        has_read: true,
                        has_update: true,
                        has_delete: true,
                        has_delegate: true,
                    };

                    diesel::insert_into(group_namespacepermissions::table)
                        .values(&group_permission)
                        .execute(conn)?;
                }
                Assignee::User(user_id) => {
                    let user_permission = NewUserNamespacePermission {
                        namespace_id: namespace.id,
                        user_id: user_id.0,
                        has_create: true,
                        has_read: true,
                        has_update: true,
                        has_delete: true,
                        has_delegate: true,
                    };

                    diesel::insert_into(user_namespacepermissions::table)
                        .values(&user_permission)
                        .execute(conn)?;
                }
            }

            Ok(namespace)
        })
    }

    pub async fn update_with_permissions(
        self,
        pool: &DbPool,
        permissions: NewNamespaceRequest,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::*;

        permissions.validate()?;

        let mut conn = pool.get()?;
        conn.transaction::<_, ApiError, _>(|conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(&self)
                .get_result::<Namespace>(conn)?;

            if let Some(user_id) = permissions.assign_to_user_id {
                let user_permission = NewUserNamespacePermission {
                    namespace_id: namespace.id,
                    user_id,
                    has_create: true,
                    has_read: true,
                    has_update: true,
                    has_delete: true,
                    has_delegate: true,
                };

                diesel::insert_into(user_namespacepermissions::table)
                    .values(&user_permission)
                    .execute(conn)?;
            }

            if let Some(group_id) = permissions.assign_to_group_id {
                let group_permission = NewGroupNamespacePermission {
                    namespace_id: namespace.id,
                    group_id,
                    has_create: true,
                    has_read: true,
                    has_update: true,
                    has_delete: true,
                    has_delegate: true,
                };

                diesel::insert_into(group_namespacepermissions::table)
                    .values(&group_permission)
                    .execute(conn)?;
            }

            Ok(namespace)
        })
    }
}
