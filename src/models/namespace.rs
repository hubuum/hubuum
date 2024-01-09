use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::models::user::UserID;

use crate::db::connection::DbPool;

use crate::schema::group_datapermissions;
use crate::schema::group_namespacepermissions;
use crate::schema::namespaces;
use crate::schema::user_datapermissions;
use crate::schema::user_namespacepermissions;

use crate::errors::ApiError;

use crate::models::permissions::{Assignee, NamespacePermissions};

#[derive(Serialize, Deserialize, Queryable)]
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
    pub fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission_type: NamespacePermissions,
    ) -> Result<Self, ApiError> {
        user_can_on(pool, user_id, permission_type, NamespaceID(self.id))
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
    pub fn update(&self, pool: &DbPool, new_data: UpdateNamespace) -> Result<Self, ApiError> {
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
    pub fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::delete(namespaces.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(result)
    }
}

#[derive(Serialize, Deserialize)]
pub struct NamespaceID(pub i32);

impl NamespaceID {
    pub fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission_type: NamespacePermissions,
    ) -> Result<Namespace, ApiError> {
        user_can_on(pool, user_id, permission_type, NamespaceID(self.0))
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
    pub fn validate(&self) -> Result<(), crate::errors::ApiError> {
        match (self.assign_to_user_id, self.assign_to_group_id) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            _ => Err(ApiError::BadRequest(
                "Exactly one of assign_to_user_id or assign_to_group_id must be set".to_string(),
            )),
        }
    }

    pub fn save_and_grant_all(self, pool: &DbPool) -> Result<Namespace, ApiError> {
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
                    let user_permission = UserNamespacePermission {
                        id: 0,
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
                    let group_permission = GroupNamespacePermission {
                        id: 0,
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
    pub fn save_and_grant_all_to(
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
                    let group_permission = GroupNamespacePermission {
                        id: 0,
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
                    let user_permission = UserNamespacePermission {
                        id: 0,
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

    pub fn update_with_permissions(
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

            // Check if permissions are assigned to a user
            if let Some(user_id) = permissions.assign_to_user_id {
                let user_permission = UserNamespacePermission {
                    id: 0,
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

            // Check if permissions are assigned to a group
            if let Some(group_id) = permissions.assign_to_group_id {
                let group_permission = GroupNamespacePermission {
                    id: 0,
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

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = user_namespacepermissions)]
pub struct UserNamespacePermission {
    pub id: i32,
    pub namespace_id: i32,
    pub user_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
    pub has_delegate: bool,
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = group_namespacepermissions)]
pub struct GroupNamespacePermission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
    pub has_delegate: bool,
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = user_datapermissions)]
pub struct UserDataPermission {
    pub id: i32,
    pub namespace_id: i32,
    pub user_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = group_datapermissions)]
pub struct GroupDataPermission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
}

/// Check if a user has a specific permission to a given namespace ID
///
/// ## Arguments
///
/// * pool - Database connection pool
/// * user_id - ID of the user to check permissions for
/// * permission_type - Type of permission to check
/// * namespace_target_id - ID of the namespace to check permissions for
///
/// ## Returns
/// * Ok(Namespace) - Namespace if the user has the requested permission
/// * Err(ApiError) - Always returns 404 if there is no match (we never do 403/401)
pub fn user_can_on(
    pool: &DbPool,
    user_id: UserID,
    permission_type: NamespacePermissions,
    namespace_target: NamespaceID,
) -> Result<Namespace, ApiError> {
    use crate::schema::user_groups;
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let permission_field = permission_type.db_field();
    let namespace_target_id = namespace_target.0;
    let uid = user_id.0;

    // Check direct user permissions
    let user_permission: bool = user_namespacepermissions::table
        .filter(user_namespacepermissions::namespace_id.eq(namespace_target_id))
        .filter(user_namespacepermissions::user_id.eq(uid))
        .select(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
            "bool_or({})",
            permission_field
        )))
        .first(&mut conn)
        .unwrap_or(false);

    // If user has direct permissions, return the namespace
    if user_permission {
        let namespace = namespaces::table
            .filter(namespaces::id.eq(namespace_target_id))
            .first(&mut conn)?;
        return Ok(namespace);
    }

    // Check group permissions only if direct user permission check failed
    let group_permission: bool = group_namespacepermissions::table
        .inner_join(
            user_groups::table.on(group_namespacepermissions::group_id.eq(user_groups::group_id)),
        )
        .filter(group_namespacepermissions::namespace_id.eq(namespace_target_id))
        .filter(user_groups::user_id.eq(uid))
        .select(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
            "bool_or({})",
            permission_field
        )))
        .first(&mut conn)
        .unwrap_or(false);

    if group_permission {
        let namespace = namespaces::table
            .filter(namespaces::id.eq(namespace_target_id))
            .first(&mut conn)?;
        return Ok(namespace);
    }

    // If neither direct nor group permissions are found, return an error
    Err(ApiError::NotFound("Not found".to_string()))
}

/// Check if a user has a specific permission to any namespace
///
/// ## Arguments
/// * pool - Database connection pool
/// * user_id - ID of the user to check permissions for
/// * permission_type - Type of permission to check
///
/// ## Returns
/// * Ok(Vec<Namespace>) - List of namespaces the user has the requested permission for.
///                        If no matching namespaces are found, an empty list is returned
/// * Err(ApiError) - On query errors only.
pub fn user_can_on_any(
    pool: &DbPool,
    user_id: UserID,
    permission_type: NamespacePermissions,
) -> Result<Vec<Namespace>, ApiError> {
    use crate::schema::user_groups;
    use diesel::prelude::*;
    use std::collections::HashSet;

    let mut conn = pool.get()?;
    let permission_field = permission_type.db_field();

    let uid = user_id.0;

    // Subquery for direct user permissions
    let direct_user_permissions = user_namespacepermissions::table
        .filter(user_namespacepermissions::user_id.eq(uid))
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
            "{} = true",
            permission_field
        )))
        .select(user_namespacepermissions::namespace_id);

    // Subquery for group permissions
    let group_permissions = group_namespacepermissions::table
        .inner_join(
            user_groups::table.on(group_namespacepermissions::group_id.eq(user_groups::group_id)),
        )
        .filter(user_groups::user_id.eq(uid))
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
            "{} = true",
            permission_field
        )))
        .select(group_namespacepermissions::namespace_id);

    // Fetch IDs for both queries
    let mut user_namespace_ids: Vec<i32> = direct_user_permissions.load(&mut conn)?;
    let mut group_namespace_ids: Vec<i32> = group_permissions.load(&mut conn)?;

    // Combine and deduplicate namespace IDs in Rust, this is mostly to avoid issues
    // with the query planner and .distinct() and typing not playing ball.
    user_namespace_ids.append(&mut group_namespace_ids);
    let unique_namespace_ids: HashSet<i32> = user_namespace_ids.into_iter().collect();

    // Fetch the namespaces
    let accessible_namespaces = namespaces::table
        .filter(namespaces::id.eq_any(unique_namespace_ids))
        .load::<Namespace>(&mut conn)?;

    if accessible_namespaces.is_empty() {
        return Ok(vec![]);
    }

    Ok(accessible_namespaces)
}
