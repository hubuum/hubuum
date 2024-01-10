use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::models::group::GroupID;
use crate::models::namespace::{Namespace, NamespaceID};
use crate::models::user::UserID;

use crate::db::connection::DbPool;

use crate::schema::group_datapermissions;
use crate::schema::group_namespacepermissions;
use crate::schema::namespaces;
use crate::schema::user_datapermissions;
use crate::schema::user_namespacepermissions;

use crate::errors::ApiError;

use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NamespacePermissions {
    Create,
    Read,
    Update,
    Delete,
    Delegate,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataPermissions {
    Create,
    Read,
    Update,
    Delete,
}

#[derive(Serialize, Deserialize)]
pub enum Assignee {
    Group(GroupID),
    User(UserID),
}

#[derive(Serialize, Deserialize)]
pub struct NamespacePermissionAssignment {
    pub assignee: Assignee,
    pub permissions: HashSet<NamespacePermissions>,
}

impl NamespacePermissions {
    pub fn db_field(&self) -> &'static str {
        match self {
            NamespacePermissions::Create => "has_create",
            NamespacePermissions::Read => "has_read",
            NamespacePermissions::Update => "has_update",
            NamespacePermissions::Delete => "has_delete",
            NamespacePermissions::Delegate => "has_delegate",
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DataPermission {
    pub assignee: Assignee,
    pub permissions: HashSet<DataPermissions>,
}

impl DataPermissions {
    pub fn db_field(&self) -> &'static str {
        match self {
            DataPermissions::Create => "has_create",
            DataPermissions::Read => "has_read",
            DataPermissions::Update => "has_update",
            DataPermissions::Delete => "has_delete",
        }
    }
}

// Base permission models.
#[derive(Serialize, Deserialize, Queryable)]
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

#[derive(Serialize, Deserialize, Queryable)]
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

#[derive(Serialize, Deserialize, Queryable)]
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

#[derive(Serialize, Deserialize, Queryable)]
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

// Insertable permission models.
#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = user_namespacepermissions)]
pub struct NewUserNamespacePermission {
    pub namespace_id: i32,
    pub user_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
    pub has_delegate: bool,
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = group_namespacepermissions)]
pub struct NewGroupNamespacePermission {
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
    pub has_delegate: bool,
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = user_datapermissions)]
pub struct NewUserDataPermission {
    pub namespace_id: i32,
    pub user_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = group_datapermissions)]
pub struct NewGroupDataPermission {
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
