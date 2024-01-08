use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::schema::namespacepermissions;
use crate::schema::namespaces;
use crate::schema::objectpermissions;

use crate::errors::{map_error, ApiError};

#[derive(Debug, PartialEq, Eq)]
pub enum PermissionsForNamespaces {
    Create,
    Read,
    Update,
    Delete,
    Delegate,
}

impl PermissionsForNamespaces {
    fn db_field(&self) -> &'static str {
        match self {
            PermissionsForNamespaces::Create => "has_create",
            PermissionsForNamespaces::Read => "has_read",
            PermissionsForNamespaces::Update => "has_update",
            PermissionsForNamespaces::Delete => "has_delete",
            PermissionsForNamespaces::Delegate => "has_delegate",
        }
    }
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = namespaces)]
pub struct Namespace {
    pub id: i32,
    pub name: String,
    pub description: String,
}

impl Namespace {
    pub fn user_can(
        &self,
        pool: &crate::db::connection::DbPool,
        user_id: i32,
        permission_type: PermissionsForNamespaces,
    ) -> Result<bool, ApiError> {
        user_can_on(pool, user_id, permission_type, self.id)
    }

    pub fn save(
        self,
        pool: &crate::db::connection::DbPool,
    ) -> Result<Self, crate::errors::ApiError> {
        use crate::schema::namespaces::dsl::*;

        let mut conn = pool.get()?;
        diesel::insert_into(namespaces)
            .values(&self)
            .get_result::<Namespace>(&mut conn)
            .map_err(|e| map_error(e, "Failed to save namespace"))
    }
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = namespacepermissions)]
pub struct NamespacePermission {
    pub id: i32,
    pub namespace_id: i32,
    pub user_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
    pub has_delegate: bool,
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = objectpermissions)]
pub struct ObjectPermission {
    pub id: i32,
    pub namespace_id: i32,
    pub user_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
}

pub fn user_can_on(
    pool: &crate::db::connection::DbPool,
    user_id: i32,
    permission_type: PermissionsForNamespaces,
    namespace_target_id: i32,
) -> Result<bool, ApiError> {
    use crate::schema::user_groups;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    // Construct the permission field string based on the enum
    let permission_field = permission_type.db_field();

    // Construct the query
    let has_permission: bool = namespacepermissions::table
        // Join with user_groups on group_id, allowing for user_id or group_id to be 0
        .left_outer_join(
            user_groups::table.on(namespacepermissions::group_id.eq(user_groups::group_id)),
        )
        // Filter for the specific namespace
        .filter(namespacepermissions::namespace_id.eq(namespace_target_id))
        // Check for direct user permissions or permissions through group membership
        .filter(
            namespacepermissions::user_id
                .eq(user_id)
                .or(user_groups::user_id
                    .eq(user_id)
                    .and(namespacepermissions::group_id.ne(0))),
        )
        // Check for the specific permission type
        .select(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
            "bool_or({})",
            permission_field
        )))
        // Execute the query
        .first(&mut conn)?;

    Ok(has_permission)
}

pub fn user_can_on_all(
    pool: &crate::db::connection::DbPool,
    user_id: i32,
    permission_type: PermissionsForNamespaces,
) -> Result<Vec<Namespace>, ApiError> {
    use crate::schema::user_groups;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    let permission_field = permission_type.db_field();

    let namespaces_with_permission = namespaces::table
        .inner_join(
            namespacepermissions::table.on(namespaces::id.eq(namespacepermissions::namespace_id)),
        )
        .left_outer_join(
            user_groups::table.on(namespacepermissions::group_id.eq(user_groups::group_id)),
        )
        .filter(
            namespacepermissions::user_id
                .eq(user_id)
                .or(user_groups::user_id
                    .eq(user_id)
                    .and(namespacepermissions::group_id.ne(0))),
        )
        .select(namespaces::all_columns)
        .distinct()
        .load::<Namespace>(&mut conn)?;

    Ok(namespaces_with_permission)
}
