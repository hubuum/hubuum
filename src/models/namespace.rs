use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::models::user::UserID;

use crate::db::DbPool;

use crate::schema::namespacepermissions;
use crate::schema::namespaces;

use crate::models::permissions::NewNamespacePermission;

use crate::errors::ApiError;

use crate::models::permissions::NamespacePermissions;

use super::group::GroupID;

#[derive(Serialize, Deserialize, Queryable, PartialEq, Debug, Clone)]
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
        user_can_on(pool, user_id, permission_type, self.clone()).await
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

#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct NamespaceID(pub i32);

impl NamespaceID {
    pub async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission_type: NamespacePermissions,
    ) -> Result<Namespace, ApiError> {
        user_can_on(pool, user_id, permission_type, self.clone()).await
    }
}

pub trait NamespaceGenerics {
    fn id(&self) -> i32;
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
}

impl NamespaceGenerics for Namespace {
    fn id(&self) -> i32 {
        self.id
    }

    async fn namespace(&self, _: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceGenerics for NamespaceID {
    fn id(&self) -> i32 {
        self.0
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
    pub group_id: i32,
}

impl NewNamespaceRequest {
    pub async fn save_and_grant_all(self, pool: &DbPool) -> Result<Namespace, ApiError> {
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
        assignee: GroupID,
    ) -> Result<Namespace, ApiError> {
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

            diesel::insert_into(namespacepermissions::table)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }

    pub async fn update_with_permissions(
        self,
        pool: &DbPool,
        permissions: NewNamespaceRequest,
    ) -> Result<Namespace, ApiError> {
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

            diesel::insert_into(namespacepermissions::table)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
    }
}

/// Check if a user has a specific permission to a given namespace ID
///
/// ## Arguments
///
/// * pool - Database connection pool
/// * user_id - ID of the user to check permissions for
/// * permission_type - Type of permission to check
/// * namespace_ref - Namespace or NamespaceID to check permissions for
///
/// ## Returns
/// * Ok(Namespace) - Namespace if the user has the requested permission
/// * Err(ApiError) - Always returns 404 if there is no match (we never do 403/401)
pub async fn user_can_on<T: NamespaceGenerics>(
    pool: &DbPool,
    user_id: UserID,
    permission_type: NamespacePermissions,
    namespace_ref: T,
) -> Result<Namespace, ApiError> {
    use crate::models::permissions::{NamespacePermission, PermissionFilter};
    use crate::schema::namespacepermissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let namespace_target_id = namespace_ref.id();

    let group_ids_subquery = user_id.group_ids_subquery();

    let base_query = namespacepermissions
        .into_boxed()
        .filter(namespace_id.eq(namespace_target_id))
        .filter(group_id.eq_any(group_ids_subquery));

    let result = PermissionFilter::filter(permission_type, base_query)
        .first::<NamespacePermission>(&mut conn)
        .optional()?;

    if let Some(_) = result {
        return Ok(namespace_ref.namespace(pool).await?);
    }

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
pub async fn user_can_on_any(
    pool: &DbPool,
    user_id: UserID,
    permission_type: NamespacePermissions,
) -> Result<Vec<Namespace>, ApiError> {
    use crate::models::permissions::PermissionFilter;

    use crate::schema::namespacepermissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    let group_ids_subquery = user_id.group_ids_subquery();

    let base_query = namespacepermissions
        .into_boxed()
        .filter(group_id.eq_any(group_ids_subquery));

    let filtered_query = PermissionFilter::filter(permission_type, base_query);

    let accessible_namespace_ids = filtered_query.select(namespace_id).load::<i32>(&mut conn)?;

    let accessible_namespaces = if !accessible_namespace_ids.is_empty() {
        namespaces::table
            .filter(namespaces::id.eq_any(accessible_namespace_ids))
            .load::<Namespace>(&mut conn)?
    } else {
        vec![]
    };

    Ok(accessible_namespaces)
}
