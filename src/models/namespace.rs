use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::models::user::UserID;

use crate::db::DbPool;

use crate::schema::namespaces;

use crate::errors::ApiError;

use crate::models::permissions::NamespacePermissions;
use crate::traits::NamespaceAccessors;

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
        user_can_on(pool, user_id, permission_type, *self).await
    }
}

#[derive(Serialize, Deserialize, Clone, AsChangeset)]
#[diesel(table_name = namespaces)]
pub struct UpdateNamespace {
    pub name: Option<String>,
    pub description: Option<String>,
}

/// A new namespace, with an assignee. Used for creating new namespace entries
/// into the database and assign all permissions to the group given as group_id.
///
/// This wraps the NewNamespace struct and uses the group_id to grant all permissions
/// to the group in a single transaction.
#[derive(Serialize, Deserialize, Clone)]
pub struct NewNamespaceWithAssignee {
    pub name: String,
    pub description: String,
    pub group_id: i32,
}

/// A new namespace, without an assignee. Used for creating new namespace entries
/// into the database.
///
/// Odds are pretty good that you want to use NewNamespaceWithAssignee instead.
#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = namespaces)]
pub struct NewNamespace {
    pub name: String,
    pub description: String,
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
pub async fn user_can_on<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    permission_type: NamespacePermissions,
    namespace_ref: T,
) -> Result<Namespace, ApiError> {
    use crate::models::permissions::{NamespacePermission, PermissionFilter};
    use crate::schema::namespacepermissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let namespace_target_id = namespace_ref.namespace_id(pool).await?;

    let group_ids_subquery = user_id.group_ids_subquery();

    let base_query = namespacepermissions
        .into_boxed()
        .filter(namespace_id.eq(namespace_target_id))
        .filter(group_id.eq_any(group_ids_subquery));

    let result = PermissionFilter::filter(permission_type, base_query)
        .first::<NamespacePermission>(&mut conn)
        .optional()?;

    if result.is_some() {
        return namespace_ref.namespace(pool).await;
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

pub async fn groups_can_on(
    pool: &DbPool,
    nid: i32,
    permission_type: NamespacePermissions,
) -> Result<Vec<crate::models::group::Group>, ApiError> {
    use crate::models::permissions::PermissionFilter;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::namespacepermissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    // Adapted to start with a base query that might include a subquery for group IDs
    let base_query = namespacepermissions
        .into_boxed()
        .filter(namespace_id.eq(nid));

    // Then filter on the given permission type using the PermissionFilter
    let filtered_query = PermissionFilter::filter(permission_type, base_query);

    // Selecting namespace IDs from the filtered query
    let group_ids = filtered_query
        .select(group_id)
        .distinct() // Ensuring distinct group IDs to avoid duplicates
        .load::<i32>(&mut conn)?;

    // Finally, fetching groups based on the obtained group IDs
    let results = if !group_ids.is_empty() {
        groups
            .filter(group_table_id.eq_any(group_ids))
            .load::<crate::models::group::Group>(&mut conn)?
    } else {
        Vec::new() // Returning an empty vector if no group IDs were found
    };

    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::models::group::NewGroup;
    use crate::tests::create_namespace;
    use crate::traits::CanDelete;

    async fn assign_to_groups(
        pool: &DbPool,
        namespace: &Namespace,
        groups: &[crate::models::group::Group],
        permissions: Vec<NamespacePermissions>,
    ) {
        let namespace = namespace.clone();

        for group in groups {
            namespace
                .grant(pool, group.id, permissions.clone())
                .await
                .unwrap();
        }
    }

    async fn groups_can_on_number(
        pool: &DbPool,
        nid: i32,
        permission_type: NamespacePermissions,
        expected_count: i32,
    ) {
        let groups = groups_can_on(pool, nid, permission_type).await.unwrap();
        assert_eq!(groups.len() as i32, expected_count);
    }

    #[actix_rt::test]
    async fn grant_to_nonexistent_group() {
        let (pool, _) = crate::tests::get_pool_and_config().await;

        let namespace = create_namespace(&pool, "grant_to_nonexistent_group")
            .await
            .unwrap();

        // This should return an ApiError::NotFound
        let result = namespace
            .grant(&pool, 99999999, vec![NamespacePermissions::ReadCollection])
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ApiError::NotFound(_)));
    }

    #[actix_rt::test]
    async fn test_list_groups_who_can() {
        let (pool, _) = crate::tests::get_pool_and_config().await;

        let mut groups = Vec::new();
        for group_number in vec![1, 2, 3, 4, 5] {
            let group_name = format!("test_list_group_{}", group_number);
            groups.push(
                NewGroup {
                    groupname: group_name.to_string(),
                    description: Some("Test group".to_string()),
                }
                .save(&pool)
                .await
                .unwrap(),
            );
        }

        // Note, the admin group createed automatically gets all permissions to namespaces created
        // via create_namespace, so we have one extra group for all permissions
        let namespace = create_namespace(&pool, "test_list_groups").await.unwrap();

        // Note: Slicing is *NOT* inclusive, so this will assign to groups 0, 1, and 2
        assign_to_groups(
            &pool,
            &namespace,
            &groups[0..3],
            vec![NamespacePermissions::ReadCollection],
        )
        .await;

        groups_can_on_number(&pool, namespace.id, NamespacePermissions::ReadCollection, 4).await;
        groups_can_on_number(
            &pool,
            namespace.id,
            NamespacePermissions::UpdateCollection,
            1,
        )
        .await;

        assign_to_groups(
            &pool,
            &namespace,
            &groups[2..4],
            vec![
                NamespacePermissions::ReadCollection,
                NamespacePermissions::UpdateCollection,
            ],
        )
        .await;

        groups_can_on_number(&pool, namespace.id, NamespacePermissions::ReadCollection, 5).await;
        groups_can_on_number(
            &pool,
            namespace.id,
            NamespacePermissions::UpdateCollection,
            3,
        )
        .await;

        namespace.delete(&pool).await.unwrap();
        for group in groups {
            group.delete(&pool).await.unwrap();
        }
    }
}
