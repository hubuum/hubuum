use diesel::prelude::*;

use diesel::pg::Pg;
use diesel::sql_types::{Integer, Text, Timestamp};

use serde::{Deserialize, Serialize};

use crate::models::group::Group;
use crate::models::user::{User, UserID};

use crate::db::DbPool;

use crate::schema::namespaces;

use crate::errors::ApiError;

use crate::models::output::GroupPermission;
use crate::models::{Permission, Permissions};

use crate::models::traits::GroupAccessors;
use crate::traits::{NamespaceAccessors, SelfAccessors};

use tracing::info;

use super::PermissionsList;

#[derive(Serialize, Deserialize, Queryable, PartialEq, Debug, Clone, Selectable)]
#[diesel(table_name = namespaces)]
pub struct Namespace {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone)]
pub struct NamespaceID(pub i32);

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

/// Check what permissions a user has to a given namespace
///
/// ## Arguments
/// * pool - Database connection pool
/// * user_id - ID of the user to check permissions for
/// * namespace_ref - Namespace or NamespaceID to check permissions for
///
/// ## Returns
/// * Ok(Vec(Group, NamespacePermissions)) - List of groups and their permissions
/// * Err(ApiError) - On query errors only.
pub async fn user_on<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{group_id, namespace_id, permissions};
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let namespace_target_id = namespace_ref.namespace_id(pool).await?;

    let group_ids_subquery = user_id.group_ids_subquery();

    let query = groups
        .inner_join(permissions.on(group_table_id.eq(group_id)))
        .filter(namespace_id.eq(namespace_target_id))
        .filter(group_id.eq_any(group_ids_subquery))
        .select((groups::all_columns(), permissions::all_columns()))
        .load::<(Group, Permission)>(&mut conn)?;

    let structured_results: Vec<GroupPermission> =
        query.into_iter().map(GroupPermission::from_tuple).collect();

    Ok(structured_results)
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
///   If no matching namespaces are found, an empty list is returned
/// * Err(ApiError) - On query errors only.
pub async fn user_can_on_any<U: SelfAccessors<User> + GroupAccessors>(
    pool: &DbPool,
    user_id: U,
    permission_type: Permissions,
) -> Result<Vec<Namespace>, ApiError> {
    use crate::models::permissions::PermissionFilter;

    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    let base_query = if user_id.instance(pool).await?.is_admin(pool).await {
        permissions.into_boxed()
    } else {
        let group_ids_subquery = user_id.group_ids_subquery();

        permissions
            .into_boxed()
            .filter(group_id.eq_any(group_ids_subquery))
    };

    let filtered_query = permission_type.create_boxed_filter(base_query, true);

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

/// Check if a group has a specific permission to a given namespace ID
///
/// ## Arguments
/// * pool - Database connection pool
/// * gid - ID of the group to check permissions for
/// * permission_type - Type of permission to check
/// * namespace_ref - Namespace or NamespaceID to check permissions for
///
/// ## Returns
/// * Ok(bool) - True if the group has the requested permission
/// * Err(ApiError) - On query errors only.
pub async fn group_can_on<T: NamespaceAccessors>(
    pool: &DbPool,
    gid: i32,
    namespace_ref: T,
    permission_type: Permissions,
) -> Result<bool, ApiError> {
    use crate::models::permissions::PermissionFilter;
    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    let base_query = permissions
        .into_boxed()
        .filter(group_id.eq(gid))
        .filter(namespace_id.eq(namespace_ref.namespace_id(pool).await?));

    let filtered_query = permission_type.create_boxed_filter(base_query, true);

    let result = filtered_query.execute(&mut conn)?;

    if result == 0 {
        return Ok(false);
    }

    Ok(true)
}

/// Check what groups have a specific permission to a given namespace ID
///
/// ## Arguments
/// * pool - Database connection pool
/// * nid - ID of the namespace to check permissions for
/// * permission_type - Type of permission to check
///
/// ## Returns
/// * Ok(Vec<Group>) - List of groups that have the requested permission
/// * Err(ApiError) - On query errors only.
pub async fn groups_can_on(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError> {
    use crate::models::permissions::PermissionFilter;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    // Adapted to start with a base query that might include a subquery for group IDs
    let base_query = permissions.into_boxed().filter(namespace_id.eq(nid));

    // Then filter on the given permission type using the PermissionFilter
    let filtered_query = permission_type.create_boxed_filter(base_query, true);

    // Selecting namespace IDs from the filtered query
    let group_ids = filtered_query
        .select(group_id)
        .distinct() // Ensuring distinct group IDs to avoid duplicates
        .load::<i32>(&mut conn)?;

    // Finally, fetching groups based on the obtained group IDs
    let results = if !group_ids.is_empty() {
        groups
            .filter(group_table_id.eq_any(group_ids))
            .load::<Group>(&mut conn)?
    } else {
        Vec::new() // Returning an empty vector if no group IDs were found
    };

    Ok(results)
}

/// List all groups and their permissions for a namespace
///
/// ## Arguments
/// * pool - Database connection pool
/// * nid - ID of the namespace to check permissions for
///
/// ## Returns
/// * Ok(Vec<(Group, NamespacePermissions)>) - List of groups and their permissions
/// * Err(ApiError) - On query errors only.
pub async fn groups_on<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::models::PermissionFilter;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let namespace_target_id = namespace_ref.namespace_id(pool).await?;

    let mut base_query = permissions
        .filter(namespace_id.eq(namespace_target_id))
        .into_boxed();

    for perm in permissions_filter.into_iter() {
        base_query = perm.create_boxed_filter(base_query, true);
    }

    let query = base_query
        .inner_join(groups.on(group_table_id.eq(group_id)))
        .select((groups::all_columns(), permissions::all_columns()))
        .load::<(Group, Permission)>(&mut conn)?;

    let structured_results: Vec<GroupPermission> =
        query.into_iter().map(GroupPermission::from_tuple).collect();

    Ok(structured_results)
}

/// List all permissions for a given group on a namespace
pub async fn group_on(pool: &DbPool, nid: i32, gid: i32) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    let mut conn = pool.get()?;

    let results = permissions
        .filter(namespace_id.eq(nid))
        .filter(group_id.eq(gid))
        .first::<Permission>(&mut conn)?;

    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::models::group::NewGroup;
    use crate::models::permissions::PermissionsList;
    use crate::tests::{create_namespace, generate_all_subsets};
    use crate::traits::{CanDelete, PermissionController};

    async fn assign_to_groups(
        pool: &DbPool,
        namespace: &Namespace,
        groups: &[Group],
        permissions: PermissionsList<Permissions>,
    ) {
        let namespace = namespace.clone();

        for group in groups {
            namespace
                .clone()
                .grant(pool, group.id, permissions.clone())
                .await
                .unwrap();

            // Validate that the permissions were granted
            for permission in permissions.iter() {
                assert!(
                    group_can_on(pool, group.id, namespace.clone(), *permission)
                        .await
                        .unwrap(),
                    "Group {} does not have permission {:?} on namespace {}",
                    group.id,
                    permission,
                    namespace.id
                );
            }
        }
    }

    async fn groups_can_on_count(
        pool: &DbPool,
        nid: i32,
        permission_type: Permissions,
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
            .grant_one(&pool, 99999999, Permissions::ReadCollection)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ApiError::NotFound(_)));
    }

    #[actix_rt::test]
    async fn test_list_groups_who_can() {
        let (pool, _) = crate::tests::get_pool_and_config().await;

        let mut groups = Vec::new();
        for group_number in [1, 2, 3, 4, 5] {
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

        type NP = Permissions;
        type PL = PermissionsList<Permissions>;

        // Note: Slicing is *NOT* inclusive, so this will assign to groups 0, 1, and 2
        assign_to_groups(
            &pool,
            &namespace,
            &groups[0..3],
            PL::new([NP::ReadCollection]),
        )
        .await;

        groups_can_on_count(&pool, namespace.id, NP::ReadCollection, 4).await;
        groups_can_on_count(&pool, namespace.id, NP::UpdateCollection, 1).await;

        assign_to_groups(
            &pool,
            &namespace,
            &groups[2..4],
            PL::new([NP::ReadCollection, NP::UpdateCollection]),
        )
        .await;

        groups_can_on_count(&pool, namespace.id, NP::ReadCollection, 5).await;
        groups_can_on_count(&pool, namespace.id, NP::UpdateCollection, 3).await;
        groups_can_on_count(&pool, namespace.id, NP::DeleteCollection, 1).await;

        assign_to_groups(
            &pool,
            &namespace,
            &groups[3..4],
            PL::new([NP::DelegateCollection]),
        )
        .await;

        groups_can_on_count(&pool, namespace.id, NP::DelegateCollection, 2).await;
        groups_can_on_count(&pool, namespace.id, NP::CreateClass, 1).await;
        groups_can_on_count(&pool, namespace.id, NP::CreateObject, 1).await;

        let all_on = groups_on(&pool, namespace.clone(), vec![]).await.unwrap();
        assert_eq!(all_on.len(), 5);

        namespace.delete(&pool).await.unwrap();
        for group in groups {
            group.delete(&pool).await.unwrap();
        }
    }

    #[actix_rt::test]
    async fn test_permission_grant_combinations() {
        let (pool, _) = crate::tests::get_pool_and_config().await;

        let permissions = vec![
            Permissions::ReadCollection,
            Permissions::UpdateCollection,
            Permissions::DeleteCollection,
            Permissions::DelegateCollection,
            /*
            Permissions::CreateClass,
            Permissions::ReadClass,
            Permissions::UpdateClass,
            Permissions::DeleteClass,
            Permissions::CreateObject,
            Permissions::ReadObject,
            Permissions::UpdateObject,
            Permissions::DeleteObject,
            */
        ];

        let subsets = generate_all_subsets(&permissions);

        for subset in subsets.iter() {
            let namespace = create_namespace(&pool, "test_perm_grant_combinations")
                .await
                .unwrap();

            let group = NewGroup {
                groupname: "test_perm_grant_combinations".to_string(),
                description: Some("Test group for combinations".to_string()),
            }
            .save(&pool)
            .await
            .unwrap();

            let group_id = group.id;
            // Grant this subset of permissions
            namespace
                .grant(&pool, group_id, PermissionsList::new(subset.clone()))
                .await
                .unwrap();

            // Test that only the granted permissions are set
            for permission in permissions.iter() {
                let expected = subset.contains(permission);
                let actual = group_can_on(&pool, group_id, namespace.clone(), *permission)
                    .await
                    .unwrap();
                assert_eq!(expected, actual, "Mismatch for permission {:?}", permission);
            }

            namespace.delete(&pool).await.unwrap();
            group.delete(&pool).await.unwrap();
        }
    }

    #[actix_rt::test]
    async fn test_permission_revoke_combinations() {
        let (pool, _) = crate::tests::get_pool_and_config().await;

        type NP = Permissions;

        let permissions = vec![
            NP::ReadCollection,
            NP::UpdateCollection,
            NP::DeleteCollection,
            NP::DelegateCollection,
            /*
            NP::CreateClass,
            NP::ReadClass,
            NP::UpdateClass,
            NP::DeleteClass,
            NP::CreateObject,
            NP::ReadObject,
            NP::UpdateObject,
            NP::DeleteObject,
            */
        ];

        // Generate all permission permutations, but filter out the empty set as that update will
        // cause diesel to complain that there is nothing to do.
        let subsets = generate_all_subsets(&permissions)
            .into_iter()
            .filter(|x| !x.is_empty());

        for subset in subsets {
            let namespace = create_namespace(&pool, "test_perm_revoke_ombinations")
                .await
                .unwrap();

            let group = NewGroup {
                groupname: "test_perm_revoke_combinations".to_string(),
                description: Some("Test group for combinations".to_string()),
            }
            .save(&pool)
            .await
            .unwrap();

            let group_id = group.id;
            // Grant all permissions
            namespace
                .grant(&pool, group_id, PermissionsList::new(permissions.clone()))
                .await
                .unwrap();

            // Revoke this subset of permissions
            namespace
                .revoke(&pool, group_id, PermissionsList::new(subset.clone()))
                .await
                .unwrap();

            // Test that only the revoked permissions are set
            for permission in permissions.iter() {
                let expected = !subset.contains(permission);
                let actual = group_can_on(&pool, group_id, namespace.clone(), *permission)
                    .await
                    .unwrap();
                assert_eq!(expected, actual, "Mismatch for permission {:?}", permission);
            }

            namespace.delete(&pool).await.unwrap();
            group.delete(&pool).await.unwrap();
        }
    }

    /// Test to ensure that we can grant and revoke permissions without losing or gaining
    /// any other permissions.
    #[actix_rt::test]
    async fn test_permission_grant_without_side_effects() {
        let (pool, _) = crate::tests::get_pool_and_config().await;

        type NP = Permissions;

        let namespace = create_namespace(&pool, "test_perm_grant_without_side_effects")
            .await
            .unwrap();

        let group = NewGroup {
            groupname: "test_perm_grant_without_side_effects".to_string(),
            description: Some("Test group for combinations".to_string()),
        }
        .save(&pool)
        .await
        .unwrap();

        let group_id = group.id;

        namespace
            .grant_one(&pool, group_id, NP::ReadCollection)
            .await
            .unwrap();

        assert!(
            group_can_on(&pool, group_id, namespace.clone(), NP::ReadCollection)
                .await
                .unwrap(),
            "Permission {:?} should be set",
            NP::ReadCollection
        );

        for permission in [
            NP::UpdateCollection,
            NP::DeleteCollection,
            NP::DelegateCollection,
            NP::CreateClass,
            NP::CreateObject,
        ] {
            assert!(
                !group_can_on(&pool, group_id, namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {:?} should not be set",
                permission
            );
        }

        namespace
            .grant_one(&pool, group_id, NP::UpdateCollection)
            .await
            .unwrap();

        for permission in [NP::ReadCollection, NP::UpdateCollection] {
            assert!(
                group_can_on(&pool, group_id, namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {:?} should be set",
                permission
            );
        }

        for permission in [
            NP::DeleteCollection,
            NP::DelegateCollection,
            NP::CreateClass,
            NP::CreateObject,
        ] {
            assert!(
                !group_can_on(&pool, group_id, namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {:?} should not be set",
                permission
            );
        }

        namespace
            .revoke_one(&pool, group_id, NP::UpdateCollection)
            .await
            .unwrap();

        assert!(
            group_can_on(&pool, group_id, namespace.clone(), NP::ReadCollection)
                .await
                .unwrap(),
            "Permission {:?} should be set",
            NP::ReadCollection
        );

        for permission in [
            NP::UpdateCollection,
            NP::DeleteCollection,
            NP::DelegateCollection,
            NP::CreateClass,
            NP::CreateObject,
        ] {
            assert!(
                !group_can_on(&pool, group_id, namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {:?} should not be set",
                permission
            );
        }
        namespace.delete(&pool).await.unwrap();
        group.delete(&pool).await.unwrap();
    }
}
