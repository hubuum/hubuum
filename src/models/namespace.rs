use diesel::prelude::*;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::traits::namespace as namespace_backend;
use crate::models::group::Group;
use crate::models::user::{User, UserID};

use crate::db::DbPool;

use crate::schema::namespaces;

use crate::errors::ApiError;

use crate::models::output::GroupPermission;
use crate::models::search::QueryOptions;
use crate::models::{Permission, Permissions};

use crate::models::traits::GroupAccessors;
use crate::traits::{BackendContext, NamespaceAccessors, SelfAccessors};

#[derive(Serialize, Deserialize, Queryable, PartialEq, Debug, Clone, Selectable, ToSchema)]
#[diesel(table_name = namespaces)]
pub struct Namespace {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone, ToSchema)]
pub struct NamespaceID(pub i32);

#[derive(Serialize, Deserialize, Clone, AsChangeset, ToSchema)]
#[schema(example = update_namespace_example)]
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
#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = new_namespace_with_assignee_example)]
pub struct NewNamespaceWithAssignee {
    pub name: String,
    pub description: String,
    pub group_id: i32,
}

/// A new namespace, without an assignee. Used for creating new namespace entries
/// into the database.
///
/// Odds are pretty good that you want to use NewNamespaceWithAssignee instead.
#[derive(Serialize, Deserialize, Insertable, ToSchema)]
#[diesel(table_name = namespaces)]
pub struct NewNamespace {
    pub name: String,
    pub description: String,
}

#[allow(dead_code)]
fn update_namespace_example() -> UpdateNamespace {
    UpdateNamespace {
        name: Some("global-assets".to_string()),
        description: Some("Shared assets and metadata".to_string()),
    }
}

#[allow(dead_code)]
fn new_namespace_with_assignee_example() -> NewNamespaceWithAssignee {
    NewNamespaceWithAssignee {
        name: "global-assets".to_string(),
        description: "Shared assets and metadata".to_string(),
        group_id: 1,
    }
}

pub async fn total_namespace_count<C>(backend: &C) -> Result<i64, ApiError>
where
    C: BackendContext + ?Sized,
{
    namespace_backend::total_namespace_count_from_backend(backend.db_pool()).await
}

/// Check what permissions a user has to a given namespace
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * user_id - ID of the user to check permissions for
/// * namespace_ref - Namespace or NamespaceID to check permissions for
///
/// ## Returns
/// * Ok(Vec(Group, NamespacePermissions)) - List of groups and their permissions
/// * Err(ApiError) - On query errors only.
#[allow(dead_code)]
pub async fn user_on<T: NamespaceAccessors>(
    backend: &impl BackendContext,
    user_id: UserID,
    namespace_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    namespace_backend::user_on_from_backend(backend.db_pool(), user_id, namespace_ref).await
}

pub async fn user_on_paginated<C, T>(
    backend: &C,
    user_id: UserID,
    namespace_ref: T,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError>
where
    C: BackendContext + ?Sized,
    T: NamespaceAccessors,
{
    namespace_backend::user_on_paginated_from_backend(
        backend.db_pool(),
        user_id,
        namespace_ref,
        query_options,
    )
    .await
}

/// Check if a user has a specific permission to any namespace
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * user_id - User accessor to check permissions for
/// * permission_type - Type of permission to check
///
/// ## Returns
/// * Ok(Vec<Namespace>) - List of namespaces the user has the requested permission for.
///   If no matching namespaces are found, an empty list is returned
/// * Err(ApiError) - On query errors only.
#[allow(dead_code)]
pub async fn user_can_on_any<C, U>(
    backend: &C,
    user_id: U,
    permission_type: Permissions,
) -> Result<Vec<Namespace>, ApiError>
where
    C: BackendContext + ?Sized,
    U: SelfAccessors<User> + GroupAccessors,
{
    namespace_backend::user_can_on_any_from_backend(backend.db_pool(), user_id, permission_type)
        .await
}

/// Check if a group has a specific permission to a given namespace ID
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * gid - ID of the group to check permissions for
/// * permission_type - Type of permission to check
/// * namespace_ref - Namespace or NamespaceID to check permissions for
///
/// ## Returns
/// * Ok(bool) - True if the group has the requested permission
/// * Err(ApiError) - On query errors only.
pub async fn group_can_on<C, T>(
    backend: &C,
    gid: i32,
    namespace_ref: T,
    permission_type: Permissions,
) -> Result<bool, ApiError>
where
    C: BackendContext + ?Sized,
    T: NamespaceAccessors,
{
    namespace_backend::group_can_on_from_backend(
        backend.db_pool(),
        gid,
        namespace_ref,
        permission_type,
    )
    .await
}

/// Check what groups have a specific permission to a given namespace ID
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * nid - ID of the namespace to check permissions for
/// * permission_type - Type of permission to check
///
/// ## Returns
/// * Ok(Vec<Group>) - List of groups that have the requested permission
/// * Err(ApiError) - On query errors only.
#[allow(dead_code)]
pub async fn groups_can_on<C>(
    backend: &C,
    nid: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError>
where
    C: BackendContext + ?Sized,
{
    namespace_backend::groups_can_on_from_backend(backend.db_pool(), nid, permission_type).await
}

pub async fn groups_can_on_paginated<C>(
    backend: &C,
    nid: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<Vec<Group>, ApiError>
where
    C: BackendContext + ?Sized,
{
    namespace_backend::groups_can_on_paginated_from_backend(
        backend.db_pool(),
        nid,
        permission_type,
        query_options,
    )
    .await
}

/// List all groups and their permissions for a namespace
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * namespace_ref - Namespace or NamespaceID to check permissions for
///
/// ## Returns
/// * Ok(Vec<(Group, NamespacePermissions)>) - List of groups and their permissions
/// * Err(ApiError) - On query errors only.
#[allow(dead_code)]
pub async fn groups_on<C, T>(
    backend: &C,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError>
where
    C: BackendContext + ?Sized,
    T: NamespaceAccessors,
{
    namespace_backend::groups_on_from_backend(
        backend.db_pool(),
        namespace_ref,
        permissions_filter,
        query_options,
    )
    .await
}

pub async fn groups_on_paginated<C, T>(
    backend: &C,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError>
where
    C: BackendContext + ?Sized,
    T: NamespaceAccessors,
{
    namespace_backend::groups_on_paginated_from_backend(
        backend.db_pool(),
        namespace_ref,
        permissions_filter,
        query_options,
    )
    .await
}

/// List all permissions for a given group on a namespace
pub async fn group_on<C>(backend: &C, nid: i32, gid: i32) -> Result<Permission, ApiError>
where
    C: BackendContext + ?Sized,
{
    namespace_backend::group_on_from_backend(backend.db_pool(), nid, gid).await
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::models::group::NewGroup;
    use crate::models::permissions::PermissionsList;
    use crate::tests::{TestScope, generate_all_subsets};
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
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        let namespace = scope.namespace_fixture("grant_to_nonexistent_group").await;

        // This should return an ApiError::NotFound
        let result = namespace
            .namespace
            .grant_one(&pool, 99999999, Permissions::ReadCollection)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ApiError::NotFound(_)));
    }

    #[actix_rt::test]
    async fn test_list_groups_who_can() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        let mut groups = Vec::new();
        for group_number in [1, 2, 3, 4, 5] {
            let group_name = format!("test_list_group_{group_number}");
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

        // The fixture owner group is granted full permissions when the namespace is created,
        // so we have one extra group for all permissions.
        let namespace = scope.namespace_fixture("test_list_groups").await;

        type NP = Permissions;
        type PL = PermissionsList<Permissions>;

        // Note: Slicing is *NOT* inclusive, so this will assign to groups 0, 1, and 2
        assign_to_groups(
            &pool,
            &namespace.namespace,
            &groups[0..3],
            PL::new([NP::ReadCollection]),
        )
        .await;

        groups_can_on_count(&pool, namespace.namespace.id, NP::ReadCollection, 4).await;
        groups_can_on_count(&pool, namespace.namespace.id, NP::UpdateCollection, 1).await;

        assign_to_groups(
            &pool,
            &namespace.namespace,
            &groups[2..4],
            PL::new([NP::ReadCollection, NP::UpdateCollection]),
        )
        .await;

        groups_can_on_count(&pool, namespace.namespace.id, NP::ReadCollection, 5).await;
        groups_can_on_count(&pool, namespace.namespace.id, NP::UpdateCollection, 3).await;
        groups_can_on_count(&pool, namespace.namespace.id, NP::DeleteCollection, 1).await;

        assign_to_groups(
            &pool,
            &namespace.namespace,
            &groups[3..4],
            PL::new([NP::DelegateCollection]),
        )
        .await;

        groups_can_on_count(&pool, namespace.namespace.id, NP::DelegateCollection, 2).await;
        groups_can_on_count(&pool, namespace.namespace.id, NP::CreateClass, 1).await;
        groups_can_on_count(&pool, namespace.namespace.id, NP::CreateObject, 1).await;

        let all_on = groups_on(
            &pool,
            namespace.namespace.clone(),
            vec![],
            QueryOptions {
                filters: vec![],
                sort: vec![],
                limit: None,
                cursor: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(all_on.len(), 5);

        namespace.cleanup().await.unwrap();
        for group in groups {
            group.delete(&pool).await.unwrap();
        }
    }

    #[actix_rt::test]
    async fn test_permission_grant_combinations() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

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
            let namespace = scope
                .namespace_fixture("test_perm_grant_combinations")
                .await;

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
                .namespace
                .grant(&pool, group_id, PermissionsList::new(subset.clone()))
                .await
                .unwrap();

            // Test that only the granted permissions are set
            for permission in permissions.iter() {
                let expected = subset.contains(permission);
                let actual =
                    group_can_on(&pool, group_id, namespace.namespace.clone(), *permission)
                        .await
                        .unwrap();
                assert_eq!(expected, actual, "Mismatch for permission {permission:?}");
            }

            namespace.cleanup().await.unwrap();
            group.delete(&pool).await.unwrap();
        }
    }

    #[actix_rt::test]
    async fn test_permission_revoke_combinations() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

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
            let namespace = scope
                .namespace_fixture("test_perm_revoke_ombinations")
                .await;

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
                .namespace
                .grant(&pool, group_id, PermissionsList::new(permissions.clone()))
                .await
                .unwrap();

            // Revoke this subset of permissions
            namespace
                .namespace
                .revoke(&pool, group_id, PermissionsList::new(subset.clone()))
                .await
                .unwrap();

            // Test that only the revoked permissions are set
            for permission in permissions.iter() {
                let expected = !subset.contains(permission);
                let actual =
                    group_can_on(&pool, group_id, namespace.namespace.clone(), *permission)
                        .await
                        .unwrap();
                assert_eq!(expected, actual, "Mismatch for permission {permission:?}");
            }

            namespace.cleanup().await.unwrap();
            group.delete(&pool).await.unwrap();
        }
    }

    /// Test to ensure that we can grant and revoke permissions without losing or gaining
    /// any other permissions.
    #[actix_rt::test]
    async fn test_permission_grant_without_side_effects() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        type NP = Permissions;

        let namespace = scope
            .namespace_fixture("test_perm_grant_without_side_effects")
            .await;

        let group = NewGroup {
            groupname: "test_perm_grant_without_side_effects".to_string(),
            description: Some("Test group for combinations".to_string()),
        }
        .save(&pool)
        .await
        .unwrap();

        let group_id = group.id;

        namespace
            .namespace
            .grant_one(&pool, group_id, NP::ReadCollection)
            .await
            .unwrap();

        assert!(
            group_can_on(
                &pool,
                group_id,
                namespace.namespace.clone(),
                NP::ReadCollection
            )
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
                !group_can_on(&pool, group_id, namespace.namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should not be set",
            );
        }

        namespace
            .namespace
            .grant_one(&pool, group_id, NP::UpdateCollection)
            .await
            .unwrap();

        for permission in [NP::ReadCollection, NP::UpdateCollection] {
            assert!(
                group_can_on(&pool, group_id, namespace.namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should be set",
            );
        }

        for permission in [
            NP::DeleteCollection,
            NP::DelegateCollection,
            NP::CreateClass,
            NP::CreateObject,
        ] {
            assert!(
                !group_can_on(&pool, group_id, namespace.namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should not be set",
            );
        }

        namespace
            .namespace
            .revoke_one(&pool, group_id, NP::UpdateCollection)
            .await
            .unwrap();

        assert!(
            group_can_on(
                &pool,
                group_id,
                namespace.namespace.clone(),
                NP::ReadCollection
            )
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
                !group_can_on(&pool, group_id, namespace.namespace.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should not be set",
            );
        }
        namespace.cleanup().await.unwrap();
        group.delete(&pool).await.unwrap();
    }
}
