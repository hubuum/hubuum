use diesel::prelude::*;

use diesel::pg::Pg;
use diesel::sql_types::{Integer, Text, Timestamp};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::models::group::Group;
use crate::models::user::{User, UserID};

use crate::db::{DbPool, with_connection};

use crate::schema::namespaces;

use crate::errors::ApiError;

use crate::models::output::GroupPermission;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::{Permission, Permissions};

use crate::db::traits::user::GroupMemberships;
use crate::models::traits::GroupAccessors;
use crate::traits::{NamespaceAccessors, SelfAccessors};

use tracing::info;

use super::PermissionsList;

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

fn permission_filter_sql(permission: Permissions, target: bool) -> &'static str {
    match (permission, target) {
        (Permissions::ReadCollection, true) => "permissions.has_read_namespace = TRUE",
        (Permissions::ReadCollection, false) => "permissions.has_read_namespace = FALSE",
        (Permissions::UpdateCollection, true) => "permissions.has_update_namespace = TRUE",
        (Permissions::UpdateCollection, false) => "permissions.has_update_namespace = FALSE",
        (Permissions::DeleteCollection, true) => "permissions.has_delete_namespace = TRUE",
        (Permissions::DeleteCollection, false) => "permissions.has_delete_namespace = FALSE",
        (Permissions::DelegateCollection, true) => "permissions.has_delegate_namespace = TRUE",
        (Permissions::DelegateCollection, false) => "permissions.has_delegate_namespace = FALSE",
        (Permissions::CreateClass, true) => "permissions.has_create_class = TRUE",
        (Permissions::CreateClass, false) => "permissions.has_create_class = FALSE",
        (Permissions::ReadClass, true) => "permissions.has_read_class = TRUE",
        (Permissions::ReadClass, false) => "permissions.has_read_class = FALSE",
        (Permissions::UpdateClass, true) => "permissions.has_update_class = TRUE",
        (Permissions::UpdateClass, false) => "permissions.has_update_class = FALSE",
        (Permissions::DeleteClass, true) => "permissions.has_delete_class = TRUE",
        (Permissions::DeleteClass, false) => "permissions.has_delete_class = FALSE",
        (Permissions::CreateObject, true) => "permissions.has_create_object = TRUE",
        (Permissions::CreateObject, false) => "permissions.has_create_object = FALSE",
        (Permissions::ReadObject, true) => "permissions.has_read_object = TRUE",
        (Permissions::ReadObject, false) => "permissions.has_read_object = FALSE",
        (Permissions::UpdateObject, true) => "permissions.has_update_object = TRUE",
        (Permissions::UpdateObject, false) => "permissions.has_update_object = FALSE",
        (Permissions::DeleteObject, true) => "permissions.has_delete_object = TRUE",
        (Permissions::DeleteObject, false) => "permissions.has_delete_object = FALSE",
        (Permissions::CreateClassRelation, true) => "permissions.has_create_class_relation = TRUE",
        (Permissions::CreateClassRelation, false) => {
            "permissions.has_create_class_relation = FALSE"
        }
        (Permissions::ReadClassRelation, true) => "permissions.has_read_class_relation = TRUE",
        (Permissions::ReadClassRelation, false) => "permissions.has_read_class_relation = FALSE",
        (Permissions::UpdateClassRelation, true) => "permissions.has_update_class_relation = TRUE",
        (Permissions::UpdateClassRelation, false) => {
            "permissions.has_update_class_relation = FALSE"
        }
        (Permissions::DeleteClassRelation, true) => "permissions.has_delete_class_relation = TRUE",
        (Permissions::DeleteClassRelation, false) => {
            "permissions.has_delete_class_relation = FALSE"
        }
        (Permissions::CreateObjectRelation, true) => {
            "permissions.has_create_object_relation = TRUE"
        }
        (Permissions::CreateObjectRelation, false) => {
            "permissions.has_create_object_relation = FALSE"
        }
        (Permissions::ReadObjectRelation, true) => "permissions.has_read_object_relation = TRUE",
        (Permissions::ReadObjectRelation, false) => "permissions.has_read_object_relation = FALSE",
        (Permissions::UpdateObjectRelation, true) => {
            "permissions.has_update_object_relation = TRUE"
        }
        (Permissions::UpdateObjectRelation, false) => {
            "permissions.has_update_object_relation = FALSE"
        }
        (Permissions::DeleteObjectRelation, true) => {
            "permissions.has_delete_object_relation = TRUE"
        }
        (Permissions::DeleteObjectRelation, false) => {
            "permissions.has_delete_object_relation = FALSE"
        }
    }
}

pub async fn total_namespace_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::namespaces::dsl::*;

    let count = with_connection(pool, |conn| namespaces.count().get_result::<i64>(conn))?;

    Ok(count)
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
#[allow(dead_code)]
pub async fn user_on<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{group_id, namespace_id, permissions};
    use diesel::prelude::*;

    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let group_ids_subquery = user_id.group_ids_subquery();
    let query = with_connection(pool, |conn| {
        groups
            .inner_join(permissions.on(group_table_id.eq(group_id)))
            .filter(namespace_id.eq(namespace_target_id))
            .filter(group_id.eq_any(group_ids_subquery))
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;

    let structured_results: Vec<GroupPermission> =
        query.into_iter().map(GroupPermission::from_tuple).collect();

    Ok(structured_results)
}

pub async fn user_on_paginated<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groupname, groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        created_at as permission_created_at, group_id, id as permission_id, namespace_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search, string_search};
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let group_ids_subquery = user_id.group_ids_subquery();

    let mut query = groups
        .inner_join(permissions.on(group_table_id.eq(group_id)))
        .filter(namespace_id.eq(namespace_target_id))
        .filter(group_id.eq_any(group_ids_subquery))
        .into_boxed();

    for perm in query_options.filters.permissions()?.iter().cloned() {
        query = query.filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
            permission_filter_sql(perm, true),
        ));
    }

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, permission_id),
            FilterField::Name | FilterField::Groupname => {
                string_search!(query, param, operator, groupname)
            }
            FilterField::CreatedAt => {
                date_search!(query, param, operator, permission_created_at)
            }
            FilterField::UpdatedAt => {
                date_search!(query, param, operator, permission_updated_at)
            }
            FilterField::Permissions => {}
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    param.field
                )));
            }
        }
    }

    crate::apply_query_options!(query, query_options, GroupPermission);

    let rows = query.load::<(Group, Permission)>(&mut conn)?;
    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
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
#[allow(dead_code)]
pub async fn user_can_on_any<U: SelfAccessors<User> + GroupAccessors>(
    pool: &DbPool,
    user_id: U,
    permission_type: Permissions,
) -> Result<Vec<Namespace>, ApiError> {
    use crate::models::permissions::PermissionFilter;

    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    let base_query = if user_id.instance(pool).await?.is_admin(pool).await? {
        permissions.into_boxed()
    } else {
        let group_ids_subquery = user_id.group_ids_subquery();

        permissions
            .into_boxed()
            .filter(group_id.eq_any(group_ids_subquery))
    };

    let filtered_query = permission_type.create_boxed_filter(base_query, true);
    let accessible_namespace_ids = with_connection(pool, |conn| {
        filtered_query.select(namespace_id).load::<i32>(conn)
    })?;

    let accessible_namespaces = if accessible_namespace_ids.is_empty() {
        vec![]
    } else {
        with_connection(pool, |conn| {
            namespaces::table
                .filter(namespaces::id.eq_any(accessible_namespace_ids))
                .load::<Namespace>(conn)
        })?
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

    let base_query = permissions
        .into_boxed()
        .filter(group_id.eq(gid))
        .filter(namespace_id.eq(namespace_ref.namespace_id(pool).await?));

    let filtered_query = permission_type.create_boxed_filter(base_query, true);
    let result = with_connection(pool, |conn| filtered_query.execute(conn))?;

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
#[allow(dead_code)]
pub async fn groups_can_on(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError> {
    use crate::models::permissions::PermissionFilter;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    // Adapted to start with a base query that might include a subquery for group IDs
    let base_query = permissions.into_boxed().filter(namespace_id.eq(nid));

    // Then filter on the given permission type using the PermissionFilter
    let filtered_query = permission_type.create_boxed_filter(base_query, true);

    // Selecting namespace IDs from the filtered query
    let group_ids = with_connection(pool, |conn| {
        filtered_query
            .select(group_id)
            .distinct() // Ensuring distinct group IDs to avoid duplicates
            .load::<i32>(conn)
    })?;

    // Finally, fetching groups based on the obtained group IDs
    let results = if !group_ids.is_empty() {
        with_connection(pool, |conn| {
            groups
                .filter(group_table_id.eq_any(group_ids))
                .load::<Group>(conn)
        })?
    } else {
        Vec::new() // Returning an empty vector if no group IDs were found
    };

    Ok(results)
}

pub async fn groups_can_on_paginated(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<Vec<Group>, ApiError> {
    use crate::models::permissions::PermissionFilter;
    use crate::schema::groups::dsl::{
        created_at, description, groupname, groups, id as group_table_id, updated_at,
    };
    use crate::schema::permissions::dsl::*;
    use crate::{date_search, numeric_search, string_search};
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let base_query = permissions.into_boxed().filter(namespace_id.eq(nid));
    let filtered_query = permission_type.create_boxed_filter(base_query, true);

    let mut query = groups
        .filter(group_table_id.eq_any(filtered_query.select(group_id).distinct()))
        .into_boxed();

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, group_table_id),
            FilterField::Name | FilterField::Groupname => {
                string_search!(query, param, operator, groupname)
            }
            FilterField::Description => string_search!(query, param, operator, description),
            FilterField::CreatedAt => date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(query, param, operator, updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for groups",
                    param.field
                )));
            }
        }
    }

    crate::apply_query_options!(query, query_options, Group);

    query.load::<Group>(&mut conn).map_err(ApiError::from)
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
#[allow(dead_code)]
pub async fn groups_on<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::PermissionFilter;
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        created_at as permission_created_at, group_id, id as permission_id, namespace_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search};
    use diesel::prelude::*;

    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let query_params = query_options.filters;

    let mut permission_filters = query_params.permissions()?;
    permission_filters.ensure_contains(&permissions_filter);

    let mut base_query = permissions
        .filter(namespace_id.eq(namespace_target_id))
        .into_boxed();

    for perm in permission_filters.iter().cloned() {
        base_query = perm.create_boxed_filter(base_query, true);
    }

    for param in query_params {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(base_query, param, operator, permission_id),
            FilterField::CreatedAt => {
                date_search!(base_query, param, operator, permission_created_at)
            }
            FilterField::UpdatedAt => {
                date_search!(base_query, param, operator, permission_updated_at)
            }
            FilterField::Permissions => {} // handled above
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    param.field
                )));
            }
        }
    }

    for order in query_options.sort.iter() {
        match (&order.field, &order.descending) {
            (FilterField::Id, false) => base_query = base_query.order_by(permission_id.asc()),
            (FilterField::Id, true) => base_query = base_query.order_by(permission_id.desc()),
            (FilterField::CreatedAt, false) => {
                base_query = base_query.order_by(permission_created_at.asc())
            }
            (FilterField::CreatedAt, true) => {
                base_query = base_query.order_by(permission_created_at.desc())
            }
            (FilterField::UpdatedAt, false) => {
                base_query = base_query.order_by(permission_updated_at.asc())
            }
            (FilterField::UpdatedAt, true) => {
                base_query = base_query.order_by(permission_updated_at.desc())
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't orderable (or does not exist) for permissions",
                    order.field
                )));
            }
        }
    }

    if let Some(limit) = query_options.limit {
        base_query = base_query.limit(limit as i64);
    }

    let query = with_connection(pool, |conn| {
        base_query
            .inner_join(groups.on(group_table_id.eq(group_id)))
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;

    let structured_results: Vec<GroupPermission> =
        query.into_iter().map(GroupPermission::from_tuple).collect();

    Ok(structured_results)
}

pub async fn groups_on_paginated<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groupname, groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        created_at as permission_created_at, group_id, id as permission_id, namespace_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search, string_search};
    use diesel::prelude::*;

    let mut conn = pool.get()?;
    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let mut permission_filters = query_options.filters.permissions()?;
    permission_filters.ensure_contains(&permissions_filter);

    let mut query = groups
        .inner_join(permissions.on(group_table_id.eq(group_id)))
        .filter(namespace_id.eq(namespace_target_id))
        .into_boxed();

    for perm in permission_filters.iter().cloned() {
        query = query.filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
            permission_filter_sql(perm, true),
        ));
    }

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, permission_id),
            FilterField::Name | FilterField::Groupname => {
                string_search!(query, param, operator, groupname)
            }
            FilterField::CreatedAt => {
                date_search!(query, param, operator, permission_created_at)
            }
            FilterField::UpdatedAt => {
                date_search!(query, param, operator, permission_updated_at)
            }
            FilterField::Permissions => {}
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    param.field
                )));
            }
        }
    }

    crate::apply_query_options!(query, query_options, GroupPermission);

    let rows = query
        .select((groups::all_columns(), permissions::all_columns()))
        .load::<(Group, Permission)>(&mut conn)?;
    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
}

/// List all permissions for a given group on a namespace
pub async fn group_on(pool: &DbPool, nid: i32, gid: i32) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;
    use diesel::prelude::*;

    with_connection(pool, |conn| {
        permissions
            .filter(namespace_id.eq(nid))
            .filter(group_id.eq(gid))
            .first::<Permission>(conn)
    })
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
