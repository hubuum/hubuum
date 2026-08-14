use crate::models::token_scope::TokenScope;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::group::{Group, GroupID};
use crate::models::output::{EffectiveGroupPermission, GroupPermission};
use crate::models::search::QueryOptions;
use crate::models::traits::GroupAccessors;
use crate::models::{Permission, Permissions, ResourceRevision};
use crate::permissions::{
    AuthzTarget, ResourceAttrs, ResourceKind, ResourceRef, authorization_collection_from_storage,
    authorization_effective_group_grant_from_storage, authorization_group_from_storage,
    authorization_group_grant_from_storage, grant_from_storage, permission_to_storage,
};
use crate::services::identity::token_scope_to_storage;
use crate::services::storage_boundary::collection_from_storage;
use crate::storage::{
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationGroupCollectionQuery, AuthorizationPrincipalCollectionPageQuery,
    AuthorizationPrincipalCollectionQuery, CollectionAuthorizationStorage, StorageContext,
    storage_handle,
};
use crate::traits::AuthzSubject;
use crate::traits::{CollectionAccessors, SelfAccessors};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct Collection {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub parent_collection_id: Option<i32>,
    pub revision: ResourceRevision,
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`Collection`].
    pub struct CollectionID;
    noun = "collection id";
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = update_collection_example)]
pub struct UpdateCollection {
    pub name: Option<String>,
    pub description: Option<String>,
}

impl UpdateCollection {
    pub(crate) fn has_changes(&self, current: &Collection) -> bool {
        self.name
            .as_ref()
            .is_some_and(|value| value != &current.name)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
    }
}

/// A new collection, with an assignee. Used for creating new collection entries
/// into the database and assign all permissions to the group given as group_id.
///
/// This wraps the NewCollection struct and uses the group_id to grant all permissions
/// to the group in a single transaction.
#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[schema(example = new_collection_with_assignee_example)]
pub struct NewCollectionWithAssignee {
    pub name: String,
    pub description: String,
    #[schema(value_type = i32, minimum = 1)]
    pub group_id: GroupID,
    pub parent_collection_id: Option<CollectionID>,
}

/// A new collection, without an assignee. Used for creating new collection entries
/// into the database.
///
/// Odds are pretty good that you want to use NewCollectionWithAssignee instead.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct NewCollection {
    pub name: String,
    pub description: String,
    pub parent_collection_id: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct UpdateCollectionParent {
    pub parent_collection_id: CollectionID,
}

fn update_collection_example() -> UpdateCollection {
    UpdateCollection {
        name: Some("global-assets".to_string()),
        description: Some("Shared assets and metadata".to_string()),
    }
}

fn new_collection_with_assignee_example() -> NewCollectionWithAssignee {
    NewCollectionWithAssignee {
        name: "global-assets".to_string(),
        description: "Shared assets and metadata".to_string(),
        group_id: GroupID::new(1).expect("valid example group id"),
        parent_collection_id: None,
    }
}

/// Check what permissions a user has to a given collection
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * user_id - ID of the user to check permissions for
/// * collection_ref - Collection or CollectionID to check permissions for
///
/// ## Returns
/// * Ok(Vec(Group, CollectionPermissions)) - List of groups and their permissions
/// * Err(ApiError) - On query errors only.
pub async fn principal_on<C, S, T>(
    backend: &C,
    principal: S,
    collection_ref: T,
) -> Result<Vec<GroupPermission>, ApiError>
where
    C: StorageContext,
    S: AuthzSubject,
    T: CollectionAccessors,
{
    let query = AuthorizationPrincipalCollectionQuery::new(
        principal.principal_id(),
        collection_ref.collection_id(backend).await?.id(),
    );
    storage_handle(backend)
        .principal_collection_permissions(query)
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(authorization_group_grant_from_storage)
                .collect()
        })
}

/// All of a principal's direct permission rows across every collection, as
/// `(collection, group, permission-row)` tuples.
pub async fn principal_all_permissions<C, S>(
    backend: &C,
    principal: S,
) -> Result<Vec<(Collection, Group, Permission)>, ApiError>
where
    C: StorageContext,
    S: AuthzSubject,
{
    storage_handle(backend)
        .principal_all_collection_permissions(principal.principal_id())
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    let (grant, group, collection) = row.into_parts();
                    Ok((
                        authorization_collection_from_storage(collection)?,
                        authorization_group_from_storage(group)?,
                        grant_from_storage(grant),
                    ))
                })
                .collect()
        })
}

pub async fn principal_on_paginated_with_total_count<C, S, T>(
    backend: &C,
    principal: S,
    collection_ref: T,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError>
where
    C: StorageContext,
    S: AuthzSubject,
    T: CollectionAccessors,
{
    let principal = AuthorizationPrincipalCollectionQuery::new(
        principal.principal_id(),
        collection_ref.collection_id(backend).await?.id(),
    );
    storage_handle(backend)
        .principal_collection_permissions_page(AuthorizationPrincipalCollectionPageQuery::new(
            principal,
            query_options.clone(),
        ))
        .await
        .map_err(ApiError::from)
        .and_then(|page| {
            let (rows, total) = page.into_parts();
            Ok((
                rows.into_iter()
                    .map(authorization_group_grant_from_storage)
                    .collect::<Result<Vec<_>, _>>()?,
                total,
            ))
        })
}

pub async fn effective_principal_on<C, S, T>(
    backend: &C,
    principal: S,
    collection_ref: T,
) -> Result<Vec<EffectiveGroupPermission>, ApiError>
where
    C: StorageContext,
    S: AuthzSubject,
    T: CollectionAccessors,
{
    let query = AuthorizationPrincipalCollectionQuery::new(
        principal.principal_id(),
        collection_ref.collection_id(backend).await?.id(),
    );
    storage_handle(backend)
        .effective_principal_collection_permissions(query)
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(authorization_effective_group_grant_from_storage)
                .collect()
        })
}

/// Check if a user has a specific permission to any collection
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * user_id - User accessor to check permissions for
/// * permission_type - Type of permission to check
///
/// ## Returns
/// * Ok(Vec<Collection>) - List of collections the user has the requested permission for.
///   If no matching collections are found, an empty list is returned
/// * Err(ApiError) - On query errors only.
pub async fn user_can_on_any<C, U>(
    backend: &C,
    user_id: U,
    permission_type: Permissions,
    scopes: Option<&TokenScope>,
) -> Result<Vec<Collection>, ApiError>
where
    C: StorageContext,
    U: GroupAccessors + AuthzSubject,
{
    storage_handle(backend)
        .visible_collections(AuthorizationCollectionVisibilityQuery::new(
            user_id.principal_id(),
            permission_to_storage(permission_type),
            scopes.map(token_scope_to_storage),
        ))
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(authorization_collection_from_storage)
                .collect()
        })
}

/// Check if a group has a specific permission to a given collection ID
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * gid - ID of the group to check permissions for
/// * permission_type - Type of permission to check
/// * collection_ref - Collection or CollectionID to check permissions for
///
/// ## Returns
/// * Ok(bool) - True if the group has the requested permission
/// * Err(ApiError) - On query errors only.
pub async fn group_can_on<C, T>(
    backend: &C,
    gid: i32,
    collection_ref: T,
    permission_type: Permissions,
) -> Result<bool, ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    storage_handle(backend)
        .group_has_collection_permission(AuthorizationGroupCollectionQuery::new(
            collection_ref.collection_id(backend).await?.id(),
            gid,
            permission_to_storage(permission_type),
        ))
        .await
        .map_err(ApiError::from)
}

pub async fn effective_group_on<C>(
    backend: &C,
    target_collection_id: i32,
    gid: i32,
) -> Result<Vec<EffectiveGroupPermission>, ApiError>
where
    C: StorageContext,
{
    storage_handle(backend)
        .effective_group_collection_permissions(target_collection_id, gid)
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(authorization_effective_group_grant_from_storage)
                .collect()
        })
}

/// Check what groups have a specific permission to a given collection ID
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * target_collection_id - ID of the collection to check permissions for
/// * permission_type - Type of permission to check
///
/// ## Returns
/// * Ok(Vec<Group>) - List of groups that have the requested permission
/// * Err(ApiError) - On query errors only.
pub async fn groups_can_on<C>(
    backend: &C,
    target_collection_id: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError>
where
    C: StorageContext,
{
    storage_handle(backend)
        .groups_with_collection_permission(AuthorizationCollectionGroupsQuery::new(
            target_collection_id,
            permission_to_storage(permission_type),
        ))
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(authorization_group_from_storage)
                .collect()
        })
}

pub async fn groups_can_on_paginated_with_total_count<C>(
    backend: &C,
    target_collection_id: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError>
where
    C: StorageContext,
{
    storage_handle(backend)
        .groups_with_collection_permission_page(AuthorizationCollectionGroupsPageQuery::new(
            AuthorizationCollectionGroupsQuery::new(
                target_collection_id,
                permission_to_storage(permission_type),
            ),
            query_options.clone(),
        ))
        .await
        .map_err(ApiError::from)
        .and_then(|page| {
            let (rows, total) = page.into_parts();
            Ok((
                rows.into_iter()
                    .map(authorization_group_from_storage)
                    .collect::<Result<Vec<_>, _>>()?,
                total,
            ))
        })
}

/// List all groups and their permissions for a collection
///
/// ## Arguments
/// * backend - Backend context used to execute the query
/// * collection_ref - Collection or CollectionID to check permissions for
///
/// ## Returns
/// * Ok(Vec<(Group, CollectionPermissions)>) - List of groups and their permissions
/// * Err(ApiError) - On query errors only.
pub async fn groups_on<C, T>(
    backend: &C,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    storage_handle(backend)
        .list_collection_group_permissions(AuthorizationCollectionGrantListQuery::new(
            collection_ref.collection_id(backend).await?.id(),
            permissions_filter.into_iter().map(permission_to_storage),
            query_options,
        ))
        .await
        .map_err(ApiError::from)
        .and_then(|rows| {
            rows.into_iter()
                .map(authorization_group_grant_from_storage)
                .collect()
        })
}

pub async fn groups_on_paginated<C, T>(
    backend: &C,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    let page = storage_handle(backend)
        .list_collection_group_permissions_page(AuthorizationCollectionGrantListQuery::new(
            collection_ref.collection_id(backend).await?.id(),
            permissions_filter.into_iter().map(permission_to_storage),
            query_options.clone(),
        ))
        .await
        .map_err(ApiError::from)?;
    let (items, _) = page.into_parts();
    items
        .into_iter()
        .map(authorization_group_grant_from_storage)
        .collect()
}

pub async fn groups_on_paginated_with_total_count<C, T>(
    backend: &C,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    storage_handle(backend)
        .list_collection_group_permissions_page(AuthorizationCollectionGrantListQuery::new(
            collection_ref.collection_id(backend).await?.id(),
            permissions_filter.into_iter().map(permission_to_storage),
            query_options.clone(),
        ))
        .await
        .map_err(ApiError::from)
        .and_then(|page| {
            let (rows, total) = page.into_parts();
            Ok((
                rows.into_iter()
                    .map(authorization_group_grant_from_storage)
                    .collect::<Result<Vec<_>, _>>()?,
                total,
            ))
        })
}

pub async fn count_groups_on_paginated<C, T>(
    backend: &C,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<i64, ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    let (_, total) = storage_handle(backend)
        .list_collection_group_permissions_page(AuthorizationCollectionGrantListQuery::new(
            collection_ref.collection_id(backend).await?.id(),
            permissions_filter.into_iter().map(permission_to_storage),
            query_options.clone(),
        ))
        .await
        .map_err(ApiError::from)?
        .into_parts();
    Ok(total)
}

/// List all permissions for a given group on a collection
pub async fn group_on<C>(
    backend: &C,
    target_collection_id: i32,
    gid: i32,
) -> Result<Permission, ApiError>
where
    C: StorageContext,
{
    storage_handle(backend)
        .collection_group_permission(target_collection_id, gid)
        .await
        .map_err(ApiError::from)
        .map(grant_from_storage)
}

pub async fn collection_children<C, T>(
    backend: &C,
    collection_ref: T,
) -> Result<Vec<Collection>, ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    let collection_id = collection_ref.collection_id(backend).await?;
    storage_handle(backend)
        .collection_store()
        .collection_children(collection_id.id())
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(collection_from_storage)
        .collect()
}

pub async fn collection_ancestors<C, T>(
    backend: &C,
    collection_ref: T,
) -> Result<Vec<Collection>, ApiError>
where
    C: StorageContext,
    T: CollectionAccessors,
{
    let collection_id = collection_ref.collection_id(backend).await?;
    storage_handle(backend)
        .collection_store()
        .collection_ancestors(collection_id.id())
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(collection_from_storage)
        .collect()
}

pub async fn move_collection<C>(
    backend: &C,
    collection_id: i32,
    new_parent_collection_id: i32,
    context: Option<&crate::events::EventContext>,
) -> Result<Collection, ApiError>
where
    C: StorageContext,
{
    storage_handle(backend)
        .collection_store()
        .move_collection(collection_id, new_parent_collection_id, context)
        .await
        .map_err(ApiError::from)
        .and_then(collection_from_storage)
}

#[derive(serde::Serialize, Clone, Debug, ToSchema)]
pub struct CollectionHistory {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub parent_collection_id: Option<i32>,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
    pub actor_kind: Option<String>,
    pub initiator_user_id: Option<i32>,
    pub task_id: Option<i32>,
    pub revision: ResourceRevision,
}

impl crate::traits::CursorPaginated for CollectionHistory {
    fn supports_sort(field: &crate::models::search::FilterField) -> bool {
        matches!(
            field,
            crate::models::search::FilterField::HistoryId
                | crate::models::search::FilterField::Revision
        )
    }

    fn cursor_value(
        &self,
        field: &crate::models::search::FilterField,
    ) -> Result<crate::traits::CursorValue, ApiError> {
        Ok(match field {
            crate::models::search::FilterField::HistoryId => {
                crate::traits::CursorValue::Integer(self.history_id)
            }
            crate::models::search::FilterField::Revision => {
                crate::traits::CursorValue::Integer(self.revision.get())
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for history"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        vec![crate::models::search::SortParam {
            field: crate::models::search::FilterField::HistoryId,
            descending: true,
        }]
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        Self::default_sort()
    }
}

#[async_trait]
impl AuthzTarget for Collection {
    async fn to_resource_ref(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        Ok(ResourceRef {
            kind: ResourceKind::Collection,
            id: self.id,
            attrs: ResourceAttrs {
                collection_id: Some(self.id),
                name: Some(self.name.clone()),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for CollectionID {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::storage::postgres::prelude::*;
    use diesel::sql_query;

    use super::*;

    use crate::models::group::{GroupID, NewGroup};
    use crate::models::permissions::PermissionsList;
    use crate::tests::{TestScope, create_test_user, generate_all_subsets};
    use crate::traits::UserPermissions;
    use crate::traits::{CanDelete, CanSave, PermissionController};

    async fn assign_to_groups(
        pool: &impl crate::storage::StorageContext,
        collection: &Collection,
        groups: &[Group],
        permissions: PermissionsList,
    ) {
        let collection = collection.clone();

        for group in groups {
            collection
                .clone()
                .grant_without_events(pool, GroupID::new(group.id).unwrap(), permissions.clone())
                .await
                .unwrap();

            // Validate that the permissions were granted
            for permission in permissions.iter() {
                assert!(
                    group_can_on(pool, group.id, collection.clone(), *permission)
                        .await
                        .unwrap(),
                    "Group {} does not have permission {:?} on collection {}",
                    group.id,
                    permission,
                    collection.id
                );
            }
        }
    }

    async fn groups_can_on_count(
        pool: &impl crate::storage::StorageContext,
        target_collection_id: i32,
        permission_type: Permissions,
        expected_count: i32,
    ) {
        let groups = groups_can_on(pool, target_collection_id, permission_type)
            .await
            .unwrap();
        assert_eq!(groups.len() as i32, expected_count);
    }

    #[actix_rt::test]
    async fn inherited_permissions_apply_without_unioning_rows() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let parent = scope.collection_fixture("inherited_parent").await;
        let child_group = NewGroup {
            identity_scope: None,
            groupname: scope.scoped_name("inherited_child_group"),
            description: Some("Child group".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();
        let child = NewCollectionWithAssignee {
            name: scope.scoped_name("inherited_child"),
            description: "Child collection".to_string(),
            group_id: GroupID::new(child_group.id).unwrap(),
            parent_collection_id: Some(CollectionID::new(parent.collection.id).unwrap()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();
        let user = create_test_user(&pool).await;

        parent
            .owner_group
            .add_member_without_events(&pool, &user)
            .await
            .unwrap();
        child_group
            .add_member_without_events(&pool, &user)
            .await
            .unwrap();

        parent
            .collection
            .set_permissions_without_events(
                &pool,
                GroupID::new(parent.owner_group.id).unwrap(),
                PermissionsList::new([Permissions::ReadCollection]),
            )
            .await
            .unwrap();
        child
            .set_permissions_without_events(
                &pool,
                GroupID::new(child_group.id).unwrap(),
                PermissionsList::new([Permissions::UpdateCollection]),
            )
            .await
            .unwrap();

        user.can(&pool, [Permissions::ReadCollection], [child.clone()], None)
            .await
            .unwrap();
        user.can(
            &pool,
            [Permissions::UpdateCollection],
            [child.clone()],
            None,
        )
        .await
        .unwrap();

        let combined_result = user
            .can(
                &pool,
                [Permissions::ReadCollection, Permissions::UpdateCollection],
                [child.clone()],
                None,
            )
            .await;
        assert!(matches!(combined_result, Err(ApiError::Forbidden(_))));

        assert!(
            group_can_on(
                &pool,
                parent.owner_group.id,
                child.clone(),
                Permissions::ReadCollection,
            )
            .await
            .unwrap()
        );

        let groups_with_read = groups_can_on(&pool, child.id, Permissions::ReadCollection)
            .await
            .unwrap();
        assert!(
            groups_with_read
                .iter()
                .any(|group| group.id == parent.owner_group.id)
        );

        let effective = effective_group_on(&pool, child.id, parent.owner_group.id)
            .await
            .unwrap();
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].source_collection.id, parent.collection.id);
        assert!(effective[0].inherited);

        child.delete_without_events(&pool).await.unwrap();
        parent.cleanup().await.unwrap();
        child_group.delete_without_events(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn moving_collections_updates_ancestors_and_rejects_cycles() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let parent = scope.collection_fixture("move_parent").await;
        let root_id = parent.collection.parent_collection_id.unwrap();

        let child_group = NewGroup {
            identity_scope: None,
            groupname: scope.scoped_name("move_child_group"),
            description: Some("Child group".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();
        let child = NewCollectionWithAssignee {
            name: scope.scoped_name("move_child"),
            description: "Child collection".to_string(),
            group_id: GroupID::new(child_group.id).unwrap(),
            parent_collection_id: Some(CollectionID::new(parent.collection.id).unwrap()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let grandchild_group = NewGroup {
            identity_scope: None,
            groupname: scope.scoped_name("move_grandchild_group"),
            description: Some("Grandchild group".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();
        let grandchild = NewCollectionWithAssignee {
            name: scope.scoped_name("move_grandchild"),
            description: "Grandchild collection".to_string(),
            group_id: GroupID::new(grandchild_group.id).unwrap(),
            parent_collection_id: Some(CollectionID::new(child.id).unwrap()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let initial_ancestors = collection_ancestors(&pool, grandchild.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();
        assert_eq!(
            &initial_ancestors[0..3],
            &[child.id, parent.collection.id, root_id]
        );

        let cycle_result = move_collection(&pool, parent.collection.id, grandchild.id, None).await;
        assert!(matches!(cycle_result, Err(ApiError::BadRequest(_))));

        let moved = move_collection(&pool, child.id, root_id, None)
            .await
            .unwrap();
        assert_eq!(moved.parent_collection_id, Some(root_id));

        let moved_ancestors = collection_ancestors(&pool, grandchild.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();
        assert_eq!(&moved_ancestors[0..2], &[child.id, root_id]);

        grandchild.delete_without_events(&pool).await.unwrap();
        child.delete_without_events(&pool).await.unwrap();
        parent.cleanup().await.unwrap();
        grandchild_group.delete_without_events(&pool).await.unwrap();
        child_group.delete_without_events(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn database_rejects_direct_root_delete_and_reparent() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let parent = scope.collection_fixture("protect_root").await;
        let root_id = parent.collection.parent_collection_id.unwrap();

        let delete_result = crate::storage::postgres::with_connection(&pool, async |conn| {
            sql_query("DELETE FROM collections WHERE id = $1")
                .bind::<diesel::sql_types::Integer, _>(root_id)
                .execute(conn)
                .await
        })
        .await;
        assert!(delete_result.is_err());

        let reparent_result = crate::storage::postgres::with_connection(&pool, async |conn| {
            sql_query("UPDATE collections SET parent_collection_id = $1 WHERE id = $2")
                .bind::<diesel::sql_types::Integer, _>(parent.collection.id)
                .bind::<diesel::sql_types::Integer, _>(root_id)
                .execute(conn)
                .await
        })
        .await;
        assert!(reparent_result.is_err());

        parent.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn grant_to_nonexistent_group() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        let collection = scope.collection_fixture("grant_to_nonexistent_group").await;

        // This should return an ApiError::NotFound
        let result = collection
            .collection
            .grant_one(
                &pool,
                GroupID::new(99_999_999).unwrap(),
                Permissions::ReadCollection,
            )
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
                    identity_scope: None,
                    groupname: group_name.to_string(),
                    description: Some("Test group".to_string()),
                }
                .save_without_events(&pool)
                .await
                .unwrap(),
            );
        }

        // The fixture owner group is granted full permissions when the collection is created,
        // so we have one extra group for all permissions.
        let collection = scope.collection_fixture("test_list_groups").await;

        type NP = Permissions;
        type PL = PermissionsList;

        // Note: Slicing is *NOT* inclusive, so this will assign to groups 0, 1, and 2
        assign_to_groups(
            &pool,
            &collection.collection,
            &groups[0..3],
            PL::new([NP::ReadCollection]),
        )
        .await;

        groups_can_on_count(&pool, collection.collection.id, NP::ReadCollection, 4).await;
        groups_can_on_count(&pool, collection.collection.id, NP::UpdateCollection, 1).await;

        assign_to_groups(
            &pool,
            &collection.collection,
            &groups[2..4],
            PL::new([NP::ReadCollection, NP::UpdateCollection]),
        )
        .await;

        groups_can_on_count(&pool, collection.collection.id, NP::ReadCollection, 5).await;
        groups_can_on_count(&pool, collection.collection.id, NP::UpdateCollection, 3).await;
        groups_can_on_count(&pool, collection.collection.id, NP::DeleteCollection, 1).await;

        assign_to_groups(
            &pool,
            &collection.collection,
            &groups[3..4],
            PL::new([NP::DelegateCollection]),
        )
        .await;

        groups_can_on_count(&pool, collection.collection.id, NP::DelegateCollection, 2).await;
        groups_can_on_count(&pool, collection.collection.id, NP::CreateClass, 1).await;
        groups_can_on_count(&pool, collection.collection.id, NP::CreateObject, 1).await;

        let all_on = groups_on(
            &pool,
            collection.collection.clone(),
            vec![],
            QueryOptions {
                filters: vec![],
                sort: vec![],
                limit: None,
                cursor: None,
                include_total: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(all_on.len(), 5);

        collection.cleanup().await.unwrap();
        for group in groups {
            group.delete_without_events(&pool).await.unwrap();
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
            let collection = scope
                .collection_fixture("test_perm_grant_combinations")
                .await;

            let group = NewGroup {
                identity_scope: None,
                groupname: "test_perm_grant_combinations".to_string(),
                description: Some("Test group for combinations".to_string()),
            }
            .save_without_events(&pool)
            .await
            .unwrap();

            let group_id = group.id;
            // Grant this subset of permissions
            collection
                .collection
                .grant_without_events(
                    &pool,
                    GroupID::new(group_id).unwrap(),
                    PermissionsList::new(subset.clone()),
                )
                .await
                .unwrap();

            // Test that only the granted permissions are set
            for permission in permissions.iter() {
                let expected = subset.contains(permission);
                let actual =
                    group_can_on(&pool, group_id, collection.collection.clone(), *permission)
                        .await
                        .unwrap();
                assert_eq!(expected, actual, "Mismatch for permission {permission:?}");
            }

            collection.cleanup().await.unwrap();
            group.delete_without_events(&pool).await.unwrap();
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
            let collection = scope
                .collection_fixture("test_perm_revoke_combinations")
                .await;

            let group = NewGroup {
                identity_scope: None,
                groupname: "test_perm_revoke_combinations".to_string(),
                description: Some("Test group for combinations".to_string()),
            }
            .save_without_events(&pool)
            .await
            .unwrap();

            let group_id = group.id;
            // Grant all permissions
            collection
                .collection
                .grant_without_events(
                    &pool,
                    GroupID::new(group_id).unwrap(),
                    PermissionsList::new(permissions.clone()),
                )
                .await
                .unwrap();

            // Revoke this subset of permissions
            collection
                .collection
                .revoke_without_events(
                    &pool,
                    GroupID::new(group_id).unwrap(),
                    PermissionsList::new(subset.clone()),
                )
                .await
                .unwrap();

            // Test that only the revoked permissions are set
            for permission in permissions.iter() {
                let expected = !subset.contains(permission);
                let actual =
                    group_can_on(&pool, group_id, collection.collection.clone(), *permission)
                        .await
                        .unwrap();
                assert_eq!(expected, actual, "Mismatch for permission {permission:?}");
            }

            collection.cleanup().await.unwrap();
            group.delete_without_events(&pool).await.unwrap();
        }
    }

    /// Test to ensure that we can grant and revoke permissions without losing or gaining
    /// any other permissions.
    #[actix_rt::test]
    async fn test_permission_grant_without_side_effects() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        type NP = Permissions;

        let collection = scope
            .collection_fixture("test_perm_grant_without_side_effects")
            .await;

        let group = NewGroup {
            identity_scope: None,
            groupname: "test_perm_grant_without_side_effects".to_string(),
            description: Some("Test group for combinations".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let group_id = group.id;

        collection
            .collection
            .grant_one(&pool, GroupID::new(group_id).unwrap(), NP::ReadCollection)
            .await
            .unwrap();

        assert!(
            group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
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
                !group_can_on(&pool, group_id, collection.collection.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should not be set",
            );
        }

        collection
            .collection
            .grant_one(&pool, GroupID::new(group_id).unwrap(), NP::UpdateCollection)
            .await
            .unwrap();

        for permission in [NP::ReadCollection, NP::UpdateCollection] {
            assert!(
                group_can_on(&pool, group_id, collection.collection.clone(), permission)
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
                !group_can_on(&pool, group_id, collection.collection.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should not be set",
            );
        }

        collection
            .collection
            .revoke_one(&pool, GroupID::new(group_id).unwrap(), NP::UpdateCollection)
            .await
            .unwrap();

        assert!(
            group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
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
                !group_can_on(&pool, group_id, collection.collection.clone(), permission)
                    .await
                    .unwrap(),
                "Permission {permission:?} should not be set",
            );
        }
        collection.cleanup().await.unwrap();
        group.delete_without_events(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_template_permissions_set_grant_and_revoke() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        let collection = scope
            .collection_fixture("test_template_permissions_set_grant_and_revoke")
            .await;

        let group = NewGroup {
            identity_scope: None,
            groupname: "test_template_permissions_set_grant_and_revoke".to_string(),
            description: Some("Template permission controller coverage".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();
        let group_id = group.id;

        collection
            .collection
            .set_permissions_without_events(
                &pool,
                GroupID::new(group_id).unwrap(),
                PermissionsList::new([Permissions::ReadTemplate, Permissions::UpdateTemplate]),
            )
            .await
            .unwrap();

        assert!(
            group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::ReadTemplate
            )
            .await
            .unwrap()
        );
        assert!(
            group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::UpdateTemplate
            )
            .await
            .unwrap()
        );
        assert!(
            !group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::CreateTemplate
            )
            .await
            .unwrap()
        );
        assert!(
            !group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::DeleteTemplate
            )
            .await
            .unwrap()
        );

        collection
            .collection
            .grant_one(
                &pool,
                GroupID::new(group_id).unwrap(),
                Permissions::CreateTemplate,
            )
            .await
            .unwrap();

        assert!(
            group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::CreateTemplate
            )
            .await
            .unwrap()
        );

        collection
            .collection
            .revoke_one(
                &pool,
                GroupID::new(group_id).unwrap(),
                Permissions::UpdateTemplate,
            )
            .await
            .unwrap();

        assert!(
            !group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::UpdateTemplate
            )
            .await
            .unwrap()
        );
        assert!(
            group_can_on(
                &pool,
                group_id,
                collection.collection.clone(),
                Permissions::ReadTemplate
            )
            .await
            .unwrap()
        );

        collection.cleanup().await.unwrap();
        group.delete_without_events(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_template_permission_backfill_updates_only_delegators() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let collection = scope
            .collection_fixture("test_template_permission_backfill")
            .await;

        let delegate_group = NewGroup {
            identity_scope: None,
            groupname: format!("template_backfill_delegate_{}", collection.collection.id),
            description: Some("Template backfill delegate group".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let non_delegate_group = NewGroup {
            identity_scope: None,
            groupname: format!(
                "template_backfill_non_delegate_{}",
                collection.collection.id
            ),
            description: Some("Template backfill non-delegate group".to_string()),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        collection
            .collection
            .grant_one(
                &pool,
                GroupID::new(delegate_group.id).unwrap(),
                Permissions::DelegateCollection,
            )
            .await
            .unwrap();
        collection
            .collection
            .grant_one(
                &pool,
                GroupID::new(non_delegate_group.id).unwrap(),
                Permissions::ReadCollection,
            )
            .await
            .unwrap();

        let delegate_before = group_on(&pool, collection.collection.id, delegate_group.id)
            .await
            .unwrap();
        assert!(delegate_before.has_delegate_collection);
        assert!(!delegate_before.has_read_template);
        assert!(!delegate_before.has_create_template);
        assert!(!delegate_before.has_update_template);
        assert!(!delegate_before.has_delete_template);

        let non_delegate_before = group_on(&pool, collection.collection.id, non_delegate_group.id)
            .await
            .unwrap();
        assert!(!non_delegate_before.has_delegate_collection);
        assert!(!non_delegate_before.has_read_template);
        assert!(!non_delegate_before.has_create_template);
        assert!(!non_delegate_before.has_update_template);
        assert!(!non_delegate_before.has_delete_template);

        let mut conn = pool.get().await.unwrap();
        sql_query(
            "UPDATE permissions
             SET
                 has_read_template = TRUE,
                 has_create_template = TRUE,
                 has_update_template = TRUE,
                 has_delete_template = TRUE
             WHERE has_delegate_collection = TRUE",
        )
        .execute(&mut conn)
        .await
        .unwrap();

        let delegate_after = group_on(&pool, collection.collection.id, delegate_group.id)
            .await
            .unwrap();
        assert!(delegate_after.has_delegate_collection);
        assert!(delegate_after.has_read_template);
        assert!(delegate_after.has_create_template);
        assert!(delegate_after.has_update_template);
        assert!(delegate_after.has_delete_template);

        let non_delegate_after = group_on(&pool, collection.collection.id, non_delegate_group.id)
            .await
            .unwrap();
        assert!(!non_delegate_after.has_delegate_collection);
        assert!(!non_delegate_after.has_read_template);
        assert!(!non_delegate_after.has_create_template);
        assert!(!non_delegate_after.has_update_template);
        assert!(!non_delegate_after.has_delete_template);

        collection.cleanup().await.unwrap();
        delegate_group.delete_without_events(&pool).await.unwrap();
        non_delegate_group
            .delete_without_events(&pool)
            .await
            .unwrap();
    }
}
