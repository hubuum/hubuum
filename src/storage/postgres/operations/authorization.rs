use diesel::dsl::{exists, select};

use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions};
use crate::models::{
    Collection, CollectionID, Group, GroupPermission, Permission, Permissions, PermissionsList,
    PrincipalID,
};
use crate::storage::postgres::operations::collection as collection_backend;
use crate::storage::postgres::operations::collection::CollectionRow;
use crate::storage::postgres::operations::group::GroupRow;
use crate::storage::postgres::operations::permissions as permission_backend;
use crate::storage::postgres::operations::permissions::PermissionRow;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresPool, with_connection};
use crate::storage::{
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup, AuthorizationGroupGrant,
    AuthorizationGroupGrantPage, AuthorizationGroupIdentity, AuthorizationGroupMembershipQuery,
    AuthorizationGroupProfile, AuthorizationGroupSyncState, AuthorizationObjectResource,
    AuthorizationPermission, AuthorizationPermissionSet, AuthorizationPermissionSetQuery,
    AuthorizationPolicySnapshotRow, AuthorizationPrincipal, AuthorizationResourceIds,
};

pub(crate) const fn permission_from_storage(permission: AuthorizationPermission) -> Permissions {
    match permission {
        AuthorizationPermission::ReadCollection => Permissions::ReadCollection,
        AuthorizationPermission::UpdateCollection => Permissions::UpdateCollection,
        AuthorizationPermission::DeleteCollection => Permissions::DeleteCollection,
        AuthorizationPermission::DelegateCollection => Permissions::DelegateCollection,
        AuthorizationPermission::CreateClass => Permissions::CreateClass,
        AuthorizationPermission::ReadClass => Permissions::ReadClass,
        AuthorizationPermission::UpdateClass => Permissions::UpdateClass,
        AuthorizationPermission::DeleteClass => Permissions::DeleteClass,
        AuthorizationPermission::CreateObject => Permissions::CreateObject,
        AuthorizationPermission::ReadObject => Permissions::ReadObject,
        AuthorizationPermission::UpdateObject => Permissions::UpdateObject,
        AuthorizationPermission::DeleteObject => Permissions::DeleteObject,
        AuthorizationPermission::CreateClassRelation => Permissions::CreateClassRelation,
        AuthorizationPermission::ReadClassRelation => Permissions::ReadClassRelation,
        AuthorizationPermission::UpdateClassRelation => Permissions::UpdateClassRelation,
        AuthorizationPermission::DeleteClassRelation => Permissions::DeleteClassRelation,
        AuthorizationPermission::CreateObjectRelation => Permissions::CreateObjectRelation,
        AuthorizationPermission::ReadObjectRelation => Permissions::ReadObjectRelation,
        AuthorizationPermission::UpdateObjectRelation => Permissions::UpdateObjectRelation,
        AuthorizationPermission::DeleteObjectRelation => Permissions::DeleteObjectRelation,
        AuthorizationPermission::ReadTemplate => Permissions::ReadTemplate,
        AuthorizationPermission::CreateTemplate => Permissions::CreateTemplate,
        AuthorizationPermission::UpdateTemplate => Permissions::UpdateTemplate,
        AuthorizationPermission::DeleteTemplate => Permissions::DeleteTemplate,
        AuthorizationPermission::ReadRemoteTarget => Permissions::ReadRemoteTarget,
        AuthorizationPermission::CreateRemoteTarget => Permissions::CreateRemoteTarget,
        AuthorizationPermission::UpdateRemoteTarget => Permissions::UpdateRemoteTarget,
        AuthorizationPermission::DeleteRemoteTarget => Permissions::DeleteRemoteTarget,
        AuthorizationPermission::ExecuteRemoteTarget => Permissions::ExecuteRemoteTarget,
        AuthorizationPermission::ReadAudit => Permissions::ReadAudit,
        AuthorizationPermission::ManageEventSubscription => Permissions::ManageEventSubscription,
    }
}

pub(crate) async fn load_authorization_classes(
    pool: &PostgresPool,
    query: AuthorizationResourceIds,
) -> Result<Vec<AuthorizationClassResource>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, id};

    with_connection(pool, async |conn| {
        hubuumclass
            .filter(id.eq_any(query.ids()))
            .select((id, collection_id))
            .load::<(i32, i32)>(conn)
            .await
    })
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(class_id, class_collection_id)| {
                AuthorizationClassResource::new(class_id, class_collection_id)
            })
            .collect()
    })
}

pub(crate) async fn load_authorization_objects(
    pool: &PostgresPool,
    query: AuthorizationResourceIds,
) -> Result<Vec<AuthorizationObjectResource>, ApiError> {
    use crate::schema::hubuumobject::dsl::{
        collection_id, hubuum_class_id, hubuumobject, id, name,
    };

    with_connection(pool, async |conn| {
        hubuumobject
            .filter(id.eq_any(query.ids()))
            .select((id, collection_id, hubuum_class_id, name))
            .load::<(i32, i32, i32, String)>(conn)
            .await
    })
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(object_id, object_collection_id, class_id, object_name)| {
                AuthorizationObjectResource::new(
                    object_id,
                    object_collection_id,
                    class_id,
                    object_name,
                )
            })
            .collect()
    })
}

fn permission_to_storage(permission: Permissions) -> AuthorizationPermission {
    match permission {
        Permissions::ReadCollection => AuthorizationPermission::ReadCollection,
        Permissions::UpdateCollection => AuthorizationPermission::UpdateCollection,
        Permissions::DeleteCollection => AuthorizationPermission::DeleteCollection,
        Permissions::DelegateCollection => AuthorizationPermission::DelegateCollection,
        Permissions::CreateClass => AuthorizationPermission::CreateClass,
        Permissions::ReadClass => AuthorizationPermission::ReadClass,
        Permissions::UpdateClass => AuthorizationPermission::UpdateClass,
        Permissions::DeleteClass => AuthorizationPermission::DeleteClass,
        Permissions::CreateObject => AuthorizationPermission::CreateObject,
        Permissions::ReadObject => AuthorizationPermission::ReadObject,
        Permissions::UpdateObject => AuthorizationPermission::UpdateObject,
        Permissions::DeleteObject => AuthorizationPermission::DeleteObject,
        Permissions::CreateClassRelation => AuthorizationPermission::CreateClassRelation,
        Permissions::ReadClassRelation => AuthorizationPermission::ReadClassRelation,
        Permissions::UpdateClassRelation => AuthorizationPermission::UpdateClassRelation,
        Permissions::DeleteClassRelation => AuthorizationPermission::DeleteClassRelation,
        Permissions::CreateObjectRelation => AuthorizationPermission::CreateObjectRelation,
        Permissions::ReadObjectRelation => AuthorizationPermission::ReadObjectRelation,
        Permissions::UpdateObjectRelation => AuthorizationPermission::UpdateObjectRelation,
        Permissions::DeleteObjectRelation => AuthorizationPermission::DeleteObjectRelation,
        Permissions::ReadTemplate => AuthorizationPermission::ReadTemplate,
        Permissions::CreateTemplate => AuthorizationPermission::CreateTemplate,
        Permissions::UpdateTemplate => AuthorizationPermission::UpdateTemplate,
        Permissions::DeleteTemplate => AuthorizationPermission::DeleteTemplate,
        Permissions::ReadRemoteTarget => AuthorizationPermission::ReadRemoteTarget,
        Permissions::CreateRemoteTarget => AuthorizationPermission::CreateRemoteTarget,
        Permissions::UpdateRemoteTarget => AuthorizationPermission::UpdateRemoteTarget,
        Permissions::DeleteRemoteTarget => AuthorizationPermission::DeleteRemoteTarget,
        Permissions::ExecuteRemoteTarget => AuthorizationPermission::ExecuteRemoteTarget,
        Permissions::ReadAudit => AuthorizationPermission::ReadAudit,
        Permissions::ManageEventSubscription => AuthorizationPermission::ManageEventSubscription,
    }
}

fn collection_to_storage(collection: Collection) -> AuthorizationCollection {
    AuthorizationCollection::new(
        collection.id,
        collection.name,
        collection.description,
        collection.created_at,
        collection.updated_at,
        collection.parent_collection_id,
        collection.revision.get(),
    )
}

fn group_to_storage(group: impl Into<Group>) -> AuthorizationGroup {
    let group = group.into();
    AuthorizationGroup::new(
        AuthorizationGroupIdentity::new(
            group.id,
            group.groupname,
            group.identity_scope_id,
            group.managed_by,
            group.external_key,
        ),
        AuthorizationGroupProfile::new(
            group.description,
            group.created_at,
            group.updated_at,
            group.revision.get(),
        ),
        AuthorizationGroupSyncState::new(group.last_sync_attempted_at, group.last_sync_success_at),
    )
}

fn grant_to_storage(grant: impl Into<Permission>) -> AuthorizationGrant {
    let grant = grant.into();
    AuthorizationGrant::new(
        grant.id,
        grant.collection_id,
        grant.group_id,
        grant.granted().into_iter().map(permission_to_storage),
        grant.created_at,
        grant.updated_at,
    )
}

fn group_grant_to_storage(row: GroupPermission) -> AuthorizationGroupGrant {
    AuthorizationGroupGrant::new(
        group_to_storage(row.group),
        grant_to_storage(row.permission),
    )
}

pub(crate) async fn load_authorization_principal(
    pool: &PostgresPool,
    principal_id: i32,
) -> Result<AuthorizationPrincipal, ApiError> {
    use crate::schema::group_memberships;

    let group_ids = with_connection(pool, async |conn| {
        group_memberships::table
            .filter(group_memberships::principal_id.eq(principal_id))
            .order_by(group_memberships::group_id.asc())
            .select(group_memberships::group_id)
            .load::<i32>(conn)
            .await
    })
    .await?;
    Ok(AuthorizationPrincipal::new(principal_id, group_ids))
}

pub(crate) async fn authorization_principal_is_group_member(
    pool: &PostgresPool,
    query: AuthorizationGroupMembershipQuery,
) -> Result<bool, ApiError> {
    use crate::schema::{group_memberships, groups, identity_scopes};

    let principal_id = query.principal_id();
    let group_name = query.group_name().to_string();
    let identity_scope = query.identity_scope().to_string();
    with_connection(pool, async move |conn| {
        select(exists(
            group_memberships::table
                .inner_join(groups::table)
                .inner_join(
                    identity_scopes::table.on(groups::identity_scope_id.eq(identity_scopes::id)),
                )
                .filter(group_memberships::principal_id.eq(principal_id))
                .filter(groups::groupname.eq(group_name))
                .filter(identity_scopes::name.eq(identity_scope)),
        ))
        .get_result(conn)
        .await
    })
    .await
}

pub(crate) async fn authorize_local_collection(
    pool: &PostgresPool,
    query: AuthorizationCollectionAccessQuery,
) -> Result<bool, ApiError> {
    use crate::schema::{group_memberships, permissions};
    use crate::storage::postgres::operations::permissions::PermissionFilter;

    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(query.principal_id()))
        .select(group_memberships::group_id);
    let mut permission_query = permissions::table
        .filter(permissions::collection_id.eq(query.collection_id()))
        .filter(permissions::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in query.permissions().iter().copied() {
        permission_query =
            permission_from_storage(permission).create_boxed_filter(permission_query, true);
    }

    with_connection(pool, async |conn| {
        select(exists(permission_query)).get_result(conn).await
    })
    .await
}

pub(crate) async fn authorize_local_collections(
    pool: &PostgresPool,
    query: AuthorizationCollectionsAccessQuery,
) -> Result<bool, ApiError> {
    use diesel::{AggregateExpressionMethods, dsl::count};

    use crate::schema::{collection_closure, group_memberships, permissions};

    if query.collection_ids().is_empty() {
        return Ok(true);
    }

    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(query.principal_id()))
        .select(group_memberships::group_id);
    let mut permission_query = permissions::table
        .inner_join(
            collection_closure::table
                .on(permissions::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .select(permissions::all_columns)
        .filter(collection_closure::descendant_collection_id.eq_any(query.collection_ids()))
        .filter(permissions::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in query.permissions().iter().copied() {
        crate::apply_permission_filter!(
            permission_query,
            permission_from_storage(permission),
            true
        );
    }

    let matching_collections = with_connection(pool, async |conn| {
        permission_query
            .select(count(collection_closure::descendant_collection_id).aggregate_distinct())
            .first::<i64>(conn)
            .await
    })
    .await?;

    Ok(matching_collections as usize == query.collection_ids().len())
}

pub(crate) async fn local_authorized_collections(
    pool: &PostgresPool,
    query: AuthorizationCollectionsQuery,
) -> Result<Vec<AuthorizationCollection>, ApiError> {
    use crate::schema::{collections, group_memberships, permissions};

    let permissions_requested = query
        .permissions()
        .iter()
        .copied()
        .map(permission_from_storage)
        .collect::<Vec<_>>();
    if permissions_requested.is_empty() {
        let group_ids = group_memberships::table
            .filter(group_memberships::principal_id.eq(query.principal_id()))
            .select(group_memberships::group_id);
        let collection_ids = permissions::table
            .filter(permissions::group_id.eq_any(group_ids))
            .select(permissions::collection_id);
        let rows = with_connection(pool, async |conn| {
            collections::table
                .filter(collections::id.eq_any(collection_ids))
                .distinct()
                .load::<CollectionRow>(conn)
                .await
        })
        .await?;
        return Ok(rows
            .into_iter()
            .map(Into::into)
            .map(collection_to_storage)
            .collect());
    }

    let principal_id = PrincipalID::new(query.principal_id())?;
    let mut authorized_collection_ids: Option<Vec<i32>> = None;
    for permission in permissions_requested {
        let rows =
            collection_backend::user_can_on_any_from_backend(pool, principal_id, permission, None)
                .await?;
        let mut ids = rows
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        authorized_collection_ids = Some(match authorized_collection_ids {
            Some(existing) => existing
                .into_iter()
                .filter(|id| ids.binary_search(id).is_ok())
                .collect(),
            None => ids,
        });
    }

    let ids = authorized_collection_ids.unwrap_or_default();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = with_connection(pool, async |conn| {
        collections::table
            .filter(collections::id.eq_any(ids))
            .load::<CollectionRow>(conn)
            .await
    })
    .await?;
    Ok(rows
        .into_iter()
        .map(Into::into)
        .map(collection_to_storage)
        .collect())
}

pub(crate) async fn list_authorization_collection_candidates(
    pool: &PostgresPool,
) -> Result<Vec<AuthorizationCollection>, ApiError> {
    use crate::schema::collections;

    let rows = with_connection(pool, async |conn| {
        collections::table
            .order_by(collections::id.asc())
            .load::<CollectionRow>(conn)
            .await
    })
    .await?;
    Ok(rows
        .into_iter()
        .map(Into::into)
        .map(collection_to_storage)
        .collect())
}

pub(crate) async fn list_authorization_group_candidates(
    pool: &PostgresPool,
    query_options: QueryOptions,
) -> Result<Vec<AuthorizationGroup>, ApiError> {
    use crate::schema::groups::dsl::{created_at, groupname, groups as groups_dsl, id, updated_at};
    use crate::{date_search, numeric_search, string_search};

    let mut query = groups_dsl.into_boxed();
    for parameter in &query_options.filters {
        let operator = parameter.operator.clone();
        match parameter.field {
            FilterField::Id => numeric_search!(query, parameter, operator, id),
            FilterField::Name | FilterField::Groupname => {
                string_search!(query, parameter, operator, groupname)
            }
            FilterField::CreatedAt => {
                date_search!(query, parameter, operator, created_at)
            }
            FilterField::UpdatedAt => {
                date_search!(query, parameter, operator, updated_at)
            }
            FilterField::Permissions => {}
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    parameter.field
                )));
            }
        }
    }
    let rows = with_connection(pool, async |conn| query.load::<GroupRow>(conn).await).await?;
    Ok(rows.into_iter().map(group_to_storage).collect())
}

pub(crate) async fn authorization_policy_snapshot(
    pool: &PostgresPool,
) -> Result<Vec<AuthorizationPolicySnapshotRow>, ApiError> {
    use crate::schema::{collections, groups, permissions};

    let rows = with_connection(pool, async |conn| {
        permissions::table
            .inner_join(groups::table.on(permissions::group_id.eq(groups::id)))
            .inner_join(collections::table.on(permissions::collection_id.eq(collections::id)))
            .order_by((
                permissions::collection_id.asc(),
                permissions::group_id.asc(),
            ))
            .load::<(PermissionRow, GroupRow, CollectionRow)>(conn)
            .await
    })
    .await?;
    Ok(rows
        .into_iter()
        .map(|(grant, group, collection)| {
            AuthorizationPolicySnapshotRow::new(
                grant_to_storage(grant),
                group_to_storage(group),
                collection_to_storage(collection.into()),
            )
        })
        .collect())
}

pub(crate) async fn list_local_collection_grants(
    pool: &PostgresPool,
    query: AuthorizationCollectionGrantListQuery,
) -> Result<AuthorizationGroupGrantPage, ApiError> {
    let required_permissions = query
        .required_permissions()
        .iter()
        .copied()
        .map(permission_from_storage)
        .collect();
    let (rows, total_count) =
        collection_backend::groups_on_paginated_with_total_count_from_backend(
            pool,
            CollectionID::new(query.collection_id())?,
            required_permissions,
            query.query_options(),
        )
        .await?;
    Ok(AuthorizationGroupGrantPage::new(
        rows.into_iter().map(group_grant_to_storage).collect(),
        total_count,
    ))
}

pub(crate) async fn get_local_collection_grant(
    pool: &PostgresPool,
    key: AuthorizationGrantKey,
) -> Result<Option<AuthorizationGrant>, ApiError> {
    use crate::schema::permissions;

    let row = with_connection(pool, async |conn| {
        permissions::table
            .filter(permissions::collection_id.eq(key.collection_id()))
            .filter(permissions::group_id.eq(key.group_id()))
            .first::<PermissionRow>(conn)
            .await
            .optional()
    })
    .await?;
    Ok(row.map(grant_to_storage))
}

pub(crate) async fn load_local_collection_permission_set(
    pool: &PostgresPool,
    query: AuthorizationPermissionSetQuery,
) -> Result<AuthorizationPermissionSet, ApiError> {
    use crate::schema::{collection_authorization_state, permissions};

    let rows = with_connection(pool, async |conn| {
        if let Some(group_id) = query.group_id() {
            collection_authorization_state::table
                .left_join(
                    permissions::table.on(permissions::collection_id
                        .eq(collection_authorization_state::collection_id)
                        .and(permissions::group_id.eq(group_id))),
                )
                .filter(collection_authorization_state::collection_id.eq(query.collection_id()))
                .select((
                    collection_authorization_state::revision,
                    Option::<PermissionRow>::as_select(),
                ))
                .load::<(crate::models::ResourceRevision, Option<PermissionRow>)>(conn)
                .await
        } else {
            collection_authorization_state::table
                .left_join(permissions::table.on(
                    permissions::collection_id.eq(collection_authorization_state::collection_id),
                ))
                .filter(collection_authorization_state::collection_id.eq(query.collection_id()))
                .select((
                    collection_authorization_state::revision,
                    Option::<PermissionRow>::as_select(),
                ))
                .load::<(crate::models::ResourceRevision, Option<PermissionRow>)>(conn)
                .await
        }
    })
    .await?;

    let revision = rows
        .as_slice()
        .first()
        .map(|(revision, _)| revision.get())
        .ok_or_else(|| {
            ApiError::NotFound(format!("Collection {} not found", query.collection_id()))
        })?;
    let grants = rows
        .into_iter()
        .filter_map(|(_, grant)| grant.map(grant_to_storage))
        .collect();
    Ok(AuthorizationPermissionSet::new(
        query.collection_id(),
        revision,
        grants,
    ))
}

pub(crate) async fn apply_local_collection_grant(
    pool: &PostgresPool,
    mutation: AuthorizationGrantMutation,
) -> Result<AuthorizationGrant, ApiError> {
    let key = mutation.key();
    let permissions = PermissionsList::new(
        mutation
            .permissions()
            .iter()
            .copied()
            .map(permission_from_storage),
    );
    let grant = permission_backend::apply_permission_grant(
        pool,
        key.collection_id(),
        key.group_id(),
        permissions,
        mutation.replace_existing(),
        mutation.event_context_value(),
    )
    .await?;
    Ok(grant_to_storage(grant))
}

pub(crate) async fn revoke_local_collection_grant(
    pool: &PostgresPool,
    mutation: AuthorizationGrantMutation,
) -> Result<AuthorizationGrant, ApiError> {
    let key = mutation.key();
    let permissions = PermissionsList::new(
        mutation
            .permissions()
            .iter()
            .copied()
            .map(permission_from_storage),
    );
    let grant = permission_backend::revoke_permission_grant(
        pool,
        key.collection_id(),
        key.group_id(),
        permissions,
        mutation.event_context_value(),
    )
    .await?;
    Ok(grant_to_storage(grant))
}

pub(crate) async fn revoke_all_local_collection_grants(
    pool: &PostgresPool,
    request: AuthorizationGrantDelete,
) -> Result<(), ApiError> {
    let key = request.key();
    permission_backend::revoke_all_permission_grants(
        pool,
        key.collection_id(),
        key.group_id(),
        request.event_context_value(),
    )
    .await
}
