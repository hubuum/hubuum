use std::collections::{BTreeSet, HashMap};

use diesel::dsl::{count, exists, select};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AggregateExpressionMethods, BoolExpressionMethods, JoinOnDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId, GroupId, ObjectId, PrincipalId, ResourceId};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantKey,
    AuthorizationGroup, AuthorizationGroupCollectionQuery, AuthorizationGroupGrant,
    AuthorizationGroupMembershipQuery, AuthorizationObjectResource, AuthorizationPermission,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationPrincipalCollectionPageQuery,
    AuthorizationPrincipalCollectionQuery, AuthorizationResourceIds, StorageCountedPage,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

use super::rows::{CollectionRow, GroupRow, PermissionRow};

const SKIPPED_TOTAL_COUNT: i64 = -1;

pub async fn get_authorization_principal(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<AuthorizationPrincipal, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::group_memberships;

            let group_ids = group_memberships::table
                .filter(group_memberships::principal_id.eq(principal_id))
                .order_by(group_memberships::group_id.asc())
                .select(group_memberships::group_id)
                .load::<i32>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(AuthorizationPrincipal::new(
                PrincipalId::new(principal_id)?,
                group_ids
                    .into_iter()
                    .map(GroupId::new)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        })
        .await
}

pub async fn is_authorization_principal_group_member(
    runtime: &PostgresRuntime,
    query: AuthorizationGroupMembershipQuery,
) -> Result<bool, PostgresStorageError> {
    use crate::schema::{group_memberships, groups, identity_scopes};

    let principal_id = query.principal_id().id();
    let group_name = query.group_name().to_string();
    let identity_scope = query.identity_scope().to_string();
    runtime
        .with_connection(async move |connection| {
            select(exists(
                group_memberships::table
                    .inner_join(groups::table)
                    .inner_join(
                        identity_scopes::table
                            .on(groups::identity_scope_id.eq(identity_scopes::id)),
                    )
                    .filter(group_memberships::principal_id.eq(principal_id))
                    .filter(groups::groupname.eq(group_name))
                    .filter(identity_scopes::name.eq(identity_scope)),
            ))
            .get_result(connection)
            .await
        })
        .await
}

pub async fn get_authorization_classes(
    runtime: &PostgresRuntime,
    query: AuthorizationResourceIds,
) -> Result<Vec<AuthorizationClassResource>, PostgresStorageError> {
    let ids = query
        .ids()
        .iter()
        .copied()
        .map(ResourceId::id)
        .collect::<Vec<_>>();
    runtime
        .with_connection(async |connection| {
            use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, id};

            let rows = hubuumclass
                .filter(id.eq_any(ids))
                .select((id, collection_id))
                .load::<(i32, i32)>(connection)
                .await?;
            rows.into_iter()
                .map(|(class_id, owning_collection_id)| {
                    Ok(AuthorizationClassResource::new(
                        ClassId::new(class_id)?,
                        CollectionId::new(owning_collection_id)?,
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()
        })
        .await
}

pub async fn get_authorization_objects(
    runtime: &PostgresRuntime,
    query: AuthorizationResourceIds,
) -> Result<Vec<AuthorizationObjectResource>, PostgresStorageError> {
    let ids = query
        .ids()
        .iter()
        .copied()
        .map(ResourceId::id)
        .collect::<Vec<_>>();
    runtime
        .with_connection(async |connection| {
            use crate::schema::hubuumobject::dsl::{
                collection_id, hubuum_class_id, hubuumobject, id, name,
            };

            let rows = hubuumobject
                .filter(id.eq_any(ids))
                .select((id, collection_id, hubuum_class_id, name))
                .load::<(i32, i32, i32, String)>(connection)
                .await?;
            rows.into_iter()
                .map(|(object_id, owning_collection_id, class_id, object_name)| {
                    Ok(AuthorizationObjectResource::new(
                        ObjectId::new(object_id)?,
                        CollectionId::new(owning_collection_id)?,
                        ClassId::new(class_id)?,
                        object_name,
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()
        })
        .await
}

pub async fn authorize_local_collection(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionAccessQuery,
) -> Result<bool, PostgresStorageError> {
    use crate::schema::{group_memberships, permissions};

    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(query.principal_id().id()))
        .select(group_memberships::group_id);
    let mut permission_query = permissions::table
        .filter(permissions::collection_id.eq(query.collection_id().id()))
        .filter(permissions::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in query.permissions().iter().copied() {
        apply_permission_filter!(permission_query, permission, true);
    }
    runtime
        .with_connection(async |connection| {
            select(exists(permission_query))
                .get_result(connection)
                .await
        })
        .await
}

pub async fn authorize_local_collections(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionsAccessQuery,
) -> Result<bool, PostgresStorageError> {
    use crate::schema::{collection_closure, group_memberships, permissions};

    if query.collection_ids().is_empty() {
        return Ok(true);
    }
    let collection_ids = query
        .collection_ids()
        .iter()
        .copied()
        .map(CollectionId::id)
        .collect::<Vec<_>>();
    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(query.principal_id().id()))
        .select(group_memberships::group_id);
    let mut permission_query = permissions::table
        .inner_join(
            collection_closure::table
                .on(permissions::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .select(permissions::all_columns)
        .filter(collection_closure::descendant_collection_id.eq_any(&collection_ids))
        .filter(permissions::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in query.permissions().iter().copied() {
        apply_permission_filter!(permission_query, permission, true);
    }
    let matching = runtime
        .with_connection(async |connection| {
            permission_query
                .select(count(collection_closure::descendant_collection_id).aggregate_distinct())
                .first::<i64>(connection)
                .await
        })
        .await?;
    Ok(matching as usize == query.collection_ids().len())
}

pub async fn list_local_authorized_collections(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionsQuery,
) -> Result<Vec<AuthorizationCollection>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::collections;

            let ids = if query.permissions().is_empty() {
                collection_ids_with_any_grant(connection, query.principal_id().id()).await?
            } else {
                let mut intersection: Option<BTreeSet<i32>> = None;
                for permission in query.permissions().iter().copied() {
                    let ids = authorized_collection_ids_for_permission(
                        connection,
                        query.principal_id().id(),
                        permission,
                    )
                    .await?
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                    intersection = Some(match intersection {
                        Some(current) => current.intersection(&ids).copied().collect(),
                        None => ids,
                    });
                }
                intersection.unwrap_or_default().into_iter().collect()
            };
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let rows = collections::table
                .filter(collections::id.eq_any(ids))
                .load::<CollectionRow>(connection)
                .await?;
            rows.into_iter().map(CollectionRow::into_storage).collect()
        })
        .await
}

async fn collection_ids_with_any_grant(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<Vec<i32>, PostgresStorageError> {
    use crate::schema::{group_memberships, permissions};

    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(principal_id))
        .select(group_memberships::group_id);
    permissions::table
        .filter(permissions::group_id.eq_any(group_ids))
        .select(permissions::collection_id)
        .distinct()
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn authorized_collection_ids_for_permission(
    connection: &mut PostgresConnection,
    principal_id: i32,
    permission: AuthorizationPermission,
) -> Result<Vec<i32>, PostgresStorageError> {
    use crate::schema::{collection_closure, group_memberships, permissions};

    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(principal_id))
        .select(group_memberships::group_id);
    let mut query = permissions::table
        .inner_join(
            collection_closure::table
                .on(permissions::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .filter(permissions::group_id.eq_any(group_ids))
        .into_boxed();
    apply_permission_filter!(query, permission, true);
    query
        .select(collection_closure::descendant_collection_id)
        .distinct()
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

pub async fn list_authorization_collection_candidates(
    runtime: &PostgresRuntime,
) -> Result<Vec<AuthorizationCollection>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::collections;

            let rows = collections::table
                .order_by(collections::id.asc())
                .load::<CollectionRow>(connection)
                .await?;
            rows.into_iter().map(CollectionRow::into_storage).collect()
        })
        .await
}

pub async fn list_authorization_group_candidates(
    runtime: &PostgresRuntime,
    options: QueryOptions,
) -> Result<Vec<AuthorizationGroup>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::groups::dsl::{created_at, groupname, groups, id, updated_at};

            let mut query = groups.into_boxed();
            for parameter in options.filters() {
                match parameter.field {
                    FilterField::Id => crate::postgres_integer_filter!(query, parameter, id),
                    FilterField::Name | FilterField::Groupname => {
                        crate::postgres_string_filter!(query, parameter, groupname)
                    }
                    FilterField::CreatedAt => {
                        crate::postgres_datetime_filter!(query, parameter, created_at)
                    }
                    FilterField::UpdatedAt => {
                        crate::postgres_datetime_filter!(query, parameter, updated_at)
                    }
                    FilterField::Permissions => {}
                    _ => {
                        return Err(PostgresStorageError::bad_request(format!(
                            "Field '{}' isn't searchable (or does not exist) for permissions",
                            parameter.field
                        )));
                    }
                }
            }
            let rows = query.load::<GroupRow>(connection).await?;
            rows.into_iter().map(GroupRow::into_storage).collect()
        })
        .await
}

pub async fn get_authorization_policy_snapshot(
    runtime: &PostgresRuntime,
) -> Result<Vec<AuthorizationPolicySnapshotRow>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::{collections, groups, permissions};

            let rows = permissions::table
                .inner_join(groups::table.on(permissions::group_id.eq(groups::id)))
                .inner_join(collections::table.on(permissions::collection_id.eq(collections::id)))
                .order_by((
                    permissions::collection_id.asc(),
                    permissions::group_id.asc(),
                ))
                .select((
                    PermissionRow::as_select(),
                    GroupRow::as_select(),
                    CollectionRow::as_select(),
                ))
                .load::<(PermissionRow, GroupRow, CollectionRow)>(connection)
                .await?;
            rows.into_iter()
                .map(|(grant, group, collection)| {
                    Ok::<_, PostgresStorageError>(AuthorizationPolicySnapshotRow::new(
                        grant.into_storage()?,
                        group.into_storage()?,
                        collection.into_storage()?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

/// Return the direct grants held by a principal's groups on one collection.
pub async fn list_principal_collection_permissions(
    runtime: &PostgresRuntime,
    query: AuthorizationPrincipalCollectionQuery,
) -> Result<Vec<AuthorizationGroupGrant>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::{group_memberships, groups, permissions};

            let group_ids = group_memberships::table
                .filter(group_memberships::principal_id.eq(query.principal_id().id()))
                .select(group_memberships::group_id);
            let rows = groups::table
                .inner_join(permissions::table)
                .filter(permissions::collection_id.eq(query.collection_id().id()))
                .filter(permissions::group_id.eq_any(group_ids))
                .order_by(permissions::id.asc())
                .select((GroupRow::as_select(), PermissionRow::as_select()))
                .load::<(GroupRow, PermissionRow)>(connection)
                .await?;
            group_grants_from_rows(rows)
        })
        .await
}

/// Return every direct policy row held by a principal's groups.
pub async fn list_all_principal_collection_permissions(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<Vec<AuthorizationPolicySnapshotRow>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::{collections, group_memberships, groups, permissions};

            let group_ids = group_memberships::table
                .filter(group_memberships::principal_id.eq(principal_id))
                .select(group_memberships::group_id);
            let rows = permissions::table
                .inner_join(groups::table)
                .inner_join(collections::table)
                .filter(permissions::group_id.eq_any(group_ids))
                .order_by((
                    permissions::collection_id.asc(),
                    permissions::group_id.asc(),
                ))
                .select((
                    PermissionRow::as_select(),
                    GroupRow::as_select(),
                    CollectionRow::as_select(),
                ))
                .load::<(PermissionRow, GroupRow, CollectionRow)>(connection)
                .await?;
            rows.into_iter()
                .map(|(grant, group, collection)| {
                    Ok::<_, PostgresStorageError>(AuthorizationPolicySnapshotRow::new(
                        grant.into_storage()?,
                        group.into_storage()?,
                        collection.into_storage()?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

/// Return a stable cursor page of a principal's direct grants.
pub async fn list_principal_collection_permissions_page(
    runtime: &PostgresRuntime,
    query: AuthorizationPrincipalCollectionPageQuery,
) -> Result<StorageCountedPage<AuthorizationGroupGrant>, PostgresStorageError> {
    if query.query_options().include_total() {
        runtime
            .with_read_only_snapshot(async move |connection| {
                let total = build_principal_grant_query(&query)?
                    .count()
                    .get_result::<i64>(connection)
                    .await?;
                let items = load_principal_grants(connection, &query).await?;
                Ok::<_, PostgresStorageError>(StorageCountedPage::new(items, total))
            })
            .await
    } else {
        runtime
            .with_connection(async move |connection| {
                let items = load_principal_grants(connection, &query).await?;
                Ok::<_, PostgresStorageError>(StorageCountedPage::new(items, SKIPPED_TOTAL_COUNT))
            })
            .await
    }
}

/// Return direct and inherited grants held by a principal's groups.
pub async fn list_effective_principal_collection_permissions(
    runtime: &PostgresRuntime,
    query: AuthorizationPrincipalCollectionQuery,
) -> Result<Vec<AuthorizationEffectiveGroupGrant>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::{collection_closure, group_memberships, groups, permissions};

            let group_ids = group_memberships::table
                .filter(group_memberships::principal_id.eq(query.principal_id().id()))
                .select(group_memberships::group_id);
            let rows = groups::table
                .inner_join(permissions::table)
                .inner_join(
                    collection_closure::table
                        .on(permissions::collection_id
                            .eq(collection_closure::ancestor_collection_id)),
                )
                .filter(collection_closure::descendant_collection_id.eq(query.collection_id().id()))
                .filter(permissions::group_id.eq_any(group_ids))
                .order_by((
                    collection_closure::depth.asc(),
                    groups::id.asc(),
                    permissions::collection_id.asc(),
                ))
                .select((
                    collection_closure::ancestor_collection_id,
                    collection_closure::depth,
                    GroupRow::as_select(),
                    PermissionRow::as_select(),
                ))
                .load::<(i32, i32, GroupRow, PermissionRow)>(connection)
                .await?;
            hydrate_effective_grants(connection, query.collection_id().id(), rows).await
        })
        .await
}

/// Return collections visible through one permission and an optional token scope.
pub async fn list_visible_collections(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionVisibilityQuery,
) -> Result<Vec<AuthorizationCollection>, PostgresStorageError> {
    let (principal_id, is_admin, permission, scope) = query.into_parts();
    let principal_id = principal_id.id();
    let (permission_scope, resource_scope) = scope
        .map(|scope| scope.into_parts())
        .unwrap_or((None, None));
    if let Some(permission_scope) = permission_scope {
        let allowed = permission_scope
            .iter()
            .map(|name| AuthorizationPermission::from_name(name))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                PostgresStorageError::database(format!(
                    "Invalid permission in persisted token scope: {error}"
                ))
            })?;
        if !allowed.contains(&permission) {
            return Ok(Vec::new());
        }
    }
    let collection_scope = resource_scope
        .map(|scope| scope.into_parts().0)
        .map(|ids| normalized_ids(ids.into_iter().map(CollectionId::id).collect()));

    runtime
        .with_connection(async move |connection| {
            use crate::schema::{collection_closure, collections, group_memberships, permissions};

            let mut collections_query = collections::table.into_boxed();
            if let Some(collection_ids) = collection_scope.as_ref() {
                collections_query =
                    collections_query.filter(collections::id.eq_any(collection_ids));
            }
            if !is_admin {
                let group_ids = group_memberships::table
                    .filter(group_memberships::principal_id.eq(principal_id))
                    .select(group_memberships::group_id);
                let mut grants = permissions::table
                    .filter(permissions::group_id.eq_any(group_ids))
                    .into_boxed();
                apply_permission_filter!(grants, permission, true);
                let visible_ids = grants
                    .inner_join(collection_closure::table.on(
                        permissions::collection_id.eq(collection_closure::ancestor_collection_id),
                    ))
                    .select(collection_closure::descendant_collection_id)
                    .distinct();
                collections_query = collections_query.filter(collections::id.eq_any(visible_ids));
            }
            let rows = collections_query
                .order_by(collections::id.asc())
                .load::<CollectionRow>(connection)
                .await?;
            rows.into_iter().map(CollectionRow::into_storage).collect()
        })
        .await
}

/// Return whether a group's direct or inherited grant contains one permission.
pub async fn has_group_collection_permission(
    runtime: &PostgresRuntime,
    query: AuthorizationGroupCollectionQuery,
) -> Result<bool, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::{collection_closure, permissions};

            let mut grants = permissions::table
                .filter(permissions::group_id.eq(query.group_id().id()))
                .into_boxed();
            apply_permission_filter!(grants, query.permission(), true);
            let count = grants
                .inner_join(
                    collection_closure::table
                        .on(permissions::collection_id
                            .eq(collection_closure::ancestor_collection_id)),
                )
                .filter(collection_closure::descendant_collection_id.eq(query.collection_id().id()))
                .count()
                .get_result::<i64>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(count != 0)
        })
        .await
}

/// Return direct and inherited grants for one group on one collection.
pub async fn list_effective_group_collection_permissions(
    runtime: &PostgresRuntime,
    collection_id: i32,
    group_id: i32,
) -> Result<Vec<AuthorizationEffectiveGroupGrant>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::{collection_closure, groups, permissions};

            let rows =
                groups::table
                    .inner_join(permissions::table)
                    .inner_join(collection_closure::table.on(
                        permissions::collection_id.eq(collection_closure::ancestor_collection_id),
                    ))
                    .filter(collection_closure::descendant_collection_id.eq(collection_id))
                    .filter(permissions::group_id.eq(group_id))
                    .order_by((
                        collection_closure::depth.asc(),
                        groups::id.asc(),
                        permissions::collection_id.asc(),
                    ))
                    .select((
                        collection_closure::ancestor_collection_id,
                        collection_closure::depth,
                        GroupRow::as_select(),
                        PermissionRow::as_select(),
                    ))
                    .load::<(i32, i32, GroupRow, PermissionRow)>(connection)
                    .await?;
            hydrate_effective_grants(connection, collection_id, rows).await
        })
        .await
}

/// Return every group with one direct or inherited permission.
pub async fn list_groups_with_collection_permission(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionGroupsQuery,
) -> Result<Vec<AuthorizationGroup>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            let rows = build_groups_with_permission_query(query)?
                .order_by(crate::schema::groups::id.asc())
                .load::<GroupRow>(connection)
                .await?;
            rows.into_iter().map(GroupRow::into_storage).collect()
        })
        .await
}

/// Return a stable cursor page of groups with one effective permission.
pub async fn list_groups_with_collection_permission_page(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionGroupsPageQuery,
) -> Result<StorageCountedPage<AuthorizationGroup>, PostgresStorageError> {
    if query.query_options().include_total() {
        runtime
            .with_read_only_snapshot(async move |connection| {
                let total = build_groups_page_query(&query)?
                    .count()
                    .get_result::<i64>(connection)
                    .await?;
                let groups = load_groups_page(connection, &query).await?;
                Ok::<_, PostgresStorageError>(StorageCountedPage::new(groups, total))
            })
            .await
    } else {
        runtime
            .with_connection(async move |connection| {
                let groups = load_groups_page(connection, &query).await?;
                Ok::<_, PostgresStorageError>(StorageCountedPage::new(groups, SKIPPED_TOTAL_COUNT))
            })
            .await
    }
}

fn group_grants_from_rows(
    rows: Vec<(GroupRow, PermissionRow)>,
) -> Result<Vec<AuthorizationGroupGrant>, PostgresStorageError> {
    rows.into_iter()
        .map(|(group, grant)| {
            Ok(AuthorizationGroupGrant::new(
                group.into_storage()?,
                grant.into_storage()?,
            ))
        })
        .collect()
}

fn normalized_ids(mut ids: Vec<i32>) -> Vec<i32> {
    ids.sort_unstable();
    ids.dedup();
    ids
}

async fn hydrate_effective_grants(
    connection: &mut PostgresConnection,
    target_collection_id: i32,
    rows: Vec<(i32, i32, GroupRow, PermissionRow)>,
) -> Result<Vec<AuthorizationEffectiveGroupGrant>, PostgresStorageError> {
    use crate::schema::collections;

    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut collection_ids = rows
        .iter()
        .map(|(source_collection_id, _, _, _)| *source_collection_id)
        .collect::<Vec<_>>();
    collection_ids.push(target_collection_id);
    let collection_ids = normalized_ids(collection_ids);
    let collections = collections::table
        .filter(collections::id.eq_any(collection_ids))
        .load::<CollectionRow>(connection)
        .await?
        .into_iter()
        .map(|row| Ok((row.id, row.into_storage()?)))
        .collect::<Result<HashMap<_, _>, PostgresStorageError>>()?;
    let target = collections
        .get(&target_collection_id)
        .cloned()
        .ok_or_else(|| {
            PostgresStorageError::database(format!(
                "Missing target collection {target_collection_id} for effective permission"
            ))
        })?;

    rows.into_iter()
        .map(|(source_collection_id, depth, group, grant)| {
            let source = collections
                .get(&source_collection_id)
                .cloned()
                .ok_or_else(|| {
                    PostgresStorageError::database(format!(
                        "Missing source collection {source_collection_id} for effective permission"
                    ))
                })?;
            Ok(AuthorizationEffectiveGroupGrant::new(
                target.clone(),
                source,
                depth,
                depth > 0,
                group.into_storage()?,
                grant.into_storage()?,
            ))
        })
        .collect()
}

fn build_principal_grant_query(
    query: &AuthorizationPrincipalCollectionPageQuery,
) -> Result<
    diesel::dsl::IntoBoxed<
        'static,
        diesel::dsl::InnerJoin<crate::schema::groups::table, crate::schema::permissions::table>,
        diesel::pg::Pg,
    >,
    PostgresStorageError,
> {
    use crate::schema::{group_memberships, groups, permissions};

    let principal = query.principal();
    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(principal.principal_id().id()))
        .select(group_memberships::group_id);
    let mut records = groups::table
        .inner_join(permissions::table)
        .filter(permissions::collection_id.eq(principal.collection_id().id()))
        .filter(permissions::group_id.eq_any(group_ids))
        .into_boxed();
    for parameter in query.query_options().filters() {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(records, parameter, permissions::id)
            }
            FilterField::Name | FilterField::Groupname => {
                crate::postgres_string_filter!(records, parameter, groups::groupname)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(records, parameter, permissions::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(records, parameter, permissions::updated_at)
            }
            FilterField::Permissions => {
                let permission = parse_permission_filter(parameter)?;
                apply_permission_filter!(records, permission, true);
            }
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

async fn load_principal_grants(
    connection: &mut PostgresConnection,
    query: &AuthorizationPrincipalCollectionPageQuery,
) -> Result<Vec<AuthorizationGroupGrant>, PostgresStorageError> {
    let mut records = build_principal_grant_query(query)?;
    let fields = query
        .query_options()
        .sort()
        .iter()
        .map(|sort| group_grant_cursor_field(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    crate::apply_query_options_with_fields!(records, query.query_options(), fields);
    let rows = records
        .select((GroupRow::as_select(), PermissionRow::as_select()))
        .load::<(GroupRow, PermissionRow)>(connection)
        .await?;
    group_grants_from_rows(rows)
}

fn build_groups_with_permission_query(
    query: AuthorizationCollectionGroupsQuery,
) -> Result<crate::schema::groups::BoxedQuery<'static, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::{collection_closure, groups, permissions};

    let mut grants = permissions::table.into_boxed();
    apply_permission_filter!(grants, query.permission(), true);
    let group_ids = grants
        .inner_join(
            collection_closure::table
                .on(permissions::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .filter(collection_closure::descendant_collection_id.eq(query.collection_id().id()))
        .select(permissions::group_id)
        .distinct();
    Ok(groups::table
        .filter(groups::id.eq_any(group_ids))
        .into_boxed())
}

fn build_groups_page_query(
    query: &AuthorizationCollectionGroupsPageQuery,
) -> Result<crate::schema::groups::BoxedQuery<'static, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::groups;

    let mut groups = build_groups_with_permission_query(query.groups())?;
    for parameter in query.query_options().filters() {
        match parameter.field {
            FilterField::Id => crate::postgres_integer_filter!(groups, parameter, groups::id),
            FilterField::Name | FilterField::Groupname => {
                crate::postgres_string_filter!(groups, parameter, groups::groupname)
            }
            FilterField::Description => {
                crate::postgres_string_filter!(groups, parameter, groups::description)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(groups, parameter, groups::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(groups, parameter, groups::updated_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(groups, parameter, groups::revision)
            }
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable (or does not exist) for groups",
                    parameter.field
                )));
            }
        }
    }
    Ok(groups)
}

async fn load_groups_page(
    connection: &mut PostgresConnection,
    query: &AuthorizationCollectionGroupsPageQuery,
) -> Result<Vec<AuthorizationGroup>, PostgresStorageError> {
    let mut groups = build_groups_page_query(query)?;
    let fields = query
        .query_options()
        .sort()
        .iter()
        .map(|sort| group_cursor_field(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    crate::apply_query_options_with_fields!(groups, query.query_options(), fields);
    let rows = groups.load::<GroupRow>(connection).await?;
    rows.into_iter().map(GroupRow::into_storage).collect()
}

fn group_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("groups.id", CursorSqlType::Integer),
        FilterField::Name | FilterField::Groupname => {
            cursor_field("groups.groupname", CursorSqlType::String)
        }
        FilterField::Description => cursor_field("groups.description", CursorSqlType::String),
        FilterField::CreatedAt => cursor_field("groups.created_at", CursorSqlType::DateTime),
        FilterField::UpdatedAt => cursor_field("groups.updated_at", CursorSqlType::DateTime),
        FilterField::Revision => cursor_field("groups.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{field}' is not orderable for groups"
            )));
        }
    })
}

pub async fn list_local_collection_grants(
    runtime: &PostgresRuntime,
    query: AuthorizationCollectionGrantListQuery,
) -> Result<StorageCountedPage<AuthorizationGroupGrant>, PostgresStorageError> {
    let permissions = grant_query_permissions(&query)?;
    if query.query_options().include_total() {
        runtime
            .with_read_only_snapshot(async |connection| {
                let total = build_group_grant_query(&query, &permissions)?
                    .count()
                    .get_result::<i64>(connection)
                    .await?;
                let items = load_group_grants(connection, &query, &permissions).await?;
                Ok::<_, PostgresStorageError>(StorageCountedPage::new(items, total))
            })
            .await
    } else {
        runtime
            .with_connection(async |connection| {
                let items = load_group_grants(connection, &query, &permissions).await?;
                Ok::<_, PostgresStorageError>(StorageCountedPage::new(items, SKIPPED_TOTAL_COUNT))
            })
            .await
    }
}

async fn load_group_grants(
    connection: &mut PostgresConnection,
    query: &AuthorizationCollectionGrantListQuery,
    permissions: &[AuthorizationPermission],
) -> Result<Vec<AuthorizationGroupGrant>, PostgresStorageError> {
    let mut records = build_group_grant_query(query, permissions)?;
    let fields = query
        .query_options()
        .sort()
        .iter()
        .map(|sort| group_grant_cursor_field(&sort.field))
        .collect::<Result<Vec<_>, _>>()?;
    crate::apply_query_options_with_fields!(records, query.query_options(), fields);
    let rows = records
        .select((GroupRow::as_select(), PermissionRow::as_select()))
        .load::<(GroupRow, PermissionRow)>(connection)
        .await?;
    group_grants_from_rows(rows)
}

fn build_group_grant_query<'a>(
    query: &'a AuthorizationCollectionGrantListQuery,
    required_permissions: &'a [AuthorizationPermission],
) -> Result<
    diesel::dsl::IntoBoxed<
        'a,
        diesel::dsl::InnerJoin<crate::schema::groups::table, crate::schema::permissions::table>,
        diesel::pg::Pg,
    >,
    PostgresStorageError,
> {
    use crate::schema::{groups, permissions};

    let mut records = groups::table
        .inner_join(permissions::table)
        .filter(permissions::collection_id.eq(query.collection_id().id()))
        .into_boxed();
    for permission in required_permissions.iter().copied() {
        apply_permission_filter!(records, permission, true);
    }
    for parameter in query.query_options().filters() {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(records, parameter, permissions::id)
            }
            FilterField::Name | FilterField::Groupname => {
                crate::postgres_string_filter!(records, parameter, groups::groupname)
            }
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(records, parameter, permissions::created_at)
            }
            FilterField::UpdatedAt => {
                crate::postgres_datetime_filter!(records, parameter, permissions::updated_at)
            }
            FilterField::Permissions => {}
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

fn grant_query_permissions(
    query: &AuthorizationCollectionGrantListQuery,
) -> Result<Vec<AuthorizationPermission>, PostgresStorageError> {
    let mut permissions = query.required_permissions().to_vec();
    for parameter in query.query_options().filters() {
        if parameter.field == FilterField::Permissions {
            permissions.push(parse_permission_filter(parameter)?);
        }
    }
    permissions.sort_unstable();
    permissions.dedup();
    Ok(permissions)
}

fn parse_permission_filter(
    parameter: &hubuum_query::ParsedQueryParam,
) -> Result<AuthorizationPermission, PostgresStorageError> {
    AuthorizationPermission::from_name(&parameter.value)
        .map_err(|error| PostgresStorageError::bad_request(error.to_string()))
}

fn group_grant_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("permissions.id", CursorSqlType::Integer),
        FilterField::Name | FilterField::Groupname => {
            cursor_field("groups.groupname", CursorSqlType::String)
        }
        FilterField::CreatedAt => cursor_field("permissions.created_at", CursorSqlType::DateTime),
        FilterField::UpdatedAt => cursor_field("permissions.updated_at", CursorSqlType::DateTime),
        _ => {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{field}' is not orderable for group permissions"
            )));
        }
    })
}

const fn cursor_field(column: &'static str, sql_type: CursorSqlType) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable: false,
    }
}

pub async fn get_local_collection_grant(
    runtime: &PostgresRuntime,
    key: AuthorizationGrantKey,
) -> Result<Option<AuthorizationGrant>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::permissions;

            permissions::table
                .filter(permissions::collection_id.eq(key.collection_id().id()))
                .filter(permissions::group_id.eq(key.group_id().id()))
                .first::<PermissionRow>(connection)
                .await
                .optional()
                .map_err(PostgresStorageError::from)?
                .map(PermissionRow::into_storage)
                .transpose()
        })
        .await
}

pub async fn get_collection_group_permission(
    runtime: &PostgresRuntime,
    collection_id: i32,
    group_id: i32,
) -> Result<AuthorizationGrant, PostgresStorageError> {
    get_local_collection_grant(
        runtime,
        AuthorizationGrantKey::new(CollectionId::new(collection_id)?, GroupId::new(group_id)?),
    )
    .await?
    .ok_or_else(|| {
        PostgresStorageError::not_found(format!(
            "No grant exists for group {group_id} on collection {collection_id}"
        ))
    })
}

pub async fn get_local_collection_permission_set(
    runtime: &PostgresRuntime,
    query: AuthorizationPermissionSetQuery,
) -> Result<AuthorizationPermissionSet, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            use crate::schema::{collection_authorization_state, permissions};

            let collection_id = query.collection_id().id();
            let rows = if let Some(group_id) = query.group_id() {
                let group_id = group_id.id();
                collection_authorization_state::table
                    .left_join(
                        permissions::table.on(permissions::collection_id
                            .eq(collection_authorization_state::collection_id)
                            .and(permissions::group_id.eq(group_id))),
                    )
                    .filter(collection_authorization_state::collection_id.eq(collection_id))
                    .select((
                        collection_authorization_state::revision,
                        Option::<PermissionRow>::as_select(),
                    ))
                    .load::<(crate::PostgresRevision, Option<PermissionRow>)>(connection)
                    .await?
            } else {
                collection_authorization_state::table
                    .left_join(
                        permissions::table.on(permissions::collection_id
                            .eq(collection_authorization_state::collection_id)),
                    )
                    .filter(collection_authorization_state::collection_id.eq(collection_id))
                    .select((
                        collection_authorization_state::revision,
                        Option::<PermissionRow>::as_select(),
                    ))
                    .load::<(crate::PostgresRevision, Option<PermissionRow>)>(connection)
                    .await?
            };
            let revision = rows
                .as_slice()
                .first()
                .map(|(revision, _)| revision.into_domain())
                .ok_or_else(|| {
                    PostgresStorageError::not_found(format!(
                        "Collection {} not found",
                        query.collection_id()
                    ))
                })?;
            let grants = rows
                .into_iter()
                .filter_map(|(_, grant)| grant.map(PermissionRow::into_storage))
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, PostgresStorageError>(AuthorizationPermissionSet::new(
                query.collection_id(),
                revision,
                grants,
            ))
        })
        .await
}
