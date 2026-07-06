use super::*;
use crate::db::traits::authz::AuthzSubject;
fn permission_filter_sql(permission: Permissions, target: bool) -> &'static str {
    match (permission, target) {
        (Permissions::ReadCollection, true) => "permissions.has_read_collection = TRUE",
        (Permissions::ReadCollection, false) => "permissions.has_read_collection = FALSE",
        (Permissions::UpdateCollection, true) => "permissions.has_update_collection = TRUE",
        (Permissions::UpdateCollection, false) => "permissions.has_update_collection = FALSE",
        (Permissions::DeleteCollection, true) => "permissions.has_delete_collection = TRUE",
        (Permissions::DeleteCollection, false) => "permissions.has_delete_collection = FALSE",
        (Permissions::DelegateCollection, true) => "permissions.has_delegate_collection = TRUE",
        (Permissions::DelegateCollection, false) => "permissions.has_delegate_collection = FALSE",
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
        (Permissions::ReadTemplate, true) => "permissions.has_read_template = TRUE",
        (Permissions::ReadTemplate, false) => "permissions.has_read_template = FALSE",
        (Permissions::CreateTemplate, true) => "permissions.has_create_template = TRUE",
        (Permissions::CreateTemplate, false) => "permissions.has_create_template = FALSE",
        (Permissions::UpdateTemplate, true) => "permissions.has_update_template = TRUE",
        (Permissions::UpdateTemplate, false) => "permissions.has_update_template = FALSE",
        (Permissions::DeleteTemplate, true) => "permissions.has_delete_template = TRUE",
        (Permissions::DeleteTemplate, false) => "permissions.has_delete_template = FALSE",
        (Permissions::ReadRemoteTarget, true) => "permissions.has_read_remote_target = TRUE",
        (Permissions::ReadRemoteTarget, false) => "permissions.has_read_remote_target = FALSE",
        (Permissions::CreateRemoteTarget, true) => "permissions.has_create_remote_target = TRUE",
        (Permissions::CreateRemoteTarget, false) => "permissions.has_create_remote_target = FALSE",
        (Permissions::UpdateRemoteTarget, true) => "permissions.has_update_remote_target = TRUE",
        (Permissions::UpdateRemoteTarget, false) => "permissions.has_update_remote_target = FALSE",
        (Permissions::DeleteRemoteTarget, true) => "permissions.has_delete_remote_target = TRUE",
        (Permissions::DeleteRemoteTarget, false) => "permissions.has_delete_remote_target = FALSE",
        (Permissions::ExecuteRemoteTarget, true) => "permissions.has_execute_remote_target = TRUE",
        (Permissions::ExecuteRemoteTarget, false) => {
            "permissions.has_execute_remote_target = FALSE"
        }
        (Permissions::ReadAudit, true) => "permissions.has_read_audit = TRUE",
        (Permissions::ReadAudit, false) => "permissions.has_read_audit = FALSE",
        (Permissions::ManageEventSubscription, true) => {
            "permissions.has_manage_event_subscription = TRUE"
        }
        (Permissions::ManageEventSubscription, false) => {
            "permissions.has_manage_event_subscription = FALSE"
        }
    }
}

pub async fn total_collection_count_from_backend(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::collections::dsl::*;

    with_connection(pool, |conn| collections.count().get_result::<i64>(conn))
}

pub async fn principal_on_from_backend<S: AuthzSubject, T: CollectionAccessors>(
    pool: &DbPool,
    principal: S,
    collection_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{collection_id, group_id, permissions};

    let collection_target_id = collection_ref.collection_id(pool).await?.id();
    let group_ids_subquery = principal.group_ids_subquery();
    let rows = with_connection(pool, |conn| {
        groups
            .inner_join(permissions.on(group_table_id.eq(group_id)))
            .filter(collection_id.eq(collection_target_id))
            .filter(group_id.eq_any(group_ids_subquery))
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;

    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
}

/// All of a principal's effective permissions across every collection, as
/// `(collection, group, permission-row)` tuples — one per `(collection, group)`
/// where a group the principal belongs to holds a permission. The handler folds
/// these into a per-collection, per-group report.
pub async fn principal_all_permissions_from_backend<S: AuthzSubject>(
    pool: &DbPool,
    principal: S,
) -> Result<Vec<(Collection, Group, Permission)>, ApiError> {
    use crate::schema::permissions::dsl::{group_id, permissions};
    use diesel::SelectableHelper;

    let group_ids_subquery = principal.group_ids_subquery();
    with_connection(pool, |conn| {
        permissions
            .inner_join(crate::schema::groups::table)
            .inner_join(crate::schema::collections::table)
            .filter(group_id.eq_any(group_ids_subquery))
            .select((
                Collection::as_select(),
                Group::as_select(),
                Permission::as_select(),
            ))
            .load::<(Collection, Group, Permission)>(conn)
    })
}

pub async fn principal_on_paginated_with_total_count_from_backend<
    S: AuthzSubject,
    T: CollectionAccessors,
>(
    pool: &DbPool,
    principal: S,
    collection_ref: T,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groupname, groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        collection_id, created_at as permission_created_at, group_id, id as permission_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search, string_search};

    let collection_target_id = collection_ref.collection_id(pool).await?.id();
    let build_query = || -> Result<_, ApiError> {
        let group_ids_subquery = principal.group_ids_subquery();
        let mut query = groups
            .inner_join(permissions.on(group_table_id.eq(group_id)))
            .filter(collection_id.eq(collection_target_id))
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

        Ok(query)
    };

    let query = build_query()?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_query()?;
    crate::apply_query_options!(query, query_options, GroupPermission);
    let rows = with_connection(pool, |conn| query.load::<(Group, Permission)>(conn))?;

    Ok((
        rows.into_iter().map(GroupPermission::from_tuple).collect(),
        total_count,
    ))
}

pub async fn user_can_on_any_from_backend<U: GroupAccessors + AuthzSubject>(
    pool: &DbPool,
    user_id: U,
    permission_type: Permissions,
    scopes: Option<&[Permissions]>,
) -> Result<Vec<Collection>, ApiError> {
    use crate::db::traits::authz::scope_allows;
    use crate::schema::permissions::dsl::*;

    // Fail-closed scope pre-filter: a scoped token that does not include the
    // requested permission can see nothing, before any grant/admin check.
    if !scope_allows(scopes, &[permission_type]) {
        return Ok(vec![]);
    }

    // The admin "all collections" fast path applies only to unscoped tokens; a
    // scoped admin token falls through to the scoped grant query below.
    if scopes.is_none() && AuthzSubject::is_admin(&user_id, pool).await? {
        return with_connection(pool, |conn| {
            crate::schema::collections::table.load::<Collection>(conn)
        });
    }

    let base_query = {
        let group_ids_subquery = user_id.group_ids_subquery();

        permissions
            .into_boxed()
            .filter(group_id.eq_any(group_ids_subquery))
    };

    let filtered_query = permission_type.create_boxed_filter(base_query, true);
    let accessible_collection_ids = with_connection(pool, |conn| {
        filtered_query.select(collection_id).load::<i32>(conn)
    })?;

    if accessible_collection_ids.is_empty() {
        return Ok(vec![]);
    }

    with_connection(pool, |conn| {
        crate::schema::collections::table
            .filter(crate::schema::collections::id.eq_any(accessible_collection_ids))
            .load::<Collection>(conn)
    })
}

pub async fn group_can_on_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    gid: i32,
    collection_ref: T,
    permission_type: Permissions,
) -> Result<bool, ApiError> {
    use crate::schema::permissions::dsl::*;

    let base_query = permissions
        .into_boxed()
        .filter(group_id.eq(gid))
        .filter(collection_id.eq(collection_ref.collection_id(pool).await?.id()));

    let filtered_query = permission_type.create_boxed_filter(base_query, true);
    let result = with_connection(pool, |conn| filtered_query.execute(conn))?;

    Ok(result != 0)
}

pub async fn groups_can_on_from_backend(
    pool: &DbPool,
    target_collection_id: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError> {
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::*;

    let base_query = permissions
        .into_boxed()
        .filter(collection_id.eq(target_collection_id));
    let filtered_query = permission_type.create_boxed_filter(base_query, true);

    let group_ids = with_connection(pool, |conn| {
        filtered_query.select(group_id).distinct().load::<i32>(conn)
    })?;

    if group_ids.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, |conn| {
        groups
            .filter(group_table_id.eq_any(group_ids))
            .load::<Group>(conn)
    })
}

pub async fn groups_can_on_paginated_with_total_count_from_backend(
    pool: &DbPool,
    target_collection_id: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError> {
    use crate::schema::groups::dsl::{
        created_at, description, groupname, groups, id as group_table_id, updated_at,
    };
    use crate::schema::permissions::dsl::*;
    use crate::{date_search, numeric_search, string_search};

    let build_query = || -> Result<_, ApiError> {
        let base_query = permissions
            .into_boxed()
            .filter(collection_id.eq(target_collection_id));
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

        Ok(query)
    };

    let query = build_query()?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_query()?;
    crate::apply_query_options!(query, query_options, Group);
    let items = with_connection(pool, |conn| query.load::<Group>(conn))?;

    Ok((items, total_count))
}

pub async fn groups_on_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        collection_id, created_at as permission_created_at, group_id, id as permission_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search};

    let collection_target_id = collection_ref.collection_id(pool).await?.id();
    let query_params = query_options.filters;

    let mut permission_filters = query_params.permissions()?;
    permission_filters.ensure_contains(&permissions_filter);

    let mut base_query = permissions
        .filter(collection_id.eq(collection_target_id))
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
            FilterField::Permissions => {}
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for permissions",
                    param.field
                )));
            }
        }
    }

    for order in &query_options.sort {
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

    let rows = with_connection(pool, |conn| {
        base_query
            .inner_join(groups.on(group_table_id.eq(group_id)))
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;

    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
}

pub async fn groups_on_paginated_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    let (items, _) = groups_on_paginated_with_total_count_from_backend(
        pool,
        collection_ref,
        permissions_filter,
        query_options,
    )
    .await?;
    Ok(items)
}

pub async fn groups_on_paginated_with_total_count_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError> {
    use crate::models::traits::output::FromTuple;
    use crate::schema::groups::dsl::{groupname, groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        collection_id, created_at as permission_created_at, group_id, id as permission_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search, string_search};

    let collection_target_id = collection_ref.collection_id(pool).await?.id();
    let mut permission_filters = query_options.filters.permissions()?;
    permission_filters.ensure_contains(&permissions_filter);

    let build_query = || -> Result<_, ApiError> {
        let mut query = groups
            .inner_join(permissions.on(group_table_id.eq(group_id)))
            .filter(collection_id.eq(collection_target_id))
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

        Ok(query)
    };

    let query = build_query()?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_query()?;
    crate::apply_query_options!(query, query_options, GroupPermission);
    let rows = with_connection(pool, |conn| {
        query
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;

    Ok((
        rows.into_iter().map(GroupPermission::from_tuple).collect(),
        total_count,
    ))
}

pub async fn count_groups_on_paginated_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    collection_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<i64, ApiError> {
    let (_, total_count) = groups_on_paginated_with_total_count_from_backend(
        pool,
        collection_ref,
        permissions_filter,
        query_options,
    )
    .await?;
    Ok(total_count)
}

pub async fn group_on_from_backend(
    pool: &DbPool,
    target_collection_id: i32,
    gid: i32,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;

    with_connection(pool, |conn| {
        permissions
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(gid))
            .first::<Permission>(conn)
    })
}
