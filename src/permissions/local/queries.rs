use diesel::prelude::*;

use crate::db::traits::user::GroupIdsSubqueryBackend;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::permissions::PermissionFilter;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::traits::output::FromTuple;
use crate::models::{
    Group, GroupPermission, Namespace, NewPermission, Permission, Permissions, PermissionsList,
    UpdatePermission, UserID,
};
use crate::traits::NamespaceAccessors;

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
        (Permissions::ReadTemplate, true) => "permissions.has_read_template = TRUE",
        (Permissions::ReadTemplate, false) => "permissions.has_read_template = FALSE",
        (Permissions::CreateTemplate, true) => "permissions.has_create_template = TRUE",
        (Permissions::CreateTemplate, false) => "permissions.has_create_template = FALSE",
        (Permissions::UpdateTemplate, true) => "permissions.has_update_template = TRUE",
        (Permissions::UpdateTemplate, false) => "permissions.has_update_template = FALSE",
        (Permissions::DeleteTemplate, true) => "permissions.has_delete_template = TRUE",
        (Permissions::DeleteTemplate, false) => "permissions.has_delete_template = FALSE",
    }
}

pub(crate) async fn user_can_all_query(
    pool: &DbPool,
    user_id: i32,
    namespace_id: i32,
    permissions_requested: Vec<Permissions>,
) -> Result<bool, ApiError> {
    use crate::models::UserID;
    use crate::traits::GroupMemberships;

    let user = UserID(user_id);
    if user.is_admin(pool).await? {
        return Ok(true);
    }

    let lookup_table = crate::schema::permissions::dsl::permissions;
    let group_id_field = crate::schema::permissions::dsl::group_id;
    let namespace_id_field = crate::schema::permissions::dsl::namespace_id;
    let group_id_subquery = user.group_ids_subquery_from_backend();

    let mut base_query = lookup_table
        .into_boxed()
        .filter(namespace_id_field.eq(namespace_id))
        .filter(group_id_field.eq_any(group_id_subquery));

    for permission in permissions_requested {
        base_query = permission.create_boxed_filter(base_query, true);
    }

    let result: Option<crate::models::Permission> =
        with_connection(pool, |conn| base_query.first::<Permission>(conn).optional())?;

    Ok(result.is_some())
}

pub(crate) async fn apply_permissions_query(
    pool: &DbPool,
    namespace_id_param: i32,
    group_id_for_grant: i32,
    permission_list: PermissionsList<Permissions>,
    replace_existing: bool,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;

    with_transaction(pool, |conn| -> Result<Permission, ApiError> {
        let existing_entry = permissions
            .filter(namespace_id.eq(namespace_id_param))
            .filter(group_id.eq(group_id_for_grant))
            .first::<Permission>(conn)
            .optional()?;

        match existing_entry {
            Some(_) => {
                let mut update_perm = if replace_existing {
                    UpdatePermission {
                        has_read_namespace: Some(false),
                        has_update_namespace: Some(false),
                        has_delete_namespace: Some(false),
                        has_delegate_namespace: Some(false),
                        has_create_class: Some(false),
                        has_read_class: Some(false),
                        has_update_class: Some(false),
                        has_delete_class: Some(false),
                        has_create_object: Some(false),
                        has_read_object: Some(false),
                        has_update_object: Some(false),
                        has_delete_object: Some(false),
                        has_create_class_relation: Some(false),
                        has_read_class_relation: Some(false),
                        has_update_class_relation: Some(false),
                        has_delete_class_relation: Some(false),
                        has_create_object_relation: Some(false),
                        has_read_object_relation: Some(false),
                        has_update_object_relation: Some(false),
                        has_delete_object_relation: Some(false),
                        has_read_template: Some(false),
                        has_create_template: Some(false),
                        has_update_template: Some(false),
                        has_delete_template: Some(false),
                    }
                } else {
                    UpdatePermission::default()
                };

                for permission in permission_list.into_iter() {
                    match permission {
                        Permissions::ReadCollection => {
                            update_perm.has_read_namespace = Some(true);
                        }
                        Permissions::UpdateCollection => {
                            update_perm.has_update_namespace = Some(true);
                        }
                        Permissions::DeleteCollection => {
                            update_perm.has_delete_namespace = Some(true);
                        }
                        Permissions::DelegateCollection => {
                            update_perm.has_delegate_namespace = Some(true);
                        }
                        Permissions::CreateClass => {
                            update_perm.has_create_class = Some(true);
                        }
                        Permissions::ReadClass => {
                            update_perm.has_read_class = Some(true);
                        }
                        Permissions::UpdateClass => {
                            update_perm.has_update_class = Some(true);
                        }
                        Permissions::DeleteClass => {
                            update_perm.has_delete_class = Some(true);
                        }
                        Permissions::CreateObject => {
                            update_perm.has_create_object = Some(true);
                        }
                        Permissions::ReadObject => {
                            update_perm.has_read_object = Some(true);
                        }
                        Permissions::UpdateObject => {
                            update_perm.has_update_object = Some(true);
                        }
                        Permissions::DeleteObject => {
                            update_perm.has_delete_object = Some(true);
                        }
                        Permissions::CreateClassRelation => {
                            update_perm.has_create_class_relation = Some(true);
                        }
                        Permissions::ReadClassRelation => {
                            update_perm.has_read_class_relation = Some(true);
                        }
                        Permissions::UpdateClassRelation => {
                            update_perm.has_update_class_relation = Some(true);
                        }
                        Permissions::DeleteClassRelation => {
                            update_perm.has_delete_class_relation = Some(true);
                        }
                        Permissions::CreateObjectRelation => {
                            update_perm.has_create_object_relation = Some(true);
                        }
                        Permissions::ReadObjectRelation => {
                            update_perm.has_read_object_relation = Some(true);
                        }
                        Permissions::UpdateObjectRelation => {
                            update_perm.has_update_object_relation = Some(true);
                        }
                        Permissions::DeleteObjectRelation => {
                            update_perm.has_delete_object_relation = Some(true);
                        }
                        Permissions::ReadTemplate => {
                            update_perm.has_read_template = Some(true);
                        }
                        Permissions::CreateTemplate => {
                            update_perm.has_create_template = Some(true);
                        }
                        Permissions::UpdateTemplate => {
                            update_perm.has_update_template = Some(true);
                        }
                        Permissions::DeleteTemplate => {
                            update_perm.has_delete_template = Some(true);
                        }
                    }
                }

                Ok(diesel::update(permissions)
                    .filter(namespace_id.eq(namespace_id_param))
                    .filter(group_id.eq(group_id_for_grant))
                    .set(&update_perm)
                    .get_result(conn)?)
            }
            None => {
                let new_entry = NewPermission {
                    namespace_id: namespace_id_param,
                    group_id: group_id_for_grant,
                    has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
                    has_update_namespace: permission_list.contains(&Permissions::UpdateCollection),
                    has_delete_namespace: permission_list.contains(&Permissions::DeleteCollection),
                    has_delegate_namespace: permission_list
                        .contains(&Permissions::DelegateCollection),
                    has_create_class: permission_list.contains(&Permissions::CreateClass),
                    has_read_class: permission_list.contains(&Permissions::ReadClass),
                    has_update_class: permission_list.contains(&Permissions::UpdateClass),
                    has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                    has_create_object: permission_list.contains(&Permissions::CreateObject),
                    has_read_object: permission_list.contains(&Permissions::ReadObject),
                    has_update_object: permission_list.contains(&Permissions::UpdateObject),
                    has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                    has_create_class_relation: permission_list
                        .contains(&Permissions::CreateClassRelation),
                    has_read_class_relation: permission_list
                        .contains(&Permissions::ReadClassRelation),
                    has_update_class_relation: permission_list
                        .contains(&Permissions::UpdateClassRelation),
                    has_delete_class_relation: permission_list
                        .contains(&Permissions::DeleteClassRelation),
                    has_create_object_relation: permission_list
                        .contains(&Permissions::CreateObjectRelation),
                    has_read_object_relation: permission_list
                        .contains(&Permissions::ReadObjectRelation),
                    has_update_object_relation: permission_list
                        .contains(&Permissions::UpdateObjectRelation),
                    has_delete_object_relation: permission_list
                        .contains(&Permissions::DeleteObjectRelation),
                    has_read_template: permission_list.contains(&Permissions::ReadTemplate),
                    has_create_template: permission_list.contains(&Permissions::CreateTemplate),
                    has_update_template: permission_list.contains(&Permissions::UpdateTemplate),
                    has_delete_template: permission_list.contains(&Permissions::DeleteTemplate),
                };

                Ok(diesel::insert_into(permissions)
                    .values(&new_entry)
                    .get_result(conn)?)
            }
        }
    })
}

pub(crate) async fn revoke_permissions_query(
    pool: &DbPool,
    namespace_id_param: i32,
    group_id_for_revoke: i32,
    permission_list: PermissionsList<Permissions>,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;

    with_transaction(pool, |conn| -> Result<Permission, ApiError> {
        permissions
            .filter(namespace_id.eq(namespace_id_param))
            .filter(group_id.eq(group_id_for_revoke))
            .first::<Permission>(conn)?;

        let mut update_perm = UpdatePermission::default();
        for permission in permission_list.into_iter() {
            match permission {
                Permissions::ReadCollection => {
                    update_perm.has_read_namespace = Some(false);
                }
                Permissions::UpdateCollection => {
                    update_perm.has_update_namespace = Some(false);
                }
                Permissions::DeleteCollection => {
                    update_perm.has_delete_namespace = Some(false);
                }
                Permissions::DelegateCollection => {
                    update_perm.has_delegate_namespace = Some(false);
                }
                Permissions::CreateClass => {
                    update_perm.has_create_class = Some(false);
                }
                Permissions::ReadClass => {
                    update_perm.has_read_class = Some(false);
                }
                Permissions::UpdateClass => {
                    update_perm.has_update_class = Some(false);
                }
                Permissions::DeleteClass => {
                    update_perm.has_delete_class = Some(false);
                }
                Permissions::CreateObject => {
                    update_perm.has_create_object = Some(false);
                }
                Permissions::ReadObject => {
                    update_perm.has_read_object = Some(false);
                }
                Permissions::UpdateObject => {
                    update_perm.has_update_object = Some(false);
                }
                Permissions::DeleteObject => {
                    update_perm.has_delete_object = Some(false);
                }
                Permissions::CreateClassRelation => {
                    update_perm.has_create_class_relation = Some(false);
                }
                Permissions::ReadClassRelation => {
                    update_perm.has_read_class_relation = Some(false);
                }
                Permissions::UpdateClassRelation => {
                    update_perm.has_update_class_relation = Some(false);
                }
                Permissions::DeleteClassRelation => {
                    update_perm.has_delete_class_relation = Some(false);
                }
                Permissions::CreateObjectRelation => {
                    update_perm.has_create_object_relation = Some(false);
                }
                Permissions::ReadObjectRelation => {
                    update_perm.has_read_object_relation = Some(false);
                }
                Permissions::UpdateObjectRelation => {
                    update_perm.has_update_object_relation = Some(false);
                }
                Permissions::DeleteObjectRelation => {
                    update_perm.has_delete_object_relation = Some(false);
                }
                Permissions::ReadTemplate => {
                    update_perm.has_read_template = Some(false);
                }
                Permissions::CreateTemplate => {
                    update_perm.has_create_template = Some(false);
                }
                Permissions::UpdateTemplate => {
                    update_perm.has_update_template = Some(false);
                }
                Permissions::DeleteTemplate => {
                    update_perm.has_delete_template = Some(false);
                }
            }
        }

        Ok(diesel::update(permissions)
            .filter(namespace_id.eq(namespace_id_param))
            .filter(group_id.eq(group_id_for_revoke))
            .set(&update_perm)
            .get_result(conn)?)
    })
}

pub(crate) async fn revoke_all_query(
    pool: &DbPool,
    namespace_id_param: i32,
    group_id_for_revoke: i32,
) -> Result<(), ApiError> {
    use crate::schema::permissions::dsl::*;

    with_connection(pool, |conn| {
        diesel::delete(permissions)
            .filter(namespace_id.eq(namespace_id_param))
            .filter(group_id.eq(group_id_for_revoke))
            .execute(conn)
    })?;

    Ok(())
}

pub(crate) async fn user_on_query<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{group_id, namespace_id, permissions};

    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let group_ids_subquery = user_id.group_ids_subquery_from_backend();
    let rows = with_connection(pool, |conn| {
        groups
            .inner_join(permissions.on(group_table_id.eq(group_id)))
            .filter(namespace_id.eq(namespace_target_id))
            .filter(group_id.eq_any(group_ids_subquery))
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;

    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
}

pub(crate) async fn user_on_paginated_query<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError> {
    use crate::schema::groups::dsl::{groupname, groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        created_at as permission_created_at, group_id, id as permission_id, namespace_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search, string_search};

    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let build_query = || -> Result<_, ApiError> {
        let group_ids_subquery = user_id.group_ids_subquery_from_backend();
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

/// Load all namespaces from the database without any permission filtering.
/// Used by the Treetop backend's candidate enumeration.
pub(crate) async fn all_namespaces(pool: &DbPool) -> Result<Vec<Namespace>, ApiError> {
    with_connection(pool, |conn| {
        crate::schema::namespaces::table.load::<Namespace>(conn)
    })
}

pub(crate) async fn user_can_on_any_query(
    pool: &DbPool,
    user_id: i32,
    permissions_requested: &[Permissions],
) -> Result<Vec<Namespace>, ApiError> {
    use crate::models::UserID;
    use crate::traits::GroupMemberships;

    let user = UserID(user_id);
    if user.is_admin(pool).await? {
        return all_namespaces(pool).await;
    }

    let group_id_subquery = user.group_ids_subquery_from_backend();
    let mut filtered = crate::schema::permissions::dsl::permissions
        .into_boxed()
        .filter(crate::schema::permissions::dsl::group_id.eq_any(group_id_subquery));

    for permission in permissions_requested {
        filtered = permission.create_boxed_filter(filtered, true);
    }

    let accessible_namespace_ids: Vec<i32> = with_connection(pool, |conn| {
        filtered
            .select(crate::schema::permissions::dsl::namespace_id)
            .load::<i32>(conn)
    })?;

    if accessible_namespace_ids.is_empty() {
        return Ok(vec![]);
    }

    with_connection(pool, |conn| {
        crate::schema::namespaces::table
            .filter(crate::schema::namespaces::id.eq_any(accessible_namespace_ids))
            .load::<Namespace>(conn)
    })
}

pub(crate) async fn group_can_on_query<T: NamespaceAccessors>(
    pool: &DbPool,
    gid: i32,
    namespace_ref: T,
    permission_type: Permissions,
) -> Result<bool, ApiError> {
    use crate::schema::permissions::dsl::*;

    let base_query = permissions
        .into_boxed()
        .filter(group_id.eq(gid))
        .filter(namespace_id.eq(namespace_ref.namespace_id(pool).await?));

    let filtered_query = permission_type.create_boxed_filter(base_query, true);
    let result = with_connection(pool, |conn| filtered_query.execute(conn))?;

    Ok(result != 0)
}

pub(crate) async fn groups_can_on_query(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
) -> Result<Vec<Group>, ApiError> {
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::*;

    let base_query = permissions.into_boxed().filter(namespace_id.eq(nid));
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

pub(crate) async fn groups_can_on_paginated_with_total_count_query(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError> {
    use crate::schema::groups::dsl::{
        created_at, description, groupname, groups, id as group_table_id, updated_at,
    };
    use crate::schema::permissions::dsl::*;
    use crate::{date_search, numeric_search, string_search};

    let build_query = || -> Result<_, ApiError> {
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

        Ok(query)
    };

    let query = build_query()?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_query()?;
    crate::apply_query_options!(query, query_options, Group);
    let items = with_connection(pool, |conn| query.load::<Group>(conn))?;

    Ok((items, total_count))
}

pub(crate) async fn groups_on_query<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::schema::groups::dsl::{groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        created_at as permission_created_at, group_id, id as permission_id, namespace_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search};

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

#[allow(dead_code)]
pub(crate) async fn groups_on_paginated_query<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    let (items, _) = groups_on_paginated_with_total_count_query(
        pool,
        namespace_ref,
        permissions_filter,
        query_options,
    )
    .await?;
    Ok(items)
}

pub(crate) async fn groups_on_paginated_with_total_count_query<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<(Vec<GroupPermission>, i64), ApiError> {
    use crate::schema::groups::dsl::{groupname, groups, id as group_table_id};
    use crate::schema::permissions::dsl::{
        created_at as permission_created_at, group_id, id as permission_id, namespace_id,
        permissions, updated_at as permission_updated_at,
    };
    use crate::{date_search, numeric_search, string_search};

    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
    let mut permission_filters = query_options.filters.permissions()?;
    permission_filters.ensure_contains(&permissions_filter);

    let build_query = || -> Result<_, ApiError> {
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

#[allow(dead_code)]
pub(crate) async fn count_groups_on_paginated_query<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: &QueryOptions,
) -> Result<i64, ApiError> {
    let (_, total_count) = groups_on_paginated_with_total_count_query(
        pool,
        namespace_ref,
        permissions_filter,
        query_options,
    )
    .await?;
    Ok(total_count)
}

pub(crate) async fn group_on_query(
    pool: &DbPool,
    nid: i32,
    gid: i32,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;

    with_connection(pool, |conn| {
        permissions
            .filter(namespace_id.eq(nid))
            .filter(group_id.eq(gid))
            .first::<Permission>(conn)
    })
}
