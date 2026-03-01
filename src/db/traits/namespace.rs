use diesel::prelude::*;
use tracing::{debug, trace};

use crate::db::traits::GetNamespace;
use crate::db::{with_connection, with_transaction, DbPool};
use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::output::GroupPermission;
use crate::models::permissions::PermissionFilter;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::{HubuumClassRelation, NewHubuumObjectRelation};
use crate::models::{HubuumObjectRelation, NewHubuumClassRelation};
use crate::models::{
    HubuumObjectRelationID, Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee,
    NewPermission, Permission, Permissions, UpdateNamespace, User, UserID,
};
use crate::traits::{
    ClassAccessors, GroupAccessors, NamespaceAccessors, ObjectAccessors, SelfAccessors,
};

use super::user::{GroupIdsSubqueryBackend, GroupMemberships};

impl GetNamespace<(Namespace, Namespace)> for HubuumClassRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.class_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if from_id == to_id && namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for class relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for NewHubuumClassRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.class_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for class relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for HubuumObjectRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.object_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(object_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for object relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for object relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for NewHubuumObjectRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.object_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(object_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for object relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for object relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for HubuumObjectRelationID {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.object_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(object_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for object relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for object relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find namespaces ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl<S> GetNamespace for S
where
    S: SelfAccessors<Namespace>,
{
    async fn namespace_from_backend(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let namespace = with_connection(pool, |conn| {
            namespaces.filter(id.eq(self.id())).first::<Namespace>(conn)
        })?;

        Ok(namespace)
    }
}

pub trait DeleteNamespaceRecord {
    async fn delete_namespace_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteNamespaceRecord for Namespace {
    async fn delete_namespace_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::delete(namespaces.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }
}

impl DeleteNamespaceRecord for NamespaceID {
    async fn delete_namespace_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::delete(namespaces.filter(id.eq(self.0))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait UpdateNamespaceRecord {
    async fn update_namespace_record(
        &self,
        pool: &DbPool,
        namespace_id: i32,
    ) -> Result<Namespace, ApiError>;
}

impl UpdateNamespaceRecord for UpdateNamespace {
    async fn update_namespace_record(
        &self,
        pool: &DbPool,
        namespace_id: i32,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            diesel::update(namespaces)
                .filter(id.eq(namespace_id))
                .set(self)
                .get_result::<Namespace>(conn)
        })
    }
}

pub trait SaveNamespaceWithAssigneeRecord {
    async fn save_namespace_with_assignee_record(
        &self,
        pool: &DbPool,
    ) -> Result<Namespace, ApiError>;
}

impl SaveNamespaceWithAssigneeRecord for NewNamespaceWithAssignee {
    async fn save_namespace_with_assignee_record(
        &self,
        pool: &DbPool,
    ) -> Result<Namespace, ApiError> {
        let new_namespace = NewNamespace {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        new_namespace
            .save_namespace_for_group_record(pool, self.group_id)
            .await
    }
}

pub trait SaveNamespaceForGroupRecord {
    async fn save_namespace_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Namespace, ApiError>;
}

impl SaveNamespaceForGroupRecord for NewNamespace {
    async fn save_namespace_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::namespaces;
        use crate::schema::permissions::dsl::permissions;

        with_transaction(pool, |conn| {
            let namespace = diesel::insert_into(namespaces)
                .values(self)
                .get_result::<Namespace>(conn)?;

            let group_permission = NewPermission {
                namespace_id: namespace.id,
                group_id,
                has_read_namespace: true,
                has_update_namespace: true,
                has_delete_namespace: true,
                has_delegate_namespace: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
                has_create_class_relation: true,
                has_read_class_relation: true,
                has_update_class_relation: true,
                has_delete_class_relation: true,
                has_create_object_relation: true,
                has_read_object_relation: true,
                has_update_object_relation: true,
                has_delete_object_relation: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(namespace)
        })
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

pub async fn total_namespace_count_from_backend(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::namespaces::dsl::*;

    with_connection(pool, |conn| namespaces.count().get_result::<i64>(conn))
}

pub async fn user_on_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    user_id: UserID,
    namespace_ref: T,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
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

pub async fn user_on_paginated_from_backend<T: NamespaceAccessors>(
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

    let namespace_target_id = namespace_ref.namespace_id(pool).await?;
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
                )))
            }
        }
    }

    crate::apply_query_options!(query, query_options, GroupPermission);

    let rows = with_connection(pool, |conn| query.load::<(Group, Permission)>(conn))?;
    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
}

pub async fn user_can_on_any_from_backend<U: SelfAccessors<User> + GroupAccessors>(
    pool: &DbPool,
    user_id: U,
    permission_type: Permissions,
) -> Result<Vec<Namespace>, ApiError> {
    use crate::schema::permissions::dsl::*;

    let base_query = if user_id.instance(pool).await?.is_admin(pool).await? {
        permissions.into_boxed()
    } else {
        let group_ids_subquery = user_id.group_ids_subquery_from_backend();

        permissions
            .into_boxed()
            .filter(group_id.eq_any(group_ids_subquery))
    };

    let filtered_query = permission_type.create_boxed_filter(base_query, true);
    let accessible_namespace_ids = with_connection(pool, |conn| {
        filtered_query.select(namespace_id).load::<i32>(conn)
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

pub async fn group_can_on_from_backend<T: NamespaceAccessors>(
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

pub async fn groups_can_on_from_backend(
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

pub async fn groups_can_on_paginated_from_backend(
    pool: &DbPool,
    nid: i32,
    permission_type: Permissions,
    query_options: &QueryOptions,
) -> Result<Vec<Group>, ApiError> {
    use crate::schema::groups::dsl::{
        created_at, description, groupname, groups, id as group_table_id, updated_at,
    };
    use crate::schema::permissions::dsl::*;
    use crate::{date_search, numeric_search, string_search};

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
                )))
            }
        }
    }

    crate::apply_query_options!(query, query_options, Group);

    with_connection(pool, |conn| query.load::<Group>(conn))
}

pub async fn groups_on_from_backend<T: NamespaceAccessors>(
    pool: &DbPool,
    namespace_ref: T,
    permissions_filter: Vec<Permissions>,
    query_options: QueryOptions,
) -> Result<Vec<GroupPermission>, ApiError> {
    use crate::models::traits::output::FromTuple;
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
                )))
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
                )))
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

pub async fn groups_on_paginated_from_backend<T: NamespaceAccessors>(
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
                )))
            }
        }
    }

    crate::apply_query_options!(query, query_options, GroupPermission);

    let rows = with_connection(pool, |conn| {
        query
            .select((groups::all_columns(), permissions::all_columns()))
            .load::<(Group, Permission)>(conn)
    })?;
    Ok(rows.into_iter().map(GroupPermission::from_tuple).collect())
}

pub async fn group_on_from_backend(
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
