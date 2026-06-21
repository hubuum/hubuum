use super::*;
pub trait LoadUserGroups: SelfAccessors<User> {
    async fn load_user_groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError>;
}

impl<T: ?Sized> LoadUserGroups for T
where
    T: SelfAccessors<User>,
{
    async fn load_user_groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

        with_connection(pool, |conn| {
            user_groups
                .inner_join(groups.on(id.eq(group_id)))
                .filter(user_id.eq(self.id()))
                .select(groups::all_columns())
                .load::<Group>(conn)
        })
    }
}

pub trait LoadUserGroupsPaginated: SelfAccessors<User> {
    async fn load_user_groups_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<Group>, i64), ApiError>;
}

impl<T: ?Sized> LoadUserGroupsPaginated for T
where
    T: SelfAccessors<User>,
{
    async fn load_user_groups_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<Group>, i64), ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};
        use crate::{date_search, numeric_search, string_search};

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = user_groups
                .inner_join(groups.on(id.eq(group_id)))
                .filter(user_id.eq(self.id()))
                .into_boxed();

            for param in &query_options.filters {
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, id),
                    FilterField::Name | FilterField::Groupname => {
                        string_search!(base_query, param, operator, groupname)
                    }
                    FilterField::Description => {
                        string_search!(base_query, param, operator, description)
                    }
                    FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                    FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for groups",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = with_connection(pool, |conn| base_query.count().get_result::<i64>(conn))?;

        let mut base_query = build_query()?.select(groups::all_columns());
        crate::apply_query_options!(base_query, query_options, Group);
        let items = with_connection(pool, |conn| base_query.load::<Group>(conn))?;

        Ok((items, total_count))
    }
}

pub trait GroupIdsSubqueryBackend: SelfAccessors<User> {
    fn group_ids_subquery_from_backend<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>;
}

impl<T: ?Sized> GroupIdsSubqueryBackend for T
where
    T: SelfAccessors<User>,
{
    fn group_ids_subquery_from_backend<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>
    {
        use crate::schema::user_groups::dsl::*;

        user_groups
            .filter(user_id.eq(self.id()))
            .select(group_id)
            .into_boxed()
    }
}

pub trait LoadPermittedNamespaces: SelfAccessors<User> + GroupAccessors + GroupMemberships {
    async fn load_namespaces_with_permissions<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.load_namespaces_with_permissions_with_admin_status(pool, permissions_list, is_admin)
            .await
    }

    async fn load_namespaces_with_permissions_with_admin_status<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
        is_admin: bool,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>;
}

impl<T: ?Sized> LoadPermittedNamespaces for T
where
    T: SelfAccessors<User> + GroupAccessors + GroupMemberships,
{
    async fn load_namespaces_with_permissions_with_admin_status<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
        is_admin: bool,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        use crate::models::PermissionFilter;
        use crate::schema::namespaces::dsl::{id as namespaces_table_id, namespaces};
        use crate::schema::permissions::dsl::{group_id, namespace_id, permissions};
        if is_admin {
            return with_connection(pool, |conn| {
                namespaces
                    .select(namespaces::all_columns())
                    .load::<Namespace>(conn)
            });
        }

        let groups_id_subquery = self.group_ids_subquery_from_backend();

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in permissions_list {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        with_connection(pool, |conn| {
            base_query
                .inner_join(namespaces.on(namespace_id.eq(namespaces_table_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })
    }
}
