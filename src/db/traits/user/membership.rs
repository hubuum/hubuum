use super::*;
use crate::db::traits::authz::{AuthzSubject, scope_allows};

pub trait LoadUserGroups: AuthzSubject {
    async fn load_user_groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError>;
}

impl<T: ?Sized> LoadUserGroups for T
where
    T: AuthzSubject,
{
    async fn load_user_groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
        use crate::schema::groups::dsl::*;

        with_connection(pool, |conn| {
            group_memberships
                .inner_join(groups.on(id.eq(group_id)))
                .filter(principal_id.eq(self.principal_id()))
                .select(groups::all_columns())
                .load::<Group>(conn)
        })
    }
}

pub trait LoadUserGroupsPaginated: AuthzSubject {
    async fn load_user_groups_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<Group>, i64), ApiError>;
}

impl<T: ?Sized> LoadUserGroupsPaginated for T
where
    T: AuthzSubject,
{
    async fn load_user_groups_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<Group>, i64), ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
        use crate::schema::groups::dsl::*;
        use crate::{date_search, numeric_search, string_search};

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = group_memberships
                .inner_join(groups.on(id.eq(group_id)))
                .filter(principal_id.eq(self.principal_id()))
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

pub trait LoadPermittedCollections: GroupAccessors + AuthzSubject {
    /// Load all collections the subject has the given permissions on, intersected
    /// with the token scope set.
    ///
    /// * `scopes = None` — unscoped (admins get all collections via the fast path).
    /// * `scopes = Some(..)` — the requested permissions must be within scope; a
    ///   scoped admin falls through to the per-collection permission query rather
    ///   than the all-collections fast path.
    async fn load_collections_with_permissions<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        let is_admin = AuthzSubject::is_admin(self, pool).await?;
        self.load_collections_with_permissions_with_admin_status(
            pool,
            permissions_list,
            is_admin,
            scopes,
        )
        .await
    }

    async fn load_collections_with_permissions_with_admin_status<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
        is_admin: bool,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>;
}

impl<T: ?Sized> LoadPermittedCollections for T
where
    T: GroupAccessors + AuthzSubject,
{
    async fn load_collections_with_permissions_with_admin_status<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
        is_admin: bool,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        use crate::models::PermissionFilter;
        use crate::schema::collection_closure::dsl::{
            ancestor_collection_id, collection_closure, descendant_collection_id,
        };
        use crate::schema::collections::dsl::{collections, id as collections_table_id};
        use crate::schema::permissions::dsl::{
            collection_id as permission_collection_id, group_id, permissions,
        };

        let requested: Vec<Permissions> = permissions_list.into_iter().copied().collect();

        // Fail-closed: a scoped token that requests anything outside its scope
        // can see no collections through that request.
        if !scope_allows(scopes, &requested) {
            return Ok(Vec::new());
        }

        // Unscoped admins see everything; scoped admins fall through to the
        // permission query so their token scope still bounds the listing.
        if is_admin && scopes.is_none() {
            return with_connection(pool, |conn| {
                collections
                    .select(collections::all_columns())
                    .load::<Collection>(conn)
            });
        }

        let groups_id_subquery = self.group_ids_subquery();

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in &requested {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        with_connection(pool, |conn| {
            base_query
                .inner_join(
                    collection_closure.on(permission_collection_id.eq(ancestor_collection_id)),
                )
                .inner_join(collections.on(descendant_collection_id.eq(collections_table_id)))
                .select(collections::all_columns())
                .distinct()
                .load::<Collection>(conn)
        })
    }
}
