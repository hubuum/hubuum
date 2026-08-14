use super::*;
use crate::models::token_scope::TokenScope;
use crate::storage::postgres::operations::authz::{AuthzSubject, principal_is_admin, scope_allows};
use crate::storage::postgres::operations::collection::CollectionRow;
use crate::storage::postgres::operations::group::GroupRow;
use diesel_async::RunQueryDsl;

pub trait LoadUserGroups: AuthzSubject {
    async fn load_user_groups(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<Group>, ApiError>;
}

impl<T: ?Sized> LoadUserGroups for T
where
    T: AuthzSubject,
{
    async fn load_user_groups(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
        use crate::schema::groups::dsl::*;

        let principal_id_value = self.principal_id();
        with_connection(pool, async |conn| {
            group_memberships
                .inner_join(groups.on(id.eq(group_id)))
                .filter(principal_id.eq(principal_id_value))
                .select(groups::all_columns())
                .load::<GroupRow>(conn)
                .await
        })
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

pub trait LoadPermittedCollections: GroupAccessors + AuthzSubject {
    /// Load the collections the subject has the given permissions on after
    /// enforcing the token's permission scope.
    ///
    /// * `scopes = None` — unscoped (admins get all collections via the fast path).
    /// * `scopes = Some(..)` — the requested permissions must be within scope.
    ///   Admins retain their collection authority. The downstream entity query
    ///   applies the resource boundary because a class- or object-only scope may
    ///   select a descendant without exposing its parent collection.
    async fn load_collections_with_permissions<'a, I>(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        permissions_list: &'a I,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        let is_admin = principal_is_admin(pool, self.principal_id()).await?;
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
        pool: &crate::storage::postgres::PostgresPool,
        permissions_list: &'a I,
        is_admin: bool,
        scopes: Option<&TokenScope>,
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
        pool: &crate::storage::postgres::PostgresPool,
        permissions_list: &'a I,
        is_admin: bool,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        use crate::schema::collection_closure::dsl::{
            ancestor_collection_id, collection_closure, descendant_collection_id,
        };
        use crate::schema::collections::dsl::{collections, id as collections_table_id};
        use crate::schema::permissions::dsl::{
            collection_id as permission_collection_id, group_id, permissions,
        };
        use crate::storage::postgres::operations::permissions::PermissionFilter;

        let requested: Vec<Permissions> = permissions_list.into_iter().copied().collect();

        // Fail-closed: a scoped token that requests anything outside its scope
        // can see no collections through that request.
        if !scope_allows(scopes, &requested) {
            return Ok(Vec::new());
        }

        // Do not filter this collection context by collection-only resource IDs:
        // class- and object-scoped tokens still need their parent collection as
        // query context. Every downstream entity query applies its own resource
        // predicate before returning rows.
        if is_admin {
            return with_connection(pool, async |conn| {
                collections
                    .select(collections::all_columns())
                    .load::<CollectionRow>(conn)
                    .await
            })
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect());
        }

        let groups_id_subquery = self.group_ids_subquery();

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in &requested {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        with_connection(pool, async |conn| {
            base_query
                .inner_join(
                    collection_closure.on(permission_collection_id.eq(ancestor_collection_id)),
                )
                .inner_join(collections.on(descendant_collection_id.eq(collections_table_id)))
                .select(collections::all_columns())
                .distinct()
                .load::<CollectionRow>(conn)
                .await
        })
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
