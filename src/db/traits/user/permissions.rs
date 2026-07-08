use super::*;
use crate::db::traits::authz::{AuthzSubject, scope_allows};
pub trait UserPermissions: AuthzSubject {
    /// ## Check if a subject has a set of permissions in a set of collections
    ///
    /// All permissions must be present in all collections for the function to return true.
    ///
    /// ### Parameters
    ///
    /// * `pool` - A database connection pool
    /// * `permissions` - An iterable of permissions to check for
    /// * `collections` - An iterable of collections to check against
    /// * `scopes` - The token scope set (`None` = unscoped/full authority)
    ///
    /// ### Returns
    ///
    /// * Nothing if the subject has the required permissions, or an ApiError::Forbidden if they do not.
    async fn can<P, N, I>(
        &self,
        pool: &DbPool,
        permissions: P,
        collections: I,
        scopes: Option<&[Permissions]>,
    ) -> Result<(), ApiError>
    where
        P: IntoIterator<Item = Permissions>,
        I: IntoIterator<Item = N>,
        N: CollectionAccessors,
    {
        use diesel::{AggregateExpressionMethods, dsl::count};
        use futures::stream::{self, StreamExt, TryStreamExt};
        use std::collections::HashSet;

        let requested: Vec<Permissions> = permissions.into_iter().collect();

        // Fail-closed scope pre-filter, before the admin bypass.
        if !scope_allows(scopes, &requested) {
            return Err(ApiError::Forbidden(
                "Token scope does not permit the requested action".to_string(),
            ));
        }

        if AuthzSubject::is_admin(self, pool).await? {
            return Ok(());
        }

        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let collection_id_field = crate::schema::permissions::dsl::collection_id;
        let closure_table = crate::schema::collection_closure::dsl::collection_closure;
        let ancestor_collection_id = crate::schema::collection_closure::dsl::ancestor_collection_id;
        let descendant_collection_id =
            crate::schema::collection_closure::dsl::descendant_collection_id;

        let group_id_subquery = self.group_ids_subquery();

        let collection_ids: HashSet<i32> = stream::iter(collections)
            .map(|collection_fixture| async move {
                collection_fixture
                    .collection_id(pool)
                    .await
                    .map(|collection_id| collection_id.id())
            })
            // Batch the futures into groups of 5, to avoid overwhelming the database
            .buffered(5)
            .try_collect()
            .await?;

        let mut base_query = lookup_table
            .inner_join(closure_table.on(collection_id_field.eq(ancestor_collection_id)))
            .into_boxed()
            .filter(descendant_collection_id.eq_any(&collection_ids))
            .filter(group_id_field.eq_any(group_id_subquery));

        // Apply all permission filters
        for perm in requested {
            crate::apply_permission_filter!(base_query, perm, true);
        }

        // Count the number of distinct collections that match all criteria
        let matching_collections_count = with_connection(pool, |conn| {
            base_query
                .select(count(descendant_collection_id).aggregate_distinct())
                .first::<i64>(conn)
        })?;

        // Check if the count of matching collections equals the number of input collections
        if matching_collections_count as usize == collection_ids.len() {
            Ok(())
        } else {
            Err(ApiError::Forbidden(
                "User does not have the required permissions".to_string(),
            ))
        }
    }
}

// `.can(...)` is available to every authorization subject — humans, service
// accounts, and bare principals alike — via the identity-only contract.
impl<T: AuthzSubject + ?Sized> UserPermissions for T {}
