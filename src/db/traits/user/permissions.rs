use super::*;
use crate::db::traits::authz::{AuthzSubject, scope_allows};
pub trait UserPermissions: AuthzSubject {
    /// ## Check if a subject has a set of permissions in a set of namespaces
    ///
    /// All permissions must be present in all namespaces for the function to return true.
    ///
    /// ### Parameters
    ///
    /// * `pool` - A database connection pool
    /// * `permissions` - An iterable of permissions to check for
    /// * `namespaces` - An iterable of namespaces to check against
    /// * `scopes` - The token scope set (`None` = unscoped/full authority)
    ///
    /// ### Returns
    ///
    /// * Nothing if the subject has the required permissions, or an ApiError::Forbidden if they do not.
    async fn can<P, N, I>(
        &self,
        pool: &DbPool,
        permissions: P,
        namespaces: I,
        scopes: Option<&[Permissions]>,
    ) -> Result<(), ApiError>
    where
        P: IntoIterator<Item = Permissions>,
        I: IntoIterator<Item = N>,
        N: NamespaceAccessors,
    {
        use crate::models::PermissionFilter;
        use diesel::{dsl::sql, sql_types::BigInt};
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
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;

        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: HashSet<i32> = stream::iter(namespaces)
            .map(|ns| async move { ns.namespace_id(pool).await.map(|nid| nid.id()) })
            // Batch the futures into groups of 5, to avoid overwhelming the database
            .buffered(5)
            .try_collect()
            .await?;

        let mut base_query = lookup_table
            .into_boxed()
            .filter(namespace_id_field.eq_any(&namespace_ids))
            .filter(group_id_field.eq_any(group_id_subquery));

        // Apply all permission filters
        for perm in requested {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        // Count the number of distinct namespaces that match all criteria
        let matching_namespaces_count = with_connection(pool, |conn| {
            base_query
                .select(sql::<BigInt>("COUNT(DISTINCT namespace_id)"))
                .first::<i64>(conn)
        })?;

        // Check if the count of matching namespaces equals the number of input namespaces
        if matching_namespaces_count as usize == namespace_ids.len() {
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
