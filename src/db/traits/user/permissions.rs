use super::*;
pub trait UserPermissions: SelfAccessors<User> + GroupAccessors + GroupMemberships {
    /// ## Check if a user has a set of permissions in a set of namespaces
    ///
    /// All permissions must be present in all namespaces for the function to return true.
    ///
    /// ### Parameters
    ///
    /// * `pool` - A database connection pool
    /// * `permissions` - An iterable of permissions to check for
    /// * `namespaces` - An iterable of namespaces to check against
    ///
    /// ### Returns
    ///
    /// * Nothing if the user has the required permissions, or an ApiError::Forbidden if they do not.
    async fn can<P, N, I>(
        &self,
        pool: &DbPool,
        permissions: P,
        namespaces: I,
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

        if self.is_admin(pool).await? {
            return Ok(());
        }

        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;

        let group_id_subquery = self.group_ids_subquery_from_backend();

        let namespace_ids: HashSet<i32> = stream::iter(namespaces)
            .map(|ns| async move { ns.namespace_id(pool).await })
            // Batch the futures into groups of 5, to avoid overwhelming the database
            .buffered(5)
            .try_collect()
            .await?;

        let mut base_query = lookup_table
            .into_boxed()
            .filter(namespace_id_field.eq_any(&namespace_ids))
            .filter(group_id_field.eq_any(group_id_subquery));

        // Apply all permission filters
        for perm in permissions {
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

impl UserPermissions for User {}
impl UserPermissions for UserID {}

pub trait GroupMemberships: SelfAccessors<User> {
    /// At some point, we need to get the name of the admin group. Right now it's hard coded.
    async fn admin_groupname(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?.admin_groupname.clone())
    }

    /// Check if the user is in a group by name
    ///
    /// This function checks if the user is a member of a group with the specified name.
    ///
    /// ## Parameters
    ///
    /// * `groupname_queried` - The name of the group to check for membership.
    /// * `pool` - The database connection pool.
    ///
    /// ## Returns
    ///
    /// * Ok(true) if the user is in the group
    /// * Ok(false) if the user is not in the group
    /// * Err(ApiError) if something failed.
    async fn is_in_group_by_name(
        &self,
        groupname_queried: &str,
        pool: &DbPool,
    ) -> Result<bool, ApiError> {
        use crate::schema::groups::dsl::{groupname, groups};
        use crate::schema::user_groups::dsl::{user_groups, user_id as ug_user_id};
        use diesel::dsl::{exists, select};

        let is_in_group = with_connection(pool, |conn| {
            select(exists(
                user_groups
                    .inner_join(groups)
                    .filter(ug_user_id.eq(self.id()))
                    .filter(groupname.eq(groupname_queried)),
            ))
            .get_result(conn)
        })?;

        trace!(
            message = "Group by name check result",
            user_id = self.id(),
            groupname = groupname_queried,
            is_in_group = is_in_group,
        );

        Ok(is_in_group)
    }

    /// Check if the user is an admin
    ///
    /// This function checks the user's admin status in the database, but checking if they are
    /// a member of the group with the name "admin".
    async fn is_admin(&self, pool: &DbPool) -> Result<bool, ApiError> {
        let is_admin = self
            .is_in_group_by_name(&self.admin_groupname().await?, pool)
            .await?;

        trace!(
            message = "Admin check result",
            user_id = self.id(),
            is_admin = is_admin,
        );

        Ok(is_admin)
    }
}

impl GroupMemberships for User {}
impl GroupMemberships for UserID {}
