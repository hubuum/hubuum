use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl, Table};
use std::iter::IntoIterator;

use tracing::{debug, trace};

use crate::models::{Group, Permissions, User, UserID};
use crate::utilities::auth::hash_password;
use crate::traits::{GroupAccessors, NamespaceAccessors, SelfAccessors};

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;

use crate::models::search::{FilterField, QueryOptions};

use crate::{date_search, numeric_search, string_search, trace_query};

impl User {
    pub async fn get_by_username(pool: &DbPool, username_arg: &str) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;

        with_connection(pool, |conn| {
            users
                .filter(username.eq(username_arg))
                .first::<User>(conn)
        })
    }

    /// Set a new password for a user
    /// 
    /// The password will be hashed before storing it in the database, so the input should be the
    /// desired plaintext password.
    pub async fn set_password(&self, pool: &DbPool, new_password: &str) -> Result<(), ApiError> {        
        use crate::schema::users::dsl::*;
        debug!(
            message = "Setting new password",
            id = self.id(),
            username = self.username,
        );
        let new_password = hash_password(new_password).map_err(|e| {
            ApiError::HashError(format!("Failed to hash password: {e}"))
        })?;

        with_connection(pool, |conn| {
            diesel::update(users.filter(id.eq(self.id)))
                .set(password.eq(new_password))
                .execute(conn)
        })?;

        Ok(())
    }
}

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
        use futures::stream::{self, StreamExt, TryStreamExt};
        use diesel::{dsl::sql, sql_types::BigInt};
        use std::collections::HashSet;
        use crate::models::PermissionFilter;

        if self.is_admin(pool).await? {
            return Ok(());
        }

        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;
    
        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: HashSet<i32> = stream::iter(namespaces)
            .map(|ns| async move { ns.namespace_id(&pool).await })
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
            Err(ApiError::Forbidden("User does not have the required permissions".to_string()))
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
    async fn is_in_group_by_name(&self, groupname_queried: &str, pool: &DbPool) -> Result<bool, ApiError> {
        use diesel::dsl::{exists, select};
        use crate::schema::groups::dsl::{groupname, groups};
        use crate::schema::user_groups::dsl::{user_id as ug_user_id,user_groups};

        let is_in_group = with_connection(pool, |conn| {
            select(exists(
                user_groups
                    .inner_join(groups)
                    .filter(ug_user_id.eq(self.id()))
                    .filter(groupname.eq(groupname_queried))
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
        let is_admin = self.is_in_group_by_name(&self.admin_groupname().await?, pool).await?;

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

impl User {
    pub async fn search_users(
        &self,
        pool: &DbPool,
        query_options: QueryOptions
    ) -> Result<Vec<User>, ApiError> {
        use crate::schema::users::dsl::*;

        let query_params = query_options.filters;

        debug!(
            message = "Searching users",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut base_query = users.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Username => string_search!(base_query, param, operator, username),
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching users");

        let result = with_connection(pool, |conn| base_query
            .select(users::all_columns())
            .distinct() // TODO: Is it the joins that makes this required?
            .load::<User>(conn))?;

        Ok(result)
    }

    pub async fn search_groups(
        &self,
        pool: &DbPool,
        query_options: QueryOptions
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::{id, created_at, updated_at, groupname, description, groups};

        let query_params = query_options.filters;

        debug!(
            message = "Searching groups",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut base_query = groups.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, groupname),
                FilterField::Description => string_search!(base_query, param, operator, description),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching groups");

        let result = with_connection(pool, |conn| base_query
            .select(groups::all_columns())
            .distinct() 
            .load::<Group>(conn))?;

        Ok(result)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use yare::parameterized;

    use crate::models::{Permissions as P, PermissionsList as PL};
    use crate::tests::{
        create_namespace, create_test_group, create_user_with_params, get_pool_and_config,
    };
    use crate::traits::PermissionController;

    // user_idx, namespaces_idx, permissions, expected
    #[parameterized(
        u1_ns1_classread_true = { 0, vec![0], vec![P::ReadClass], true },
        u1_ns1_classcreate_true = { 0, vec![0], vec![P::CreateClass], true },
        u1_ns1_classreadcreate_true = { 0, vec![0], vec![P::ReadClass, P::CreateClass], true },
        u1_ns2_classdelete_true = { 0, vec![1], vec![P::DeleteClass], true },
        u1_ns2_classcreate_true = { 0, vec![1], vec![P::CreateClass], true },
        u1_ns2_classcreatedelete_true = { 0, vec![1], vec![P::CreateClass, P::DeleteClass], true },        
        u1_ns12_classcreate_true = { 0, vec![0,1], vec![P::CreateClass], true },

        u1_ns1_objectread_false = { 0, vec![0], vec![P::ReadObject], false },
        u1_ns1_namespacecreate_false = { 0, vec![0], vec![P::ReadCollection], false },
        u1_ns12_classreadcreate_false = { 0, vec![0,1], vec![P::CreateClass, P::ReadClass], false },
        u1_ns12_classreadcreatedelete_false = { 0, vec![0,1], vec![P::CreateClass, P::ReadClass, P::DeleteClass], false }, 

        u2_ns1_objectread_true = { 1, vec![0], vec![P::ReadObject], true },
        u2_ns1_objectcreate_true = { 1, vec![0], vec![P::CreateObject], true },
        u2_ns1_objectreadcreate_true = { 1, vec![0], vec![P::ReadObject, P::CreateObject], true },
        u2_ns2_objectdelete_true = { 1, vec![1], vec![P::DeleteObject], true },
        u2_ns2_objectcreate_true = { 1, vec![1], vec![P::CreateObject], true },
        u2_ns2_objectcreatedelete_true = { 1, vec![1], vec![P::CreateObject, P::DeleteObject], true },
        

    )]
    #[test_macro(actix_web::test)]
    async fn test_user_can(
        user_idx: usize,
        namespaces_idx: Vec<usize>,
        permissions: Vec<Permissions>,
        expected: bool,
    ) {
        let (pool, _) = get_pool_and_config().await;
        let suffix = format!(
            "_{}_{}_{}_{}",
            user_idx,
            namespaces_idx
                .iter()
                .map(|&x| x.to_string())
                .collect::<Vec<String>>()
                .join("_"),
            permissions
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<String>>()
                .join("_"),
            expected
        );

        let namespaces = [create_namespace(&pool, &format!("test_user_can_ns1_{suffix}"))
                .await
                .unwrap(),
            create_namespace(&pool, &format!("test_user_can_ns2_{suffix}"))
                .await
                .unwrap()];
        let groups = [create_test_group(&pool).await,
            create_test_group(&pool).await];
        let users = vec![
            create_user_with_params(&pool, &format!("test_user_can_u1_{suffix}"), "foo").await,
            create_user_with_params(&pool, &format!("test_user_can_u2_{suffix}"), "foo").await,
        ];

        groups[0].add_member(&pool, &users[0]).await.unwrap();
        groups[1].add_member(&pool, &users[1]).await.unwrap();

        namespaces[0]
            .grant(
                &pool,
                groups[0].id,
                PL::new(vec![P::CreateClass, P::ReadClass]),
            )
            .await
            .unwrap();
        namespaces[1]
            .grant(
                &pool,
                groups[0].id,
                PL::new(vec![P::CreateClass, P::DeleteClass]),
            )
            .await
            .unwrap();

        namespaces[0]
            .grant(
                &pool,
                groups[1].id,
                PL::new(vec![P::CreateObject, P::ReadObject]),
            )
            .await
            .unwrap();
        namespaces[1]
            .grant(
                &pool,
                groups[1].id,
                PL::new(vec![P::CreateObject, P::DeleteObject]),
            )
            .await
            .unwrap();

        let user = &users[user_idx];
        let namespaces = namespaces_idx
            .iter()
            .map(|i| &namespaces[*i])
            .collect::<Vec<_>>();

        let result = user.can(&pool, permissions, namespaces).await;

        match (result, expected) {
            (Ok(()), true) => {
                // Success case: We expected permission and got it
            },
            (Err(ApiError::Forbidden(_)), false) => {
                // Expected failure case: We expected no permission and got Forbidden error
            },
            (Ok(()), false) => {
                if user.is_admin(&pool).await.unwrap() {
                    panic!("Expected permission check to fail, but it succeeded (user is admin)");
                } else {
                    panic!("Expected permission check to fail, but it succeeded");
                }
            },
            (Err(ApiError::Forbidden(msg)), true) => {
                panic!("Expected permission check to succeed, but got Forbidden error: {msg}");
            },
            (Err(e), _) => {
                panic!("Unexpected error occurred: {e:?}");
            },
        }
    }
}
