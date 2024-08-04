use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl, Table};
use std::iter::IntoIterator;

use tracing::debug;

use crate::models::{Permissions, User, UserID};

use crate::traits::{GroupAccessors, NamespaceAccessors, SelfAccessors};

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;

use crate::models::search::{FilterField, ParsedQueryParam};

use crate::{date_search, numeric_search, string_search, trace_query};

pub trait UserPermissions: SelfAccessors<User> + GroupAccessors {
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
    
        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;
    
        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: HashSet<i32> = stream::iter(namespaces)
            .then(|n| async move { n.namespace_id(pool).await })
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
    }}

impl UserPermissions for User {}
impl UserPermissions for UserID {}

impl User {
    pub async fn search_users(
        &self,
        pool: &DbPool,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<User>, ApiError> {
        use crate::schema::users::dsl::*;

        debug!(
            message = "Searching users",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut conn = pool.get()?;

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

        let result = base_query
            .select(users::all_columns())
            .distinct() // TODO: Is it the joins that makes this required?
            .load::<User>(&mut conn)?;

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

        let namespaces = vec![
            create_namespace(&pool, &format!("test_user_can_ns1_{}", suffix))
                .await
                .unwrap(),
            create_namespace(&pool, &format!("test_user_can_ns2_{}", suffix))
                .await
                .unwrap(),
        ];
        let groups = vec![
            create_test_group(&pool).await,
            create_test_group(&pool).await,
        ];
        let users = vec![
            create_user_with_params(&pool, &format!("test_user_can_u1_{}", suffix), "foo").await,
            create_user_with_params(&pool, &format!("test_user_can_u2_{}", suffix), "foo").await,
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
                panic!("Expected permission check to fail, but it succeeded");
            },
            (Err(ApiError::Forbidden(msg)), true) => {
                panic!("Expected permission check to succeed, but got Forbidden error: {}", msg);
            },
            (Err(e), _) => {
                panic!("Unexpected error occurred: {:?}", e);
            },
        }
    }
}
