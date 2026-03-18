use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl, Table};
use std::iter::IntoIterator;

use tracing::{debug, trace};

use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, QueryParamsExt, SearchOperator,
};
use crate::models::traits::ExpandNamespaceFromMap;
use crate::models::traits::user::UserNamespaceAccessors;
use crate::models::{
    ClassGraphRow, Group, HubuumClass, HubuumClassExpanded, HubuumClassRelation, HubuumObject,
    HubuumObjectRelation, Namespace, NewUser, Permissions, RelatedObjectGraphRow, Token,
    UpdateUser, User, UserID, UserToken,
};
use crate::traits::{ClassAccessors, GroupAccessors, NamespaceAccessors, SelfAccessors};
use crate::utilities::auth::hash_password;
use crate::utilities::extensions::CustomStringExtensions;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;

use crate::{date_search, numeric_search, string_search, trace_query};

mod auth;
mod membership;
mod permissions;
mod search;
mod unified_search;

pub use auth::*;
pub use membership::*;
pub use permissions::*;
pub use search::*;
pub use unified_search::*;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    use crate::models::{Permissions as P, PermissionsList as PL};
    use crate::tests::{TestScope, create_test_group, create_user_with_params};
    use crate::traits::PermissionController;

    // user_idx, namespaces_idx, permissions, expected
    #[rstest]
    #[case::u1_ns1_classread_true(0, vec![0], vec![P::ReadClass], true)]
    #[case::u1_ns1_classcreate_true(0, vec![0], vec![P::CreateClass], true)]
    #[case::u1_ns1_classreadcreate_true(0, vec![0], vec![P::ReadClass, P::CreateClass], true)]
    #[case::u1_ns2_classdelete_true(0, vec![1], vec![P::DeleteClass], true)]
    #[case::u1_ns2_classcreate_true(0, vec![1], vec![P::CreateClass], true)]
    #[case::u1_ns2_classcreatedelete_true(0, vec![1], vec![P::CreateClass, P::DeleteClass], true)]
    #[case::u1_ns12_classcreate_true(0, vec![0, 1], vec![P::CreateClass], true)]
    #[case::u1_ns1_objectread_false(0, vec![0], vec![P::ReadObject], false)]
    #[case::u1_ns1_namespacecreate_false(0, vec![0], vec![P::ReadCollection], false)]
    #[case::u1_ns12_classreadcreate_false(0, vec![0, 1], vec![P::CreateClass, P::ReadClass], false)]
    #[case::u1_ns12_classreadcreatedelete_false(
        0,
        vec![0, 1],
        vec![P::CreateClass, P::ReadClass, P::DeleteClass],
        false
    )]
    #[case::u2_ns1_objectread_true(1, vec![0], vec![P::ReadObject], true)]
    #[case::u2_ns1_objectcreate_true(1, vec![0], vec![P::CreateObject], true)]
    #[case::u2_ns1_objectreadcreate_true(1, vec![0], vec![P::ReadObject, P::CreateObject], true)]
    #[case::u2_ns2_objectdelete_true(1, vec![1], vec![P::DeleteObject], true)]
    #[case::u2_ns2_objectcreate_true(1, vec![1], vec![P::CreateObject], true)]
    #[case::u2_ns2_objectcreatedelete_true(1, vec![1], vec![P::CreateObject, P::DeleteObject], true)]
    #[actix_web::test]
    async fn test_user_can(
        #[case] user_idx: usize,
        #[case] namespaces_idx: Vec<usize>,
        #[case] permissions: Vec<Permissions>,
        #[case] expected: bool,
    ) {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
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

        let namespaces = [
            scope
                .namespace_fixture(&format!("test_user_can_ns1_{suffix}"))
                .await,
            scope
                .namespace_fixture(&format!("test_user_can_ns2_{suffix}"))
                .await,
        ];
        let groups = [
            create_test_group(&pool).await,
            create_test_group(&pool).await,
        ];
        let users = [
            create_user_with_params(&pool, &format!("test_user_can_u1_{suffix}"), "foo").await,
            create_user_with_params(&pool, &format!("test_user_can_u2_{suffix}"), "foo").await,
        ];

        groups[0].add_member(&pool, &users[0]).await.unwrap();
        groups[1].add_member(&pool, &users[1]).await.unwrap();

        namespaces[0]
            .namespace
            .grant(
                &pool,
                groups[0].id,
                PL::new(vec![P::CreateClass, P::ReadClass]),
            )
            .await
            .unwrap();
        namespaces[1]
            .namespace
            .grant(
                &pool,
                groups[0].id,
                PL::new(vec![P::CreateClass, P::DeleteClass]),
            )
            .await
            .unwrap();

        namespaces[0]
            .namespace
            .grant(
                &pool,
                groups[1].id,
                PL::new(vec![P::CreateObject, P::ReadObject]),
            )
            .await
            .unwrap();
        namespaces[1]
            .namespace
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
            .map(|i| &namespaces[*i].namespace)
            .collect::<Vec<_>>();

        let result = user.can(&pool, permissions, namespaces).await;

        match (result, expected) {
            (Ok(()), true) => {
                // Success case: We expected permission and got it
            }
            (Err(ApiError::Forbidden(_)), false) => {
                // Expected failure case: We expected no permission and got Forbidden error
            }
            (Ok(()), false) => {
                if user.is_admin(&pool).await.unwrap() {
                    panic!("Expected permission check to fail, but it succeeded (user is admin)");
                } else {
                    panic!("Expected permission check to fail, but it succeeded");
                }
            }
            (Err(ApiError::Forbidden(msg)), true) => {
                panic!("Expected permission check to succeed, but got Forbidden error: {msg}");
            }
            (Err(e), _) => {
                panic!("Unexpected error occurred: {e:?}");
            }
        }
    }
}
