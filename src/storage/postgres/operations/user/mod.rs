use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use diesel::{AsChangeset, ExpressionMethods, JoinOnDsl, QueryDsl, Queryable, Selectable, Table};
use std::iter::IntoIterator;

use tracing::debug;

use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, QueryParamsExt, SearchOperator, SortParam,
};
use crate::models::traits::ExpandCollectionFromMap;
use crate::models::traits::user::UserCollectionAccessors;
use crate::models::{
    ClassGraphRow, Collection, ExportIncludeRelatedDirection, ExportIncludeRelatedQuery,
    ExportIncludeRelatedSort, Group, HubuumClass, HubuumClassExpanded, HubuumClassRelation,
    HubuumObject, HubuumObjectRelation, NewUser, Permissions, PermissionsList,
    RelatedObjectGraphRow, RelatedObjectIncludeRow, UpdateUser, User, UserWithName,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{with_connection, with_transaction};
use crate::traits::{ClassAccessors, CursorPaginated, CursorValue, GroupAccessors, SelfAccessors};

use crate::{date_search, numeric_search, revision_search, string_search, trace_query};

mod auth;
mod membership;
mod object_aggregate;
pub(crate) mod search;
mod unified_search;

pub use auth::*;
pub use membership::*;
pub(crate) use object_aggregate::aggregate_objects;
pub use search::*;
pub use unified_search::*;

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::users)]
pub(crate) struct UserRow {
    pub(crate) id: i32,
    pub(crate) kind: String,
    pub(crate) password: Option<String>,
    pub(crate) proper_name: Option<String>,
    pub(crate) email: Option<String>,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) anonymized_at: Option<chrono::NaiveDateTime>,
}

impl From<UserRow> for User {
    fn from(row: UserRow) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            password: row.password,
            proper_name: row.proper_name,
            email: row.email,
            created_at: row.created_at,
            updated_at: row.updated_at,
            anonymized_at: row.anonymized_at,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::users)]
struct UpdateUserRow<'a> {
    password: Option<&'a String>,
    proper_name: Option<&'a String>,
    email: Option<&'a String>,
}

impl<'a> From<&'a UpdateUser> for UpdateUserRow<'a> {
    fn from(update: &'a UpdateUser) -> Self {
        Self {
            password: update.password.as_ref(),
            proper_name: update.proper_name.as_ref(),
            email: update.email.as_ref(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct UserWithNameQueryRow(pub(crate) UserWithName);

impl CursorPaginated for UserWithNameQueryRow {
    fn supports_sort(field: &FilterField) -> bool {
        UserWithName::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        self.0.cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        UserWithName::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        UserWithName::tie_breaker_sort()
    }
}

impl CursorSqlMapping for UserWithNameQueryRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "users.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Username => CursorSqlField {
                column: "principals.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::IdentityScope => CursorSqlField {
                column: "identity_scopes.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::ProperName => CursorSqlField {
                column: "users.proper_name",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::Email => CursorSqlField {
                column: "users.email",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "users.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "users.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "principals.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    use crate::models::{GroupID, Permissions as P, PermissionsList as PL};
    use crate::tests::{TestScope, create_test_group, create_user_with_params};
    use crate::traits::{AuthzSubject, PermissionController, UserPermissions};

    // user_idx, collections_idx, permissions, expected
    #[rstest]
    #[case::u1_collection1_classread_true(0, vec![0], vec![P::ReadClass], true)]
    #[case::u1_collection1_classcreate_true(0, vec![0], vec![P::CreateClass], true)]
    #[case::u1_collection1_classreadcreate_true(0, vec![0], vec![P::ReadClass, P::CreateClass], true)]
    #[case::u1_collection2_classdelete_true(0, vec![1], vec![P::DeleteClass], true)]
    #[case::u1_collection2_classcreate_true(0, vec![1], vec![P::CreateClass], true)]
    #[case::u1_collection2_classcreatedelete_true(0, vec![1], vec![P::CreateClass, P::DeleteClass], true)]
    #[case::u1_collection12_classcreate_true(0, vec![0, 1], vec![P::CreateClass], true)]
    #[case::u1_collection1_objectread_false(0, vec![0], vec![P::ReadObject], false)]
    #[case::u1_collection1_collectioncreate_false(0, vec![0], vec![P::ReadCollection], false)]
    #[case::u1_collection12_classreadcreate_false(0, vec![0, 1], vec![P::CreateClass, P::ReadClass], false)]
    #[case::u1_collection12_classreadcreatedelete_false(
        0,
        vec![0, 1],
        vec![P::CreateClass, P::ReadClass, P::DeleteClass],
        false
    )]
    #[case::u2_collection1_objectread_true(1, vec![0], vec![P::ReadObject], true)]
    #[case::u2_collection1_objectcreate_true(1, vec![0], vec![P::CreateObject], true)]
    #[case::u2_collection1_objectreadcreate_true(1, vec![0], vec![P::ReadObject, P::CreateObject], true)]
    #[case::u2_collection2_objectdelete_true(1, vec![1], vec![P::DeleteObject], true)]
    #[case::u2_collection2_objectcreate_true(1, vec![1], vec![P::CreateObject], true)]
    #[case::u2_collection2_objectcreatedelete_true(1, vec![1], vec![P::CreateObject, P::DeleteObject], true)]
    #[actix_web::test]
    async fn test_user_can(
        #[case] user_idx: usize,
        #[case] collections_idx: Vec<usize>,
        #[case] permissions: Vec<Permissions>,
        #[case] expected: bool,
    ) {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let suffix = format!(
            "_{}_{}_{}_{}",
            user_idx,
            collections_idx
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

        let collections = [
            scope
                .collection_fixture(&format!("test_user_can_collection1_{suffix}"))
                .await,
            scope
                .collection_fixture(&format!("test_user_can_collection2_{suffix}"))
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

        groups[0]
            .add_member_without_events(&pool, &users[0])
            .await
            .unwrap();
        groups[1]
            .add_member_without_events(&pool, &users[1])
            .await
            .unwrap();

        collections[0]
            .collection
            .grant_without_events(
                &pool,
                GroupID::new(groups[0].id).unwrap(),
                PL::new(vec![P::CreateClass, P::ReadClass]),
            )
            .await
            .unwrap();
        collections[1]
            .collection
            .grant_without_events(
                &pool,
                GroupID::new(groups[0].id).unwrap(),
                PL::new(vec![P::CreateClass, P::DeleteClass]),
            )
            .await
            .unwrap();

        collections[0]
            .collection
            .grant_without_events(
                &pool,
                GroupID::new(groups[1].id).unwrap(),
                PL::new(vec![P::CreateObject, P::ReadObject]),
            )
            .await
            .unwrap();
        collections[1]
            .collection
            .grant_without_events(
                &pool,
                GroupID::new(groups[1].id).unwrap(),
                PL::new(vec![P::CreateObject, P::DeleteObject]),
            )
            .await
            .unwrap();

        let user = &users[user_idx];
        let collections = collections_idx
            .iter()
            .map(|i| &collections[*i].collection)
            .collect::<Vec<_>>();

        let result = user.can(&pool, permissions, collections, None).await;

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
