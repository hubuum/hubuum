use argon2::password_hash::rand_core::le;
use diesel::dsl::Filter;
use diesel::query_builder;
use diesel::sql_types::Integer;
use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl, Table, pg::Pg};

use std::iter::IntoIterator;

use futures::future::try_join_all;
use tracing::debug;

use crate::api::v1::handlers::namespaces;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, QueryParamsExt, SearchOperator, SortParam,
};
use crate::models::traits::ExpandNamespaceFromMap;
use crate::models::{
    ClassClosureView, Group, HubuumClass, HubuumClassExpanded, HubuumClassRelation, HubuumObject,
    HubuumObjectRelation, HubuumObjectWithPath, Namespace, ObjectClosureView, Permission,
    Permissions, User, UserID, class, group, permissions,
};

use crate::schema::hubuumclass::namespace_id;
use crate::schema::{hubuumclass, hubuumobject};
use crate::traits::{
    ClassAccessors, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
    NamespaceAccessors, SelfAccessors,
};
use crate::traits::accessors::{IdAccessor, InstanceAdapter};

use crate::db::traits::user::{
    LoadPermittedNamespaces, LoadUserGroups, LoadUserRecord, QueryJsonDataIds,
    QueryJsonSchemaIds, UserSearchBackend,
};
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;

pub trait Search: SelfAccessors<User> + GroupAccessors + UserNamespaceAccessors {
    async fn search_namespaces(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<Namespace>, ApiError> {
        self.search_namespaces_from_backend(pool, query_options).await
    }

    async fn search_classes(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        self.search_classes_from_backend(pool, query_options).await
    }

    async fn search_objects(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        self.search_objects_from_backend(pool, query_options).await
    }

    async fn search_class_relations(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        self.search_class_relations_from_backend(pool, query_options)
            .await
    }

    async fn search_object_relations(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        self.search_object_relations_from_backend(pool, query_options)
            .await
    }

    async fn search_objects_related_to<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
    ) -> Result<Vec<ObjectClosureView>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        self.search_objects_related_to_from_backend(pool, object, query_options)
            .await
    }
}

pub trait GroupAccessors: SelfAccessors<User> {
    /// Return all groups that the user is a member of.
    #[allow(async_fn_in_trait, dead_code)]
    async fn groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        self.load_user_groups(pool).await
    }

    #[allow(async_fn_in_trait)]
    async fn groups_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};
        use crate::{date_search, numeric_search, string_search};

        let mut base_query = user_groups
            .inner_join(groups.on(id.eq(group_id)))
            .filter(user_id.eq(self.id()))
            .select(groups::all_columns())
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

        crate::apply_query_options!(base_query, query_options, Group);

        with_connection(pool, |conn| base_query.load::<Group>(conn))
    }

    /*
      async fn group_ids(&self, pool: &DbPool) -> Result<Vec<i32>, ApiError> {
          use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

          let mut conn = pool.get()?;
          let group_list = user_groups
              .filter(user_id.eq(self.id()))
              .select(group_id)
              .load::<i32>(&mut conn)?;

          Ok(group_list)
      }
    */

    fn json_schema_subquery(
        &self,
        pool: &DbPool,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError> {
        self.query_class_ids_for_json_schema(pool, json_schema_query_params)
    }

    // Umm, async? Also, the name implies we return a subquery, but we return the Vec<i32> of the executed query.
    fn json_data_subquery(
        &self,
        pool: &DbPool,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError> {
        self.query_object_ids_for_json_data(pool, json_schema_query_params)
    }
}

pub trait UserNamespaceAccessors: SelfAccessors<User> + GroupAccessors {
    /// Return all namespaces that the user has NamespacePermissions::ReadCollection on.
    #[allow(dead_code)] // Lazy-used in tests.
    async fn namespaces_read(&self, pool: &DbPool) -> Result<Vec<Namespace>, ApiError> {
        self.namespaces(pool, &[Permissions::ReadCollection]).await
    }

    /// Return all namespaces that the user has the given permissions on.
    async fn namespaces<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        self.load_namespaces_with_permissions(pool, permissions_list)
            .await
    }
}

impl UserNamespaceAccessors for User {}
impl UserNamespaceAccessors for UserID {}

impl GroupAccessors for User {}
impl GroupAccessors for UserID {}

impl GroupAccessors for &User {}
impl GroupAccessors for &UserID {}

impl Search for User {}
impl Search for UserID {}

fn string_or_null(value: Option<&str>) -> CursorValue {
    match value {
        Some(value) => CursorValue::String(value.to_string()),
        None => CursorValue::Null,
    }
}

impl CursorPaginated for User {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Username
                | FilterField::Email
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Username => CursorValue::String(self.username.clone()),
            FilterField::Email => string_or_null(self.email.as_deref()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for User {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "users.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Username => CursorSqlField {
                column: "users.username",
                sql_type: CursorSqlType::String,
                nullable: false,
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )));
            }
        })
    }
}

impl IdAccessor for User {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<User> for User {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<User, ApiError> {
        Ok(self.clone())
    }
}

impl IdAccessor for UserID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<User> for UserID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<User, ApiError> {
        self.load_user_record(pool).await
    }
}

#[cfg(test)]
mod test {

    use std::vec;

    use super::*;
    use crate::models::{GroupID, NewHubuumClass, Permissions, PermissionsList};
    use crate::tests::{
        TestContext, create_test_group, create_test_user, ensure_admin_group, ensure_admin_user,
        test_context,
    };
    use crate::traits::PermissionController;
    use crate::traits::{CanDelete, CanSave};
    use crate::{assert_contains, assert_not_contains};
    use rstest::rstest;

    fn make_query_options_from_query_param(filter: &ParsedQueryParam) -> QueryOptions {
        QueryOptions {
            filters: vec![filter.clone()],
            sort: vec![],
            limit: None,
            cursor: None,
        }
    }

    #[rstest]
    #[actix_rt::test]
    async fn test_user_permissions_namespace_and_class_listing(
        #[future(awt)] test_context: TestContext,
    ) {
        use crate::models::namespace::NewNamespace;
        use crate::models::search::{FilterField, ParsedQueryParam, SearchOperator};

        let context = test_context;
        let test_user_1 = create_test_user(&context.pool).await;
        let test_group_1 = create_test_group(&context.pool).await;
        let test_user_2 = create_test_user(&context.pool).await;
        let test_group_2 = create_test_group(&context.pool).await;

        test_group_1
            .add_member(&context.pool, &test_user_1)
            .await
            .unwrap();
        test_group_2
            .add_member(&context.pool, &test_user_2)
            .await
            .unwrap();

        let ns = NewNamespace {
            name: "test_user_namespace_listing".to_string(),
            description: "Test namespace".to_string(),
        }
        .save_and_grant_all_to(&context.pool, GroupID(test_group_1.id))
        .await
        .unwrap();

        let class = NewHubuumClass {
            name: "test_user_namespace_listing".to_string(),
            description: "Test class".to_string(),
            json_schema: None,
            validate_schema: None,
            namespace_id: ns.id,
        }
        .save(&context.pool)
        .await
        .unwrap();

        class
            .grant(
                &context.pool,
                test_group_1.id,
                PermissionsList::new([
                    Permissions::ReadClass,
                    Permissions::UpdateClass,
                    Permissions::DeleteClass,
                    Permissions::CreateObject,
                ]),
            )
            .await
            .unwrap();

        let read_class_param = ParsedQueryParam {
            field: FilterField::Permissions,
            operator: SearchOperator::Equals { is_negated: false },
            value: "ReadClass".to_string(),
        };

        let read_namespace_param = ParsedQueryParam {
            field: FilterField::Permissions,
            operator: SearchOperator::Equals { is_negated: false },
            value: "ReadCollection".to_string(),
        };

        let nslist = test_user_1
            .search_namespaces(
                &context.pool,
                make_query_options_from_query_param(&read_namespace_param),
            )
            .await
            .unwrap();
        assert_contains!(&nslist, &ns);

        let nslist = test_user_2
            .search_namespaces(
                &context.pool,
                make_query_options_from_query_param(&read_namespace_param),
            )
            .await
            .unwrap();
        assert_not_contains!(&nslist, &ns);

        let classlist = test_user_1
            .search_classes(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
            )
            .await
            .unwrap();
        assert_contains!(&classlist, &class);

        let classlist = test_user_2
            .search_classes(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
            )
            .await
            .unwrap();
        assert_not_contains!(&classlist, &class);

        ns.grant_one(&context.pool, test_group_2.id, Permissions::ReadCollection)
            .await
            .unwrap();

        let nslist = test_user_2
            .search_namespaces(
                &context.pool,
                make_query_options_from_query_param(&read_namespace_param),
            )
            .await
            .unwrap();
        assert_contains!(&nslist, &ns);

        let classlist = test_user_1
            .search_classes(
                &context.pool,
                QueryOptions {
                    filters: vec![],
                    sort: vec![],
                    limit: None,
                    cursor: None,
                },
            )
            .await
            .unwrap();
        assert_contains!(&classlist, &class);

        class
            .grant_one(&context.pool, test_group_2.id, Permissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2
            .search_classes(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
            )
            .await
            .unwrap();
        assert_contains!(&classlist, &class);

        class
            .revoke_one(&context.pool, test_group_2.id, Permissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2
            .search_classes(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
            )
            .await
            .unwrap();
        assert_not_contains!(&classlist, &class);

        let nslist = test_user_2
            .search_namespaces(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
            )
            .await
            .unwrap();
        assert_contains!(&nslist, &ns);

        ns.revoke_all(&context.pool, test_group_2.id).await.unwrap();

        let nslist = test_user_2
            .search_namespaces(
                &context.pool,
                make_query_options_from_query_param(&read_namespace_param),
            )
            .await
            .unwrap();
        assert_not_contains!(&nslist, &ns);

        test_user_1.delete(&context.pool).await.unwrap();
        test_user_2.delete(&context.pool).await.unwrap();
        test_group_1.delete(&context.pool).await.unwrap();
        test_group_2.delete(&context.pool).await.unwrap();
        ns.delete(&context.pool).await.unwrap();
    }
}
