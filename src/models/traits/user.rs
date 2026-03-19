use std::iter::IntoIterator;

use crate::models::search::{FilterField, ParsedQueryParam, QueryOptions, SortParam};
use crate::models::{
    ClassClosureRow, Group, HubuumClass, HubuumClassExpanded, HubuumClassRelation, HubuumObject,
    HubuumObjectRelation, Namespace, Permissions, RelatedObjectClosureRow, UnifiedSearchSpec, User,
    UserID,
};

use crate::db::DbPool;
use crate::db::traits::user::{
    LoadPermittedNamespaces, LoadUserGroups, LoadUserGroupsPaginated, LoadUserRecord,
    QueryJsonDataIds, QueryJsonSchemaIds, UnifiedSearchBackend, UserSearchBackend,
};
use crate::errors::ApiError;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    BackendContext, ClassAccessors, CursorPaginated, CursorSqlField, CursorSqlMapping,
    CursorSqlType, CursorValue, GroupMemberships, SelfAccessors,
};

/// Search resources that are visible to a user.
///
/// The methods on this trait delegate into backend search implementations while keeping the
/// model-facing API expressed in terms of `User` / `UserID` style accessors.
pub trait Search: SelfAccessors<User> + UserNamespaceAccessors {
    async fn search_namespaces<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_namespaces_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn count_namespaces<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_namespaces_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn search_classes<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_classes_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn count_classes<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_classes_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn search_objects<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_objects_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn count_objects<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_objects_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn search_class_relations<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_class_relations_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn class_relations_page<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.class_relations_page_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn search_classes_related_to<C, K>(
        &self,
        backend: &C,
        class: K,
        query_options: QueryOptions,
    ) -> Result<Vec<ClassClosureRow>, ApiError>
    where
        C: BackendContext + ?Sized,
        K: SelfAccessors<HubuumClass>,
    {
        self.search_classes_related_to_from_backend(backend.db_pool(), class, query_options)
            .await
    }

    async fn classes_related_to_page<C, K>(
        &self,
        backend: &C,
        class: K,
        query_options: QueryOptions,
    ) -> Result<(Vec<ClassClosureRow>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        K: SelfAccessors<HubuumClass>,
    {
        self.classes_related_to_page_from_backend(backend.db_pool(), class, query_options)
            .await
    }

    async fn class_relations_touching_page<C, K>(
        &self,
        backend: &C,
        class: K,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        K: SelfAccessors<HubuumClass>,
    {
        self.class_relations_touching_page_from_backend(backend.db_pool(), class, query_options)
            .await
    }

    async fn search_class_relations_between_ids<C>(
        &self,
        backend: &C,
        class_ids: &[i32],
    ) -> Result<Vec<HubuumClassRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_class_relations_between_ids_from_backend(backend.db_pool(), class_ids)
            .await
    }

    async fn search_object_relations<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_object_relations_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn object_relations_page<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.object_relations_page_from_backend(backend.db_pool(), query_options)
            .await
    }

    async fn search_objects_related_to<C, O>(
        &self,
        backend: &C,
        object: O,
        query_options: QueryOptions,
    ) -> Result<Vec<RelatedObjectClosureRow>, ApiError>
    where
        C: BackendContext + ?Sized,
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        self.search_objects_related_to_from_backend(backend.db_pool(), object, query_options)
            .await
    }

    async fn objects_related_to_page<C, O>(
        &self,
        backend: &C,
        object: O,
        query_options: QueryOptions,
    ) -> Result<(Vec<RelatedObjectClosureRow>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        self.objects_related_to_page_from_backend(backend.db_pool(), object, query_options)
            .await
    }

    async fn object_relations_touching_page<C, O>(
        &self,
        backend: &C,
        object: O,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        O: SelfAccessors<HubuumObject>,
    {
        self.object_relations_touching_page_from_backend(backend.db_pool(), object, query_options)
            .await
    }

    async fn search_object_relations_between_ids<C>(
        &self,
        backend: &C,
        object_ids: &[i32],
    ) -> Result<Vec<HubuumObjectRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_object_relations_between_ids_from_backend(backend.db_pool(), object_ids)
            .await
    }

    async fn search_unified_namespaces<C>(
        &self,
        backend: &C,
        query: &UnifiedSearchSpec,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_unified_namespaces_from_backend(backend.db_pool(), query)
            .await
    }

    async fn search_unified_classes<C>(
        &self,
        backend: &C,
        query: &UnifiedSearchSpec,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_unified_classes_from_backend(backend.db_pool(), query)
            .await
    }

    async fn search_unified_objects<C>(
        &self,
        backend: &C,
        query: &UnifiedSearchSpec,
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_unified_objects_from_backend(backend.db_pool(), query)
            .await
    }
}

/// Access groups and related backend-backed filters for a user.
pub trait GroupAccessors: SelfAccessors<User> {
    /// Return all groups that the user is a member of.
    #[allow(async_fn_in_trait, dead_code)]
    async fn groups<C>(&self, backend: &C) -> Result<Vec<Group>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_user_groups(backend.db_pool()).await
    }

    #[allow(async_fn_in_trait)]
    async fn groups_paginated_with_total_count<C>(
        &self,
        backend: &C,
        query_options: &QueryOptions,
    ) -> Result<(Vec<Group>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_user_groups_paginated_with_total_count(backend.db_pool(), query_options)
            .await
    }

    /// Execute the JSON schema filter query for classes and return matching class IDs.
    ///
    /// The name is historical: this returns the executed result set rather than a Diesel subquery.
    fn json_schema_subquery<C>(
        &self,
        backend: &C,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.query_class_ids_for_json_schema(backend.db_pool(), json_schema_query_params)
    }

    /// Execute the JSON data filter query for objects and return matching object IDs.
    ///
    /// The name is historical: this returns the executed result set rather than a Diesel subquery.
    fn json_data_subquery<C>(
        &self,
        backend: &C,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.query_object_ids_for_json_data(backend.db_pool(), json_schema_query_params)
    }
}

/// Access namespaces that are visible to a user through direct or group-derived permissions.
pub trait UserNamespaceAccessors: SelfAccessors<User> + GroupAccessors + GroupMemberships {
    /// Return all namespaces that the user has NamespacePermissions::ReadCollection on.
    #[allow(dead_code)] // Lazy-used in tests.
    async fn namespaces_read<C>(&self, backend: &C) -> Result<Vec<Namespace>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.namespaces(backend, &[Permissions::ReadCollection])
            .await
    }

    /// Return all namespaces that the user has the given permissions on.
    async fn namespaces<'a, C, I>(
        &self,
        backend: &C,
        permissions_list: &'a I,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        C: BackendContext + ?Sized,
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        self.load_namespaces_with_permissions(backend.db_pool(), permissions_list)
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
    use crate::tests::{TestContext, create_test_group, create_test_user, test_context};
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
