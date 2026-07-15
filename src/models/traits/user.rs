use std::iter::IntoIterator;

use crate::models::search::QueryOptions;
use crate::models::{
    ClassGraphRow, Collection, ExportIncludeRelatedQuery, Group, HubuumClass, HubuumClassExpanded,
    HubuumClassRelation, HubuumObject, HubuumObjectRelation, Permissions, RelatedObjectForRootRow,
    RelatedObjectGraphRow, RelatedObjectIncludeRow, UnifiedSearchSpec, User, UserID,
};

use crate::db::DbPool;
use crate::db::traits::user::{
    LoadPermittedCollections, LoadUserGroups, LoadUserGroupsPaginated, LoadUserRecord,
    UnifiedSearchBackend, UserSearchBackend,
};
use crate::errors::ApiError;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{AuthzSubject, BackendContext, ClassAccessors, SelfAccessors};

/// Search resources that are visible to a user.
///
/// The methods on this trait delegate into backend search implementations while keeping the
/// model-facing API expressed in terms of `User` / `UserID` style accessors.
pub trait Search: UserCollectionAccessors {
    async fn search_collections<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_collections_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn count_collections<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_collections_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn search_classes<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_classes_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn count_classes<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_classes_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn search_objects<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_objects_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn count_objects<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_objects_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn search_class_relations<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_class_relations_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn class_relations_page<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.class_relations_page_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn search_classes_related_to<C, K>(
        &self,
        backend: &C,
        class: K,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<ClassGraphRow>, ApiError>
    where
        C: BackendContext + ?Sized,
        K: SelfAccessors<HubuumClass>,
    {
        self.search_classes_related_to_from_backend(backend.db_pool(), class, query_options, scopes)
            .await
    }

    async fn classes_related_to_page<C, K>(
        &self,
        backend: &C,
        class: K,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<(Vec<ClassGraphRow>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        K: SelfAccessors<HubuumClass>,
    {
        self.classes_related_to_page_from_backend(backend.db_pool(), class, query_options, scopes)
            .await
    }

    async fn class_relations_touching_page<C, K>(
        &self,
        backend: &C,
        class: K,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        K: SelfAccessors<HubuumClass>,
    {
        self.class_relations_touching_page_from_backend(
            backend.db_pool(),
            class,
            query_options,
            scopes,
        )
        .await
    }

    async fn search_class_relations_between_ids<C>(
        &self,
        backend: &C,
        class_ids: &[i32],
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_class_relations_between_ids_from_backend(backend.db_pool(), class_ids, scopes)
            .await
    }

    async fn search_object_relations<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_object_relations_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn object_relations_page<C>(
        &self,
        backend: &C,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.object_relations_page_from_backend(backend.db_pool(), query_options, scopes)
            .await
    }

    async fn search_objects_related_to<C, O>(
        &self,
        backend: &C,
        object: O,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<RelatedObjectGraphRow>, ApiError>
    where
        C: BackendContext + ?Sized,
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        self.search_objects_related_to_from_backend(
            backend.db_pool(),
            object,
            query_options,
            scopes,
        )
        .await
    }

    async fn objects_related_to_page<C, O>(
        &self,
        backend: &C,
        object: O,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<(Vec<RelatedObjectGraphRow>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        self.objects_related_to_page_from_backend(backend.db_pool(), object, query_options, scopes)
            .await
    }

    async fn related_objects_for_roots<C>(
        &self,
        backend: &C,
        root_object_ids: &[i32],
        include: ExportIncludeRelatedQuery,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<RelatedObjectIncludeRow>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.related_objects_for_roots_from_backend(
            backend.db_pool(),
            root_object_ids,
            include,
            scopes,
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots<C>(
        &self,
        backend: &C,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.bidirectionally_related_objects_for_roots_from_backend(
            backend.db_pool(),
            root_object_ids,
            max_depth,
            per_root_cap,
            scopes,
        )
        .await
    }

    async fn object_relations_touching_page<C, O>(
        &self,
        backend: &C,
        object: O,
        query_options: QueryOptions,
        scopes: Option<&[Permissions]>,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        C: BackendContext + ?Sized,
        O: SelfAccessors<HubuumObject>,
    {
        self.object_relations_touching_page_from_backend(
            backend.db_pool(),
            object,
            query_options,
            scopes,
        )
        .await
    }

    async fn search_object_relations_between_ids<C>(
        &self,
        backend: &C,
        object_ids: &[i32],
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_object_relations_between_ids_from_backend(backend.db_pool(), object_ids, scopes)
            .await
    }

    async fn search_unified_collections<C>(
        &self,
        backend: &C,
        query: &UnifiedSearchSpec,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<Collection>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_unified_collections_from_backend(backend.db_pool(), query, scopes)
            .await
    }

    async fn search_unified_classes<C>(
        &self,
        backend: &C,
        query: &UnifiedSearchSpec,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_unified_classes_from_backend(backend.db_pool(), query, scopes)
            .await
    }

    async fn search_unified_objects<C>(
        &self,
        backend: &C,
        query: &UnifiedSearchSpec,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.search_unified_objects_from_backend(backend.db_pool(), query, scopes)
            .await
    }
}

/// Access groups and related backend-backed filters for a user.
pub trait GroupAccessors: AuthzSubject {
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
}

/// Access collections that are visible to a user through direct or group-derived permissions.
pub trait UserCollectionAccessors: GroupAccessors + AuthzSubject {
    /// Return all collections that the user has CollectionPermissions::ReadCollection on.
    async fn collections_read<C>(&self, backend: &C) -> Result<Vec<Collection>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.collections(backend, &[Permissions::ReadCollection])
            .await
    }

    /// Return all collections that the user has the given permissions on.
    async fn collections<'a, C, I>(
        &self,
        backend: &C,
        permissions_list: &'a I,
    ) -> Result<Vec<Collection>, ApiError>
    where
        C: BackendContext + ?Sized,
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        // NOTE: scopes are passed as `None` here (unscoped). Live token-scope
        // threading through the collection/search visibility helpers is wired in
        // the handler/search-scope pass; the admin fast path stays correct for
        // the `None` case.
        self.load_collections_with_permissions(backend.db_pool(), permissions_list, None)
            .await
    }
}

// Group/collection accessors are available to every authorization subject (human
// users, service accounts, bare principals) via the identity-only contract.
impl<T: AuthzSubject + ?Sized> GroupAccessors for T {}
impl<T: GroupAccessors + AuthzSubject + ?Sized> UserCollectionAccessors for T {}

impl<T: UserCollectionAccessors + ?Sized> Search for T {}

// User list/search cursoring lives on `UserWithName` (which carries the
// principal name); `User` itself maps the `users` table and is not cursor-sorted
// directly.

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
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
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
    use crate::models::search::ParsedQueryParam;
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
            include_total: true,
        }
    }

    #[rstest]
    #[actix_rt::test]
    async fn test_user_permissions_collection_and_class_listing(
        #[future(awt)] test_context: TestContext,
    ) {
        use crate::models::collection::NewCollection;
        use crate::models::search::{FilterField, ParsedQueryParam, SearchOperator};

        let context = test_context;
        let test_user_1 = create_test_user(&context.pool).await;
        let test_group_1 = create_test_group(&context.pool).await;
        let test_user_2 = create_test_user(&context.pool).await;
        let test_group_2 = create_test_group(&context.pool).await;

        test_group_1
            .add_member_without_events(&context.pool, &test_user_1)
            .await
            .unwrap();
        test_group_2
            .add_member_without_events(&context.pool, &test_user_2)
            .await
            .unwrap();

        let collection_fixture = NewCollection {
            name: "test_user_collection_listing".to_string(),
            description: "Test collection".to_string(),
            parent_collection_id: None,
        }
        .save_and_grant_all_to(&context.pool, GroupID::new(test_group_1.id).unwrap())
        .await
        .unwrap();

        let class = NewHubuumClass {
            name: "test_user_collection_listing".to_string(),
            description: "Test class".to_string(),
            json_schema: None,
            validate_schema: None,
            collection_id: collection_fixture.id,
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        class
            .grant_without_events(
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

        let read_collection_param = ParsedQueryParam {
            field: FilterField::Permissions,
            operator: SearchOperator::Equals { is_negated: false },
            value: "ReadCollection".to_string(),
        };

        let collection_list = test_user_1
            .search_collections(
                &context.pool,
                make_query_options_from_query_param(&read_collection_param),
                None,
            )
            .await
            .unwrap();
        assert_contains!(&collection_list, &collection_fixture);

        let collection_list = test_user_2
            .search_collections(
                &context.pool,
                make_query_options_from_query_param(&read_collection_param),
                None,
            )
            .await
            .unwrap();
        assert_not_contains!(&collection_list, &collection_fixture);

        let classlist = test_user_1
            .search_classes(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
                None,
            )
            .await
            .unwrap();
        assert_contains!(&classlist, &class);

        let classlist = test_user_2
            .search_classes(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
                None,
            )
            .await
            .unwrap();
        assert_not_contains!(&classlist, &class);

        collection_fixture
            .grant_one(&context.pool, test_group_2.id, Permissions::ReadCollection)
            .await
            .unwrap();

        let collection_list = test_user_2
            .search_collections(
                &context.pool,
                make_query_options_from_query_param(&read_collection_param),
                None,
            )
            .await
            .unwrap();
        assert_contains!(&collection_list, &collection_fixture);

        let classlist = test_user_1
            .search_classes(
                &context.pool,
                QueryOptions {
                    filters: vec![],
                    sort: vec![],
                    limit: None,
                    cursor: None,
                    include_total: true,
                },
                None,
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
                None,
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
                None,
            )
            .await
            .unwrap();
        assert_not_contains!(&classlist, &class);

        let collection_list = test_user_2
            .search_collections(
                &context.pool,
                make_query_options_from_query_param(&read_class_param),
                None,
            )
            .await
            .unwrap();
        assert_contains!(&collection_list, &collection_fixture);

        collection_fixture
            .revoke_all_without_events(&context.pool, test_group_2.id)
            .await
            .unwrap();

        let collection_list = test_user_2
            .search_collections(
                &context.pool,
                make_query_options_from_query_param(&read_collection_param),
                None,
            )
            .await
            .unwrap();
        assert_not_contains!(&collection_list, &collection_fixture);

        test_user_1
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        test_user_2
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        test_group_1
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        test_group_2
            .delete_without_events(&context.pool)
            .await
            .unwrap();
        collection_fixture
            .delete_without_events(&context.pool)
            .await
            .unwrap();
    }
}
