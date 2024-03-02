use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl, Table};

use crate::api::v1::handlers::namespaces;
use crate::models::{
    class, permissions, Group, HubuumClass, HubuumObject, Namespace, Permission, Permissions, User,
    UserID,
};
use crate::schema::{hubuumclass, hubuumobject};
use crate::traits::{ClassAccessors, NamespaceAccessors, SelfAccessors};

use crate::db::DbPool;
use crate::errors::ApiError;

use futures::future::try_join_all;
use tracing::debug;

pub trait SearchClasses: SelfAccessors<User> + GroupAccessors + UserNamespaceAccessors {
    async fn search_classes(
        &self,
        pool: &DbPool,
        selected_namespaces: Vec<i32>,
        selected_permissions: Vec<Permissions>,
    ) -> Result<Vec<HubuumClass>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::hubuumclass::dsl::{hubuumclass, namespace_id as hubuum_classes_nid};
        use crate::schema::permissions::dsl::*;

        debug!(
            message = "Searching classes",
            stage = "Starting",
            user_id = self.id(),
            selected_namespaces = ?selected_namespaces,
            selected_permissions = ?selected_permissions
        );

        let mut conn = pool.get()?;
        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: Vec<i32> = self
            .namespaces_read(pool)
            .await?
            .into_iter()
            .filter(|n| selected_namespaces.is_empty() || selected_namespaces.contains(&n.id))
            .map(|n| n.id)
            .collect();

        debug!(message = "Searching classes", stage = "Filtered namespaces", filtered_namespaces = ?namespace_ids);

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(group_id_subquery));

        for perm in selected_permissions {
            base_query = PermissionFilter::filter(perm, base_query);
        }

        let result = base_query
            .inner_join(hubuumclass.on(hubuum_classes_nid.eq_any(namespace_ids)))
            .select(hubuumclass::all_columns())
            .load::<HubuumClass>(&mut conn)?;

        Ok(result)
    }
}

pub trait GroupAccessors: SelfAccessors<User> {
    /// Return all groups that the user is a member of.
    async fn groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

        let mut conn = pool.get()?;
        let group_list = user_groups
            .inner_join(groups.on(id.eq(group_id)))
            .filter(user_id.eq(self.id()))
            .select(groups::all_columns())
            .load::<Group>(&mut conn)?;

        Ok(group_list)
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

    /// Generate a subquery to get all group IDs for a user.
    ///
    /// Note that this does not execute the query, it only creates it.
    ///
    /// ## Example
    ///
    /// Check if a user has a specific class permission to a given namespace ID
    ///
    /// ```
    /// let group_id_subquery = user_id.group_ids_subquery();
    ///
    /// let base_query = classpermissions
    /// .into_boxed()
    /// .filter(namespace_id.eq(self.namespace_id))
    /// .filter(group_id.eq_any(group_id_subquery));
    ///
    /// let result = PermissionFilter::filter(permission, base_query)
    /// .first::<ClassPermission>(&mut conn)
    /// .optional()?;
    /// ```
    ///
    fn group_ids_subquery<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>
    {
        use crate::schema::user_groups::dsl::*;
        user_groups
            .filter(user_id.eq(self.id()))
            .select(group_id)
            .into_boxed()
    }
}

pub trait UserNamespaceAccessors: SelfAccessors<User> + GroupAccessors {
    /// Return all namespaces that the user has NamespacePermissions::ReadCollection on.
    async fn namespaces_read(&self, pool: &DbPool) -> Result<Vec<Namespace>, ApiError> {
        self.namespaces(pool, vec![Permissions::ReadCollection])
            .await
    }

    async fn namespaces(
        &self,
        pool: &DbPool,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::namespaces::dsl::{id as namespaces_table_id, namespaces};
        use crate::schema::permissions::dsl::{group_id, namespace_id, permissions};

        let mut conn = pool.get()?;

        let groups_id_subquery = self.group_ids_subquery();

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in permissions_list {
            base_query = PermissionFilter::filter(perm, base_query);
        }

        let result = base_query
            .inner_join(namespaces.on(namespace_id.eq(namespaces_table_id)))
            .select(namespaces::all_columns())
            .load::<Namespace>(&mut conn)?;

        Ok(result)
    }
}

pub trait UserClassAccessors: SearchClasses {
    async fn classes_read(&self, pool: &DbPool) -> Result<Vec<HubuumClass>, ApiError> {
        self.search_classes(pool, vec![], vec![Permissions::ReadClass])
            .await
    }

    async fn classes_read_within_namespaces<N: NamespaceAccessors>(
        &self,
        pool: &DbPool,
        namespaces: Vec<N>,
    ) -> Result<Vec<HubuumClass>, ApiError> {
        let futures: Vec<_> = namespaces
            .into_iter()
            .map(|n| {
                let pool_ref = &pool;
                async move { n.namespace_id(pool_ref).await }
            })
            .collect();
        let namespace_ids: Vec<i32> = try_join_all(futures).await?;

        self.search_classes(pool, namespace_ids, vec![Permissions::ReadClass])
            .await
    }

    async fn classes_within_namespaces_with_permissions<N: NamespaceAccessors>(
        &self,
        pool: &DbPool,
        namespaces: Vec<N>,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<HubuumClass>, ApiError> {
        let futures: Vec<_> = namespaces
            .into_iter()
            .map(|n| {
                let pool_ref = &pool;
                async move { n.namespace_id(pool_ref).await }
            })
            .collect();
        let namespace_ids: Vec<i32> = try_join_all(futures).await?;

        self.search_classes(pool, namespace_ids, permissions_list)
            .await
    }

    async fn classes_with_permissions(
        &self,
        pool: &DbPool,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<HubuumClass>, ApiError> {
        self.search_classes(pool, vec![], permissions_list).await
    }

    async fn classes(&self, pool: &DbPool) -> Result<Vec<HubuumClass>, ApiError> {
        self.search_classes(pool, vec![], vec![]).await
    }
}

pub trait ObjectAccessors: UserClassAccessors + UserNamespaceAccessors {
    async fn objects_in_class_read<C: UserClassAccessors>(
        &self,
        pool: &DbPool,
        class_id: C,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        self.objects_in_classes_read(pool, vec![class_id]).await
    }

    async fn objects_in_classes_read<C: UserClassAccessors>(
        &self,
        pool: &DbPool,
        class_ids: Vec<C>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        self.objects(pool, class_ids, vec![Permissions::ReadClass])
            .await
    }

    async fn objects<C: UserClassAccessors>(
        &self,
        pool: &DbPool,
        class_ids: Vec<C>,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::hubuumobject::dsl::{
            hubuum_class_id, hubuumobject, namespace_id as hubuumobject_nid,
        };
        use crate::schema::permissions::dsl::*;

        let mut conn = pool.get()?;
        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: Vec<i32> = self
            .namespaces_read(pool)
            .await?
            .iter()
            .map(|n| n.id)
            .collect();

        let mut base_query = permissions
            .into_boxed()
            .filter(namespace_id.eq_any(namespace_ids.clone()))
            .filter(group_id.eq_any(group_id_subquery));

        for perm in permissions_list {
            base_query = PermissionFilter::filter(perm, base_query);
        }

        let mut joined_query =
            base_query.inner_join(hubuumobject.on(hubuumobject_nid.eq_any(namespace_ids)));

        if !class_ids.is_empty() {
            let valid_class_ids = class_ids.iter().map(|c| c.id()).collect::<Vec<i32>>();
            joined_query = joined_query.filter(hubuum_class_id.eq_any(valid_class_ids));
        }

        let result = joined_query
            .select(hubuumobject::all_columns())
            .load::<HubuumObject>(&mut conn)?;

        Ok(result)
    }
}

impl UserNamespaceAccessors for User {}
impl UserNamespaceAccessors for UserID {}

impl UserClassAccessors for User {}
impl UserClassAccessors for UserID {}

impl GroupAccessors for User {}
impl GroupAccessors for UserID {}

impl SearchClasses for User {}
impl SearchClasses for UserID {}

impl SelfAccessors<User> for User {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<User, ApiError> {
        Ok(self.clone())
    }
}

impl SelfAccessors<User> for UserID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(users
            .filter(id.eq(self.0))
            .first::<User>(&mut pool.get()?)?)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::models::{GroupID, NewHubuumClass, Permissions, PermissionsList};
    use crate::tests::{create_test_group, create_test_user, setup_pool_and_tokens};
    use crate::traits::PermissionController;
    use crate::traits::{CanDelete, CanSave};
    use crate::{assert_contains, assert_not_contains};

    #[actix_rt::test]
    async fn test_user_permissions_namespace_and_class_listing() {
        use crate::models::namespace::NewNamespace;

        let (pool, _, _) = setup_pool_and_tokens().await;
        let test_user_1 = create_test_user(&pool).await;
        let test_group_1 = create_test_group(&pool).await;
        let test_user_2 = create_test_user(&pool).await;
        let test_group_2 = create_test_group(&pool).await;

        test_group_1.add_member(&pool, &test_user_1).await.unwrap();
        test_group_2.add_member(&pool, &test_user_2).await.unwrap();

        let ns = NewNamespace {
            name: "test_user_namespace_listing".to_string(),
            description: "Test namespace".to_string(),
        }
        .save_and_grant_all_to(&pool, GroupID(test_group_1.id))
        .await
        .unwrap();

        let class = NewHubuumClass {
            name: "test_user_namespace_listing".to_string(),
            description: "Test class".to_string(),
            json_schema: serde_json::json!({}),
            validate_schema: false,
            namespace_id: ns.id,
        }
        .save(&pool)
        .await
        .unwrap();

        class
            .grant(
                &pool,
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

        let nslist = test_user_1.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_not_contains!(&nslist, &ns);

        let classlist = test_user_1.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_not_contains!(&classlist, &class);

        ns.grant_one(&pool, test_group_2.id, Permissions::ReadCollection)
            .await
            .unwrap();

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        let classlist = test_user_1.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        class
            .grant_one(&pool, test_group_2.id, Permissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        class
            .revoke_one(&pool, test_group_2.id, Permissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_not_contains!(&classlist, &class);

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        ns.revoke_all(&pool, test_group_2.id).await.unwrap();

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_not_contains!(&nslist, &ns);

        test_user_1.delete(&pool).await.unwrap();
        test_user_2.delete(&pool).await.unwrap();
        test_group_1.delete(&pool).await.unwrap();
        test_group_2.delete(&pool).await.unwrap();
        ns.delete(&pool).await.unwrap();
    }
}
