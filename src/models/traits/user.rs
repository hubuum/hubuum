use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl, Table};

use crate::models::namespace::Namespace;
use crate::models::permissions::{ClassPermissions, NamespacePermissions};
use crate::traits::SelfAccessors;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::class::HubuumClass;
use crate::models::group::Group;
use crate::models::user::{User, UserID};

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

pub trait NamespaceAccessors: SelfAccessors<User> + GroupAccessors {
    /// Return all namespaces that the user has NamespacePermissions::ReadCollection on.
    async fn namespaces_read(&self, pool: &DbPool) -> Result<Vec<Namespace>, ApiError> {
        self.namespaces(pool, vec![NamespacePermissions::ReadCollection])
            .await
    }

    async fn namespaces(
        &self,
        pool: &DbPool,
        permissions: Vec<NamespacePermissions>,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::models::permissions::PermissionFilter;
        use crate::schema::namespacepermissions::dsl::{
            group_id, namespace_id, namespacepermissions,
        };
        use crate::schema::namespaces::dsl::{id as namespaces_table_id, namespaces};

        let mut conn = pool.get()?;

        let groups_id_subquery = self.group_ids_subquery();

        let mut base_query = namespacepermissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in permissions {
            base_query = PermissionFilter::filter(perm, base_query);
        }

        let result = base_query
            .inner_join(namespaces.on(namespace_id.eq(namespaces_table_id)))
            .select(namespaces::all_columns())
            .load::<Namespace>(&mut conn)?;

        Ok(result)
    }
}

pub trait ClassAccessors: NamespaceAccessors {
    /// Return all classes that the user has ClassPermissions::ReadClass on.
    async fn classes_read(&self, pool: &DbPool) -> Result<Vec<HubuumClass>, ApiError> {
        self.classes(pool, vec![ClassPermissions::ReadClass]).await
    }

    async fn classes(
        &self,
        pool: &DbPool,
        permissions: Vec<ClassPermissions>,
    ) -> Result<Vec<HubuumClass>, ApiError> {
        use crate::models::permissions::PermissionFilter;
        use crate::schema::classpermissions::dsl::*;
        use crate::schema::hubuumclass::dsl::{hubuumclass, namespace_id as hubuum_classes_nid};

        let mut conn = pool.get()?;
        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: Vec<i32> = self
            .namespaces_read(pool)
            .await?
            .iter()
            .map(|n| n.id)
            .collect();

        let mut base_query = classpermissions
            .into_boxed()
            .filter(namespace_id.eq_any(namespace_ids.clone()))
            .filter(group_id.eq_any(group_id_subquery));

        for perm in permissions {
            base_query = PermissionFilter::filter(perm, base_query);
        }

        let result = base_query
            .inner_join(hubuumclass.on(hubuum_classes_nid.eq_any(namespace_ids)))
            .select(hubuumclass::all_columns())
            .load::<HubuumClass>(&mut conn)?;

        Ok(result)
    }
}

impl GroupAccessors for User {}
impl GroupAccessors for UserID {}

impl NamespaceAccessors for User {}
impl NamespaceAccessors for UserID {}

impl ClassAccessors for User {}
impl ClassAccessors for UserID {}

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
    use crate::models::class::NewHubuumClass;
    use crate::models::group::GroupID;
    use crate::models::permissions::{ClassPermissions, NamespacePermissions, PermissionsList};
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
                    ClassPermissions::ReadClass,
                    ClassPermissions::UpdateClass,
                    ClassPermissions::DeleteClass,
                    ClassPermissions::CreateObject,
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

        ns.grant_one(&pool, test_group_2.id, NamespacePermissions::ReadCollection)
            .await
            .unwrap();

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        let classlist = test_user_1.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        class
            .grant_one(&pool, test_group_2.id, ClassPermissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        class
            .revoke_one(&pool, test_group_2.id, ClassPermissions::ReadClass)
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
