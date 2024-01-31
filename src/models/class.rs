use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::schema::hubuumclass;

use crate::models::permissions::ClassPermissions;
use crate::models::user::UserID;

use super::namespace::Namespace;

#[derive(Serialize, Deserialize, Queryable, Insertable, Clone)]
#[diesel(table_name = hubuumclass )]
pub struct HubuumClass {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub json_schema: serde_json::Value,
    pub validate_schema: bool,
    pub description: String,
}

pub struct HubuumClassID(pub i32);

pub trait ClassGenerics {
    fn id(&self) -> i32;
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;

    /// Check if the user has the given permission on this class.
    ///
    /// If this is called on a HubuumClassID, a full HubuumClass is created to extract
    /// the namespace_id. To avoid creating the HubuumClass multiple times during use
    /// do this:
    /// ```
    /// class = class_id.class(pool).await?;
    /// if (class.user_can(pool, userid, ClassPermissions::ReadClass).await?) {
    ///     return Ok(class);
    /// }
    /// ```
    /// And not this:
    /// ```
    /// if (class_id.user_can(pool, userid, ClassPermissions::ReadClass).await?) {
    ///    return Ok(class_id.class(pool).await?);
    /// }
    /// ```
    ///
    /// ## Arguments
    ///
    /// * `pool` - The database pool to use for the query.
    /// * `user_id` - The user to check permissions for.
    /// * `permission` - The permission to check.
    ///
    /// ## Returns
    ///
    /// * `Ok(true)` if the user has the given permission on this class.
    /// * `Ok(false)` if the user does not have the given permission on this class.
    /// * `Err(_)` if the user does not have the given permission on this class, or if the
    ///  permission is invalid.
    ///
    /// ## Example
    ///
    /// ```
    /// if (hubuum_class_or_classid.user_can(pool, userid, ClassPermissions::ReadClass).await?) {
    ///     // Do something
    /// }
    async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission: ClassPermissions,
    ) -> Result<bool, ApiError> {
        use crate::models::permissions::ClassPermission;
        use crate::models::permissions::PermissionFilter;
        use crate::schema::classpermissions::dsl::*;

        let mut conn = pool.get()?;
        let group_id_subquery = user_id.group_ids_subquery();

        // Note that self.namespace_id(pool).await? is only a query if the caller is a HubuumClassID, otherwise
        // it's a simple field access (which ignores the passed pool).
        let base_query = classpermissions
            .into_boxed()
            .filter(namespace_id.eq(self.namespace_id(pool).await?))
            .filter(group_id.eq_any(group_id_subquery));

        let result = PermissionFilter::filter(permission, base_query)
            .first::<ClassPermission>(&mut conn)
            .optional()?;

        Ok(result.is_some())
    }
}

impl ClassGenerics for HubuumClass {
    fn id(&self) -> i32 {
        self.id
    }

    async fn class(&self, _pool: &DbPool) -> Result<HubuumClass, ApiError> {
        Ok(self.clone())
    }

    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let mut conn = pool.get()?;
        let namespace = namespaces
            .filter(id.eq(self.namespace_id))
            .first::<Namespace>(&mut conn)?;

        Ok(namespace)
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace_id)
    }
}

impl ClassGenerics for HubuumClassID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.0))
            .first::<HubuumClass>(&mut conn)?;

        Ok(class)
    }

    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.0))
            .first::<HubuumClass>(&mut conn)?;

        class.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

pub async fn total_class_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumclass::dsl::*;

    let mut conn = pool.get()?;
    let count = hubuumclass.count().get_result::<i64>(&mut conn)?;

    Ok(count)
}
