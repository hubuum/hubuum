use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer};
use serde::{Deserialize, Serialize};

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::schema::hubuumobject;

use crate::models::class::HubuumClass;
use crate::models::namespace::Namespace;
use crate::models::permissions::ObjectPermissions;
use crate::models::user::UserID;

#[derive(QueryableByName, Debug, Serialize, Deserialize)]
pub struct ObjectsByClass {
    #[diesel(sql_type = Integer)]
    pub hubuum_class_id: i32,
    #[diesel(sql_type = BigInt)]
    pub count: i64,
}

#[derive(Serialize, Deserialize, Queryable, Insertable, Clone)]
#[diesel(table_name = hubuumobject )]
pub struct HubuumObject {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub hubuum_class_id: i32,
    pub data: serde_json::Value,
    pub description: String,
}

pub struct HubuumObjectID(pub i32);

pub trait ObjectGenerics {
    fn id(&self) -> i32;
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
    async fn object(&self, pool: &DbPool) -> Result<HubuumObject, ApiError>;

    /// Check if the user has the given permission on this object.
    ///
    /// If this is called on a HubuumObjectID, a full HubuumObject is created to extract
    /// the namespace_id. To avoid creating the HubuumObject multiple times during use
    /// do this:
    /// ```
    /// obj = obj_id.Object(pool).await?;
    /// if (obj.user_can(pool, userid, ObjectPermissions::ReadObject).await?) {
    ///     return Ok(obj);
    /// }
    /// ```
    /// And not this:
    /// ```
    /// if (obj_id.user_can(pool, userid, ObjectPermissions::ReadObject).await?) {
    ///    return Ok(obj_id.Object(pool).await?);
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
    /// * `Ok(true)` if the user has the given permission on this object.
    /// * `Ok(false)` if the user does not have the given permission on this object.
    /// * `Err(_)` if the user does not have the given permission on this object, or if the
    ///  permission is invalid.
    ///
    /// ## Example
    ///
    /// ```
    /// if (hubuum_object_or_objectid.user_can(pool, userid, ObjectPermissions::ReadObject).await?) {
    ///     // Do something
    /// }
    async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission: ObjectPermissions,
    ) -> Result<bool, ApiError> {
        use crate::models::permissions::ObjectPermission;
        use crate::models::permissions::PermissionFilter;
        use crate::schema::objectpermissions::dsl::*;

        let mut conn = pool.get()?;
        let group_id_subquery = user_id.group_ids_subquery();

        // Note that self.namespace_id(pool).await? is only a query if the caller is a HubuumObjectID, otherwise
        // it's a simple field access (which ignores the passed pool).
        let base_query = objectpermissions
            .into_boxed()
            .filter(namespace_id.eq(self.namespace_id(pool).await?))
            .filter(group_id.eq_any(group_id_subquery));

        let result = PermissionFilter::filter(permission, base_query)
            .first::<ObjectPermission>(&mut conn)
            .optional()?;

        Ok(result.is_some())
    }
}

impl ObjectGenerics for HubuumObject {
    fn id(&self) -> i32 {
        self.id
    }

    async fn object(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
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

    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.hubuum_class_id))
            .first::<HubuumClass>(&mut conn)?;

        Ok(class)
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.hubuum_class_id)
    }
}

impl ObjectGenerics for HubuumObjectID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn object(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        Ok(object)
    }

    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }

    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.class(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.class(pool).await?.id)
    }
}

pub async fn total_object_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumobject::dsl::*;

    let mut conn = pool.get()?;
    let count = hubuumobject.count().get_result::<i64>(&mut conn)?;

    Ok(count)
}

pub async fn objects_per_class_count(pool: &DbPool) -> Result<Vec<ObjectsByClass>, ApiError> {
    use diesel::sql_query;

    let mut conn = pool.get()?;

    let raw_query =
        "SELECT hubuum_class_id, COUNT(*) as count FROM hubuumobject GROUP BY hubuum_class_id";
    let results = sql_query(raw_query).load::<ObjectsByClass>(&mut conn)?;

    Ok(results)
}
