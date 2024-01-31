use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use serde_json::Value as JsonValue;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::schema::hubuumclass;

use crate::models::permissions::ClassPermissions;
use crate::models::user::UserID;

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = hubuumclass )]
pub struct HubuumClass {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub json_schema: Option<JsonValue>,
    pub validate_schema: bool,
    pub description: String,
}

impl HubuumClass {
    pub async fn user_can(
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

        let base_query = classpermissions
            .into_boxed()
            .filter(namespace_id.eq(self.namespace_id))
            .filter(group_id.eq_any(group_id_subquery));

        let result = PermissionFilter::filter(permission, base_query)
            .first::<ClassPermission>(&mut conn)
            .optional()?;

        Ok(result.is_some())
    }
}

pub async fn total_class_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumclass::dsl::*;

    let mut conn = pool.get()?;
    let count = hubuumclass.count().get_result::<i64>(&mut conn)?;

    Ok(count)
}
