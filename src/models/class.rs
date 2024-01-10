use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use serde_json::Value as JsonValue;

use crate::db::connection::DbPool;
use crate::errors::ApiError;
use crate::schema::hubuumclass;

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

pub async fn total_class_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumclass::dsl::*;

    let mut conn = pool.get()?;
    let count = hubuumclass.count().get_result::<i64>(&mut conn)?;

    Ok(count)
}
