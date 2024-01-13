use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer};
use diesel::QueryableByName;

use serde::{Deserialize, Serialize};

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::schema::hubuumobject;

#[derive(QueryableByName, Debug, Serialize, Deserialize)]
pub struct ObjectsByClass {
    #[diesel(sql_type = Integer)]
    pub hubuum_class_id: i32,
    #[diesel(sql_type = BigInt)]
    pub count: i64,
}

use serde_json::Value as JsonValue;
#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = hubuumobject)]
pub struct HubuumObject {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub hubuum_class_id: i32,
    pub data: Option<JsonValue>,
    pub description: String,
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
