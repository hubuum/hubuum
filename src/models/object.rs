use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::schema::hubuumobject;

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
