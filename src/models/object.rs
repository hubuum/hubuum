use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::schema::hubuumobject;

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = hubuumobject)]
pub struct HubuumObject {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub hubuum_class_id: i32,
    pub description: String,
}
