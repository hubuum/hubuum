use chrono::NaiveDateTime;

use crate::schema::tokens;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = tokens)]
pub struct Token {
    pub token: String,
    pub user_id: i32,
    pub expires: NaiveDateTime,
}
