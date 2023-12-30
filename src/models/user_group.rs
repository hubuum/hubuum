use crate::schema::user_groups;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = user_groups)]
pub struct UserGroup {
    pub user_id: i32,
    pub group_id: i32,
}
