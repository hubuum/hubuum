// src/models/group.rs

use crate::schema::groups;
use crate::schema::user_groups;

use crate::models::user::User;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::connection::DbPool;

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = groups)]
pub struct Group {
    pub id: i32,
    pub groupname: String,
    pub description: String,
}

impl Group {
    pub fn users(&self, pool: DbPool) -> QueryResult<Vec<User>> {
        use crate::schema::user_groups::dsl::*;
        use crate::schema::users::dsl::*;

        let mut conn = pool.get().expect("couldn't get db connection from pool");

        user_groups
            .filter(group_id.eq(self.id))
            .inner_join(users.on(id.eq(user_id)))
            .select((id, username, password, email))
            .load::<User>(&mut conn)
    }
}

#[derive(Deserialize, Serialize, Insertable)]
#[diesel(table_name = groups)]
pub struct NewGroup {
    pub groupname: String,
    pub description: String,
}

#[derive(Deserialize, Serialize, AsChangeset)]
#[diesel(table_name = groups)]
pub struct UpdateGroup {
    pub groupname: Option<String>,
}

#[derive(Serialize, Deserialize, Queryable, Insertable, Associations)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Group))]
#[diesel(table_name = user_groups)]
pub struct UserGroup {
    pub user_id: i32,
    pub group_id: i32,
}
