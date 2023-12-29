use crate::models::group::Group;
use crate::schema::users;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::connection::DbPool;

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = users)]
pub struct User {
    pub id: i32,
    pub username: String,
    pub password: String,
    pub email: Option<String>,
}

impl User {
    pub fn groups(&self, pool: DbPool) -> QueryResult<Vec<Group>> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool.get().expect("couldn't get db connection from pool");

        user_groups
            .filter(user_id.eq(self.id))
            .inner_join(groups.on(id.eq(group_id)))
            .select((id, groupname, description))
            .load::<Group>(&mut conn)
    }
}

#[derive(AsChangeset, Deserialize, Serialize)]
#[diesel(table_name = users)]
pub struct UpdateUser {
    pub username: Option<String>,
    pub password: Option<String>,
    pub email: Option<String>,
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = users)]
pub struct NewUser {
    pub username: String,
    pub password: String,
    pub email: Option<String>,
}

#[derive(AsChangeset, Deserialize, Serialize)]
#[diesel(table_name = users)]
pub struct LoginUser {
    pub username: String,
    pub password: String,
}
