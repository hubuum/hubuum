use crate::models::group::Group;
use crate::models::user_group::UserGroup;
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
    pub fn groups(&self, pool: &DbPool) -> QueryResult<Vec<Group>> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool.get().expect("couldn't get db connection from pool");

        user_groups
            .filter(user_id.eq(self.id))
            .inner_join(groups.on(id.eq(group_id)))
            .select((id, groupname, description))
            .load::<Group>(&mut conn)
    }

    pub fn is_in_group_by_name(&self, groupname_queried: &str, pool: &DbPool) -> bool {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool.get().expect("couldn't get db connection from pool");

        let result = user_groups
            .filter(user_id.eq(self.id))
            .inner_join(groups.on(id.eq(group_id)))
            .filter(groupname.eq(groupname_queried)) // Clarify the field and variable
            .first::<(UserGroup, Group)>(&mut conn); // Change the expected type

        match result {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub fn is_in_group(&self, group_id_queried: i32, pool: &DbPool) -> bool {
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool.get().expect("couldn't get db connection from pool");

        let result = user_groups
            .filter(user_id.eq(self.id))
            .filter(group_id.eq(group_id_queried))
            .first::<crate::models::user_group::UserGroup>(&mut conn);

        match result {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub fn is_admin(&self, pool: &DbPool) -> bool {
        return self.is_in_group_by_name("admin", pool);
    }
}

#[derive(AsChangeset, Deserialize, Serialize)]
#[diesel(table_name = users)]
pub struct UpdateUser {
    pub username: Option<String>,
    pub password: Option<String>,
    pub email: Option<String>,
}

#[derive(Serialize, Deserialize, Insertable, Debug)]
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
