// src/models/group.rs

use crate::errors::ApiError;
use crate::schema::groups;
use crate::schema::user_groups;

use crate::models::user::User;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::connection::DbPool;

pub type GroupID = i32;

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = groups)]
pub struct Group {
    pub id: i32,
    pub groupname: String,
    pub description: String,
}

impl Group {
    pub fn users(&self, pool: &DbPool) -> Result<Vec<User>, ApiError> {
        use crate::schema::user_groups::dsl::*;
        use crate::schema::users::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        user_groups
            .filter(group_id.eq(self.id))
            .inner_join(users.on(id.eq(user_id)))
            .select((id, username, password, email))
            .load::<User>(&mut conn)
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))
    }
}

#[derive(Deserialize, Serialize, Insertable, Debug)]
#[diesel(table_name = groups)]
pub struct NewGroup {
    pub groupname: String,
    pub description: Option<String>,
}

impl NewGroup {
    pub fn new(groupname: &str, description: Option<&str>) -> Self {
        NewGroup {
            groupname: groupname.to_string(),
            description: description.map(|s| s.to_string()),
        }
    }

    pub fn save(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        diesel::insert_into(groups)
            .values(self)
            .get_result::<Group>(&mut conn)
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))
    }
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
