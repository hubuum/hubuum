use crate::models::group::Group;
use crate::models::user::User;

use crate::schema::user_groups;

use crate::errors::map_error;
use crate::errors::ApiError;

use crate::db::connection::DbPool;

use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Queryable, Insertable, Associations)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Group))]
#[diesel(table_name = user_groups)]
pub struct UserGroup {
    pub user_id: i32,
    pub group_id: i32,
}

impl UserGroup {
    pub fn new(user: &User, group: &Group) -> Self {
        UserGroup {
            user_id: user.id,
            group_id: group.id,
        }
    }

    pub fn user(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        users
            .filter(id.eq(self.user_id))
            .first::<User>(&mut conn)
            .map_err(|e| map_error(e, "User not found"))
    }

    pub fn group(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        groups
            .filter(id.eq(self.group_id))
            .first::<Group>(&mut conn)
            .map_err(|e| map_error(e, "Group not found"))
    }

    pub fn save(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        diesel::insert_into(user_groups)
            .values(self)
            .execute(&mut conn)
            .map_err(|e| map_error(e, "Failed to save user user_group"))?;

        Ok(())
    }

    pub fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        diesel::delete(
            user_groups
                .filter(user_id.eq(self.user_id))
                .filter(group_id.eq(self.group_id)),
        )
        .execute(&mut conn)
        .map_err(|e| map_error(e, "Failed to delete user_group"))?;

        Ok(())
    }
}
