use crate::models::user::User;
use crate::{models::group::Group, traits::CanSave};

use crate::errors::ApiError;
use crate::schema::user_groups;

use crate::db::DbPool;

use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Queryable, Insertable, Associations)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Group))]
#[diesel(table_name = user_groups)]
pub struct UserGroup {
    pub user_id: i32,
    pub group_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Deserialize, Insertable)]
#[diesel(table_name = user_groups)]
pub struct NewUserGroup {
    pub user_id: i32,
    pub group_id: i32,
}

impl CanSave for NewUserGroup {
    type Output = UserGroup;
    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        use crate::schema::user_groups::dsl::*;
        Ok(diesel::insert_into(user_groups)
            .values(self)
            .get_result(&mut pool.get()?)?)
    }
}

impl UserGroup {
    pub async fn user(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(users
            .filter(id.eq(self.user_id))
            .first::<User>(&mut pool.get()?)?)
    }

    pub async fn group(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::*;
        Ok(groups
            .filter(id.eq(self.group_id))
            .first::<Group>(&mut pool.get()?)?)
    }

    pub async fn save(&self, pool: &DbPool) -> Result<UserGroup, ApiError> {
        use crate::schema::user_groups::dsl::*;
        Ok(diesel::insert_into(user_groups)
            .values(self)
            .get_result(&mut pool.get()?)?)
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;
        diesel::delete(
            user_groups
                .filter(user_id.eq(self.user_id))
                .filter(group_id.eq(self.group_id)),
        )
        .execute(&mut pool.get()?)?;

        Ok(())
    }
}
