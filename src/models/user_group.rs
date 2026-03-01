use crate::db::traits::group::{
    DeleteUserGroupRecord, SaveUserGroupRecord, UserGroupGroupLookup, UserGroupUserLookup,
};
use crate::models::user::User;
use crate::{models::group::Group, traits::CanSave};

use crate::errors::ApiError;
use crate::schema::user_groups;

use crate::db::DbPool;

use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use crate::traits::crud::SaveAdapter;

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

impl SaveAdapter for NewUserGroup {
    type Output = UserGroup;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        self.save_user_group_record(pool).await
    }
}

impl UserGroup {
    pub async fn user(&self, pool: &DbPool) -> Result<User, ApiError> {
        self.load_user_group_user(pool).await
    }

    pub async fn group(&self, pool: &DbPool) -> Result<Group, ApiError> {
        self.load_user_group_group(pool).await
    }

    pub async fn save(&self, pool: &DbPool) -> Result<UserGroup, ApiError> {
        self.save_user_group_record(pool).await
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_user_group_record(pool).await
    }
}
