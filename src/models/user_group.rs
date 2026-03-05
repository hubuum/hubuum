use crate::db::traits::group::{
    DeleteUserGroupRecord, SaveUserGroupRecord, UserGroupGroupLookup, UserGroupUserLookup,
};
use crate::models::group::Group;
use crate::models::user::User;

use crate::errors::ApiError;
use crate::schema::user_groups;
use crate::traits::BackendContext;

use crate::db::DbPool;

use crate::traits::crud::SaveAdapter;
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

impl SaveAdapter for NewUserGroup {
    type Output = UserGroup;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        self.save_user_group_record(pool).await
    }
}

impl UserGroup {
    pub async fn user<C>(&self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_user_group_user(backend.db_pool()).await
    }

    pub async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_user_group_group(backend.db_pool()).await
    }

    pub async fn save<C>(&self, backend: &C) -> Result<UserGroup, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_user_group_record(backend.db_pool()).await
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_user_group_record(backend.db_pool()).await
    }
}
