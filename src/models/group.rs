// src/models/group.rs

use crate::errors::ApiError;
use crate::models::user_group::UserGroup;
use crate::schema::groups;

use crate::models::user::User;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::traits::SelfAccessors;

use crate::db::DbPool;

#[derive(Serialize, Deserialize)]
pub struct GroupID(pub i32);

impl SelfAccessors<Group> for GroupID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<Group, ApiError> {
        self.group(pool).await
    }
}

impl GroupID {
    pub async fn group(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::*;
        Ok(groups
            .filter(id.eq(self.0))
            .first::<Group>(&mut pool.get()?)?)
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::groups::dsl::*;
        Ok(diesel::delete(groups.filter(id.eq(self.0))).execute(&mut pool.get()?)?)
    }
}

#[derive(Serialize, Deserialize, Queryable, Insertable, PartialEq, Debug, Clone)]
#[diesel(table_name = groups)]
pub struct Group {
    pub id: i32,
    pub groupname: String,
    pub description: String,
}

impl SelfAccessors<Group> for Group {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<Group, ApiError> {
        Ok(self.clone())
    }
}

impl Group {
    pub async fn members(&self, pool: &DbPool) -> Result<Vec<User>, ApiError> {
        use crate::schema::user_groups::dsl::*;
        use crate::schema::users::dsl::*;

        Ok(user_groups
            .filter(group_id.eq(self.id))
            .inner_join(users.on(id.eq(user_id)))
            .select((id, username, password, email))
            .load::<User>(&mut pool.get()?)?)
    }

    /// Add a member to a group. If the user is already a member, do nothing.
    ///
    /// ## Arguments
    /// * `user` - The user to add to the group
    /// * `pool` - The database connection pool
    ///
    /// ## Returns
    /// * `Ok(())` if the user was added to the group
    /// * `Err(ApiError)` if the user was not added to the group
    ///
    /// If the user is already a member of the group, this function is a safe noop.
    pub async fn add_member(&self, pool: &DbPool, user: &User) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;

        let new_user_group = UserGroup {
            user_id: user.id,
            group_id: self.id,
        };

        diesel::insert_into(user_groups)
            .values(&new_user_group)
            .on_conflict_do_nothing()
            .execute(&mut pool.get()?)?;

        Ok(())
    }

    pub async fn remove_member(&self, user: &User, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;

        diesel::delete(user_groups.filter(user_id.eq(user.id))).execute(&mut pool.get()?)?;
        Ok(())
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::groups::dsl::*;
        Ok(diesel::delete(groups.filter(id.eq(self.id))).execute(&mut pool.get()?)?)
    }
}

#[derive(Deserialize, Serialize, Insertable, Debug)]
#[diesel(table_name = groups)]
pub struct NewGroup {
    pub groupname: String,
    pub description: Option<String>,
}

impl NewGroup {
    pub async fn new(groupname: &str, description: Option<&str>) -> Self {
        NewGroup {
            groupname: groupname.to_string(),
            description: description.map(|s| s.to_string()),
        }
    }

    pub async fn save(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::*;
        Ok(diesel::insert_into(groups)
            .values(self)
            .get_result::<Group>(&mut pool.get()?)?)
    }
}

#[derive(Deserialize, Serialize, AsChangeset)]
#[diesel(table_name = groups)]
pub struct UpdateGroup {
    pub groupname: Option<String>,
}

impl UpdateGroup {
    pub async fn save(&self, group_id: i32, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::*;
        Ok(diesel::update(groups.filter(id.eq(group_id)))
            .set(self)
            .get_result::<Group>(&mut pool.get()?)?)
    }
}
