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

/// Trait to hash a password.
pub trait PasswordHashable {
    fn hash_password(&mut self) -> Result<(), String>;
}

/// Struct to update a user.
///
/// The password, if present, is expected to be hashed
/// before being passed to the database.
#[derive(AsChangeset, Deserialize, Serialize)]
#[diesel(table_name = users)]
pub struct UpdateUser {
    pub username: Option<String>,
    pub password: Option<String>,
    pub email: Option<String>,
}

impl PasswordHashable for UpdateUser {
    fn hash_password(&mut self) -> Result<(), String> {
        if let Some(ref password) = self.password {
            match crate::utilities::auth::hash_password(password) {
                Ok(hashed_password) => {
                    self.password = Some(hashed_password);
                    Ok(())
                }
                Err(e) => Err(format!("Failed to hash password: {}", e)),
            }
        } else {
            Ok(())
        }
    }
}
/// Struct to create a new user.
///
/// The password is expected to be hashed
/// before being passed to the database.
#[derive(Serialize, Deserialize, Insertable, Debug)]
#[diesel(table_name = users)]
pub struct NewUser {
    pub username: String,
    pub password: String,
    pub email: Option<String>,
}

impl PasswordHashable for NewUser {
    fn hash_password(&mut self) -> Result<(), String> {
        match crate::utilities::auth::hash_password(&self.password) {
            Ok(hashed_password) => {
                self.password = hashed_password;
                Ok(())
            }
            Err(e) => Err(format!("Failed to hash password: {}", e)),
        }
    }
}
/// Struct to log in a user.
///
/// The password is expected to be plaintext.
#[derive(AsChangeset, Deserialize, Serialize)]
#[diesel(table_name = users)]
pub struct LoginUser {
    pub username: String,
    pub password: String,
}
