use crate::db::traits::user::{
    CreateUserRecord, DeleteUserRecord, OwnedUserTokenRecord, StoreUserTokenRecord,
    UpdateUserRecord,
};
use crate::models::group::Group;
use crate::models::token::{Token, UserToken};
use crate::models::user_group::UserGroup;
use crate::schema::users;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::{with_connection, DbPool};

use crate::errors::ApiError;

use tracing::{error, warn};

#[derive(Serialize, Deserialize, Queryable, Insertable, PartialEq, Debug, Clone, ToSchema)]
#[diesel(table_name = users)]
pub struct User {
    pub id: i32,
    pub username: String,
    pub password: String,
    pub email: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl User {
    pub async fn create_token(&self, pool: &DbPool) -> Result<Token, ApiError> {
        let generated_token = crate::utilities::auth::generate_token();

        self.store_user_token_record(pool, &generated_token).await?;

        Ok(generated_token)
    }

    pub async fn token_is_mine(
        &self,
        token_param: Token,
        pool: &DbPool,
    ) -> Result<UserToken, ApiError> {
        self.load_owned_user_token_record(&token_param, pool).await
    }

    pub async fn delete_token(&self, token_param: Token, pool: &DbPool) -> Result<usize, ApiError> {
        self.delete_owned_user_token_record(&token_param, pool).await
    }

    pub async fn delete_all_tokens(&self, pool: &DbPool) -> Result<usize, ApiError> {
        self.delete_all_user_tokens_record(pool).await
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        self.delete_user_record(pool).await
    }
}

/// Struct to update a user.
///
/// The password, if present, is expected to be hashed
/// before being passed to the database.
#[derive(AsChangeset, Deserialize, Serialize, Clone, ToSchema)]
#[schema(example = update_user_example)]
#[diesel(table_name = users)]
pub struct UpdateUser {
    pub username: Option<String>,
    pub password: Option<String>,
    pub email: Option<String>,
}

impl UpdateUser {
    pub fn hash_password(self) -> Result<Self, ApiError> {
        if let Some(ref pass) = self.password {
            match crate::utilities::auth::hash_password(pass) {
                Ok(hashed_password) => {
                    return Ok(UpdateUser {
                        password: Some(hashed_password),
                        ..self
                    });
                }
                Err(e) => return Err(ApiError::HashError(format!("Failed to hash password: {e}"))),
            }
        }
        Ok(self)
    }

    pub async fn save(self, user_id: i32, pool: &DbPool) -> Result<User, ApiError> {
        let hashed = self.hash_password()?;
        hashed.update_user_record(user_id, pool).await
    }
}

/// Struct to create a new user.
///
/// The password is expected to be hashed
/// before being passed to the database.
#[derive(Serialize, Deserialize, Insertable, Debug, ToSchema)]
#[schema(example = new_user_example)]
#[diesel(table_name = users)]
pub struct NewUser {
    pub username: String,
    pub password: String,
    pub email: Option<String>,
}

impl NewUser {
    pub async fn new(username: &str, password: &str, email: Option<&str>) -> Self {
        let email = email.map(|e| e.to_string());
        NewUser {
            username: username.to_string(),
            password: password.to_string(),
            email,
        }
    }

    pub async fn save(self, pool: &DbPool) -> Result<User, ApiError> {
        let hashed = self.hash_password()?;
        hashed.create_user_record(pool).await
    }

    pub fn hash_password(mut self) -> Result<Self, ApiError> {
        if !self.password.starts_with("$argon2") {
            match crate::utilities::auth::hash_password(&self.password) {
                Ok(hashed_password) => {
                    self.password = hashed_password;
                }
                Err(e) => return Err(ApiError::HashError(format!("Failed to hash password: {e}"))),
            }
        }
        Ok(self)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UserID(pub i32);

impl UserID {
    pub async fn user(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::db::traits::user::LoadUserRecord;
        self.load_user_record(pool).await
    }

    /*
    pub async fn group_ids(&self, pool: &DbPool) -> Result<Vec<i32>, ApiError> {
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool.get()?;
        let result = user_groups
            .filter(user_id.eq(self.0))
            .select(group_id)
            .load::<i32>(&mut conn)?;

        Ok(result)
    }
    */

    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        self.delete_user_record(pool).await
    }
}

/// Struct to log in a user.
///
/// The password is expected to be plaintext.
#[derive(AsChangeset, Deserialize, Serialize, ToSchema)]
#[schema(example = login_user_example)]
#[diesel(table_name = users)]
pub struct LoginUser {
    pub username: String,
    pub password: String,
}

impl LoginUser {
    /// Check if the user exists and the plaintext password in the struct
    /// matches the hashed password in the database.
    pub async fn login(self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;

        // We could do .first::<User>(&mut conn)? here, due to the way errors.rs uses "From"
        // to map diesel errors. But, we specifically map Diesel's NotFound to our own NotFound
        // which would lead to a 404 instead of a 401, leaking information about the existence
        // of the user.
        let user = match with_connection(pool, |conn| {
            users
                .filter(username.eq(&self.username))
                .first::<User>(conn)
        }) {
            Ok(user) => user,
            Err(_) => {
                warn!(
                    message = "Login failed (user not found)",
                    user = self.username,
                );

                return Err(auth_failure());
            }
        };

        let plaintext_password = &self.password;
        let hashed_password = &user.password;

        match crate::utilities::auth::verify_password(plaintext_password, hashed_password) {
            Ok(true) => Ok(user),
            Ok(false) => {
                warn!(
                    message = "Login failed (password mismatch)",
                    user = self.username,
                );

                Err(auth_failure())
            }

            Err(e) => {
                error!(
                    message = "Login failed (hashing error)",
                    user = self.username,
                    hash = user.password,
                    error = e.to_string()
                );
                Err(auth_failure())
            }
        }
    }
}

pub fn auth_failure() -> ApiError {
    ApiError::Unauthorized("Authentication failure".to_string())
}

#[allow(dead_code)]
fn update_user_example() -> UpdateUser {
    UpdateUser {
        username: Some("alice".to_string()),
        password: Some("new-password".to_string()),
        email: Some("alice@example.com".to_string()),
    }
}

#[allow(dead_code)]
fn new_user_example() -> NewUser {
    NewUser {
        username: "alice".to_string(),
        password: "correct-horse-battery-staple".to_string(),
        email: Some("alice@example.com".to_string()),
    }
}

#[allow(dead_code)]
fn login_user_example() -> LoginUser {
    LoginUser {
        username: "alice".to_string(),
        password: "correct-horse-battery-staple".to_string(),
    }
}
