use crate::db::traits::user::{
    CreateUserRecord, DeleteUserRecord, OwnedUserTokenRecord, StoreUserTokenRecord,
    UpdateUserRecord,
};
use crate::models::token::{Token, UserToken};
use crate::schema::users;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::traits::BackendContext;

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
    pub async fn create_token<C>(&self, backend: &C) -> Result<Token, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let generated_token = crate::utilities::auth::generate_token();

        self.store_user_token_record(backend.db_pool(), &generated_token)
            .await?;

        Ok(generated_token)
    }

    pub async fn token_is_mine<C>(
        &self,
        token_param: Token,
        backend: &C,
    ) -> Result<UserToken, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_owned_user_token_record(&token_param, backend.db_pool())
            .await
    }

    pub async fn delete_token<C>(&self, token_param: Token, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_owned_user_token_record(&token_param, backend.db_pool())
            .await
    }

    pub async fn delete_all_tokens<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_all_user_tokens_record(backend.db_pool()).await
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_user_record(backend.db_pool()).await
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

    pub async fn save<C>(self, user_id: i32, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password()?;
        hashed.update_user_record(user_id, backend.db_pool()).await
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

    pub async fn save<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password()?;
        hashed.create_user_record(backend.db_pool()).await
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
    pub async fn user<C>(&self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        use crate::db::traits::user::LoadUserRecord;
        self.load_user_record(backend.db_pool()).await
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_user_record(backend.db_pool()).await
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
    pub async fn login<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        // We could do .first::<User>(&mut conn)? here, due to the way errors.rs uses "From"
        // to map diesel errors. But, we specifically map Diesel's NotFound to our own NotFound
        // which would lead to a 404 instead of a 401, leaking information about the existence
        // of the user.
        let user = match User::get_by_username(backend.db_pool(), &self.username).await {
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
