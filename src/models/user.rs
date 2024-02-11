use crate::models::group::Group;
use crate::models::token::{Token, UserToken};
use crate::models::user_group::UserGroup;
use crate::schema::users;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::DbPool;

use crate::errors::ApiError;

use tracing::{error, warn};

#[derive(Serialize, Deserialize, Queryable, Insertable, PartialEq, Debug)]
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
        use crate::schema::tokens::dsl::*;
        let generated_token = crate::utilities::auth::generate_token();

        Ok(diesel::insert_into(crate::schema::tokens::table)
            .values((
                user_id.eq(self.id),
                token.eq(&generated_token.get_token()),
                issued.eq(chrono::Utc::now().naive_utc()),
            ))
            .execute(&mut pool.get()?)
            .map(|_| generated_token)?)
    }

    pub async fn get_tokens(&self, pool: &DbPool) -> Result<Vec<UserToken>, ApiError> {
        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        crate::models::token::valid_tokens_for_user(&mut conn, self.id).await
    }

    pub async fn token_is_mine(
        &self,
        token_param: Token,
        pool: &DbPool,
    ) -> Result<UserToken, ApiError> {
        use crate::schema::tokens::dsl::*;

        let mut conn = pool.get()?;

        let result = tokens
            .filter(user_id.eq(self.id))
            .filter(token.eq(token_param.get_token()))
            .first::<crate::models::token::UserToken>(&mut conn)?;

        Ok(result)
    }

    pub async fn delete_token(&self, token_param: Token, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::*;
        Ok(diesel::delete(tokens.filter(user_id.eq(self.id)))
            .filter(token.eq(token_param.get_token()))
            .execute(&mut pool.get()?)?)
    }

    pub async fn delete_all_tokens(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::*;
        Ok(diesel::delete(tokens.filter(user_id.eq(self.id))).execute(&mut pool.get()?)?)
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(diesel::delete(users.filter(id.eq(self.id))).execute(&mut pool.get()?)?)
    }

    pub async fn groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

        Ok(user_groups
            .filter(user_id.eq(self.id))
            .inner_join(groups.on(id.eq(group_id)))
            .select((id, groupname, description, created_at, updated_at))
            .load::<Group>(&mut pool.get()?)?)
    }

    pub async fn is_in_group_by_name(&self, groupname_queried: &str, pool: &DbPool) -> bool {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::*;

        match pool.get() {
            Ok(mut conn) => user_groups
                .filter(user_id.eq(self.id))
                .inner_join(groups.on(id.eq(group_id)))
                .filter(groupname.eq(groupname_queried)) // Clarify the field and variable
                .first::<(UserGroup, Group)>(&mut conn) // Change the expected type
                .is_ok(),
            Err(e) => {
                error!(
                    message = "Failed to get db connection from pool",
                    error = e.to_string()
                );
                false
            }
        }
    }

    pub async fn is_in_group(&self, group_id_queried: i32, pool: &DbPool) -> bool {
        use crate::schema::user_groups::dsl::*;

        match pool.get() {
            Ok(mut conn) => user_groups
                .filter(user_id.eq(self.id))
                .filter(group_id.eq(group_id_queried))
                .first::<crate::models::user_group::UserGroup>(&mut conn)
                .is_ok(),
            Err(e) => {
                error!(
                    message = "Failed to get db connection from pool",
                    error = e.to_string()
                );
                false
            }
        }
    }

    pub async fn is_admin(&self, pool: &DbPool) -> bool {
        self.is_in_group_by_name("admin", pool).await
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
#[derive(AsChangeset, Deserialize, Serialize, Clone)]
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
                Err(e) => {
                    return Err(ApiError::HashError(format!(
                        "Failed to hash password: {}",
                        e
                    )))
                }
            }
        }
        Ok(self)
    }

    pub async fn save(self, user_id: i32, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(diesel::update(users.filter(id.eq(user_id)))
            .set(self.hash_password()?)
            .get_result::<User>(&mut pool.get()?)?)
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

impl NewUser {
    pub async fn new(username: &str, password: &str, email: Option<&str>) -> Self {
        NewUser {
            username: username.to_string(),
            password: password.to_string(),
            email: email.map(|s| s.to_string()),
        }
    }

    pub async fn save(self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(diesel::insert_into(users)
            .values(&self.hash_password()?)
            .get_result::<User>(&mut pool.get()?)?)
    }

    pub fn hash_password(mut self) -> Result<Self, ApiError> {
        if !self.password.starts_with("$argon2") {
            match crate::utilities::auth::hash_password(&self.password) {
                Ok(hashed_password) => {
                    self.password = hashed_password;
                }
                Err(e) => {
                    return Err(ApiError::HashError(format!(
                        "Failed to hash password: {}",
                        e
                    )))
                }
            }
        }
        Ok(self)
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserID(pub i32);

impl UserID {
    pub async fn user(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(users
            .filter(id.eq(self.0))
            .first::<User>(&mut pool.get()?)?)
    }

    pub async fn group_ids(&self, pool: &DbPool) -> Result<Vec<i32>, ApiError> {
        use crate::schema::user_groups::dsl::*;

        let mut conn = pool.get()?;
        let result = user_groups
            .filter(user_id.eq(self.0))
            .select(group_id)
            .load::<i32>(&mut conn)?;

        Ok(result)
    }

    pub async fn delete(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(diesel::delete(users.filter(id.eq(self.0))).execute(&mut pool.get()?)?)
    }

    /// Generate a subquery to get all group IDs for a user.
    ///
    /// Note that this does not execute the query, it only creates it.
    ///
    /// ## Example
    ///
    /// Check if a user has a specific class permission to a given namespace ID
    ///
    /// ```
    /// let group_id_subquery = user_id.group_ids_subquery();
    ///
    /// let base_query = classpermissions
    /// .into_boxed()
    /// .filter(namespace_id.eq(self.namespace_id))
    /// .filter(group_id.eq_any(group_id_subquery));
    ///
    /// let result = PermissionFilter::filter(permission, base_query)
    /// .first::<ClassPermission>(&mut conn)
    /// .optional()?;
    /// ```
    ///
    pub fn group_ids_subquery<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>
    {
        use crate::schema::user_groups::dsl::*;
        user_groups
            .filter(user_id.eq(self.0))
            .select(group_id)
            .into_boxed()
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

impl LoginUser {
    /// Check if the user exists and the plaintext password in the struct
    /// matches the hashed password in the database.
    pub async fn login(self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;

        let mut conn = pool
            .get()
            .map_err(|e| ApiError::DbConnectionError(e.to_string()))?;

        // We could do .first::<User>(&mut conn)? here, due to the way errors.rs uses "From"
        // to map diesel errors. But, we specifically map Diesel's NotFound to our own NotFound
        // which would lead to a 404 instead of a 401, leaking information about the existence
        // of the user.
        let user = match users
            .filter(username.eq(&self.username))
            .first::<User>(&mut conn)
        {
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
