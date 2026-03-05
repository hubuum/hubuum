use super::*;
impl User {
    pub async fn get_by_username(pool: &DbPool, username_arg: &str) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;

        with_connection(pool, |conn| {
            users.filter(username.eq(username_arg)).first::<User>(conn)
        })
    }

    /// Set a new password for a user
    ///
    /// The password will be hashed before storing it in the database, so the input should be the
    /// desired plaintext password.
    pub async fn set_password(&self, pool: &DbPool, new_password: &str) -> Result<(), ApiError> {
        use crate::schema::users::dsl::*;
        debug!(
            message = "Setting new password",
            id = self.id(),
            username = self.username,
        );
        let new_password = hash_password(new_password)
            .map_err(|e| ApiError::HashError(format!("Failed to hash password: {e}")))?;

        with_connection(pool, |conn| {
            diesel::update(users.filter(id.eq(self.id)))
                .set(password.eq(new_password))
                .execute(conn)
        })?;

        Ok(())
    }
}

pub trait StoreUserTokenRecord {
    async fn store_user_token_record(
        &self,
        pool: &DbPool,
        token_value: &Token,
    ) -> Result<(), ApiError>;
}

impl StoreUserTokenRecord for User {
    async fn store_user_token_record(
        &self,
        pool: &DbPool,
        token_value: &Token,
    ) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, user_id};

        with_connection(pool, |conn| {
            diesel::insert_into(crate::schema::tokens::table)
                .values((user_id.eq(self.id), token.eq(token_value.get_token())))
                .execute(conn)
        })?;
        Ok(())
    }
}

pub trait OwnedUserTokenRecord {
    async fn load_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<UserToken, ApiError>;

    async fn delete_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<usize, ApiError>;

    async fn delete_all_user_tokens_record(&self, pool: &DbPool) -> Result<usize, ApiError>;
}

impl OwnedUserTokenRecord for User {
    async fn load_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<UserToken, ApiError> {
        use crate::schema::tokens::dsl::{token, tokens, user_id};

        with_connection(pool, |conn| {
            tokens
                .filter(user_id.eq(self.id))
                .filter(token.eq(token_value.get_token()))
                .first::<UserToken>(conn)
        })
    }

    async fn delete_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{token, tokens, user_id};

        with_connection(pool, |conn| {
            diesel::delete(tokens.filter(user_id.eq(self.id)))
                .filter(token.eq(token_value.get_token()))
                .execute(conn)
        })
    }

    async fn delete_all_user_tokens_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{tokens, user_id};

        with_connection(pool, |conn| {
            diesel::delete(tokens.filter(user_id.eq(self.id))).execute(conn)
        })
    }
}

pub trait DeleteUserRecord {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError>;
}

impl DeleteUserRecord for User {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| {
            diesel::delete(users.filter(id.eq(self.id))).execute(conn)
        })
    }
}

impl DeleteUserRecord for UserID {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| {
            diesel::delete(users.filter(id.eq(self.0))).execute(conn)
        })
    }
}

pub trait CreateUserRecord {
    async fn create_user_record(&self, pool: &DbPool) -> Result<User, ApiError>;
}

impl CreateUserRecord for NewUser {
    async fn create_user_record(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::users;

        with_connection(pool, |conn| {
            diesel::insert_into(users)
                .values(self)
                .get_result::<User>(conn)
        })
    }
}

pub trait UpdateUserRecord {
    async fn update_user_record(&self, user_id: i32, pool: &DbPool) -> Result<User, ApiError>;
}

impl UpdateUserRecord for UpdateUser {
    async fn update_user_record(&self, user_id: i32, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| {
            diesel::update(users.filter(id.eq(user_id)))
                .set(self)
                .get_result::<User>(conn)
        })
    }
}

pub trait DeleteTokenRecord {
    async fn delete_token_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteTokenRecord for Token {
    async fn delete_token_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, tokens};

        with_connection(pool, |conn| {
            diesel::delete(tokens.filter(token.eq(&self.0))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait LoadUserRecord {
    async fn load_user_record(&self, pool: &DbPool) -> Result<User, ApiError>;
}

impl LoadUserRecord for User {
    async fn load_user_record(&self, _pool: &DbPool) -> Result<User, ApiError> {
        Ok(self.clone())
    }
}

impl LoadUserRecord for UserID {
    async fn load_user_record(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| users.filter(id.eq(self.0)).first::<User>(conn))
    }
}
