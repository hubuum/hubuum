use super::*;
use crate::models::principal::{NewPrincipal, PrincipalKind};

impl User {
    /// Resolve a human user by its principal name.
    pub async fn get_by_name(pool: &DbPool, name_arg: &str) -> Result<User, ApiError> {
        use crate::schema::principals;
        use crate::schema::users;

        with_connection(pool, |conn| {
            users::table
                .inner_join(principals::table.on(users::id.eq(principals::id)))
                .filter(principals::name.eq(name_arg))
                .select(users::all_columns)
                .first::<User>(conn)
        })
    }

    /// Set a new password for a user.
    ///
    /// The password will be hashed before storing it in the database, so the input should be the
    /// desired plaintext password.
    pub async fn set_password(&self, pool: &DbPool, new_password: &str) -> Result<(), ApiError> {
        use crate::schema::users::dsl::*;
        debug!(message = "Setting new password", id = self.id());
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
        use crate::schema::tokens::dsl::{principal_id, token};
        let token_hash = token_value.storage_hash();

        with_connection(pool, |conn| {
            diesel::insert_into(crate::schema::tokens::table)
                .values((principal_id.eq(self.id), token.eq(token_hash)))
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
    ) -> Result<PrincipalToken, ApiError>;

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
    ) -> Result<PrincipalToken, ApiError> {
        use crate::schema::tokens::dsl::{principal_id, token, tokens};
        let token_hash = token_value.storage_hash();

        with_connection(pool, |conn| {
            tokens
                .filter(principal_id.eq(self.id))
                .filter(token.eq(token_hash))
                .first::<PrincipalToken>(conn)
        })
    }

    async fn delete_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{principal_id, revoked_at, token, tokens};
        let token_hash = token_value.storage_hash();

        // Soft-revoke: revoked rows are retained for auditability.
        with_connection(pool, |conn| {
            diesel::update(
                tokens
                    .filter(principal_id.eq(self.id))
                    .filter(token.eq(token_hash))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
        })
    }

    async fn delete_all_user_tokens_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{principal_id, revoked_at, tokens};

        with_connection(pool, |conn| {
            diesel::update(
                tokens
                    .filter(principal_id.eq(self.id))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
        })
    }
}

pub trait DeleteUserRecord {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError>;
}

/// Delete a user by removing its principal row, which cascades to the `users`
/// row, group memberships, and tokens. (The FK cascades principal → subtype, so
/// deleting the `users` row alone would orphan the principal.)
fn delete_principal(pool: &DbPool, principal_id_value: i32) -> Result<usize, ApiError> {
    use crate::schema::principals::dsl::{id, principals};
    with_connection(pool, |conn| {
        diesel::delete(principals.filter(id.eq(principal_id_value))).execute(conn)
    })
}

impl DeleteUserRecord for User {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        delete_principal(pool, self.id)
    }
}

impl DeleteUserRecord for UserID {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        delete_principal(pool, self.id())
    }
}

pub trait CreateUserRecord {
    async fn create_user_record(&self, pool: &DbPool) -> Result<User, ApiError>;
}

impl CreateUserRecord for NewUser {
    /// Principal-first user creation: insert the `principals` row (kind=human,
    /// name) then the `users` row sharing the same id, in one transaction.
    async fn create_user_record(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users;

        let name = self.name.clone();
        let password = self.password.clone();
        let email = self.email.clone();

        with_transaction(pool, |conn| -> Result<User, ApiError> {
            let principal = NewPrincipal {
                kind: PrincipalKind::Human.as_str(),
                name: &name,
            }
            .insert(conn)?;

            let user = diesel::insert_into(users::table)
                .values((
                    users::id.eq(principal.id),
                    users::password.eq(&password),
                    users::email.eq(&email),
                ))
                .get_result::<User>(conn)?;

            Ok(user)
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
        use crate::schema::tokens::dsl::{revoked_at, token, tokens};
        let token_hash = self.storage_hash();

        // Soft-revoke rather than hard-delete.
        with_connection(pool, |conn| {
            diesel::update(
                tokens
                    .filter(token.eq(token_hash))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
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

        with_connection(pool, |conn| {
            users.filter(id.eq(self.id())).first::<User>(conn)
        })
    }
}
