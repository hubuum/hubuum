use super::*;
use crate::db::traits::identity::identity_scope_by_name;
use crate::db::traits::principal::InsertPrincipalRecord;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::{NewPrincipal, PrincipalKind};

/// Sentinel password value set during anonymization. It is not a valid Argon2
/// PHC hash, so verification can never succeed.
const ANONYMIZED_PASSWORD: &str = "!anonymized-no-login";

fn user_snapshot(user: &User, name: &str) -> serde_json::Value {
    serde_json::json!({
        "id": user.id,
        "name": name,
        "proper_name": user.proper_name,
        "email": user.email,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    })
}

fn user_event(
    user: &User,
    name: &str,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(
        NewEvent::new(EntityType::User, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(user.id)
            .with_entity_name(name.to_string()),
    )
}

fn load_user_with_name(
    conn: &mut diesel::PgConnection,
    user_id_value: i32,
) -> Result<(User, String), diesel::result::Error> {
    use crate::schema::{principals, users};

    users::table
        .inner_join(principals::table.on(users::id.eq(principals::id)))
        .filter(users::id.eq(user_id_value))
        .select((users::all_columns, principals::name))
        .first::<(User, String)>(conn)
}

fn ensure_user_allows_local_write_conn(
    conn: &mut diesel::PgConnection,
    principal_id_value: i32,
) -> Result<(), ApiError> {
    use crate::schema::principals;

    let provider_managed = principals::table
        .filter(principals::id.eq(principal_id_value))
        .select(principals::provider_managed)
        .first::<bool>(conn)?;
    if provider_managed {
        return Err(ApiError::Forbidden(
            "Provider-managed users are read-only in Hubuum".to_string(),
        ));
    }
    Ok(())
}

impl User {
    /// Resolve a human user by its principal name.
    pub async fn get_by_name(pool: &DbPool, name_arg: &str) -> Result<User, ApiError> {
        Self::get_by_name_in_scope(pool, LOCAL_IDENTITY_SCOPE, name_arg).await
    }

    /// Resolve a human user by identity scope and principal name.
    pub async fn get_by_name_in_scope(
        pool: &DbPool,
        scope_arg: &str,
        name_arg: &str,
    ) -> Result<User, ApiError> {
        use crate::schema::identity_scopes;
        use crate::schema::principals;
        use crate::schema::users;

        with_connection(pool, |conn| {
            users::table
                .inner_join(principals::table.on(users::id.eq(principals::id)))
                .inner_join(
                    identity_scopes::table
                        .on(principals::identity_scope_id.eq(identity_scopes::id)),
                )
                .filter(principals::name.eq(name_arg))
                .filter(identity_scopes::name.eq(scope_arg))
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

        with_connection(pool, |conn| -> Result<usize, ApiError> {
            ensure_user_allows_local_write_conn(conn, self.id)?;
            Ok(diesel::update(users.filter(id.eq(self.id)))
                .set(password.eq(Some(new_password)))
                .execute(conn)?)
        })?;

        Ok(())
    }
}

pub fn count_user_records(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::users::dsl::users;
    with_connection(pool, |conn| users.count().get_result::<i64>(conn))
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
    async fn delete_user_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError>;

    async fn delete_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        let _ = context;
        self.delete_user_record_without_events(pool).await
    }
}

/// Delete a user by removing its principal row, which cascades to the `users`
/// row, group memberships, and tokens. (The FK cascades principal → subtype, so
/// deleting the `users` row alone would orphan the principal.)
fn delete_principal_without_events(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    use crate::schema::principals::dsl::{id, principals};
    with_connection(pool, |conn| -> Result<usize, ApiError> {
        ensure_user_allows_local_write_conn(conn, principal_id_value)?;
        Ok(diesel::delete(principals.filter(id.eq(principal_id_value))).execute(conn)?)
    })
}

fn delete_principal(
    pool: &DbPool,
    principal_id_value: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    let Some(context) = context else {
        return delete_principal_without_events(pool, principal_id_value);
    };

    use crate::schema::principals::dsl::{id, principals};

    with_transaction(pool, |conn| -> Result<usize, ApiError> {
        let (user, name) = load_user_with_name(conn, principal_id_value)?;
        ensure_user_allows_local_write_conn(conn, principal_id_value)?;
        let deleted = diesel::delete(principals.filter(id.eq(principal_id_value))).execute(conn)?;
        let event = user_event(
            &user,
            &name,
            Action::Deleted,
            context,
            format!("User '{name}' deleted"),
        )?
        .with_before(user_snapshot(&user, &name));
        emit_event(conn, &event)?;
        Ok(deleted)
    })
}

impl DeleteUserRecord for User {
    async fn delete_user_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError> {
        delete_principal_without_events(pool, self.id)
    }

    async fn delete_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        delete_principal(pool, self.id, context)
    }
}

impl DeleteUserRecord for UserID {
    async fn delete_user_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError> {
        delete_principal_without_events(pool, self.id())
    }

    async fn delete_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        delete_principal(pool, self.id(), context)
    }
}

pub trait CreateUserRecord {
    async fn create_user_record_without_events(&self, pool: &DbPool) -> Result<User, ApiError>;

    async fn create_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError> {
        let _ = context;
        self.create_user_record_without_events(pool).await
    }
}

impl CreateUserRecord for NewUser {
    /// Principal-first user creation: insert the `principals` row (kind=human,
    /// name) then the `users` row sharing the same id, in one transaction.
    async fn create_user_record_without_events(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users;

        let name = self.name.clone();
        let scope_name = self
            .identity_scope
            .clone()
            .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let password = self.password.clone();
        let proper_name = self.proper_name.clone();
        let email = self.email.clone();

        if scope_name != LOCAL_IDENTITY_SCOPE {
            return Err(ApiError::BadRequest(
                "users in non-local identity scopes are managed by their identity provider"
                    .to_string(),
            ));
        }
        let scope = identity_scope_by_name(pool, &scope_name).await?;

        with_transaction(pool, |conn| -> Result<User, ApiError> {
            let principal = NewPrincipal {
                identity_scope_id: scope.id,
                kind: PrincipalKind::Human.as_str(),
                name: &name,
            }
            .insert(conn)?;

            let user = diesel::insert_into(users::table)
                .values((
                    users::id.eq(principal.id),
                    users::password.eq(Some(&password)),
                    users::proper_name.eq(&proper_name),
                    users::email.eq(&email),
                ))
                .get_result::<User>(conn)?;

            Ok(user)
        })
    }

    async fn create_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError> {
        let Some(context) = context else {
            return self.create_user_record_without_events(pool).await;
        };

        use crate::schema::users;

        let name = self.name.clone();
        let scope_name = self
            .identity_scope
            .clone()
            .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let password = self.password.clone();
        let proper_name = self.proper_name.clone();
        let email = self.email.clone();

        if scope_name != LOCAL_IDENTITY_SCOPE {
            return Err(ApiError::BadRequest(
                "users in non-local identity scopes are managed by their identity provider"
                    .to_string(),
            ));
        }
        let scope = identity_scope_by_name(pool, &scope_name).await?;

        with_transaction(pool, |conn| -> Result<User, ApiError> {
            let principal = NewPrincipal {
                identity_scope_id: scope.id,
                kind: PrincipalKind::Human.as_str(),
                name: &name,
            }
            .insert(conn)?;

            let user = diesel::insert_into(users::table)
                .values((
                    users::id.eq(principal.id),
                    users::password.eq(Some(&password)),
                    users::proper_name.eq(&proper_name),
                    users::email.eq(&email),
                ))
                .get_result::<User>(conn)?;

            let event = user_event(
                &user,
                &name,
                Action::Created,
                context,
                format!("User '{name}' created"),
            )?
            .with_after(user_snapshot(&user, &name));
            emit_event(conn, &event)?;
            Ok(user)
        })
    }
}

pub trait UpdateUserRecord {
    async fn update_user_record_without_events(
        &self,
        user_id: i32,
        pool: &DbPool,
    ) -> Result<User, ApiError>;

    async fn update_user_record(
        &self,
        user_id: i32,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError> {
        let _ = context;
        self.update_user_record_without_events(user_id, pool).await
    }
}

impl UpdateUserRecord for UpdateUser {
    async fn update_user_record_without_events(
        &self,
        user_id: i32,
        pool: &DbPool,
    ) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| -> Result<User, ApiError> {
            ensure_user_allows_local_write_conn(conn, user_id)?;
            Ok(diesel::update(users.filter(id.eq(user_id)))
                .set(self)
                .get_result::<User>(conn)?)
        })
    }

    async fn update_user_record(
        &self,
        user_id: i32,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError> {
        let Some(context) = context else {
            return self.update_user_record_without_events(user_id, pool).await;
        };

        use crate::schema::users::dsl::{id, users};

        with_transaction(pool, |conn| -> Result<User, ApiError> {
            let (before, name) = load_user_with_name(conn, user_id)?;
            ensure_user_allows_local_write_conn(conn, user_id)?;
            let after = diesel::update(users.filter(id.eq(user_id)))
                .set(self)
                .get_result::<User>(conn)?;
            let event = user_event(
                &after,
                &name,
                Action::Updated,
                context,
                format!("User '{name}' updated"),
            )?
            .with_before(user_snapshot(&before, &name))
            .with_after(user_snapshot(&after, &name))
            .with_metadata(serde_json::json!({
                "password_changed": self.password.is_some(),
            }));
            emit_event(conn, &event)?;
            Ok(after)
        })
    }
}

pub trait AnonymizeUserRecord {
    async fn anonymize_user_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl AnonymizeUserRecord for UserID {
    async fn anonymize_user_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        anonymize_user_record(pool, self.id())
    }
}

impl AnonymizeUserRecord for User {
    async fn anonymize_user_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        anonymize_user_record(pool, self.id)
    }
}

fn anonymize_user_record(pool: &DbPool, target_id: i32) -> Result<(), ApiError> {
    use crate::schema::principals::dsl as p;
    use crate::schema::tokens::dsl as t;
    use crate::schema::users::dsl as u;

    with_transaction(pool, |conn| -> Result<(), ApiError> {
        ensure_user_allows_local_write_conn(conn, target_id)?;
        let updated = diesel::update(u::users.filter(u::id.eq(target_id)))
            .set((
                u::proper_name.eq::<Option<String>>(None),
                u::email.eq::<Option<String>>(None),
                u::password.eq(Some(ANONYMIZED_PASSWORD)),
                u::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(conn)?;
        if updated == 0 {
            return Err(ApiError::NotFound(format!("User {target_id} not found")));
        }

        diesel::update(p::principals.filter(p::id.eq(target_id)))
            .set(p::name.eq(format!("anonymized-{target_id}")))
            .execute(conn)?;
        diesel::update(
            t::tokens
                .filter(t::principal_id.eq(target_id))
                .filter(t::revoked_at.is_null()),
        )
        .set(t::revoked_at.eq(diesel::dsl::now))
        .execute(conn)?;
        Ok(())
    })
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
