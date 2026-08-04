use super::*;
use crate::db::traits::identity::identity_scope_by_name;
use crate::db::traits::principal::InsertPrincipalRecord;
use crate::db::traits::token::revoke_all_tokens_for_principal_conn;
use crate::models::PrincipalID;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::{NewPrincipal, PrincipalKind};
use diesel_async::RunQueryDsl;

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

async fn load_user_with_name(
    conn: &mut crate::db::DbConnection,
    user_id_value: i32,
) -> Result<(User, String), diesel::result::Error> {
    use crate::schema::{principals, users};

    users::table
        .inner_join(principals::table.on(users::id.eq(principals::id)))
        .filter(users::id.eq(user_id_value))
        .select((users::all_columns, principals::name))
        .first::<(User, String)>(conn)
        .await
}

async fn ensure_user_allows_local_write_conn(
    conn: &mut crate::db::DbConnection,
    principal_id_value: i32,
) -> Result<(), ApiError> {
    use crate::schema::principals;

    let provider_managed = principals::table
        .filter(principals::id.eq(principal_id_value))
        .select(principals::provider_managed)
        .first::<bool>(conn)
        .await?;
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

        let pool = pool.clone();
        let name = name_arg.to_string();
        let scope = scope_arg.to_string();
        crate::db::with_connection_async(pool, async move |conn| {
            users::table
                .inner_join(principals::table.on(users::id.eq(principals::id)))
                .inner_join(
                    identity_scopes::table
                        .on(principals::identity_scope_id.eq(identity_scopes::id)),
                )
                .filter(principals::name.eq(name))
                .filter(identity_scopes::name.eq(scope))
                .select(users::all_columns)
                .first::<User>(conn)
                .await
        })
        .await
    }
}

pub(crate) trait SetUserPasswordRecord {
    /// Persist a pre-hashed password and revoke all active bearer tokens in one
    /// transaction. Returns the number of tokens revoked.
    async fn set_password_record(
        &self,
        pool: &DbPool,
        password_hash: &str,
    ) -> Result<usize, ApiError>;
}

impl SetUserPasswordRecord for User {
    async fn set_password_record(
        &self,
        pool: &DbPool,
        password_hash: &str,
    ) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::{id, password, users};

        let principal_id = PrincipalID::new(self.id)?;
        with_transaction(pool, async |conn| -> Result<usize, ApiError> {
            ensure_user_allows_local_write_conn(conn, principal_id.id()).await?;
            diesel::update(users.filter(id.eq(principal_id.id())))
                .set(password.eq(Some(password_hash)))
                .execute(conn)
                .await?;
            revoke_all_tokens_for_principal_conn(conn, principal_id).await
        })
        .await
    }
}

pub async fn count_user_records(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::users::dsl::users;
    with_connection(pool, async |conn| {
        users.count().get_result::<i64>(conn).await
    })
    .await
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
        use crate::schema::tokens::dsl::{expires_at, issued, principal_id, token};
        let token_hash = token_value.storage_hash();
        let lifetime = crate::models::configured_token_lifetime()?;

        with_connection(pool, async |conn| {
            let issued_at = diesel::dsl::sql::<diesel::sql_types::Timestamp>(
                "statement_timestamp() AT TIME ZONE 'UTC'",
            );
            let expiry = diesel::dsl::sql::<
                diesel::sql_types::Nullable<diesel::sql_types::Timestamp>,
            >("(statement_timestamp() AT TIME ZONE 'UTC') + (")
            .bind::<diesel::sql_types::BigInt, _>(lifetime.hours())
            .sql(" * INTERVAL '1 hour')");
            diesel::insert_into(crate::schema::tokens::table)
                .values((
                    principal_id.eq(self.id),
                    token.eq(token_hash),
                    issued.eq(issued_at),
                    expires_at.eq(expiry),
                ))
                .execute(conn)
                .await
        })
        .await?;
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

        with_connection(pool, async |conn| {
            tokens
                .filter(principal_id.eq(self.id))
                .filter(token.eq(token_hash))
                .first::<PrincipalToken>(conn)
                .await
        })
        .await
    }

    async fn delete_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{principal_id, revoked_at, token, tokens};
        let token_hash = token_value.storage_hash();

        // Soft-revoke: revoked rows are retained for auditability.
        with_connection(pool, async |conn| {
            diesel::update(
                tokens
                    .filter(principal_id.eq(self.id))
                    .filter(token.eq(token_hash))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
            .await
        })
        .await
    }

    async fn delete_all_user_tokens_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        let principal_id = PrincipalID::new(self.id)?;
        with_connection(pool, async |conn| {
            revoke_all_tokens_for_principal_conn(conn, principal_id).await
        })
        .await
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
async fn delete_principal_without_events(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    use crate::schema::principals::dsl::{id, principals};
    with_connection(pool, async |conn| -> Result<usize, ApiError> {
        ensure_user_allows_local_write_conn(conn, principal_id_value).await?;
        Ok(diesel::delete(principals.filter(id.eq(principal_id_value)))
            .execute(conn)
            .await?)
    })
    .await
}

async fn delete_principal(
    pool: &DbPool,
    principal_id_value: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    let Some(context) = context else {
        return delete_principal_without_events(pool, principal_id_value).await;
    };

    use crate::schema::principals::dsl::{id, principals};

    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        principals
            .filter(id.eq(principal_id_value))
            .for_update()
            .select(id)
            .first::<i32>(conn)
            .await?;
        let (user, name) = load_user_with_name(conn, principal_id_value).await?;
        ensure_user_allows_local_write_conn(conn, principal_id_value).await?;
        let deleted = diesel::delete(principals.filter(id.eq(principal_id_value)))
            .execute(conn)
            .await?;
        let event = user_event(
            &user,
            &name,
            Action::Deleted,
            context,
            format!("User '{name}' deleted"),
        )?
        .with_before(user_snapshot(&user, &name));
        emit_event(conn, &event).await?;
        Ok(deleted)
    })
    .await
}

impl DeleteUserRecord for User {
    async fn delete_user_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError> {
        delete_principal_without_events(pool, self.id).await
    }

    async fn delete_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        delete_principal(pool, self.id, context).await
    }
}

impl DeleteUserRecord for UserID {
    async fn delete_user_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError> {
        delete_principal_without_events(pool, self.id()).await
    }

    async fn delete_user_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        delete_principal(pool, self.id(), context).await
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

        with_transaction(pool, async |conn| -> Result<User, ApiError> {
            let principal = NewPrincipal {
                identity_scope_id: scope.id,
                kind: PrincipalKind::Human.as_str(),
                name: &name,
            }
            .insert(conn)
            .await?;

            let user = diesel::insert_into(users::table)
                .values((
                    users::id.eq(principal.id),
                    users::password.eq(Some(&password)),
                    users::proper_name.eq(&proper_name),
                    users::email.eq(&email),
                ))
                .get_result::<User>(conn)
                .await?;

            Ok(user)
        })
        .await
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

        with_transaction(pool, async |conn| -> Result<User, ApiError> {
            let principal = NewPrincipal {
                identity_scope_id: scope.id,
                kind: PrincipalKind::Human.as_str(),
                name: &name,
            }
            .insert(conn)
            .await?;

            let user = diesel::insert_into(users::table)
                .values((
                    users::id.eq(principal.id),
                    users::password.eq(Some(&password)),
                    users::proper_name.eq(&proper_name),
                    users::email.eq(&email),
                ))
                .get_result::<User>(conn)
                .await?;

            let event = user_event(
                &user,
                &name,
                Action::Created,
                context,
                format!("User '{name}' created"),
            )?
            .with_after(user_snapshot(&user, &name));
            emit_event(conn, &event).await?;
            Ok(user)
        })
        .await
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

        let principal_id = PrincipalID::new(user_id)?;
        with_transaction(pool, async |conn| -> Result<User, ApiError> {
            ensure_user_allows_local_write_conn(conn, principal_id.id()).await?;
            let updated = diesel::update(users.filter(id.eq(principal_id.id())))
                .set(self)
                .get_result::<User>(conn)
                .await?;
            if self.password.is_some() {
                revoke_all_tokens_for_principal_conn(conn, principal_id).await?;
            }
            Ok(updated)
        })
        .await
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

        let principal_id = PrincipalID::new(user_id)?;
        with_transaction(pool, async |conn| -> Result<User, ApiError> {
            use crate::schema::principals;

            let before = users
                .filter(id.eq(principal_id.id()))
                .for_update()
                .first::<User>(conn)
                .await?;
            let name = principals::table
                .filter(principals::id.eq(principal_id.id()))
                .select(principals::name)
                .first::<String>(conn)
                .await?;
            ensure_user_allows_local_write_conn(conn, principal_id.id()).await?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let after = diesel::update(users.filter(id.eq(principal_id.id())))
                .set(self)
                .get_result::<User>(conn)
                .await?;
            if self.password.is_some() {
                revoke_all_tokens_for_principal_conn(conn, principal_id).await?;
            }
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
            emit_event(conn, &event).await?;
            Ok(after)
        })
        .await
    }
}

pub trait AnonymizeUserRecord {
    async fn anonymize_user_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl AnonymizeUserRecord for UserID {
    async fn anonymize_user_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        anonymize_user_record(pool, self.id()).await
    }
}

impl AnonymizeUserRecord for User {
    async fn anonymize_user_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        anonymize_user_record(pool, self.id).await
    }
}

async fn anonymize_user_record(pool: &DbPool, target_id: i32) -> Result<(), ApiError> {
    use crate::schema::principals::dsl as p;
    use crate::schema::users::dsl as u;

    let principal_id = PrincipalID::new(target_id)?;
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        ensure_user_allows_local_write_conn(conn, principal_id.id()).await?;
        use crate::schema::computed_field_definitions::dsl as computed;
        diesel::delete(
            computed::computed_field_definitions
                .filter(computed::owner_user_id.eq(Some(principal_id.id())))
                .filter(computed::visibility.eq("personal")),
        )
        .execute(conn)
        .await?;
        let updated = diesel::update(u::users.filter(u::id.eq(principal_id.id())))
            .set((
                u::proper_name.eq::<Option<String>>(None),
                u::email.eq::<Option<String>>(None),
                u::password.eq(Some(ANONYMIZED_PASSWORD)),
                u::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(conn)
            .await?;
        if updated == 0 {
            return Err(ApiError::NotFound(format!("User {target_id} not found")));
        }

        diesel::update(p::principals.filter(p::id.eq(principal_id.id())))
            .set(p::name.eq(format!("anonymized-{target_id}")))
            .execute(conn)
            .await?;
        revoke_all_tokens_for_principal_conn(conn, principal_id).await?;
        Ok(())
    })
    .await
}

pub trait DeleteTokenRecord {
    async fn delete_token_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteTokenRecord for Token {
    async fn delete_token_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{revoked_at, token, tokens};
        let token_hash = self.storage_hash();

        // Soft-revoke rather than hard-delete.
        with_connection(pool, async |conn| {
            diesel::update(
                tokens
                    .filter(token.eq(token_hash))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
            .await
        })
        .await?;
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

        with_connection(pool, async |conn| {
            users.filter(id.eq(self.id())).first::<User>(conn).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::traits::Status;
    use crate::models::user::UserID;
    use crate::tests::{TestScope, create_user_with_params};

    async fn user_with_tokens(scope: &TestScope, label: &str) -> (User, Vec<Token>) {
        let user =
            create_user_with_params(&scope.pool, &scope.scoped_name(label), "initial-password")
                .await;
        let first = user.create_token(&scope.pool).await.unwrap();
        let second = user.create_token(&scope.pool).await.unwrap();
        (user, vec![first, second])
    }

    async fn assert_tokens_active(pool: &DbPool, tokens: &[Token]) {
        for token in tokens {
            token.is_valid(pool).await.expect("token should be active");
        }
    }

    async fn assert_tokens_revoked(pool: &DbPool, tokens: &[Token]) {
        for token in tokens {
            assert!(matches!(
                token.is_valid(pool).await,
                Err(ApiError::Unauthorized(_))
            ));
        }
    }

    #[actix_web::test]
    async fn setting_password_revokes_all_active_tokens() {
        let scope = TestScope::new();
        let (user, tokens) = user_with_tokens(&scope, "set_password_revokes").await;
        assert_tokens_active(&scope.pool, &tokens).await;

        user.set_password(&scope.pool, "replacement-password")
            .await
            .unwrap();

        assert_tokens_revoked(&scope.pool, &tokens).await;
        user.delete_without_events(&scope.pool).await.unwrap();
    }

    #[actix_web::test]
    async fn password_update_revokes_all_active_tokens() {
        let scope = TestScope::new();
        let (user, tokens) = user_with_tokens(&scope, "password_update_revokes").await;
        assert_tokens_active(&scope.pool, &tokens).await;
        let context = EventContext::user(user.id, None, None);

        UpdateUser {
            password: Some("replacement-password".to_string()),
            proper_name: None,
            email: None,
        }
        .save(UserID::new(user.id).unwrap(), &scope.pool, Some(&context))
        .await
        .unwrap();

        assert_tokens_revoked(&scope.pool, &tokens).await;
        user.delete_without_events(&scope.pool).await.unwrap();
    }

    #[actix_web::test]
    async fn profile_update_preserves_active_tokens() {
        let scope = TestScope::new();
        let (user, tokens) = user_with_tokens(&scope, "profile_update_preserves").await;
        let context = EventContext::user(user.id, None, None);

        UpdateUser {
            password: None,
            proper_name: Some("Updated Name".to_string()),
            email: None,
        }
        .save(UserID::new(user.id).unwrap(), &scope.pool, Some(&context))
        .await
        .unwrap();

        assert_tokens_active(&scope.pool, &tokens).await;
        user.delete_without_events(&scope.pool).await.unwrap();
    }
}
