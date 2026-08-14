use super::*;
use crate::models::PrincipalID;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::{NewPrincipal, PrincipalKind};
use crate::storage::postgres::operations::identity::identity_scope_by_name;
use crate::storage::postgres::operations::principal::{
    InsertPrincipalRecord, lock_principal_revision_conn, principal_revision_conn,
};
use crate::storage::postgres::operations::token::revoke_all_tokens_for_principal_conn;
use diesel_async::RunQueryDsl;
use hubuum_storage_postgres::PostgresRevision;

/// Sentinel password value set during anonymization. It is not a valid Argon2
/// PHC hash, so verification can never succeed.
const ANONYMIZED_PASSWORD: &str = "!anonymized-no-login";

fn user_snapshot(user: &User, name: &str, revision: PostgresRevision) -> serde_json::Value {
    serde_json::json!({
        "id": user.id,
        "name": name,
        "proper_name": user.proper_name,
        "email": user.email,
        "revision": revision,
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
    conn: &mut crate::storage::postgres::PostgresConnection,
    user_id_value: i32,
) -> Result<(User, String), diesel::result::Error> {
    use crate::schema::{principals, users};

    users::table
        .inner_join(principals::table.on(users::id.eq(principals::id)))
        .filter(users::id.eq(user_id_value))
        .select((users::all_columns, principals::name))
        .first::<(UserRow, String)>(conn)
        .await
        .map(|(user, name)| (user.into(), name))
}

async fn ensure_user_allows_local_write_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
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

/// Resolve a human user by identity scope and principal name.
pub(crate) async fn load_user_by_name_record(
    pool: &crate::storage::postgres::PostgresPool,
    scope_arg: &str,
    name_arg: &str,
) -> Result<User, ApiError> {
    use crate::schema::{identity_scopes, principals, users};

    let name = name_arg.to_string();
    let scope = scope_arg.to_string();
    crate::storage::postgres::with_connection_async(pool, async move |conn| {
        users::table
            .inner_join(principals::table.on(users::id.eq(principals::id)))
            .inner_join(
                identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
            )
            .filter(principals::name.eq(name))
            .filter(identity_scopes::name.eq(scope))
            .select(users::all_columns)
            .first::<UserRow>(conn)
            .await
            .map(Into::into)
    })
    .await
}

async fn set_local_password_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
    principal_id: PrincipalID,
    password_hash: &str,
) -> Result<usize, ApiError> {
    use crate::schema::users::dsl::{id, password, users};

    ensure_user_allows_local_write_conn(conn, principal_id.id()).await?;
    diesel::update(users.filter(id.eq(principal_id.id())))
        .set(password.eq(Some(password_hash)))
        .execute(conn)
        .await?;
    revoke_all_tokens_for_principal_conn(conn, principal_id).await
}

/// Resolve one local human by name, replace its pre-hashed credential, and
/// revoke all active bearer tokens in one transaction.
pub(crate) async fn reset_local_password_record(
    pool: &crate::storage::postgres::PostgresPool,
    principal_name: &str,
    password_hash: &str,
) -> Result<usize, ApiError> {
    use crate::schema::{identity_scopes, principals, users};

    let principal_name = principal_name.to_string();
    let password_hash = password_hash.to_string();
    with_transaction(pool, async move |conn| -> Result<usize, ApiError> {
        let principal_id = users::table
            .inner_join(principals::table.on(users::id.eq(principals::id)))
            .inner_join(
                identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
            )
            .filter(principals::name.eq(principal_name))
            .filter(identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
            .select(users::id)
            .first::<i32>(conn)
            .await?;
        set_local_password_conn(conn, PrincipalID::new(principal_id)?, &password_hash).await
    })
    .await
}

pub(crate) async fn set_user_password_record(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id: PrincipalID,
    password_hash: &str,
) -> Result<usize, ApiError> {
    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        set_local_password_conn(conn, principal_id, password_hash).await
    })
    .await
}

pub async fn count_user_records(
    pool: &crate::storage::postgres::PostgresPool,
) -> Result<i64, ApiError> {
    use crate::schema::users::dsl::users;
    with_connection(pool, async |conn| {
        users.count().get_result::<i64>(conn).await
    })
    .await
}

/// Delete a user by removing its principal row, which cascades to the `users`
/// row, group memberships, and tokens. (The FK cascades principal → subtype, so
/// deleting the `users` row alone would orphan the principal.)
async fn delete_principal_without_events(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id_value: i32,
) -> Result<usize, ApiError> {
    use crate::schema::principals::dsl::{id, principals};
    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        lock_principal_revision_conn(conn, principal_id_value).await?;
        ensure_user_allows_local_write_conn(conn, principal_id_value).await?;
        Ok(diesel::delete(principals.filter(id.eq(principal_id_value)))
            .execute(conn)
            .await?)
    })
    .await
}

pub(crate) async fn delete_user_record(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id_value: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    let Some(context) = context else {
        return delete_principal_without_events(pool, principal_id_value).await;
    };

    use crate::schema::principals::dsl::{id, principals};

    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        let before_revision = lock_principal_revision_conn(conn, principal_id_value).await?;
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
        .with_before(user_snapshot(&user, &name, before_revision));
        emit_event(conn, &event).await?;
        Ok(deleted)
    })
    .await
}

pub trait CreateUserRecord {
    async fn create_user_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<User, ApiError>;

    async fn create_user_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError> {
        let _ = context;
        self.create_user_record_without_events(pool).await
    }
}

impl CreateUserRecord for NewUser {
    /// Principal-first user creation: insert the `principals` row (kind=human,
    /// name) then the `users` row sharing the same id, in one transaction.
    async fn create_user_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<User, ApiError> {
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
                .get_result::<UserRow>(conn)
                .await?
                .into();
            Ok(user)
        })
        .await
    }

    async fn create_user_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
                .get_result::<UserRow>(conn)
                .await?
                .into();
            let revision = principal_revision_conn(conn, principal.id).await?;

            let event = user_event(
                &user,
                &name,
                Action::Created,
                context,
                format!("User '{name}' created"),
            )?
            .with_after(user_snapshot(&user, &name, revision));
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<User, ApiError>;

    async fn update_user_record(
        &self,
        user_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        let principal_id = PrincipalID::new(user_id)?;
        with_transaction(pool, async |conn| -> Result<User, ApiError> {
            lock_principal_revision_conn(conn, principal_id.id()).await?;
            ensure_user_allows_local_write_conn(conn, principal_id.id()).await?;
            let before = users
                .filter(id.eq(principal_id.id()))
                .first::<UserRow>(conn)
                .await?
                .into();
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(users.filter(id.eq(principal_id.id())))
                .set(UpdateUserRow::from(self))
                .get_result::<UserRow>(conn)
                .await?
                .into();
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
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError> {
        let Some(context) = context else {
            return self.update_user_record_without_events(user_id, pool).await;
        };

        use crate::schema::users::dsl::{id, users};

        let principal_id = PrincipalID::new(user_id)?;
        with_transaction(pool, async |conn| -> Result<User, ApiError> {
            use crate::schema::principals;

            let before_revision = lock_principal_revision_conn(conn, principal_id.id()).await?;
            let before = users
                .filter(id.eq(principal_id.id()))
                .first::<UserRow>(conn)
                .await?
                .into();
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
                .set(UpdateUserRow::from(self))
                .get_result::<UserRow>(conn)
                .await?
                .into();
            let after_revision = principal_revision_conn(conn, principal_id.id()).await?;
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
            .with_before(user_snapshot(&before, &name, before_revision))
            .with_after(user_snapshot(&after, &name, after_revision))
            .with_metadata(serde_json::json!({
                "password_changed": self.password.is_some(),
            }));
            emit_event(conn, &event).await?;
            Ok(after)
        })
        .await
    }
}

pub(crate) async fn anonymize_user_record(
    pool: &crate::storage::postgres::PostgresPool,
    target_id: i32,
) -> Result<(), ApiError> {
    use crate::schema::principals::dsl as p;
    use crate::schema::users::dsl as u;

    let principal_id = PrincipalID::new(target_id)?;
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        lock_principal_revision_conn(conn, principal_id.id()).await?;
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

pub(crate) async fn load_user_record(
    pool: &crate::storage::postgres::PostgresPool,
    user_id: i32,
) -> Result<User, ApiError> {
    use crate::schema::users::dsl::{id, users};

    with_connection(pool, async |conn| {
        users
            .filter(id.eq(user_id))
            .first::<UserRow>(conn)
            .await
            .map(Into::into)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Token;
    use crate::models::user::UserID;
    use crate::services::authentication::authenticate_bearer_token;
    use crate::tests::{TestScope, create_user_with_params};

    async fn user_with_tokens(scope: &TestScope, label: &str) -> (User, Vec<Token>) {
        let user =
            create_user_with_params(&scope.pool, &scope.scoped_name(label), "initial-password")
                .await;
        let first = user.create_token(&scope.pool).await.unwrap();
        let second = user.create_token(&scope.pool).await.unwrap();
        (user, vec![first, second])
    }

    async fn assert_tokens_active(pool: &crate::storage::postgres::PostgresPool, tokens: &[Token]) {
        for token in tokens {
            authenticate_bearer_token(pool, token)
                .await
                .expect("token should be active");
        }
    }

    async fn assert_tokens_revoked(
        pool: &crate::storage::postgres::PostgresPool,
        tokens: &[Token],
    ) {
        for token in tokens {
            assert!(matches!(
                authenticate_bearer_token(pool, token).await,
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
