use chrono::NaiveDateTime;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Nullable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{IdentityScopeId, PrincipalId, UserId};
use hubuum_storage_core::{
    AuthenticatedToken, AuthenticationAttempt, AuthenticationHuman, AuthenticationIdentity,
    AuthenticationPrincipal, AuthenticationPrincipalKind, AuthenticationTokenScope,
    AuthenticationTokenScopeQuery,
};

use crate::schema;
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

/// Advance `last_used_at` at most this often. Routine authenticated requests
/// stay read-only on the hot path, and a telemetry write failure never rejects
/// an otherwise valid credential.
const LAST_USED_AT_THROTTLE_SECS: i64 = 60;

/// Boxed PostgreSQL predicate for an active token.
///
/// Explicit expiry is authoritative. Only legacy rows without `expires_at`
/// use the configured validity cutoff supplied by the application.
pub fn active_token_predicate(
    observed_at: NaiveDateTime,
    legacy_valid_after: NaiveDateTime,
) -> Box<dyn BoxableExpression<schema::tokens::table, Pg, SqlType = Nullable<Bool>>> {
    use crate::schema::tokens::dsl::{expires_at, issued, revoked_at};
    Box::new(
        revoked_at.is_null().and(
            expires_at
                .gt(observed_at)
                .or(expires_at.is_null().and(issued.gt(legacy_valid_after))),
        ),
    )
}

pub async fn authenticate_bearer_token(
    runtime: &PostgresRuntime,
    attempt: AuthenticationAttempt,
) -> Result<AuthenticatedToken, PostgresStorageError> {
    use crate::schema::service_accounts;
    use crate::schema::tokens::dsl::{
        description, expires_at, id as token_id, issued, last_used_at, name, permission_scoped,
        principal_id, resource_scoped, revision, token, tokens,
    };

    let (credential, observed_at, legacy_valid_after) = attempt.into_parts();
    let lookup_value = credential.lookup_value().to_string();
    let row = runtime
        .with_connection(async move |conn| {
            tokens
                .filter(token.eq(lookup_value))
                .filter(active_token_predicate(observed_at, legacy_valid_after))
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    service_accounts::table
                        .filter(service_accounts::id.eq(principal_id))
                        .filter(service_accounts::disabled_at.is_not_null()),
                )))
                .select((
                    token_id,
                    principal_id,
                    name,
                    description,
                    issued,
                    expires_at,
                    permission_scoped,
                    resource_scoped,
                    last_used_at,
                    revision,
                ))
                .first::<(
                    i32,
                    i32,
                    Option<String>,
                    Option<String>,
                    NaiveDateTime,
                    Option<NaiveDateTime>,
                    bool,
                    bool,
                    Option<NaiveDateTime>,
                    PostgresRevision,
                )>(conn)
                .await
                .optional()
        })
        .await?;

    let Some((
        id,
        principal_id_value,
        token_name,
        token_description,
        token_issued,
        token_expires_at,
        is_permission_scoped,
        is_resource_scoped,
        last_used,
        token_revision,
    )) = row
    else {
        return Err(PostgresStorageError::authentication_required(
            "Invalid token",
        ));
    };

    let throttle = chrono::Duration::seconds(LAST_USED_AT_THROTTLE_SECS);
    let mut observed_last_used = last_used;
    let last_used_is_stale = last_used
        .map(|previous| observed_at - previous >= throttle)
        .unwrap_or(true);
    if last_used_is_stale {
        let updated = runtime
            .with_connection(async move |conn| {
                diesel::update(tokens.filter(token_id.eq(id)))
                    .set(last_used_at.eq(observed_at))
                    .execute(conn)
                    .await
            })
            .await;
        if updated.is_ok() {
            observed_last_used = Some(observed_at);
        }
    }

    Ok(AuthenticatedToken::builder(
        hubuum_domain::TokenId::new(id)?,
        hubuum_domain::PrincipalId::new(principal_id_value)?,
        token_issued,
        token_revision.into_domain(),
    )
    .name(token_name)
    .description(token_description)
    .expires_at(token_expires_at)
    .last_used_at(observed_last_used)
    .permission_scoped(is_permission_scoped)
    .resource_scoped(is_resource_scoped)
    .build())
}

type AuthenticationIdentityRow = (
    i32,
    String,
    String,
    i32,
    Option<i32>,
    Option<String>,
    Option<String>,
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
);

pub async fn get_authentication_identity(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<AuthenticationIdentity, PostgresStorageError> {
    use crate::schema::{principals, users};

    let row = runtime
        .with_connection(async |conn| {
            principals::table
                .left_join(users::table.on(users::id.eq(principals::id)))
                .filter(principals::id.eq(principal_id))
                .select((
                    principals::id,
                    principals::kind,
                    principals::name,
                    principals::identity_scope_id,
                    users::id.nullable(),
                    users::proper_name.nullable(),
                    users::email.nullable(),
                    users::created_at.nullable(),
                    users::updated_at.nullable(),
                    users::anonymized_at.nullable(),
                ))
                .first::<AuthenticationIdentityRow>(conn)
                .await
        })
        .await?;

    authentication_identity_from_row(row)
}

fn authentication_identity_from_row(
    row: AuthenticationIdentityRow,
) -> Result<AuthenticationIdentity, PostgresStorageError> {
    let (
        principal_id,
        persisted_kind,
        name,
        identity_scope_id,
        human_id,
        proper_name,
        email,
        created_at,
        updated_at,
        anonymized_at,
    ) = row;
    let kind = match persisted_kind.as_str() {
        "human" => AuthenticationPrincipalKind::Human,
        "service_account" => AuthenticationPrincipalKind::ServiceAccount,
        other => {
            return Err(PostgresStorageError::database(format!(
                "Unknown principal kind '{other}'"
            )));
        }
    };
    let principal = AuthenticationPrincipal::new(
        PrincipalId::new(principal_id)?,
        kind,
        name,
        IdentityScopeId::new(identity_scope_id)?,
    );
    let human = human_id
        .map(|human_id| {
            if kind != AuthenticationPrincipalKind::Human || human_id != principal_id {
                return Err(PostgresStorageError::database(format!(
                    "Principal '{principal_id}' has an inconsistent human identity row"
                )));
            }
            let created_at = created_at.ok_or_else(|| {
                PostgresStorageError::database(format!(
                    "Human principal '{principal_id}' has no creation timestamp"
                ))
            })?;
            let updated_at = updated_at.ok_or_else(|| {
                PostgresStorageError::database(format!(
                    "Human principal '{principal_id}' has no update timestamp"
                ))
            })?;
            Ok(AuthenticationHuman::new(
                UserId::new(human_id)?,
                proper_name,
                email,
                created_at,
                updated_at,
                anonymized_at,
            ))
        })
        .transpose()?;

    Ok(AuthenticationIdentity::new(principal, human))
}

pub async fn get_authentication_token_scope(
    runtime: &PostgresRuntime,
    query: AuthenticationTokenScopeQuery,
) -> Result<Option<AuthenticationTokenScope>, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            super::token::load_token_scope(connection, query).await
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_persisted_principal_kind() {
        let timestamp = NaiveDateTime::default();
        let error = authentication_identity_from_row((
            1,
            "robot".to_string(),
            "bad-kind".to_string(),
            1,
            None,
            None,
            None,
            Some(timestamp),
            Some(timestamp),
            None,
        ))
        .unwrap_err();

        assert!(error.to_string().contains("Unknown principal kind 'robot'"));
    }
}
