use chrono::NaiveDateTime;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Nullable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{IdentityScopeId, PrincipalId, PrincipalKind, UserId};
use hubuum_storage_core::{
    StorageAuthenticatedToken, StorageAuthenticationAttempt, StorageAuthenticationCredential,
    StorageAuthenticationHuman, StorageAuthenticationIdentity, StorageAuthenticationPrincipal,
    StorageAuthenticationTokenScope, StorageAuthenticationTokenScopeQuery, StorageTokenFormat,
    StorageTokenHashAlgorithm, StorageTokenHashKeyId, StorageTokenMigrationOutcome,
};

use crate::schema;
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

/// Advance `last_used_at` at most this often. Routine authenticated requests
/// stay read-only on the hot path, and a telemetry write failure never rejects
/// an otherwise valid credential.
const LAST_USED_AT_THROTTLE_SECS: i64 = 60;

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::tokens)]
struct AuthenticationTokenRow {
    id: i32,
    token: String,
    principal_id: i32,
    name: Option<String>,
    description: Option<String>,
    issued: NaiveDateTime,
    expires_at: Option<NaiveDateTime>,
    last_used_at: Option<NaiveDateTime>,
    permission_scoped: bool,
    resource_scoped: bool,
    revision: PostgresRevision,
    token_format: i16,
    token_hash_algorithm: i16,
    token_hash_key_id: Option<String>,
}

impl AuthenticationTokenRow {
    fn matches(
        &self,
        credential: &StorageAuthenticationCredential,
    ) -> Result<bool, PostgresStorageError> {
        persisted_credential_matches(
            &self.token,
            self.token_format,
            self.token_hash_algorithm,
            self.token_hash_key_id.as_deref(),
            credential,
        )
    }
}

pub(crate) fn persisted_credential_matches(
    token_hash: &str,
    token_format: i16,
    token_hash_algorithm: i16,
    token_hash_key_id: Option<&str>,
    credential: &StorageAuthenticationCredential,
) -> Result<bool, PostgresStorageError> {
    let persisted_format = StorageTokenFormat::from_persistence(token_format)
        .map_err(|error| PostgresStorageError::invalid_persisted_value("token format", error))?;
    let persisted_algorithm = StorageTokenHashAlgorithm::from_persistence(token_hash_algorithm)
        .map_err(|error| {
            PostgresStorageError::invalid_persisted_value("token hash algorithm", error)
        })?;
    let persisted_key_id = token_hash_key_id
        .map(StorageTokenHashKeyId::try_new)
        .transpose()
        .map_err(|error| {
            PostgresStorageError::invalid_persisted_value("token hash key ID", error)
        })?;
    let candidate = credential.digest();
    if !candidate.matches_lookup_value(token_hash)
        || persisted_format != candidate.format()
        || persisted_algorithm != candidate.algorithm()
    {
        return Ok(false);
    }
    Ok(match persisted_format {
        StorageTokenFormat::Version1 => persisted_key_id.as_ref() == candidate.key_id(),
        StorageTokenFormat::Legacy => {
            persisted_key_id.is_none()
                || candidate.key_id().is_none()
                || persisted_key_id.as_ref() == candidate.key_id()
        }
    })
}

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
    attempt: StorageAuthenticationAttempt,
) -> Result<StorageAuthenticatedToken, PostgresStorageError> {
    use crate::schema::service_accounts;
    use crate::schema::tokens::dsl::{
        id as token_id, last_used_at, token, token_format, token_hash_algorithm, token_hash_key_id,
        tokens,
    };

    let (credentials, migration_target, observed_at, legacy_valid_after) = attempt.into_parts();
    let observed_at = observed_at.naive_utc();
    let legacy_valid_after = legacy_valid_after.naive_utc();
    let lookup_values = credentials
        .iter()
        .map(|credential| credential.lookup_value().to_string())
        .collect::<Vec<_>>();
    let rows = runtime
        .with_connection(async move |conn| {
            tokens
                .filter(token.eq_any(lookup_values))
                .filter(active_token_predicate(observed_at, legacy_valid_after))
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    service_accounts::table
                        .filter(service_accounts::id.eq(crate::schema::tokens::principal_id))
                        .filter(service_accounts::disabled_at.is_not_null()),
                )))
                .select(AuthenticationTokenRow::as_select())
                .load::<AuthenticationTokenRow>(conn)
                .await
        })
        .await?;
    let mut matching = rows
        .into_iter()
        .map(|row| {
            let matches = credentials
                .iter()
                .map(|credential| row.matches(credential))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .any(|matches| matches);
            Ok(matches.then_some(row))
        })
        .collect::<Result<Vec<_>, PostgresStorageError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(PostgresStorageError::database(
            "multiple token rows matched one bearer credential",
        ));
    }
    let Some(row) = matching.pop() else {
        return Err(PostgresStorageError::authentication_required(
            "Invalid token",
        ));
    };

    let mut migration_outcome = StorageTokenMigrationOutcome::NotNeeded;
    if row.token_format == StorageTokenFormat::Legacy.persistence_value()
        && row.token_hash_key_id.is_none()
        && let Some(target) = migration_target
    {
        let (target_hash, target_format, target_algorithm, target_key_id) = target.into_parts();
        let target_key_id = target_key_id.map(|id| id.to_string());
        let matched_hash = row.token.clone();
        let id = row.id;
        let migration = runtime
            .with_connection(async move |conn| {
                diesel::update(
                    tokens
                        .filter(token_id.eq(id))
                        .filter(token.eq(matched_hash))
                        .filter(token_format.eq(StorageTokenFormat::Legacy.persistence_value()))
                        .filter(token_hash_key_id.is_null())
                        .filter(active_token_predicate(observed_at, legacy_valid_after)),
                )
                .set((
                    token.eq(target_hash),
                    token_format.eq(target_format.persistence_value()),
                    token_hash_algorithm.eq(target_algorithm.persistence_value()),
                    token_hash_key_id.eq(target_key_id),
                ))
                .execute(conn)
                .await
            })
            .await;
        match migration {
            Ok(1) => migration_outcome = StorageTokenMigrationOutcome::Migrated,
            Ok(_) => migration_outcome = StorageTokenMigrationOutcome::Conflict,
            Err(_) => {
                migration_outcome = StorageTokenMigrationOutcome::Conflict;
                tracing::warn!(
                    message = "Legacy token digest migration did not complete",
                    outcome = "storage_conflict"
                );
            }
        }
    }

    let throttle = chrono::Duration::seconds(LAST_USED_AT_THROTTLE_SECS);
    let mut observed_last_used = row.last_used_at;
    let last_used_is_stale = row
        .last_used_at
        .map(|previous| observed_at - previous >= throttle)
        .unwrap_or(true);
    if last_used_is_stale {
        let id = row.id;
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

    crate::validate_persisted(
        "authenticated token",
        StorageAuthenticatedToken::builder(
            hubuum_domain::TokenId::new(row.id)?,
            hubuum_domain::PrincipalId::new(row.principal_id)?,
            row.issued.and_utc(),
            row.revision.into_domain(),
        )
        .name(row.name)
        .description(row.description)
        .expires_at(row.expires_at.map(|timestamp| timestamp.and_utc()))
        .last_used_at(observed_last_used.map(|timestamp| timestamp.and_utc()))
        .permission_scoped(row.permission_scoped)
        .resource_scoped(row.resource_scoped)
        .migration_outcome(migration_outcome)
        .try_build(),
    )
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
) -> Result<StorageAuthenticationIdentity, PostgresStorageError> {
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
) -> Result<StorageAuthenticationIdentity, PostgresStorageError> {
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
    let kind = persisted_kind
        .parse::<PrincipalKind>()
        .map_err(|error| PostgresStorageError::database(error.to_string()))?;
    let principal = StorageAuthenticationPrincipal::new(
        PrincipalId::new(principal_id)?,
        kind,
        name,
        IdentityScopeId::new(identity_scope_id)?,
    );
    let human = human_id
        .map(|human_id| {
            if kind != PrincipalKind::Human || human_id != principal_id {
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
            crate::validate_persisted(
                "authentication human",
                StorageAuthenticationHuman::try_new(
                    UserId::new(human_id)?,
                    proper_name,
                    email,
                    created_at.and_utc(),
                    updated_at.and_utc(),
                    anonymized_at.map(|timestamp| timestamp.and_utc()),
                ),
            )
        })
        .transpose()?;

    crate::validate_persisted(
        "authentication identity",
        StorageAuthenticationIdentity::try_new(principal, human),
    )
}

pub async fn get_authentication_token_scope(
    runtime: &PostgresRuntime,
    query: StorageAuthenticationTokenScopeQuery,
) -> Result<Option<StorageAuthenticationTokenScope>, PostgresStorageError> {
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
