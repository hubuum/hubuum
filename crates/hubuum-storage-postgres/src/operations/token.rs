//! PostgreSQL implementation of retained bearer-token lifecycle contracts.

use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::pg::Pg;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{BigInt, Nullable, Text, Timestamp};
use diesel::{
    BoolExpressionMethods, Insertable, Queryable, QueryableByName, Selectable, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId, ObjectId, TokenId, TokenIssuancePolicy};
use hubuum_events_core::{Action, AuditDocument, EntityType, EventContext, NewEvent};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    StorageAuditReceipt, StorageAuditReceipts, StorageAuthenticationResourceScope,
    StorageAuthenticationTokenScope, StorageAuthenticationTokenScopeQuery,
    StorageAuthorizationPermission, StorageMutationOutcome, StoragePage,
    StoragePrincipalTokensRevoke, StorageTokenCreate, StorageTokenDigest, StorageTokenFormat,
    StorageTokenHashAlgorithm, StorageTokenHashKeyId, StorageTokenHashRevoke, StorageTokenKeyUsage,
    StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata, StorageTokenObservation,
    StorageTokenRenew, StorageTokenRevoke,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::authentication::active_token_predicate;
use crate::operations::event_record::append_event;
use crate::revision::RevisionOwner;
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

const SERVICE_ACCOUNT_PRINCIPAL_KIND: &str = "service_account";

#[derive(QueryableByName)]
struct TokenKeyUsageRow {
    #[diesel(sql_type = Nullable<Text>)]
    token_hash_key_id: Option<String>,
    #[diesel(sql_type = BigInt)]
    active: i64,
    #[diesel(sql_type = BigInt)]
    revoked: i64,
    #[diesel(sql_type = BigInt)]
    expired: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    latest_validation: Option<NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    earliest_expiry: Option<NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    latest_expiry: Option<NaiveDateTime>,
}

fn permission_from_persisted(
    value: String,
) -> Result<StorageAuthorizationPermission, PostgresStorageError> {
    StorageAuthorizationPermission::from_name(&value).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("token permission scope", error)
    })
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::tokens)]
struct TokenRow {
    id: i32,
    principal_id: i32,
    name: Option<String>,
    description: Option<String>,
    issued: NaiveDateTime,
    expires_at: Option<NaiveDateTime>,
    last_used_at: Option<NaiveDateTime>,
    revoked_at: Option<NaiveDateTime>,
    permission_scoped: bool,
    resource_scoped: bool,
    revision: PostgresRevision,
    token_format: i16,
    token_hash_algorithm: i16,
    token_hash_key_id: Option<String>,
}

impl TokenRow {
    fn is_scoped(&self) -> bool {
        self.permission_scoped || self.resource_scoped
    }

    fn into_metadata(
        self,
        scope: Option<StorageAuthenticationTokenScope>,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, PostgresStorageError> {
        let token_format =
            StorageTokenFormat::from_persistence(self.token_format).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("token format", error)
            })?;
        StorageTokenHashAlgorithm::from_persistence(self.token_hash_algorithm).map_err(
            |error| PostgresStorageError::invalid_persisted_value("token hash algorithm", error),
        )?;
        let key_id = self
            .token_hash_key_id
            .as_deref()
            .map(StorageTokenHashKeyId::try_new)
            .transpose()
            .map_err(|error| {
                PostgresStorageError::invalid_persisted_value("token hash key ID", error)
            })?;
        if token_format == StorageTokenFormat::Version1 && key_id.is_none() {
            return Err(PostgresStorageError::database(
                "versioned token row is missing its hash key ID",
            ));
        }
        if self.is_scoped() != scope.is_some() {
            return Err(PostgresStorageError::database(format!(
                "Token {} has inconsistent scope flags and rows",
                self.id
            )));
        }
        let (observed_at, legacy_valid_after) = observation.into_parts();
        let observed_at = observed_at.naive_utc();
        let legacy_valid_after = legacy_valid_after.naive_utc();
        let expired = self
            .expires_at
            .map_or(self.issued <= legacy_valid_after, |expiry| {
                expiry <= observed_at
            });
        crate::validate_persisted(
            "token metadata",
            StorageTokenMetadata::builder(
                hubuum_domain::TokenId::new(self.id)?,
                hubuum_domain::PrincipalId::new(self.principal_id)?,
                self.issued.and_utc(),
                self.revision.into_domain(),
            )
            .name(self.name)
            .description(self.description)
            .expires_at(self.expires_at.map(|timestamp| timestamp.and_utc()))
            .last_used_at(self.last_used_at.map(|timestamp| timestamp.and_utc()))
            .revoked_at(self.revoked_at.map(|timestamp| timestamp.and_utc()))
            .active(self.revoked_at.is_none() && !expired)
            .expired(expired)
            .scope(scope)
            .try_build(),
        )
    }

    fn snapshot(&self, scope: Option<&StorageAuthenticationTokenScope>) -> Value {
        json!({
            "id": self.id,
            "principal_id": self.principal_id,
            "name": self.name,
            "description": self.description,
            "issued": self.issued,
            "expires_at": self.expires_at,
            "last_used_at": self.last_used_at,
            "revoked_at": self.revoked_at,
            "scoped": self.is_scoped(),
            "permission_scoped": self.permission_scoped,
            "resource_scoped": self.resource_scoped,
            "scope": scope.map(scope_snapshot),
            "revision": self.revision,
            "token_format": self.token_format,
            "token_hash_algorithm": self.token_hash_algorithm,
            "token_hash_key_id": self.token_hash_key_id,
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::token_scopes)]
struct NewTokenPermissionScope<'value> {
    token_id: i32,
    permission: &'value str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::token_collection_scopes)]
struct NewTokenCollectionScope {
    token_id: i32,
    collection_id: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::token_class_scopes)]
struct NewTokenClassScope {
    token_id: i32,
    class_id: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::token_object_scopes)]
struct NewTokenObjectScope {
    token_id: i32,
    object_id: i32,
}

type TokenQuery<'query> = crate::schema::tokens::BoxedQuery<'query, Pg>;

macro_rules! apply_token_filters {
    ($query:ident, $options:expr) => {
        for parameter in $options.filters() {
            match parameter.field {
                FilterField::IssuedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::tokens::issued
                ),
                FilterField::ExpiresAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::tokens::expires_at
                ),
                FilterField::LastUsedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::tokens::last_used_at
                ),
                FilterField::Name => {
                    crate::postgres_string_filter!($query, parameter, crate::schema::tokens::name)
                }
                FilterField::Revision => crate::postgres_revision_filter!(
                    $query,
                    parameter,
                    crate::schema::tokens::revision
                ),
                _ => {
                    return Err(PostgresStorageError::invalid_input(format!(
                        "Field '{}' isn't searchable (or does not exist) for tokens",
                        parameter.field
                    )));
                }
            }
        }
    };
}

pub async fn list_retained_tokens(
    runtime: &PostgresRuntime,
    query: StorageTokenListQuery,
) -> Result<StoragePage<StorageTokenMetadata>, PostgresStorageError> {
    let (principal_id, options, state, observation) = query.into_parts();
    let principal_id = principal_id.id();
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_read_only_snapshot(async move |connection| {
            let build_query = || -> Result<TokenQuery<'_>, PostgresStorageError> {
                build_token_query(principal_id, &options, state, observation)
            };
            let total = if options.include_total() {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = options
                .sort()
                .iter()
                .map(|sort| token_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(
                records,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    FilterField::Id,
                    false,
                    token_cursor_field(&FilterField::Id)?,
                )
            );
            let rows = records
                .select(TokenRow::as_select())
                .load::<TokenRow>(connection)
                .await?;
            let metadata = metadata_for_rows(connection, rows, observation).await?;
            crate::persisted_page(metadata, total)
        })
        .await
}

pub async fn create_token(
    runtime: &PostgresRuntime,
    request: StorageTokenCreate,
) -> Result<StorageMutationOutcome<StorageTokenMetadata>, PostgresStorageError> {
    let parts = request.into_parts();
    let principal_id = parts.principal_id().id();
    let digest = parts.digest().clone();
    let name = parts.name().map(str::to_string);
    let description = parts.description().map(str::to_string);
    let expires_at = parts.expires_at().map(|timestamp| timestamp.naive_utc());
    let scope = parts.scope().cloned();
    let policy = parts.policy();
    let event_context = parts.event_context().clone();
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_transaction(
            async move |connection| -> Result<
                StorageMutationOutcome<StorageTokenMetadata>,
                PostgresStorageError,
            > {
                let (token, audit) = create_token_row(
                    connection,
                    TokenCreateParts {
                        principal_id,
                        digest,
                        name,
                        description,
                        expires_at,
                        scope: scope.clone(),
                        policy,
                        event_context,
                        renewed_from_token_id: None,
                        principal_already_checked: false,
                    },
                )
                .await?;
                Ok(StorageMutationOutcome::committed(
                    created_metadata(token, scope)?,
                    audit,
                ))
            },
        )
        .await
}

pub async fn token_key_usage(
    runtime: &PostgresRuntime,
    observation: StorageTokenObservation,
) -> Result<Vec<StorageTokenKeyUsage>, PostgresStorageError> {
    let (observed_at, legacy_valid_after) = observation.into_parts();
    let observed_at = observed_at.naive_utc();
    let legacy_valid_after = legacy_valid_after.naive_utc();
    let rows = runtime
        .with_connection(async move |connection| {
            diesel::sql_query(
                "SELECT token_hash_key_id, \
                 COUNT(*) FILTER (WHERE revoked_at IS NULL AND \
                     (expires_at > $1 OR (expires_at IS NULL AND issued > $2))) AS active, \
                 COUNT(*) FILTER (WHERE revoked_at IS NOT NULL) AS revoked, \
                 COUNT(*) FILTER (WHERE revoked_at IS NULL AND NOT \
                     (expires_at > $1 OR (expires_at IS NULL AND issued > $2))) AS expired, \
                 MAX(last_used_at) AS latest_validation, \
                 MIN(expires_at) AS earliest_expiry, \
                 MAX(expires_at) AS latest_expiry \
                 FROM tokens GROUP BY token_hash_key_id ORDER BY token_hash_key_id NULLS FIRST",
            )
            .bind::<Timestamp, _>(observed_at)
            .bind::<Timestamp, _>(legacy_valid_after)
            .load::<TokenKeyUsageRow>(connection)
            .await
        })
        .await?;
    rows.into_iter()
        .map(|row| {
            let key_id = row
                .token_hash_key_id
                .map(StorageTokenHashKeyId::try_new)
                .transpose()
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("token hash key ID", error)
                })?;
            crate::validate_persisted(
                "token key usage",
                StorageTokenKeyUsage::try_new(
                    key_id,
                    row.active,
                    row.revoked,
                    row.expired,
                    row.latest_validation.map(|value| value.and_utc()),
                    row.earliest_expiry.map(|value| value.and_utc()),
                    row.latest_expiry.map(|value| value.and_utc()),
                ),
            )
        })
        .collect()
}

pub async fn renew_token(
    runtime: &PostgresRuntime,
    request: StorageTokenRenew,
) -> Result<StorageMutationOutcome<StorageTokenMetadata>, PostgresStorageError> {
    let (source_token_id, principal_id, digest, expires_at, policy, event_context) =
        request.into_parts();
    let expires_at = expires_at.map(|timestamp| timestamp.naive_utc());
    let source_token_id = source_token_id.id();
    let principal_id = principal_id.id();
    validate_positive_id(source_token_id, "source token id")?;
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_transaction(
            async move |connection| -> Result<
                StorageMutationOutcome<StorageTokenMetadata>,
                PostgresStorageError,
            > {
                // Disable takes the same service-account row lock before revoking
                // tokens. Preserve that lock order so renewal cannot race disable.
                ensure_principal_can_mint(connection, principal_id).await?;
                let source = crate::schema::tokens::table
                    .filter(crate::schema::tokens::id.eq(source_token_id))
                    .filter(crate::schema::tokens::principal_id.eq(principal_id))
                    .for_update()
                    .select(TokenRow::as_select())
                    .first::<TokenRow>(connection)
                    .await?;
                if source.revoked_at.is_some() {
                    return Err(PostgresStorageError::conflict(
                        "Revoked tokens cannot be renewed",
                    ));
                }
                let scope = load_token_scope(
                    connection,
                    StorageAuthenticationTokenScopeQuery::new(
                        TokenId::new(source.id)?,
                        source.permission_scoped,
                        source.resource_scoped,
                    ),
                )
                .await?;
                let (token, audit) = create_token_row(
                    connection,
                    TokenCreateParts {
                        principal_id,
                        digest,
                        name: source.name,
                        description: source.description,
                        expires_at,
                        scope: scope.clone(),
                        policy,
                        event_context,
                        renewed_from_token_id: Some(source.id),
                        principal_already_checked: true,
                    },
                )
                .await?;
                Ok(StorageMutationOutcome::committed(
                    created_metadata(token, scope)?,
                    audit,
                ))
            },
        )
        .await
}

pub async fn get_token_metadata(
    runtime: &PostgresRuntime,
    principal_id: i32,
    token_id: i32,
    observation: StorageTokenObservation,
) -> Result<StorageTokenMetadata, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    validate_positive_id(token_id, "token id")?;
    runtime
        .with_read_only_snapshot(async move |connection| {
            let row = crate::schema::tokens::table
                .filter(crate::schema::tokens::id.eq(token_id))
                .filter(crate::schema::tokens::principal_id.eq(principal_id))
                .select(TokenRow::as_select())
                .first::<TokenRow>(connection)
                .await?;
            metadata_for_row(connection, row, observation).await
        })
        .await
}

pub async fn load_token_metadata_by_ids(
    runtime: &PostgresRuntime,
    token_ids: Vec<i32>,
    observation: StorageTokenObservation,
) -> Result<Vec<StorageTokenMetadata>, PostgresStorageError> {
    if token_ids.is_empty() {
        return Ok(Vec::new());
    }
    for token_id in &token_ids {
        validate_positive_id(*token_id, "token id")?;
    }
    runtime
        .with_read_only_snapshot(async move |connection| {
            let rows = crate::schema::tokens::table
                .filter(crate::schema::tokens::id.eq_any(&token_ids))
                .select(TokenRow::as_select())
                .load::<TokenRow>(connection)
                .await?;
            let rows_by_id = rows
                .into_iter()
                .map(|token| (token.id, token))
                .collect::<HashMap<_, _>>();
            let ordered = token_ids
                .iter()
                .map(|token_id| {
                    rows_by_id.get(token_id).cloned().ok_or_else(|| {
                        PostgresStorageError::not_found(format!(
                            "Token {token_id} was purged before metadata could be loaded"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            metadata_for_rows(connection, ordered, observation).await
        })
        .await
}

pub async fn revoke_token(
    runtime: &PostgresRuntime,
    request: StorageTokenRevoke,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let (token_id, principal_id, event_context) = request.into_parts();
    let token_id = token_id.id();
    let principal_id = principal_id.id();
    validate_positive_id(token_id, "token id")?;
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_transaction(
            async move |connection| -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
                let before = crate::schema::tokens::table
                    .filter(crate::schema::tokens::id.eq(token_id))
                    .filter(crate::schema::tokens::principal_id.eq(principal_id))
                    .for_update()
                    .select(TokenRow::as_select())
                    .first::<TokenRow>(connection)
                    .await
                    .optional()?;
                let Some(before) = before else {
                    return Ok(StorageMutationOutcome::unchanged(0));
                };
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::Token.key(before.id),
                    before.revision,
                )
                .await?;
                if before.revoked_at.is_some() {
                    return Ok(StorageMutationOutcome::unchanged(1));
                }
                let scope = load_token_scope(
                    connection,
                    StorageAuthenticationTokenScopeQuery::new(
                        TokenId::new(before.id)?,
                        before.permission_scoped,
                        before.resource_scoped,
                    ),
                )
                .await?;
                let after = diesel::update(
                    crate::schema::tokens::table
                        .filter(crate::schema::tokens::id.eq(token_id))
                        .filter(crate::schema::tokens::principal_id.eq(principal_id))
                        .filter(crate::schema::tokens::revoked_at.is_null()),
                )
                .set(crate::schema::tokens::revoked_at.eq(diesel::dsl::now))
                .returning(TokenRow::as_returning())
                .get_result::<TokenRow>(connection)
                .await
                .optional()?;
                let Some(after) = after else {
                    return Ok(StorageMutationOutcome::unchanged(0));
                };
                let event = token_event(
                    &after,
                    Action::Revoked,
                    &event_context,
                    format!(
                        "Token {} revoked for principal {}",
                        after.id, after.principal_id
                    ),
                    None,
                    Some(before.snapshot(scope.as_ref())),
                    Some(after.snapshot(scope.as_ref())),
                )?;
                let audit = append_event(connection, &event).await?.into_audit_receipt();
                Ok(StorageMutationOutcome::committed(1, audit))
            },
        )
        .await
}

pub async fn revoke_token_by_hash(
    runtime: &PostgresRuntime,
    request: StorageTokenHashRevoke,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let (principal_id, credentials, event_context) = request.into_parts();
    let principal_id = principal_id.map(|principal_id| principal_id.id());
    if let Some(principal_id) = principal_id {
        validate_positive_id(principal_id, "principal id")?;
    }
    runtime
        .with_transaction(async move |connection| {
            let lookup_values = credentials
                .iter()
                .map(|credential| credential.lookup_value().to_string())
                .collect::<Vec<_>>();
            let mut candidates = crate::schema::tokens::table
                .filter(crate::schema::tokens::token.eq_any(lookup_values))
                .filter(crate::schema::tokens::revoked_at.is_null())
                .select((
                    crate::schema::tokens::id,
                    crate::schema::tokens::token,
                    crate::schema::tokens::token_format,
                    crate::schema::tokens::token_hash_algorithm,
                    crate::schema::tokens::token_hash_key_id,
                ))
                .for_update()
                .load::<(i32, String, i16, i16, Option<String>)>(connection)
                .await?
                .into_iter()
                .map(|candidate| {
                    let matches = credentials
                        .iter()
                        .map(|credential| {
                            crate::operations::authentication::persisted_credential_matches(
                                &candidate.1,
                                candidate.2,
                                candidate.3,
                                candidate.4.as_deref(),
                                credential,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .any(|matches| matches);
                    Ok(matches.then_some(candidate.0))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            if candidates.len() > 1 {
                return Err(PostgresStorageError::database(
                    "multiple token rows matched one bearer credential",
                ));
            }
            let Some(token_id) = candidates.pop() else {
                return Ok(StorageMutationOutcome::unchanged(0));
            };
            let before = crate::schema::tokens::table
                .filter(crate::schema::tokens::id.eq(token_id))
                .select(TokenRow::as_select())
                .first::<TokenRow>(connection)
                .await?;
            if principal_id.is_some_and(|principal_id| principal_id != before.principal_id) {
                return Ok(StorageMutationOutcome::unchanged(0));
            }
            let scope = load_token_scope(
                connection,
                StorageAuthenticationTokenScopeQuery::new(
                    TokenId::new(before.id)?,
                    before.permission_scoped,
                    before.resource_scoped,
                ),
            )
            .await?;
            let after = diesel::update(
                crate::schema::tokens::table.filter(crate::schema::tokens::id.eq(before.id)),
            )
            .set(crate::schema::tokens::revoked_at.eq(diesel::dsl::now))
            .returning(TokenRow::as_returning())
            .get_result::<TokenRow>(connection)
            .await?;
            let event = token_event(
                &after,
                Action::Revoked,
                &event_context,
                format!(
                    "Token {} revoked for principal {}",
                    after.id, after.principal_id
                ),
                None,
                Some(before.snapshot(scope.as_ref())),
                Some(after.snapshot(scope.as_ref())),
            )?;
            let audit = append_event(connection, &event).await?.into_audit_receipt();
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(1, audit))
        })
        .await
}

pub async fn revoke_all_principal_tokens(
    runtime: &PostgresRuntime,
    request: StoragePrincipalTokensRevoke,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let (principal_id, event_context) = request.into_parts();
    let principal_id = principal_id.id();
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_transaction(async move |connection| {
            let before = crate::schema::tokens::table
                .filter(crate::schema::tokens::principal_id.eq(principal_id))
                .filter(crate::schema::tokens::revoked_at.is_null())
                .for_update()
                .select(TokenRow::as_select())
                .load::<TokenRow>(connection)
                .await?;
            if before.is_empty() {
                return Ok(StorageMutationOutcome::unchanged(0));
            }
            let mut scopes = HashMap::with_capacity(before.len());
            for token in &before {
                let scope = load_token_scope(
                    connection,
                    StorageAuthenticationTokenScopeQuery::new(
                        TokenId::new(token.id)?,
                        token.permission_scoped,
                        token.resource_scoped,
                    ),
                )
                .await?;
                scopes.insert(token.id, scope);
            }
            let token_ids = before.iter().map(|token| token.id).collect::<Vec<_>>();
            let mut after = diesel::update(
                crate::schema::tokens::table.filter(crate::schema::tokens::id.eq_any(token_ids)),
            )
            .set(crate::schema::tokens::revoked_at.eq(diesel::dsl::now))
            .returning(TokenRow::as_returning())
            .get_results::<TokenRow>(connection)
            .await?;
            after.sort_unstable_by_key(|token| token.id);
            let before_by_id = before
                .into_iter()
                .map(|token| (token.id, token))
                .collect::<HashMap<_, _>>();
            let mut audits = Vec::with_capacity(after.len());
            for token in &after {
                let before = before_by_id.get(&token.id).ok_or_else(|| {
                    PostgresStorageError::database("bulk token revocation lost its locked row")
                })?;
                let scope = scopes.get(&token.id).and_then(Option::as_ref);
                let event = token_event(
                    token,
                    Action::Revoked,
                    &event_context,
                    format!(
                        "Token {} revoked for principal {}",
                        token.id, token.principal_id
                    ),
                    None,
                    Some(before.snapshot(scope)),
                    Some(token.snapshot(scope)),
                )?;
                audits.push(append_event(connection, &event).await?.into_audit_receipt());
            }
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed_with_audits(
                after.len(),
                crate::validate_persisted(
                    "token revocation audit receipts",
                    StorageAuditReceipts::try_from_vec(audits),
                )?,
            ))
        })
        .await
}

pub async fn revoke_all_principal_tokens_on_connection(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<usize, PostgresStorageError> {
    diesel::update(
        crate::schema::tokens::table
            .filter(crate::schema::tokens::principal_id.eq(principal_id))
            .filter(crate::schema::tokens::revoked_at.is_null()),
    )
    .set(crate::schema::tokens::revoked_at.eq(diesel::dsl::now))
    .execute(connection)
    .await
    .map_err(PostgresStorageError::from)
}

pub(crate) async fn load_token_scope(
    connection: &mut PostgresConnection,
    query: StorageAuthenticationTokenScopeQuery,
) -> Result<Option<StorageAuthenticationTokenScope>, PostgresStorageError> {
    if !query.is_scoped() {
        return Ok(None);
    }
    let token_id = query.token_id().id();
    let permissions = if query.is_permission_scoped() {
        Some(
            crate::schema::token_scopes::table
                .filter(crate::schema::token_scopes::token_id.eq(token_id))
                .order_by(crate::schema::token_scopes::permission.asc())
                .select(crate::schema::token_scopes::permission)
                .load::<String>(connection)
                .await?
                .into_iter()
                .map(permission_from_persisted)
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        None
    };
    let resources = if query.is_resource_scoped() {
        let collection_ids = crate::schema::token_collection_scopes::table
            .filter(crate::schema::token_collection_scopes::token_id.eq(token_id))
            .order_by(crate::schema::token_collection_scopes::collection_id.asc())
            .select(crate::schema::token_collection_scopes::collection_id)
            .load::<i32>(connection)
            .await?;
        let class_ids = crate::schema::token_class_scopes::table
            .filter(crate::schema::token_class_scopes::token_id.eq(token_id))
            .order_by(crate::schema::token_class_scopes::class_id.asc())
            .select(crate::schema::token_class_scopes::class_id)
            .load::<i32>(connection)
            .await?;
        let object_ids = crate::schema::token_object_scopes::table
            .filter(crate::schema::token_object_scopes::token_id.eq(token_id))
            .order_by(crate::schema::token_object_scopes::object_id.asc())
            .select(crate::schema::token_object_scopes::object_id)
            .load::<i32>(connection)
            .await?;
        Some(StorageAuthenticationResourceScope::new(
            collection_ids
                .into_iter()
                .map(hubuum_domain::CollectionId::new)
                .collect::<Result<Vec<_>, _>>()?,
            class_ids
                .into_iter()
                .map(hubuum_domain::ClassId::new)
                .collect::<Result<Vec<_>, _>>()?,
            object_ids
                .into_iter()
                .map(hubuum_domain::ObjectId::new)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        None
    };
    Ok(Some(StorageAuthenticationTokenScope::new(
        permissions,
        resources,
    )))
}

struct TokenCreateParts {
    principal_id: i32,
    digest: StorageTokenDigest,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<NaiveDateTime>,
    scope: Option<StorageAuthenticationTokenScope>,
    policy: hubuum_storage_core::StorageTokenIssuancePolicy,
    event_context: EventContext,
    renewed_from_token_id: Option<i32>,
    principal_already_checked: bool,
}

async fn create_token_row(
    connection: &mut PostgresConnection,
    parts: TokenCreateParts,
) -> Result<(TokenRow, StorageAuditReceipt), PostgresStorageError> {
    let TokenCreateParts {
        principal_id,
        digest,
        name,
        description,
        expires_at,
        scope,
        policy,
        event_context,
        renewed_from_token_id,
        principal_already_checked,
    } = parts;
    let (token_hash, token_format, token_hash_algorithm, token_hash_key_id) = digest.into_parts();
    let token_hash_key_id = token_hash_key_id.map(|id| id.to_string());
    let (permissions, resources) = scope
        .clone()
        .map(StorageAuthenticationTokenScope::into_parts)
        .unwrap_or((None, None));
    let permission_scoped = permissions.is_some();
    let resource_scoped = resources.is_some();
    let (collection_ids, class_ids, object_ids) = resources
        .map(StorageAuthenticationResourceScope::into_parts)
        .unwrap_or_default();
    let collection_ids = collection_ids
        .into_iter()
        .map(|id| id.id())
        .collect::<Vec<_>>();
    let class_ids = class_ids.into_iter().map(|id| id.id()).collect::<Vec<_>>();
    let object_ids = object_ids.into_iter().map(|id| id.id()).collect::<Vec<_>>();
    let issued_at = diesel::select(diesel::dsl::sql::<Timestamp>(
        "statement_timestamp() AT TIME ZONE 'UTC'",
    ))
    .get_result::<NaiveDateTime>(connection)
    .await?;
    let (default_hours, maximum_hours) = policy.into_parts();
    let policy = TokenIssuancePolicy::from_hours(default_hours, maximum_hours)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let effective_expiry = policy
        .resolve_expiry(issued_at, expires_at)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;

    if !principal_already_checked {
        ensure_principal_can_mint(connection, principal_id).await?;
    }
    validate_resource_ids(connection, &collection_ids, &class_ids, &object_ids).await?;

    let token = diesel::insert_into(crate::schema::tokens::table)
        .values((
            crate::schema::tokens::token.eq(token_hash),
            crate::schema::tokens::principal_id.eq(principal_id),
            crate::schema::tokens::name.eq(&name),
            crate::schema::tokens::description.eq(&description),
            crate::schema::tokens::issued.eq(issued_at),
            crate::schema::tokens::expires_at.eq(Some(effective_expiry)),
            crate::schema::tokens::permission_scoped.eq(permission_scoped),
            crate::schema::tokens::resource_scoped.eq(resource_scoped),
            crate::schema::tokens::token_format.eq(token_format.persistence_value()),
            crate::schema::tokens::token_hash_algorithm
                .eq(token_hash_algorithm.persistence_value()),
            crate::schema::tokens::token_hash_key_id.eq(token_hash_key_id),
        ))
        .returning(TokenRow::as_returning())
        .get_result::<TokenRow>(connection)
        .await?;
    insert_scope_rows(
        connection,
        token.id,
        permissions.as_deref().unwrap_or_default(),
        &collection_ids,
        &class_ids,
        &object_ids,
    )
    .await?;

    let event = token_event(
        &token,
        Action::Created,
        &event_context,
        format!(
            "Token {} created for principal {}",
            token.id, token.principal_id
        ),
        renewed_from_token_id,
        None,
        Some(token.snapshot(scope.as_ref())),
    )?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok((token, audit))
}

async fn ensure_principal_can_mint(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<(), PostgresStorageError> {
    let kind = crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::kind)
        .first::<String>(connection)
        .await?;
    if kind == SERVICE_ACCOUNT_PRINCIPAL_KIND {
        let disabled_at = crate::schema::service_accounts::table
            .filter(crate::schema::service_accounts::id.eq(principal_id))
            .for_update()
            .select(crate::schema::service_accounts::disabled_at)
            .first::<Option<NaiveDateTime>>(connection)
            .await?;
        if disabled_at.is_some() {
            return Err(PostgresStorageError::conflict(
                "Service account is disabled",
            ));
        }
    }
    Ok(())
}

async fn validate_resource_ids(
    connection: &mut PostgresConnection,
    collection_ids: &[i32],
    class_ids: &[i32],
    object_ids: &[i32],
) -> Result<(), PostgresStorageError> {
    if !collection_ids.is_empty() {
        let found = crate::schema::collections::table
            .filter(crate::schema::collections::id.eq_any(collection_ids))
            .count()
            .get_result::<i64>(connection)
            .await?;
        if found != collection_ids.len() as i64 {
            return Err(PostgresStorageError::invalid_input(
                "scope.resources contains an unknown collection id",
            ));
        }
    }
    if !class_ids.is_empty() {
        let found = crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq_any(class_ids))
            .count()
            .get_result::<i64>(connection)
            .await?;
        if found != class_ids.len() as i64 {
            return Err(PostgresStorageError::invalid_input(
                "scope.resources contains an unknown class id",
            ));
        }
    }
    if !object_ids.is_empty() {
        let found = crate::schema::hubuumobject::table
            .filter(crate::schema::hubuumobject::id.eq_any(object_ids))
            .count()
            .get_result::<i64>(connection)
            .await?;
        if found != object_ids.len() as i64 {
            return Err(PostgresStorageError::invalid_input(
                "scope.resources contains an unknown object id",
            ));
        }
    }
    Ok(())
}

async fn insert_scope_rows(
    connection: &mut PostgresConnection,
    token_id: i32,
    permissions: &[StorageAuthorizationPermission],
    collection_ids: &[i32],
    class_ids: &[i32],
    object_ids: &[i32],
) -> Result<(), PostgresStorageError> {
    if !permissions.is_empty() {
        let rows = permissions
            .iter()
            .map(|permission| NewTokenPermissionScope {
                token_id,
                permission: permission.as_str(),
            })
            .collect::<Vec<_>>();
        diesel::insert_into(crate::schema::token_scopes::table)
            .values(rows)
            .execute(connection)
            .await?;
    }
    if !collection_ids.is_empty() {
        let rows = collection_ids
            .iter()
            .map(|collection_id| NewTokenCollectionScope {
                token_id,
                collection_id: *collection_id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(crate::schema::token_collection_scopes::table)
            .values(rows)
            .execute(connection)
            .await?;
    }
    if !class_ids.is_empty() {
        let rows = class_ids
            .iter()
            .map(|class_id| NewTokenClassScope {
                token_id,
                class_id: *class_id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(crate::schema::token_class_scopes::table)
            .values(rows)
            .execute(connection)
            .await?;
    }
    if !object_ids.is_empty() {
        let rows = object_ids
            .iter()
            .map(|object_id| NewTokenObjectScope {
                token_id,
                object_id: *object_id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(crate::schema::token_object_scopes::table)
            .values(rows)
            .execute(connection)
            .await?;
    }
    Ok(())
}

async fn metadata_for_rows(
    connection: &mut PostgresConnection,
    rows: Vec<TokenRow>,
    observation: StorageTokenObservation,
) -> Result<Vec<StorageTokenMetadata>, PostgresStorageError> {
    let scopes = load_token_scopes_for_rows(connection, &rows).await?;
    rows.into_iter()
        .zip(scopes)
        .map(|(row, scope)| row.into_metadata(scope, observation))
        .collect()
}

async fn metadata_for_row(
    connection: &mut PostgresConnection,
    row: TokenRow,
    observation: StorageTokenObservation,
) -> Result<StorageTokenMetadata, PostgresStorageError> {
    metadata_for_rows(connection, vec![row], observation)
        .await?
        .pop()
        .ok_or_else(|| PostgresStorageError::internal("Token metadata projection returned no row"))
}

#[derive(Clone, Default)]
struct TokenScopeRows {
    permissions: Vec<String>,
    collection_ids: Vec<i32>,
    class_ids: Vec<i32>,
    object_ids: Vec<i32>,
}

async fn load_token_scopes_for_rows(
    connection: &mut PostgresConnection,
    rows: &[TokenRow],
) -> Result<Vec<Option<StorageAuthenticationTokenScope>>, PostgresStorageError> {
    let mut permission_token_ids = rows
        .iter()
        .filter(|row| row.permission_scoped)
        .map(|row| row.id)
        .collect::<Vec<_>>();
    permission_token_ids.sort_unstable();
    permission_token_ids.dedup();
    let mut resource_token_ids = rows
        .iter()
        .filter(|row| row.resource_scoped)
        .map(|row| row.id)
        .collect::<Vec<_>>();
    resource_token_ids.sort_unstable();
    resource_token_ids.dedup();

    let permissions = if permission_token_ids.is_empty() {
        Vec::new()
    } else {
        crate::schema::token_scopes::table
            .filter(crate::schema::token_scopes::token_id.eq_any(&permission_token_ids))
            .order_by((
                crate::schema::token_scopes::token_id.asc(),
                crate::schema::token_scopes::permission.asc(),
            ))
            .select((
                crate::schema::token_scopes::token_id,
                crate::schema::token_scopes::permission,
            ))
            .load::<(i32, String)>(connection)
            .await?
    };
    let collection_ids = if resource_token_ids.is_empty() {
        Vec::new()
    } else {
        crate::schema::token_collection_scopes::table
            .filter(crate::schema::token_collection_scopes::token_id.eq_any(&resource_token_ids))
            .order_by((
                crate::schema::token_collection_scopes::token_id.asc(),
                crate::schema::token_collection_scopes::collection_id.asc(),
            ))
            .select((
                crate::schema::token_collection_scopes::token_id,
                crate::schema::token_collection_scopes::collection_id,
            ))
            .load::<(i32, i32)>(connection)
            .await?
    };
    let class_ids = if resource_token_ids.is_empty() {
        Vec::new()
    } else {
        crate::schema::token_class_scopes::table
            .filter(crate::schema::token_class_scopes::token_id.eq_any(&resource_token_ids))
            .order_by((
                crate::schema::token_class_scopes::token_id.asc(),
                crate::schema::token_class_scopes::class_id.asc(),
            ))
            .select((
                crate::schema::token_class_scopes::token_id,
                crate::schema::token_class_scopes::class_id,
            ))
            .load::<(i32, i32)>(connection)
            .await?
    };
    let object_ids = if resource_token_ids.is_empty() {
        Vec::new()
    } else {
        crate::schema::token_object_scopes::table
            .filter(crate::schema::token_object_scopes::token_id.eq_any(&resource_token_ids))
            .order_by((
                crate::schema::token_object_scopes::token_id.asc(),
                crate::schema::token_object_scopes::object_id.asc(),
            ))
            .select((
                crate::schema::token_object_scopes::token_id,
                crate::schema::token_object_scopes::object_id,
            ))
            .load::<(i32, i32)>(connection)
            .await?
    };

    let mut by_token = rows
        .iter()
        .filter(|row| row.is_scoped())
        .map(|row| (row.id, TokenScopeRows::default()))
        .collect::<HashMap<_, _>>();
    for (token_id, permission) in permissions {
        if let Some(scope) = by_token.get_mut(&token_id) {
            scope.permissions.push(permission);
        }
    }
    for (token_id, collection_id) in collection_ids {
        if let Some(scope) = by_token.get_mut(&token_id) {
            scope.collection_ids.push(collection_id);
        }
    }
    for (token_id, class_id) in class_ids {
        if let Some(scope) = by_token.get_mut(&token_id) {
            scope.class_ids.push(class_id);
        }
    }
    for (token_id, object_id) in object_ids {
        if let Some(scope) = by_token.get_mut(&token_id) {
            scope.object_ids.push(object_id);
        }
    }

    rows.iter()
        .map(|row| {
            if !row.is_scoped() {
                return Ok(None);
            }
            let scope = by_token.get(&row.id).cloned().unwrap_or_default();
            let permissions = row
                .permission_scoped
                .then(|| {
                    scope
                        .permissions
                        .into_iter()
                        .map(permission_from_persisted)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
            let resources = row
                .resource_scoped
                .then_some(StorageAuthenticationResourceScope::new(
                    scope
                        .collection_ids
                        .into_iter()
                        .map(CollectionId::new)
                        .collect::<Result<Vec<_>, _>>()?,
                    scope
                        .class_ids
                        .into_iter()
                        .map(ClassId::new)
                        .collect::<Result<Vec<_>, _>>()?,
                    scope
                        .object_ids
                        .into_iter()
                        .map(ObjectId::new)
                        .collect::<Result<Vec<_>, _>>()?,
                ));
            Ok(Some(StorageAuthenticationTokenScope::new(
                permissions,
                resources,
            )))
        })
        .collect()
}

fn created_metadata(
    token: TokenRow,
    scope: Option<StorageAuthenticationTokenScope>,
) -> Result<StorageTokenMetadata, PostgresStorageError> {
    let observed_at = token.issued;
    let observation =
        StorageTokenObservation::try_new(observed_at.and_utc(), observed_at.and_utc())
            .map_err(|error| PostgresStorageError::internal(error.to_string()))?;
    token.into_metadata(scope, observation)
}

fn build_token_query<'query>(
    principal_id: i32,
    options: &'query QueryOptions,
    state: StorageTokenListState,
    observation: StorageTokenObservation,
) -> Result<TokenQuery<'query>, PostgresStorageError> {
    let (observed_at, legacy_valid_after) = observation.into_parts();
    let observed_at = observed_at.naive_utc();
    let legacy_valid_after = legacy_valid_after.naive_utc();
    let mut records = crate::schema::tokens::table
        .filter(crate::schema::tokens::principal_id.eq(principal_id))
        .into_boxed();
    records = match state {
        StorageTokenListState::Active => {
            records.filter(active_token_predicate(observed_at, legacy_valid_after))
        }
        StorageTokenListState::Expired => records.filter(
            crate::schema::tokens::expires_at
                .le(observed_at)
                .or(crate::schema::tokens::expires_at
                    .is_null()
                    .and(crate::schema::tokens::issued.le(legacy_valid_after))),
        ),
        StorageTokenListState::Revoked => {
            records.filter(crate::schema::tokens::revoked_at.is_not_null())
        }
        StorageTokenListState::All => records,
    };
    apply_token_filters!(records, options);
    Ok(records)
}

fn token_event(
    token: &TokenRow,
    action: Action,
    context: &EventContext,
    summary: String,
    renewed_from_token_id: Option<i32>,
    before: Option<Value>,
    after: Option<Value>,
) -> Result<NewEvent, PostgresStorageError> {
    let mut metadata = json!({
        "principal_id": token.principal_id,
        "scoped": token.is_scoped(),
        "permission_scoped": token.permission_scoped,
        "resource_scoped": token.resource_scoped,
    });
    if let Some(source_id) = renewed_from_token_id {
        metadata["renewed_from_token_id"] = json!(source_id);
    }
    let document = AuditDocument::try_new(summary, before, after, metadata)?;
    NewEvent::from_document(EntityType::Token, action, context.actor_kind(), document)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
        .and_then(|event| {
            Ok(event
                .with_context(context)
                .with_entity_id(hubuum_events_core::EventEntityId::new(token.id)?)
                .with_entity_name(token.name.clone().unwrap_or_else(|| token.id.to_string())))
        })
}

fn scope_snapshot(scope: &StorageAuthenticationTokenScope) -> Value {
    let (permissions, resources) = scope.clone().into_parts();
    let permissions = permissions.map(|permissions| {
        permissions
            .into_iter()
            .map(StorageAuthorizationPermission::as_str)
            .collect::<Vec<_>>()
    });
    let resources = resources.map(|resources| {
        let (collections, classes, objects) = resources.into_parts();
        collections
            .into_iter()
            .map(|id| json!({"kind": "collection", "id": id}))
            .chain(
                classes
                    .into_iter()
                    .map(|id| json!({"kind": "class", "id": id})),
            )
            .chain(
                objects
                    .into_iter()
                    .map(|id| json!({"kind": "object", "id": id})),
            )
            .collect::<Vec<_>>()
    });
    json!({
        "permissions": permissions,
        "resources": resources,
    })
}

fn token_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("tokens.id", CursorSqlType::Integer, false),
        FilterField::Name => cursor_field("tokens.name", CursorSqlType::String, true),
        FilterField::IssuedAt => cursor_field("tokens.issued", CursorSqlType::DateTime, false),
        FilterField::ExpiresAt => cursor_field("tokens.expires_at", CursorSqlType::DateTime, true),
        FilterField::LastUsedAt => {
            cursor_field("tokens.last_used_at", CursorSqlType::DateTime, true)
        }
        FilterField::Revision => cursor_field("tokens.revision", CursorSqlType::BigInt, false),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for tokens"
            )));
        }
    })
}

const fn cursor_field(
    column: &'static str,
    sql_type: CursorSqlType,
    nullable: bool,
) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable,
    }
}

fn validate_positive_id(id: i32, field: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{field} must be greater than zero"
        )))
    }
}
