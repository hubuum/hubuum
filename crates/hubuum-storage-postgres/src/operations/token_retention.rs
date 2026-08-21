use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};
use diesel::sql_types::{BigInt, Bool, Integer, Timestamp};
use diesel::{ExpressionMethods, QueryDsl, Queryable, QueryableByName};
use diesel_async::RunQueryDsl;
use hubuum_domain::MAX_TOKEN_RESOURCE_SCOPES;
use hubuum_events_core::{Action, ActorKind, EntityType, NewEvent};
use hubuum_storage_core::{AuthorizationPermission, StorageError};

use crate::operations::event_record::append_events;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

const TOKEN_RETENTION_LOCK_KEY: i64 = 4_850_188_191_125_219;
#[derive(Debug, QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

#[derive(Debug, QueryableByName)]
struct TokenRetentionCandidate {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(Queryable)]
struct RetainedTokenRow {
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
}

impl RetainedTokenRow {
    const fn is_scoped(&self) -> bool {
        self.permission_scoped || self.resource_scoped
    }
}

#[derive(Debug, Clone, Copy)]
enum TokenRetentionBasis {
    Revocation,
    ExplicitExpiry,
    ImplicitExpiry,
}

impl TokenRetentionBasis {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Revocation => "revocation",
            Self::ExplicitExpiry => "explicit_expiry",
            Self::ImplicitExpiry => "implicit_expiry",
        }
    }
}

#[derive(Clone, Default)]
struct StoredTokenScopeRows {
    permissions: Vec<String>,
    collection_ids: Vec<i32>,
    class_ids: Vec<i32>,
    object_ids: Vec<i32>,
}

impl StoredTokenScopeRows {
    fn snapshot(self, token: &RetainedTokenRow) -> Result<serde_json::Value, PostgresStorageError> {
        let permissions = token
            .permission_scoped
            .then(|| {
                self.permissions
                    .iter()
                    .map(|permission| {
                        AuthorizationPermission::from_name(permission)
                            .map(|permission| permission.as_str())
                            .map_err(storage_error_to_postgres)
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        let resources = if token.resource_scoped {
            let resource_count =
                self.collection_ids.len() + self.class_ids.len() + self.object_ids.len();
            if resource_count > MAX_TOKEN_RESOURCE_SCOPES {
                return Err(PostgresStorageError::internal(format!(
                    "Stored token resource scope exceeds the {MAX_TOKEN_RESOURCE_SCOPES}-entry limit"
                )));
            }
            let collections = self
                .collection_ids
                .into_iter()
                .map(|id| resource_scope_snapshot("collection", id));
            let classes = self
                .class_ids
                .into_iter()
                .map(|id| resource_scope_snapshot("class", id));
            let objects = self
                .object_ids
                .into_iter()
                .map(|id| resource_scope_snapshot("object", id));
            Some(
                collections
                    .chain(classes)
                    .chain(objects)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            None
        };

        Ok(serde_json::json!({
            "permissions": permissions,
            "resources": resources,
        }))
    }
}

/// Delete one bounded batch of tokens older than the configured retention horizon.
pub async fn purge_expired_tokens(
    runtime: &PostgresRuntime,
    settings: hubuum_domain::TokenRetentionSettings,
) -> Result<usize, PostgresStorageError> {
    purge_expired_tokens_at(runtime, settings, Utc::now().naive_utc()).await
}

/// Time-controlled retention entry point used by adapter integration tests.
#[doc(hidden)]
pub async fn purge_expired_tokens_at(
    runtime: &PostgresRuntime,
    settings: hubuum_domain::TokenRetentionSettings,
    now: NaiveDateTime,
) -> Result<usize, PostgresStorageError> {
    let cutoffs = settings
        .cutoffs(now)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let batch_size = settings.batch_size().as_i64();

    runtime
        .with_transaction(async |connection| -> Result<usize, PostgresStorageError> {
            if !crate::operations::maintenance::maintenance_state_on_connection(connection)
                .await?
                .is_normal()
            {
                return Ok(0);
            }
            if !try_acquire_token_retention_lock(connection).await? {
                return Ok(0);
            }

            purge_expired_token_batch_on_connection(
                connection,
                cutoffs.explicit_expiry(),
                cutoffs.implicit_issue(),
                batch_size,
            )
            .await
        })
        .await
}

async fn try_acquire_token_retention_lock(
    connection: &mut PostgresConnection,
) -> Result<bool, PostgresStorageError> {
    Ok(
        diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS locked")
            .bind::<BigInt, _>(TOKEN_RETENTION_LOCK_KEY)
            .get_result::<AdvisoryLockRow>(connection)
            .await?
            .locked,
    )
}

async fn purge_expired_token_batch_on_connection(
    connection: &mut PostgresConnection,
    explicit_expiry_cutoff: NaiveDateTime,
    implicit_issue_cutoff: NaiveDateTime,
    batch_size: i64,
) -> Result<usize, PostgresStorageError> {
    // Give every indexed terminal stream an initial share, then let each use
    // remaining capacity so a one-sided backlog still fills the batch.
    let revoked_share = batch_size / 3 + i64::from(batch_size % 3 > 0);
    let explicit_share = batch_size / 3 + i64::from(batch_size % 3 > 1);
    let implicit_share = batch_size / 3;

    let mut deleted =
        purge_revoked_tokens(connection, explicit_expiry_cutoff, revoked_share).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_explicit_expired_tokens(
        connection,
        explicit_expiry_cutoff,
        explicit_share.min(remaining),
    )
    .await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_implicit_expired_tokens(
        connection,
        implicit_issue_cutoff,
        implicit_share.min(remaining),
    )
    .await?;

    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_revoked_tokens(connection, explicit_expiry_cutoff, remaining).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_explicit_expired_tokens(connection, explicit_expiry_cutoff, remaining).await?;
    let remaining = batch_size.saturating_sub(deleted as i64);
    deleted += purge_implicit_expired_tokens(connection, implicit_issue_cutoff, remaining).await?;

    Ok(deleted)
}

async fn purge_revoked_tokens(
    connection: &mut PostgresConnection,
    revocation_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, PostgresStorageError> {
    let candidates = select_candidates(
        connection,
        "SELECT id
         FROM tokens
         WHERE revoked_at IS NOT NULL
           AND revoked_at <= $1
         ORDER BY revoked_at ASC, id ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
        revocation_cutoff,
        limit,
    )
    .await?;
    purge_selected_tokens(connection, candidates, TokenRetentionBasis::Revocation).await
}

async fn purge_explicit_expired_tokens(
    connection: &mut PostgresConnection,
    expiry_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, PostgresStorageError> {
    let candidates = select_candidates(
        connection,
        "SELECT id
         FROM tokens
         WHERE expires_at IS NOT NULL
           AND expires_at <= $1
         ORDER BY expires_at ASC, id ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
        expiry_cutoff,
        limit,
    )
    .await?;
    purge_selected_tokens(connection, candidates, TokenRetentionBasis::ExplicitExpiry).await
}

async fn purge_implicit_expired_tokens(
    connection: &mut PostgresConnection,
    issue_cutoff: NaiveDateTime,
    limit: i64,
) -> Result<usize, PostgresStorageError> {
    let candidates = select_candidates(
        connection,
        "SELECT id
         FROM tokens
         WHERE expires_at IS NULL
           AND issued <= $1
         ORDER BY issued ASC, id ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
        issue_cutoff,
        limit,
    )
    .await?;
    purge_selected_tokens(connection, candidates, TokenRetentionBasis::ImplicitExpiry).await
}

async fn select_candidates(
    connection: &mut PostgresConnection,
    statement: &'static str,
    cutoff: NaiveDateTime,
    limit: i64,
) -> Result<Vec<TokenRetentionCandidate>, PostgresStorageError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    Ok(diesel::sql_query(statement)
        .bind::<Timestamp, _>(cutoff)
        .bind::<BigInt, _>(limit)
        .load::<TokenRetentionCandidate>(connection)
        .await?)
}

async fn purge_selected_tokens(
    connection: &mut PostgresConnection,
    candidates: Vec<TokenRetentionCandidate>,
    basis: TokenRetentionBasis,
) -> Result<usize, PostgresStorageError> {
    if candidates.is_empty() {
        return Ok(0);
    }

    let token_ids = candidates
        .into_iter()
        .map(|candidate| candidate.id)
        .collect::<Vec<_>>();
    let retained = crate::schema::tokens::table
        .filter(crate::schema::tokens::id.eq_any(&token_ids))
        .order_by(crate::schema::tokens::id.asc())
        .select((
            crate::schema::tokens::id,
            crate::schema::tokens::principal_id,
            crate::schema::tokens::name,
            crate::schema::tokens::description,
            crate::schema::tokens::issued,
            crate::schema::tokens::expires_at,
            crate::schema::tokens::last_used_at,
            crate::schema::tokens::revoked_at,
            crate::schema::tokens::permission_scoped,
            crate::schema::tokens::resource_scoped,
            crate::schema::tokens::revision,
        ))
        .load::<RetainedTokenRow>(connection)
        .await?;
    let scopes = load_token_scope_rows(connection, &retained).await?;
    let events = retained
        .iter()
        .zip(scopes)
        .map(|(token, scope)| token_purge_event(token, scope, basis))
        .collect::<Result<Vec<_>, _>>()?;
    append_events(connection, &events).await?;

    let deleted = diesel::delete(
        crate::schema::tokens::table.filter(crate::schema::tokens::id.eq_any(&token_ids)),
    )
    .execute(connection)
    .await?;
    if deleted != token_ids.len() {
        return Err(PostgresStorageError::internal(format!(
            "Token retention selected {} locked rows but deleted {deleted}",
            token_ids.len()
        )));
    }
    Ok(deleted)
}

async fn load_token_scope_rows(
    connection: &mut PostgresConnection,
    tokens: &[RetainedTokenRow],
) -> Result<Vec<Option<StoredTokenScopeRows>>, PostgresStorageError> {
    let permission_token_ids = sorted_scope_token_ids(tokens, |token| token.permission_scoped);
    let resource_token_ids = sorted_scope_token_ids(tokens, |token| token.resource_scoped);
    if permission_token_ids.is_empty() && resource_token_ids.is_empty() {
        return Ok(vec![None; tokens.len()]);
    }

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

    let mut rows_by_token = tokens
        .iter()
        .filter(|token| token.is_scoped())
        .map(|token| (token.id, StoredTokenScopeRows::default()))
        .collect::<HashMap<_, _>>();
    append_scope_rows(&mut rows_by_token, permissions, |scope| {
        &mut scope.permissions
    });
    append_scope_rows(&mut rows_by_token, collection_ids, |scope| {
        &mut scope.collection_ids
    });
    append_scope_rows(&mut rows_by_token, class_ids, |scope| &mut scope.class_ids);
    append_scope_rows(&mut rows_by_token, object_ids, |scope| {
        &mut scope.object_ids
    });

    Ok(tokens
        .iter()
        .map(|token| {
            token
                .is_scoped()
                .then(|| rows_by_token.get(&token.id).cloned().unwrap_or_default())
        })
        .collect())
}

fn sorted_scope_token_ids(
    tokens: &[RetainedTokenRow],
    include: impl Fn(&RetainedTokenRow) -> bool,
) -> Vec<i32> {
    let mut ids = tokens
        .iter()
        .filter(|token| include(token))
        .map(|token| token.id)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn append_scope_rows<T>(
    scopes: &mut HashMap<i32, StoredTokenScopeRows>,
    rows: Vec<(i32, T)>,
    values: impl Fn(&mut StoredTokenScopeRows) -> &mut Vec<T>,
) {
    for (token_id, value) in rows {
        if let Some(scope) = scopes.get_mut(&token_id) {
            values(scope).push(value);
        }
    }
}

fn token_purge_event(
    token: &RetainedTokenRow,
    scope: Option<StoredTokenScopeRows>,
    basis: TokenRetentionBasis,
) -> Result<NewEvent, PostgresStorageError> {
    let scope = scope.map(|scope| scope.snapshot(token)).transpose()?;
    let snapshot = serde_json::json!({
        "id": token.id,
        "principal_id": token.principal_id,
        "name": token.name,
        "description": token.description,
        "issued": token.issued,
        "expires_at": token.expires_at,
        "last_used_at": token.last_used_at,
        "revoked_at": token.revoked_at,
        "scoped": token.is_scoped(),
        "permission_scoped": token.permission_scoped,
        "resource_scoped": token.resource_scoped,
        "scope": scope,
        "revision": token.revision,
    });
    NewEvent::new(
        EntityType::Token,
        Action::Purged,
        ActorKind::System,
        format!(
            "Token {} purged after retention for principal {}",
            token.id, token.principal_id
        ),
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))
    .and_then(|event| {
        Ok(event
            .with_entity_id(hubuum_events_core::EventEntityId::new(token.id)?)
            .with_entity_name(token.name.clone().unwrap_or_else(|| token.id.to_string()))
            .with_before(snapshot)
            .with_metadata(serde_json::json!({
                "principal_id": token.principal_id,
                "retention_basis": basis.as_str(),
            })))
    })
}

fn resource_scope_snapshot(
    kind: &'static str,
    id: i32,
) -> Result<serde_json::Value, PostgresStorageError> {
    if id <= 0 {
        return Err(PostgresStorageError::invalid_input(format!(
            "Invalid {kind} ID: expected a positive integer"
        )));
    }
    Ok(serde_json::json!({ "kind": kind, "id": id }))
}

fn storage_error_to_postgres(error: StorageError) -> PostgresStorageError {
    error.into()
}
