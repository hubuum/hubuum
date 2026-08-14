use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::principal::PrincipalKind;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    PrincipalID, PrincipalToken, PrincipalTokenCreateParts, PrincipalTokenMetadata,
    TokenIssuancePolicy, TokenScope, TokenScopeDetails,
};
use crate::schema::{
    principals, service_accounts, token_class_scopes, token_collection_scopes, token_object_scopes,
    token_scopes, tokens,
};
use crate::storage::postgres::operations::authz::{
    load_token_scope_conn, load_token_scopes_for_tokens_conn,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{PostgresConnection, with_connection, with_transaction};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::tokens)]
pub(crate) struct PrincipalTokenRow {
    pub(crate) id: i32,
    pub(crate) token: String,
    pub(crate) principal_id: i32,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) issued: chrono::NaiveDateTime,
    pub(crate) expires_at: Option<chrono::NaiveDateTime>,
    pub(crate) last_used_at: Option<chrono::NaiveDateTime>,
    pub(crate) revoked_at: Option<chrono::NaiveDateTime>,
    pub(crate) permission_scoped: bool,
    pub(crate) resource_scoped: bool,
    pub(crate) revision: PostgresRevision,
}

impl From<PrincipalTokenRow> for PrincipalToken {
    fn from(row: PrincipalTokenRow) -> Self {
        Self {
            id: row.id,
            token: row.token,
            principal_id: row.principal_id,
            name: row.name,
            description: row.description,
            issued: row.issued,
            expires_at: row.expires_at,
            last_used_at: row.last_used_at,
            revoked_at: row.revoked_at,
            permission_scoped: row.permission_scoped,
            resource_scoped: row.resource_scoped,
            revision: row.revision.into_domain(),
        }
    }
}

impl CursorPaginated for PrincipalTokenRow {
    fn supports_sort(field: &FilterField) -> bool {
        PrincipalToken::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        PrincipalToken::from(self.clone()).cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        PrincipalToken::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        PrincipalToken::tie_breaker_sort()
    }
}

impl CursorSqlMapping for PrincipalTokenRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "tokens.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "tokens.name",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::IssuedAt => CursorSqlField {
                column: "tokens.issued",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::ExpiresAt => CursorSqlField {
                column: "tokens.expires_at",
                sql_type: CursorSqlType::DateTime,
                nullable: true,
            },
            FilterField::LastUsedAt => CursorSqlField {
                column: "tokens.last_used_at",
                sql_type: CursorSqlType::DateTime,
                nullable: true,
            },
            FilterField::Revision => CursorSqlField {
                column: "tokens.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for tokens",
                    field
                )));
            }
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = token_scopes)]
struct NewTokenScope<'a> {
    token_id: i32,
    permission: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = token_collection_scopes)]
struct NewTokenCollectionScope {
    token_id: i32,
    collection_id: i32,
}

#[derive(Insertable)]
#[diesel(table_name = token_class_scopes)]
struct NewTokenClassScope {
    token_id: i32,
    class_id: i32,
}

#[derive(Insertable)]
#[diesel(table_name = token_object_scopes)]
struct NewTokenObjectScope {
    token_id: i32,
    object_id: i32,
}

pub(crate) async fn principal_token_metadata_by_ids_db(
    pool: &crate::storage::postgres::PostgresPool,
    token_ids: Vec<i32>,
) -> Result<Vec<PrincipalTokenMetadata>, ApiError> {
    if token_ids.is_empty() {
        return Ok(Vec::new());
    }

    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let locked = crate::schema::tokens::table
            .filter(crate::schema::tokens::id.eq_any(&token_ids))
            .order_by(crate::schema::tokens::id.asc())
            .for_update()
            .load::<PrincipalTokenRow>(conn)
            .await?;
        let locked_by_id = locked
            .into_iter()
            .map(|token| (token.id, PrincipalToken::from(token)))
            .collect::<std::collections::HashMap<_, _>>();
        let ordered = token_ids
            .iter()
            .map(|token_id| {
                locked_by_id.get(token_id).cloned().ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Token {token_id} was purged before metadata could be loaded"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        principal_token_metadata_conn(conn, &ordered).await
    })
    .await
}

pub(crate) async fn principal_token_metadata_conn(
    conn: &mut PostgresConnection,
    tokens: &[PrincipalToken],
) -> Result<Vec<PrincipalTokenMetadata>, ApiError> {
    let now = chrono::Utc::now().naive_utc();
    let active_after = crate::models::configured_token_lifetime()?.cutoff_from(now)?;
    let scopes = load_token_scopes_for_tokens_conn(conn, tokens).await?;
    tokens
        .iter()
        .zip(scopes)
        .map(|(token, scope)| {
            PrincipalTokenMetadata::from_token_and_scope(token, scope, now, active_after)
        })
        .collect()
}

pub async fn principal_token_metadata_by_id_for_principal_db(
    pool: &crate::storage::postgres::PostgresPool,
    token_id_value: i32,
    principal_id_value: i32,
) -> Result<PrincipalTokenMetadata, ApiError> {
    use crate::schema::tokens::dsl::{id, principal_id, tokens};

    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let token = tokens
            .filter(id.eq(token_id_value))
            .filter(principal_id.eq(principal_id_value))
            .for_update()
            .first::<PrincipalTokenRow>(conn)
            .await?
            .into();
        principal_token_metadata_conn(conn, std::slice::from_ref(&token))
            .await?
            .pop()
            .ok_or_else(|| {
                ApiError::InternalServerError(
                    "Token metadata projection returned no token".to_string(),
                )
            })
    })
    .await
}

pub(crate) fn token_snapshot(
    token: &PrincipalToken,
    scope: Option<&TokenScope>,
) -> Result<serde_json::Value, ApiError> {
    let scope = scope
        .cloned()
        .map(TokenScopeDetails::from_scope)
        .transpose()?;
    Ok(serde_json::json!({
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
    }))
}

fn token_event(
    token: &PrincipalToken,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
    renewed_from_token_id: Option<i32>,
) -> Result<NewEvent, ApiError> {
    let mut metadata = serde_json::json!({
        "principal_id": token.principal_id,
        "scoped": token.is_scoped(),
        "permission_scoped": token.permission_scoped,
        "resource_scoped": token.resource_scoped,
    });
    if let Some(source_id) = renewed_from_token_id {
        metadata["renewed_from_token_id"] = serde_json::json!(source_id);
    }

    Ok(
        NewEvent::new(EntityType::Token, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(token.id)
            .with_entity_name(token.name.clone().unwrap_or_else(|| token.id.to_string()))
            .with_metadata(metadata),
    )
}

/// Soft-revoke every unrevoked token belonging to one principal.
///
/// Accepting an existing connection lets callers compose token revocation with
/// security-sensitive principal changes in the same transaction.
pub(crate) async fn revoke_all_tokens_for_principal_conn(
    conn: &mut PostgresConnection,
    principal: PrincipalID,
) -> Result<usize, ApiError> {
    use crate::schema::tokens::dsl::{principal_id, revoked_at, tokens};

    Ok(diesel::update(
        tokens
            .filter(principal_id.eq(principal.id()))
            .filter(revoked_at.is_null()),
    )
    .set(revoked_at.eq(diesel::dsl::now))
    .execute(conn)
    .await?)
}

pub async fn revoke_token_by_id_for_principal_without_events_db(
    pool: &crate::storage::postgres::PostgresPool,
    token_id: i32,
    principal: i32,
) -> Result<usize, ApiError> {
    use crate::schema::tokens::dsl::{id, principal_id, revoked_at, tokens};
    with_connection(pool, async |conn| {
        diesel::update(
            tokens
                .filter(id.eq(token_id))
                .filter(principal_id.eq(principal))
                .filter(revoked_at.is_null()),
        )
        .set(revoked_at.eq(diesel::dsl::now))
        .execute(conn)
        .await
    })
    .await
}

pub async fn revoke_token_by_id_for_principal_db(
    pool: &crate::storage::postgres::PostgresPool,
    token_id: i32,
    principal: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    let Some(context) = context else {
        return revoke_token_by_id_for_principal_without_events_db(pool, token_id, principal).await;
    };

    use crate::schema::tokens::dsl::{id, principal_id, revoked_at, tokens};
    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        let before: Option<PrincipalToken> = tokens
            .filter(id.eq(token_id))
            .filter(principal_id.eq(principal))
            .for_update()
            .first::<PrincipalTokenRow>(conn)
            .await
            .optional()?
            .map(Into::into);

        let Some(before) = before else {
            return Ok(0);
        };
        crate::storage::postgres::assert_locked_revision_precondition(
            conn,
            &RevisionOwner::Token.key(before.id),
            before.revision,
        )
        .await?;
        if before.revoked_at.is_some() {
            return Ok(1);
        }
        let before_scope = load_token_scope_conn(conn, &before).await?;

        let updated = diesel::update(
            tokens
                .filter(id.eq(token_id))
                .filter(principal_id.eq(principal))
                .filter(revoked_at.is_null()),
        )
        .set(revoked_at.eq(diesel::dsl::now))
        .get_result::<PrincipalTokenRow>(conn)
        .await
        .optional()?
        .map(Into::into);

        if let Some(after) = updated {
            let event = token_event(
                &after,
                Action::Revoked,
                context,
                format!(
                    "Token {} revoked for principal {}",
                    after.id, after.principal_id
                ),
                None,
            )?
            .with_before(token_snapshot(&before, before_scope.as_ref())?)
            .with_after(token_snapshot(&after, before_scope.as_ref())?);
            emit_event(conn, &event).await?;
            Ok(1)
        } else {
            Ok(0)
        }
    })
    .await
}

pub(crate) async fn create_principal_token_hashed_db(
    pool: &crate::storage::postgres::PostgresPool,
    request: PrincipalTokenCreateParts,
    token_hash: String,
    issuance_policy: TokenIssuancePolicy,
    context: Option<&EventContext>,
) -> Result<PrincipalToken, ApiError> {
    with_transaction(pool, async |conn| {
        create_principal_token_parts_conn(
            conn,
            request,
            token_hash,
            issuance_policy,
            context,
            None,
            false,
        )
        .await
    })
    .await
}

pub(crate) async fn renew_principal_token_hashed_db(
    pool: &crate::storage::postgres::PostgresPool,
    source_token_id: i32,
    principal: i32,
    token_hash: String,
    expires_at: Option<chrono::NaiveDateTime>,
    issuance_policy: TokenIssuancePolicy,
    context: Option<&EventContext>,
) -> Result<PrincipalToken, ApiError> {
    with_transaction(pool, async |conn| {
        // Service-account disable takes the same row lock before revoking its
        // tokens. Keeping that lock order here prevents renewal from racing a
        // disable into a newly usable credential.
        ensure_principal_can_mint_conn(conn, principal).await?;

        let source: PrincipalToken = tokens::table
            .filter(tokens::id.eq(source_token_id))
            .filter(tokens::principal_id.eq(principal))
            .for_update()
            .first::<PrincipalTokenRow>(conn)
            .await?
            .into();
        if source.revoked_at.is_some() {
            return Err(ApiError::Conflict(
                "Revoked tokens cannot be renewed".to_string(),
            ));
        }
        let scope = load_token_scope_conn(conn, &source).await?;
        let principal_id = PrincipalID::new(principal)?;
        let request = PrincipalTokenCreateParts {
            principal_id,
            name: source.name.clone(),
            description: source.description.clone(),
            expires_at,
            scope,
        };

        create_principal_token_parts_conn(
            conn,
            request,
            token_hash,
            issuance_policy,
            context,
            Some(source.id),
            true,
        )
        .await
    })
    .await
}

async fn ensure_principal_can_mint_conn(
    conn: &mut PostgresConnection,
    principal: i32,
) -> Result<(), ApiError> {
    let principal_kind = principals::table
        .filter(principals::id.eq(principal))
        .select(principals::kind)
        .first::<String>(conn)
        .await?;

    if principal_kind == PrincipalKind::ServiceAccount.as_str() {
        let disabled_at = service_accounts::table
            .filter(service_accounts::id.eq(principal))
            .for_update()
            .select(service_accounts::disabled_at)
            .first::<Option<chrono::NaiveDateTime>>(conn)
            .await?;
        if disabled_at.is_some() {
            return Err(ApiError::Conflict(
                "Service account is disabled".to_string(),
            ));
        }
    }
    Ok(())
}

async fn create_principal_token_parts_conn(
    conn: &mut PostgresConnection,
    request: PrincipalTokenCreateParts,
    token_hash: String,
    issuance_policy: TokenIssuancePolicy,
    context: Option<&EventContext>,
    renewed_from_token_id: Option<i32>,
    principal_already_checked: bool,
) -> Result<PrincipalToken, ApiError> {
    let PrincipalTokenCreateParts {
        principal_id,
        name,
        description,
        expires_at,
        scope,
    } = request;
    let principal = principal_id.id();
    let scope = scope.as_ref();
    let permission_scoped = scope.is_some_and(TokenScope::is_permission_scoped);
    let resource_scoped = scope.is_some_and(TokenScope::is_resource_scoped);
    let scope_strings = scope
        .and_then(TokenScope::permissions)
        .map(|permissions| {
            permissions
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let resource_ids = scope.and_then(TokenScope::resource_ids);
    let collection_scope_ids = resource_ids
        .map(|ids| ids.collection_ids().to_vec())
        .unwrap_or_default();
    let class_scope_ids = resource_ids
        .map(|ids| ids.class_ids().to_vec())
        .unwrap_or_default();
    let object_scope_ids = resource_ids
        .map(|ids| ids.object_ids().to_vec())
        .unwrap_or_default();
    let issued_at = diesel::select(diesel::dsl::sql::<diesel::sql_types::Timestamp>(
        "statement_timestamp() AT TIME ZONE 'UTC'",
    ))
    .get_result::<chrono::NaiveDateTime>(conn)
    .await?;
    let effective_expiry = issuance_policy.resolve_expiry(issued_at, expires_at)?;

    if !principal_already_checked {
        ensure_principal_can_mint_conn(conn, principal).await?;
    }

    if !collection_scope_ids.is_empty() {
        let found = crate::schema::collections::table
            .filter(crate::schema::collections::id.eq_any(&collection_scope_ids))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if found != collection_scope_ids.len() as i64 {
            return Err(ApiError::BadRequest(
                "scope.resources contains an unknown collection id".to_string(),
            ));
        }
    }
    if !class_scope_ids.is_empty() {
        let found = crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq_any(&class_scope_ids))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if found != class_scope_ids.len() as i64 {
            return Err(ApiError::BadRequest(
                "scope.resources contains an unknown class id".to_string(),
            ));
        }
    }
    if !object_scope_ids.is_empty() {
        let found = crate::schema::hubuumobject::table
            .filter(crate::schema::hubuumobject::id.eq_any(&object_scope_ids))
            .count()
            .get_result::<i64>(conn)
            .await?;
        if found != object_scope_ids.len() as i64 {
            return Err(ApiError::BadRequest(
                "scope.resources contains an unknown object id".to_string(),
            ));
        }
    }

    let token: PrincipalToken = diesel::insert_into(tokens::table)
        .values((
            tokens::token.eq(&token_hash),
            tokens::principal_id.eq(principal),
            tokens::name.eq(&name),
            tokens::description.eq(&description),
            tokens::issued.eq(issued_at),
            tokens::expires_at.eq(Some(effective_expiry)),
            tokens::permission_scoped.eq(permission_scoped),
            tokens::resource_scoped.eq(resource_scoped),
        ))
        .get_result::<PrincipalTokenRow>(conn)
        .await?
        .into();

    if !scope_strings.is_empty() {
        let rows = scope_strings
            .iter()
            .map(|permission| NewTokenScope {
                token_id: token.id,
                permission,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(token_scopes::table)
            .values(&rows)
            .execute(conn)
            .await?;
    }

    if !collection_scope_ids.is_empty() {
        let rows = collection_scope_ids
            .iter()
            .map(|collection_id| NewTokenCollectionScope {
                token_id: token.id,
                collection_id: *collection_id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(token_collection_scopes::table)
            .values(&rows)
            .execute(conn)
            .await?;
    }
    if !class_scope_ids.is_empty() {
        let rows = class_scope_ids
            .iter()
            .map(|class_id| NewTokenClassScope {
                token_id: token.id,
                class_id: *class_id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(token_class_scopes::table)
            .values(&rows)
            .execute(conn)
            .await?;
    }
    if !object_scope_ids.is_empty() {
        let rows = object_scope_ids
            .iter()
            .map(|object_id| NewTokenObjectScope {
                token_id: token.id,
                object_id: *object_id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(token_object_scopes::table)
            .values(&rows)
            .execute(conn)
            .await?;
    }

    if let Some(context) = context {
        let event = token_event(
            &token,
            Action::Created,
            context,
            format!(
                "Token {} created for principal {}",
                token.id, token.principal_id
            ),
            renewed_from_token_id,
        )?
        .with_after(token_snapshot(&token, scope)?);
        emit_event(conn, &event).await?;
    }

    Ok(token)
}

pub(crate) async fn revoke_token_by_hash_db(
    pool: &crate::storage::postgres::PostgresPool,
    principal: Option<i32>,
    token_hash: String,
) -> Result<usize, ApiError> {
    use crate::schema::tokens::dsl::{principal_id, revoked_at, token, tokens};

    with_connection(pool, async |conn| {
        if let Some(principal) = principal {
            diesel::update(
                tokens
                    .filter(token.eq(token_hash))
                    .filter(principal_id.eq(principal))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
            .await
        } else {
            diesel::update(
                tokens
                    .filter(token.eq(token_hash))
                    .filter(revoked_at.is_null()),
            )
            .set(revoked_at.eq(diesel::dsl::now))
            .execute(conn)
            .await
        }
    })
    .await
}

pub(crate) async fn revoke_all_tokens_for_principal_db(
    pool: &crate::storage::postgres::PostgresPool,
    principal: PrincipalID,
) -> Result<usize, ApiError> {
    with_connection(pool, async |conn| {
        revoke_all_tokens_for_principal_conn(conn, principal).await
    })
    .await
}
