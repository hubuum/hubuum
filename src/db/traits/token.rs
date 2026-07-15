use crate::db::prelude::*;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::principal::PrincipalKind;
use crate::models::{Permissions, PrincipalToken, Token};
use crate::schema::{principals, service_accounts, token_scopes, tokens};

#[derive(Insertable)]
#[diesel(table_name = token_scopes)]
struct NewTokenScope<'a> {
    token_id: i32,
    permission: &'a str,
}

fn token_snapshot(token: &PrincipalToken) -> serde_json::Value {
    serde_json::json!({
        "id": token.id,
        "principal_id": token.principal_id,
        "name": token.name,
        "description": token.description,
        "issued": token.issued,
        "expires_at": token.expires_at,
        "last_used_at": token.last_used_at,
        "revoked_at": token.revoked_at,
        "scoped": token.scoped,
    })
}

fn token_event(
    token: &PrincipalToken,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(
        NewEvent::new(EntityType::Token, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(token.id)
            .with_entity_name(token.name.clone().unwrap_or_else(|| token.id.to_string()))
            .with_metadata(serde_json::json!({
                "principal_id": token.principal_id,
                "scoped": token.scoped,
            })),
    )
}

pub async fn revoke_token_by_id_for_principal_without_events_db(
    pool: &DbPool,
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
    pool: &DbPool,
    token_id: i32,
    principal: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    let Some(context) = context else {
        return revoke_token_by_id_for_principal_without_events_db(pool, token_id, principal).await;
    };

    use crate::schema::tokens::dsl::{id, principal_id, revoked_at, tokens};
    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        let before = tokens
            .filter(id.eq(token_id))
            .filter(principal_id.eq(principal))
            .filter(revoked_at.is_null())
            .first::<PrincipalToken>(conn)
            .await
            .optional()?;

        let updated = diesel::update(
            tokens
                .filter(id.eq(token_id))
                .filter(principal_id.eq(principal))
                .filter(revoked_at.is_null()),
        )
        .set(revoked_at.eq(diesel::dsl::now))
        .get_result::<PrincipalToken>(conn)
        .await
        .optional()?;

        if let (Some(before), Some(after)) = (before, updated) {
            let event = token_event(
                &after,
                Action::Revoked,
                context,
                format!(
                    "Token {} revoked for principal {}",
                    after.id, after.principal_id
                ),
            )?
            .with_before(token_snapshot(&before))
            .with_after(token_snapshot(&after));
            emit_event(conn, &event).await?;
            Ok(1)
        } else {
            Ok(0)
        }
    })
    .await
}

pub async fn create_principal_token_db(
    pool: &DbPool,
    principal: i32,
    name: Option<&str>,
    description: Option<&str>,
    expires_at: Option<chrono::NaiveDateTime>,
    scopes: Option<&[Permissions]>,
    context: Option<&EventContext>,
) -> Result<Token, ApiError> {
    let raw = crate::utilities::auth::generate_token();
    let hash = raw.storage_hash();
    let scoped = scopes.is_some();
    let scope_strings = scopes
        .map(|permissions| {
            permissions
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let name = name.map(ToOwned::to_owned);
    let description = description.map(ToOwned::to_owned);

    with_transaction(pool, async |conn| -> Result<(), ApiError> {
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

        let token = diesel::insert_into(tokens::table)
            .values((
                tokens::token.eq(&hash),
                tokens::principal_id.eq(principal),
                tokens::name.eq(&name),
                tokens::description.eq(&description),
                tokens::expires_at.eq(expires_at),
                tokens::scoped.eq(scoped),
            ))
            .get_result::<PrincipalToken>(conn)
            .await?;

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

        if let Some(context) = context {
            let event = token_event(
                &token,
                Action::Created,
                context,
                format!(
                    "Token {} created for principal {}",
                    token.id, token.principal_id
                ),
            )?
            .with_after(token_snapshot(&token));
            emit_event(conn, &event).await?;
        }
        Ok(())
    })
    .await?;

    Ok(raw)
}
