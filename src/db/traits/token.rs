use crate::db::prelude::*;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::principal::PrincipalKind;
use crate::models::{Permissions, PrincipalToken, Token, TokenResourceScope, TokenScope};
use crate::schema::{
    principals, service_accounts, token_class_scopes, token_collection_scopes, token_object_scopes,
    token_scopes, tokens,
};

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
        "scoped": token.is_scoped(),
        "permission_scoped": token.permission_scoped,
        "resource_scoped": token.resource_scoped,
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
                "scoped": token.is_scoped(),
                "permission_scoped": token.permission_scoped,
                "resource_scoped": token.resource_scoped,
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
    let scope = scopes
        .map(|permissions| TokenScope::from_stored_parts(Some(permissions.to_vec()), None))
        .transpose()?;
    create_principal_token_with_scope_db(
        pool,
        principal,
        name,
        description,
        expires_at,
        scope.as_ref(),
        context,
    )
    .await
}

pub async fn create_principal_token_with_scope_db(
    pool: &DbPool,
    principal: i32,
    name: Option<&str>,
    description: Option<&str>,
    expires_at: Option<chrono::NaiveDateTime>,
    scope: Option<&TokenScope>,
    context: Option<&EventContext>,
) -> Result<Token, ApiError> {
    let raw = crate::utilities::auth::generate_token();
    let hash = raw.storage_hash();
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
    let resource_scopes = scope
        .map(TokenScope::resource_scopes)
        .transpose()?
        .flatten()
        .unwrap_or_default();
    let collection_scope_ids = resource_scopes
        .iter()
        .filter_map(|resource| match resource {
            TokenResourceScope::Collection(id) => Some(id.id()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let class_scope_ids = resource_scopes
        .iter()
        .filter_map(|resource| match resource {
            TokenResourceScope::Class(id) => Some(id.id()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let object_scope_ids = resource_scopes
        .iter()
        .filter_map(|resource| match resource {
            TokenResourceScope::Object(id) => Some(id.id()),
            _ => None,
        })
        .collect::<Vec<_>>();
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

        if !collection_scope_ids.is_empty() {
            let found = crate::schema::collections::table
                .filter(crate::schema::collections::id.eq_any(&collection_scope_ids))
                .count()
                .get_result::<i64>(conn)
                .await?;
            if found != collection_scope_ids.len() as i64 {
                return Err(ApiError::BadRequest(
                    "resource_scopes contains an unknown collection id".to_string(),
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
                    "resource_scopes contains an unknown class id".to_string(),
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
                    "resource_scopes contains an unknown object id".to_string(),
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
                tokens::permission_scoped.eq(permission_scoped),
                tokens::resource_scoped.eq(resource_scoped),
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
            )?
            .with_after(token_snapshot(&token));
            emit_event(conn, &event).await?;
        }
        Ok(())
    })
    .await?;

    Ok(raw)
}
