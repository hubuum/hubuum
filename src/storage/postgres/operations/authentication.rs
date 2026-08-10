use chrono::NaiveDateTime;
use hubuum_storage_core::{
    AuthenticationHuman, AuthenticationIdentity, AuthenticationPrincipal,
    AuthenticationPrincipalKind, AuthenticationResourceScope, AuthenticationTokenScope,
    AuthenticationTokenScopeQuery,
};

use crate::errors::ApiError;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresPool, with_connection};

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

pub(crate) async fn load_authentication_identity(
    pool: &PostgresPool,
    principal_id: i32,
) -> Result<AuthenticationIdentity, ApiError> {
    use crate::schema::{principals, users};

    let row = with_connection(pool, async |conn| {
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
) -> Result<AuthenticationIdentity, ApiError> {
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
            return Err(ApiError::InternalServerError(format!(
                "Unknown principal kind '{other}'"
            )));
        }
    };
    let principal = AuthenticationPrincipal::new(principal_id, kind, name, identity_scope_id);
    let human = human_id
        .map(|human_id| {
            if kind != AuthenticationPrincipalKind::Human || human_id != principal_id {
                return Err(ApiError::InternalServerError(format!(
                    "Principal '{principal_id}' has an inconsistent human identity row"
                )));
            }
            let created_at = created_at.ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "Human principal '{principal_id}' has no creation timestamp"
                ))
            })?;
            let updated_at = updated_at.ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "Human principal '{principal_id}' has no update timestamp"
                ))
            })?;
            Ok(AuthenticationHuman::new(
                human_id,
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

pub(crate) async fn load_authentication_token_scope(
    pool: &PostgresPool,
    query: AuthenticationTokenScopeQuery,
) -> Result<Option<AuthenticationTokenScope>, ApiError> {
    use crate::schema::{
        token_class_scopes, token_collection_scopes, token_object_scopes, token_scopes,
    };

    if !query.is_scoped() {
        return Ok(None);
    }

    with_connection(pool, async |conn| {
        let permissions = if query.is_permission_scoped() {
            Some(
                token_scopes::table
                    .filter(token_scopes::token_id.eq(query.token_id()))
                    .order_by(token_scopes::permission.asc())
                    .select(token_scopes::permission)
                    .load::<String>(conn)
                    .await?,
            )
        } else {
            None
        };
        let resources = if query.is_resource_scoped() {
            let collection_ids = token_collection_scopes::table
                .filter(token_collection_scopes::token_id.eq(query.token_id()))
                .order_by(token_collection_scopes::collection_id.asc())
                .select(token_collection_scopes::collection_id)
                .load::<i32>(conn)
                .await?;
            let class_ids = token_class_scopes::table
                .filter(token_class_scopes::token_id.eq(query.token_id()))
                .order_by(token_class_scopes::class_id.asc())
                .select(token_class_scopes::class_id)
                .load::<i32>(conn)
                .await?;
            let object_ids = token_object_scopes::table
                .filter(token_object_scopes::token_id.eq(query.token_id()))
                .order_by(token_object_scopes::object_id.asc())
                .select(token_object_scopes::object_id)
                .load::<i32>(conn)
                .await?;
            Some(AuthenticationResourceScope::new(
                collection_ids,
                class_ids,
                object_ids,
            ))
        } else {
            None
        };

        Ok::<_, diesel::result::Error>(AuthenticationTokenScope::new(permissions, resources))
    })
    .await
    .map(Some)
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
