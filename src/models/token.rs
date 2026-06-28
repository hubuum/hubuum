use chrono::NaiveDateTime;

use diesel::prelude::*;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::ToSchema;

use crate::config::token_hash_key_bytes;
use crate::db::traits::user::DeleteTokenRecord;
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::schema::tokens;
use crate::traits::{
    BackendContext, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

/// A persisted bearer token, keyed to a principal, with a full lifecycle. The
/// `token` field stores the HMAC hash, never the raw value.
#[derive(Serialize, Deserialize, Queryable, Insertable, Selectable, Clone, Debug, ToSchema)]
#[diesel(table_name = tokens)]
pub struct PrincipalToken {
    pub id: i32,
    pub token: String,
    pub principal_id: i32,
    pub name: Option<String>,
    pub description: Option<String>,
    pub issued: NaiveDateTime,
    pub expires_at: Option<NaiveDateTime>,
    pub last_used_at: Option<NaiveDateTime>,
    pub revoked_at: Option<NaiveDateTime>,
    pub scoped: bool,
}

/// Public, hash-free projection of a token for listing.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct PrincipalTokenMetadata {
    pub id: i32,
    pub principal_id: i32,
    pub name: Option<String>,
    pub description: Option<String>,
    pub issued: NaiveDateTime,
    pub expires_at: Option<NaiveDateTime>,
    pub last_used_at: Option<NaiveDateTime>,
    pub revoked_at: Option<NaiveDateTime>,
    pub scoped: bool,
}

impl From<PrincipalToken> for PrincipalTokenMetadata {
    fn from(value: PrincipalToken) -> Self {
        Self {
            id: value.id,
            principal_id: value.principal_id,
            name: value.name,
            description: value.description,
            issued: value.issued,
            expires_at: value.expires_at,
            last_used_at: value.last_used_at,
            revoked_at: value.revoked_at,
            scoped: value.scoped,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Token(pub String);

impl Token {
    pub fn get_token(&self) -> String {
        self.0.clone()
    }

    /// Return a string where we only expose the first three and last three characters.
    /// The middle part is replaced with "..."
    pub fn obfuscate(&self) -> String {
        let chars: Vec<char> = self.0.chars().collect();
        if chars.len() > 6 {
            let start: String = chars[..3].iter().collect();
            let end: String = chars[chars.len() - 3..].iter().collect();
            format!("{start}...{end}")
        } else {
            "...".to_string()
        }
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_token_record(backend.db_pool()).await
    }

    pub fn storage_hash(&self) -> String {
        Self::storage_hash_from_raw(&self.0)
    }

    pub fn storage_hash_from_raw(raw_token: &str) -> String {
        type HmacSha256 = Hmac<Sha256>;

        let mut mac =
            HmacSha256::new_from_slice(token_hash_key_bytes()).expect("invalid HMAC key length");
        mac.update(raw_token.as_bytes());
        let digest = mac.finalize().into_bytes();
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Soft-revoke a token by id, scoped to the owning principal. Filtering on BOTH
/// ids prevents a manager of principal A from revoking principal B's token by
/// guessing its id. Returns the number of rows updated (0 = not found / not theirs).
pub async fn revoke_token_by_id_for_principal<C>(
    backend: &C,
    token_id: i32,
    principal: i32,
) -> Result<usize, ApiError>
where
    C: BackendContext + ?Sized,
{
    use crate::db::with_connection;
    use crate::schema::tokens::dsl::{id, principal_id, revoked_at, tokens};
    with_connection(backend.db_pool(), |conn| {
        diesel::update(
            tokens
                .filter(id.eq(token_id))
                .filter(principal_id.eq(principal))
                .filter(revoked_at.is_null()),
        )
        .set(revoked_at.eq(diesel::dsl::now))
        .execute(conn)
    })
}

/// Create a named/expiring/optionally-scoped token for a principal and return
/// the raw value (shown once). Fail-closed: `scoped` is set in the same insert
/// as the token row, before the scope rows, so a mid-transaction failure can
/// never leave a `scoped = false` (full-authority) token with missing scopes.
pub async fn create_principal_token<C>(
    backend: &C,
    principal: i32,
    name: Option<&str>,
    description: Option<&str>,
    expires_at: Option<chrono::NaiveDateTime>,
    scopes: Option<&[crate::models::Permissions]>,
) -> Result<Token, ApiError>
where
    C: BackendContext + ?Sized,
{
    use crate::db::with_transaction;
    use crate::models::principal::PrincipalKind;
    use crate::schema::{principals, service_accounts, tokens};

    let raw = crate::utilities::auth::generate_token();
    let hash = raw.storage_hash();
    let scoped = scopes.is_some();
    let scope_strings: Vec<String> = scopes
        .map(|s| s.iter().map(|p| p.to_string()).collect())
        .unwrap_or_default();
    let name = name.map(|s| s.to_string());
    let description = description.map(|s| s.to_string());

    with_transaction(backend.db_pool(), |conn| -> Result<(), ApiError> {
        let principal_kind = principals::table
            .filter(principals::id.eq(principal))
            .select(principals::kind)
            .first::<String>(conn)?;

        // Lock the SA row in the same transaction as insert so disable-vs-mint
        // races fail closed.
        if principal_kind == PrincipalKind::ServiceAccount.as_str() {
            let disabled_at = service_accounts::table
                .filter(service_accounts::id.eq(principal))
                .for_update()
                .select(service_accounts::disabled_at)
                .first::<Option<chrono::NaiveDateTime>>(conn)?;

            if disabled_at.is_some() {
                return Err(ApiError::Conflict(
                    "Service account is disabled".to_string(),
                ));
            }
        }

        let new_token_id: i32 = diesel::insert_into(tokens::table)
            .values((
                tokens::token.eq(&hash),
                tokens::principal_id.eq(principal),
                tokens::name.eq(&name),
                tokens::description.eq(&description),
                tokens::expires_at.eq(expires_at),
                tokens::scoped.eq(scoped),
            ))
            .returning(tokens::id)
            .get_result(conn)?;

        for permission in &scope_strings {
            diesel::insert_into(crate::schema::token_scopes::table)
                .values((
                    crate::schema::token_scopes::token_id.eq(new_token_id),
                    crate::schema::token_scopes::permission.eq(permission),
                ))
                .execute(conn)?;
        }
        Ok(())
    })?;

    Ok(raw)
}

impl CursorPaginated for PrincipalToken {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::IssuedAt
                | FilterField::ExpiresAt
                | FilterField::LastUsedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => match &self.name {
                Some(name) => CursorValue::String(name.clone()),
                None => CursorValue::Null,
            },
            FilterField::IssuedAt => CursorValue::DateTime(self.issued),
            FilterField::ExpiresAt => match self.expires_at {
                Some(value) => CursorValue::DateTime(value),
                None => CursorValue::Null,
            },
            FilterField::LastUsedAt => match self.last_used_at {
                Some(value) => CursorValue::DateTime(value),
                None => CursorValue::Null,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for tokens",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::IssuedAt,
                descending: true,
            },
            // id is a unique, non-null tie-breaker — required now that the
            // orderable columns (name/expires_at/last_used_at) are nullable and
            // non-unique.
            SortParam {
                field: FilterField::Id,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }
}

impl CursorSqlMapping for PrincipalToken {
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
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for tokens",
                    field
                )));
            }
        })
    }
}
