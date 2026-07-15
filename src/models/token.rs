use chrono::NaiveDateTime;

use crate::db::prelude::*;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::ToSchema;

use crate::config::token_hash_key_bytes;
use crate::db::traits::user::DeleteTokenRecord;
use crate::errors::ApiError;
use crate::events::EventContext;
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
///
/// This bypasses event emission and is intended only for internal
/// infrastructure paths such as cleanup and event-system tests.
pub async fn revoke_token_by_id_for_principal_without_events<C>(
    backend: &C,
    token_id: i32,
    principal: i32,
) -> Result<usize, ApiError>
where
    C: BackendContext + ?Sized,
{
    crate::db::traits::token::revoke_token_by_id_for_principal_without_events_db(
        backend.db_pool(),
        token_id,
        principal,
    )
    .await
}

pub async fn revoke_token_by_id_for_principal<C>(
    backend: &C,
    token_id: i32,
    principal: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError>
where
    C: BackendContext + ?Sized,
{
    crate::db::traits::token::revoke_token_by_id_for_principal_db(
        backend.db_pool(),
        token_id,
        principal,
        context,
    )
    .await
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
    context: Option<&EventContext>,
) -> Result<Token, ApiError>
where
    C: BackendContext + ?Sized,
{
    crate::db::traits::token::create_principal_token_db(
        backend.db_pool(),
        principal,
        name,
        description,
        expires_at,
        scopes,
        context,
    )
    .await
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
