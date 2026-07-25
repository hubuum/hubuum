use chrono::NaiveDateTime;

use crate::db::prelude::*;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::ToSchema;

use crate::config::{DEFAULT_TOKEN_LIFETIME_HOURS, get_config, token_hash_key_bytes};
use crate::db::traits::user::DeleteTokenRecord;
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::{FilterField, SortParam};
use crate::models::{PrincipalID, TokenScope, TokenScopeDetails};
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
    pub permission_scoped: bool,
    pub resource_scoped: bool,
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`PrincipalToken`].
    pub struct TokenID;
    noun = "token id";
}

#[derive(Debug, Clone)]
pub struct PrincipalTokenCreateRequest {
    principal_id: PrincipalID,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<NaiveDateTime>,
    scope: Option<TokenScope>,
}

pub(crate) struct PrincipalTokenCreateParts {
    pub principal_id: PrincipalID,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expires_at: Option<NaiveDateTime>,
    pub scope: Option<TokenScope>,
}

impl PrincipalTokenCreateRequest {
    pub fn new(principal_id: PrincipalID) -> Self {
        Self {
            principal_id,
            name: None,
            description: None,
            expires_at: None,
            scope: None,
        }
    }

    pub fn name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    pub fn description(mut self, description: Option<String>) -> Self {
        self.description = description;
        self
    }

    pub fn expires_at(mut self, expires_at: Option<NaiveDateTime>) -> Self {
        self.expires_at = expires_at;
        self
    }

    pub fn scope(mut self, scope: Option<TokenScope>) -> Self {
        self.scope = scope;
        self
    }

    pub fn is_scoped(&self) -> bool {
        self.scope.is_some()
    }

    /// Persist this token request and return its raw bearer value once.
    ///
    /// The token row and every scope row are written in one transaction. Scope
    /// flags are stored on the token row before child rows are inserted, so a
    /// partial failure cannot create an unrestricted credential.
    pub async fn create<C>(
        self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<Token, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        Ok(self.create_issued(backend, context).await?.into_token())
    }

    /// Persist this token request and return both its raw bearer value and
    /// authoritative expiry.
    ///
    /// An omitted expiry is materialized during persistence from the same
    /// database timestamp stored as `issued`, so later configuration changes
    /// cannot alter the lifetime of an already-issued token.
    pub async fn create_issued<C>(
        self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<IssuedToken, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let default_lifetime_hours = configured_default_token_lifetime_hours();
        let (token, persisted) = crate::db::traits::token::create_principal_token_request_db(
            backend.db_pool(),
            self,
            default_lifetime_hours,
            context,
        )
        .await?;
        let expires_at = persisted.expires_at.ok_or_else(|| {
            ApiError::InternalServerError(
                "newly issued token is missing its persisted expiry".to_string(),
            )
        })?;
        Ok(IssuedToken::new(token, expires_at))
    }

    pub(crate) fn into_parts(self) -> PrincipalTokenCreateParts {
        PrincipalTokenCreateParts {
            principal_id: self.principal_id,
            name: self.name,
            description: self.description,
            expires_at: self.expires_at,
            scope: self.scope,
        }
    }
}

/// Public, hash-free projection of a token for listing, including its exact
/// permission and resource scope dimensions.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct PrincipalTokenMetadata {
    pub id: TokenID,
    pub principal_id: PrincipalID,
    pub name: Option<String>,
    pub description: Option<String>,
    pub issued: NaiveDateTime,
    pub expires_at: Option<NaiveDateTime>,
    pub last_used_at: Option<NaiveDateTime>,
    pub revoked_at: Option<NaiveDateTime>,
    /// Exact permission and resource boundaries. `None` means that this token
    /// is unscoped.
    pub scope: Option<TokenScopeDetails>,
}

/// Public metadata for the token authenticating the current request.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CurrentTokenMetadata {
    pub id: TokenID,
    pub name: Option<String>,
    pub description: Option<String>,
    pub issued: NaiveDateTime,
    pub expires_at: Option<NaiveDateTime>,
    pub last_used_at: Option<NaiveDateTime>,
    /// Exact permission and resource boundaries. `None` means that this token
    /// is unscoped.
    pub scope: Option<TokenScopeDetails>,
}

impl PrincipalTokenMetadata {
    /// Load exact scope metadata for every supplied token in a bounded number
    /// of database queries.
    ///
    /// The result preserves the input length and order, including repeated
    /// token rows.
    pub async fn load_for_tokens<C>(
        backend: &C,
        tokens: &[PrincipalToken],
    ) -> Result<Vec<Self>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        crate::db::traits::token::principal_token_metadata_db(backend.db_pool(), tokens).await
    }

    pub(crate) fn from_token_and_scope(
        value: &PrincipalToken,
        scope: Option<TokenScope>,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            id: value.metadata_id()?,
            principal_id: value.metadata_principal_id()?,
            name: value.name.clone(),
            description: value.description.clone(),
            issued: value.issued,
            expires_at: value.expires_at,
            last_used_at: value.last_used_at,
            revoked_at: value.revoked_at,
            scope: value.scope_details(scope)?,
        })
    }
}

impl CurrentTokenMetadata {
    /// Project a validated persisted token and its loaded scope for `/iam/me`.
    pub fn from_token_and_scope(
        value: &PrincipalToken,
        scope: Option<TokenScope>,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            id: value.metadata_id()?,
            name: value.name.clone(),
            description: value.description.clone(),
            issued: value.issued,
            expires_at: value.expires_at,
            last_used_at: value.last_used_at,
            scope: value.scope_details(scope)?,
        })
    }
}

impl PrincipalToken {
    pub fn is_scoped(&self) -> bool {
        self.permission_scoped || self.resource_scoped
    }

    fn metadata_id(&self) -> Result<TokenID, ApiError> {
        TokenID::new(self.id).map_err(|_| {
            ApiError::InternalServerError(format!(
                "Stored token has invalid identifier {}",
                self.id
            ))
        })
    }

    fn metadata_principal_id(&self) -> Result<PrincipalID, ApiError> {
        PrincipalID::new(self.principal_id).map_err(|_| {
            ApiError::InternalServerError(format!(
                "Stored token has invalid principal identifier {}",
                self.principal_id
            ))
        })
    }

    fn scope_details(
        &self,
        scope: Option<TokenScope>,
    ) -> Result<Option<TokenScopeDetails>, ApiError> {
        match (self.is_scoped(), scope) {
            (false, None) => Ok(None),
            (true, Some(scope)) => TokenScopeDetails::from_scope(scope).map(Some),
            (false, Some(_)) => Err(ApiError::InternalServerError(format!(
                "Unscoped token {} has stored scope rows",
                self.id
            ))),
            (true, None) => Err(ApiError::InternalServerError(format!(
                "Scoped token {} has no stored scope",
                self.id
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Token(pub String);

/// A newly persisted bearer token paired with its authoritative expiry.
#[derive(Clone)]
pub struct IssuedToken {
    token: Token,
    expires_at: NaiveDateTime,
}

impl IssuedToken {
    pub(crate) fn new(token: Token, expires_at: NaiveDateTime) -> Self {
        Self { token, expires_at }
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn get_token(&self) -> String {
        self.token.get_token()
    }

    pub fn expires_at(&self) -> NaiveDateTime {
        self.expires_at
    }

    pub fn into_token(self) -> Token {
        self.token
    }
}

pub(crate) fn configured_default_token_lifetime_hours() -> i64 {
    get_config()
        .map(|config| config.token_lifetime_hours)
        .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS)
}

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
    token_id: TokenID,
    principal_id: PrincipalID,
) -> Result<usize, ApiError>
where
    C: BackendContext + ?Sized,
{
    crate::db::traits::token::revoke_token_by_id_for_principal_without_events_db(
        backend.db_pool(),
        token_id.id(),
        principal_id.id(),
    )
    .await
}

pub async fn revoke_token_by_id_for_principal<C>(
    backend: &C,
    token_id: TokenID,
    principal_id: PrincipalID,
    context: Option<&EventContext>,
) -> Result<usize, ApiError>
where
    C: BackendContext + ?Sized,
{
    crate::db::traits::token::revoke_token_by_id_for_principal_db(
        backend.db_pool(),
        token_id.id(),
        principal_id.id(),
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
