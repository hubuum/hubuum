use std::{fmt, str::FromStr};

use chrono::NaiveDateTime;

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::ToSchema;

use crate::config::{get_config, token_hash_key_bytes};
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    PrincipalID, REDACTED_DEBUG_VALUE, ResourceRevision, TokenIssuancePolicy, TokenLifetime,
    TokenScope, TokenScopeDetails,
};
use crate::storage::postgres::operations::user::DeleteTokenRecord;
use crate::storage::{AuthenticatedToken, StorageContext};
use crate::traits::{CursorPaginated, CursorValue};

/// A persisted bearer token, keyed to a principal, with a full lifecycle. The
/// `token` field stores the HMAC hash, never the raw value.
#[derive(Clone)]
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
    pub revision: ResourceRevision,
}

impl fmt::Debug for PrincipalToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrincipalToken")
            .field("id", &self.id)
            .field("token", &REDACTED_DEBUG_VALUE)
            .field("principal_id", &self.principal_id)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("issued", &self.issued)
            .field("expires_at", &self.expires_at)
            .field("last_used_at", &self.last_used_at)
            .field("revoked_at", &self.revoked_at)
            .field("permission_scoped", &self.permission_scoped)
            .field("resource_scoped", &self.resource_scoped)
            .finish()
    }
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
        C: StorageContext,
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
        C: StorageContext,
    {
        let issuance_policy = configured_token_issuance_policy()?;
        let (token, persisted) =
            crate::storage::postgres::operations::token::create_principal_token_request_db(
                backend,
                self,
                issuance_policy,
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
    /// Whether this token can currently authenticate. This uses the same
    /// expiry and revocation rules as bearer-token validation.
    pub active: bool,
    /// Whether this token's effective expiry has elapsed. A revoked token may
    /// also be expired, so this is independent of `revoked_at`.
    pub expired: bool,
    /// Exact permission and resource boundaries. `None` means that this token
    /// is unscoped.
    pub scope: Option<TokenScopeDetails>,
    pub revision: ResourceRevision,
}

/// Canonical token representation covered completely by the token revision.
/// `last_used_at` remains available in token lists, but is excluded from this
/// point representation because routine activity does not advance revision.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct PrincipalTokenPointResponse {
    pub id: TokenID,
    pub principal_id: PrincipalID,
    pub name: Option<String>,
    pub description: Option<String>,
    pub issued: NaiveDateTime,
    pub expires_at: Option<NaiveDateTime>,
    pub revoked_at: Option<NaiveDateTime>,
    pub scope: Option<TokenScopeDetails>,
    pub revision: ResourceRevision,
}

impl From<PrincipalTokenMetadata> for PrincipalTokenPointResponse {
    fn from(token: PrincipalTokenMetadata) -> Self {
        Self {
            id: token.id,
            principal_id: token.principal_id,
            name: token.name,
            description: token.description,
            issued: token.issued,
            expires_at: token.expires_at,
            revoked_at: token.revoked_at,
            scope: token.scope,
            revision: token.revision,
        }
    }
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
    pub revision: ResourceRevision,
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
        C: StorageContext,
    {
        crate::storage::postgres::operations::token::principal_token_metadata_db(backend, tokens)
            .await
    }

    /// Load one retained token by id, constrained to its owning principal.
    ///
    /// Unlike authentication and the default token list, this lookup does not
    /// exclude expired or revoked rows. It remains available until retention
    /// purges the token.
    pub async fn load_for_principal_token<C>(
        backend: &C,
        principal_id: PrincipalID,
        token_id: TokenID,
    ) -> Result<Self, ApiError>
    where
        C: StorageContext,
    {
        crate::storage::postgres::operations::token::principal_token_metadata_by_id_for_principal_db(
            backend,
            token_id.id(),
            principal_id.id(),
        )
        .await
    }

    pub(crate) fn from_token_and_scope(
        value: &PrincipalToken,
        scope: Option<TokenScope>,
        now: NaiveDateTime,
        active_after: NaiveDateTime,
    ) -> Result<Self, ApiError> {
        let expired = value.is_expired_at(now, active_after);
        Ok(Self {
            id: value.metadata_id()?,
            principal_id: value.metadata_principal_id()?,
            name: value.name.clone(),
            description: value.description.clone(),
            issued: value.issued,
            expires_at: value.expires_at,
            last_used_at: value.last_used_at,
            revoked_at: value.revoked_at,
            active: value.revoked_at.is_none() && !expired,
            expired,
            scope: value.scope_details(scope)?,
            revision: value.revision,
        })
    }
}

impl CurrentTokenMetadata {
    /// Project a validated persisted token and its loaded scope for `/iam/me`.
    pub fn from_authenticated_token(
        value: &AuthenticatedToken,
        scope: Option<TokenScope>,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            id: TokenID::new(value.id()).map_err(|_| {
                ApiError::InternalServerError(format!(
                    "Authenticated token has invalid identifier {}",
                    value.id()
                ))
            })?,
            name: value.name().map(str::to_string),
            description: value.description().map(str::to_string),
            issued: value.issued(),
            expires_at: value.expires_at(),
            last_used_at: value.last_used_at(),
            scope: token_scope_details(value.id(), value.is_scoped(), scope)?,
            revision: ResourceRevision::new(value.revision()).map_err(|_| {
                ApiError::InternalServerError(format!(
                    "Authenticated token {} has invalid revision",
                    value.id()
                ))
            })?,
        })
    }
}

impl PrincipalToken {
    pub fn is_scoped(&self) -> bool {
        self.permission_scoped || self.resource_scoped
    }

    pub(crate) fn is_expired_at(&self, now: NaiveDateTime, active_after: NaiveDateTime) -> bool {
        self.expires_at
            .map_or(self.issued <= active_after, |expires_at| expires_at <= now)
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
        token_scope_details(self.id, self.is_scoped(), scope)
    }
}

fn token_scope_details(
    token_id: i32,
    is_scoped: bool,
    scope: Option<TokenScope>,
) -> Result<Option<TokenScopeDetails>, ApiError> {
    match (is_scoped, scope) {
        (false, None) => Ok(None),
        (true, Some(scope)) => TokenScopeDetails::from_scope(scope).map(Some),
        (false, Some(_)) => Err(ApiError::InternalServerError(format!(
            "Unscoped token {} has stored scope rows",
            token_id
        ))),
        (true, None) => Err(ApiError::InternalServerError(format!(
            "Scoped token {} has no stored scope",
            token_id
        ))),
    }
}

/// Lifecycle subset selected by a token-management list endpoint.
///
/// `Expired` and `Revoked` intentionally overlap: a revoked token remains
/// expired once its effective expiry elapses. `Active` is the validation
/// predicate, while `All` returns every retained row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TokenListState {
    #[default]
    Active,
    Expired,
    Revoked,
    All,
}

impl FromStr for TokenListState {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "expired" => Ok(Self::Expired),
            "revoked" => Ok(Self::Revoked),
            "all" => Ok(Self::All),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid token state '{value}'; expected active, expired, revoked, or all"
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

pub(crate) fn configured_token_issuance_policy() -> Result<TokenIssuancePolicy, ApiError> {
    get_config()?.token_issuance_policy()
}

pub(crate) fn configured_token_lifetime() -> Result<TokenLifetime, ApiError> {
    Ok(configured_token_issuance_policy()?.default_lifetime())
}

impl Token {
    pub fn get_token(&self) -> String {
        self.0.clone()
    }

    /// Validate this bearer token through the selected complete storage
    /// backend and return its hash-free authentication projection.
    pub async fn authenticate<C>(&self, backend: &C) -> Result<AuthenticatedToken, ApiError>
    where
        C: StorageContext,
    {
        crate::services::authentication::authenticate_bearer_token(backend, self).await
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
        C: StorageContext,
    {
        self.delete_token_record(backend).await
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
    C: StorageContext,
{
    crate::storage::postgres::operations::token::revoke_token_by_id_for_principal_without_events_db(
        backend,
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
    C: StorageContext,
{
    crate::storage::postgres::operations::token::revoke_token_by_id_for_principal_db(
        backend,
        token_id.id(),
        principal_id.id(),
        context,
    )
    .await
}

/// Mint a fresh token by copying one retained token's descriptive metadata and
/// exact permission/resource boundary.
///
/// The source token is never reactivated or modified. Explicitly revoked
/// sources are rejected; active and expired sources may be renewed.
pub async fn renew_token_by_id_for_principal<C>(
    backend: &C,
    token_id: TokenID,
    principal_id: PrincipalID,
    expires_at: Option<NaiveDateTime>,
    context: Option<&EventContext>,
) -> Result<IssuedToken, ApiError>
where
    C: StorageContext,
{
    let issuance_policy = configured_token_issuance_policy()?;
    let (token, persisted) = crate::storage::postgres::operations::token::renew_principal_token_db(
        backend,
        token_id.id(),
        principal_id.id(),
        expires_at,
        issuance_policy,
        context,
    )
    .await?;
    let expires_at = persisted.expires_at.ok_or_else(|| {
        ApiError::InternalServerError(
            "newly renewed token is missing its persisted expiry".to_string(),
        )
    })?;
    Ok(IssuedToken::new(token, expires_at))
}

impl CursorPaginated for PrincipalTokenMetadata {
    fn supports_sort(field: &FilterField) -> bool {
        PrincipalToken::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id.id() as i64),
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
        PrincipalToken::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        PrincipalToken::tie_breaker_sort()
    }
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
                | FilterField::Revision
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
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_token_debug_redacts_stored_digest() {
        let token_digest = "keyed-token-digest";
        let token = PrincipalToken {
            id: 1,
            token: token_digest.to_string(),
            principal_id: 2,
            name: Some("automation".to_string()),
            description: None,
            issued: chrono::DateTime::from_timestamp(1_700_000_000, 0)
                .unwrap()
                .naive_utc(),
            expires_at: None,
            last_used_at: None,
            revoked_at: None,
            permission_scoped: false,
            resource_scoped: false,
            revision: ResourceRevision::INITIAL,
        };

        let output = format!("{token:?}");

        assert!(output.contains(REDACTED_DEBUG_VALUE));
        assert!(!output.contains(token_digest));
    }
}
