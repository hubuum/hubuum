use std::{fmt, str::FromStr};

use chrono::NaiveDateTime;

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::ToSchema;

use crate::config::{TokenHashKeyRing, get_config, token_hash_key_ring};
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    PrincipalID, REDACTED_DEBUG_VALUE, ResourceRevision, TokenIssuancePolicy, TokenLifetime,
    TokenScope, TokenScopeDetails,
};
use crate::storage::{
    StorageAuthenticatedToken, StorageAuthenticationCredential, StorageContext, StorageTokenDigest,
    StorageTokenFormat, StorageTokenHashAlgorithm, StorageTokenHashKeyId,
};
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

pub use hubuum_domain::TokenId as TokenID;

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
    pub async fn create<C>(self, backend: &C, context: &EventContext) -> Result<Token, ApiError>
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
        context: &EventContext,
    ) -> Result<IssuedToken, ApiError>
    where
        C: StorageContext,
    {
        let issuance_policy = configured_token_issuance_policy()?;
        crate::services::identity::create_token(backend, self, issuance_policy, context).await
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
        crate::services::identity::load_token_metadata_by_ids(backend, tokens).await
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
        crate::services::identity::get_token_metadata(backend, principal_id.id(), token_id.id())
            .await
    }
}

impl CurrentTokenMetadata {
    /// Project a validated persisted token and its loaded scope for `/iam/me`.
    pub fn from_authenticated_token(
        value: &StorageAuthenticatedToken,
        scope: Option<TokenScope>,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            id: TokenID::new(value.id().id()).map_err(|_| {
                ApiError::InternalServerError(format!(
                    "Authenticated token has invalid identifier {}",
                    value.id().id()
                ))
            })?,
            name: value.name().map(str::to_string),
            description: value.description().map(str::to_string),
            issued: value.issued().naive_utc(),
            expires_at: value.expires_at().map(|timestamp| timestamp.naive_utc()),
            last_used_at: value.last_used_at().map(|timestamp| timestamp.naive_utc()),
            scope: token_scope_details(value.id().id(), value.is_scoped(), scope)?,
            revision: value.revision(),
        })
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

pub(crate) struct TokenCredentialPlan {
    pub credentials: Vec<StorageAuthenticationCredential>,
    pub migration_target: Option<StorageTokenDigest>,
    pub format: StorageTokenFormat,
    pub key_state: &'static str,
}

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
    pub async fn authenticate<C>(&self, backend: &C) -> Result<StorageAuthenticatedToken, ApiError>
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
        crate::services::identity::revoke_token_by_hash(
            backend,
            None,
            self.credentials()?.credentials,
            &EventContext::system(),
        )
        .await?;
        Ok(())
    }

    pub fn storage_hash(&self) -> String {
        self.storage_digest()
            .expect("issued and legacy token values must be hashable")
            .lookup_value()
            .to_string()
    }

    pub fn storage_hash_from_raw(raw_token: &str) -> String {
        let ring = token_hash_key_ring()
            .expect("token hash key-ring configuration must be validated at startup");
        Self::storage_hash_from_raw_with_ring(raw_token, ring)
    }

    fn storage_hash_from_raw_with_ring(raw_token: &str, ring: &TokenHashKeyRing) -> String {
        let key = raw_token
            .strip_prefix("hbt1.")
            .and_then(|value| value.split_once('.'))
            .and_then(|(key_id, _)| StorageTokenHashKeyId::try_new(key_id).ok())
            .and_then(|key_id| ring.key_bytes(&key_id))
            .unwrap_or_else(|| ring.active_key_bytes());
        hash_with_key(raw_token, key)
    }

    pub(crate) fn issued(secret: impl AsRef<str>) -> Result<Self, ApiError> {
        let ring = token_hash_key_ring()
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        Self::issued_with_ring(secret, ring)
    }

    fn issued_with_ring(
        secret: impl AsRef<str>,
        ring: &TokenHashKeyRing,
    ) -> Result<Self, ApiError> {
        let secret = secret.as_ref();
        if !is_valid_token_secret(secret) {
            return Err(ApiError::InternalServerError(
                "generated token secret has an invalid representation".to_string(),
            ));
        }
        Ok(Self(format!("hbt1.{}.{}", ring.active_key_id(), secret)))
    }

    pub(crate) fn storage_digest(&self) -> Result<StorageTokenDigest, ApiError> {
        let plan = self.credentials()?;
        plan.credentials
            .into_iter()
            .next()
            .map(|credential| credential.digest().clone())
            .ok_or_else(|| ApiError::InternalServerError("token digest plan was empty".to_string()))
    }

    pub(crate) fn credentials(&self) -> Result<TokenCredentialPlan, ApiError> {
        let ring = token_hash_key_ring()
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        self.credentials_with_ring(ring)
    }

    fn credentials_with_ring(
        &self,
        ring: &TokenHashKeyRing,
    ) -> Result<TokenCredentialPlan, ApiError> {
        if self.0.starts_with("hbt") {
            let mut parts = self.0.split('.');
            let version = parts.next();
            let key_id = parts.next();
            let secret = parts.next();
            if version != Some("hbt1")
                || parts.next().is_some()
                || secret.is_none_or(|value| !is_valid_token_secret(value))
            {
                return Err(invalid_bearer_token());
            }
            let key_id = StorageTokenHashKeyId::try_new(key_id.unwrap_or_default())
                .map_err(|_| invalid_bearer_token())?;
            let key = ring.key_bytes(&key_id).ok_or_else(invalid_bearer_token)?;
            let digest =
                digest_with_key(&self.0, key, StorageTokenFormat::Version1, key_id.clone())?;
            let key_state = if &key_id == ring.active_key_id() {
                "active"
            } else {
                "previous"
            };
            return Ok(TokenCredentialPlan {
                credentials: vec![StorageAuthenticationCredential::from_digest(digest)],
                migration_target: None,
                format: StorageTokenFormat::Version1,
                key_state,
            });
        }

        let credentials = ring
            .keys()
            .map(|(key_id, key)| {
                digest_with_key(&self.0, key, StorageTokenFormat::Legacy, key_id.clone())
                    .map(StorageAuthenticationCredential::from_digest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let migration_target = credentials
            .first()
            .map(|credential| credential.digest().clone());
        Ok(TokenCredentialPlan {
            credentials,
            migration_target,
            format: StorageTokenFormat::Legacy,
            key_state: "legacy",
        })
    }

    #[cfg(test)]
    fn storage_digest_with_ring(
        &self,
        ring: &TokenHashKeyRing,
    ) -> Result<StorageTokenDigest, ApiError> {
        self.credentials_with_ring(ring)?
            .credentials
            .into_iter()
            .next()
            .map(|credential| credential.digest().clone())
            .ok_or_else(|| ApiError::InternalServerError("token digest plan was empty".to_string()))
    }
}

fn is_valid_token_secret(value: &str) -> bool {
    value.len() == 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_with_key(
    raw_token: &str,
    key: &[u8],
    format: StorageTokenFormat,
    key_id: StorageTokenHashKeyId,
) -> Result<StorageTokenDigest, ApiError> {
    StorageTokenDigest::try_new(
        hash_with_key(raw_token, key),
        format,
        StorageTokenHashAlgorithm::HmacSha256V1,
        Some(key_id),
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))
}

fn hash_with_key(raw_token: &str, key: &[u8]) -> String {
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(raw_token.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn invalid_bearer_token() -> ApiError {
    ApiError::Unauthorized("Invalid token".to_string())
}

/// Soft-revoke a token by id, scoped to the owning principal. Filtering on BOTH
/// ids prevents a manager of principal A from revoking principal B's token by
/// guessing its id. Returns the number of rows updated (0 = not found / not theirs).
///
/// The compatibility name is retained for internal callers; the mutation is
/// attributed to the system actor and still emits its audit event.
pub async fn revoke_token_by_id_for_principal_without_events<C>(
    backend: &C,
    token_id: TokenID,
    principal_id: PrincipalID,
) -> Result<usize, ApiError>
where
    C: StorageContext,
{
    crate::services::identity::revoke_token(
        backend,
        token_id.id(),
        principal_id.id(),
        &EventContext::system(),
    )
    .await
}

pub async fn revoke_token_by_id_for_principal<C>(
    backend: &C,
    token_id: TokenID,
    principal_id: PrincipalID,
    context: &EventContext,
) -> Result<usize, ApiError>
where
    C: StorageContext,
{
    crate::services::identity::revoke_token(backend, token_id.id(), principal_id.id(), context)
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
    context: &EventContext,
) -> Result<IssuedToken, ApiError>
where
    C: StorageContext,
{
    let issuance_policy = configured_token_issuance_policy()?;
    crate::services::identity::renew_token(
        backend,
        token_id.id(),
        principal_id.id(),
        expires_at,
        issuance_policy,
        context,
    )
    .await
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

    fn rotation_ring(active: &str, previous: &[&str]) -> TokenHashKeyRing {
        let material = |id: &str| match id {
            "old" => vec![1; 32],
            "new" => vec![2; 32],
            _ => panic!("unexpected rotation-test key ID"),
        };
        let entries = std::iter::once(active)
            .chain(previous.iter().copied())
            .map(|id| (StorageTokenHashKeyId::try_new(id).unwrap(), material(id)))
            .collect();
        TokenHashKeyRing::try_new(entries, true, true).unwrap()
    }

    fn token_verifies(
        token: &Token,
        persisted: &StorageTokenDigest,
        verifier: &TokenHashKeyRing,
    ) -> bool {
        token.credentials_with_ring(verifier).is_ok_and(|plan| {
            plan.credentials
                .iter()
                .any(|candidate| candidate.digest() == persisted)
        })
    }

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

    #[test]
    fn newly_issued_tokens_carry_the_active_key_id() {
        let token = Token::issued("a".repeat(128)).unwrap();
        let ring = token_hash_key_ring().unwrap();

        assert!(
            token
                .0
                .starts_with(&format!("hbt1.{}.", ring.active_key_id()))
        );
        let plan = token.credentials().unwrap();
        assert_eq!(plan.format, StorageTokenFormat::Version1);
        assert_eq!(plan.credentials.len(), 1);
        assert!(plan.migration_target.is_none());
    }

    #[test]
    fn token_issuance_rejects_an_invalid_generated_secret() {
        let ring = rotation_ring("old", &[]);

        assert!(Token::issued_with_ring("not-hex", &ring).is_err());
    }

    #[test]
    fn malformed_versioned_tokens_do_not_fall_back_to_legacy_hashing() {
        let token = Token("hbt2.legacy.not-a-version-one-secret".to_string());

        assert!(matches!(
            token.credentials(),
            Err(ApiError::Unauthorized(_))
        ));
    }

    #[test]
    fn legacy_tokens_generate_one_bounded_candidate_per_configured_key() {
        let token = Token("legacy-bearer-value".to_string());
        let ring = token_hash_key_ring().unwrap();
        let plan = token.credentials().unwrap();

        assert_eq!(plan.format, StorageTokenFormat::Legacy);
        assert_eq!(plan.credentials.len(), ring.key_ids().count());
        assert!(plan.migration_target.is_some());
    }

    #[test]
    fn supported_replica_ring_combinations_validate_each_others_tokens() {
        let old_only = rotation_ring("old", &[]);
        let old_with_future = rotation_ring("old", &["new"]);
        let new_with_old = rotation_ring("new", &["old"]);
        let new_only = rotation_ring("new", &[]);

        let old_token = Token::issued_with_ring("a".repeat(128), &old_only).unwrap();
        let old_digest = old_token.storage_digest_with_ring(&old_only).unwrap();
        assert!(token_verifies(&old_token, &old_digest, &old_only));
        assert!(token_verifies(&old_token, &old_digest, &old_with_future));
        assert!(token_verifies(&old_token, &old_digest, &new_with_old));

        let staged_token = Token::issued_with_ring("b".repeat(128), &old_with_future).unwrap();
        let staged_digest = staged_token
            .storage_digest_with_ring(&old_with_future)
            .unwrap();
        assert!(token_verifies(&staged_token, &staged_digest, &old_only));
        assert!(token_verifies(
            &staged_token,
            &staged_digest,
            &old_with_future
        ));
        assert!(token_verifies(&staged_token, &staged_digest, &new_with_old));

        let switched_token = Token::issued_with_ring("c".repeat(128), &new_with_old).unwrap();
        let switched_digest = switched_token
            .storage_digest_with_ring(&new_with_old)
            .unwrap();
        assert!(token_verifies(
            &switched_token,
            &switched_digest,
            &old_with_future
        ));
        assert!(token_verifies(
            &switched_token,
            &switched_digest,
            &new_with_old
        ));
        assert!(token_verifies(&switched_token, &switched_digest, &new_only));

        let retired_token = Token::issued_with_ring("d".repeat(128), &new_only).unwrap();
        let retired_digest = retired_token.storage_digest_with_ring(&new_only).unwrap();
        assert!(token_verifies(
            &retired_token,
            &retired_digest,
            &new_with_old
        ));
        assert!(token_verifies(&retired_token, &retired_digest, &new_only));

        assert!(!token_verifies(
            &switched_token,
            &switched_digest,
            &old_only
        ));
        assert!(!token_verifies(&old_token, &old_digest, &new_only));
    }
}
