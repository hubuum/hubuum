use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, TokenId};
use hubuum_events_core::EventContext;

use crate::{
    MAX_TOKEN_HASH_KEYS, StorageAuthenticationCredential, StorageAuthenticationTokenScope,
    StorageError, StorageMutationOutcome, StoragePage, StorageTokenDigest, StorageTokenListQuery,
    StorageTokenMetadata, StorageTokenObservation, StorageValidationError,
};

/// Aggregate retirement evidence for one persisted token-hash key identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageTokenKeyUsage {
    key_id: Option<crate::StorageTokenHashKeyId>,
    active: i64,
    revoked: i64,
    expired: i64,
    latest_validation: Option<DateTime<Utc>>,
    earliest_expiry: Option<DateTime<Utc>>,
    latest_expiry: Option<DateTime<Utc>>,
}

impl StorageTokenKeyUsage {
    pub fn try_new(
        key_id: Option<crate::StorageTokenHashKeyId>,
        active: i64,
        revoked: i64,
        expired: i64,
        latest_validation: Option<DateTime<Utc>>,
        earliest_expiry: Option<DateTime<Utc>>,
        latest_expiry: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        if active < 0 || revoked < 0 || expired < 0 {
            return Err(StorageValidationError::invalid(
                "token key usage counts cannot be negative",
            ));
        }
        if earliest_expiry
            .zip(latest_expiry)
            .is_some_and(|(earliest, latest)| earliest > latest)
        {
            return Err(StorageValidationError::invalid(
                "token key usage expiry bounds are inverted",
            ));
        }
        Ok(Self {
            key_id,
            active,
            revoked,
            expired,
            latest_validation,
            earliest_expiry,
            latest_expiry,
        })
    }

    #[must_use]
    pub const fn key_id(&self) -> Option<&crate::StorageTokenHashKeyId> {
        self.key_id.as_ref()
    }

    #[must_use]
    pub const fn active(&self) -> i64 {
        self.active
    }

    #[must_use]
    pub const fn revoked(&self) -> i64 {
        self.revoked
    }

    #[must_use]
    pub const fn expired(&self) -> i64 {
        self.expired
    }

    #[must_use]
    pub const fn latest_validation(&self) -> Option<DateTime<Utc>> {
        self.latest_validation
    }

    #[must_use]
    pub const fn earliest_expiry(&self) -> Option<DateTime<Utc>> {
        self.earliest_expiry
    }

    #[must_use]
    pub const fn latest_expiry(&self) -> Option<DateTime<Utc>> {
        self.latest_expiry
    }
}

/// Named token-creation fields exposed to an adapter.
pub struct StorageTokenCreateParts {
    principal_id: PrincipalId,
    digest: StorageTokenDigest,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    scope: Option<StorageAuthenticationTokenScope>,
    policy: StorageTokenIssuancePolicy,
    event_context: EventContext,
}

impl StorageTokenCreateParts {
    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub const fn digest(&self) -> &StorageTokenDigest {
        &self.digest
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }

    #[must_use]
    pub const fn scope(&self) -> Option<&StorageAuthenticationTokenScope> {
        self.scope.as_ref()
    }

    #[must_use]
    pub const fn policy(&self) -> StorageTokenIssuancePolicy {
        self.policy
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

/// Validated application policy used to materialize token expiry at the
/// backend's authoritative issuance timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageTokenIssuancePolicy {
    default_lifetime_hours: i64,
    maximum_lifetime_hours: i64,
}

impl StorageTokenIssuancePolicy {
    pub const fn try_new(
        default_lifetime_hours: i64,
        maximum_lifetime_hours: i64,
    ) -> Result<Self, StorageTokenIssuancePolicyError> {
        if default_lifetime_hours <= 0 {
            return Err(StorageTokenIssuancePolicyError::NonPositiveDefault);
        }
        if maximum_lifetime_hours <= 0 {
            return Err(StorageTokenIssuancePolicyError::NonPositiveMaximum);
        }
        if default_lifetime_hours > maximum_lifetime_hours {
            return Err(StorageTokenIssuancePolicyError::DefaultExceedsMaximum);
        }
        Ok(Self {
            default_lifetime_hours,
            maximum_lifetime_hours,
        })
    }

    #[must_use]
    pub const fn into_parts(self) -> (i64, i64) {
        (self.default_lifetime_hours, self.maximum_lifetime_hours)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageTokenIssuancePolicyError {
    NonPositiveDefault,
    NonPositiveMaximum,
    DefaultExceedsMaximum,
}

impl fmt::Display for StorageTokenIssuancePolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NonPositiveDefault => "default token lifetime must be positive",
            Self::NonPositiveMaximum => "maximum token lifetime must be positive",
            Self::DefaultExceedsMaximum => {
                "default token lifetime cannot exceed the maximum token lifetime"
            }
        })
    }
}

impl std::error::Error for StorageTokenIssuancePolicyError {}

/// Token issuance input. The raw bearer secret never crosses the storage
/// boundary; only its application-generated HMAC is persisted.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenCreate {
    principal_id: PrincipalId,
    digest: StorageTokenDigest,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    scope: Option<StorageAuthenticationTokenScope>,
    policy: StorageTokenIssuancePolicy,
    event_context: EventContext,
}

impl StorageTokenCreate {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        digest: StorageTokenDigest,
        policy: StorageTokenIssuancePolicy,
        event_context: EventContext,
    ) -> Self {
        Self {
            principal_id,
            digest,
            name: None,
            description: None,
            expires_at: None,
            scope: None,
            policy,
            event_context,
        }
    }

    #[must_use]
    pub fn name(mut self, value: Option<String>) -> Self {
        self.name = value;
        self
    }

    #[must_use]
    pub fn description(mut self, value: Option<String>) -> Self {
        self.description = value;
        self
    }

    #[must_use]
    pub const fn expires_at(mut self, value: Option<DateTime<Utc>>) -> Self {
        self.expires_at = value;
        self
    }

    #[must_use]
    pub fn scope(mut self, value: Option<StorageAuthenticationTokenScope>) -> Self {
        self.scope = value;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> StorageTokenCreateParts {
        StorageTokenCreateParts {
            principal_id: self.principal_id,
            digest: self.digest,
            name: self.name,
            description: self.description,
            expires_at: self.expires_at,
            scope: self.scope,
            policy: self.policy,
            event_context: self.event_context,
        }
    }
}

impl fmt::Debug for StorageTokenCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenCreate")
            .field("principal_id", &"<redacted>")
            .field("digest", &self.digest)
            .field("has_name", &self.name.is_some())
            .field("has_description", &self.description.is_some())
            .field("has_expiry", &self.expires_at.is_some())
            .field("has_scope", &self.scope.is_some())
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Renewal input. The backend copies source metadata and scope atomically into
/// a row carrying the supplied application-generated HMAC.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenRenew {
    source_token_id: TokenId,
    principal_id: PrincipalId,
    digest: StorageTokenDigest,
    expires_at: Option<DateTime<Utc>>,
    policy: StorageTokenIssuancePolicy,
    event_context: EventContext,
}

impl StorageTokenRenew {
    #[must_use]
    pub fn new(
        source_token_id: TokenId,
        principal_id: PrincipalId,
        digest: StorageTokenDigest,
        expires_at: Option<DateTime<Utc>>,
        policy: StorageTokenIssuancePolicy,
        event_context: EventContext,
    ) -> Self {
        Self {
            source_token_id,
            principal_id,
            digest,
            expires_at,
            policy,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        TokenId,
        PrincipalId,
        StorageTokenDigest,
        Option<DateTime<Utc>>,
        StorageTokenIssuancePolicy,
        EventContext,
    ) {
        (
            self.source_token_id,
            self.principal_id,
            self.digest,
            self.expires_at,
            self.policy,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageTokenRenew {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenRenew")
            .field("source_token_id", &"<redacted>")
            .field("principal_id", &"<redacted>")
            .field("digest", &self.digest)
            .field("has_expiry", &self.expires_at.is_some())
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Principal-scoped token revocation.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenRevoke {
    token_id: TokenId,
    principal_id: PrincipalId,
    event_context: EventContext,
}

impl StorageTokenRevoke {
    #[must_use]
    pub const fn new(
        token_id: TokenId,
        principal_id: PrincipalId,
        event_context: EventContext,
    ) -> Self {
        Self {
            token_id,
            principal_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (TokenId, PrincipalId, EventContext) {
        (self.token_id, self.principal_id, self.event_context)
    }
}

impl fmt::Debug for StorageTokenRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenRevoke")
            .field("token_id", &"<redacted>")
            .field("principal_id", &"<redacted>")
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// HMAC-keyed revocation, optionally constrained to one principal.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenHashRevoke {
    principal_id: Option<PrincipalId>,
    credentials: Vec<StorageAuthenticationCredential>,
    event_context: EventContext,
}

impl StorageTokenHashRevoke {
    #[must_use]
    pub fn new(
        principal_id: Option<PrincipalId>,
        lookup_value: impl Into<String>,
        event_context: EventContext,
    ) -> Self {
        Self {
            principal_id,
            credentials: vec![StorageAuthenticationCredential::new(lookup_value)],
            event_context,
        }
    }

    pub fn try_candidates(
        principal_id: Option<PrincipalId>,
        credentials: Vec<StorageAuthenticationCredential>,
        event_context: EventContext,
    ) -> Result<Self, StorageValidationError> {
        if credentials.is_empty() || credentials.len() > MAX_TOKEN_HASH_KEYS {
            return Err(StorageValidationError::invalid(
                "token revocation requires a bounded, non-empty candidate set",
            ));
        }
        if credentials.iter().enumerate().any(|(index, candidate)| {
            credentials[index + 1..]
                .iter()
                .any(|other| candidate.digest() == other.digest())
        }) {
            return Err(StorageValidationError::invalid(
                "token revocation candidates must be unique",
            ));
        }
        Ok(Self {
            principal_id,
            credentials,
            event_context,
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Option<PrincipalId>,
        Vec<StorageAuthenticationCredential>,
        EventContext,
    ) {
        (self.principal_id, self.credentials, self.event_context)
    }
}

impl fmt::Debug for StorageTokenHashRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenHashRevoke")
            .field("has_principal", &self.principal_id.is_some())
            .field("credential_count", &self.credentials.len())
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Revoke every active token for one principal with audit attribution.
#[derive(Clone, PartialEq, Eq)]
pub struct StoragePrincipalTokensRevoke {
    principal_id: PrincipalId,
    event_context: EventContext,
}

impl StoragePrincipalTokensRevoke {
    #[must_use]
    pub const fn new(principal_id: PrincipalId, event_context: EventContext) -> Self {
        Self {
            principal_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (PrincipalId, EventContext) {
        (self.principal_id, self.event_context)
    }
}

impl fmt::Debug for StoragePrincipalTokensRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoragePrincipalTokensRevoke")
            .field("principal_id", &"<redacted>")
            .field("event_context", &"<redacted>")
            .finish()
    }
}

/// Complete bearer-token lifecycle required of every selectable backend.
#[async_trait]
pub trait TokenStorage: Send + Sync {
    /// Return hash-free retained token metadata using the requested lifecycle
    /// state, filters, stable cursor page, and optional exact total.
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError>;

    /// Aggregate persisted token usage by non-secret hash key identity.
    async fn token_key_usage(
        &self,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenKeyUsage>, StorageError>;

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError>;

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError>;

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError>;

    /// Load metadata for token IDs in the same order, including duplicates.
    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError>;

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError>;

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError>;

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_request_debug_output_redacts_hashes_and_ids() {
        let request = StorageTokenCreate::new(
            PrincipalId::new(42).unwrap(),
            StorageTokenDigest::legacy_unidentified("sensitive-token-hash"),
            StorageTokenIssuancePolicy::try_new(24, 48).unwrap(),
            EventContext::system(),
        )
        .name(Some("sensitive-name".to_string()));
        let debug = format!("{request:?}");

        assert!(!debug.contains("42"));
        assert!(!debug.contains("sensitive-token-hash"));
        assert!(!debug.contains("sensitive-name"));
    }

    #[test]
    fn token_issuance_policy_rejects_invalid_lifetimes() {
        assert_eq!(
            StorageTokenIssuancePolicy::try_new(0, 48),
            Err(StorageTokenIssuancePolicyError::NonPositiveDefault)
        );
        assert_eq!(
            StorageTokenIssuancePolicy::try_new(24, 0),
            Err(StorageTokenIssuancePolicyError::NonPositiveMaximum)
        );
        assert_eq!(
            StorageTokenIssuancePolicy::try_new(49, 48),
            Err(StorageTokenIssuancePolicyError::DefaultExceedsMaximum)
        );
    }
}
