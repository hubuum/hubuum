use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, TokenId};
use hubuum_events_core::EventContext;

use crate::{
    AuthenticationTokenScope, MutationOutcome, StorageError, StoragePage, StorageTokenListQuery,
    StorageTokenMetadata, StorageTokenObservation,
};

/// Named token-creation fields exposed to an adapter.
pub struct StorageTokenCreateParts {
    principal_id: PrincipalId,
    token_hash: String,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    scope: Option<AuthenticationTokenScope>,
    policy: StorageTokenIssuancePolicy,
    event_context: EventContext,
}

impl StorageTokenCreateParts {
    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub fn token_hash(&self) -> &str {
        &self.token_hash
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
    pub const fn scope(&self) -> Option<&AuthenticationTokenScope> {
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
    pub const fn new(
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
    token_hash: String,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    scope: Option<AuthenticationTokenScope>,
    policy: StorageTokenIssuancePolicy,
    event_context: EventContext,
}

impl StorageTokenCreate {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        token_hash: impl Into<String>,
        policy: StorageTokenIssuancePolicy,
        event_context: EventContext,
    ) -> Self {
        Self {
            principal_id,
            token_hash: token_hash.into(),
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
    pub fn scope(mut self, value: Option<AuthenticationTokenScope>) -> Self {
        self.scope = value;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> StorageTokenCreateParts {
        StorageTokenCreateParts {
            principal_id: self.principal_id,
            token_hash: self.token_hash,
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
            .field("token_hash", &"<redacted>")
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
    token_hash: String,
    expires_at: Option<DateTime<Utc>>,
    policy: StorageTokenIssuancePolicy,
    event_context: EventContext,
}

impl StorageTokenRenew {
    #[must_use]
    pub fn new(
        source_token_id: TokenId,
        principal_id: PrincipalId,
        token_hash: impl Into<String>,
        expires_at: Option<DateTime<Utc>>,
        policy: StorageTokenIssuancePolicy,
        event_context: EventContext,
    ) -> Self {
        Self {
            source_token_id,
            principal_id,
            token_hash: token_hash.into(),
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
        String,
        Option<DateTime<Utc>>,
        StorageTokenIssuancePolicy,
        EventContext,
    ) {
        (
            self.source_token_id,
            self.principal_id,
            self.token_hash,
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
            .field("token_hash", &"<redacted>")
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
    token_hash: String,
    event_context: EventContext,
}

impl StorageTokenHashRevoke {
    #[must_use]
    pub fn new(
        principal_id: Option<PrincipalId>,
        token_hash: impl Into<String>,
        event_context: EventContext,
    ) -> Self {
        Self {
            principal_id,
            token_hash: token_hash.into(),
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<PrincipalId>, String, EventContext) {
        (self.principal_id, self.token_hash, self.event_context)
    }
}

impl fmt::Debug for StorageTokenHashRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenHashRevoke")
            .field("has_principal", &self.principal_id.is_some())
            .field("token_hash", &"<redacted>")
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

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError>;

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<MutationOutcome<StorageTokenMetadata>, StorageError>;

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
    ) -> Result<MutationOutcome<usize>, StorageError>;

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError>;

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<MutationOutcome<usize>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_request_debug_output_redacts_hashes_and_ids() {
        let request = StorageTokenCreate::new(
            PrincipalId::new(42).unwrap(),
            "sensitive-token-hash",
            StorageTokenIssuancePolicy::new(24, 48).unwrap(),
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
            StorageTokenIssuancePolicy::new(0, 48),
            Err(StorageTokenIssuancePolicyError::NonPositiveDefault)
        );
        assert_eq!(
            StorageTokenIssuancePolicy::new(24, 0),
            Err(StorageTokenIssuancePolicyError::NonPositiveMaximum)
        );
        assert_eq!(
            StorageTokenIssuancePolicy::new(49, 48),
            Err(StorageTokenIssuancePolicyError::DefaultExceedsMaximum)
        );
    }
}
