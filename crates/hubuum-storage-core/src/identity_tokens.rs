use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::EventContext;

use crate::{AuthenticationTokenScope, StorageError, StorageTokenMetadata};

/// Owned fields returned when token creation input enters an adapter.
pub type StorageTokenCreateParts = (
    i32,
    String,
    Option<String>,
    Option<String>,
    Option<NaiveDateTime>,
    Option<AuthenticationTokenScope>,
    StorageTokenIssuancePolicy,
    Option<EventContext>,
);

/// Validated application policy used to materialize token expiry at the
/// backend's authoritative issuance timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageTokenIssuancePolicy {
    default_lifetime_hours: i64,
    maximum_lifetime_hours: i64,
}

impl StorageTokenIssuancePolicy {
    #[must_use]
    pub const fn new(default_lifetime_hours: i64, maximum_lifetime_hours: i64) -> Self {
        Self {
            default_lifetime_hours,
            maximum_lifetime_hours,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i64, i64) {
        (self.default_lifetime_hours, self.maximum_lifetime_hours)
    }
}

/// Token issuance input. The raw bearer secret never crosses the storage
/// boundary; only its application-generated HMAC is persisted.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenCreate {
    principal_id: i32,
    token_hash: String,
    name: Option<String>,
    description: Option<String>,
    expires_at: Option<NaiveDateTime>,
    scope: Option<AuthenticationTokenScope>,
    policy: StorageTokenIssuancePolicy,
    event_context: Option<EventContext>,
}

impl StorageTokenCreate {
    #[must_use]
    pub fn new(
        principal_id: i32,
        token_hash: impl Into<String>,
        policy: StorageTokenIssuancePolicy,
    ) -> Self {
        Self {
            principal_id,
            token_hash: token_hash.into(),
            name: None,
            description: None,
            expires_at: None,
            scope: None,
            policy,
            event_context: None,
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
    pub const fn expires_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.expires_at = value;
        self
    }

    #[must_use]
    pub fn scope(mut self, value: Option<AuthenticationTokenScope>) -> Self {
        self.scope = value;
        self
    }

    #[must_use]
    pub fn event_context(mut self, value: Option<EventContext>) -> Self {
        self.event_context = value;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> StorageTokenCreateParts {
        (
            self.principal_id,
            self.token_hash,
            self.name,
            self.description,
            self.expires_at,
            self.scope,
            self.policy,
            self.event_context,
        )
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
            .field("emit_event", &self.event_context.is_some())
            .finish()
    }
}

/// Renewal input. The backend copies source metadata and scope atomically into
/// a row carrying the supplied application-generated HMAC.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenRenew {
    source_token_id: i32,
    principal_id: i32,
    token_hash: String,
    expires_at: Option<NaiveDateTime>,
    policy: StorageTokenIssuancePolicy,
    event_context: Option<EventContext>,
}

impl StorageTokenRenew {
    #[must_use]
    pub fn new(
        source_token_id: i32,
        principal_id: i32,
        token_hash: impl Into<String>,
        expires_at: Option<NaiveDateTime>,
        policy: StorageTokenIssuancePolicy,
        event_context: Option<EventContext>,
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
        i32,
        i32,
        String,
        Option<NaiveDateTime>,
        StorageTokenIssuancePolicy,
        Option<EventContext>,
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
            .field("emit_event", &self.event_context.is_some())
            .finish()
    }
}

/// Principal-scoped token revocation.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenRevoke {
    token_id: i32,
    principal_id: i32,
    event_context: Option<EventContext>,
}

impl StorageTokenRevoke {
    #[must_use]
    pub const fn new(
        token_id: i32,
        principal_id: i32,
        event_context: Option<EventContext>,
    ) -> Self {
        Self {
            token_id,
            principal_id,
            event_context,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i32, Option<EventContext>) {
        (self.token_id, self.principal_id, self.event_context)
    }
}

impl fmt::Debug for StorageTokenRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenRevoke")
            .field("token_id", &"<redacted>")
            .field("principal_id", &"<redacted>")
            .field("emit_event", &self.event_context.is_some())
            .finish()
    }
}

/// HMAC-keyed revocation, optionally constrained to one principal.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenHashRevoke {
    principal_id: Option<i32>,
    token_hash: String,
}

impl StorageTokenHashRevoke {
    #[must_use]
    pub fn new(principal_id: Option<i32>, token_hash: impl Into<String>) -> Self {
        Self {
            principal_id,
            token_hash: token_hash.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<i32>, String) {
        (self.principal_id, self.token_hash)
    }
}

impl fmt::Debug for StorageTokenHashRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenHashRevoke")
            .field("has_principal", &self.principal_id.is_some())
            .field("token_hash", &"<redacted>")
            .finish()
    }
}

/// Complete bearer-token lifecycle required of every selectable backend.
#[async_trait]
pub trait TokenStorage: Send + Sync {
    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageTokenMetadata, StorageError>;

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageTokenMetadata, StorageError>;

    async fn load_token_metadata(
        &self,
        principal_id: i32,
        token_id: i32,
    ) -> Result<StorageTokenMetadata, StorageError>;

    /// Load metadata for token IDs in the same order, including duplicates.
    async fn load_token_metadata_batch(
        &self,
        token_ids: Vec<i32>,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError>;

    async fn revoke_token(&self, request: StorageTokenRevoke) -> Result<usize, StorageError>;

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<usize, StorageError>;

    async fn revoke_all_principal_tokens(&self, principal_id: i32) -> Result<usize, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_request_debug_output_redacts_hashes_and_ids() {
        let request = StorageTokenCreate::new(
            42,
            "sensitive-token-hash",
            StorageTokenIssuancePolicy::new(24, 48),
        )
        .name(Some("sensitive-name".to_string()));
        let debug = format!("{request:?}");

        assert!(!debug.contains("42"));
        assert!(!debug.contains("sensitive-token-hash"));
        assert!(!debug.contains("sensitive-name"));
    }
}
