//! Durable, operation-bound proof of fresh human authentication.
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, RestoreJobId, TokenId};
use hubuum_events_core::EventContext;
use serde::{Deserialize, Serialize};

use crate::StorageValidationError;

pub const CREDENTIAL_APPROVAL_LIFETIME_SECONDS: i64 = 120;
pub const CREDENTIAL_APPROVAL_REQUIRED: &str = "Fresh credential approval is required";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageCredentialOperation {
    CreateToken,
    RenewToken,
    CreateUser,
    UpdateUser,
    ImportCredentials,
    ConfirmRestore,
}

impl StorageCredentialOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateToken => "create_token",
            Self::RenewToken => "renew_token",
            Self::CreateUser => "create_user",
            Self::UpdateUser => "update_user",
            Self::ImportCredentials => "import_credentials",
            Self::ConfirmRestore => "confirm_restore",
        }
    }
}

/// A bounded digest, with its representation deliberately omitted from Debug.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageCredentialFingerprint(String);
impl StorageCredentialFingerprint {
    pub fn new(value: String) -> Result<Self, StorageValidationError> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(StorageValidationError::invalid(
                "credential fingerprint must be a SHA-256 hex digest",
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for StorageCredentialFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Application-validated binding, rechecked against durable state by the adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageCredentialClaim {
    actor_id: PrincipalId,
    token_id: TokenId,
    operation: StorageCredentialOperation,
    target_id: Option<PrincipalId>,
    restore_job_id: Option<RestoreJobId>,
    secret_digest: StorageCredentialFingerprint,
    request_digest: StorageCredentialFingerprint,
    legacy_valid_after: DateTime<Utc>,
}
impl StorageCredentialClaim {
    #[must_use]
    pub const fn new(
        actor_id: PrincipalId,
        token_id: TokenId,
        operation: StorageCredentialOperation,
        secret_digest: StorageCredentialFingerprint,
        request_digest: StorageCredentialFingerprint,
        legacy_valid_after: DateTime<Utc>,
    ) -> Self {
        Self {
            actor_id,
            token_id,
            operation,
            target_id: None,
            restore_job_id: None,
            secret_digest,
            request_digest,
            legacy_valid_after,
        }
    }
    #[must_use]
    pub const fn restore_job(mut self, id: RestoreJobId) -> Self {
        self.restore_job_id = Some(id);
        self
    }
    #[must_use]
    pub const fn target(mut self, target_id: Option<PrincipalId>) -> Self {
        self.target_id = target_id;
        self
    }
    #[must_use]
    pub const fn actor_id(&self) -> PrincipalId {
        self.actor_id
    }
    #[must_use]
    pub const fn token_id(&self) -> TokenId {
        self.token_id
    }
    #[must_use]
    pub const fn operation(&self) -> StorageCredentialOperation {
        self.operation
    }
    #[must_use]
    pub const fn restore_job_id(&self) -> Option<RestoreJobId> {
        self.restore_job_id
    }
    #[must_use]
    pub const fn target_id(&self) -> Option<PrincipalId> {
        self.target_id
    }
    #[must_use]
    pub const fn secret_digest(&self) -> &StorageCredentialFingerprint {
        &self.secret_digest
    }
    #[must_use]
    pub const fn request_digest(&self) -> &StorageCredentialFingerprint {
        &self.request_digest
    }
    #[must_use]
    pub const fn legacy_valid_after(&self) -> DateTime<Utc> {
        self.legacy_valid_after
    }
}

#[derive(Clone, Debug)]
pub struct StorageCredentialApprovalCreate {
    claim: StorageCredentialClaim,
    authenticated_at: DateTime<Utc>,
    event_context: EventContext,
}
impl StorageCredentialApprovalCreate {
    #[must_use]
    pub const fn new(
        claim: StorageCredentialClaim,
        authenticated_at: DateTime<Utc>,
        event_context: EventContext,
    ) -> Self {
        Self {
            claim,
            authenticated_at,
            event_context,
        }
    }
    #[must_use]
    pub fn into_parts(self) -> (StorageCredentialClaim, DateTime<Utc>, EventContext) {
        (self.claim, self.authenticated_at, self.event_context)
    }
}

/// Retained, non-secret evidence. Secret and request digests are never projected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCredentialApprovalMetadata {
    id: i32,
    actor_id: PrincipalId,
    token_id: TokenId,
    operation: StorageCredentialOperation,
    target_id: Option<PrincipalId>,
    restore_job_id: Option<RestoreJobId>,
    authenticated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    consumed_at: Option<DateTime<Utc>>,
    invalidated_at: Option<DateTime<Utc>>,
}
impl StorageCredentialApprovalMetadata {
    pub fn new(
        id: i32,
        claim: &StorageCredentialClaim,
        authenticated_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let valid_target = match claim.operation {
            StorageCredentialOperation::CreateToken
            | StorageCredentialOperation::RenewToken
            | StorageCredentialOperation::UpdateUser => {
                claim.target_id.is_some() && claim.restore_job_id.is_none()
            }
            StorageCredentialOperation::ConfirmRestore => {
                claim.target_id.is_none() && claim.restore_job_id.is_some()
            }
            StorageCredentialOperation::CreateUser
            | StorageCredentialOperation::ImportCredentials => {
                claim.target_id.is_none() && claim.restore_job_id.is_none()
            }
        };
        if !valid_target
            || id <= 0
            || expires_at <= authenticated_at
            || expires_at.signed_duration_since(authenticated_at)
                > chrono::Duration::seconds(CREDENTIAL_APPROVAL_LIFETIME_SECONDS)
        {
            return Err(StorageValidationError::invalid(
                "invalid credential approval identity or lifetime",
            ));
        }
        Ok(Self {
            id,
            actor_id: claim.actor_id,
            token_id: claim.token_id,
            operation: claim.operation,
            target_id: claim.target_id,
            restore_job_id: claim.restore_job_id,
            authenticated_at,
            expires_at,
            consumed_at: None,
            invalidated_at: None,
        })
    }
    pub fn invalidate(&mut self, at: DateTime<Utc>) {
        if self.consumed_at.is_none() && self.invalidated_at.is_none() {
            self.invalidated_at = Some(at);
        }
    }
    #[must_use]
    pub const fn invalidated_at(&self) -> Option<DateTime<Utc>> {
        self.invalidated_at
    }
    pub fn consume(&mut self, at: DateTime<Utc>) -> Result<(), StorageValidationError> {
        if self.invalidated_at.is_some()
            || self.consumed_at.is_some()
            || at < self.authenticated_at
            || at >= self.expires_at
        {
            return Err(StorageValidationError::invalid(
                CREDENTIAL_APPROVAL_REQUIRED,
            ));
        }
        self.consumed_at = Some(at);
        Ok(())
    }
    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }
    #[must_use]
    pub const fn actor_id(&self) -> PrincipalId {
        self.actor_id
    }
    #[must_use]
    pub const fn token_id(&self) -> TokenId {
        self.token_id
    }
    #[must_use]
    pub const fn operation(&self) -> StorageCredentialOperation {
        self.operation
    }
    #[must_use]
    pub const fn restore_job_id(&self) -> Option<RestoreJobId> {
        self.restore_job_id
    }
    #[must_use]
    pub const fn target_id(&self) -> Option<PrincipalId> {
        self.target_id
    }
    #[must_use]
    pub const fn authenticated_at(&self) -> DateTime<Utc> {
        self.authenticated_at
    }
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    #[must_use]
    pub const fn consumed_at(&self) -> Option<DateTime<Utc>> {
        self.consumed_at
    }
}

/// An approval claim and its audit attribution travel together to a transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageCredentialUse {
    claim: StorageCredentialClaim,
    context: EventContext,
}
impl StorageCredentialUse {
    #[must_use]
    pub const fn new(claim: StorageCredentialClaim, context: EventContext) -> Self {
        Self { claim, context }
    }
    #[must_use]
    pub const fn claim(&self) -> &StorageCredentialClaim {
        &self.claim
    }
    #[must_use]
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

/// Confirmation and its optional approval are committed with maintenance entry.
#[derive(Clone, Debug)]
pub struct StorageRestoreConfirmation {
    job_id: RestoreJobId,
    approval: Option<StorageCredentialUse>,
}
impl From<RestoreJobId> for StorageRestoreConfirmation {
    fn from(job_id: RestoreJobId) -> Self {
        Self {
            job_id,
            approval: None,
        }
    }
}
impl StorageRestoreConfirmation {
    #[must_use]
    pub fn with_approval(mut self, approval: StorageCredentialUse) -> Self {
        self.approval = Some(approval);
        self
    }
    #[must_use]
    pub const fn job_id(&self) -> RestoreJobId {
        self.job_id
    }
    #[must_use]
    pub const fn approval(&self) -> Option<&StorageCredentialUse> {
        self.approval.as_ref()
    }
}
