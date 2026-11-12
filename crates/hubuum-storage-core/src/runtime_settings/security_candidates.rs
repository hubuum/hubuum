//! Short-lived verification authority for one exact security-settings revision.
use chrono::{DateTime, Duration, Utc};
use hubuum_domain::{PrincipalId, ResourceRevision, TokenId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    StorageCredentialClaim, StorageCredentialFingerprint, StorageCredentialOperation,
    StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup, StorageRuntimeSettingsUpdate,
    StorageValidationError,
};

pub const SECURITY_CANDIDATE_LIFETIME_SECONDS: i64 = 300;
pub const MAX_SECURITY_CANDIDATES_PER_ACTOR: usize = 8;
pub const MAX_SECURITY_CANDIDATES: usize = 256;

/// The initiating human credential, never merely an administrator user ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSecurityCandidateOwner {
    actor_id: PrincipalId,
    token_id: TokenId,
}

impl StorageSecurityCandidateOwner {
    pub const fn new(actor_id: PrincipalId, token_id: TokenId) -> Self {
        Self { actor_id, token_id }
    }

    pub const fn actor_id(self) -> PrincipalId {
        self.actor_id
    }
    pub const fn token_id(self) -> TokenId {
        self.token_id
    }
}

/// Digests cover the complete definition, resolved secret versions and deployment policy.
/// None of these are public report fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageSecurityCandidateBinding {
    definition: StorageCredentialFingerprint,
    secrets: StorageCredentialFingerprint,
    deployment: StorageCredentialFingerprint,
}

impl StorageSecurityCandidateBinding {
    pub const fn new(
        definition: StorageCredentialFingerprint,
        secrets: StorageCredentialFingerprint,
        deployment: StorageCredentialFingerprint,
    ) -> Self {
        Self {
            definition,
            secrets,
            deployment,
        }
    }

    pub const fn definition(&self) -> &StorageCredentialFingerprint {
        &self.definition
    }
    pub const fn secrets(&self) -> &StorageCredentialFingerprint {
        &self.secrets
    }
    pub const fn deployment(&self) -> &StorageCredentialFingerprint {
        &self.deployment
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSecurityCheckKind {
    Configuration,
    Secret,
    Tls,
    Connectivity,
    Discovery,
    Identity,
    Membership,
    RecoveryAdministrator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSecurityCheckStatus {
    Passed,
    Failed,
    NotRun,
}

/// Only bounded, application-selected diagnostic codes may be retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CheckDocument", into = "CheckDocument")]
pub struct StorageSecurityCheck {
    kind: StorageSecurityCheckKind,
    status: StorageSecurityCheckStatus,
    provider_id: Option<Uuid>,
    reason: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CheckDocument {
    kind: StorageSecurityCheckKind,
    status: StorageSecurityCheckStatus,
    provider_id: Option<Uuid>,
    reason: String,
}

impl TryFrom<CheckDocument> for StorageSecurityCheck {
    type Error = StorageValidationError;
    fn try_from(value: CheckDocument) -> Result<Self, Self::Error> {
        Self::try_new(value.kind, value.status, value.provider_id, value.reason)
    }
}

impl From<StorageSecurityCheck> for CheckDocument {
    fn from(value: StorageSecurityCheck) -> Self {
        Self {
            kind: value.kind,
            status: value.status,
            provider_id: value.provider_id,
            reason: value.reason,
        }
    }
}

impl StorageSecurityCheck {
    pub fn try_new(
        kind: StorageSecurityCheckKind,
        status: StorageSecurityCheckStatus,
        provider_id: Option<Uuid>,
        reason: impl Into<String>,
    ) -> Result<Self, StorageValidationError> {
        let reason = reason.into();
        if reason.is_empty()
            || reason.len() > 255
            || !reason
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || provider_id.is_some_and(|id| id.is_nil())
        {
            return Err(StorageValidationError::invalid(
                "Invalid security verification diagnostic",
            ));
        }
        Ok(Self {
            kind,
            status,
            provider_id,
            reason,
        })
    }
    pub const fn kind(&self) -> StorageSecurityCheckKind {
        self.kind
    }
    pub const fn status(&self) -> StorageSecurityCheckStatus {
        self.status
    }
    pub const fn provider_id(&self) -> Option<Uuid> {
        self.provider_id
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageSecurityCandidateState {
    Unverified,
    Verified,
    Failed,
    Expired,
    Activated,
}

/// Immutable candidate material plus its single-use verification state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageSecurityCandidate {
    id: Uuid,
    owner: StorageSecurityCandidateOwner,
    assignment: StorageRuntimeSettingsAssignment,
    verification_assignment: Option<StorageRuntimeSettingsAssignment>,
    base_revision: ResourceRevision,
    binding: StorageSecurityCandidateBinding,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    checks: Vec<StorageSecurityCheck>,
    verification_revision: u32,
    receipt: Option<StorageCredentialFingerprint>,
    activated_at: Option<DateTime<Utc>>,
}

impl StorageSecurityCandidate {
    pub fn try_new(
        id: Uuid,
        owner: StorageSecurityCandidateOwner,
        update: &StorageRuntimeSettingsUpdate,
        binding: StorageSecurityCandidateBinding,
    ) -> Result<Self, StorageValidationError> {
        if id.is_nil()
            || update.context().actor_user_id() != Some(owner.actor_id())
            || !matches!(
                update.group(),
                StorageRuntimeSettingsGroup::Authentication
                    | StorageRuntimeSettingsGroup::Authorization
            )
        {
            return Err(StorageValidationError::invalid(
                "Invalid security candidate identity or group",
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id,
            owner,
            assignment: update.assignment().clone(),
            verification_assignment: None,
            base_revision: update.expected_revision(),
            binding,
            created_at: now,
            expires_at: now + Duration::seconds(SECURITY_CANDIDATE_LIFETIME_SECONDS),
            checks: Vec::new(),
            verification_revision: 0,
            receipt: None,
            activated_at: None,
        })
    }

    /// Release ownership only after verifying the complete deployment selection.
    pub fn release_ownership(mut self) -> Result<Self, StorageValidationError> {
        if self.assignment.value().is_none()
            || self.verification_assignment.is_some()
            || self.receipt.is_some()
        {
            return Err(StorageValidationError::invalid(
                "Release requires an unverified complete deployment selection",
            ));
        }
        self.verification_assignment = Some(self.assignment.clone());
        self.assignment = StorageRuntimeSettingsAssignment::Deployment(self.assignment.group());
        Ok(self)
    }

    pub fn verification_assignment(&self) -> &StorageRuntimeSettingsAssignment {
        self.verification_assignment
            .as_ref()
            .unwrap_or(&self.assignment)
    }

    /// A previously accepted, unchanged enabled definition retains sessions.
    /// First adoption and release conservatively invalidate external credentials.
    pub fn retains_authenticated_provider(
        &self,
        previous: &StorageRuntimeSettingsAssignment,
        name: &str,
    ) -> bool {
        use crate::StorageRuntimeSettingsValue::Authentication;
        match (previous.value(), self.assignment.value()) {
            (Some(Authentication(old)), Some(Authentication(new))) => {
                let old = old
                    .providers()
                    .iter()
                    .find(|provider| provider.name() == name);
                let new = new
                    .providers()
                    .iter()
                    .find(|provider| provider.name() == name);
                matches!((old, new), (Some(old), Some(new)) if old.enabled() && new.enabled() && old.kind() == new.kind() && old.configuration() == new.configuration())
            }
            _ => false,
        }
    }

    pub const fn releases_ownership(&self) -> bool {
        self.verification_assignment.is_some()
    }

    /// Verification is all-or-nothing. A missing or not-run check cannot mint authority.
    pub fn with_checks(
        mut self,
        checks: Vec<StorageSecurityCheck>,
    ) -> Result<Self, StorageValidationError> {
        if checks.is_empty()
            || checks.len() > 128
            || self.receipt.is_some()
            || self.activated_at.is_some()
        {
            return Err(StorageValidationError::invalid(
                "Invalid security candidate checks",
            ));
        }
        self.verification_revision =
            self.verification_revision.checked_add(1).ok_or_else(|| {
                StorageValidationError::invalid("Candidate verification revision exhausted")
            })?;
        self.checks = checks;
        Ok(self)
    }

    pub fn verify(
        mut self,
        receipt: StorageCredentialFingerprint,
    ) -> Result<Self, StorageValidationError> {
        if self.state(Utc::now()) != StorageSecurityCandidateState::Unverified
            || self.checks.is_empty()
            || self
                .checks
                .iter()
                .any(|check| check.status != StorageSecurityCheckStatus::Passed)
            || !self
                .checks
                .iter()
                .any(|check| check.kind == StorageSecurityCheckKind::Configuration)
            || !self
                .checks
                .iter()
                .any(|check| check.kind == StorageSecurityCheckKind::RecoveryAdministrator)
        {
            return Err(StorageValidationError::invalid(
                "Candidate verification is incomplete",
            ));
        }
        self.receipt = Some(receipt);
        Ok(self)
    }

    pub fn state(&self, now: DateTime<Utc>) -> StorageSecurityCandidateState {
        if self.activated_at.is_some() {
            StorageSecurityCandidateState::Activated
        } else if now < self.created_at || now >= self.expires_at {
            StorageSecurityCandidateState::Expired
        } else if self
            .checks
            .iter()
            .any(|check| check.status == StorageSecurityCheckStatus::Failed)
        {
            StorageSecurityCandidateState::Failed
        } else if self.receipt.is_some() {
            StorageSecurityCandidateState::Verified
        } else {
            StorageSecurityCandidateState::Unverified
        }
    }

    pub fn accept_verification(
        &mut self,
        verified: Self,
        now: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if self.state(now) != StorageSecurityCandidateState::Unverified
            || !matches!(
                verified.state(now),
                StorageSecurityCandidateState::Verified
                    | StorageSecurityCandidateState::Unverified
                    | StorageSecurityCandidateState::Failed
            )
            || self.id != verified.id
            || self.owner != verified.owner
            || self.assignment != verified.assignment
            || self.verification_assignment != verified.verification_assignment
            || self.base_revision != verified.base_revision
            || self.binding != verified.binding
            || self.created_at != verified.created_at
            || self.expires_at != verified.expires_at
            || self.verification_revision.checked_add(1) != Some(verified.verification_revision)
        {
            return Err(StorageValidationError::invalid(
                "Candidate verification inputs changed",
            ));
        }
        *self = verified;
        Ok(())
    }

    pub fn activate(
        &mut self,
        request: &StorageSecurityCandidateActivation,
        now: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if self.id != request.candidate_id
            || self.state(now) != StorageSecurityCandidateState::Verified
            || self.binding != request.binding
            || self.receipt.as_ref() != Some(&request.receipt)
            || self.base_revision != request.update.expected_revision()
            || self.assignment != *request.update.assignment()
            || self.owner.actor_id != request.claim.actor_id()
            || self.owner.token_id != request.claim.token_id()
        {
            return Err(StorageValidationError::invalid(
                "Security candidate is expired, changed, unverified or consumed",
            ));
        }
        self.activated_at = Some(now);
        self.receipt = None;
        Ok(())
    }

    pub const fn id(&self) -> Uuid {
        self.id
    }
    pub const fn owner(&self) -> StorageSecurityCandidateOwner {
        self.owner
    }
    pub const fn assignment(&self) -> &StorageRuntimeSettingsAssignment {
        &self.assignment
    }
    pub const fn base_revision(&self) -> ResourceRevision {
        self.base_revision
    }
    pub const fn binding(&self) -> &StorageSecurityCandidateBinding {
        &self.binding
    }
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub fn checks(&self) -> &[StorageSecurityCheck] {
        &self.checks
    }

    /// Internal persistence representation. It must never be returned by an API or exported.
    pub fn to_document(&self) -> Value {
        json!({
            "id": self.id, "owner": self.owner, "group": self.assignment.group().as_str(),
            "verification_assignment": self.verification_assignment.as_ref().and_then(StorageRuntimeSettingsAssignment::to_document),
            "assignment": self.assignment.to_document(), "base_revision": self.base_revision,
            "definition": self.binding.definition.as_str(), "secrets": self.binding.secrets.as_str(),
            "deployment": self.binding.deployment.as_str(), "created_at": self.created_at,
            "expires_at": self.expires_at, "checks": self.checks,
            "verification_revision": self.verification_revision,
            "receipt": self.receipt.as_ref().map(StorageCredentialFingerprint::as_str),
            "activated_at": self.activated_at,
        })
    }

    pub fn from_document(value: Value) -> Result<Self, StorageValidationError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Document {
            id: Uuid,
            owner: StorageSecurityCandidateOwner,
            group: String,
            assignment: Option<Value>,
            verification_assignment: Option<Value>,
            base_revision: ResourceRevision,
            definition: String,
            secrets: String,
            deployment: String,
            created_at: DateTime<Utc>,
            expires_at: DateTime<Utc>,
            checks: Vec<StorageSecurityCheck>,
            verification_revision: u32,
            receipt: Option<String>,
            activated_at: Option<DateTime<Utc>>,
        }
        let d: Document = serde_json::from_value(value)
            .map_err(|_| StorageValidationError::invalid("Invalid security candidate document"))?;
        let group = StorageRuntimeSettingsGroup::parse(&d.group)?;
        if (d.verification_assignment.is_some() && d.assignment.is_some())
            || d.id.is_nil()
            || !matches!(
                group,
                StorageRuntimeSettingsGroup::Authentication
                    | StorageRuntimeSettingsGroup::Authorization
            )
            || d.expires_at <= d.created_at
            || d.expires_at - d.created_at > Duration::seconds(SECURITY_CANDIDATE_LIFETIME_SECONDS)
            || d.verification_revision == 0
            || d.checks.is_empty()
            || d.checks.len() > 128
            || d.activated_at
                .is_some_and(|at| at < d.created_at || at >= d.expires_at)
            || (d.activated_at.is_some() && d.receipt.is_some())
            || (d.receipt.is_some()
                && (d
                    .checks
                    .iter()
                    .any(|c| c.status != StorageSecurityCheckStatus::Passed)
                    || !d
                        .checks
                        .iter()
                        .any(|c| c.kind == StorageSecurityCheckKind::Configuration)
                    || !d
                        .checks
                        .iter()
                        .any(|c| c.kind == StorageSecurityCheckKind::RecoveryAdministrator)))
        {
            return Err(StorageValidationError::invalid(
                "Invalid security candidate state",
            ));
        }
        Ok(Self {
            id: d.id,
            owner: d.owner,
            assignment: StorageRuntimeSettingsAssignment::from_document(
                group,
                d.assignment.as_ref(),
            )?,
            verification_assignment: d
                .verification_assignment
                .as_ref()
                .map(|value| StorageRuntimeSettingsAssignment::from_document(group, Some(value)))
                .transpose()?,
            base_revision: d.base_revision,
            binding: StorageSecurityCandidateBinding::new(
                StorageCredentialFingerprint::new(d.definition)?,
                StorageCredentialFingerprint::new(d.secrets)?,
                StorageCredentialFingerprint::new(d.deployment)?,
            ),
            created_at: d.created_at,
            expires_at: d.expires_at,
            checks: d.checks,
            verification_revision: d.verification_revision,
            receipt: d
                .receipt
                .map(StorageCredentialFingerprint::new)
                .transpose()?,
            activated_at: d.activated_at,
        })
    }
}

/// Both fresh approval and verified candidate authority accompany the exact write.
#[derive(Clone, Debug)]
pub struct StorageSecurityCandidateActivation {
    candidate_id: Uuid,
    binding: StorageSecurityCandidateBinding,
    receipt: StorageCredentialFingerprint,
    update: StorageRuntimeSettingsUpdate,
    claim: StorageCredentialClaim,
    recovery_group: String,
}

impl StorageSecurityCandidateActivation {
    pub fn try_new(
        candidate: &StorageSecurityCandidate,
        receipt: StorageCredentialFingerprint,
        update: StorageRuntimeSettingsUpdate,
        claim: StorageCredentialClaim,
        recovery_group: String,
    ) -> Result<Self, StorageValidationError> {
        if recovery_group.is_empty()
            || recovery_group.len() > 255
            || claim.operation() != StorageCredentialOperation::ActivateSecurityCandidate
            || claim.security_candidate_id() != Some(candidate.id)
            || claim.actor_id() != candidate.owner.actor_id
            || claim.token_id() != candidate.owner.token_id
            || update.context().actor_user_id() != Some(claim.actor_id())
            || update.assignment() != candidate.assignment()
            || update.expected_revision() != candidate.base_revision()
        {
            return Err(StorageValidationError::invalid(
                "Security activation requires a bound administrator approval",
            ));
        }
        Ok(Self {
            candidate_id: candidate.id,
            binding: candidate.binding.clone(),
            receipt,
            update,
            claim,
            recovery_group,
        })
    }
    pub fn recovery_group(&self) -> &str {
        &self.recovery_group
    }

    pub const fn candidate_id(&self) -> Uuid {
        self.candidate_id
    }
    pub const fn update(&self) -> &StorageRuntimeSettingsUpdate {
        &self.update
    }
    pub const fn claim(&self) -> &StorageCredentialClaim {
        &self.claim
    }
}

#[cfg(test)]
mod tests;
