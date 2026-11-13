//! Bounded interaction aggregates. Adapters commit rejections that change state,
//! and commit purpose-specific issuance in the same transaction as consumption.
use crate::StorageProviderIdentityBinding;
use chrono::{DateTime, Duration, Utc};
use hubuum_domain::{
    InteractionAuthenticationClaim, InteractionBinding, InteractionBrowserBinding,
    InteractionClientId, InteractionDigest, InteractionId, InteractionIssuedResult,
    InteractionLifecycle, InteractionMode, InteractionPurpose, InteractionState,
    InteractionTransitionError, PrincipalId, ResourceRevision, VerifiedInteractionSubject,
};
use hubuum_events_core::{EventContext, EventSubject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{
    SessionCredentials, SessionPolicy, StorageCredentialApprovalMetadata, StorageCredentialClaim,
    StorageLogin, StorageMutationOutcome, StorageSecurityCandidate, StorageSecurityCandidateState,
    StorageSessionIssued, StorageUser, StorageValidationError,
};

pub const MAX_AUTH_INTERACTIONS: usize = 65_536;
pub const MAX_ACTIVE_AUTH_INTERACTIONS: usize = 4096;
pub const MAX_ACTIVE_AUTH_INTERACTIONS_PER_CLIENT: usize = 256;
pub const MAX_ACTIVE_AUTH_INTERACTIONS_PER_ACTOR: usize = 32;
pub const AUTH_INTERACTION_CLEANUP_SECONDS: i64 = 3600;
pub const MAX_AUTH_INTERACTION_DOCUMENT_BYTES: usize = 262_144;

fn invalid() -> StorageValidationError {
    StorageValidationError::invalid("Invalid authentication interaction")
}

/// Hashes arrive from the service's domain-separated secret hashing; plaintext
/// redemption and user-code values must never enter storage or debug output.
#[derive(Clone, Debug)]
pub struct StorageInteractionAccess {
    id: InteractionId,
    client: InteractionClientId,
    redemption: InteractionDigest,
}
impl StorageInteractionAccess {
    pub const fn new(
        id: InteractionId,
        client: InteractionClientId,
        redemption: InteractionDigest,
    ) -> Self {
        Self {
            id,
            client,
            redemption,
        }
    }
    pub const fn id(&self) -> InteractionId {
        self.id
    }
}

#[derive(Clone, Debug)]
pub enum StorageInteractionAuthorization {
    Redemption(StorageInteractionAccess),
    Browser {
        id: InteractionId,
        binding: InteractionBrowserBinding,
    },
    Claim(InteractionAuthenticationClaim),
}
impl StorageInteractionAuthorization {
    pub fn id(&self) -> InteractionId {
        match self {
            Self::Redemption(access) => access.id,
            Self::Browser { id, .. } => *id,
            Self::Claim(claim) => claim.binding().id(),
        }
    }
}

/// A short-lived fence on the original privileged credential. The service
/// derives the deadline from its validated token lifetime, not request input.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageInteractionActorLease {
    valid_until: DateTime<Utc>,
    recovery_group: Option<String>,
}
impl StorageInteractionActorLease {
    pub const fn approval(valid_until: DateTime<Utc>) -> Self {
        Self {
            valid_until,
            recovery_group: None,
        }
    }
    pub fn verification(
        valid_until: DateTime<Utc>,
        recovery_group: impl Into<String>,
    ) -> Result<Self, StorageValidationError> {
        let recovery_group = recovery_group.into();
        if recovery_group.trim().is_empty() || recovery_group.len() > 255 {
            return Err(invalid());
        }
        Ok(Self {
            valid_until,
            recovery_group: Some(recovery_group),
        })
    }
    pub const fn valid_until(&self) -> DateTime<Utc> {
        self.valid_until
    }
    pub fn recovery_group(&self) -> Option<&str> {
        self.recovery_group.as_deref()
    }
}

#[derive(Clone, Debug)]
pub struct StorageInteractionCreate {
    record: StorageInteractionRecord,
}
impl StorageInteractionCreate {
    pub fn builder(
        lifecycle: InteractionLifecycle,
        redemption: InteractionDigest,
    ) -> StorageInteractionCreateBuilder {
        StorageInteractionCreateBuilder {
            lifecycle,
            redemption,
            user_code: None,
            actor: None,
            provider: None,
        }
    }
    pub const fn record(&self) -> &StorageInteractionRecord {
        &self.record
    }
    pub fn into_record(self) -> StorageInteractionRecord {
        self.record
    }
}

pub struct StorageInteractionCreateBuilder {
    lifecycle: InteractionLifecycle,
    redemption: InteractionDigest,
    user_code: Option<InteractionDigest>,
    actor: Option<StorageInteractionActorLease>,
    provider: Option<StorageProviderIdentityBinding>,
}
impl StorageInteractionCreateBuilder {
    pub fn user_code(mut self, value: InteractionDigest) -> Self {
        self.user_code = Some(value);
        self
    }
    pub fn actor(mut self, value: StorageInteractionActorLease) -> Self {
        self.actor = Some(value);
        self
    }
    pub fn provider(mut self, value: StorageProviderIdentityBinding) -> Self {
        self.provider = Some(value);
        self
    }
    pub fn try_build(self) -> Result<StorageInteractionCreate, StorageValidationError> {
        let clean = InteractionLifecycle::try_new(
            self.lifecycle.binding().clone(),
            self.lifecycle.mode(),
            self.lifecycle.created_at(),
            self.lifecycle.expires_at(),
        )
        .map_err(|_| invalid())?;
        if clean != self.lifecycle {
            return Err(invalid());
        }
        let record = StorageInteractionRecord {
            lifecycle: self.lifecycle,
            redemption: self.redemption,
            user_code: self.user_code,
            actor: self.actor,
            provider: self.provider,
            authenticated: None,
        };
        record.validate_bindings()?;
        record.to_document()?;
        Ok(StorageInteractionCreate { record })
    }
}

/// Durable human evidence is a credential fingerprint plus the event-time
/// identity snapshot. Rehydration must still match the current locked user row.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageInteractionLogin {
    subject: EventSubject,
    fingerprint: InteractionDigest,
    revision: Option<ResourceRevision>,
    authenticated_at: DateTime<Utc>,
}
impl StorageInteractionLogin {
    pub const fn subject(&self) -> &EventSubject {
        &self.subject
    }
    /// Session issuance is attributed to the authenticated proof's principal;
    /// the redeeming request contributes correlation, never actor authority.
    pub fn event_context(&self, request: &EventContext) -> EventContext {
        EventContext::user(
            self.subject.principal_id(),
            request.request_id(),
            request.correlation_id().cloned(),
        )
        .with_trace_link(request.trace_link().cloned())
    }

    pub const fn authenticated_at(&self) -> DateTime<Utc> {
        self.authenticated_at
    }
    pub fn restore_login(&self, user: StorageUser) -> Result<StorageLogin, StorageValidationError> {
        if self.fingerprint != StorageLogin::user_interaction_fingerprint(&user) {
            return Err(invalid());
        }
        match self.revision {
            Some(revision) => StorageLogin::external(self.subject.clone(), user, revision),
            None => StorageLogin::try_new(self.subject.clone(), user),
        }
    }
    fn validate(&self, binding: &InteractionBinding) -> Result<(), StorageValidationError> {
        if self.subject.kind() != hubuum_domain::PrincipalKind::Human
            || match binding.provider().external_id() {
                None => {
                    self.revision.is_some()
                        || self.subject.identity_scope() != "local"
                        || self.subject.authentication_provider_kind() != "local"
                }
                Some(_) => {
                    self.revision != Some(binding.revision())
                        || self.subject.identity_scope() == "local"
                        || self.subject.authentication_provider_kind() == "local"
                }
            }
            || binding
                .authority()
                .actor()
                .is_some_and(|actor| actor.principal_id() != self.subject.principal_id())
            || binding.authority().purpose() == InteractionPurpose::ProviderVerification
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum StorageInteractionEvidence {
    Login(StorageInteractionLogin),
    Verification(Box<StorageSecurityCandidate>),
}

#[derive(Clone, Debug)]
pub struct StorageInteractionAuthentication {
    proof: VerifiedInteractionSubject,
    evidence: StorageInteractionEvidence,
}
impl StorageInteractionAuthentication {
    pub fn login(
        proof: VerifiedInteractionSubject,
        login: &StorageLogin,
        authenticated_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let snapshot = StorageInteractionLogin {
            subject: login.subject().clone(),
            fingerprint: login.interaction_fingerprint(),
            revision: login.authentication_revision(),
            authenticated_at,
        };
        snapshot.validate(proof.binding())?;
        if proof.binding().provider().external_id().is_none()
            && proof.subject() != login.subject().principal_id().id().to_string()
        {
            return Err(invalid());
        }
        Ok(Self {
            proof,
            evidence: StorageInteractionEvidence::Login(snapshot),
        })
    }
    pub fn verification(
        proof: VerifiedInteractionSubject,
        candidate: StorageSecurityCandidate,
    ) -> Result<Self, StorageValidationError> {
        let binding = proof.binding();
        let authority = binding.authority();
        let actor = authority.actor().ok_or_else(invalid)?;
        let Some(crate::StorageRuntimeSettingsValue::Authentication(providers)) =
            candidate.verification_assignment().value()
        else {
            return Err(invalid());
        };
        if !providers
            .providers()
            .iter()
            .any(|provider| provider.id() == binding.provider().external_id())
            || !candidate.checks().iter().any(|check| {
                check.kind() == crate::StorageSecurityCheckKind::Identity
                    && check.status() == crate::StorageSecurityCheckStatus::Passed
                    && check.provider_id() == binding.provider().external_id()
            })
            || candidate.assignment().group() != crate::StorageRuntimeSettingsGroup::Authentication
            || authority.purpose() != InteractionPurpose::ProviderVerification
            || authority.candidate_id() != Some(candidate.id())
            || authority.candidate_digest() != Some(&Self::candidate_digest(&candidate))
            || actor.principal_id() != candidate.owner().actor_id()
            || actor.token_id() != candidate.owner().token_id()
            || binding.revision() != candidate.base_revision()
            || binding.verification_receipt().is_some_and(|receipt| {
                !candidate.verification_receipt().is_some_and(|value| {
                    bool::from(value.as_str().as_bytes().ct_eq(receipt.as_str().as_bytes()))
                })
            })
        {
            return Err(invalid());
        }
        Ok(Self {
            proof,
            evidence: StorageInteractionEvidence::Verification(Box::new(candidate)),
        })
    }
    pub fn candidate_digest(candidate: &StorageSecurityCandidate) -> InteractionDigest {
        let binding = candidate.binding();
        let value = json!([
            candidate.id(),
            candidate.owner(),
            candidate.base_revision(),
            binding.definition().as_str(),
            binding.secrets().as_str(),
            binding.deployment().as_str()
        ]);
        InteractionDigest::new(
            Sha256::digest(value.to_string().as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
        .expect("SHA-256 is a lowercase hexadecimal digest")
    }
    pub const fn proof(&self) -> &VerifiedInteractionSubject {
        &self.proof
    }
    pub const fn evidence(&self) -> &StorageInteractionEvidence {
        &self.evidence
    }
}

#[derive(Clone, Debug)]
pub enum StorageInteractionAction {
    Inspect,
    Poll,
    Cancel,
    Claim(Uuid),
    Authenticate {
        authentication: Box<StorageInteractionAuthentication>,
        approval: Option<InteractionBrowserBinding>,
    },
    Reject,
    Decide {
        approve: bool,
    },
}

#[derive(Clone, Debug)]
pub struct StorageInteractionCommand {
    authorization: StorageInteractionAuthorization,
    action: StorageInteractionAction,
}
impl StorageInteractionCommand {
    pub const fn new(
        authorization: StorageInteractionAuthorization,
        action: StorageInteractionAction,
    ) -> Self {
        Self {
            authorization,
            action,
        }
    }
    pub const fn authorization(&self) -> &StorageInteractionAuthorization {
        &self.authorization
    }
    pub const fn action(&self) -> &StorageInteractionAction {
        &self.action
    }
}

#[derive(Clone, Debug)]
pub struct StorageInteractionView {
    binding: InteractionBinding,
    state: InteractionState,
    expires_at: DateTime<Utc>,
    interval_seconds: u32,
    issued: Option<InteractionIssuedResult>,
    authenticated_name: Option<String>,
    verification: Option<Box<VerifiedInteractionSubject>>,
}
impl StorageInteractionView {
    pub fn verification(&self) -> Option<&VerifiedInteractionSubject> {
        self.verification.as_deref()
    }
    pub fn authenticated_name(&self) -> Option<&str> {
        self.authenticated_name.as_deref()
    }
    pub const fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub const fn state(&self) -> InteractionState {
        self.state
    }
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub const fn interval_seconds(&self) -> u32 {
        self.interval_seconds
    }
    pub const fn issued(&self) -> Option<InteractionIssuedResult> {
        self.issued
    }
}

#[derive(Clone, Debug)]
pub enum StorageInteractionOutcome {
    View(StorageInteractionView),
    Claim(InteractionAuthenticationClaim),
    Rejected(InteractionTransitionError),
}

#[derive(Clone, Debug)]
pub enum StorageInteractionIssuance {
    Session {
        credentials: SessionCredentials,
        policy: SessionPolicy,
        context: EventContext,
    },
    Approval {
        claim: StorageCredentialClaim,
        operation_digest: InteractionDigest,
        context: EventContext,
    },
    ProviderVerification,
}

#[derive(Clone, Debug)]
pub struct StorageInteractionComplete {
    access: StorageInteractionAccess,
    issuance: StorageInteractionIssuance,
}
impl StorageInteractionComplete {
    pub const fn new(
        access: StorageInteractionAccess,
        issuance: StorageInteractionIssuance,
    ) -> Self {
        Self { access, issuance }
    }
    pub const fn access(&self) -> &StorageInteractionAccess {
        &self.access
    }
    pub const fn issuance(&self) -> &StorageInteractionIssuance {
        &self.issuance
    }
}

#[derive(Clone, Debug)]
pub enum StorageInteractionCompletion {
    Session(StorageMutationOutcome<StorageSessionIssued>),
    Approval(StorageMutationOutcome<StorageCredentialApprovalMetadata>),
    ProviderVerification(Uuid),
    Rejected(InteractionTransitionError),
}

#[derive(Clone, Debug)]
pub struct StorageInteractionRecord {
    lifecycle: InteractionLifecycle,
    redemption: InteractionDigest,
    user_code: Option<InteractionDigest>,
    actor: Option<StorageInteractionActorLease>,
    provider: Option<StorageProviderIdentityBinding>,
    authenticated: Option<StorageInteractionEvidence>,
}
impl StorageInteractionRecord {
    pub const fn lifecycle(&self) -> &InteractionLifecycle {
        &self.lifecycle
    }
    pub const fn user_code(&self) -> Option<&InteractionDigest> {
        self.user_code.as_ref()
    }
    pub const fn actor_lease(&self) -> Option<&StorageInteractionActorLease> {
        self.actor.as_ref()
    }
    pub const fn provider(&self) -> Option<&StorageProviderIdentityBinding> {
        self.provider.as_ref()
    }
    pub const fn evidence(&self) -> Option<&StorageInteractionEvidence> {
        self.authenticated.as_ref()
    }
    pub fn principal_id(&self) -> Option<PrincipalId> {
        match &self.authenticated {
            Some(StorageInteractionEvidence::Login(login)) => Some(login.subject.principal_id()),
            _ => self
                .lifecycle
                .binding()
                .authority()
                .actor()
                .map(|actor| actor.principal_id()),
        }
    }
    pub fn view(&self, now: DateTime<Utc>) -> StorageInteractionView {
        StorageInteractionView {
            binding: self.lifecycle.binding().clone(),
            state: self.lifecycle.state(now),
            expires_at: self.lifecycle.expires_at(),
            interval_seconds: self.lifecycle.interval_seconds(),
            issued: self.lifecycle.issued_result(),
            verification: self
                .lifecycle
                .authenticated_subject()
                .filter(|p| p.verification().is_some())
                .cloned()
                .map(Box::new),
            authenticated_name: match self.authenticated.as_ref() {
                Some(StorageInteractionEvidence::Login(login)) => {
                    Some(login.subject().name().to_owned())
                }
                Some(StorageInteractionEvidence::Verification(_)) => self
                    .lifecycle
                    .authenticated_subject()
                    .map(|proof| proof.subject().to_owned()),
                _ => None,
            },
        }
    }
    pub fn retain_until(&self) -> DateTime<Utc> {
        if self.lifecycle.issued_result().is_some() {
            self.lifecycle
                .expires_at()
                .checked_add_signed(Duration::seconds(AUTH_INTERACTION_CLEANUP_SECONDS))
                .unwrap_or(DateTime::<Utc>::MAX_UTC)
        } else {
            self.lifecycle.expires_at()
        }
    }
    pub fn allows(&self, access: &StorageInteractionAccess) -> bool {
        access.id == self.lifecycle.binding().id()
            && access.client == *self.lifecycle.binding().client()
            && access.redemption == self.redemption
    }
    pub fn invalidate(&mut self) {
        self.lifecycle.invalidate();
        self.authenticated = None;
    }
    pub fn bind_browser(
        &mut self,
        browser: InteractionBrowserBinding,
        now: DateTime<Utc>,
    ) -> Result<StorageInteractionView, InteractionTransitionError> {
        self.lifecycle.bind_browser(browser, now)?;
        Ok(self.view(now))
    }
    pub fn ready(
        &mut self,
        access: &StorageInteractionAccess,
        now: DateTime<Utc>,
    ) -> Result<&StorageInteractionEvidence, InteractionTransitionError> {
        if !self.allows(access) {
            return Err(InteractionTransitionError::Invalid);
        }
        if let Err(error) = self.lifecycle.ready_subject(now).map(|_| ()) {
            self.discard_inactive_evidence(now);
            return Err(error);
        }
        self.authenticated
            .as_ref()
            .ok_or(InteractionTransitionError::Invalid)
    }
    pub fn consume(
        &mut self,
        result: InteractionIssuedResult,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.lifecycle.consume(result, now)?;
        self.authenticated = None;
        Ok(())
    }
    fn validate_bindings(&self) -> Result<(), StorageValidationError> {
        if (self.lifecycle.mode() == InteractionMode::Browser) != self.user_code.is_some()
            || self.user_code.as_ref() == Some(&self.redemption)
            || self
                .provider
                .as_ref()
                .map(StorageProviderIdentityBinding::id)
                != self.lifecycle.binding().provider().external_id()
        {
            return Err(invalid());
        }

        match (self.lifecycle.binding().authority().purpose(), &self.actor) {
            (InteractionPurpose::Login, None) => Ok(()),
            (InteractionPurpose::CredentialApproval, Some(actor))
                if actor.recovery_group.is_none()
                    && actor.valid_until > self.lifecycle.created_at() =>
            {
                Ok(())
            }
            (InteractionPurpose::ProviderVerification, Some(actor))
                if actor
                    .recovery_group
                    .as_ref()
                    .is_some_and(|name| !name.trim().is_empty() && name.len() <= 255)
                    && actor.valid_until > self.lifecycle.created_at() =>
            {
                Ok(())
            }
            _ => Err(invalid()),
        }
    }

    pub fn discard_inactive_evidence(&mut self, now: DateTime<Utc>) {
        if !matches!(
            self.lifecycle.state(now),
            InteractionState::Pending | InteractionState::Ready
        ) {
            // Materialize expiry/abandoned claims before discarding their proof.
            // Otherwise a restart would see a ready phase without its evidence.
            let _ = self.lifecycle.ready_subject(now);
            self.authenticated = None;
        }
    }

    pub fn accepts_issuance(&self, issuance: &StorageInteractionIssuance) -> bool {
        let authority = self.lifecycle.binding().authority();
        match (authority.purpose(), issuance) {
            (InteractionPurpose::Login, StorageInteractionIssuance::Session { .. }) => true,
            (
                InteractionPurpose::ProviderVerification,
                StorageInteractionIssuance::ProviderVerification,
            ) => true,
            (
                InteractionPurpose::CredentialApproval,
                StorageInteractionIssuance::Approval {
                    claim,
                    operation_digest,
                    ..
                },
            ) => {
                authority.actor().is_some_and(|actor| {
                    actor.principal_id() == claim.actor_id() && actor.token_id() == claim.token_id()
                }) && authority
                    .operation_digest()
                    .is_some_and(|digest| digest == operation_digest)
            }
            _ => false,
        }
    }
}

mod persistence;
mod transitions;
