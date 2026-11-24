//! Encrypted upstream credentials bound to one issued Hubuum session. Adapters
//! fence these transitions with the current provider revision and session state.
use chrono::{DateTime, Duration, Utc};
use hubuum_domain::{
    BrowserIdentityNamespace, FreshExternalAuthentication, InteractionBinding, InteractionPurpose,
    PrincipalId, ResourceRevision, VerifiedExternalAuthentication,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AuthSessionId, StorageValidationError};

/// Whether a token returned by a failed renewal can be revoked after commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageUpstreamCleanup {
    Preserve,
    Revoke,
}

/// Whether a failed attempt could have spent its rotating refresh token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageUpstreamFailure {
    CredentialExpired,
    UnavailableBeforeSubmission,
    Unavailable,
    Rejected,
}
impl StorageUpstreamFailure {
    pub fn sync_failure(self) -> Option<crate::StorageExternalSyncFailure> {
        match self {
            Self::CredentialExpired => None,
            Self::Rejected => Some(crate::StorageExternalSyncFailure::Rejected),
            Self::Unavailable | Self::UnavailableBeforeSubmission => {
                Some(crate::StorageExternalSyncFailure::Unavailable)
            }
        }
    }
}

/// Local revocation count and the upstream grants captured by the same transaction.
#[derive(Clone, Debug, Default)]
pub struct StorageTokenRevocations {
    count: usize,
    grants: Vec<StorageUpstreamSession>,
}
impl StorageTokenRevocations {
    pub fn new(count: usize, grants: Vec<StorageUpstreamSession>) -> Self {
        Self { count, grants }
    }
    pub fn count(&self) -> usize {
        self.count
    }
    pub fn into_grants(self) -> Vec<StorageUpstreamSession> {
        self.grants
    }
}

/// Select encrypted credentials inside the authorized local revocation.
#[derive(Clone, Debug)]
pub enum StorageUpstreamRevocation {
    Sessions(crate::SessionRevocation),
    Provider(crate::StorageProviderIdentityBinding),
}
impl From<crate::SessionRevocation> for StorageUpstreamRevocation {
    fn from(value: crate::SessionRevocation) -> Self {
        Self::Sessions(value)
    }
}

fn invalid() -> StorageValidationError {
    StorageValidationError::invalid("Invalid upstream authentication state")
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StorageEncryptedUpstreamCredential(String);
impl StorageEncryptedUpstreamCredential {
    pub fn new(value: String) -> Result<Self, StorageValidationError> {
        if value.is_empty() || value.len() > 128 * 1024 {
            return Err(invalid());
        }
        Ok(Self(value))
    }
    pub fn ciphertext(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for StorageEncryptedUpstreamCredential {
    type Error = StorageValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<StorageEncryptedUpstreamCredential> for String {
    fn from(value: StorageEncryptedUpstreamCredential) -> Self {
        value.0
    }
}
impl std::fmt::Debug for StorageEncryptedUpstreamCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptedUpstreamCredential(<redacted>)")
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "RetainedInput")]
pub struct StorageRetainedAuthentication {
    binding: InteractionBinding,
    scope: String,
    namespace: BrowserIdentityNamespace,
    subject: String,
    credential: StorageEncryptedUpstreamCredential,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedInput {
    binding: InteractionBinding,
    scope: String,
    namespace: BrowserIdentityNamespace,
    subject: String,
    credential: StorageEncryptedUpstreamCredential,
}
impl TryFrom<RetainedInput> for StorageRetainedAuthentication {
    type Error = StorageValidationError;
    fn try_from(value: RetainedInput) -> Result<Self, Self::Error> {
        validate_identity(&value.binding, &value.scope, &value.subject)?;
        Ok(Self {
            binding: value.binding,
            scope: value.scope,
            namespace: value.namespace,
            subject: value.subject,
            credential: value.credential,
        })
    }
}
impl std::fmt::Debug for StorageRetainedAuthentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RetainedAuthentication(<redacted>)")
    }
}
impl StorageRetainedAuthentication {
    pub(crate) fn new(
        authentication: &VerifiedExternalAuthentication,
        credential: StorageEncryptedUpstreamCredential,
    ) -> Result<Self, StorageValidationError> {
        if authentication.binding().authority().purpose() != InteractionPurpose::Login {
            return Err(invalid());
        }
        let identity = authentication.identity();
        Ok(Self {
            binding: authentication.binding().clone(),
            scope: identity.scope().as_str().into(),
            namespace: BrowserIdentityNamespace::from_identity(identity),
            subject: identity.subject().as_str().into(),
            credential,
        })
    }
    pub fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "SessionInput")]
pub struct StorageUpstreamSession {
    session: Uuid,
    principal: PrincipalId,
    binding: InteractionBinding,
    scope: String,
    namespace: BrowserIdentityNamespace,
    subject: String,
    credential: Option<StorageEncryptedUpstreamCredential>,
    lease: Option<(Uuid, DateTime<Utc>)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_revision: Option<ResourceRevision>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInput {
    session: Uuid,
    principal: PrincipalId,
    binding: InteractionBinding,
    scope: String,
    namespace: BrowserIdentityNamespace,
    subject: String,
    credential: Option<StorageEncryptedUpstreamCredential>,
    lease: Option<(Uuid, DateTime<Utc>)>,
    retry_after: Option<DateTime<Utc>>,
    current_revision: Option<ResourceRevision>,
}
impl TryFrom<SessionInput> for StorageUpstreamSession {
    type Error = StorageValidationError;
    fn try_from(value: SessionInput) -> Result<Self, Self::Error> {
        validate_identity(&value.binding, &value.scope, &value.subject)?;
        if value
            .current_revision
            .is_some_and(|revision| revision < value.binding.revision())
            || value.session.is_nil()
            || value.credential.is_some() && value.lease.is_some() && !value.namespace.is_oauth()
            || value.retry_after.is_some() && value.lease.is_some()
            || value.retry_after.is_some() && value.credential.is_none()
            || value.lease.is_some_and(|(id, _)| id.is_nil())
        {
            return Err(invalid());
        }
        Ok(Self {
            session: value.session,
            principal: value.principal,
            binding: value.binding,
            scope: value.scope,
            namespace: value.namespace,
            subject: value.subject,
            credential: value.credential,
            lease: value.lease,
            retry_after: value.retry_after,
            current_revision: value.current_revision,
        })
    }
}
impl std::fmt::Debug for StorageUpstreamSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UpstreamSession(<redacted>)")
    }
}
fn validate_identity(
    binding: &InteractionBinding,
    scope: &str,
    subject: &str,
) -> Result<(), StorageValidationError> {
    if binding.authority().purpose() != InteractionPurpose::Login
        || binding.provider().external_id().is_none()
        || scope == "local"
    {
        return Err(invalid());
    }
    hubuum_domain::IdentityScopeName::new(scope).map_err(|_| invalid())?;
    hubuum_domain::ExternalSubject::new(subject).map_err(|_| invalid())?;
    Ok(())
}

impl StorageUpstreamSession {
    pub fn try_new(
        session: AuthSessionId,
        principal: PrincipalId,
        retained: StorageRetainedAuthentication,
    ) -> Result<Self, StorageValidationError> {
        if session.value().is_nil() {
            return Err(invalid());
        }
        Ok(Self {
            session: session.value(),
            principal,
            binding: retained.binding,
            scope: retained.scope,
            namespace: retained.namespace,
            subject: retained.subject,
            credential: Some(retained.credential),
            lease: None,
            retry_after: None,
            current_revision: None,
        })
    }
    pub fn session(&self) -> AuthSessionId {
        AuthSessionId::new(self.session)
    }
    pub fn principal(&self) -> PrincipalId {
        self.principal
    }
    pub fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub fn namespace(&self) -> &str {
        self.namespace.as_str()
    }
    pub fn subject(&self) -> &str {
        &self.subject
    }
    pub fn credential(&self) -> Option<&StorageEncryptedUpstreamCredential> {
        self.credential.as_ref()
    }
    /// The original binding remains the authenticated data for the ciphertext.
    pub fn current_revision(&self) -> ResourceRevision {
        self.current_revision.unwrap_or(self.binding.revision())
    }
    pub fn retain_at_revision(&mut self, revision: ResourceRevision) {
        if self.current_revision() != revision {
            // A claim made under the old revision can no longer finish. Its
            // potentially spent OIDC grant stays consumed. OAuth reads keep
            // their bearer in the session while the lease is outstanding.
            self.lease = None;
            self.current_revision = Some(revision);
        }
    }
    pub fn retry_deferred(&self, now: DateTime<Utc>) -> bool {
        self.retry_after.is_some_and(|after| now < after)
    }
    pub fn has_live_lease(&self, now: DateTime<Utc>) -> bool {
        self.lease.is_some_and(|(_, deadline)| now < deadline)
    }
    pub fn has_credential_or_live_lease(&self, now: DateTime<Utc>) -> bool {
        self.credential.is_some() || self.has_live_lease(now)
    }

    pub fn claim(&mut self, now: DateTime<Utc>) -> StorageUpstreamClaimOutcome {
        if self.retry_deferred(now) {
            return StorageUpstreamClaimOutcome::RetryLater;
        }
        self.retry_after = None;
        if self.has_live_lease(now) {
            return StorageUpstreamClaimOutcome::Busy;
        }
        let credential = if self.namespace.is_oauth() {
            self.credential.clone()
        } else {
            self.credential.take()
        };
        let Some(credential) = credential else {
            return StorageUpstreamClaimOutcome::Unavailable;
        };
        let attempt = Uuid::new_v4();
        let deadline = now + Duration::seconds(30);
        self.lease = Some((attempt, deadline));
        StorageUpstreamClaimOutcome::Claim(Box::new(StorageUpstreamClaim {
            session: self.clone(),
            credential,
            attempt,
            deadline,
        }))
    }
    pub fn accepts(&self, claim: &StorageUpstreamClaim, now: DateTime<Utc>) -> bool {
        self.current_revision() == claim.session.current_revision()
            && self.session == claim.session.session
            && self.principal == claim.session.principal
            && self.binding == claim.session.binding
            && self.subject == claim.session.subject
            && self.scope == claim.session.scope
            && self.namespace == claim.session.namespace
            && self.lease == Some((claim.attempt, claim.deadline))
            && now < claim.deadline
    }
    /// An OAuth read or pre-submission OIDC failure leaves the grant usable. Retry after
    /// a bounded delay, without advancing the last successful verification.
    pub fn defer_credential_retry(
        &mut self,
        claim: &StorageUpstreamClaim,
        provider: &crate::StorageProviderIdentityBinding,
        failure: StorageUpstreamFailure,
        now: DateTime<Utc>,
    ) -> bool {
        if !(failure == StorageUpstreamFailure::UnavailableBeforeSubmission
            || failure == StorageUpstreamFailure::Unavailable
                && provider.kind() == crate::StorageAuthenticationProviderKind::Oauth)
            || Some(provider.id()) != self.binding.provider().external_id()
            || provider.namespace() != self.namespace()
            || !self.accepts(claim, now)
        {
            return false;
        }
        self.credential = Some(claim.credential.clone());
        self.lease = None;
        self.retry_after = Some(now + Duration::seconds(30));
        true
    }
    pub fn renewed(
        &mut self,
        claim: &StorageUpstreamClaim,
        proof: &VerifiedExternalAuthentication,
        credential: Option<StorageEncryptedUpstreamCredential>,
        now: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if !self.accepts(claim, now)
            || proof.binding() != &self.binding
            || proof.identity().scope().as_str() != self.scope
            || proof.identity().namespace() != self.namespace.as_str()
            || proof.identity().subject().as_str() != self.subject
            || proof.verified_at() > now
            || proof.verified_at() < claim.deadline - Duration::seconds(30)
        {
            return Err(invalid());
        }
        self.credential = credential;
        self.lease = None;
        Ok(())
    }

    /// A fresh verified grant supersedes any in-flight refresh of the old grant.
    /// Keep the original login binding so stored ciphertext and renewal proofs
    /// retain their existing session identity and serialized representation.
    pub fn reauthenticated(
        &mut self,
        approval: &StorageUpstreamApproval,
        credential: StorageEncryptedUpstreamCredential,
        now: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        let resolved = StorageUpstreamApproval::new(self, approval.proof.clone())?;
        if resolved.session != approval.session
            || resolved.binding != approval.binding
            || approval.proof.authentication().verified_at() > now
        {
            return Err(invalid());
        }
        self.credential = Some(credential);
        self.retry_after = None;
        self.lease = None;
        Ok(())
    }
}

/// An existing session resolved against a fresh proof of the same immutable
/// provider identity. It exposes only the binding needed to encrypt its new
/// credential; the adapter rechecks live session/revision fences before saving.
#[derive(Clone, Debug)]
pub struct StorageUpstreamApproval {
    session: AuthSessionId,
    binding: InteractionBinding,
    proof: FreshExternalAuthentication,
}
impl StorageUpstreamApproval {
    pub fn new(
        session: &StorageUpstreamSession,
        proof: FreshExternalAuthentication,
    ) -> Result<Self, StorageValidationError> {
        let authentication = proof.authentication();
        let binding = authentication.binding();
        let identity = authentication.identity();
        if binding.authority().purpose() != InteractionPurpose::CredentialApproval
            || binding
                .authority()
                .actor()
                .map(|actor| actor.principal_id())
                != Some(session.principal)
            || binding.provider() != session.binding.provider()
            || binding.revision() != session.current_revision()
            || identity.scope().as_str() != session.scope
            || identity.namespace() != session.namespace.as_str()
            || identity.subject().as_str() != session.subject
        {
            return Err(invalid());
        }
        Ok(Self {
            session: session.session(),
            binding: session.binding.clone(),
            proof,
        })
    }
    pub fn session(&self) -> AuthSessionId {
        self.session
    }
    pub fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub fn proof(&self) -> &FreshExternalAuthentication {
        &self.proof
    }
}

#[derive(Clone, Debug)]
pub struct StorageUpstreamClaim {
    session: StorageUpstreamSession,
    credential: StorageEncryptedUpstreamCredential,
    attempt: Uuid,
    deadline: DateTime<Utc>,
}
impl StorageUpstreamClaim {
    pub fn started_at(&self) -> DateTime<Utc> {
        self.deadline - Duration::seconds(30)
    }
    pub fn session(&self) -> &StorageUpstreamSession {
        &self.session
    }
    pub fn credential(&self) -> &StorageEncryptedUpstreamCredential {
        &self.credential
    }
}
#[derive(Clone, Debug)]
pub enum StorageUpstreamClaimOutcome {
    Unavailable,
    RetryLater,
    Busy,
    Claim(Box<StorageUpstreamClaim>),
}

#[derive(Clone, Debug)]
pub struct StorageUpstreamRenewal {
    claim: StorageUpstreamClaim,
    proof: VerifiedExternalAuthentication,
    credential: Option<StorageEncryptedUpstreamCredential>,
}
impl StorageUpstreamRenewal {
    pub fn new(
        claim: StorageUpstreamClaim,
        proof: VerifiedExternalAuthentication,
        credential: Option<StorageEncryptedUpstreamCredential>,
    ) -> Result<Self, StorageValidationError> {
        let mut session = claim.session.clone();
        session.renewed(&claim, &proof, credential.clone(), Utc::now())?;
        Ok(Self {
            claim,
            proof,
            credential,
        })
    }
    pub fn claim(&self) -> &StorageUpstreamClaim {
        &self.claim
    }
    pub fn proof(&self) -> &VerifiedExternalAuthentication {
        &self.proof
    }
    pub fn credential(&self) -> Option<&StorageEncryptedUpstreamCredential> {
        self.credential.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageAuthenticationProviderKind, StorageProviderIdentityBinding};
    use hubuum_domain::{
        ExternalAuthenticationContext, IdentityScopeName, InteractionAuthority,
        InteractionClientId, InteractionId, InteractionProviderId, MembershipClaims,
        OAuthProviderDefinition, ResourceRevision,
    };
    use serde_json::json;

    fn oauth_session() -> (StorageUpstreamSession, StorageProviderIdentityBinding) {
        let definition: OAuthProviderDefinition = serde_json::from_value(json!({
            "profile":"json_user", "authorization_endpoint":"https://id.example/auth", "token_endpoint":"https://id.example/token",
            "profile_endpoint":"https://id.example/user", "client_id":"client", "client_authentication":"client_secret_post", "scopes":[],
            "subject_pointer":"/id", "subject_type":"string", "name_pointer":"/name", "admission":{"mode":"existing_only"},
            "membership":{"mode":"local_only","refresh_interval_seconds":300,"max_stale_seconds":900}
        })).unwrap();
        let now = Utc::now();
        let provider = StorageProviderIdentityBinding::try_new(
            "company".into(),
            Uuid::new_v4(),
            StorageAuthenticationProviderKind::Oauth,
            definition.identity_namespace(),
        )
        .unwrap();
        let binding = InteractionBinding::try_new(
            InteractionId::new(Uuid::new_v4()).unwrap(),
            InteractionProviderId::external(provider.id()).unwrap(),
            InteractionClientId::new("test").unwrap(),
            ResourceRevision::INITIAL,
            InteractionAuthority::login(),
        )
        .unwrap();
        let proof = ExternalAuthenticationContext::oauth(
            binding,
            IdentityScopeName::new("company").unwrap(),
            &definition,
        )
        .unwrap()
        .verified_oauth(&json!({"id":"alice"}), MembershipClaims::Absent, now)
        .unwrap();
        let retained = StorageRetainedAuthentication::new(
            &proof,
            StorageEncryptedUpstreamCredential::new("encrypted-access-token".into()).unwrap(),
        )
        .unwrap();
        let record = StorageUpstreamSession::try_new(
            AuthSessionId::new(Uuid::new_v4()),
            PrincipalId::new(1).unwrap(),
            retained,
        )
        .unwrap();
        (record, provider)
    }

    #[test]
    fn single_oauth_session_retains_its_bearer_when_activation_fences_a_profile_read() {
        let (mut record, _) = oauth_session();
        let now = Utc::now();
        let StorageUpstreamClaimOutcome::Claim(old) = record.claim(now) else {
            panic!("initial claim")
        };
        // Exercise the durable form read by another replica or by logout.
        let mut record: StorageUpstreamSession =
            serde_json::from_value(serde_json::to_value(record).unwrap()).unwrap();
        assert_eq!(
            record.credential().unwrap().ciphertext(),
            old.credential().ciphertext()
        );
        assert!(matches!(
            record.claim(now),
            StorageUpstreamClaimOutcome::Busy
        ));
        record.retain_at_revision(ResourceRevision::INITIAL.checked_advance().unwrap());
        assert!(!record.accepts(&old, now));
        let StorageUpstreamClaimOutcome::Claim(next) = record.claim(now) else {
            panic!("single retained session grant")
        };
        assert_eq!(
            next.credential().ciphertext(),
            old.credential().ciphertext()
        );
    }

    #[test]
    fn profile_outage_retries_same_access_token_after_backoff() {
        let (mut record, provider) = oauth_session();
        let now = Utc::now();
        let StorageUpstreamClaimOutcome::Claim(claim) = record.claim(now) else {
            panic!("initial claim")
        };
        assert!(record.defer_credential_retry(
            &claim,
            &provider,
            StorageUpstreamFailure::Unavailable,
            now
        ));
        assert!(matches!(
            record.claim(now),
            StorageUpstreamClaimOutcome::RetryLater
        ));
        let StorageUpstreamClaimOutcome::Claim(retry) = record.claim(now + Duration::seconds(30))
        else {
            panic!("retry")
        };
        assert_eq!(retry.credential().ciphertext(), "encrypted-access-token");
    }
}
