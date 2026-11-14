//! Encrypted upstream credentials bound to one issued Hubuum session. Adapters
//! fence these transitions with the current provider revision and session state.
use chrono::{DateTime, Duration, Utc};
use hubuum_domain::{
    InteractionBinding, InteractionPurpose, PrincipalId, VerifiedExternalAuthentication,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AuthSessionId, StorageValidationError};

/// Select encrypted credentials before an authorized local revocation commits.
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
    namespace: String,
    subject: String,
    credential: StorageEncryptedUpstreamCredential,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedInput {
    binding: InteractionBinding,
    scope: String,
    namespace: String,
    subject: String,
    credential: StorageEncryptedUpstreamCredential,
}
impl TryFrom<RetainedInput> for StorageRetainedAuthentication {
    type Error = StorageValidationError;
    fn try_from(value: RetainedInput) -> Result<Self, Self::Error> {
        validate_identity(
            &value.binding,
            &value.scope,
            &value.namespace,
            &value.subject,
        )?;
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
            namespace: identity.namespace().into(),
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
    namespace: String,
    subject: String,
    credential: Option<StorageEncryptedUpstreamCredential>,
    lease: Option<(Uuid, DateTime<Utc>)>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInput {
    session: Uuid,
    principal: PrincipalId,
    binding: InteractionBinding,
    scope: String,
    namespace: String,
    subject: String,
    credential: Option<StorageEncryptedUpstreamCredential>,
    lease: Option<(Uuid, DateTime<Utc>)>,
}
impl TryFrom<SessionInput> for StorageUpstreamSession {
    type Error = StorageValidationError;
    fn try_from(value: SessionInput) -> Result<Self, Self::Error> {
        validate_identity(
            &value.binding,
            &value.scope,
            &value.namespace,
            &value.subject,
        )?;
        if value.session.is_nil()
            || value.credential.is_some() && value.lease.is_some()
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
    namespace: &str,
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
    hubuum_domain::AuthenticationEndpoint::try_from(namespace.to_owned()).map_err(|_| invalid())?;
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
        &self.namespace
    }
    pub fn subject(&self) -> &str {
        &self.subject
    }
    pub fn credential(&self) -> Option<&StorageEncryptedUpstreamCredential> {
        self.credential.as_ref()
    }
    pub fn claim(&mut self, now: DateTime<Utc>) -> StorageUpstreamClaimOutcome {
        if self.lease.is_some_and(|(_, deadline)| now < deadline) {
            return StorageUpstreamClaimOutcome::Busy;
        }
        let Some(credential) = self.credential.take() else {
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
        self.session == claim.session.session
            && self.principal == claim.session.principal
            && self.binding == claim.session.binding
            && self.subject == claim.session.subject
            && self.scope == claim.session.scope
            && self.namespace == claim.session.namespace
            && self.lease == Some((claim.attempt, claim.deadline))
            && now < claim.deadline
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
            || proof.identity().namespace() != self.namespace
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
