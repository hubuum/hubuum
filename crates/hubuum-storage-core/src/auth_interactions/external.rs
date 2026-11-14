use super::*;
use crate::{StorageExternalGroup, StorageExternalUserSync, StorageSyncedHuman};
use hubuum_domain::{
    ExternalIdentity, FreshExternalAuthentication, PrincipalKind, VerifiedExternalAuthentication,
};

/// Browser evidence remains a proof until the locked interaction and stable
/// identity have been checked. This value cannot be deserialized from a client.
#[derive(Clone, Debug)]
pub enum StorageExternalInteractionAuthentication {
    Login(VerifiedExternalAuthentication),
    LoginWithCredential {
        authentication: VerifiedExternalAuthentication,
        credential: crate::StorageEncryptedUpstreamCredential,
    },
    Approval(FreshExternalAuthentication),
}

impl StorageExternalInteractionAuthentication {
    pub fn authentication(&self) -> &VerifiedExternalAuthentication {
        match self {
            Self::Login(proof)
            | Self::LoginWithCredential {
                authentication: proof,
                ..
            } => proof,
            Self::Approval(proof) => proof.authentication(),
        }
    }

    /// Use the server observation as the upper bound when a validated signed
    /// authentication time is slightly ahead within the permitted clock skew.
    /// This never extends an approval's lifetime using a future timestamp.
    pub fn authenticated_at(&self) -> DateTime<Utc> {
        match self {
            Self::Login(proof)
            | Self::LoginWithCredential {
                authentication: proof,
                ..
            } => proof.verified_at(),
            Self::Approval(proof) => proof
                .authenticated_at()
                .min(proof.authentication().verified_at()),
        }
    }

    /// Preflight before any writes, under the interaction lock. The adapter must
    /// also hold the current authentication revision and actor credential fences.
    pub fn prepare(
        &self,
        record: &StorageInteractionRecord,
        claim: &InteractionAuthenticationClaim,
        approval: Option<InteractionBrowserBinding>,
        now: DateTime<Utc>,
    ) -> Result<VerifiedInteractionSubject, StorageValidationError> {
        let authentication = self.authentication();
        let identity = authentication.identity();
        let provider = record.provider().ok_or_else(invalid)?;
        let binding = record.lifecycle().binding();
        if authentication.binding() != binding
            || identity.provider().external_id() != Some(provider.id())
            || identity.scope().as_str() != provider.name()
            || identity.provider_kind() != provider.kind().as_str()
            || identity.namespace() != provider.namespace()
            || authentication.verified_at() < record.lifecycle().created_at()
            || authentication.verified_at() > now
            || self.authenticated_at() > now
            || match self {
                Self::Login(_) | Self::LoginWithCredential { .. } => {
                    binding.authority().purpose() != InteractionPurpose::Login
                }
                Self::Approval(fresh) => {
                    fresh.started_at() != record.lifecycle().created_at()
                        || binding.authority().purpose() != InteractionPurpose::CredentialApproval
                        || now - self.authenticated_at()
                            > Duration::seconds(crate::CREDENTIAL_APPROVAL_LIFETIME_SECONDS)
                }
            }
        {
            return Err(invalid());
        }
        let proof = VerifiedInteractionSubject::new(binding.clone(), identity.subject().as_str())
            .map_err(|_| invalid())?;
        record
            .lifecycle()
            .clone()
            .finish_authentication(claim, proof.clone(), approval, now)
            .map_err(|_| invalid())?;
        Ok(proof)
    }

    /// The adapter supplies only a stable scope/subject lookup under its identity
    /// lock. An existing username or email must never be supplied as this fact.
    pub fn admit(
        &self,
        existing: Option<PrincipalId>,
    ) -> Result<StorageAdmittedExternalIdentity, StorageValidationError> {
        match self {
            Self::Login(authentication) | Self::LoginWithCredential { authentication, .. } => {
                authentication
                    .clone()
                    .admit(existing.map(|_| authentication.identity()))
                    .map_err(|_| invalid())?;
            }
            Self::Approval(proof) => {
                if existing
                    != proof
                        .authentication()
                        .binding()
                        .authority()
                        .actor()
                        .map(|actor| actor.principal_id())
                    || existing.is_none()
                {
                    return Err(invalid());
                }
            }
        }
        Ok(StorageAdmittedExternalIdentity {
            proof: self.clone(),
            existing,
        })
    }
}

/// Admission and the resolved principal remain attached to the verified facts
/// while an adapter reconciles the user and only this provider's memberships.
pub struct StorageAdmittedExternalIdentity {
    proof: StorageExternalInteractionAuthentication,
    existing: Option<PrincipalId>,
}
impl StorageAdmittedExternalIdentity {
    pub fn authentication(&self) -> &VerifiedExternalAuthentication {
        self.proof.authentication()
    }
    pub fn identity(&self) -> &ExternalIdentity {
        self.authentication().identity()
    }
    pub fn synchronization(&self) -> StorageExternalUserSync {
        let profile = self.authentication().profile();
        StorageExternalUserSync::builder(
            self.identity().scope().as_str(),
            self.identity().provider_kind(),
            self.identity().subject().as_str(),
            self.name(),
        )
        .proper_name(profile.name().map(ToOwned::to_owned))
        .email(profile.email().map(ToOwned::to_owned))
        .groups(
            self.authentication()
                .memberships()
                .provider_groups()
                .unwrap_or_default()
                .iter()
                .map(|group| StorageExternalGroup::new(group.key(), group.key(), None))
                .collect(),
        )
        .build()
    }
    fn name(&self) -> &str {
        self.authentication()
            .profile()
            .name()
            .unwrap_or(self.identity().subject().as_str())
    }
    pub fn interaction_authentication(
        &self,
        proof: VerifiedInteractionSubject,
        user: StorageSyncedHuman,
    ) -> Result<StorageInteractionAuthentication, StorageValidationError> {
        proof
            .require_binding(self.authentication().binding())
            .map_err(|_| invalid())?;
        if proof.subject() != self.identity().subject().as_str() {
            return Err(invalid());
        }
        let principal = PrincipalId::new(user.id().id()).map_err(|_| invalid())?;
        if self.existing.is_some_and(|existing| existing != principal) {
            return Err(invalid());
        }
        let subject = EventSubject::try_new(
            principal,
            self.name().to_owned(),
            PrincipalKind::Human,
            self.identity().scope().as_str().to_owned(),
            self.identity().provider_kind().to_owned(),
            false,
        )
        .map_err(|_| invalid())?;
        let user = StorageUser::try_new(
            user.id(),
            None,
            user.proper_name().map(ToOwned::to_owned),
            user.email().map(ToOwned::to_owned),
            user.created_at(),
            user.updated_at(),
            user.anonymized_at(),
        )?;
        let login =
            StorageLogin::external(subject, user, self.authentication().binding().revision())?;
        StorageInteractionAuthentication::from_login(proof, &login, self.proof.authenticated_at())
    }
}
