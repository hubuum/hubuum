//! Bounded verification evidence. The candidate assignment stays in its own row.
use super::*;
use crate::{
    StorageCredentialFingerprint, StorageRuntimeSettingsGroup, StorageRuntimeSettingsValue,
    StorageSecurityCandidateState, StorageSecurityCheck, StorageSecurityCheckKind,
    StorageSecurityCheckStatus,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VerificationDocument {
    id: Uuid,
    digest: InteractionDigest,
    revision: u32,
    assignment_digest: InteractionDigest,
    provider_id: Uuid,
    provider_digest: InteractionDigest,
    created: DateTime<Utc>,
    expires: DateTime<Utc>,
    identity_check: StorageSecurityCheck,
    receipt: Option<InteractionDigest>,
}

/// Checked result for one candidate revision. Adapters apply it while holding the candidate lock.
#[derive(Clone, Debug)]
pub struct StorageCandidateVerification(VerificationDocument);

fn provider_digest(provider: &StorageProviderIdentityBinding) -> InteractionDigest {
    document_digest(json!([
        provider.id(),
        provider.kind().as_str(),
        provider.name(),
        provider.namespace()
    ]))
}

fn assignment_digest(candidate: &StorageSecurityCandidate) -> InteractionDigest {
    // The configuration lives only in the candidate. Include ownership since a
    // deployment handoff can verify the same definition as a managed assignment.
    let mut value = candidate
        .verification_assignment()
        .to_document()
        .unwrap_or(Value::Null);
    value.sort_all_objects();
    document_digest(json!([candidate.releases_ownership(), value]))
}

impl StorageCandidateVerification {
    pub(super) fn new(
        candidate: &StorageSecurityCandidate,
        binding: &InteractionBinding,
    ) -> Result<Self, StorageValidationError> {
        let Some(StorageRuntimeSettingsValue::Authentication(providers)) =
            candidate.verification_assignment().value()
        else {
            return Err(invalid());
        };
        let provider = providers
            .providers()
            .iter()
            .find(|p| p.id() == binding.provider().external_id())
            .ok_or_else(invalid)?;
        let provider = StorageProviderIdentityBinding::from_provider(provider)?;
        let actor = binding.authority().actor().ok_or_else(invalid)?;
        if candidate.assignment().group() != StorageRuntimeSettingsGroup::Authentication
            || actor.principal_id() != candidate.owner().actor_id()
            || actor.token_id() != candidate.owner().token_id()
            || binding.revision() != candidate.base_revision()
            || !matches!(
                candidate.state(candidate.created_at()),
                StorageSecurityCandidateState::Unverified | StorageSecurityCandidateState::Verified
            )
        {
            return Err(invalid());
        }
        Self::restore(
            VerificationDocument {
                id: candidate.id(),
                digest: StorageInteractionAuthentication::candidate_digest(candidate),
                revision: candidate.verification_revision(),
                provider_id: provider.id(),
                provider_digest: provider_digest(&provider),
                created: candidate.created_at(),
                expires: candidate.expires_at(),
                assignment_digest: assignment_digest(candidate),
                identity_check: candidate
                    .checks()
                    .iter()
                    .find(|check| {
                        check.kind() == StorageSecurityCheckKind::Identity
                            && check.provider_id() == Some(provider.id())
                            && check.status() == StorageSecurityCheckStatus::Passed
                    })
                    .cloned()
                    .ok_or_else(invalid)?,

                receipt: candidate
                    .verification_receipt()
                    .map(|r| InteractionDigest::new(r.as_str()))
                    .transpose()
                    .map_err(|_| invalid())?,
            },
            binding,
        )
    }

    pub(super) fn restore(
        doc: VerificationDocument,
        binding: &InteractionBinding,
    ) -> Result<Self, StorageValidationError> {
        let authority = binding.authority();
        if authority.purpose() != InteractionPurpose::ProviderVerification
            || authority.candidate_id() != Some(doc.id)
            || authority.candidate_digest() != Some(&doc.digest)
            || binding.provider().external_id() != Some(doc.provider_id)
            || doc.revision == 0
            || doc.expires <= doc.created
            || doc.expires - doc.created
                > Duration::seconds(crate::SECURITY_CANDIDATE_LIFETIME_SECONDS)
            || doc.identity_check.kind() != StorageSecurityCheckKind::Identity
            || doc.identity_check.status() != StorageSecurityCheckStatus::Passed
            || doc.identity_check.provider_id() != Some(doc.provider_id)
            || doc
                .receipt
                .as_ref()
                .zip(binding.verification_receipt())
                .is_some_and(|(a, b)| a != b)
        {
            return Err(invalid());
        }
        Ok(Self(doc))
    }

    pub(super) fn matches_provider(
        &self,
        provider: Option<&StorageProviderIdentityBinding>,
    ) -> bool {
        provider.is_some_and(|p| provider_digest(p) == self.0.provider_digest)
    }

    pub(super) fn document(&self) -> &VerificationDocument {
        &self.0
    }
    pub fn id(&self) -> Uuid {
        self.0.id
    }

    pub(super) fn matches(
        &self,
        provider: Option<&StorageProviderIdentityBinding>,
        now: DateTime<Utc>,
    ) -> bool {
        now >= self.0.created && now < self.0.expires && self.matches_provider(provider)
    }

    pub fn apply(
        self,
        stored: &mut StorageSecurityCandidate,
        now: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        let doc = self.0;
        if stored.id() != doc.id
            || StorageInteractionAuthentication::candidate_digest(stored) != doc.digest
            || assignment_digest(stored) != doc.assignment_digest
            || stored.created_at() != doc.created
            || stored.expires_at() != doc.expires
            || stored.verification_revision().checked_add(1) != Some(doc.revision)
        {
            return Err(StorageValidationError::invalid(
                "Candidate verification inputs changed",
            ));
        }
        let mut checks = stored.checks().to_vec();
        let check = checks
            .iter_mut()
            .find(|c| {
                c.kind() == StorageSecurityCheckKind::Identity
                    && c.provider_id() == Some(doc.provider_id)
            })
            .ok_or_else(invalid)?;
        *check = doc.identity_check;
        let mut updated = stored.clone().with_checks(checks)?;
        if let Some(receipt) = doc.receipt {
            updated = updated.verify(StorageCredentialFingerprint::new(
                receipt.as_str().to_owned(),
            )?)?;
        }
        stored.accept_verification(updated, now)
    }
}
