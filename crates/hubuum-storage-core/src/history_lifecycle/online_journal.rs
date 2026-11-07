//! Bounded restore-independent online expiry obligations. Capacity exhaustion
//! blocks admission; retained membership and receipts are never evicted.

use std::collections::BTreeSet;

use uuid::Uuid;

use super::*;
use crate::{StorageIdentityEvidence, StorageRecordIdentity};

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    try_from = "Vec<HistoryExpiryIntent>",
    into = "Vec<HistoryExpiryIntent>"
)]
pub struct HistoryExpiryJournal(Vec<HistoryExpiryIntent>);
impl HistoryExpiryJournal {
    pub const MAX_INTENTS: usize = 256;
    pub const MAX_BYTES: usize = 4 * 1024 * 1024;
    pub const REMOVAL_RESERVE_BYTES: usize = 512;
    pub const CANCELLATION_RESERVE_BYTES: usize = 256;

    pub fn new(mut intents: Vec<HistoryExpiryIntent>) -> Result<Self, StorageValidationError> {
        if intents.len() > Self::MAX_INTENTS {
            return Err(invalid("Retained online expiry intent capacity exhausted"));
        }
        intents.sort_by_key(HistoryExpiryIntent::identity);
        if intents
            .windows(2)
            .any(|pair| pair[0].identity() == pair[1].identity())
        {
            return Err(invalid("Duplicate retained online expiry intent identity"));
        }
        let mut targets = BTreeSet::new();
        for intent in &intents {
            for member in intent.manifest().members() {
                if !targets.insert((
                    member.family(),
                    *member.target().resource().as_bytes(),
                    *member.target().version().as_bytes(),
                )) {
                    return Err(invalid(
                        "Exact expiry version is already owned by a retained intent",
                    ));
                }
            }
        }
        let journal = Self(intents);
        if serde_json::to_vec(&journal.0)
            .map_err(|_| invalid("Cannot encode retained expiry journal"))?
            .len()
            + journal.completion_reserve_bytes()
            > Self::MAX_BYTES
        {
            return Err(invalid(
                "Retained expiry journal must preserve completion capacity",
            ));
        }
        Ok(journal)
    }
    pub fn completion_reserve_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|intent| {
                (intent.manifest().members().len() - intent.removals().len())
                    * Self::REMOVAL_RESERVE_BYTES
                    + usize::from(intent.cancellation().is_none())
                        * Self::CANCELLATION_RESERVE_BYTES
            })
            .sum()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn intents(&self) -> &[HistoryExpiryIntent] {
        &self.0
    }
    pub fn intent(&self, identity: Uuid) -> Option<&HistoryExpiryIntent> {
        self.0.iter().find(|intent| intent.identity() == identity)
    }
    pub fn excludes(
        &self,
        family: StorageIdentityHistoryKind,
        target: StorageRecordIdentity,
    ) -> bool {
        self.0.iter().any(|intent| intent.excludes(family, target))
    }
    /// Match restored copies using validated source ancestry. Unresolved identity
    /// cannot prove separation from retained membership of the same family;
    /// unknown ancestry cannot prove separation within the same resource.
    pub fn excludes_evidence(
        &self,
        family: StorageIdentityHistoryKind,
        evidence: Option<&StorageIdentityEvidence>,
    ) -> bool {
        let captured = evidence.and_then(|evidence| {
            evidence
                .subject()?
                .record_identity()
                .map(|target| (evidence, target))
        });
        self.0
            .iter()
            .flat_map(|intent| intent.manifest().members())
            .any(|member| {
                member.family() == family
                    && captured.is_none_or(|(evidence, target)| {
                        member.target().resource() == target.resource()
                            && (member.target().version() == target.version()
                                || evidence
                                    .restore_source_versions()
                                    .contains(member.target().version())
                                || evidence.has_unknown_restore_source())
                    })
            })
    }
    /// Select retained gap evidence after the adapter selects its native as-of
    /// candidate. Never filter first and silently fall back to an older value.
    /// Equal-start ambiguity fails closed; newer descendants remain readable.
    pub fn restriction_as_of(
        &self,
        family: StorageIdentityHistoryKind,
        resource: ResourceIdentity,
        at: DateTime<Utc>,
        selected_from: Option<DateTime<Utc>>,
    ) -> Option<&HistoryExpiryMember> {
        self.0
            .iter()
            .flat_map(|intent| intent.manifest().members())
            .filter(|member| {
                member.family() == family
                    && member.target().resource() == resource
                    && member.read_span().contains(at)
                    && selected_from.is_none_or(|from| from <= member.read_span().valid_from())
            })
            .max_by_key(|member| member.read_span().valid_from())
    }
    /// Counts immutable decisions, not transient claim/publication activity.
    pub fn revision(&self) -> HistoryAuthorityRevision {
        let decisions = self
            .0
            .iter()
            .map(|intent| {
                1 + usize::from(intent.cancellation().is_some()) + intent.removals().len()
            })
            .sum::<usize>();
        HistoryAuthorityRevision::new(decisions as u64)
            .expect("bounded intent decisions fit authority revision")
    }
    pub fn admit(&self, manifest: HistoryExpiryManifest) -> Result<Self, StorageValidationError> {
        if let Some(existing) = self.intent(manifest.identity()) {
            if existing.manifest() != &manifest {
                return Err(invalid(
                    "Retained intent identity cannot retarget expiry membership",
                ));
            }
            return Ok(self.clone());
        }
        let mut intents = self.0.clone();
        intents.push(HistoryExpiryIntent::new(manifest));
        Self::new(intents)
    }
    /// Outcome/cancellation updates may only add original facts to this exact
    /// retained manifest. The owning adapter serializes this transition.
    pub fn retain(&self, updated: HistoryExpiryIntent) -> Result<Self, StorageValidationError> {
        let mut intents = self.0.clone();
        let Some(existing) = intents
            .iter_mut()
            .find(|intent| intent.identity() == updated.identity())
        else {
            return Err(invalid("Online expiry intent was not admitted"));
        };
        if !updated.covers(existing) {
            return Err(invalid(
                "Online expiry transition would lose retained evidence",
            ));
        }
        *existing = updated;
        Self::new(intents)
    }
    pub fn covers(&self, required: &Self) -> bool {
        required.0.iter().all(|intent| {
            self.intent(intent.identity())
                .is_some_and(|local| local.covers(intent))
        })
    }
    pub fn merge(&self, required: &Self) -> Result<Self, StorageValidationError> {
        let mut intents = self.0.clone();
        for intent in &required.0 {
            if let Some(local) = intents
                .iter_mut()
                .find(|local| local.identity() == intent.identity())
            {
                *local = local.merge(intent)?;
            } else {
                intents.push(intent.clone());
            }
        }
        Self::new(intents)
    }
}
impl TryFrom<Vec<HistoryExpiryIntent>> for HistoryExpiryJournal {
    type Error = StorageValidationError;
    fn try_from(intents: Vec<HistoryExpiryIntent>) -> Result<Self, Self::Error> {
        Self::new(intents)
    }
}
impl From<HistoryExpiryJournal> for Vec<HistoryExpiryIntent> {
    fn from(journal: HistoryExpiryJournal) -> Self {
        journal.0
    }
}
