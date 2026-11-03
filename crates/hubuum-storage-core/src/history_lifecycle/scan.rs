use super::*;
use crate::{StorageCapturedIdentity, StorageIdentityEvidence, StorageIdentityResourceKind};

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ScanInput")]
pub struct HistoryLifecycleScan {
    key: HistoryPolicyKey,
    limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanInput {
    key: HistoryPolicyKey,
    limit: u32,
}
impl HistoryLifecycleScan {
    pub const MAX_INPUT_BYTES: usize = 1024 * 1024;
    pub fn new(key: HistoryPolicyKey, limit: u32) -> Result<Self, StorageValidationError> {
        if limit == 0 || limit > HistoryPreviewImpact::MAX_SCANNED {
            return Err(invalid("History inspection limit must be 1..=1000"));
        }
        Ok(Self { key, limit })
    }
    pub const fn key(&self) -> HistoryPolicyKey {
        self.key
    }
    pub const fn limit(&self) -> u32 {
        self.limit
    }
    pub fn includes(&self, candidate: &HistoryExpiryCandidate) -> bool {
        self.key.family() == candidate.family()
            && (self.key.scope() == HistoryPolicyScope::Deployment
                || candidate.attributions().is_empty()
                || candidate
                    .attributions()
                    .iter()
                    .any(|attribution| attribution.matches(self.key.scope())))
    }
}
impl TryFrom<ScanInput> for HistoryLifecycleScan {
    type Error = StorageValidationError;
    fn try_from(value: ScanInput) -> Result<Self, Self::Error> {
        Self::new(value.key, value.limit)
    }
}

/// Bounded native scan, containing no historical payloads. A full scan budget
/// is always reported as bounded, even when its last row happened to be final.
#[derive(Clone, Debug)]
pub struct HistoryLifecycleScanResult {
    candidates: Vec<HistoryExpiryCandidate>,
    scanned: u32,
    complete: bool,
    unresolved_identities: u32,
}
impl HistoryLifecycleScanResult {
    pub fn new(
        candidates: Vec<HistoryExpiryCandidate>,
        scanned: u32,
        complete: bool,
        unresolved_identities: u32,
    ) -> Result<Self, StorageValidationError> {
        if scanned > HistoryPreviewImpact::MAX_SCANNED
            || candidates.len() > scanned as usize
            || unresolved_identities > scanned
        {
            return Err(invalid("Invalid history scan coverage"));
        }
        Ok(Self {
            candidates,
            scanned,
            complete,
            unresolved_identities,
        })
    }
    pub fn candidates(&self) -> &[HistoryExpiryCandidate] {
        &self.candidates
    }
    pub const fn scanned(&self) -> u32 {
        self.scanned
    }
    pub const fn complete(&self) -> bool {
        self.complete && self.unresolved_identities == 0
    }
    pub const fn unresolved_identities(&self) -> u32 {
        self.unresolved_identities
    }
}

impl HistoryExpiryCandidate {
    /// Decode captured references once at the adapter boundary. Missing captured
    /// collection ownership is unresolved, including for deleted/moved classes.
    pub fn from_evidence(
        family: StorageIdentityHistoryKind,
        evidence: &StorageIdentityEvidence,
        anchor: Option<HistoryRetentionAnchor>,
        has_dependency: bool,
    ) -> Result<Option<Self>, StorageValidationError> {
        let Some(target) = evidence
            .subject()
            .and_then(|subject| subject.record_identity())
        else {
            return Ok(None);
        };
        let reference = |path: &str, kind| match evidence.references().get(path) {
            Some(StorageCapturedIdentity::Resource { family, reference }) if *family == kind => {
                reference.known_identity()
            }
            _ => None,
        };
        let collection = reference("/collection_id", StorageIdentityResourceKind::Collection);
        let class = reference("/class_id", StorageIdentityResourceKind::Class);
        let attribution = match family {
            StorageIdentityHistoryKind::Collection => {
                Some(vec![HistoryScopeAttribution::new(target.resource(), None)])
            }
            StorageIdentityHistoryKind::Class => collection.map(|collection| {
                vec![HistoryScopeAttribution::new(
                    collection,
                    Some(target.resource()),
                )]
            }),
            StorageIdentityHistoryKind::Object => {
                collection.zip(class).map(|(collection, class)| {
                    vec![HistoryScopeAttribution::new(collection, Some(class))]
                })
            }
            StorageIdentityHistoryKind::ExportTemplate
            | StorageIdentityHistoryKind::RemoteTarget => {
                collection.map(|collection| vec![HistoryScopeAttribution::new(collection, class)])
            }
            StorageIdentityHistoryKind::ComputedDefinitions
            | StorageIdentityHistoryKind::ClassSchema => {
                collection.zip(class).map(|(collection, class)| {
                    vec![HistoryScopeAttribution::new(collection, Some(class))]
                })
            }
            StorageIdentityHistoryKind::ClassRelation
            | StorageIdentityHistoryKind::ObjectRelation => reference(
                "/lifecycle/from/collection",
                StorageIdentityResourceKind::Collection,
            )
            .zip(reference(
                "/lifecycle/from/class",
                StorageIdentityResourceKind::Class,
            ))
            .zip(
                reference(
                    "/lifecycle/to/collection",
                    StorageIdentityResourceKind::Collection,
                )
                .zip(reference(
                    "/lifecycle/to/class",
                    StorageIdentityResourceKind::Class,
                )),
            )
            .map(
                |((left_collection, left_class), (right_collection, right_class))| {
                    vec![
                        HistoryScopeAttribution::new(left_collection, Some(left_class)),
                        HistoryScopeAttribution::new(right_collection, Some(right_class)),
                    ]
                },
            ),
        };
        Self::new(
            family,
            target,
            anchor,
            attribution.unwrap_or_default(),
            has_dependency,
        )
        .map(Some)
    }
}
