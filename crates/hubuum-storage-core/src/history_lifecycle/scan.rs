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
        // A known subject/scope cannot prove separation from unknown restore
        // ancestry. Inspection and admission share this boundary.
        if evidence.has_unknown_restore_source() {
            return Ok(None);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_domain::{IdentityReference, VersionIdentity};
    use rstest::rstest;
    use std::collections::BTreeMap;

    #[rstest]
    #[case::collection(StorageIdentityHistoryKind::Collection)]
    #[case::class(StorageIdentityHistoryKind::Class)]
    #[case::object(StorageIdentityHistoryKind::Object)]
    #[case::class_relation(StorageIdentityHistoryKind::ClassRelation)]
    #[case::object_relation(StorageIdentityHistoryKind::ObjectRelation)]
    #[case::template(StorageIdentityHistoryKind::ExportTemplate)]
    #[case::remote(StorageIdentityHistoryKind::RemoteTarget)]
    #[case::computed(StorageIdentityHistoryKind::ComputedDefinitions)]
    #[case::schema(StorageIdentityHistoryKind::ClassSchema)]
    fn restored_candidate_requires_proven_ancestry(
        #[case] family: StorageIdentityHistoryKind,
        #[values(false, true)] proven: bool,
    ) {
        let mut references = BTreeMap::new();
        for (kind, paths) in [
            (
                StorageIdentityResourceKind::Collection,
                [
                    "/collection_id",
                    "/lifecycle/from/collection",
                    "/lifecycle/to/collection",
                ],
            ),
            (
                StorageIdentityResourceKind::Class,
                ["/class_id", "/lifecycle/from/class", "/lifecycle/to/class"],
            ),
        ] {
            let identity = ResourceIdentity::generate();
            for path in paths {
                references.insert(
                    path.into(),
                    StorageCapturedIdentity::Resource {
                        family: kind,
                        reference: IdentityReference::Known { identity },
                    },
                );
            }
        }
        let mut bytes = *VersionIdentity::generate().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        let derived = VersionIdentity::from_bytes(bytes).unwrap();
        let evidence = StorageIdentityEvidence::try_new(
            Some(crate::StorageIdentitySubject::Resource {
                identity: IdentityReference::Known {
                    identity: ResourceIdentity::generate(),
                },
                version: if proven {
                    VersionIdentity::generate()
                } else {
                    derived
                },
            }),
            references,
        )
        .unwrap();
        let evidence = if proven {
            evidence.at_restore_baseline(derived).unwrap()
        } else {
            evidence
        };
        let candidate =
            HistoryExpiryCandidate::from_evidence(family, &evidence, None, false).unwrap();
        assert_eq!(candidate.is_some(), proven);
    }
}
