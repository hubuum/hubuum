//! Captured identity evidence, independent of a table layout or UUID implementation.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use hubuum_domain::{
    ClassId, HistoryRecordId, IdentityReference, PrincipalId, PrincipalIdentity, ResourceId,
    ResourceIdentity, SchemaRevision, TaskId, TaskIdentity, VersionIdentity,
};
use hubuum_events_core::EventId;
use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageRecordIdentity, StorageValidationError};

/// Resource families whose integer compatibility identifiers are not identities.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageIdentityResourceKind {
    Collection,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    ExportTemplate,
    RemoteTarget,
    IdentityScope,
    Group,
    ComputedDefinition,
    EventSink,
    EventSubscription,
    QueryUsageDeclaration,
}

/// Families retaining immutable historical versions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageIdentityHistoryKind {
    Collection,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    ExportTemplate,
    RemoteTarget,
    ComputedDefinitions,
    ClassSchema,
}

/// A point lookup resolved inside the selected adapter's snapshot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StorageIdentityTarget {
    Resource {
        kind: StorageIdentityResourceKind,
        id: ResourceId,
    },
    Principal(PrincipalId),
    Task(TaskId),
    History {
        kind: StorageIdentityHistoryKind,
        id: HistoryRecordId,
    },
    SchemaRevision {
        class_id: ClassId,
        revision: SchemaRevision,
    },
}

/// The identity retained by one current record or historical version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageIdentitySubject {
    Resource {
        identity: IdentityReference<ResourceIdentity>,
        version: VersionIdentity,
    },
    Principal {
        identity: IdentityReference<PrincipalIdentity>,
    },
    Task {
        identity: IdentityReference<TaskIdentity>,
    },
    Event {
        identity: EventId,
    },
}

impl StorageIdentitySubject {
    /// A retained resource/version pair is usable only when its resource
    /// correspondence is known. Unresolved legacy attribution cannot be promoted.
    #[must_use]
    pub const fn record_identity(self) -> Option<StorageRecordIdentity> {
        match self {
            Self::Resource {
                identity: IdentityReference::Known { identity },
                version,
            } => Some(StorageRecordIdentity::new(identity, version)),
            _ => None,
        }
    }
}

/// Typed reference captured when a snapshot or retained outcome was written.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageCapturedIdentity {
    Resource {
        family: StorageIdentityResourceKind,
        reference: IdentityReference<ResourceIdentity>,
    },
    Principal {
        reference: IdentityReference<PrincipalIdentity>,
    },
    Task {
        reference: IdentityReference<TaskIdentity>,
    },
    Version {
        reference: IdentityReference<VersionIdentity>,
    },
    Event {
        reference: IdentityReference<EventId>,
    },
}

impl StorageCapturedIdentity {
    #[must_use]
    pub const fn is_resolved(self) -> bool {
        match self {
            Self::Resource { reference, .. } => reference.known_identity().is_some(),
            Self::Principal { reference } => reference.known_identity().is_some(),
            Self::Task { reference } => reference.known_identity().is_some(),
            Self::Version { reference } => reference.known_identity().is_some(),
            Self::Event { reference } => reference.known_identity().is_some(),
        }
    }
}

/// Bounded identity evidence for one persisted row. JSON Pointer keys identify
/// defined reference fields, including references inside typed embedded snapshots.
/// Arbitrary user JSON is never searched for integers or automatically remapped.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "IdentityEvidenceInput")]
pub struct StorageIdentityEvidence {
    subject: Option<StorageIdentitySubject>,
    references: BTreeMap<String, StorageCapturedIdentity>,
    #[serde(skip)]
    restore_source_versions: StorageRestoreVersionAncestry,
}

const RESTORE_SOURCE_PREFIX: &str = "/restore_source_versions";
const UNKNOWN_RESTORE_SOURCE_PREFIX: &str = "/restore_unknown_source_versions";

/// Validated source versions retained when a history-free restore synthesizes
/// a new baseline. Unknown origins remain explicit across subsequent restores.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageRestoreVersionAncestry {
    versions: BTreeSet<VersionIdentity>,
    unknown_sources: BTreeSet<VersionIdentity>,
}

impl StorageRestoreVersionAncestry {
    /// Refuse further baseline synthesis rather than discard older provenance.
    pub const MAX_VERSIONS: usize = 64;

    #[must_use]
    pub fn contains(&self, version: VersionIdentity) -> bool {
        self.versions.contains(&version)
    }

    pub fn iter(&self) -> impl Iterator<Item = VersionIdentity> + '_ {
        self.versions.iter().copied()
    }

    fn capture(
        subject: Option<StorageIdentitySubject>,
        references: &BTreeMap<String, StorageCapturedIdentity>,
    ) -> Result<Self, StorageValidationError> {
        let mut ancestry = Self::default();
        for (path, captured) in references {
            let (prefix, unknown) = if path == RESTORE_SOURCE_PREFIX
                || path.starts_with(&format!("{RESTORE_SOURCE_PREFIX}/"))
            {
                (RESTORE_SOURCE_PREFIX, false)
            } else if path == UNKNOWN_RESTORE_SOURCE_PREFIX
                || path.starts_with(&format!("{UNKNOWN_RESTORE_SOURCE_PREFIX}/"))
            {
                (UNKNOWN_RESTORE_SOURCE_PREFIX, true)
            } else {
                continue;
            };
            let StorageCapturedIdentity::Version {
                reference: IdentityReference::Known { identity: source },
            } = captured
            else {
                return Err(StorageValidationError::invalid(
                    "Restore source must identify a known version",
                ));
            };
            if path != &format!("{prefix}/{source}") {
                return Err(StorageValidationError::invalid(
                    "Restore source path must match its canonical version identity",
                ));
            }
            let Some(StorageIdentitySubject::Resource { version, .. }) = subject else {
                return Err(StorageValidationError::invalid(
                    "Restore source ancestry requires a resource subject",
                ));
            };
            if *source == version {
                return Err(StorageValidationError::invalid(
                    "Restore source ancestry cannot contain its current version",
                ));
            }
            if unknown {
                if !derived_version(*source) {
                    return Err(StorageValidationError::invalid(
                        "Unknown restore source must identify a derived version",
                    ));
                }
                ancestry.unknown_sources.insert(*source);
            } else {
                ancestry.versions.insert(*source);
            }
        }
        if ancestry.versions.len() > Self::MAX_VERSIONS {
            return Err(StorageValidationError::invalid(
                "Restore source ancestry exceeds its retained version limit",
            ));
        }
        if !ancestry.unknown_sources.is_subset(&ancestry.versions) {
            return Err(StorageValidationError::invalid(
                "Unknown restore source must also be retained in source ancestry",
            ));
        }
        Ok(ancestry)
    }

    fn extend_references(&self, references: &mut BTreeMap<String, StorageCapturedIdentity>) {
        for (prefix, versions) in [
            (RESTORE_SOURCE_PREFIX, &self.versions),
            (UNKNOWN_RESTORE_SOURCE_PREFIX, &self.unknown_sources),
        ] {
            references.extend(versions.iter().map(|version| {
                (
                    format!("{prefix}/{version}"),
                    StorageCapturedIdentity::Version {
                        reference: IdentityReference::Known { identity: *version },
                    },
                )
            }));
        }
    }
}

fn derived_version(version: VersionIdentity) -> bool {
    version.as_bytes()[6] >> 4 == 8
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityEvidenceInput {
    subject: Option<StorageIdentitySubject>,
    references: BTreeMap<String, StorageCapturedIdentity>,
}

impl TryFrom<IdentityEvidenceInput> for StorageIdentityEvidence {
    type Error = StorageValidationError;

    fn try_from(input: IdentityEvidenceInput) -> Result<Self, Self::Error> {
        Self::try_new(input.subject, input.references)
    }
}

impl StorageIdentityEvidence {
    /// Resolve only a captured, immutable task correspondence.
    pub fn task_reference(&self, path: &str) -> Option<TaskIdentity> {
        match self.references.get(path) {
            Some(StorageCapturedIdentity::Task { reference }) => reference.known_identity(),
            _ => None,
        }
    }
    /// Return only proven correspondence. An unresolved legacy id is never a lookup key.
    #[must_use]
    pub fn principal_reference(&self, path: &str) -> Option<PrincipalIdentity> {
        match self.references.get(path) {
            Some(StorageCapturedIdentity::Principal { reference }) => reference.known_identity(),
            _ => None,
        }
    }

    /// Check that evidence belongs to the requested kind of owning record.
    /// This checks shape, not authorization or completeness for archive publication.
    pub fn for_target(self, target: StorageIdentityTarget) -> Result<Self, StorageValidationError> {
        let matches = matches!(
            (target, self.subject),
            (
                StorageIdentityTarget::Principal(_),
                Some(StorageIdentitySubject::Principal { .. })
            ) | (
                StorageIdentityTarget::Task(_),
                Some(StorageIdentitySubject::Task { .. })
            ) | (
                StorageIdentityTarget::Resource { .. }
                    | StorageIdentityTarget::History { .. }
                    | StorageIdentityTarget::SchemaRevision { .. },
                Some(StorageIdentitySubject::Resource { .. })
            )
        );
        if !matches {
            return Err(StorageValidationError::invalid(
                "Logical identity subject does not match the requested record kind",
            ));
        }
        Ok(self)
    }

    pub fn try_new(
        subject: Option<StorageIdentitySubject>,
        references: BTreeMap<String, StorageCapturedIdentity>,
    ) -> Result<Self, StorageValidationError> {
        // Payload and replay budgets bound the containing record. Do not impose
        // a second, incompatible limit on valid definition sets or export aliases.
        for path in references.keys() {
            if !path.starts_with('/')
                || path.contains('\0')
                || path
                    .split('~')
                    .skip(1)
                    .any(|suffix| !suffix.starts_with('0') && !suffix.starts_with('1'))
            {
                return Err(StorageValidationError::invalid(
                    "Identity reference must use a valid JSON Pointer",
                ));
            }
        }
        Ok(Self {
            subject,
            restore_source_versions: StorageRestoreVersionAncestry::capture(subject, &references)?,
            references,
        })
    }

    #[must_use]
    pub const fn restore_source_versions(&self) -> &StorageRestoreVersionAncestry {
        &self.restore_source_versions
    }

    /// Older derived baselines without provenance cannot establish that a
    /// retained deletion on this resource targets an unrelated version.
    #[must_use]
    pub fn has_unknown_restore_source(&self) -> bool {
        !self.restore_source_versions.unknown_sources.is_empty()
            || matches!(self.subject,
                Some(StorageIdentitySubject::Resource { version, .. })
                    if derived_version(version) && self.restore_source_versions.versions.is_empty())
    }

    /// Legacy restore synthesis can assign a new task UUID without proving its
    /// correspondence to earlier task deletion decisions.
    #[must_use]
    pub fn has_unknown_task_restore_source(&self) -> bool {
        matches!(self.subject,
            Some(StorageIdentitySubject::Task {
                identity: IdentityReference::Known { identity },
            }) if identity.as_bytes()[6] >> 4 == 8)
    }

    /// A legacy derived principal has no proven correspondence to retained
    /// receipt ownership from another restore cohort.
    #[must_use]
    pub fn has_unknown_principal_restore_source(&self) -> bool {
        matches!(self.subject,
            Some(StorageIdentitySubject::Principal {
                identity: IdentityReference::Known { identity },
            }) if identity.as_bytes()[6] >> 4 == 8)
    }

    /// Preserve proven ancestry while refreshing references for the same version.
    pub fn with_restore_source_versions(
        self,
        ancestry: &StorageRestoreVersionAncestry,
    ) -> Result<Self, StorageValidationError> {
        let mut references = self.references;
        ancestry.extend_references(&mut references);
        Self::try_new(self.subject, references)
    }

    /// Synthesize a new baseline without forgetting the source's immutable
    /// version or uncertainty inherited from older backup writers.
    pub fn at_restore_baseline(
        self,
        baseline_version: VersionIdentity,
    ) -> Result<Self, StorageValidationError> {
        let Some(StorageIdentitySubject::Resource { identity, version }) = self.subject else {
            return Err(StorageValidationError::invalid(
                "Restore baseline requires a resource subject",
            ));
        };
        let unknown = derived_version(version) && self.restore_source_versions.versions.is_empty();
        let mut ancestry = self.restore_source_versions;
        ancestry.versions.insert(version);
        if unknown {
            ancestry.unknown_sources.insert(version);
        }
        let mut references = self.references;
        ancestry.extend_references(&mut references);
        Self::try_new(
            Some(StorageIdentitySubject::Resource {
                identity,
                version: baseline_version,
            }),
            references,
        )
    }

    #[must_use]
    pub const fn subject(&self) -> Option<StorageIdentitySubject> {
        self.subject
    }

    #[must_use]
    pub const fn references(&self) -> &BTreeMap<String, StorageCapturedIdentity> {
        &self.references
    }

    #[must_use]
    pub fn is_resolved(&self) -> bool {
        let subject_resolved = match self.subject {
            None => true,
            Some(StorageIdentitySubject::Resource { identity, .. }) => {
                identity.known_identity().is_some()
            }
            Some(StorageIdentitySubject::Principal { identity }) => {
                identity.known_identity().is_some()
            }
            Some(StorageIdentitySubject::Task { identity }) => identity.known_identity().is_some(),
            Some(StorageIdentitySubject::Event { .. }) => true,
        };
        subject_resolved
            && self
                .references
                .values()
                .all(|reference| reference.is_resolved())
    }
}

/// Resolve identity and captured dependencies together, inside one backend
/// snapshot. Callers must retain this evidence instead of joining old integers
/// against today's resources or reconstructing UUIDs outside the adapter.
#[async_trait]
pub trait LogicalIdentityStorage: Send + Sync {
    async fn resolve_logical_identity(
        &self,
        target: StorageIdentityTarget,
    ) -> Result<StorageIdentityEvidence, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_domain::{LegacyIdentityCohort, UnresolvedIdentity};
    use rstest::rstest;

    fn resource(version: VersionIdentity) -> StorageIdentitySubject {
        StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: ResourceIdentity::generate(),
            },
            version,
        }
    }

    fn derived() -> VersionIdentity {
        let mut bytes = *VersionIdentity::generate().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        VersionIdentity::from_bytes(bytes).unwrap()
    }

    #[rstest]
    #[case::ordinary_task(false, false)]
    #[case::legacy_derived_task(true, false)]
    #[case::ordinary_principal(false, true)]
    #[case::legacy_derived_principal(true, true)]
    fn task_and_principal_restore_uncertainty_survives_serialization(
        #[case] derived: bool,
        #[case] principal: bool,
    ) {
        let mut bytes = *TaskIdentity::generate().as_bytes();
        if derived {
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
        }
        let subject = if principal {
            StorageIdentitySubject::Principal {
                identity: IdentityReference::Known {
                    identity: PrincipalIdentity::from_bytes(bytes).unwrap(),
                },
            }
        } else {
            StorageIdentitySubject::Task {
                identity: IdentityReference::Known {
                    identity: TaskIdentity::from_bytes(bytes).unwrap(),
                },
            }
        };
        let evidence = StorageIdentityEvidence::try_new(Some(subject), BTreeMap::new()).unwrap();
        let decoded: StorageIdentityEvidence =
            serde_json::from_slice(&serde_json::to_vec(&evidence).unwrap()).unwrap();
        assert_eq!(
            decoded.has_unknown_task_restore_source(),
            derived && !principal
        );
        assert_eq!(
            decoded.has_unknown_principal_restore_source(),
            derived && principal
        );
    }

    #[rstest]
    #[case::namespace_root("root")]
    #[case::mismatched_uuid("mismatch")]
    #[case::wrong_kind("kind")]
    #[case::unresolved_source("unresolved")]
    #[case::nonresource_subject("subject")]
    #[case::self_reference("cycle")]
    #[case::unknown_not_in_sources("unknown")]
    #[case::unknown_nonderived("nonderived")]
    fn malformed_restore_source_evidence_is_rejected(#[case] fault: &str) {
        let source = derived();
        let reference = serde_json::to_value(StorageCapturedIdentity::Version {
            reference: IdentityReference::Known { identity: source },
        })
        .unwrap();
        let mut document = serde_json::json!({
            "subject": resource(VersionIdentity::generate()),
            "references": {
                format!("{RESTORE_SOURCE_PREFIX}/{source}"): reference,
            },
        });
        let references = document["references"].as_object_mut().unwrap();
        match fault {
            "root" => {
                references.clear();
                references.insert(RESTORE_SOURCE_PREFIX.into(), reference);
            }
            "mismatch" => {
                references.clear();
                references.insert(
                    format!("{RESTORE_SOURCE_PREFIX}/{}", VersionIdentity::generate()),
                    reference,
                );
            }
            "kind" => {
                references.insert(
                    format!("{RESTORE_SOURCE_PREFIX}/{source}"),
                    serde_json::to_value(StorageCapturedIdentity::Principal {
                        reference: IdentityReference::Known {
                            identity: PrincipalIdentity::generate(),
                        },
                    })
                    .unwrap(),
                );
            }
            "unresolved" => {
                references.insert(
                    format!("{RESTORE_SOURCE_PREFIX}/{source}"),
                    serde_json::to_value(StorageCapturedIdentity::Version {
                        reference: IdentityReference::Unresolved {
                            legacy: UnresolvedIdentity::new(
                                LegacyIdentityCohort::generate(),
                                ResourceId::new(1).unwrap(),
                            ),
                        },
                    })
                    .unwrap(),
                );
            }
            "subject" => document["subject"] = serde_json::Value::Null,
            "cycle" => document["subject"] = serde_json::to_value(resource(source)).unwrap(),
            "unknown" => {
                references.clear();
                references.insert(
                    format!("{UNKNOWN_RESTORE_SOURCE_PREFIX}/{source}"),
                    reference,
                );
            }
            "nonderived" => {
                let plain = VersionIdentity::generate();
                let reference = serde_json::to_value(StorageCapturedIdentity::Version {
                    reference: IdentityReference::Known { identity: plain },
                })
                .unwrap();
                references.clear();
                references.insert(
                    format!("{RESTORE_SOURCE_PREFIX}/{plain}"),
                    reference.clone(),
                );
                references.insert(
                    format!("{UNKNOWN_RESTORE_SOURCE_PREFIX}/{plain}"),
                    reference,
                );
            }
            _ => unreachable!(),
        }
        assert!(serde_json::from_value::<StorageIdentityEvidence>(document).is_err());
    }

    #[test]
    fn restore_source_ancestry_limit_refuses_to_forget_old_versions() {
        let original = VersionIdentity::generate();
        let mut evidence =
            StorageIdentityEvidence::try_new(Some(resource(original)), BTreeMap::new()).unwrap();
        for _ in 0..StorageRestoreVersionAncestry::MAX_VERSIONS {
            evidence = evidence.at_restore_baseline(derived()).unwrap();
        }
        assert!(evidence.restore_source_versions().contains(original));
        assert_eq!(
            evidence.restore_source_versions().iter().count(),
            StorageRestoreVersionAncestry::MAX_VERSIONS
        );
        assert!(evidence.at_restore_baseline(derived()).is_err());
    }

    #[rstest]
    #[case::known_source(false)]
    #[case::old_unknown_baseline(true)]
    fn repeated_baselines_preserve_serialized_ancestry_and_uncertainty(#[case] unknown: bool) {
        let original = if unknown {
            derived()
        } else {
            VersionIdentity::generate()
        };
        let evidence =
            StorageIdentityEvidence::try_new(Some(resource(original)), BTreeMap::new()).unwrap();
        let first = derived();
        let first_evidence = evidence.at_restore_baseline(first).unwrap();
        let restored: StorageIdentityEvidence =
            serde_json::from_value(serde_json::to_value(first_evidence).unwrap()).unwrap();
        let latest = restored.at_restore_baseline(derived()).unwrap();
        assert_eq!(
            latest
                .restore_source_versions()
                .iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([original, first])
        );
        assert_eq!(latest.has_unknown_restore_source(), unknown);
    }

    #[rstest]
    #[case::not_pointer("class_id".to_owned())]
    #[case::bad_escape("/definitions/~2".to_owned())]
    #[case::nul("/class\0".to_owned())]
    fn rejects_invalid_reference_paths(#[case] path: String) {
        let references = BTreeMap::from([(
            path,
            StorageCapturedIdentity::Principal {
                reference: IdentityReference::Known {
                    identity: PrincipalIdentity::generate(),
                },
            },
        )]);
        assert!(StorageIdentityEvidence::try_new(None, references).is_err());
    }

    #[test]
    fn unresolved_subject_cannot_supply_record_identity() {
        let subject = StorageIdentitySubject::Resource {
            identity: IdentityReference::Unresolved {
                legacy: UnresolvedIdentity::new(
                    LegacyIdentityCohort::generate(),
                    ResourceId::new(42).unwrap(),
                ),
            },
            version: VersionIdentity::generate(),
        };
        assert_eq!(subject.record_identity(), None);
    }

    #[test]
    fn unresolved_dependency_prevents_complete_resolution() {
        let evidence = StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Resource {
                identity: IdentityReference::Known {
                    identity: ResourceIdentity::generate(),
                },
                version: VersionIdentity::generate(),
            }),
            BTreeMap::from([(
                "/actor_id".to_owned(),
                StorageCapturedIdentity::Principal {
                    reference: IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            LegacyIdentityCohort::generate(),
                            ResourceId::new(42).unwrap(),
                        ),
                    },
                },
            )]),
        )
        .unwrap();
        assert!(!evidence.is_resolved());
    }
}
