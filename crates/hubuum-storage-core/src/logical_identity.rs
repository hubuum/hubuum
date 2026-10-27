//! Captured identity evidence, independent of a table layout or UUID implementation.

use std::collections::BTreeMap;

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
            references,
        })
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
