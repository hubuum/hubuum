use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::{
    ApplicationFence, ArtifactReference, HISTORY_ARCHIVE_MAX_CAPTURES, StorageError,
    StorageRecordIdentity,
};

/// Immutable locations named by query captures or retained after reclamation or catalog replacement.
/// A pin may outlive its query or original image. The observation grants no retirement authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryArchiveCopyHistory {
    target: StorageRecordIdentity,
    fence: ApplicationFence,
    locations: Vec<ArtifactReference>,
}

impl HistoryArchiveCopyHistory {
    pub fn from_locations(
        target: StorageRecordIdentity,
        fence: ApplicationFence,
        locations: impl IntoIterator<Item = ArtifactReference>,
    ) -> Result<Self, StorageError> {
        Self::from_sources(target, fence, locations, std::iter::empty())
    }

    pub fn from_sources(
        target: StorageRecordIdentity,
        fence: ApplicationFence,
        live: impl IntoIterator<Item = ArtifactReference>,
        historical: impl IntoIterator<Item = ArtifactReference>,
    ) -> Result<Self, StorageError> {
        let mut distinct = BTreeMap::new();
        // Bound each source before deduplication, and the physical union after it.
        for (index, reference) in live
            .into_iter()
            .enumerate()
            .chain(historical.into_iter().enumerate())
        {
            if index >= HISTORY_ARCHIVE_MAX_CAPTURES {
                return Err(StorageError::input_too_large(
                    "Archive copy history exceeds its source limit",
                ));
            }
            let id = reference.identity();
            let key = (id.destination(), id.artifact(), id.attempt(), id.version());
            match distinct.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(reference);
                    if distinct.len() > HISTORY_ARCHIVE_MAX_CAPTURES {
                        return Err(StorageError::input_too_large(
                            "Archive copy inventory exceeds capacity",
                        ));
                    }
                }
                Entry::Occupied(entry) if entry.get() != &reference => {
                    return Err(StorageError::conflict(
                        "Archive copy references disagree on the immutable manifest",
                    ));
                }
                Entry::Occupied(_) => {}
            }
        }
        Ok(Self {
            target,
            fence,
            locations: distinct.into_values().collect(),
        })
    }

    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub fn locations(&self) -> &[ArtifactReference] {
        &self.locations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{ArtifactIdentity, DestinationId};
    use hubuum_domain::{ResourceIdentity, VersionIdentity};
    use uuid::Uuid;

    fn observe(
        locations: impl IntoIterator<Item = ArtifactReference>,
    ) -> Result<HistoryArchiveCopyHistory, StorageError> {
        HistoryArchiveCopyHistory::from_locations(
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate()),
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
            locations,
        )
    }

    #[test]
    fn conflicting_pins_for_one_immutable_identity_fail_closed() {
        let id = ArtifactIdentity::new(DestinationId::generate());
        let error = observe([
            ArtifactReference::from_catalog(id.clone(), [1; 32]),
            ArtifactReference::from_catalog(id, [2; 32]),
        ])
        .unwrap_err();
        assert_eq!(error.kind(), crate::StorageErrorKind::Conflict);
    }

    #[rstest::rstest]
    #[case::same_physical_identity(true)]
    #[case::additional_physical_identity(false)]
    fn union_at_the_distinct_location_bound(#[case] shared: bool) {
        let destination = DestinationId::generate();
        let live: Vec<_> = (0..HISTORY_ARCHIVE_MAX_CAPTURES)
            .map(|_| ArtifactReference::from_catalog(ArtifactIdentity::new(destination), [1; 32]))
            .collect();
        let historical = if shared {
            live[0].clone()
        } else {
            ArtifactReference::from_catalog(ArtifactIdentity::new(destination), [1; 32])
        };
        let observation = HistoryArchiveCopyHistory::from_sources(
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate()),
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
            live,
            [historical],
        );
        if shared {
            assert_eq!(
                observation.unwrap().locations().len(),
                HISTORY_ARCHIVE_MAX_CAPTURES
            );
        } else {
            assert_eq!(
                observation.unwrap_err().kind(),
                crate::StorageErrorKind::InputTooLarge
            );
        }
    }

    #[test]
    fn duplicate_references_still_count_toward_the_capture_bound() {
        let reference = ArtifactReference::from_catalog(
            ArtifactIdentity::new(DestinationId::generate()),
            [1; 32],
        );
        let error = observe(std::iter::repeat_n(
            reference,
            HISTORY_ARCHIVE_MAX_CAPTURES + 1,
        ))
        .unwrap_err();
        assert_eq!(error.kind(), crate::StorageErrorKind::InputTooLarge);
    }
}
