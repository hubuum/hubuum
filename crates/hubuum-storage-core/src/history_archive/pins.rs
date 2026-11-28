use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::{
    ApplicationFence, ArtifactReference, HISTORY_ARCHIVE_MAX_CAPTURES, HistoryArchiveCatalog,
    HistoryArchiveCatalogGeneration, StorageError, StorageRecordIdentity,
};

/// Current and historical immutable locations observed in one catalog and application snapshot.
/// A pin may outlive its query or original image. The observation grants no retirement authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryArchiveCopyHistory {
    target: StorageRecordIdentity,
    fence: ApplicationFence,
    catalog_generation: HistoryArchiveCatalogGeneration,
    current: Option<ArtifactReference>,
    locations: Vec<ArtifactReference>,
}

impl HistoryArchiveCopyHistory {
    pub fn from_locations(
        target: StorageRecordIdentity,
        fence: ApplicationFence,
        catalog: &HistoryArchiveCatalog,
        locations: impl IntoIterator<Item = ArtifactReference>,
    ) -> Result<Self, StorageError> {
        Self::from_sources(target, fence, catalog, locations, std::iter::empty())
    }

    pub fn from_sources(
        target: StorageRecordIdentity,
        fence: ApplicationFence,
        catalog: &HistoryArchiveCatalog,
        live: impl IntoIterator<Item = ArtifactReference>,
        historical: impl IntoIterator<Item = ArtifactReference>,
    ) -> Result<Self, StorageError> {
        if catalog.resource() != target.resource() {
            return Err(StorageError::invalid_input(
                "Archive copy history catalog resource mismatch",
            ));
        }
        let current = catalog
            .versions()
            .iter()
            .find(|descriptor| descriptor.identity() == target)
            .map(|descriptor| descriptor.location());
        let mut distinct = BTreeMap::new();
        // Bound each source before deduplication, and the physical union after it.
        for (index, reference) in live
            .into_iter()
            .enumerate()
            .chain(historical.into_iter().enumerate())
            .chain(current.into_iter().cloned().enumerate())
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
            catalog_generation: catalog.generation(),
            current: current.cloned(),
            locations: distinct.into_values().collect(),
        })
    }

    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub const fn catalog_generation(&self) -> HistoryArchiveCatalogGeneration {
        self.catalog_generation
    }
    pub fn current_location(&self) -> Option<&ArtifactReference> {
        self.current.as_ref()
    }
    /// Every distinct published or recorded location for the target in this snapshot.
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
        let target =
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
        HistoryArchiveCopyHistory::from_locations(
            target,
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
            &HistoryArchiveCatalog::empty(target.resource()),
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
        let target =
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
        let observation = HistoryArchiveCopyHistory::from_sources(
            target,
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
            &HistoryArchiveCatalog::empty(target.resource()),
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

    #[rstest::rstest]
    #[case::already_recorded(true, false, None)]
    #[case::additional(false, false, Some(crate::StorageErrorKind::InputTooLarge))]
    #[case::conflicting_digest(true, true, Some(crate::StorageErrorKind::Conflict))]
    fn current_catalog_location_shares_the_bounded_physical_inventory(
        #[case] already_recorded: bool,
        #[case] conflicting_digest: bool,
        #[case] expected_error: Option<crate::StorageErrorKind>,
    ) {
        use super::super::{HistoryArchiveDescriptor, HistoryArchiveRow, tests::row};
        let original = row();
        let row = HistoryArchiveRow::new(
            original.identity(),
            original.valid_from(),
            Some(original.valid_from() + chrono::Duration::seconds(1)),
            original.payload().clone(),
        )
        .unwrap();
        let destination = DestinationId::generate();
        let historical: Vec<_> = (0..HISTORY_ARCHIVE_MAX_CAPTURES)
            .map(|_| ArtifactReference::from_catalog(ArtifactIdentity::new(destination), [1; 32]))
            .collect();
        let id = if already_recorded {
            historical[0].identity().clone()
        } else {
            ArtifactIdentity::new(destination)
        };
        let current =
            ArtifactReference::from_catalog(id, if conflicting_digest { [2; 32] } else { [1; 32] });
        let empty = HistoryArchiveCatalog::empty(row.identity().resource());
        let descriptor = HistoryArchiveDescriptor::new(&row, vec![], 1, current.clone()).unwrap();
        let catalog = empty.publish(empty.generation(), vec![descriptor]).unwrap();
        let result = HistoryArchiveCopyHistory::from_locations(
            row.identity(),
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
            &catalog,
            historical,
        );
        match expected_error {
            Some(expected) => assert_eq!(result.unwrap_err().kind(), expected),
            None => {
                let observed = result.unwrap();
                assert_eq!(observed.current_location(), Some(&current));
                assert_eq!(observed.locations().len(), HISTORY_ARCHIVE_MAX_CAPTURES);
            }
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
