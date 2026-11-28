//! Admit the archive inputs to an exact erasure batch before staging any output.
use super::*;
use crate::errors::ApiError;
use crate::services::archive::HistoryArchiveReaders;
use crate::services::artifacts::error as artifact_error;
use hubuum_storage_core::{
    ApplicationFence, HistoryArchiveCatalogGeneration, HistoryArchiveStorage,
};

/// A planning observation. A catalog handoff must recheck these generations and
/// deletion authority before replacing a published source.
pub struct ObservedArchiveRewrite {
    fence: ApplicationFence,
    catalogs: BTreeMap<ResourceIdentity, HistoryArchiveCatalogGeneration>,
    locations: Vec<ArtifactReference>,
    admitted: AdmittedRewriteSources,
}

impl ObservedArchiveRewrite {
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }

    pub fn catalogs(&self) -> &BTreeMap<ResourceIdentity, HistoryArchiveCatalogGeneration> {
        &self.catalogs
    }

    pub fn locations(&self) -> &[ArtifactReference] {
        &self.locations
    }

    pub fn for_source(
        &self,
        source: &RetainedHistorySegment,
    ) -> ArtifactResult<&SegmentRewriteSelection> {
        self.admitted.for_source(source)
    }
}

impl SegmentRewriteSelection {
    /// Observe every selected version through the privileged storage capability.
    /// Group shared segments before reading their indexes, and charge each
    /// physical selected frame and its bytes to the same batch.
    pub async fn admit_observed_archive_copies(
        &self,
        operator: &impl HistoryArchiveStorage,
        readers: &HistoryArchiveReaders,
        fence: ApplicationFence,
        index_budget: &mut ByteBudget,
    ) -> Result<ObservedArchiveRewrite, ApiError> {
        let mut catalogs = BTreeMap::new();
        let mut physical =
            BTreeMap::<ArtifactIdentity, (ArtifactReference, Vec<StorageRecordIdentity>)>::new();
        let mut remaining_frames = HistoryExpiryManifest::MAX_MEMBERS;
        let mut admission = self.start_admission();
        for (&version, &resource) in self.members.iter() {
            let target = StorageRecordIdentity::new(resource, version);
            let observation = operator.history_archive_copy_history(target, fence).await?;
            if observation.target() != target || observation.fence() != fence {
                return Err(ApiError::Conflict(
                    "Archive copy observation differs from the requested version or fence".into(),
                ));
            }
            if catalogs
                .insert(resource, observation.catalog_generation())
                .is_some_and(|previous| previous != observation.catalog_generation())
            {
                return Err(ApiError::Conflict(
                    "Archive catalog changed during rewrite planning".into(),
                ));
            }
            let locations = observation.locations();
            remaining_frames = remaining_frames
                .checked_sub(locations.len())
                .ok_or_else(|| {
                    ApiError::PayloadTooLarge("Too many selected physical history frames".into())
                })?;
            if locations.is_empty() {
                // This exact version has no published or recorded archive copy in
                // its snapshot. Keep it in the batch's surviving-dependency checks.
                admission.unseen.remove(&version);
            }
            for location in locations {
                let (reference, targets) = physical
                    .entry(location.identity().clone())
                    .or_insert_with(|| (location.clone(), Vec::new()));
                if reference != location {
                    return Err(ApiError::Conflict(
                        "Archive copy observations disagree on the immutable manifest".into(),
                    ));
                }
                targets.push(target);
            }
        }
        let mut locations = Vec::with_capacity(physical.len());
        for (location, targets) in physical.into_values() {
            let store = readers.for_reference(&location);
            let source = RetainedHistorySegment::open(store.as_ref(), &location, index_budget)
                .await
                .map_err(artifact_error)?;
            admission = admission
                .include(&source, targets)
                .map_err(artifact_error)?;
            locations.push(location);
        }
        Ok(ObservedArchiveRewrite {
            fence,
            catalogs,
            locations,
            admitted: admission.finish().map_err(artifact_error)?,
        })
    }
}
