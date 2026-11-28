//! Sealed filesystem staging for a single closed collection version.
use chrono::Utc;
use hubuum_artifacts::{
    ArtifactCreationIntent, ArtifactIdentity, ArtifactLimits, ArtifactManifest,
    FilesystemArtifactStore, ManifestBuilder, OwnedArtifactAdmission, OwnedArtifactPhase,
    OwnedArtifactResolution, OwnedArtifactStore, VerifiedArtifact, verify,
};
use hubuum_budget::ByteLimit;
use hubuum_storage_core::{
    ExecutionStorage, HISTORY_ARCHIVE_PUBLICATION_BYTES, HistoryArchiveCapture,
    HistoryArchiveEncodedArtifactIntent, HistoryArchiveExecutionPermit,
    HistoryArchivePublicationClaim, HistoryArchivePublicationManifest,
    HistoryArchivePublicationRequest, HistoryArchiveStorage,
};

use crate::errors::ApiError;
use crate::services::artifacts::error as artifact_error;
use crate::services::history::segments::{
    BLOCK_BYTES, SegmentRecord, SegmentScope, VersionInterval, encode_claimed_record,
};
use crate::storage::StorageHandle;

/// A durable claim for a bound filesystem intent. Dropping this handle leaves
/// the physical obligation and capture pin in the recovery ledger.
pub struct ClaimedFilesystemHistory {
    storage: StorageHandle,
    claim: HistoryArchivePublicationClaim,
}

/// Exact bytes independently read back from this claim's sealed destination.
/// Catalog handoff still needs its own live transaction and publication fence.
pub struct SealedFilesystemHistory<'a> {
    claim: &'a HistoryArchivePublicationClaim,
    artifact: VerifiedArtifact,
}

impl SealedFilesystemHistory<'_> {
    pub fn claim(&self) -> &HistoryArchivePublicationClaim {
        self.claim
    }
    pub fn artifact(&self) -> &VerifiedArtifact {
        &self.artifact
    }
}

impl ClaimedFilesystemHistory {
    pub async fn reserve(
        storage: &StorageHandle,
        store: &FilesystemArtifactStore,
        capture: &HistoryArchiveCapture,
        permit: &HistoryArchiveExecutionPermit,
        location: ArtifactIdentity,
    ) -> Result<Self, ApiError> {
        let scope = permit.reenter_scope(&storage.current_execution_scope())?;
        permit.validate_scope(&scope)?;
        if capture.archive().is_some() {
            return Err(ApiError::Conflict(
                "History version already has an archive location".into(),
            ));
        }
        let manifest =
            HistoryArchivePublicationManifest::from_capture(capture, permit, location, Utc::now())?;
        let limits = ArtifactLimits::builder()
            .block_bytes(BLOCK_BYTES)
            .artifact_bytes(HISTORY_ARCHIVE_PUBLICATION_BYTES)
            .build()
            .map_err(artifact_error)?;
        let intent = ArtifactCreationIntent::new(
            manifest.location().clone(),
            limits,
            *permit.producer().id().as_bytes(),
            *permit.application().fence().incarnation().as_bytes(),
            manifest.request_commitment()?,
        )
        .map_err(artifact_error)?;
        let intent = store
            .bind_registered_intent(intent)
            .await
            .map_err(artifact_error)?;
        let encoded = HistoryArchiveEncodedArtifactIntent::try_new(
            intent.identity().clone(),
            limits,
            intent.encode().map_err(artifact_error)?,
            *intent.request_commitment(),
        )?;
        let request = HistoryArchivePublicationRequest::try_new(manifest, encoded)?;
        let claim = storage
            .run_in_scope_send(
                scope,
                storage.claim_history_archive_publication(request, permit),
            )
            .await?;
        Ok(Self {
            storage: storage.clone(),
            claim,
        })
    }

    pub fn claim(&self) -> &HistoryArchivePublicationClaim {
        &self.claim
    }

    async fn original_capture(&self) -> Result<HistoryArchiveCapture, ApiError> {
        let manifest = self.claim.manifest();
        let permit = self.claim.permit();
        let catalog = self
            .storage
            .history_archive_catalog(manifest.identity().resource(), permit)
            .await?;
        if catalog.generation() != manifest.generation() {
            return Err(ApiError::Conflict(
                "History archive catalog changed during staging".into(),
            ));
        }
        let capture = self
            .storage
            .history_archive_capture(manifest.capture(), manifest.principal(), permit)
            .await?
            .ok_or_else(|| ApiError::Gone("Claimed history capture is unavailable".into()))?;
        manifest.validate_capture(&capture, permit, Utc::now())?;
        Ok(capture)
    }

    pub async fn seal(
        &self,
        store: &FilesystemArtifactStore,
    ) -> Result<SealedFilesystemHistory<'_>, ApiError> {
        let permit = self.claim.permit();
        let scope = permit.reenter_scope(&self.storage.current_execution_scope())?;
        permit.validate_scope(&scope)?;
        self.storage
            .run_in_scope_send(scope, async {
                let capture = self.original_capture().await?;
                let intent =
                    ArtifactCreationIntent::decode(self.claim.request().intent().encoded())
                        .map_err(artifact_error)?;
                let location = intent.identity().clone();
                let limits = intent.limits();
                let (bytes, manifest) =
                    tokio::task::spawn_blocking(move || encode_attempt(location, limits, capture))
                        .await
                        .map_err(|_| {
                            ApiError::ServiceUnavailable(
                                "History segment encoding was interrupted".into(),
                            )
                        })??;

                let check_deadline = || -> Result<(), ApiError> {
                    permit.validate_scope(&self.storage.current_execution_scope())?;
                    if Utc::now() >= self.claim.manifest().execution_deadline() {
                        return Err(ApiError::Gone("History staging deadline expired".into()));
                    }
                    Ok(())
                };
                check_deadline()?;
                let admission = store
                    .create_owned(&intent)
                    .await
                    .map_err(|error| artifact_error(error.into()))?;
                let receipt = match admission {
                    OwnedArtifactAdmission::Created(receipt)
                    | OwnedArtifactAdmission::ResumedSameOwner {
                        artifact: receipt,
                        phase: OwnedArtifactPhase::Staging,
                    } => {
                        // Identical ordinals are replayable. An ambiguous failure leaves
                        // the claim and provider state available for retry or offline cleanup.
                        for (ordinal, block) in
                            bytes.chunks(limits.block_bytes() as usize).enumerate()
                        {
                            check_deadline()?;
                            store
                                .append_owned(&receipt, ordinal as u32, block)
                                .await
                                .map_err(|error| artifact_error(error.into()))?;
                        }
                        check_deadline()?;
                        store
                            .seal_owned(&receipt, &manifest)
                            .await
                            .map_err(|error| artifact_error(error.into()))?;
                        receipt
                    }
                    OwnedArtifactAdmission::ResumedSameOwner {
                        artifact,
                        phase: OwnedArtifactPhase::Sealed,
                    } => artifact,
                    OwnedArtifactAdmission::ResumedSameOwner { .. } => {
                        return Err(ApiError::Conflict(
                            "Claimed history attempt is no longer available for staging".into(),
                        ));
                    }
                };
                drop(bytes);
                let artifact = verify(&receipt.session(store), &manifest)
                    .await
                    .map_err(artifact_error)?;
                let phase = store
                    .resolve_owned(&intent)
                    .await
                    .map_err(|error| artifact_error(error.into()))?;
                if !matches!(
                    phase,
                    OwnedArtifactResolution::Established {
                        phase: OwnedArtifactPhase::Sealed,
                        ..
                    }
                ) {
                    return Err(ApiError::Conflict(
                        "History artifact is no longer sealed".into(),
                    ));
                }
                self.storage
                    .checkpoint_verified_history_archive(self.claim.verified_checkpoint(&artifact)?)
                    .await?;
                Ok(SealedFilesystemHistory {
                    claim: &self.claim,
                    artifact,
                })
            })
            .await
    }
}

fn encode_attempt(
    location: ArtifactIdentity,
    limits: ArtifactLimits,
    capture: HistoryArchiveCapture,
) -> Result<(Vec<u8>, ArtifactManifest), ApiError> {
    let row = capture
        .hot()
        .ok_or_else(|| ApiError::Gone("Claimed history row is unavailable".into()))?;
    let identity = row.identity();
    let interval = VersionInterval::new(
        identity.resource(),
        identity.version(),
        row.valid_from(),
        row.valid_to()
            .ok_or_else(|| ApiError::Conflict("Claimed history interval is open".into()))?,
    )
    .map_err(artifact_error)?;
    let record = SegmentRecord::new(
        SegmentScope::new(capture.selection().family(), identity.resource()),
        interval,
        row.payload().clone(),
    );

    let bytes = encode_claimed_record(record, ByteLimit::new(limits.artifact_bytes() as usize))
        .map_err(artifact_error)?;
    let mut manifest = ManifestBuilder::new(location, limits);
    for block in bytes.chunks(limits.block_bytes() as usize) {
        manifest.push(block).map_err(artifact_error)?;
    }
    Ok((bytes, manifest.finish().map_err(artifact_error)?))
}
