use super::*;
use hubuum_storage_core::artifacts::*;

#[derive(Clone, Default)]
pub(super) struct Artifacts {
    destinations: BTreeMap<DestinationId, String>,
    attempts: BTreeMap<ArtifactId, Attempt>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Staging,
    Sealed,
    Published,
    Deleting,
    Discarded,
}
#[derive(Clone)]
struct Owner {
    lease: StorageTaskLease,
    task: TaskIdentity,
    reservation: ArtifactReservation,
    reserved: u64,
    cleanup_after: Option<DateTime<Utc>>,
    expires: DateTime<Utc>,
}
#[derive(Clone)]
struct Attempt {
    identity: ArtifactIdentity,
    limits: ArtifactLimits,
    phase: Phase,
    blocks: BTreeMap<u32, Arc<Vec<u8>>>,
    bytes: u64,
    manifest: Option<ArtifactManifest>,
    reference: Option<ArtifactReference>,
    expires: DateTime<Utc>,
    owner: Option<Owner>,
}
fn conflict() -> ArtifactError {
    ArtifactError::Conflict("Artifact identity, state or owner changed".into())
}
fn boundary(error: StorageError) -> ArtifactError {
    ArtifactError::Backend(error.to_string())
}
impl Artifacts {
    pub(super) fn reservations(&self) -> impl Iterator<Item = (TaskIdentity, u64)> + '_ {
        self.attempts.values().filter_map(|attempt| {
            attempt
                .owner
                .as_ref()
                .map(|owner| (owner.task, owner.reserved))
        })
    }
    fn attempt(&self, identity: &ArtifactIdentity) -> ArtifactResult<&Attempt> {
        let attempt = self
            .attempts
            .get(&identity.artifact())
            .ok_or(ArtifactError::NotFound)?;
        if &attempt.identity != identity {
            return Err(conflict());
        }
        Ok(attempt)
    }
    fn attempt_mut(&mut self, identity: &ArtifactIdentity) -> ArtifactResult<&mut Attempt> {
        self.attempt(identity)?;
        Ok(self
            .attempts
            .get_mut(&identity.artifact())
            .expect("checked attempt"))
    }
    fn publish(&mut self, proof: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        let attempt = self.attempt_mut(proof.manifest().identity())?;
        attempt.readable()?;
        if attempt.manifest.as_ref() != Some(proof.manifest()) {
            return Err(conflict());
        }
        attempt.phase = Phase::Published;
        let reference = proof.reference();
        attempt.reference = Some(reference.clone());
        if let Some(owner) = &mut attempt.owner {
            owner.reserved = attempt.bytes;
            owner.reservation = ArtifactReservation::new(
                *owner.reservation.worker(),
                0,
                owner.reservation.worker_bytes(),
            )
            .expect("existing valid reservation");
        }
        Ok(reference)
    }
}
impl Attempt {
    fn writable(&self) -> ArtifactResult<()> {
        if self.phase != Phase::Staging || self.expires <= Utc::now() {
            return Err(conflict());
        }
        Ok(())
    }
    fn readable(&self) -> ArtifactResult<()> {
        if self.phase != Phase::Published
            && !(self.phase == Phase::Sealed && self.expires > Utc::now())
        {
            return Err(conflict());
        }
        Ok(())
    }
}
impl MemoryState {
    pub(super) fn publish_task_artifact(
        &mut self,
        lease: &StorageTaskLease,
        proof: &VerifiedArtifact,
    ) -> Result<ArtifactReference, StorageError> {
        let identity = self.task_identity(lease.task_id())?;
        let attempt = self
            .artifacts
            .attempt(proof.manifest().identity())
            .map_err(|error| StorageError::conflict(error.to_string()))?;
        let owner = attempt
            .owner
            .as_ref()
            .ok_or_else(|| StorageError::conflict("Task artifact has no owner"))?;
        if owner.task != identity || &owner.lease != lease || owner.expires <= Utc::now() {
            return Err(StorageError::conflict("Task artifact owner changed"));
        }
        self.artifacts
            .publish(proof)
            .map_err(|error| StorageError::conflict(error.to_string()))
    }
}

#[async_trait]
impl ArtifactStore for MemoryStorage {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        let mut state = self.state.write().await;
        if let Some(name) = state.artifacts.destinations.get(&destination.id()) {
            return if name == destination.name() {
                Ok(())
            } else {
                Err(conflict())
            };
        }
        if state
            .artifacts
            .destinations
            .values()
            .any(|name| name == destination.name())
        {
            return Err(conflict());
        }
        state
            .artifacts
            .destinations
            .insert(destination.id(), destination.name().into());
        Ok(())
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        let mut state = self.state.write().await;
        if !state
            .artifacts
            .destinations
            .contains_key(&identity.destination())
        {
            return Err(ArtifactError::NotFound);
        }
        if state.artifacts.attempts.contains_key(&identity.artifact()) {
            let attempt = state.artifacts.attempt(identity)?;
            attempt.writable()?;
            return if attempt.limits == limits {
                Ok(())
            } else {
                Err(conflict())
            };
        }
        if state.artifacts.attempts.values().any(|attempt| {
            attempt.identity.attempt() == identity.attempt()
                || attempt.identity.version() == identity.version()
        }) {
            return Err(conflict());
        }
        state.artifacts.attempts.insert(
            identity.artifact(),
            Attempt {
                identity: identity.clone(),
                limits,
                phase: Phase::Staging,
                blocks: BTreeMap::new(),
                bytes: 0,
                manifest: None,
                reference: None,
                expires: Utc::now() + Duration::hours(1),
                owner: None,
            },
        );
        Ok(())
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        let mut state = self.state.write().await;
        let attempt = state.artifacts.attempt_mut(identity)?;
        attempt.writable()?;
        if let Some(old) = attempt.blocks.get(&ordinal) {
            return if old.as_slice() == bytes {
                Ok(())
            } else {
                Err(conflict())
            };
        }
        if ordinal as usize != attempt.blocks.len()
            || bytes.is_empty()
            || bytes.len() > attempt.limits.block_bytes() as usize
            || ordinal >= attempt.limits.blocks()
            || 117 + (u64::from(ordinal) + 1) * 36 > u64::from(attempt.limits.manifest_bytes())
            || attempt.bytes + bytes.len() as u64 > attempt.limits.artifact_bytes()
            || attempt
                .blocks
                .last_key_value()
                .is_some_and(|(_, bytes)| bytes.len() != attempt.limits.block_bytes() as usize)
        {
            return Err(ArtifactError::Limit("Artifact block exceeds limits".into()));
        }
        attempt.blocks.insert(ordinal, Arc::new(bytes.to_vec()));
        attempt.bytes += bytes.len() as u64;
        Ok(())
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        let mut state = self.state.write().await;
        let attempt = state.artifacts.attempt_mut(manifest.identity())?;
        attempt.writable()?;
        ArtifactManifest::decode(&manifest.encode(), &attempt.identity, attempt.limits)?;
        if manifest.block_count() as usize != attempt.blocks.len()
            || manifest.byte_size() != attempt.bytes
        {
            return Err(conflict());
        }
        attempt.phase = Phase::Sealed;
        attempt.manifest = Some(manifest.clone());
        Ok(())
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        let state = self.state.read().await;
        let attempt = state.artifacts.attempt(identity)?;
        attempt.readable()?;
        Ok(attempt.manifest.as_ref().ok_or_else(conflict)?.encode())
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        let state = self.state.read().await;
        let attempt = state.artifacts.attempt(reference.identity())?;
        if attempt.phase != Phase::Published || attempt.reference.as_ref() != Some(reference) {
            return Err(conflict());
        }
        Ok(attempt.manifest.as_ref().ok_or_else(conflict)?.encode())
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        let state = self.state.read().await;
        let attempt = state.artifacts.attempt(identity)?;
        attempt.readable()?;
        Ok(attempt
            .blocks
            .get(&ordinal)
            .map(|bytes| bytes.as_ref().clone()))
    }
    async fn publish(&self, proof: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        let mut state = self.state.write().await;
        if state
            .artifacts
            .attempt(proof.manifest().identity())?
            .owner
            .is_some()
        {
            return Err(conflict());
        }
        state.artifacts.publish(proof)
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        let mut state = self.state.write().await;
        let attempt = state.artifacts.attempt_mut(identity)?;
        if attempt.phase == Phase::Published {
            return Err(conflict());
        }
        if attempt.phase != Phase::Discarded {
            attempt.phase = Phase::Deleting;
        }
        Ok(())
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        let candidate = state
            .artifacts
            .attempts
            .iter()
            .find(|(_, attempt)| {
                if attempt
                    .owner
                    .as_ref()
                    .and_then(|owner| owner.cleanup_after)
                    .is_some_and(|after| after > now)
                {
                    return false;
                }
                if attempt.phase == Phase::Discarded {
                    return false;
                }
                if attempt.phase == Phase::Deleting {
                    return true;
                }
                if let Some(owner) = &attempt.owner {
                    if owner.expires <= now {
                        return true;
                    }
                    let task = state.tasks.get(&owner.lease.task_id().id());
                    if state.task_identity(owner.lease.task_id()).ok() != Some(owner.task) {
                        return true;
                    }
                    if attempt.phase == Phase::Published {
                        let current = state
                            .export_outputs
                            .get(&owner.lease.task_id().id())
                            .and_then(|output| output.artifact_reference())
                            .or_else(|| {
                                state
                                    .backup_outputs
                                    .get(&owner.lease.task_id().id())
                                    .and_then(|output| output.artifact_reference())
                            });
                        if current != attempt.reference.as_ref() {
                            return true;
                        }
                    }
                    if attempt.phase != Phase::Published
                        && task.is_none_or(|task| {
                            !task.lease_matches(&owner.lease) || !task.status.is_active()
                        })
                    {
                        return true;
                    }
                }
                attempt.phase != Phase::Published && attempt.expires <= now
            })
            .map(|(id, _)| *id);
        let Some(id) = candidate else {
            return Ok(0);
        };
        let attempt = state
            .artifacts
            .attempts
            .get_mut(&id)
            .expect("selected attempt");
        attempt.phase = Phase::Deleting;
        if let Some(owner) = &mut attempt.owner
            && owner.reservation.spool_bytes() > 0
            && owner.cleanup_after.is_none()
        {
            owner.cleanup_after = Some(
                now + Duration::from_std(LOCAL_SPOOL_CLEANUP_GRACE).expect("finite cleanup grace"),
            );
            return Ok(0);
        }
        let mut deleted = 0;
        while deleted < 16 && attempt.blocks.pop_first().is_some() {
            deleted += 1;
        }
        if attempt.blocks.is_empty() {
            attempt.phase = Phase::Discarded;
            attempt.manifest = None;
            attempt.owner = None;
        }
        Ok(deleted)
    }
}

#[async_trait]
impl TaskArtifactStore for MemoryStorage {
    async fn bind_task(
        &self,
        identity: &ArtifactIdentity,
        lease: &StorageTaskLease,
        reservation: ArtifactReservation,
        output_expires_at: DateTime<Utc>,
    ) -> ArtifactResult<()> {
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .ok_or_else(conflict)?;
        if !task.lease_matches(lease)
            || !task.status.is_active()
            || task.control.stop_reason(Utc::now()).is_some()
        {
            return Err(conflict());
        }
        let task_identity = state.task_identity(lease.task_id()).map_err(boundary)?;
        let attempt = state.artifacts.attempt(identity)?;
        attempt.writable()?;
        if attempt.owner.is_some() {
            return Err(conflict());
        }
        let reserved = attempt
            .limits
            .artifact_bytes()
            .checked_add(reservation.spool_bytes())
            .ok_or_else(conflict)?;
        let mut worker = 0u128;
        for attempt in state.artifacts.attempts.values() {
            if let Some(owner) = &attempt.owner
                && attempt.phase != Phase::Published
                && owner.reservation.worker() == reservation.worker()
            {
                worker += u128::from(owner.reserved);
            }
        }
        if worker + u128::from(reserved) > u128::from(reservation.worker_bytes()) {
            return Err(ArtifactError::Limit(
                "Artifact resident-space capacity exhausted".into(),
            ));
        }
        state.artifacts.attempt_mut(identity)?.owner = Some(Owner {
            lease: lease.clone(),
            task: task_identity,
            reservation,
            reserved,
            cleanup_after: None,
            expires: output_expires_at,
        });
        if let Err(error) = state.check_artifact_capacity(lease.task_id()) {
            state.artifacts.attempt_mut(identity)?.owner = None;
            return Err(boundary(error));
        }
        Ok(())
    }
}

impl MemoryState {
    pub(super) fn capture_export_artifacts(&self) -> Result<Vec<StorageBackupRow>, StorageError> {
        let mut frames = Vec::new();
        for output in self.export_outputs.values() {
            if !self
                .tasks
                .get(&output.task_id().id())
                .is_some_and(|task| task.status.is_terminal())
            {
                continue;
            }
            let Some(reference) = output.artifact_reference() else {
                continue;
            };
            let attempt = self
                .artifacts
                .attempt(reference.identity())
                .map_err(archive_error)?;
            if attempt.phase != Phase::Published {
                return Err(archive_error("Output is not published"));
            }
            let manifest = attempt
                .manifest
                .as_ref()
                .ok_or_else(|| archive_error("Missing manifest"))?;
            if &manifest.manifest_digest() != reference.manifest_digest() {
                return Err(archive_error("Published manifest digest differs"));
            }
            let mut verifier = manifest.verifier();
            frames.push(
                ArtifactBackupFrame::header(
                    output.task_id(),
                    output.output_expires_at(),
                    reference.clone(),
                    manifest,
                )
                .into_row()?,
            );
            for (&ordinal, bytes) in &attempt.blocks {
                verifier.push(bytes).map_err(archive_error)?;
                frames.push(
                    ArtifactBackupFrame::block(reference.identity().artifact(), ordinal, bytes)
                        .into_row()?,
                );
            }
            verifier.finish().map_err(archive_error)?;
        }
        Ok(frames)
    }
    pub(super) fn restore_export_artifacts(
        &mut self,
        rows: &[StorageBackupRow],
    ) -> Result<BTreeMap<ArtifactId, ArtifactReference>, StorageError> {
        let mut decoder = BackupArtifactDecoder::default();
        let mut current: Option<MemoryRestoreArtifact> = None;
        let mut references = BTreeMap::new();
        for row in rows {
            match decoder.push(row.clone())? {
                BackupArtifactPart::Header(header) => {
                    if let Some(previous) = current.take() {
                        previous.finish(self, &mut references)?;
                    }
                    self.check_restored_artifact_space(header.manifest().byte_size())?;
                    let identity = ArtifactIdentity::new(
                        DestinationId::from_bytes([0x50; 16]).map_err(archive_error)?,
                    );
                    current = Some(MemoryRestoreArtifact {
                        builder: ManifestBuilder::new(identity.clone(), header.limits()),
                        identity,
                        header: *header,
                        blocks: BTreeMap::new(),
                    });
                }
                BackupArtifactPart::Block { ordinal, bytes } => {
                    let current = current
                        .as_mut()
                        .ok_or_else(|| archive_error("Missing header"))?;
                    current.builder.push(&bytes).map_err(archive_error)?;
                    current.blocks.insert(ordinal, Arc::new(bytes));
                }
            }
        }
        decoder.finish()?;
        if let Some(previous) = current {
            previous.finish(self, &mut references)?;
        }
        Ok(references)
    }
}
fn archive_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::invalid_input(format!("Backup artifact: {error}"))
}
struct MemoryRestoreArtifact {
    builder: ManifestBuilder,
    identity: ArtifactIdentity,
    header: BackupArtifactHeader,
    blocks: BTreeMap<u32, Arc<Vec<u8>>>,
}
impl MemoryRestoreArtifact {
    fn finish(
        self,
        state: &mut MemoryState,
        references: &mut BTreeMap<ArtifactId, ArtifactReference>,
    ) -> Result<(), StorageError> {
        let manifest = self.builder.finish().map_err(archive_error)?;
        let mut verifier = manifest.verifier();
        for bytes in self.blocks.values() {
            verifier.push(bytes).map_err(archive_error)?;
        }
        let proof = verifier.finish().map_err(archive_error)?;
        let reference = proof.reference();
        let owner = Owner {
            lease: StorageTaskLease::new(
                self.header.task_id(),
                StorageTaskClaimToken::new(uuid::Uuid::new_v4().to_string()),
            ),
            task: state.task_identity(self.header.task_id())?,
            reservation: ArtifactReservation::new(
                *uuid::Uuid::new_v4().as_bytes(),
                0,
                i64::MAX as u64,
            )
            .map_err(archive_error)?,
            reserved: manifest.byte_size(),
            cleanup_after: None,
            expires: self.header.expires_at(),
        };
        state
            .artifacts
            .destinations
            .insert(self.identity.destination(), "primary-artifacts".into());
        references.insert(
            self.header.reference().identity().artifact(),
            reference.clone(),
        );
        state.artifacts.attempts.insert(
            self.identity.artifact(),
            Attempt {
                identity: self.identity,
                limits: self.header.limits(),
                phase: Phase::Published,
                blocks: self.blocks,
                bytes: manifest.byte_size(),
                manifest: Some(manifest),
                reference: Some(reference),
                expires: Utc::now() + chrono::Duration::hours(1),
                owner: Some(owner),
            },
        );
        Ok(())
    }
}
