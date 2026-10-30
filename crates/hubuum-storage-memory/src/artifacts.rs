use super::*;
use hubuum_storage_core::artifacts::*;
use hubuum_storage_core::{TaskPurgeBlock, TaskReceiptActivation};

use super::task_retention::{PurgeWork, measured};

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
    pub(super) fn has_available_task_output(&self, task: TaskIdentity, now: DateTime<Utc>) -> bool {
        self.attempts.values().any(|attempt| {
            attempt.phase == Phase::Published
                && attempt
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.task == task && owner.expires > now)
        })
    }

    pub(super) fn has_task_output_for_cleanup(
        &self,
        task: TaskIdentity,
        now: DateTime<Utc>,
    ) -> bool {
        self.attempts.values().any(|attempt| {
            attempt.owner.as_ref().is_some_and(|owner| {
                owner.task == task && (attempt.phase != Phase::Published || owner.expires <= now)
            })
        })
    }

    pub(super) fn purge_task(
        &mut self,
        task: TaskIdentity,
        work: &mut PurgeWork,
    ) -> Result<Result<bool, TaskPurgeBlock>, StorageError> {
        let ids: Vec<_> = self
            .attempts
            .iter()
            .filter(|(_, attempt)| {
                attempt
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.task == task)
            })
            .take(work.remaining_rows().max(1) as usize)
            .map(|(id, _)| *id)
            .collect();
        macro_rules! charge {
            ($bytes:expr) => {
                match work.charge($bytes) {
                    Ok(true) => {}
                    Ok(false) => return Ok(Ok(false)),
                    Err(block) => return Ok(Err(block)),
                }
            };
        }
        let now = Utc::now();
        for id in ids {
            let attempt = self.attempts.get_mut(&id).expect("selected owned attempt");
            let owner = attempt.owner.as_mut().expect("selected owner");
            if attempt.phase == Phase::Published && owner.expires > now {
                return Ok(Err(TaskPurgeBlock::OutputAvailable));
            }
            let owner_bytes = measured(&(
                owner.task,
                owner.lease.task_id().id(),
                owner.lease.token().adapter_value(),
                owner.lease.execution_fence(),
                owner.reservation.worker(),
                owner.reservation.spool_bytes(),
                owner.reservation.worker_bytes(),
                owner.reserved,
                owner.cleanup_after,
                owner.expires,
            ))?;
            if owner.reservation.spool_bytes() > 0 && owner.cleanup_after.is_none() {
                charge!(owner_bytes);
                owner.cleanup_after = Some(
                    now + Duration::from_std(LOCAL_SPOOL_CLEANUP_GRACE)
                        .expect("finite cleanup grace"),
                );
                return Ok(Ok(false));
            }
            if owner.cleanup_after.is_some_and(|after| after > now) {
                return Ok(Err(TaskPurgeBlock::Dependency));
            }
            if attempt.phase != Phase::Discarded {
                attempt.phase = Phase::Deleting;
                while let Some((ordinal, bytes)) = attempt.blocks.first_key_value() {
                    charge!(bytes.len() as u64 + measured(&(&attempt.identity, ordinal))?);
                    attempt.blocks.pop_first();
                }
                charge!(
                    measured(&(
                        &attempt.identity,
                        attempt.limits.block_bytes(),
                        attempt.limits.artifact_bytes(),
                        attempt.limits.blocks(),
                        attempt.limits.manifest_bytes(),
                        attempt.bytes,
                        attempt.expires,
                        &attempt.reference,
                    ))? + attempt
                        .manifest
                        .as_ref()
                        .map_or(0, |manifest| manifest.encoded_len() as u64)
                );
                attempt.manifest = None;
                attempt.phase = Phase::Discarded;
            }
            charge!(owner_bytes);
            attempt.owner = None;
        }
        Ok(Ok(!self.attempts.values().any(|attempt| {
            attempt
                .owner
                .as_ref()
                .is_some_and(|owner| owner.task == task)
        })))
    }

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
            owner.reserved = proof.manifest().stored_bytes();
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
        output_expires_at: DateTime<Utc>,
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
        let reference = self
            .artifacts
            .publish(proof)
            .map_err(|error| StorageError::conflict(error.to_string()))?;
        self.artifacts
            .attempt_mut(proof.manifest().identity())
            .map_err(|error| StorageError::conflict(error.to_string()))?
            .owner
            .as_mut()
            .expect("validated task owner")
            .expires = output_expires_at;
        Ok(reference)
    }
}

#[async_trait]
impl ArtifactStore for MemoryStorage {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        let mut state = self.state.write().await.map_err(boundary)?;
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
        let mut state = self.state.write().await.map_err(boundary)?;
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
        let mut state = self.state.write().await.map_err(boundary)?;
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
        let mut state = self.state.write().await.map_err(boundary)?;
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
        let state = self.state.read().await.map_err(boundary)?;
        let attempt = state.artifacts.attempt(identity)?;
        attempt.readable()?;
        Ok(attempt.manifest.as_ref().ok_or_else(conflict)?.encode())
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        let state = self.state.read().await.map_err(boundary)?;
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
        let state = self.state.read().await.map_err(boundary)?;
        let attempt = state.artifacts.attempt(identity)?;
        attempt.readable()?;
        Ok(attempt
            .blocks
            .get(&ordinal)
            .map(|bytes| bytes.as_ref().clone()))
    }
    async fn publish(&self, proof: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        let mut state = self.state.write().await.map_err(boundary)?;
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
        let mut state = self.state.write().await.map_err(boundary)?;
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
        let mut state = self.state.write().await.map_err(boundary)?;
        let now = Utc::now();
        let candidate = state
            .artifacts
            .attempts
            .iter()
            .find(|(_, attempt)| {
                if let Some(owner) = &attempt.owner
                    && !matches!(
                        state.task_control.activation(),
                        TaskReceiptActivation::Disabled
                    )
                    && (attempt.reference.is_some()
                        || state.task_control.holds_task(owner.task, now))
                {
                    return false;
                }
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
        let mut state = self.state.write().await.map_err(boundary)?;
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
            .checked_add(u64::from(attempt.limits.manifest_bytes()))
            .and_then(|bytes| bytes.checked_add(reservation.spool_bytes()))
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
                    self.check_restored_artifact_space(header.manifest().stored_bytes())?;
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
            reserved: manifest.stored_bytes(),
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

#[cfg(test)]
mod retention_tests {
    use super::*;
    use rstest::rstest;

    async fn fixture(
        spool_bytes: u64,
        terminal: bool,
    ) -> (MemoryStorage, TaskIdentity, ArtifactReference) {
        let storage = MemoryStorage::new();
        let StorageTaskSubmissionOutcome::CreatedReady(task) = storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    PrincipalId::new(1).unwrap(),
                    serde_json::json!({}),
                    1,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .unwrap()
        else {
            panic!("fixture task")
        };
        let claim = storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
            .await
            .unwrap()
            .unwrap();
        if terminal {
            storage
                .fail_task(StorageTaskFailure::new(
                    claim.lease().clone(),
                    "Fixture settled",
                    StorageTaskEventInput::new("failed", "Fixture settled"),
                ))
                .await
                .unwrap();
        }
        let mut state = storage.state.write_control().await;
        let task_identity = state.task_identity(task.id()).unwrap();
        let identity = ArtifactIdentity::new(DestinationId::generate());
        let limits = ArtifactLimits::builder()
            .block_bytes(1024)
            .artifact_bytes(20 * 1024)
            .blocks(20)
            .manifest_bytes(1024)
            .build()
            .unwrap();
        let mut builder = ManifestBuilder::new(identity.clone(), limits);
        for _ in 0..20 {
            builder.push(&[7; 1024]).unwrap();
        }
        let manifest = builder.finish().unwrap();
        let mut verifier = manifest.verifier();
        for _ in 0..20 {
            verifier.push(&[7; 1024]).unwrap();
        }
        let proof = verifier.finish().unwrap();
        let reference = proof.reference();
        let published = terminal && spool_bytes == 0;
        let reserved = if published {
            manifest.stored_bytes()
        } else {
            limits.artifact_bytes() + u64::from(limits.manifest_bytes()) + spool_bytes
        };
        let lease = if terminal {
            claim.lease().clone()
        } else {
            let mut old = StorageTaskLease::new(
                task.id(),
                StorageTaskClaimToken::new(Uuid::new_v4().to_string()),
            );
            if let Some(fence) = claim.lease().execution_fence() {
                old = old.with_execution_fence(fence);
            }
            old
        };
        let key_fingerprint = ControlKeyFingerprint::new("a".repeat(64)).unwrap();
        state.task_control = state
            .task_control
            .with_activation(TaskReceiptActivation::Backfilling {
                key_fingerprint: key_fingerprint.clone(),
            })
            .unwrap()
            .with_activation(TaskReceiptActivation::Protected { key_fingerprint })
            .unwrap();
        state.artifacts.attempts.insert(
            identity.artifact(),
            Attempt {
                identity,
                limits,
                phase: if published {
                    Phase::Published
                } else {
                    Phase::Sealed
                },
                blocks: (0..20)
                    .map(|ordinal| (ordinal, Arc::new(vec![7; 1024])))
                    .collect(),
                bytes: manifest.byte_size(),
                manifest: Some(manifest),
                reference: published.then(|| reference.clone()),
                expires: Utc::now() + Duration::hours(1),
                owner: Some(Owner {
                    lease,
                    task: task_identity,
                    reservation: ArtifactReservation::new(
                        *Uuid::new_v4().as_bytes(),
                        spool_bytes,
                        64 * 1024,
                    )
                    .unwrap(),
                    reserved,
                    cleanup_after: None,
                    expires: if published {
                        Utc::now() - Duration::seconds(1)
                    } else {
                        Utc::now() + Duration::days(1)
                    },
                }),
            },
        );
        drop(state);
        (storage, task_identity, reference)
    }

    async fn proof(storage: &MemoryStorage) -> ProtectedTaskPurge {
        let ticket = storage
            .prepare_expired_task_outputs(
                TaskPurgeSelection::new(1, TaskPurgeBudget::default()).unwrap(),
            )
            .await
            .unwrap()
            .expect("expired physical output without a catalog reference");
        let state = storage.read_task_control().await.unwrap();
        ProtectedTaskPurge::new(
            ticket,
            TaskControlCheckpoint::new(
                state.lineage(),
                state.epoch(),
                state.revision(),
                DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn ordinary_cleanup_cannot_bypass_a_protected_artifact_purge() {
        let (storage, _, reference) = fixture(0, true).await;
        storage.cleanup().await.unwrap();
        assert!(storage.published_manifest(&reference).await.is_ok());
    }

    #[rstest]
    #[case::reclaim_retry_capacity(false)]
    #[case::retained_hold(true)]
    #[tokio::test]
    async fn abandoned_staging_cleanup_respects_holds_during_another_attempt(#[case] held: bool) {
        let (storage, task, reference) = fixture(0, false).await;
        if held {
            let state = storage.read_task_control().await.unwrap();
            let hold = TaskRetentionHold::new(
                TaskHoldId::generate(),
                task,
                Utc::now(),
                None,
                TaskHoldReason::Legal,
            )
            .unwrap();
            storage
                .change_task_hold(TaskHoldChange::new(&state, hold))
                .await
                .unwrap();
        }
        storage.cleanup().await.unwrap();
        storage.cleanup().await.unwrap();
        let state = storage.state.read_control().await;
        let attempt = &state.artifacts.attempts[&reference.identity().artifact()];
        assert_eq!(attempt.blocks.len(), if held { 20 } else { 0 });
    }

    #[tokio::test]
    async fn abandoned_unpublished_artifact_does_not_wait_for_result_expiry() {
        let (storage, _, reference) = fixture(0, true).await;
        {
            let mut state = storage.state.write_control().await;
            let attempt = state
                .artifacts
                .attempts
                .get_mut(&reference.identity().artifact())
                .unwrap();
            attempt.phase = Phase::Sealed;
            attempt.reference = None;
            let reservation =
                attempt.limits.artifact_bytes() + u64::from(attempt.limits.manifest_bytes());
            let owner = attempt.owner.as_mut().unwrap();
            owner.reserved = reservation;
            owner.expires = Utc::now() + Duration::days(1);
        }
        let protected = proof(&storage).await;
        assert!(matches!(
            storage
                .purge_task_details_batch(protected, TaskPurgeBudget::default())
                .await
                .unwrap(),
            TaskPurgeProgress::Removed { .. }
        ));
    }

    #[tokio::test]
    async fn protected_artifact_purge_charges_chunks_manifest_and_reservation_before_completion() {
        let (storage, _, reference) = fixture(0, true).await;
        let protected = proof(&storage).await;
        let budget = TaskPurgeBudget::new(2, 4096).unwrap();
        let mut complete = false;
        for _ in 0..32 {
            let progress = storage
                .purge_task_details_batch(protected.clone(), budget)
                .await
                .unwrap();
            let (rows, bytes) = match progress {
                TaskPurgeProgress::More { rows, bytes } => (rows, bytes),
                TaskPurgeProgress::Removed { rows, bytes, .. } => {
                    complete = true;
                    (rows, bytes)
                }
                other => panic!("unexpected purge outcome: {other:?}"),
            };
            assert!(rows <= budget.max_rows() && bytes <= budget.max_bytes());
            let state = storage.state.read_control().await;
            let attempt = &state.artifacts.attempts[&reference.identity().artifact()];
            if complete {
                assert!(
                    attempt.blocks.is_empty()
                        && attempt.manifest.is_none()
                        && attempt.owner.is_none()
                );
                break;
            }
        }
        assert!(complete);
    }

    #[rstest]
    #[case::hold(false)]
    #[case::spool_grace(true)]
    #[tokio::test]
    async fn physical_purge_preserves_blocked_bytes_and_reservations(#[case] spool: bool) {
        let (storage, task, reference) = fixture(if spool { 1024 } else { 0 }, true).await;
        let protected = proof(&storage).await;
        if !spool {
            let state = storage.read_task_control().await.unwrap();
            let hold = TaskRetentionHold::new(
                TaskHoldId::generate(),
                task,
                Utc::now(),
                None,
                TaskHoldReason::Legal,
            )
            .unwrap();
            storage
                .change_task_hold(TaskHoldChange::new(&state, hold))
                .await
                .unwrap();
        }
        let progress = storage
            .purge_task_details_batch(protected, TaskPurgeBudget::default())
            .await
            .unwrap();
        if spool {
            assert!(matches!(progress, TaskPurgeProgress::More { .. }));
        } else {
            assert!(matches!(
                progress,
                TaskPurgeProgress::Blocked(TaskPurgeBlock::Held)
            ));
        }
        let state = storage.state.read_control().await;
        let attempt = &state.artifacts.attempts[&reference.identity().artifact()];
        assert_eq!(attempt.blocks.len(), 20);
        assert!(attempt.owner.is_some());
        if spool {
            assert!(attempt.owner.as_ref().unwrap().cleanup_after.unwrap() > Utc::now());
        }
    }
}
