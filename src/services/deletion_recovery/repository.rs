use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
#[cfg(test)]
use hubuum_storage_core::DeletionControlCapture;
use hubuum_storage_core::{
    ApplicationReapplication, ControlCheckpointCapture, DELETION_CONTROL_MAX_BYTES,
    DeletionControlState, DeletionRecoveryPosition, TASK_CONTROL_MAX_RECEIPTS_BYTES,
    TASK_CONTROL_MAX_RETENTION_BYTES, TaskControlReapplication, TaskControlRevision,
    TaskControlState, TaskReceiptActivation,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::unavailable;
use crate::errors::ApiError;
use crate::observability::metrics::{
    CheckpointLockOutcome, CheckpointOperation, CheckpointTimer, checkpoint_lock_finished,
};
pub(super) use crate::observability::metrics::{CheckpointMeasurement, checkpoint_measurement};

mod measured_io;
mod payload_cache;
pub(super) use payload_cache::AuthenticatedPayloadCache;

#[cfg(all(test, unix))]
mod publication_tests;
mod retention;
mod runtime;
#[cfg(all(test, unix))]
mod size_tests;
pub(crate) use runtime::RuntimeIsolationGuard;

const MAX_FILE_BYTES: u64 = (DELETION_CONTROL_MAX_BYTES
    + TASK_CONTROL_MAX_RECEIPTS_BYTES
    + TASK_CONTROL_MAX_RETENTION_BYTES
    + 128 * 1024) as u64;
const HEAD_ALLOWANCE_BYTES: u64 = 64 * 1024;
const MAC_DOMAIN: &[u8] = b"hubuum.deletion-checkpoint.v1\0";

/// The directory, signing key and latest reference must remain outside every
/// supported rollback of the primary database. This is a single trusted-writer
/// repository; every writer uses the same advisory lock and signing key.
pub(super) struct CheckpointRepository {
    root: PathBuf,
    directory: File,
    key: Vec<u8>,
    payload_cache: Option<Arc<AuthenticatedPayloadCache>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    state: DeletionControlState,
    position: DeletionRecoveryPosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reapplication: Option<ApplicationReapplication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_control: Option<TaskControlState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_reapplication: Option<TaskControlReapplication>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    format: u32,
    lineage: Uuid,
    epoch: Uuid,
    revision: i64,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_revision: Option<TaskControlRevision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed<T> {
    payload: T,
    mac: String,
}

/// Only independent authenticated read-back can construct this proof.
pub(super) struct DurableCheckpoint {
    payload: Arc<Payload>,
    sha256: String,
    previous_sha256: Option<String>,
}

/// An owned publication session keeps cleanup and its completion checkpoint
/// ordered against every other administrator using this repository.
pub(super) struct CheckpointSession {
    repository: CheckpointRepository,
    checkpoint: DurableCheckpoint,
    _lock: File,
}

impl CheckpointSession {
    pub(super) fn into_checkpoint(self) -> DurableCheckpoint {
        self.checkpoint
    }

    pub(super) fn checkpoint(&self) -> &DurableCheckpoint {
        &self.checkpoint
    }

    #[cfg(test)]
    pub(super) fn publish(&mut self, capture: &DeletionControlCapture) -> Result<(), ApiError> {
        self.checkpoint = self.repository.publish_locked(capture, false)?;
        Ok(())
    }

    pub(super) fn publish_control(
        &mut self,
        capture: &ControlCheckpointCapture,
    ) -> Result<(), ApiError> {
        self.checkpoint = self.repository.publish_control_locked(capture, false)?;
        Ok(())
    }
}

/// Keep latest-reference publication excluded until recovery commits and its
/// state is independently read back. Process death releases the OS lock.
pub(super) struct RecoveryEvidence {
    checkpoint: DurableCheckpoint,
    _lock: File,
}

impl RecoveryEvidence {
    pub(super) fn checkpoint(&self) -> &DurableCheckpoint {
        &self.checkpoint
    }

    pub(super) fn state(&self) -> &DeletionControlState {
        self.checkpoint.state()
    }
}

impl DurableCheckpoint {
    pub(super) fn state(&self) -> &DeletionControlState {
        &self.payload.state
    }

    pub(super) fn reapplication(&self) -> Option<&ApplicationReapplication> {
        self.payload.reapplication.as_ref()
    }

    pub(super) fn identity(&self) -> &str {
        &self.sha256
    }

    pub(super) fn task_control(&self) -> Option<&TaskControlState> {
        self.payload.task_control.as_ref()
    }

    pub(super) fn task_reapplication(&self) -> Option<&TaskControlReapplication> {
        self.payload.task_reapplication.as_ref()
    }
}

#[cfg(unix)]
fn checked_file(path: &Path, create: bool) -> Result<File, ApiError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = OpenOptions::new()
        .read(true)
        .write(create)
        .create(create)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| unavailable("Control repository file is unavailable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| unavailable("Cannot inspect control file"))?;
    // SAFETY: geteuid has no pointer arguments or preconditions.
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(unavailable(
            "Control files must be ordinary, owner-only files owned by the administrator",
        ));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn checked_file(_path: &Path, _create: bool) -> Result<File, ApiError> {
    Err(unavailable(
        "Independent control repositories currently require Unix filesystem semantics",
    ))
}

fn read_bounded(file: File) -> Result<Vec<u8>, ApiError> {
    let bytes = measured_io::read(file, MAX_FILE_BYTES + 1)
        .map_err(|_| unavailable("Cannot read recovery evidence"))?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(unavailable("Recovery evidence exceeds its byte limit"));
    }
    Ok(bytes)
}

impl CheckpointRepository {
    pub(super) fn task_receipt_key(&self) -> Result<super::task_key::TaskReceiptKey, ApiError> {
        let bytes = self
            .key
            .as_slice()
            .try_into()
            .map_err(|_| unavailable("Invalid task-control authentication key"))?;
        Ok(super::task_key::TaskReceiptKey::new(bytes))
    }

    pub(super) fn open(root: &Path, key_file: &Path) -> Result<Self, ApiError> {
        let metadata = fs::symlink_metadata(root)
            .map_err(|_| unavailable("Control repository directory is unavailable"))?;
        if !metadata.is_dir() {
            return Err(unavailable(
                "Control repository must be a directory, not a symlink",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // SAFETY: geteuid has no pointer arguments or preconditions.
            if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
                return Err(unavailable(
                    "Control repository must be administrator-owned with mode 0700",
                ));
            }
        }
        let root =
            fs::canonicalize(root).map_err(|_| unavailable("Cannot resolve control repository"))?;
        let directory =
            File::open(&root).map_err(|_| unavailable("Cannot open control repository"))?;
        let key = read_bounded(checked_file(key_file, false)?)?;
        if key.len() != 32 {
            return Err(unavailable(
                "Control signing key must contain exactly 32 random bytes",
            ));
        }
        Ok(Self {
            root,
            directory,
            key,
            payload_cache: None,
        })
    }

    pub(super) fn with_payload_cache(mut self, cache: Arc<AuthenticatedPayloadCache>) -> Self {
        self.payload_cache = Some(cache);
        self
    }

    fn signed<T: Serialize>(&self, payload: T) -> Result<Vec<u8>, ApiError> {
        let timer = CheckpointTimer::start(CheckpointOperation::EncodeSign);
        let bytes = serde_json::to_vec(&payload)
            .map_err(|_| unavailable("Cannot encode recovery evidence"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| unavailable("Invalid control key"))?;
        mac.update(MAC_DOMAIN);
        mac.update(&bytes);
        let result = serde_json::to_vec(&Signed {
            payload,
            mac: STANDARD.encode(mac.finalize().into_bytes()),
        })
        .map_err(|_| unavailable("Cannot encode authenticated recovery evidence"));
        timer.finish(result.is_ok());
        result
    }

    fn verified<T: Serialize + DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, ApiError> {
        let timer = CheckpointTimer::start(CheckpointOperation::Verify);
        let signed: Signed<T> =
            serde_json::from_slice(bytes).map_err(|_| unavailable("Invalid recovery evidence"))?;
        let bytes = serde_json::to_vec(&signed.payload)
            .map_err(|_| unavailable("Invalid recovery evidence"))?;
        let tag = STANDARD
            .decode(&signed.mac)
            .map_err(|_| unavailable("Invalid recovery authentication"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| unavailable("Invalid control key"))?;
        mac.update(MAC_DOMAIN);
        mac.update(&bytes);
        mac.verify_slice(&tag)
            .map_err(|_| unavailable("Recovery evidence authentication failed"))?;
        timer.finish(true);
        Ok(signed.payload)
    }

    #[cfg(test)]
    pub(super) fn latest(&self) -> Result<DurableCheckpoint, ApiError> {
        let _lock = self.lock()?;
        self.latest_locked()
    }

    pub(super) fn recovery_evidence(&self) -> Result<RecoveryEvidence, ApiError> {
        let lock = self.lock()?;
        Ok(RecoveryEvidence {
            checkpoint: self.latest_locked()?,
            _lock: lock,
        })
    }

    pub(super) fn into_session(self) -> Result<CheckpointSession, ApiError> {
        self.try_into_session()?.ok_or_else(repository_busy)
    }

    /// Contention is the only deferred outcome. Authentication and filesystem
    /// failures remain errors, and an acquired session still verifies HEAD.
    pub(super) fn try_into_session(self) -> Result<Option<CheckpointSession>, ApiError> {
        let Some(lock) = self.try_lock()? else {
            return Ok(None);
        };
        let checkpoint = self.latest_locked()?;
        Ok(Some(CheckpointSession {
            repository: self,
            checkpoint,
            _lock: lock,
        }))
    }

    fn latest_locked(&self) -> Result<DurableCheckpoint, ApiError> {
        let head: Head = self.verified(&read_bounded(checked_file(
            &self.root.join("HEAD"),
            false,
        )?)?)?;
        if !matches!(head.format, 1 | 2)
            || head.sha256.len() != 64
            || !head
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(unavailable("Invalid latest checkpoint reference"));
        }
        let bytes = read_bounded(checked_file(
            &self.root.join(format!("{}.json", head.sha256)),
            false,
        )?)?;
        if digest(&bytes) != head.sha256 {
            return Err(unavailable("Checkpoint recovery material digest mismatch"));
        }
        // A hit reuses only the immutable decoding of these exact independently
        // read and authenticated bytes. HEAD, file checks, digest, correspondence
        // and durability are established afresh for every returned proof.
        let key_fingerprint = self.task_receipt_key()?.fingerprint()?;
        let cached = self
            .payload_cache
            .as_ref()
            .map(|cache| cache.get(&head.sha256, &key_fingerprint))
            .transpose()?
            .flatten();
        let payload = match cached {
            Some(payload) => payload,
            None => Arc::new(self.verified::<Payload>(&bytes)?),
        };
        if payload.state.lineage() != head.lineage
            || payload.state.epoch() != head.epoch
            || payload.state.revision() != head.revision
            || payload
                .reapplication
                .as_ref()
                .is_some_and(|receipt| receipt.revision() != head.revision)
            || head.task_revision
                != payload
                    .task_control
                    .as_ref()
                    .map(TaskControlState::revision)
            || (head.format == 2) != payload.task_control.is_some()
            || payload
                .task_control
                .as_ref()
                .is_some_and(|tasks| tasks.lineage() != head.lineage || tasks.epoch() != head.epoch)
            || payload.task_reapplication.as_ref().is_some_and(|proof| {
                payload
                    .task_control
                    .as_ref()
                    .is_none_or(|tasks| proof.task_revision() > tasks.revision())
                    || payload
                        .reapplication
                        .as_ref()
                        .is_some_and(|deletion| deletion.incarnation() != proof.incarnation())
            })
            || head.previous_sha256.as_ref().is_some_and(|previous| {
                previous == &head.sha256
                    || previous.len() != 64
                    || !previous
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            return Err(unavailable(
                "Checkpoint identity differs from latest reference",
            ));
        }
        self.validate_task_key(&payload)?;
        // A prior publisher may have lost acknowledgement after rename but
        // before directory synchronization. A visible HEAD alone is not proof
        // that its name survives power loss; finish the durability boundary on
        // read/retry before returning a durable proof.
        measured_io::sync_directory(&self.directory)
            .map_err(|_| unavailable("Cannot establish latest checkpoint durability"))?;
        if let Some(cache) = &self.payload_cache {
            cache.remember(head.sha256.clone(), key_fingerprint, Arc::clone(&payload))?;
        }
        Ok(DurableCheckpoint {
            payload,
            sha256: head.sha256,
            previous_sha256: head.previous_sha256,
        })
    }

    #[cfg(test)]
    pub(super) fn publish(
        &self,
        capture: &DeletionControlCapture,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        let _lock = self.lock()?;
        self.publish_locked(capture, initialize)
    }

    #[cfg(test)]
    fn publish_locked(
        &self,
        capture: &DeletionControlCapture,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        self.publish_payload_locked(
            Payload {
                state: capture.state().clone(),
                position: capture.position().clone(),
                reapplication: capture.reapplication().cloned(),
                task_control: None,
                task_reapplication: None,
            },
            initialize,
        )
    }

    pub(super) fn publish_control(
        &self,
        capture: &ControlCheckpointCapture,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        let _lock = self.lock()?;
        self.publish_control_locked(capture, initialize)
    }

    fn publish_control_locked(
        &self,
        capture: &ControlCheckpointCapture,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        self.publish_payload_locked(
            Payload {
                state: capture.deletion().state().clone(),
                position: capture.deletion().position().clone(),
                reapplication: capture.deletion().reapplication().cloned(),
                task_control: Some(capture.task_control().clone()),
                task_reapplication: capture.task_reapplication().cloned(),
            },
            initialize,
        )
    }

    fn publish_payload_locked(
        &self,
        payload: Payload,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        let timer = CheckpointTimer::start(CheckpointOperation::Publish);
        let result = self.publish_payload(payload, initialize);
        timer.finish(result.is_ok());
        result
    }

    fn publish_payload(
        &self,
        payload: Payload,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        self.validate_task_key(&payload)?;
        let previous = if initialize {
            // Bootstrap is explicit and restricted to a fresh, empty authority
            // and repository. Missing HEAD is never interpreted as an empty ledger.
            if payload.state.revision() != 0
                || !payload.state.intents().is_empty()
                || payload.task_control.as_ref().is_some_and(|tasks| {
                    tasks.revision().value() != 0
                        || !matches!(tasks.activation(), TaskReceiptActivation::Disabled)
                        || !tasks.receipts().is_empty()
                        || !tasks.purge_batches().is_empty()
                        || !tasks.holds().is_empty()
                        || tasks.policies().len() != 1
                })
            {
                return Err(unavailable(
                    "Only empty deletion authority can initialize a repository",
                ));
            }
            let entries =
                fs::read_dir(&self.root).map_err(|_| unavailable("Cannot inspect repository"))?;
            for entry in entries {
                if entry
                    .map_err(|_| unavailable("Cannot inspect repository entry"))?
                    .file_name()
                    != "LOCK"
                {
                    return Err(unavailable(
                        "Repository initialization requires an empty directory",
                    ));
                }
            }
            None
        } else {
            let current = self.latest_locked()?;
            if !payload.state.covers(current.state()) {
                return Err(unavailable(
                    "Checkpoint would discard or roll back acknowledged deletion authority",
                ));
            }
            match (&payload.task_control, current.task_control()) {
                (None, Some(_)) => {
                    return Err(unavailable(
                        "Checkpoint cannot discard protected task authority",
                    ));
                }
                (Some(tasks), Some(old)) if !tasks.covers(old) => {
                    return Err(unavailable(
                        "Checkpoint would discard protected task receipts or retention decisions",
                    ));
                }
                _ => {}
            }
            if payload.state.revision() == current.state().revision()
                && payload
                    .task_control
                    .as_ref()
                    .map(TaskControlState::revision)
                    == current.task_control().map(TaskControlState::revision)
            {
                if &payload.state != current.state()
                    || payload.reapplication.as_ref() != current.reapplication()
                    || payload.task_control.as_ref() != current.task_control()
                    || payload.task_reapplication.as_ref() != current.task_reapplication()
                {
                    return Err(unavailable(
                        "Conflicting checkpoint at the same revision vector",
                    ));
                }
                self.collect_superseded(&current)?;
                return Ok(current);
            }
            self.collect_superseded(&current)?;
            Some(current)
        };
        let bytes = self.signed(&payload)?;
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(unavailable("Checkpoint exceeds its byte limit"));
        }
        let sha256 = digest(&bytes);
        let path = self.root.join(format!("{sha256}.json"));
        if path
            .try_exists()
            .map_err(|_| unavailable("Cannot inspect staged checkpoint"))?
        {
            let mut file = checked_file(&path, true)?;
            let existing = read_bounded(
                file.try_clone()
                    .map_err(|_| unavailable("Cannot inspect staged checkpoint"))?,
            )?;
            if !bytes.starts_with(&existing) {
                return Err(unavailable("Conflicting staged checkpoint"));
            }
            let missing = bytes.len() - existing.len();
            if missing != 0
                && previous.as_ref().is_some_and(|current| {
                    current.sha256 == sha256 || current.previous_sha256.as_ref() == Some(&sha256)
                })
            {
                return Err(unavailable(
                    "Published checkpoint material cannot be repaired in place",
                ));
            }
            self.reserve_publication_space(missing as u64 + HEAD_ALLOWANCE_BYTES)?;
            // The cloned descriptor shares the original cursor. Complete only
            // the exact known prefix, through that same checked open file.
            measured_io::write(&mut file, &bytes[existing.len()..])
                .and_then(|()| measured_io::sync_file(&file))
                .map_err(|_| unavailable("Cannot sync existing staged checkpoint"))?;
        } else {
            self.reserve_publication_space(bytes.len() as u64 + HEAD_ALLOWANCE_BYTES)?;
            self.write_new(&path, &bytes)?;
        }
        measured_io::sync_directory(&self.directory)
            .map_err(|_| unavailable("Cannot sync recovery material directory"))?;
        // Read back before the monotonic publication point.
        if read_bounded(checked_file(&path, false)?)? != bytes {
            return Err(unavailable("Recovery material read-back failed"));
        }
        let head = self.signed(Head {
            format: if payload.task_control.is_some() { 2 } else { 1 },
            lineage: payload.state.lineage(),
            epoch: payload.state.epoch(),
            revision: payload.state.revision(),
            sha256: sha256.clone(),
            task_revision: payload
                .task_control
                .as_ref()
                .map(TaskControlState::revision),
            previous_sha256: previous
                .as_ref()
                .map(|checkpoint| checkpoint.sha256.clone()),
        })?;
        if head.len() as u64 > HEAD_ALLOWANCE_BYTES {
            return Err(unavailable("Checkpoint reference exceeds its byte limit"));
        }
        let temporary = self.root.join(format!("HEAD-{}", Uuid::new_v4()));
        self.write_new(&temporary, &head)?;
        fs::rename(&temporary, self.root.join("HEAD"))
            .map_err(|_| unavailable("Cannot publish latest checkpoint"))?;
        measured_io::sync_directory(&self.directory)
            .map_err(|_| unavailable("Cannot durably publish latest checkpoint"))?;
        if let Some(cache) = &self.payload_cache {
            // This exact immutable payload was validated, signed, written and
            // read back above. Reuse its decoding, while latest_locked still
            // authenticates HEAD and checks the file, digest and durability.
            cache.remember(
                sha256,
                self.task_receipt_key()?.fingerprint()?,
                Arc::new(payload),
            )?;
        }
        let current = self.latest_locked()?;
        self.collect_superseded(&current)?;
        Ok(current)
    }

    fn validate_task_key(&self, payload: &Payload) -> Result<(), ApiError> {
        if let Some(fingerprint) = payload
            .task_control
            .as_ref()
            .and_then(|tasks| tasks.activation().key_fingerprint())
            && fingerprint != &self.task_receipt_key()?.fingerprint()?
        {
            return Err(unavailable(
                "Task control key does not match the independent repository",
            ));
        }
        Ok(())
    }

    fn write_new(&self, path: &Path, bytes: &[u8]) -> Result<(), ApiError> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options
            .open(path)
            .map_err(|_| unavailable("Cannot stage recovery evidence"))?;
        measured_io::write(&mut file, bytes)
            .and_then(|()| measured_io::sync_file(&file))
            .map_err(|_| unavailable("Cannot sync recovery evidence"))
    }

    fn lock(&self) -> Result<File, ApiError> {
        self.try_lock()?.ok_or_else(repository_busy)
    }

    fn try_lock(&self) -> Result<Option<File>, ApiError> {
        let started = Instant::now();
        let file = match checked_file(&self.root.join("LOCK"), true) {
            Ok(file) => file,
            Err(error) => {
                checkpoint_lock_finished(CheckpointLockOutcome::Failure, started.elapsed());
                return Err(error);
            }
        };
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: file owns this live descriptor. Closing it releases the
            // advisory lock, including when a publisher crashes.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock {
                    checkpoint_lock_finished(CheckpointLockOutcome::Contended, started.elapsed());
                    return Ok(None);
                }
                checkpoint_lock_finished(CheckpointLockOutcome::Failure, started.elapsed());
                return Err(unavailable(
                    "Cannot acquire control repository publication lock",
                ));
            }
        }
        checkpoint_lock_finished(CheckpointLockOutcome::Acquired, started.elapsed());
        Ok(Some(file))
    }
}

pub(super) fn repository_busy() -> ApiError {
    unavailable("Control repository is busy; retry without bypassing protection")
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use hubuum_storage_core::DeletionSubject;
    use rstest::rstest;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::thread;
    use std::time::{Duration, Instant};

    pub(super) struct Fixture {
        root: PathBuf,
        pub(super) repository: CheckpointRepository,
        pub(super) initial: DeletionControlCapture,
    }

    impl Fixture {
        pub(super) fn new() -> Self {
            let root = std::env::temp_dir().join(format!("hubuum-control-test-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let directory = root.join("repository");
            fs::create_dir(&directory).unwrap();
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
            let key = root.join("key");
            fs::write(&key, [42; 32]).unwrap();
            fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
            let repository = CheckpointRepository::open(&directory, &key).unwrap();
            let state = DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4()).unwrap();
            let initial = DeletionControlCapture::new(
                state,
                DeletionRecoveryPosition::memory(Uuid::new_v4()).unwrap(),
            );
            Self {
                root,
                repository,
                initial,
            }
        }

        pub(super) fn next(&self) -> DeletionControlCapture {
            let target = DeletionSubject::Version {
                identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
            };
            let change = self
                .initial
                .state()
                .record_intent(Uuid::new_v4(), target, vec![Uuid::new_v4()])
                .unwrap();
            DeletionControlCapture::new(change.next().clone(), self.initial.position().clone())
        }

        fn head(&self) -> PathBuf {
            self.repository.root.join("HEAD")
        }
    }

    #[test]
    fn recovery_evidence_prevents_concurrent_latest_publication() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let evidence = fixture.repository.recovery_evidence().unwrap();
        assert!(fixture.repository.publish(&fixture.initial, false).is_err());
        drop(evidence);
        assert!(fixture.repository.publish(&fixture.initial, false).is_ok());
    }

    #[test]
    fn cleanup_session_retains_exclusive_publication_through_successive_checkpoints() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let mut session =
            CheckpointRepository::open(&fixture.repository.root, &fixture.root.join("key"))
                .unwrap()
                .into_session()
                .unwrap();
        let next = fixture.next();
        session.publish(&next).unwrap();
        assert!(fixture.repository.recovery_evidence().is_err());
        let identity = session.checkpoint().identity().to_owned();
        drop(session);
        // A concurrent template-worker fork can briefly inherit the open file
        // description. CLOEXEC closes that copy at exec, so dropping our guard
        // promises eventual release rather than an immediate successful flock.
        // Keep the held-lock assertion above strict; retry only this transient
        // busy result after the owner has finished.
        let deadline = Instant::now() + Duration::from_secs(5);
        let checkpoint = loop {
            match fixture.repository.latest() {
                Err(ApiError::ServiceUnavailable(message))
                    if message
                        == "Control repository is busy; retry without bypassing protection"
                        && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                result => break result.unwrap(),
            }
        };
        assert_eq!(checkpoint.identity(), identity);
    }

    #[test]
    fn signed_v1_intents_authenticate_without_binding_fields() {
        let fixture = Fixture::new();
        // These bytes use the published v1 field order and omit every v2 field.
        // Signing them directly catches accidental normalization on decoding.
        let state = r#"{"format":1,"lineage":"c39555e3-e6dc-4b42-9c78-d5726dbdc200","epoch":"b40e2188-3758-4cf6-bf35-7369784d7a16","revision":1,"intents":{"6d209981-b5cc-4271-8532-019b06a26304":{"subject":{"kind":"version","identity":"1580e33a-ecf4-4269-8482-976c29fbac33"},"generation":1,"copies":{"a6e092ca-bb89-4664-9ff8-9e641bf70004":"removed"}}}}"#;
        let payload = format!(
            r#"{{"state":{state},"position":{{"backend":"memory","instance":"014c606d-91e9-4e06-b9a6-99e2fe626b52"}}}}"#
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&[42; 32]).unwrap();
        mac.update(MAC_DOMAIN);
        mac.update(payload.as_bytes());
        let bytes = format!(
            r#"{{"payload":{payload},"mac":"{}"}}"#,
            STANDARD.encode(mac.finalize().into_bytes())
        )
        .into_bytes();
        let sha256 = digest(&bytes);
        fixture
            .repository
            .write_new(
                &fixture.repository.root.join(format!("{sha256}.json")),
                &bytes,
            )
            .unwrap();
        let control = DeletionControlState::decode(state.as_bytes()).unwrap();
        let head = fixture
            .repository
            .signed(Head {
                format: 1,
                lineage: control.lineage(),
                epoch: control.epoch(),
                revision: control.revision(),
                sha256,
                task_revision: None,
                previous_sha256: None,
            })
            .unwrap();
        fixture
            .repository
            .write_new(&fixture.head(), &head)
            .unwrap();
        let recovered = fixture.repository.latest().unwrap();
        assert_eq!(recovered.state().encode().unwrap(), state.as_bytes());
    }

    #[test]
    #[ignore = "subprocess fixture for cleanup session crash test"]
    fn hold_cleanup_session_child() {
        let root = std::env::current_dir().unwrap();
        let _session = CheckpointRepository::open(&root.join("repository"), &root.join("key"))
            .unwrap()
            .into_session()
            .unwrap();
        fs::write(root.join("locked"), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn killed_cleanup_process_releases_its_session_without_losing_evidence() {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        let fixture = Fixture::new();
        let checkpoint = fixture.repository.publish(&fixture.initial, true).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "services::deletion_recovery::repository::tests::hold_cleanup_session_child",
                "--ignored",
            ])
            .current_dir(&fixture.root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !fixture.root.join("locked").exists() && Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let locked = fixture.root.join("locked").exists();
        let excluded = fixture.repository.recovery_evidence().is_err();
        let _ = child.kill();
        child.wait().unwrap();
        assert!(
            locked && excluded,
            "cleanup child must hold the publication lock"
        );
        assert_eq!(
            fixture.repository.latest().unwrap().identity(),
            checkpoint.identity()
        );
    }

    #[test]
    #[ignore = "subprocess fixture for recovery lock crash test"]
    fn hold_recovery_lock_child() {
        let root = std::env::current_dir().unwrap();
        let repository =
            CheckpointRepository::open(&root.join("repository"), &root.join("key")).unwrap();
        let _evidence = repository.recovery_evidence().unwrap();
        fs::write(root.join("locked"), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn killed_recovery_process_releases_publication_lock_without_changing_evidence() {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "services::deletion_recovery::repository::tests::hold_recovery_lock_child",
                "--ignored",
            ])
            .current_dir(&fixture.root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !fixture.root.join("locked").exists() && Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let locked = fixture.root.join("locked").exists();
        let was_excluded = fixture.repository.recovery_evidence().is_err();
        let _ = child.kill();
        child.wait().unwrap();
        assert!(
            locked && was_excluded,
            "child must hold the independent publication lock"
        );
        assert_eq!(
            fixture.repository.recovery_evidence().unwrap().state(),
            fixture.initial.state()
        );
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn publication_is_recoverable_by_another_repository_instance() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let next = fixture.next();
        fixture.repository.publish(&next, false).unwrap();
        let reopened =
            CheckpointRepository::open(&fixture.repository.root, &fixture.root.join("key"))
                .unwrap();
        assert_eq!(reopened.latest().unwrap().state(), next.state());
    }

    #[test]
    fn stale_publisher_cannot_replace_latest() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        fixture.repository.publish(&fixture.next(), false).unwrap();
        assert!(fixture.repository.publish(&fixture.initial, false).is_err());
    }

    #[test]
    fn same_revision_with_different_decisions_conflicts() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        fixture.repository.publish(&fixture.next(), false).unwrap();
        assert!(fixture.repository.publish(&fixture.next(), false).is_err());
    }

    #[test]
    fn retry_after_acknowledgement_loss_is_idempotent() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let next = fixture.next();
        fixture.repository.publish(&next, false).unwrap();
        assert_eq!(
            fixture.repository.publish(&next, false).unwrap().state(),
            next.state()
        );
    }

    #[test]
    fn missing_latest_never_means_empty_authority() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        fs::remove_file(fixture.head()).unwrap();
        assert!(fixture.repository.publish(&fixture.initial, false).is_err());
    }

    #[test]
    fn missing_latest_cannot_be_bypassed_by_bootstrap() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        fs::remove_file(fixture.head()).unwrap();
        assert!(fixture.repository.publish(&fixture.initial, true).is_err());
    }

    #[test]
    fn an_old_authenticated_snapshot_is_not_latest_evidence() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let old = fixture.repository.latest().unwrap();
        fixture.repository.publish(&fixture.next(), false).unwrap();
        assert!(
            !old.state()
                .covers(fixture.repository.latest().unwrap().state())
        );
    }

    #[rstest]
    #[case::missing(false)]
    #[case::corrupt(true)]
    fn unavailable_recovery_material_blocks_latest(#[case] corrupt: bool) {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let head: Head = fixture
            .repository
            .verified(&fs::read(fixture.head()).unwrap())
            .unwrap();
        let material = fixture
            .repository
            .root
            .join(format!("{}.json", head.sha256));
        if corrupt {
            fs::write(material, b"corrupt").unwrap();
        } else {
            fs::remove_file(material).unwrap();
        }
        assert!(fixture.repository.latest().is_err());
    }

    #[test]
    fn changed_latest_reference_fails_authentication() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(fixture.head()).unwrap()).unwrap();
        value["payload"]["revision"] = serde_json::json!(99);
        fs::write(fixture.head(), serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(fixture.repository.latest().is_err());
    }

    #[test]
    fn wrong_signing_key_cannot_authenticate_latest() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        fs::write(fixture.root.join("key"), [43; 32]).unwrap();
        let wrong_key =
            CheckpointRepository::open(&fixture.repository.root, &fixture.root.join("key"))
                .unwrap();
        assert!(wrong_key.latest().is_err());
    }

    #[test]
    fn symlink_latest_is_rejected() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let actual = fixture.root.join("actual-head");
        fs::rename(fixture.head(), &actual).unwrap();
        symlink(actual, fixture.head()).unwrap();
        assert!(fixture.repository.latest().is_err());
    }

    #[test]
    fn broad_file_permissions_are_rejected() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        fs::set_permissions(fixture.head(), fs::Permissions::from_mode(0o644)).unwrap();
        assert!(fixture.repository.latest().is_err());
    }

    #[test]
    fn concurrent_publisher_does_not_bypass_the_lock() {
        let fixture = Fixture::new();
        let _held = fixture.repository.lock().unwrap();
        assert!(fixture.repository.publish(&fixture.initial, true).is_err());
    }

    #[test]
    fn abandoned_stage_is_not_successful_publication() {
        let fixture = Fixture::new();
        fixture.repository.publish(&fixture.initial, true).unwrap();
        let next = fixture.next();
        let bytes = fixture
            .repository
            .signed(Payload {
                state: next.state().clone(),
                position: next.position().clone(),
                reapplication: None,
                task_control: None,
                task_reapplication: None,
            })
            .unwrap();
        let path = fixture
            .repository
            .root
            .join(format!("{}.json", digest(&bytes)));
        fixture.repository.write_new(&path, &bytes).unwrap();
        assert_eq!(
            fixture.repository.latest().unwrap().state(),
            fixture.initial.state()
        );
    }

    #[tokio::test]
    async fn failed_independent_publication_preserves_committed_intent() {
        use crate::storage::StorageHandle;
        use hubuum_storage_core::DeletionControlStorage;

        let fixture = Fixture::new();
        let storage = StorageHandle::memory();
        let initial = storage.capture_deletion_control().await.unwrap();
        fixture.repository.publish(&initial, true).unwrap();
        let subject = DeletionSubject::Version {
            identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
        };
        let change = initial
            .state()
            .record_intent(Uuid::new_v4(), subject, vec![Uuid::new_v4()])
            .unwrap();
        let expected = change.next().clone();
        storage.commit_deletion_control(change).await.unwrap();
        fs::remove_file(fixture.head()).unwrap();
        let failed = super::super::checkpoint(
            &storage,
            &fixture.repository.root,
            &fixture.root.join("key"),
            false,
        )
        .await;
        assert!(
            failed.is_err(),
            "missing latest reference must block publication"
        );
        assert_eq!(
            storage.capture_deletion_control().await.unwrap().state(),
            &expected
        );
    }
}
