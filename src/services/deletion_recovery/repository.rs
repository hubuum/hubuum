use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use hubuum_storage_core::{
    DELETION_CONTROL_MAX_BYTES, DeletionControlCapture, DeletionControlState,
    DeletionRecoveryPosition,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::unavailable;
use crate::errors::ApiError;

const MAX_FILE_BYTES: u64 = (DELETION_CONTROL_MAX_BYTES + 65536) as u64;
const MAC_DOMAIN: &[u8] = b"hubuum.deletion-checkpoint.v1\0";

/// The directory, signing key and latest reference must remain outside every
/// supported rollback of the primary database. This is a single trusted-writer
/// repository; every writer uses the same advisory lock and signing key.
pub(super) struct CheckpointRepository {
    root: PathBuf,
    directory: File,
    key: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    state: DeletionControlState,
    position: DeletionRecoveryPosition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    format: u32,
    lineage: Uuid,
    epoch: Uuid,
    revision: i64,
    sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed<T> {
    payload: T,
    mac: String,
}

/// Only independent authenticated read-back can construct this proof.
pub(super) struct DurableCheckpoint {
    payload: Payload,
}

/// Keep latest-reference publication excluded until recovery commits and its
/// state is independently read back. Process death releases the OS lock.
pub(super) struct RecoveryEvidence {
    checkpoint: DurableCheckpoint,
    _lock: File,
}

impl RecoveryEvidence {
    pub(super) fn state(&self) -> &DeletionControlState {
        self.checkpoint.state()
    }
}

impl DurableCheckpoint {
    pub(super) fn state(&self) -> &DeletionControlState {
        &self.payload.state
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

fn read_bounded(mut file: File) -> Result<Vec<u8>, ApiError> {
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable("Cannot read recovery evidence"))?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(unavailable("Recovery evidence exceeds its byte limit"));
    }
    Ok(bytes)
}

impl CheckpointRepository {
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
        })
    }

    fn signed<T: Serialize>(&self, payload: T) -> Result<Vec<u8>, ApiError> {
        let bytes = serde_json::to_vec(&payload)
            .map_err(|_| unavailable("Cannot encode recovery evidence"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| unavailable("Invalid control key"))?;
        mac.update(MAC_DOMAIN);
        mac.update(&bytes);
        serde_json::to_vec(&Signed {
            payload,
            mac: STANDARD.encode(mac.finalize().into_bytes()),
        })
        .map_err(|_| unavailable("Cannot encode authenticated recovery evidence"))
    }

    fn verified<T: Serialize + DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, ApiError> {
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

    fn latest_locked(&self) -> Result<DurableCheckpoint, ApiError> {
        let head: Head = self.verified(&read_bounded(checked_file(
            &self.root.join("HEAD"),
            false,
        )?)?)?;
        if head.format != 1
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
        let payload: Payload = self.verified(&bytes)?;
        if payload.state.lineage() != head.lineage
            || payload.state.epoch() != head.epoch
            || payload.state.revision() != head.revision
        {
            return Err(unavailable(
                "Checkpoint identity differs from latest reference",
            ));
        }
        // A prior publisher may have lost acknowledgement after rename but
        // before directory synchronization. A visible HEAD alone is not proof
        // that its name survives power loss; finish the durability boundary on
        // read/retry before returning a durable proof.
        self.directory
            .sync_all()
            .map_err(|_| unavailable("Cannot establish latest checkpoint durability"))?;
        Ok(DurableCheckpoint { payload })
    }

    pub(super) fn publish(
        &self,
        capture: &DeletionControlCapture,
        initialize: bool,
    ) -> Result<DurableCheckpoint, ApiError> {
        let _lock = self.lock()?;
        if initialize {
            // Bootstrap is explicit and restricted to a fresh, empty authority
            // and repository. Missing HEAD is never interpreted as an empty ledger.
            if capture.state().revision() != 0 || !capture.state().intents().is_empty() {
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
        } else {
            let previous = self.latest_locked()?;
            if !capture.state().covers(previous.state()) {
                return Err(unavailable(
                    "Checkpoint would discard or roll back acknowledged deletion authority",
                ));
            }
            if capture.state().revision() == previous.state().revision() {
                if capture.state() != previous.state() {
                    return Err(unavailable("Conflicting checkpoint at the same revision"));
                }
                return Ok(previous);
            }
        }
        let bytes = self.signed(Payload {
            state: capture.state().clone(),
            position: capture.position().clone(),
        })?;
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(unavailable("Checkpoint exceeds its byte limit"));
        }
        let sha256 = digest(&bytes);
        let path = self.root.join(format!("{sha256}.json"));
        if path
            .try_exists()
            .map_err(|_| unavailable("Cannot inspect staged checkpoint"))?
        {
            if read_bounded(checked_file(&path, false)?)? != bytes {
                return Err(unavailable("Conflicting staged checkpoint"));
            }
            checked_file(&path, false)?
                .sync_all()
                .map_err(|_| unavailable("Cannot sync existing staged checkpoint"))?;
        } else {
            self.write_new(&path, &bytes)?;
        }
        self.directory
            .sync_all()
            .map_err(|_| unavailable("Cannot sync recovery material directory"))?;
        // Read back before the monotonic publication point.
        if read_bounded(checked_file(&path, false)?)? != bytes {
            return Err(unavailable("Recovery material read-back failed"));
        }
        let head = self.signed(Head {
            format: 1,
            lineage: capture.state().lineage(),
            epoch: capture.state().epoch(),
            revision: capture.state().revision(),
            sha256,
        })?;
        let temporary = self.root.join(format!("HEAD-{}", Uuid::new_v4()));
        self.write_new(&temporary, &head)?;
        fs::rename(&temporary, self.root.join("HEAD"))
            .map_err(|_| unavailable("Cannot publish latest checkpoint"))?;
        self.directory
            .sync_all()
            .map_err(|_| unavailable("Cannot durably publish latest checkpoint"))?;
        self.latest_locked()
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
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| unavailable("Cannot sync recovery evidence"))
    }

    fn lock(&self) -> Result<File, ApiError> {
        let file = checked_file(&self.root.join("LOCK"), true)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: file owns this live descriptor. Closing it releases the
            // advisory lock, including when a publisher crashes.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(unavailable(
                    "Control repository is busy; retry without bypassing protection",
                ));
            }
        }
        Ok(file)
    }
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

    struct Fixture {
        root: PathBuf,
        repository: CheckpointRepository,
        initial: DeletionControlCapture,
    }

    impl Fixture {
        fn new() -> Self {
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

        fn next(&self) -> DeletionControlCapture {
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
