use std::fs;
use std::path::Path;
use std::sync::Arc;

use hubuum_storage_core::TaskControlState;
use uuid::Uuid;

use super::{CheckpointRepository, DurableCheckpoint, Head, MAX_FILE_BYTES, Payload};
use super::{checked_file, digest, read_bounded, unavailable};
use crate::errors::ApiError;
use crate::observability::metrics::{CheckpointOperation, CheckpointTimer, checkpoint_gc_removed};

const MAX_ENTRIES: usize = 128;
const HEAD_ALLOWANCE_BYTES: u64 = 64 * 1024;
const MAX_REPOSITORY_BYTES: u64 = 3 * MAX_FILE_BYTES + HEAD_ALLOWANCE_BYTES;
const MAX_AUTHENTICATION_BYTES: u64 = 3 * MAX_FILE_BYTES;

impl CheckpointRepository {
    /// Call only while holding the publication lock. Unknown files consume
    /// capacity too: ignoring them would turn the repository bound into a bound
    /// on logical metadata rather than on retained physical files.
    pub(super) fn reserve_publication_space(&self, additional_bytes: u64) -> Result<(), ApiError> {
        let mut bytes = additional_bytes;
        for (index, entry) in fs::read_dir(&self.root)
            .map_err(|_| unavailable("Cannot inspect control repository capacity"))?
            .enumerate()
        {
            // Reserve two names for the candidate and temporary HEAD. Existing
            // candidate retries may need only one, but use the same safe bound.
            if index >= MAX_ENTRIES - 2 {
                return Err(unavailable("Control repository entry limit reached"));
            }
            let entry = entry.map_err(|_| unavailable("Cannot inspect repository entry"))?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|_| unavailable("Cannot inspect repository file size"))?;
            if !metadata.is_file() {
                return Err(unavailable(
                    "Control repository contains an unmanaged file type",
                ));
            }
            bytes = bytes
                .checked_add(metadata.len())
                .filter(|total| *total <= MAX_REPOSITORY_BYTES)
                .ok_or_else(|| unavailable("Control repository byte limit reached"))?;
        }
        if bytes > MAX_REPOSITORY_BYTES {
            return Err(unavailable("Control repository byte limit reached"));
        }
        Ok(())
    }

    /// Collect only authenticated, dominated evidence after durable HEAD
    /// read-back. The current checkpoint and its direct predecessor survive
    /// every pass, including interrupted or capacity-limited passes.
    pub(super) fn collect_superseded(&self, current: &DurableCheckpoint) -> Result<(), ApiError> {
        let timer = CheckpointTimer::start(CheckpointOperation::Gc);
        let mut paths = Vec::new();
        let mut overflow = false;
        for entry in fs::read_dir(&self.root)
            .map_err(|_| unavailable("Cannot inspect control repository retention"))?
        {
            if paths.len() == MAX_ENTRIES {
                overflow = true;
                break;
            }
            paths.push(
                entry
                    .map_err(|_| unavailable("Cannot inspect repository entry"))?
                    .path(),
            );
        }
        let mut remaining = MAX_AUTHENTICATION_BYTES;
        let mut removed = false;
        let result = (|| {
            // A temporary reference must be authenticated against its target
            // before collecting that target in the second pass.
            for path in &paths {
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if !is_temporary_head(name) {
                    continue;
                }
                let Some(bytes) = read_candidate(path, &mut remaining)? else {
                    continue;
                };
                let Ok(head) = self.verified::<Head>(&bytes) else {
                    continue;
                };
                if !is_digest(&head.sha256) {
                    continue;
                }
                let obsolete = if head.sha256 == current.sha256 {
                    head_matches(&head, &current.payload)
                } else {
                    self.retention_payload(&head.sha256, &mut remaining)?
                        .is_some_and(|payload| {
                            head_matches(&head, &payload) && dominates(&current.payload, &payload)
                        })
                };
                if obsolete {
                    remove_checkpoint(path)?;
                    removed = true;
                }
            }
            for path in &paths {
                let Some(identity) = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_suffix(".json"))
                    .filter(|identity| is_digest(identity))
                else {
                    continue;
                };
                if identity == current.sha256
                    || current.previous_sha256.as_deref() == Some(identity)
                {
                    continue;
                }
                if self
                    .retention_payload(identity, &mut remaining)?
                    .is_some_and(|payload| dominates(&current.payload, &payload))
                {
                    remove_checkpoint(path)?;
                    removed = true;
                }
            }
            if overflow {
                return Err(unavailable(
                    "Control repository retention scan limit reached",
                ));
            }
            Ok(())
        })();
        // Preserve progress even if a later entry exhausted this pass's budget.
        if removed {
            super::measured_io::sync_directory(&self.directory)
                .map_err(|_| unavailable("Cannot durably collect superseded checkpoints"))?;
        }
        timer.finish(result.is_ok());
        result
    }

    fn retention_payload(
        &self,
        identity: &str,
        remaining: &mut u64,
    ) -> Result<Option<Arc<Payload>>, ApiError> {
        let path = self.root.join(format!("{identity}.json"));
        let Some(bytes) = read_candidate(&path, remaining)? else {
            return Ok(None);
        };
        if digest(&bytes) != identity {
            return Ok(None);
        }
        if let Some(cache) = &self.payload_cache
            && let Some(payload) = cache.get(identity, &self.task_receipt_key()?.fingerprint()?)?
        {
            return Ok(Some(payload));
        }
        // Do not let unrelated directory candidates displace the recently
        // published decodings. Authentication and dominance are still required.
        Ok(self.verified(&bytes).ok().map(Arc::new))
    }
}

fn remove_checkpoint(path: &Path) -> Result<(), ApiError> {
    let bytes = fs::metadata(path)
        .map_err(|_| unavailable("Cannot inspect collected checkpoint"))?
        .len();
    fs::remove_file(path).map_err(|_| unavailable("Cannot collect superseded checkpoint"))?;
    checkpoint_gc_removed(bytes);
    Ok(())
}

fn read_candidate(path: &Path, remaining: &mut u64) -> Result<Option<Vec<u8>>, ApiError> {
    let Ok(file) = checked_file(path, false) else {
        return Ok(None);
    };
    let length = file
        .metadata()
        .map_err(|_| unavailable("Cannot inspect checkpoint candidate"))?
        .len();
    if length > MAX_FILE_BYTES {
        return Ok(None);
    }
    if length > *remaining {
        return Err(unavailable(
            "Control repository authentication budget reached",
        ));
    }
    let bytes = match read_bounded(file) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    *remaining = remaining
        .checked_sub(bytes.len() as u64)
        .ok_or_else(|| unavailable("Control repository authentication budget reached"))?;
    Ok(Some(bytes))
}

fn dominates(current: &Payload, old: &Payload) -> bool {
    current.state.covers(&old.state)
        && match (&current.task_control, &old.task_control) {
            (Some(current), Some(old)) => current.supersedes_for_recovery(old),
            (None, Some(_)) => false,
            (_, None) => true,
        }
}

fn head_matches(head: &Head, payload: &Payload) -> bool {
    head.format == if payload.task_control.is_some() { 2 } else { 1 }
        && head.lineage == payload.state.lineage()
        && head.epoch == payload.state.epoch()
        && head.revision == payload.state.revision()
        && head.task_revision
            == payload
                .task_control
                .as_ref()
                .map(TaskControlState::revision)
        && head
            .previous_sha256
            .as_ref()
            .is_none_or(|previous| is_digest(previous) && previous != &head.sha256)
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_temporary_head(value: &str) -> bool {
    value
        .strip_prefix("HEAD-")
        .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok_and(|uuid| uuid.to_string() == suffix))
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs::OpenOptions;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use hubuum_domain::{PrincipalIdentity, TaskIdentity};
    use hubuum_storage_core::{
        DeletionControlCapture, DeletionRecoveryPosition, DeletionSubject, ProtectedTaskKey,
        ReceiptExpiryBatch, StorageTaskKind, TaskControlRevision, TaskReceipt,
        TaskReceiptAdmission, TaskReceiptOutcome, TaskRequestFingerprint, TaskRetentionPolicy,
        TaskRetentionPolicyRevision, TaskSubmissionIdentity, TaskSubmissionLookup,
    };
    use rstest::rstest;

    use super::*;
    use crate::services::deletion_recovery::repository::AuthenticatedPayloadCache;
    use crate::services::deletion_recovery::repository::tests::Fixture;

    enum CandidateChange {
        None,
        Missing,
        Corrupt,
        Truncated,
        Symlink,
        PublicPermissions,
        Key,
    }

    #[rstest]
    #[case::unchanged(CandidateChange::None)]
    #[case::missing(CandidateChange::Missing)]
    #[case::corrupt(CandidateChange::Corrupt)]
    #[case::truncated(CandidateChange::Truncated)]
    #[case::symlink(CandidateChange::Symlink)]
    #[case::public_permissions(CandidateChange::PublicPermissions)]
    #[case::changed_key(CandidateChange::Key)]
    fn collection_reuses_decoding_only_after_fresh_file_and_key_checks(
        #[case] change: CandidateChange,
    ) {
        let fixture = Fixture::new();
        let key = fixture.repository.root.parent().unwrap().join("key");
        let cache = Arc::new(AuthenticatedPayloadCache::default());
        let repository = CheckpointRepository::open(&fixture.repository.root, &key)
            .unwrap()
            .with_payload_cache(Arc::clone(&cache));
        let checkpoint = repository.publish(&fixture.initial, true).unwrap();
        let path = repository
            .root
            .join(format!("{}.json", checkpoint.identity()));
        let unchanged = matches!(change, CandidateChange::None);
        match change {
            CandidateChange::None => {}
            CandidateChange::Missing => fs::remove_file(&path).unwrap(),
            CandidateChange::Corrupt => {
                let mut bytes = fs::read(&path).unwrap();
                bytes[0] ^= 1;
                fs::write(&path, bytes).unwrap();
            }
            CandidateChange::Truncated => {
                let bytes = fs::read(&path).unwrap();
                fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
            }
            CandidateChange::Symlink => {
                let original = repository.root.join("original");
                fs::rename(&path, &original).unwrap();
                symlink(original, &path).unwrap();
            }
            CandidateChange::PublicPermissions => {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            }
            CandidateChange::Key => fs::write(&key, [43; 32]).unwrap(),
        }
        let repository = CheckpointRepository::open(&fixture.repository.root, &key)
            .unwrap()
            .with_payload_cache(cache);
        let _lock = repository.lock().unwrap();
        let mut budget = MAX_AUTHENTICATION_BYTES;
        let candidate = repository
            .retention_payload(checkpoint.identity(), &mut budget)
            .unwrap();
        if unchanged {
            assert!(Arc::ptr_eq(&candidate.unwrap(), &checkpoint.payload));
            assert_eq!(
                budget,
                MAX_AUTHENTICATION_BYTES - fs::metadata(path).unwrap().len()
            );
        } else {
            assert!(candidate.is_none());
        }
    }

    fn next(capture: &DeletionControlCapture) -> DeletionControlCapture {
        let change = capture
            .state()
            .record_intent(
                Uuid::new_v4(),
                DeletionSubject::Version {
                    identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
                },
                vec![Uuid::new_v4()],
            )
            .unwrap();
        DeletionControlCapture::new(change.next().clone(), capture.position().clone())
    }

    fn payload(capture: &DeletionControlCapture) -> Payload {
        Payload {
            state: capture.state().clone(),
            position: capture.position().clone(),
            reapplication: None,
            task_control: None,
            task_reapplication: None,
        }
    }

    fn stage(repository: &CheckpointRepository, payload: &Payload) -> String {
        let bytes = repository.signed(payload).unwrap();
        let identity = digest(&bytes);
        repository
            .write_new(&repository.root.join(format!("{identity}.json")), &bytes)
            .unwrap();
        identity
    }

    fn temporary_head(
        repository: &CheckpointRepository,
        payload: &Payload,
        sha256: String,
    ) -> std::path::PathBuf {
        let head = Head {
            format: if payload.task_control.is_some() { 2 } else { 1 },
            lineage: payload.state.lineage(),
            epoch: payload.state.epoch(),
            revision: payload.state.revision(),
            sha256,
            task_revision: payload
                .task_control
                .as_ref()
                .map(TaskControlState::revision),
            previous_sha256: None,
        };
        let path = repository.root.join(format!("HEAD-{}", Uuid::new_v4()));
        repository
            .write_new(&path, &repository.signed(head).unwrap())
            .unwrap();
        path
    }

    #[test]
    fn publication_retains_current_and_previous_but_collects_older_checkpoint() {
        let fixture = Fixture::new();
        let first = fixture.repository.publish(&fixture.initial, true).unwrap();
        let second_capture = next(&fixture.initial);
        let second = fixture.repository.publish(&second_capture, false).unwrap();
        let third = fixture
            .repository
            .publish(&next(&second_capture), false)
            .unwrap();
        for checkpoint in [&second, &third] {
            assert!(
                fixture
                    .repository
                    .root
                    .join(format!("{}.json", checkpoint.identity()))
                    .exists()
            );
        }
        assert!(
            !fixture
                .repository
                .root
                .join(format!("{}.json", first.identity()))
                .exists()
        );
        assert_eq!(
            fixture.repository.latest().unwrap().identity(),
            third.identity()
        );
    }

    #[test]
    fn unauthenticated_partial_and_forward_candidates_survive_collection() {
        let fixture = Fixture::new();
        let current = fixture.repository.publish(&fixture.initial, true).unwrap();
        let future = stage(&fixture.repository, &payload(&next(&fixture.initial)));
        let mut untrusted: serde_json::Value = serde_json::from_slice(
            &fixture
                .repository
                .signed(payload(&fixture.initial))
                .unwrap(),
        )
        .unwrap();
        untrusted["mac"] = serde_json::json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        let untrusted = serde_json::to_vec(&untrusted).unwrap();
        let bad_digest = digest(&untrusted);
        let bad_path = fixture.repository.root.join(format!("{bad_digest}.json"));
        fixture.repository.write_new(&bad_path, &untrusted).unwrap();
        let partial_path = fixture
            .repository
            .root
            .join(format!("{}.json", "a".repeat(64)));
        fixture
            .repository
            .write_new(&partial_path, b"{\"payload\":")
            .unwrap();
        let unknown_path = fixture.repository.root.join("operator-notes");
        fixture
            .repository
            .write_new(&unknown_path, b"retain")
            .unwrap();
        let _lock = fixture.repository.lock().unwrap();
        fixture.repository.collect_superseded(&current).unwrap();
        for path in [
            bad_path,
            partial_path,
            unknown_path,
            fixture.repository.root.join(format!("{future}.json")),
        ] {
            assert!(path.exists());
        }
        assert_eq!(
            fixture.repository.latest_locked().unwrap().identity(),
            current.identity()
        );
    }

    #[test]
    fn only_authenticated_dominated_temporary_heads_are_collected() {
        let fixture = Fixture::new();
        let current = fixture.repository.publish(&fixture.initial, true).unwrap();
        let mut old = payload(&fixture.initial);
        old.position = DeletionRecoveryPosition::memory(Uuid::new_v4()).unwrap();
        let old_identity = stage(&fixture.repository, &old);
        let old_head = temporary_head(&fixture.repository, &old, old_identity.clone());
        let future = payload(&next(&fixture.initial));
        let future_identity = stage(&fixture.repository, &future);
        let future_head = temporary_head(&fixture.repository, &future, future_identity);
        let untrusted_head = fixture
            .repository
            .root
            .join(format!("HEAD-{}", Uuid::new_v4()));
        fixture
            .repository
            .write_new(&untrusted_head, b"partial")
            .unwrap();
        let _lock = fixture.repository.lock().unwrap();
        fixture.repository.collect_superseded(&current).unwrap();
        assert!(!old_head.exists());
        assert!(
            !fixture
                .repository
                .root
                .join(format!("{old_identity}.json"))
                .exists()
        );
        assert!(future_head.exists());
        assert!(untrusted_head.exists());
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn unknown_files_count_toward_publication_capacity(#[case] entry_limit: bool) {
        let fixture = Fixture::new();
        let current = fixture.repository.publish(&fixture.initial, true).unwrap();
        if entry_limit {
            for index in 0..MAX_ENTRIES {
                fixture
                    .repository
                    .write_new(
                        &fixture.repository.root.join(format!("unknown-{index}")),
                        b"",
                    )
                    .unwrap();
            }
        } else {
            let path = fixture.repository.root.join("unknown-large-file");
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .unwrap();
            file.set_len(MAX_REPOSITORY_BYTES).unwrap();
        }
        assert!(
            fixture
                .repository
                .reserve_publication_space(HEAD_ALLOWANCE_BYTES)
                .is_err()
        );
        assert_eq!(
            fixture.repository.latest().unwrap().identity(),
            current.identity()
        );
    }

    #[test]
    fn legacy_directory_cleanup_makes_bounded_durable_progress() {
        let fixture = Fixture::new();
        let current = fixture.repository.publish(&fixture.initial, true).unwrap();
        for _ in 0..MAX_ENTRIES {
            let mut old = payload(&fixture.initial);
            old.position = DeletionRecoveryPosition::memory(Uuid::new_v4()).unwrap();
            stage(&fixture.repository, &old);
        }
        let _lock = fixture.repository.lock().unwrap();
        assert!(fixture.repository.collect_superseded(&current).is_err());
        assert!(fs::read_dir(&fixture.repository.root).unwrap().count() < MAX_ENTRIES);
        fixture.repository.collect_superseded(&current).unwrap();
        assert_eq!(
            fixture.repository.latest_locked().unwrap().identity(),
            current.identity()
        );
    }

    #[test]
    fn expired_receipt_checkpoint_collects_old_nonterminal_evidence() {
        let fixture = Fixture::new();
        let mut admitted = payload(&fixture.initial);
        let policy = TaskRetentionPolicy::new(
            TaskRetentionPolicyRevision::new(0).unwrap(),
            Duration::from_secs(3600),
            Some(Duration::from_secs(60)),
        )
        .unwrap();
        let initial = TaskControlState::initial(
            admitted.state.lineage(),
            admitted.state.epoch(),
            policy.clone(),
        )
        .unwrap();
        let receipt = TaskReceipt::admit(
            TaskReceiptAdmission::new(
                TaskSubmissionLookup::new(
                    TaskSubmissionIdentity::new(
                        PrincipalIdentity::generate(),
                        ProtectedTaskKey::new("a".repeat(64)).unwrap(),
                    ),
                    StorageTaskKind::Import,
                    TaskRequestFingerprint::new("b".repeat(64)).unwrap(),
                ),
                TaskIdentity::generate(),
                Uuid::new_v4(),
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            )
            .unwrap(),
            &policy,
            TaskControlRevision::new(1).unwrap(),
        )
        .unwrap();
        let initial_payload = Payload {
            task_control: Some(initial.clone()),
            ..admitted.clone()
        };
        let _lock = fixture.repository.lock().unwrap();
        fixture
            .repository
            .publish_payload_locked(initial_payload, true)
            .unwrap();
        let admitted_state = initial.with_receipt(receipt.clone()).unwrap();
        admitted.task_control = Some(admitted_state.clone());
        let old = fixture
            .repository
            .publish_payload_locked(admitted.clone(), false)
            .unwrap();
        let terminal = receipt
            .settle(TaskReceiptOutcome::RecoveryUnknown {
                settled_at: receipt.admitted_at(),
            })
            .unwrap();
        let settled = admitted_state.with_receipt(terminal.clone()).unwrap();
        fixture
            .repository
            .publish_payload_locked(
                Payload {
                    task_control: Some(settled.clone()),
                    ..admitted.clone()
                },
                false,
            )
            .unwrap();
        let expired = settled
            .expire(
                &ReceiptExpiryBatch::new(
                    settled.revision(),
                    terminal.expires_at().unwrap(),
                    vec![terminal.reference().clone()],
                )
                .unwrap(),
            )
            .unwrap();
        assert!(!expired.covers(&admitted_state));
        let current = fixture
            .repository
            .publish_payload_locked(
                Payload {
                    task_control: Some(expired),
                    ..admitted
                },
                false,
            )
            .unwrap();
        assert!(
            !fixture
                .repository
                .root
                .join(format!("{}.json", old.identity()))
                .exists()
        );
        assert_eq!(
            fixture.repository.latest_locked().unwrap().identity(),
            current.identity()
        );
    }
}
