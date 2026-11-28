use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use chrono::Utc;
use hubuum_domain::{PrincipalIdentity, TaskId};
use hubuum_storage_core::StorageTaskStatus;
use hubuum_task_core::IdempotencyKey;
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::config::ControlRecoveryOptions;
use crate::storage::StorageHandle;

struct Fixture {
    root: PathBuf,
    service: TaskReceiptService,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("hubuum-task-fingerprint-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [47; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        crate::services::deletion_recovery::checkpoint(
            &StorageHandle::memory(),
            &directory,
            &key,
            true,
        )
        .await
        .unwrap();
        let options: ControlRecoveryOptions =
            serde_json::from_value(json!({"directory":directory,"key":key})).unwrap();
        let service = TaskReceiptService::configured(&options).await.unwrap();
        Self { root, service }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn legacy_task(kind: TaskKind, hash: String, payload: Option<Value>) -> StorageTask {
    let at = Utc::now();
    StorageTask::builder(
        TaskId::new(1).unwrap(),
        kind,
        StorageTaskStatus::Succeeded,
        at,
        at,
    )
    .finished_at(Some(at))
    .request_hash(Some(hash))
    .request_payload(payload)
    .try_build()
    .unwrap()
}

#[rstest]
#[case::import(TaskKind::Import, TaskSubmissionOperation::Import, json!({"version":2,"dry_run":true,"graph":{}}))]
#[case::backup(TaskKind::Backup, TaskSubmissionOperation::Backup, json!({"include_history":true}))]
#[case::direct_export(TaskKind::Export, TaskSubmissionOperation::Export, json!({"export":{"scope":{"kind":"classes"}},"template_id":null}))]
#[case::profile(TaskKind::SchemaProfiling, TaskSubmissionOperation::SchemaProfile, json!({"class_id":42,"options":{"sample_limit":100,"preset":"balanced"}}))]
#[actix_web::test]
async fn reproducible_legacy_fingerprints_match_without_retained_payload(
    #[case] kind: TaskKind,
    #[case] operation: TaskSubmissionOperation,
    #[case] original_legacy_input: Value,
) {
    let fixture = Fixture::new().await;
    let hash = request_hash(&original_legacy_input).unwrap();
    let old = legacy_task(kind, hash.clone(), None);
    let fingerprint = legacy_task_fingerprint(&fixture.service, &old).unwrap();
    let principal = PrincipalIdentity::generate();
    let key = IdempotencyKey::new("same-submission").unwrap();
    let matching = fixture
        .service
        .prepare_submission(
            principal,
            &key,
            kind,
            operation.as_str(),
            &original_legacy_input,
            Some(&hash),
        )
        .unwrap()
        .unwrap();
    assert!(matching.lookup().matches(kind, &fingerprint));
    let different_hash = request_hash(&json!({"changed":true})).unwrap();
    let changed = fixture
        .service
        .prepare_submission(
            principal,
            &key,
            kind,
            operation.as_str(),
            &original_legacy_input,
            Some(&different_hash),
        )
        .unwrap()
        .unwrap();
    assert!(!changed.lookup().matches(kind, &fingerprint));
}

#[actix_web::test]
async fn intact_remote_payload_backfills_the_original_request_without_target_lookup() {
    let fixture = Fixture::new().await;
    let payload = json!({"target_id":51,"target_revision":9,"subject":{"type":"collection","collection_id":71},"parameters":{"secret":"not-retained"},"body_override":{}});
    let original: RemoteTargetInvokeRequest = serde_json::from_value(json!({"subject":{"type":"collection","collection_id":71},"parameters":{"secret":"not-retained"},"body_override":{}})).unwrap();
    let target = hubuum_domain::RemoteTargetId::new(51).unwrap();
    let task = legacy_task(
        TaskKind::RemoteCall,
        request_hash(&payload).unwrap(),
        Some(payload),
    );
    let expected = fixture
        .service
        .fingerprint(
            TaskKind::RemoteCall,
            TaskSubmissionOperation::RemoteInvocation.as_str(),
            &(target, original),
        )
        .unwrap();
    assert_eq!(
        legacy_task_fingerprint(&fixture.service, &task).unwrap(),
        TaskFingerprint::CanonicalV1(expected)
    );
}

#[rstest]
#[case::redacted(None, false)]
#[case::ignored_field(Some("unknown_field"), false)]
#[case::new_default(Some("target_revision"), true)]
#[case::digest_mismatch(None, true)]
#[actix_web::test]
async fn unsupported_remote_evidence_never_claims_original_request_correspondence(
    #[case] field: Option<&str>,
    #[case] remove: bool,
) {
    let fixture = Fixture::new().await;
    let mut payload = json!({"target_id":51,"target_revision":9,"subject":{"type":"collection","collection_id":71},"parameters":{},"body_override":{}});
    if let Some(field) = field {
        if remove {
            payload.as_object_mut().unwrap().remove(field);
        } else {
            payload[field] = json!(true);
        }
    }
    let hash = if field.is_none() && remove {
        "f".repeat(64)
    } else {
        request_hash(&payload).unwrap()
    };
    let payload = if field.is_none() && !remove {
        None
    } else {
        Some(payload)
    };
    let task = legacy_task(TaskKind::RemoteCall, hash, payload);
    assert_eq!(
        legacy_task_fingerprint(&fixture.service, &task).unwrap(),
        TaskFingerprint::LegacyUnverifiable
    );
}

#[actix_web::test]
async fn expanded_template_payload_cannot_prove_the_original_run_options() {
    let fixture = Fixture::new().await;
    let payload =
        json!({"export":{"scope":{"kind":"classes"},"query":"name=example"},"template_id":4});
    let task = legacy_task(
        TaskKind::Export,
        request_hash(&payload).unwrap(),
        Some(payload),
    );
    assert_eq!(
        legacy_task_fingerprint(&fixture.service, &task).unwrap(),
        TaskFingerprint::LegacyUnverifiable
    );
}
