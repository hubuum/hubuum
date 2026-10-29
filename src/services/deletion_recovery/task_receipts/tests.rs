use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use chrono::Utc;
use hubuum_domain::{PrincipalId, TaskId};
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    LogicalIdentityStorage, RuntimeIsolationDomain, StorageIdentitySubject, StorageIdentityTarget,
    StorageSchemaProfileArtifact, StorageTaskCompletion, StorageTaskCompletionPayload,
    StorageTaskCreateRequest, StorageTaskEventInput, StorageTaskLeaseDuration,
    StorageTaskResultCounts, StorageTaskSubmissionOutcome, StorageTaskTerminalStatus,
    StorageTaskTerminalUpdate, StorageUserCreate, TaskControlChange, TaskControlRecovery,
    TaskExecutionStorage, TaskHoldChange, TaskHoldId, TaskHoldReason, TaskPurgeScope,
    TaskReceiptOutcome, TaskRetentionHold, TaskRetentionPolicy, TaskRetentionPolicyChange,
    UserStorage,
};
use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::storage::StorageHandle;

mod legacy_restore;

struct Fixture {
    root: PathBuf,
    storage: StorageHandle,
    service: TaskReceiptService,
    actor: PrincipalId,
    owner: PrincipalIdentity,
}

impl Fixture {
    async fn new(details: Option<Duration>, budget: TaskPurgeBudget) -> Self {
        let storage = StorageHandle::memory();
        let actor = storage
            .create_user(StorageUserCreate::new(
                None,
                "retention-owner",
                "test-hash",
                None,
                None,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts()
            .id()
            .into();
        let evidence = storage
            .resolve_logical_identity(StorageIdentityTarget::Principal(actor))
            .await
            .unwrap();
        let Some(StorageIdentitySubject::Principal { identity }) = evidence.subject() else {
            panic!("fixture principal identity");
        };
        let owner = identity.known_identity().unwrap();
        let root = std::env::temp_dir().join(format!("hubuum-task-retention-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [71; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        super::super::checkpoint(&storage, &directory, &key, true)
            .await
            .unwrap();
        let options = serde_json::from_value(json!({"directory":directory,"key":key})).unwrap();
        let service = TaskReceiptService::configured(&options).await.unwrap();
        let mut state = storage.read_task_control().await.unwrap();
        let policy = TaskRetentionPolicy::new(
            state.policy().revision().next().unwrap(),
            Duration::from_secs(3600),
            details,
        )
        .unwrap()
        .with_cleanup_budget(budget);
        let next = state.with_policy(policy).unwrap();
        state = storage
            .commit_task_control(TaskControlChange::new(state, next).unwrap())
            .await
            .unwrap();
        for activation in [
            TaskReceiptActivation::Backfilling {
                key_fingerprint: service.key_fingerprint().unwrap(),
            },
            TaskReceiptActivation::Protected {
                key_fingerprint: service.key_fingerprint().unwrap(),
            },
        ] {
            let next = state.with_activation(activation).unwrap();
            state = storage
                .commit_task_control(TaskControlChange::new(state, next).unwrap())
                .await
                .unwrap();
        }
        Self {
            root,
            storage,
            service,
            actor,
            owner,
        }
    }

    async fn admit(&self, key: &str, kind: StorageTaskKind) -> TaskAdmissionTicket {
        let key = IdempotencyKey::new(key).unwrap();
        let binding = self
            .service
            .prepare_submission(self.owner, &key, kind, "fixture.submit", &json!({}), None)
            .unwrap()
            .unwrap();
        let request = StorageTaskCreateRequest::builder(kind, self.actor, json!({}), 1)
            .idempotency_key(Some(key))
            .receipt_binding(Some(binding))
            .try_build()
            .unwrap();
        match self.storage.create_task(request).await.unwrap() {
            StorageTaskSubmissionOutcome::PendingProtection(ticket) => ticket,
            other => panic!("expected pending admission: {other:?}"),
        }
    }

    async fn complete(
        &self,
        key: &str,
        payload: StorageTaskCompletionPayload,
    ) -> (TaskAdmissionTicket, TaskId) {
        let ticket = self.admit(key, payload.task_kind()).await;
        let TaskActivationOutcome::Activated(task) = self
            .service
            .protect_admission(&self.storage, ticket.clone())
            .await
            .unwrap()
        else {
            panic!("fixture activation winner");
        };
        let claim = self
            .storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.task().id(), task.id());
        self.storage
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    claim.lease().clone(),
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "Fixture task complete"),
                payload,
            ))
            .await
            .unwrap();
        (ticket, task.id())
    }

    fn checkpoint(&self) -> super::super::repository::DurableCheckpoint {
        CheckpointRepository::open(&self.root.join("repository"), &self.root.join("key"))
            .unwrap()
            .latest()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[actix_web::test]
async fn changed_signing_key_fails_before_selecting_a_fresh_namespace() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let wrong_key = fixture.root.join("wrong-key");
    fs::write(&wrong_key, [72; 32]).unwrap();
    fs::set_permissions(&wrong_key, fs::Permissions::from_mode(0o600)).unwrap();
    let options = serde_json::from_value(
        json!({"directory":fixture.root.join("repository"),"key":wrong_key}),
    )
    .unwrap();
    let wrong = TaskReceiptService::configured(&options).await.unwrap();
    let binding = wrong
        .prepare_submission(
            fixture.owner,
            &IdempotencyKey::new("new-key").unwrap(),
            StorageTaskKind::Import,
            "fixture.submit",
            &json!({}),
            None,
        )
        .unwrap()
        .unwrap();
    let before = fixture.storage.read_task_control().await.unwrap();
    assert!(matches!(
        wrong
            .resolve_submission(&fixture.storage, binding.lookup().clone())
            .await,
        Err(ApiError::ServiceUnavailable(_))
    ));
    assert_eq!(fixture.storage.read_task_control().await.unwrap(), before);
}

#[actix_web::test]
async fn abandoned_pending_admissions_activate_in_bounded_passes() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    for index in 0..=MAX_PENDING_ADMISSIONS_PER_PASS {
        fixture
            .admit(&format!("pending-{index}"), StorageTaskKind::Import)
            .await;
    }
    assert!(
        fixture
            .storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        fixture
            .service
            .sweep_task_retention(&fixture.storage)
            .await
            .unwrap(),
        TaskRetentionSweep::Protected { more: true, .. }
    ));
    assert_eq!(
        fixture
            .storage
            .list_pending_task_admissions(64)
            .await
            .unwrap()
            .len(),
        1
    );
    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    assert!(
        fixture
            .storage
            .list_pending_task_admissions(64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
            .await
            .unwrap()
            .is_some()
    );
}

#[actix_web::test]
async fn output_only_cleanup_is_checkpointed_when_detail_retention_is_disabled() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    assert_eq!(
        fixture
            .storage
            .read_task_retention_policy()
            .await
            .unwrap()
            .details_after_terminal(),
        None
    );
    let (_, task) = fixture
        .complete(
            "profile",
            StorageTaskCompletionPayload::SchemaProfiling(
                StorageSchemaProfileArtifact::new(
                    json!({"profile":"expired"}),
                    Utc::now() - chrono::Duration::seconds(1),
                )
                .unwrap(),
            ),
        )
        .await;
    assert!(matches!(
        fixture
            .service
            .sweep_task_retention(&fixture.storage)
            .await
            .unwrap(),
        TaskRetentionSweep::Protected {
            rows: 1,
            more: true,
            ..
        }
    ));
    assert!(fixture.storage.get_task_access(task).await.is_ok());
    let state = fixture.storage.read_task_control().await.unwrap();
    assert_eq!(state.purge_batches().len(), 1);
    let decision = state.purge_batches().values().next().unwrap();
    assert_eq!(
        decision.ticket().manifest().scope(),
        TaskPurgeScope::ExpiredOutputs
    );
    assert!(decision.removed().is_some());
    assert_eq!(fixture.checkpoint().task_control(), Some(&state));
}

#[actix_web::test]
async fn held_old_manifest_does_not_prevent_new_unheld_cleanup() {
    let fixture = Fixture::new(Some(Duration::ZERO), TaskPurgeBudget::default()).await;
    let (held, _) = fixture
        .complete("held", StorageTaskCompletionPayload::Import)
        .await;
    let old = fixture
        .storage
        .prepare_task_details_purge(
            TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture
            .storage
            .list_pending_task_purges(8)
            .await
            .unwrap()
            .len(),
        1
    );
    let hold = TaskRetentionHold::new(
        TaskHoldId::generate(),
        held.receipt().task(),
        Utc::now(),
        None,
        TaskHoldReason::Operator,
    )
    .unwrap();
    fixture
        .storage
        .change_task_hold(TaskHoldChange::new(
            fixture
                .storage
                .read_task_control()
                .await
                .unwrap()
                .revision(),
            hold,
        ))
        .await
        .unwrap();
    let (free, _) = fixture
        .complete("free", StorageTaskCompletionPayload::Import)
        .await;
    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    let state = fixture.storage.read_task_control().await.unwrap();
    assert!(
        state.purge_batches()[&old.manifest().id()]
            .removed()
            .is_none()
    );
    let free = state
        .purge_batches()
        .values()
        .find(|decision| {
            decision
                .ticket()
                .manifest()
                .tasks()
                .contains(&free.receipt().task())
        })
        .unwrap();
    assert!(free.removed().is_some());
    assert_eq!(
        fixture.storage.list_pending_task_purges(8).await.unwrap(),
        vec![old]
    );
    assert_eq!(fixture.checkpoint().task_control(), Some(&state));
}

#[actix_web::test]
async fn one_sweep_shares_its_physical_budget_across_all_manifests() {
    let budget = TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES).unwrap();
    let fixture = Fixture::new(Some(Duration::ZERO), budget).await;
    for key in ["first", "second"] {
        fixture
            .complete(key, StorageTaskCompletionPayload::Import)
            .await;
        fixture
            .storage
            .prepare_task_details_purge(TaskPurgeSelection::new(64, budget).unwrap())
            .await
            .unwrap()
            .unwrap();
    }
    let TaskRetentionSweep::Protected { rows, bytes, more } = fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap()
    else {
        panic!("protected sweep");
    };
    assert_eq!(rows, 1);
    assert!(bytes <= budget.max_bytes());
    assert!(more);
}

#[actix_web::test]
async fn interrupted_expiry_acknowledgement_is_repaired_before_key_reuse() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let state = fixture.storage.read_task_control().await.unwrap();
    fixture
        .storage
        .replace_task_retention_policy(TaskRetentionPolicyChange::new(
            state.revision(),
            TaskRetentionPolicy::new(
                state.policy().revision().next().unwrap(),
                Duration::from_secs(1),
                None,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let (original, _) = fixture
        .complete("reused-key", StorageTaskCompletionPayload::Import)
        .await;
    let capture = fixture.storage.capture_control_checkpoint().await.unwrap();
    let terminal = capture
        .task_control()
        .receipt(original.receipt().submission())
        .unwrap();
    assert!(matches!(
        terminal.outcome(),
        TaskReceiptOutcome::Terminal { .. }
    ));
    let expiry = terminal.expires_at().unwrap();
    let session = open_session(fixture.service.configuration().unwrap())
        .await
        .unwrap();
    let session = publish_current(&fixture.storage, session).await.unwrap();
    let wait = (expiry - Utc::now()).to_std().unwrap_or_default() + Duration::from_millis(20);
    tokio::time::sleep(wait).await;
    let batch = fixture
        .storage
        .select_expired_task_receipts(ReceiptExpirySelection::new(64).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.receipts(), &[original.receipt().clone()]);
    let revision = fixture
        .storage
        .expire_task_receipts(
            ProtectedReceiptExpiry::new(batch, checkpoint_identity(&session).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    drop(session); // The database committed; the completion checkpoint did not.
    assert!(
        fixture
            .checkpoint()
            .task_control()
            .unwrap()
            .receipt(original.receipt().submission())
            .is_some()
    );
    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    let acknowledged = fixture.checkpoint();
    let acknowledged = acknowledged.task_control().unwrap();
    assert!(acknowledged.revision() >= revision);
    assert!(
        acknowledged
            .receipt(original.receipt().submission())
            .is_none()
    );
    assert!(acknowledged.receipt_expiry_floor() >= expiry);
    let replacement = fixture.admit("reused-key", StorageTaskKind::Import).await;
    assert_ne!(replacement.receipt().task(), original.receipt().task());
    assert!(matches!(
        fixture
            .service
            .protect_admission(&fixture.storage, replacement.clone())
            .await
            .unwrap(),
        TaskActivationOutcome::Activated(_)
    ));
    assert_eq!(
        fixture
            .checkpoint()
            .task_control()
            .unwrap()
            .receipt(replacement.receipt().submission())
            .unwrap()
            .ticket(),
        &replacement
    );
}

#[actix_web::test]
async fn recovery_of_older_admission_preserves_the_published_terminal_anchor() {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let (ticket, _) = fixture
        .complete("settled", StorageTaskCompletionPayload::Import)
        .await;
    // Initial activation checkpoint still records MayExecute. The publisher
    // must independently preserve the newer terminal anchor before recovery.
    let older = fixture.checkpoint().task_control().unwrap().clone();
    assert!(matches!(
        older
            .receipt(ticket.receipt().submission())
            .unwrap()
            .outcome(),
        TaskReceiptOutcome::MayExecute
    ));
    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    let terminal = fixture
        .storage
        .capture_control_checkpoint()
        .await
        .unwrap()
        .task_control()
        .clone();
    assert_eq!(fixture.checkpoint().task_control(), Some(&terminal));
    let authority = fixture.storage.read_deletion_control().await.unwrap();
    fixture
        .storage
        .recover_application_control(
            authority.recover_from(&authority).unwrap(),
            RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap(),
        )
        .await
        .unwrap();
    let revision = fixture
        .storage
        .recover_task_control(TaskControlRecovery::new(terminal.clone(), older).unwrap())
        .await
        .unwrap();
    assert_eq!(revision, terminal.revision());
    assert_eq!(fixture.storage.read_task_control().await.unwrap(), terminal);
    assert!(matches!(
        terminal
            .receipt(ticket.receipt().submission())
            .unwrap()
            .outcome(),
        TaskReceiptOutcome::Terminal { .. }
    ));
}
