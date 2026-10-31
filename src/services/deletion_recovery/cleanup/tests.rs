use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use hubuum_domain::{ClassId, CollectionId, ObjectId, ResourceId, VersionIdentity};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use hubuum_storage_core::{
    DeletionCopyState, DeletionSubject, HistoryStorage, LogicalIdentityStorage, StorageClassCreate,
    StorageClassSelector, StorageHistoryCollectionScope, StorageIdentityResourceKind,
    StorageIdentityTarget, StorageObjectCreate, StorageObjectHistoryListQuery,
    StorageObjectSelector, StorageObjectUpdate,
};
use rstest::rstest;

use super::*;

struct Fixture {
    root: PathBuf,
    directory: PathBuf,
    key: PathBuf,
    storage: StorageHandle,
    class_id: ClassId,
    object_id: ObjectId,
    target: StorageRecordIdentity,
    intent: Uuid,
}

impl Fixture {
    async fn new(initialize: bool) -> Self {
        let root = std::env::temp_dir().join(format!("hubuum-cleanup-test-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [43; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let storage = StorageHandle::memory();
        if initialize {
            super::super::checkpoint(&storage, &directory, &key, true)
                .await
                .unwrap();
        }
        let class = storage
            .class_store()
            .create_class(
                StorageClassCreate::builder("cleanup", CollectionId::new(1).unwrap(), "").build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let class = storage
            .class_store()
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .unwrap();
        let object = storage
            .object_store()
            .create_object(
                &class,
                StorageObjectCreate::new(
                    "retained",
                    class.class().collection_id(),
                    class.class().id(),
                    serde_json::json!({"private": "old version"}),
                    "",
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let target = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Object,
                id: ResourceId::new(object.id().id()).unwrap(),
            })
            .await
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap();
        let resolved = storage
            .object_store()
            .resolve_object(StorageObjectSelector::Ids {
                class_id: object.class_id(),
                object_id: object.id(),
            })
            .await
            .unwrap();
        storage
            .object_store()
            .update_object(
                &resolved,
                StorageObjectUpdate::builder()
                    .data(Some(serde_json::json!({"private": "current version"})))
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        Self {
            root,
            directory,
            key,
            storage,
            class_id: object.class_id(),
            object_id: object.id(),
            target,
            intent: Uuid::new_v4(),
        }
    }

    fn repository(&self) -> CheckpointRepository {
        CheckpointRepository::open(&self.directory, &self.key).unwrap()
    }

    async fn contains_copy(&self) -> bool {
        self.storage
            .list_object_history(StorageObjectHistoryListQuery::new(
                self.object_id,
                self.class_id,
                QueryOptions::new(vec![], vec![], Some(10), None, false).unwrap(),
                StorageHistoryCollectionScope::All,
            ))
            .await
            .unwrap()
            .into_parts()
            .0
            .into_iter()
            .any(|row| {
                row.into_parts()
                    .1
                    .logical_identity()
                    .and_then(|identity| identity.subject())
                    .and_then(|subject| subject.record_identity())
                    == Some(self.target)
            })
    }

    async fn run(&self) -> Result<HotHistoryCleanupReport, ApiError> {
        cleanup_object_history(
            &self.storage,
            &self.directory,
            &self.key,
            self.intent,
            self.target,
        )
        .await
    }

    async fn prepare(&self) -> (HotObjectHistoryDeletion, DeletionControlState) {
        let request =
            HotObjectHistoryDeletion::new(self.intent, Uuid::new_v4(), self.target).unwrap();
        let expected = self.storage.read_deletion_control().await.unwrap();
        let prepared = self
            .storage
            .prepare_hot_object_history_deletion(expected, request.clone())
            .await
            .unwrap();
        (request, prepared)
    }

    async fn protect(&self, request: HotObjectHistoryDeletion) -> ProtectedHotCopy {
        let session = self.repository().into_session().unwrap();
        let capture = self.storage.capture_deletion_control().await.unwrap();
        let expected = capture.state().clone();
        let session = publish(session, capture).await.unwrap();
        ProtectedHotCopy::new(session, expected, request).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[tokio::test]
async fn completion_report_requires_independent_removal_evidence() {
    let fixture = Fixture::new(true).await;
    let report = fixture.run().await.unwrap();
    let checkpoint = fixture.repository().latest().unwrap();
    let intent = &checkpoint.state().intents()[&report.intent];
    assert!(report.removed_from_hot_object_history);
    assert!(!fixture.contains_copy().await);
    assert_eq!(intent.copies()[&report.copy], DeletionCopyState::Removed);
    assert_eq!(checkpoint.identity(), report.completion_checkpoint);
    assert!(!report.application_quarantine_released);
}

#[tokio::test]
async fn retry_after_lost_acknowledgement_reuses_scope_and_checkpoint() {
    let fixture = Fixture::new(true).await;
    let first = fixture.run().await.unwrap();
    let retried = fixture.run().await.unwrap();
    assert_eq!(
        (
            retried.intent,
            retried.copy,
            retried.control_revision,
            retried.completion_checkpoint
        ),
        (
            first.intent,
            first.copy,
            first.control_revision,
            first.completion_checkpoint
        )
    );
}

enum InvalidEvidence {
    Uninitialized,
    MissingHead,
    MissingMaterial,
    InvalidAuthentication,
    WrongLineage,
    PrimaryBehindCheckpoint,
}

#[rstest]
#[case::uninitialized(InvalidEvidence::Uninitialized)]
#[case::missing_latest(InvalidEvidence::MissingHead)]
#[case::missing_material(InvalidEvidence::MissingMaterial)]
#[case::authentication(InvalidEvidence::InvalidAuthentication)]
#[case::wrong_lineage(InvalidEvidence::WrongLineage)]
#[case::rolled_back_primary(InvalidEvidence::PrimaryBehindCheckpoint)]
#[tokio::test]
async fn invalid_independent_evidence_cannot_commit_an_intent_or_remove_a_copy(
    #[case] invalid: InvalidEvidence,
) {
    let fixture = Fixture::new(!matches!(
        invalid,
        InvalidEvidence::Uninitialized | InvalidEvidence::WrongLineage
    ))
    .await;
    match invalid {
        InvalidEvidence::Uninitialized => (),
        InvalidEvidence::MissingHead => fs::remove_file(fixture.directory.join("HEAD")).unwrap(),
        InvalidEvidence::MissingMaterial => {
            let checkpoint = fixture.repository().latest().unwrap();
            fs::remove_file(
                fixture
                    .directory
                    .join(format!("{}.json", checkpoint.identity())),
            )
            .unwrap();
        }
        InvalidEvidence::InvalidAuthentication => fs::write(&fixture.key, [19; 32]).unwrap(),
        InvalidEvidence::WrongLineage => {
            super::super::checkpoint(
                &StorageHandle::memory(),
                &fixture.directory,
                &fixture.key,
                true,
            )
            .await
            .unwrap();
        }
        InvalidEvidence::PrimaryBehindCheckpoint => {
            let capture = fixture.storage.capture_deletion_control().await.unwrap();
            let newer = capture
                .state()
                .record_intent(
                    Uuid::new_v4(),
                    DeletionSubject::Version {
                        identity: VersionIdentity::generate(),
                    },
                    vec![Uuid::new_v4()],
                )
                .unwrap();
            fixture
                .repository()
                .publish(
                    &DeletionControlCapture::new(newer.next().clone(), capture.position().clone()),
                    false,
                )
                .unwrap();
        }
    }
    assert!(fixture.run().await.is_err());
    assert!(fixture.contains_copy().await);
    assert!(
        fixture
            .storage
            .read_deletion_control()
            .await
            .unwrap()
            .intents()
            .is_empty()
    );
}

#[tokio::test]
async fn registered_runtime_retains_protected_intent_until_drained() {
    let fixture = Fixture::new(true).await;
    let registration = fixture.storage.register_runtime().await.unwrap();
    assert!(fixture.run().await.is_err());
    assert!(fixture.contains_copy().await);
    let authority = fixture.storage.read_deletion_control().await.unwrap();
    assert_eq!(authority.intents().len(), 1);
    let intent = &authority.intents()[&fixture.intent];
    assert_eq!(
        intent.subject(),
        &DeletionSubject::Version {
            identity: fixture.target.version()
        }
    );
    assert_eq!(intent.copies().len(), 1);
    let (copy, binding) = intent.bindings().first_key_value().unwrap();
    assert_eq!(binding.target(), fixture.target);
    assert_eq!(intent.copies()[copy], DeletionCopyState::Pending);
    assert_eq!(fixture.repository().latest().unwrap().state(), &authority);
    assert!(
        !fixture
            .storage
            .read_application_control()
            .await
            .unwrap()
            .is_released_for(&authority)
    );
    fixture
        .storage
        .unregister_runtime(registration)
        .await
        .unwrap();
}

#[tokio::test]
async fn unprotected_intent_cannot_construct_cleanup_proof() {
    let fixture = Fixture::new(true).await;
    let (request, prepared) = fixture.prepare().await;
    let session = fixture.repository().into_session().unwrap();
    assert!(ProtectedHotCopy::new(session, prepared, request).is_err());
    assert!(fixture.contains_copy().await);
}

enum InterruptedPhase {
    IntentCommitted,
    IntentProtected,
    CopyRemoved,
}

#[rstest]
#[case::intent_committed(InterruptedPhase::IntentCommitted)]
#[case::intent_protected(InterruptedPhase::IntentProtected)]
#[case::copy_removed(InterruptedPhase::CopyRemoved)]
#[tokio::test]
async fn retry_reconciles_interrupted_cleanup_without_new_authority(
    #[case] phase: InterruptedPhase,
) {
    let fixture = Fixture::new(true).await;
    let (request, prepared) = fixture.prepare().await;
    match phase {
        InterruptedPhase::IntentCommitted => (),
        InterruptedPhase::IntentProtected => drop(fixture.protect(request.clone()).await),
        InterruptedPhase::CopyRemoved => {
            let (session, _) = fixture
                .protect(request.clone())
                .await
                .remove(&fixture.storage)
                .await
                .unwrap();
            drop(session);
        }
    }
    let report = fixture.run().await.unwrap();
    let state = fixture.storage.read_deletion_control().await.unwrap();
    assert_eq!(report.copy, request.copy());
    assert_eq!(
        state.intents()[&fixture.intent].generation(),
        prepared.intents()[&fixture.intent].generation()
    );
    assert!(!fixture.contains_copy().await);
    assert!(
        fixture
            .repository()
            .latest()
            .unwrap()
            .state()
            .covers(&state)
    );
}

#[tokio::test]
async fn failed_completion_publication_returns_pending_then_retry_recovers() {
    let fixture = Fixture::new(true).await;
    let (request, _) = fixture.prepare().await;
    let protected = fixture.protect(request.clone()).await;
    let (session, removed) = protected.remove(&fixture.storage).await.unwrap();
    let head = fixture.directory.join("HEAD");
    let previous_head = fs::read(&head).unwrap();
    // Corrupt independent evidence after physical removal. No completion may be
    // acknowledged merely because the primary already records Removed.
    fs::write(&head, b"corrupt latest evidence").unwrap();
    let result = acknowledge_completion(&fixture.storage, session, removed, request.clone()).await;
    assert!(
        matches!(result, Err(ApiError::ServiceUnavailable(message)) if message.contains("completion acknowledgement is pending"))
    );
    assert!(!fixture.contains_copy().await);
    fs::write(&head, previous_head).unwrap();
    let report = fixture.run().await.unwrap();
    assert_eq!(report.copy, request.copy());
    assert_eq!(
        report.completion_checkpoint,
        fixture.repository().latest().unwrap().identity()
    );
}

#[tokio::test]
async fn protected_reapplication_reopens_only_the_verified_application_image() {
    let fixture = Fixture::new(true).await;
    fixture.run().await.unwrap();
    let report = super::super::reapply(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert!(report.application_quarantine_released);
    let application = fixture.storage.read_application_control().await.unwrap();
    let checkpoint = fixture.repository().latest().unwrap();
    assert_eq!(checkpoint.reapplication(), application.reapplication());
    assert_eq!(
        application.checkpoint().unwrap().as_str(),
        checkpoint.identity()
    );
}

enum StatusPhase {
    CleanupRecorded,
    Reapplied,
    Recovered,
}

#[tokio::test]
async fn pending_deletion_status_matches_shared_consumer_fixture() {
    let fixture = Fixture::new(true).await;
    fixture.prepare().await;
    let report = super::super::status(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let mut actual = serde_json::to_value(report).unwrap();
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/lifecycle/control_status.json"
    ))
    .unwrap();
    assert_eq!(actual["intents"][0]["intent"], fixture.intent.to_string());
    actual["intents"][0]["intent"] = expected["intents"][0]["intent"].clone();
    assert_eq!(actual, expected);
}

#[rstest]
#[case::before_reapplication(StatusPhase::CleanupRecorded, false)]
#[case::after_reapplication(StatusPhase::Reapplied, true)]
#[case::after_physical_recovery(StatusPhase::Recovered, false)]
#[tokio::test]
async fn completion_status_requires_independently_protected_current_application(
    #[case] phase: StatusPhase,
    #[case] confirmed: bool,
) {
    let fixture = Fixture::new(true).await;
    fixture.run().await.unwrap();
    if matches!(phase, StatusPhase::Reapplied | StatusPhase::Recovered) {
        super::super::reapply(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .unwrap();
    }
    if matches!(phase, StatusPhase::Recovered) {
        super::super::recover(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .unwrap();
    }
    let report = super::super::status(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let value = serde_json::to_value(report).unwrap();
    assert_eq!(
        value["intents"][0]["confirmed_managed_scope_completion"],
        confirmed
    );
}

#[tokio::test]
async fn deletion_status_excludes_subjects_copy_locations_and_erased_payloads() {
    let fixture = Fixture::new(true).await;
    fixture.run().await.unwrap();
    let report = super::super::status(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let encoded = serde_json::to_string(&report).unwrap();
    for private in [
        fixture.target.resource().to_string(),
        fixture.target.version().to_string(),
        fixture.key.display().to_string(),
        fixture.directory.display().to_string(),
        "old version".to_owned(),
    ] {
        assert!(
            !encoded.contains(&private),
            "Status disclosed private receipt material"
        );
    }
}

#[tokio::test]
async fn physical_recovery_requires_fresh_reapplication_even_at_the_same_authority_revision() {
    let fixture = Fixture::new(true).await;
    fixture.run().await.unwrap();
    super::super::reapply(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let previous = fixture.storage.read_application_control().await.unwrap();
    super::super::recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let recovered = fixture.storage.read_application_control().await.unwrap();
    assert_ne!(
        previous.fence().incarnation(),
        recovered.fence().incarnation()
    );
    assert!(!recovered.is_released_for(&fixture.storage.read_deletion_control().await.unwrap()));
    let report = super::super::reapply(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert!(report.application_quarantine_released);
    assert!(report.control_revision > previous.fence().revision());
}
