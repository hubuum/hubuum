use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, Utc};
use hubuum_storage_core::{
    HistoryExpiryExactDeletion, HistoryLifecycleAuthority, HistoryLifecycleStorage,
    HistoryPolicyApplication, HistoryPolicyBinding, HistoryPolicyKey, HistoryPolicyRevision,
    HistoryPolicyScope, StorageIdentityHistoryKind,
};
use uuid::Uuid;

use super::*;
use crate::permissions::LocalPermissionBackend;
use crate::storage::StorageHandle;

#[rstest::rstest]
#[case::manual_first(true)]
#[case::periodic_first(false)]
#[actix_web::test]
async fn both_admission_services_share_the_original_initiation(#[case] manual_first: bool) {
    use hubuum_storage_core::{
        DeletionControlStorage, ExecutionStorage, HistoryExpiryOrigin, HistoryExpiryTaskPayload,
        HistoryExpiryTrigger, HistoryRetentionSeconds, StorageExecutionScope,
    };
    let mut fixture = Fixture::new().await;
    let key = fixture.proposal().key();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::Age {
                age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                deleted_age_seconds: None,
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    apply(
        &fixture.context,
        &fixture.admin,
        &fixture.options,
        HistoryPolicyApplyRequest::new(proposal, None),
    )
    .await
    .unwrap();
    let unbound = storage_handle(&fixture.context);
    let registration = unbound.register_history_runtime(None).await.unwrap();
    let storage = unbound.with_runtime_owner(registration.owner());
    fixture.context = AppContext::new(
        storage.clone(),
        Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into())),
    )
    .with_history_retention(fixture.options.clone());
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    storage
        .run_in_scope(scope, async {
            let scan = HistoryLifecycleScan::new(key, 1000).unwrap();
            if !manual_first {
                super::super::history_expiry::submit_periodic(&fixture.context)
                    .await
                    .unwrap();
            }
            let original = super::super::history_expiry::submit_manual(
                &fixture.context,
                &fixture.admin,
                scan.clone(),
            )
            .await
            .unwrap();
            assert_eq!(original.created(), manual_first);
            let payload =
                HistoryExpiryTaskPayload::decode(original.task().request_payload().unwrap())
                    .unwrap();
            assert_eq!(
                matches!(payload.origin(), HistoryExpiryOrigin::Manual { .. }),
                manual_first
            );
            super::super::history_expiry::submit_periodic(&fixture.context)
                .await
                .unwrap();
            let repeated = super::super::history_expiry::submit_manual(
                &fixture.context,
                &fixture.admin,
                scan.clone(),
            )
            .await
            .unwrap();
            assert!(!repeated.created());
            assert_eq!(repeated.task(), original.task());
            let denied = AppContext::new(
                storage.clone(),
                Arc::new(crate::permissions::test_support::MockTreetopBackend::new()),
            );
            assert!(matches!(
                super::super::history_expiry::submit_manual(&denied, &fixture.admin, scan).await,
                Err(ApiError::Forbidden(_))
            ));
        })
        .await;
    storage.unregister_runtime(registration).await.unwrap();
}

#[rstest::rstest]
#[case::released("released")]
#[case::original_deadline("deadline")]
#[case::cancelled("cancelled")]
#[case::authentication_failure("authentication")]
#[actix_web::test]
async fn expiry_publication_wait_preserves_original_execution_and_authentication(
    #[case] mode: &str,
) {
    use hubuum_task_core::{TaskExecutionContext, TaskStopReason};
    use std::time::Duration as StdDuration;
    let fixture = Fixture::new().await;
    let held = session(&fixture.options).await.unwrap();
    let original = held.checkpoint().identity().to_owned();
    let (execution, stop) = TaskExecutionContext::new(if mode == "deadline" {
        StdDuration::from_millis(50)
    } else {
        StdDuration::from_secs(5)
    })
    .unwrap();
    let release = async {
        tokio::time::sleep(if mode == "deadline" {
            StdDuration::from_millis(100)
        } else {
            StdDuration::from_millis(30)
        })
        .await;
        if mode == "cancelled" {
            stop.request_stop(TaskStopReason::Cancelled);
        }
        if mode == "authentication" {
            fs::write(fixture.root.join("repository/HEAD"), b"invalid checkpoint").unwrap();
        }
        drop(held);
    };
    let (result, ()) = tokio::join!(expiry_session(&fixture.options, &execution), release);
    match mode {
        "released" => assert_eq!(result.unwrap().checkpoint().identity(), original),
        "deadline" => assert!(matches!(
            result,
            Err(ApiError::TaskStopped(TaskStopReason::DeadlineExceeded))
        )),
        "cancelled" => assert!(matches!(
            result,
            Err(ApiError::TaskStopped(TaskStopReason::Cancelled))
        )),
        "authentication" => assert!(matches!(result, Err(ApiError::ServiceUnavailable(_)))),
        _ => unreachable!(),
    }
}

#[cfg(feature = "integration-test-support")]
#[actix_web::test]
async fn expiry_worker_protects_membership_once_and_receipts_before_completion() {
    use hubuum_domain::GroupId;
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::{
        DeletionControlStorage, ExecutionStorage, HistoryExpiryTrigger, HistoryRetentionSeconds,
        LogicalIdentityStorage, StorageCollectionCreate, StorageCollectionUpdate,
        StorageExecutionScope, StorageIdentityResourceKind, StorageIdentityTarget,
        StorageObservation, StorageObserver, StorageTaskStatus, TaskQueueStorage,
    };
    use std::sync::Mutex;
    #[derive(Default)]
    struct PublicationTrace {
        head: Mutex<Option<PathBuf>>,
        seen: Mutex<Vec<String>>,
        operations: Mutex<Vec<String>>,
    }
    impl StorageObserver for PublicationTrace {
        fn operation_finished(&self, observation: &StorageObservation) {
            if matches!(
                observation.operation(),
                "process_history_expiry_task" | "get_task_access"
            ) && self.head.lock().unwrap().is_some()
            {
                self.operations
                    .lock()
                    .unwrap()
                    .push(observation.operation().to_owned());
            }
            if observation.operation() == "process_history_expiry_task"
                && observation.result() == "ok"
                && let Some(head) = self.head.lock().unwrap().as_ref()
            {
                let signed: serde_json::Value =
                    serde_json::from_slice(&fs::read(head).unwrap()).unwrap();
                self.seen
                    .lock()
                    .unwrap()
                    .push(signed["payload"]["sha256"].as_str().unwrap().to_owned());
            }
        }
    }
    let observer = Arc::new(PublicationTrace::default());
    let storage = StorageHandle::from_registered_backend_with_observer(
        hubuum_storage_memory::MemoryStorage::new(),
        observer.clone(),
    );
    let mut fixture = Fixture::with_storage(storage.clone()).await;
    let event = EventContext::system();
    let collection = storage
        .collection_store()
        .create_collection(
            StorageCollectionCreate::new(
                format!("expiry_protection_{}", Uuid::new_v4().simple()),
                "before",
                GroupId::new(1).unwrap(),
                None,
            ),
            &event,
        )
        .await
        .unwrap()
        .into_value();
    let identity = storage
        .resolve_logical_identity(StorageIdentityTarget::Resource {
            kind: StorageIdentityResourceKind::Collection,
            id: collection.id().into(),
        })
        .await
        .unwrap()
        .subject()
        .unwrap()
        .record_identity()
        .unwrap()
        .resource();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Collection,
        HistoryPolicyScope::Collection { identity },
    )
    .unwrap();
    apply(
        &fixture.context,
        &fixture.admin,
        &fixture.options,
        HistoryPolicyApplyRequest::new(
            HistoryPolicyChange::new(
                key,
                HistoryPolicyRevision::new(0).unwrap(),
                HistoryPolicyBinding::Expire {
                    trigger: HistoryExpiryTrigger::Age {
                        age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                        deleted_age_seconds: None,
                    },
                    minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                },
                HistoryPolicyApplication::Prospective,
            )
            .unwrap(),
            None,
        ),
    )
    .await
    .unwrap();
    for description in ["first", "second", "third"] {
        storage
            .collection_store()
            .update_collection(
                collection.id(),
                StorageCollectionUpdate::new(None, Some(description.into())),
                &event,
            )
            .await
            .unwrap()
            .into_value();
    }
    let registration = storage.register_history_runtime(None).await.unwrap();
    let bound = storage.clone().with_runtime_owner(registration.owner());
    fixture.context = AppContext::new(
        bound.clone(),
        Arc::new(LocalPermissionBackend::new(bound.clone(), "admin".into())),
    )
    .with_history_retention(fixture.options.clone());
    let application = bound
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    bound
        .run_in_scope(scope, async {
            let admitted = super::super::history_expiry::submit_manual(
                &fixture.context,
                &fixture.admin,
                HistoryLifecycleScan::new(key, 1000).unwrap(),
            )
            .await
            .unwrap();
            *observer.head.lock().unwrap() = Some(fixture.root.join("repository/HEAD"));
            assert!(
                crate::tasks::qualify_history_expiry_iteration(&fixture.context)
                    .await
                    .unwrap()
            );
            let operations = observer.operations.lock().unwrap().clone();
            let first = operations
                .iter()
                .position(|operation| operation == "process_history_expiry_task")
                .unwrap();
            let last = operations
                .iter()
                .rposition(|operation| operation == "process_history_expiry_task")
                .unwrap();
            assert!(
                operations[first..=last]
                    .iter()
                    .all(|operation| operation != "get_task_access"),
                "Exact-copy steps must not reread advisory task progress: {operations:?}"
            );
            let completed = crate::tasks::history::retry_control(&bound, || async {
                Ok(bound
                    .get_task_access(admitted.task().id())
                    .await?
                    .into_parts()
                    .0)
            })
            .await
            .unwrap();
            assert_eq!(completed.status(), StorageTaskStatus::Succeeded);
            assert!(completed.progress().succeeded() >= 3);
            let seen = observer.seen.lock().unwrap().clone();
            assert_eq!(seen.len(), completed.progress().succeeded() as usize + 2);
            let mut distinct = seen.clone();
            distinct.dedup();
            assert_eq!(
                distinct.len(),
                3,
                "Only admission and completion may publish after the initial checkpoint: {seen:?}"
            );
            assert!(
                seen[1..seen.len() - 1]
                    .iter()
                    .all(|identity| identity == &seen[1])
            );
            let session = session(&fixture.options).await.unwrap();
            let primary = bound.capture_history_lifecycle().await.unwrap();
            assert!(
                session
                    .checkpoint()
                    .history_lifecycle()
                    .unwrap()
                    .covers(primary.control().authority())
            );
        })
        .await;
    storage.unregister_runtime(registration).await.unwrap();
}

struct Fixture {
    root: PathBuf,
    context: AppContext,
    admin: AdminAccess,
    options: ControlRecoveryOptions,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_storage(StorageHandle::memory()).await
    }
    async fn with_storage(storage: StorageHandle) -> Self {
        let context = AppContext::new(
            storage.clone(),
            Arc::new(LocalPermissionBackend::new(storage, "admin".into())),
        );
        let admin = AdminAccess {
            user: crate::services::identity::get_user(&context, 1)
                .await
                .unwrap(),
        };
        let root = std::env::temp_dir().join(format!("hubuum-policy-test-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [81; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        super::super::checkpoint(&context, &directory, &key, true)
            .await
            .unwrap();
        let options: ControlRecoveryOptions =
            serde_json::from_value(serde_json::json!({"directory":directory,"key":key})).unwrap();
        let context = context.with_history_retention(options.clone());
        Self {
            root,
            context,
            admin,
            options,
        }
    }
    async fn bearer(&self, scoped: bool) -> String {
        use hubuum_storage_core::{
            StorageAuthenticationTokenScope, StorageTokenCreate, StorageTokenDigest,
            StorageTokenIssuancePolicy, TokenStorage,
        };
        let raw = Uuid::new_v4().to_string();
        let digest = crate::models::Token::storage_hash_from_raw(&raw);
        let token = storage_handle(&self.context)
            .create_token(
                StorageTokenCreate::new(
                    hubuum_domain::PrincipalId::new(self.admin.user.id).unwrap(),
                    StorageTokenDigest::legacy_unidentified(&digest),
                    StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                    crate::events::EventContext::system(),
                )
                .scope(
                    scoped.then(|| StorageAuthenticationTokenScope::new(Some(Vec::new()), None)),
                ),
            )
            .await
            .unwrap()
            .into_value();
        assert_eq!(
            token.principal_id(),
            hubuum_domain::PrincipalId::new(self.admin.user.id).unwrap()
        );
        raw
    }
    fn proposal(&self) -> HistoryPolicyChange {
        HistoryPolicyChange::new(
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Indefinite,
            HistoryPolicyApplication::Retrospective {
                closed_through: Utc::now() - Duration::seconds(1),
            },
        )
        .unwrap()
    }
}

#[derive(Default)]
struct ApplyOnCapture {
    action: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl hubuum_storage_core::StorageObserver for ApplyOnCapture {
    fn operation_finished(&self, observation: &hubuum_storage_core::StorageObservation) {
        if observation.result() == "ok"
            && matches!(
                observation.operation(),
                "capture_history_lifecycle" | "capture_control_checkpoint"
            )
        {
            let action = self.action.lock().unwrap().take();
            if let Some(action) = action {
                // The native capture has returned; another request may now
                // attempt to commit and publish before list resumes.
                std::thread::spawn(action).join().unwrap();
            }
        }
    }
}

#[actix_web::test]
async fn policy_listing_serializes_capture_with_checkpoint_publication() {
    let observer = Arc::new(ApplyOnCapture::default());
    let storage = StorageHandle::from_registered_backend_with_observer(
        hubuum_storage_memory::MemoryStorage::new(),
        observer.clone(),
    );
    let fixture = Fixture::with_storage(storage).await;
    let proposal = HistoryPolicyChange::new(
        fixture.proposal().key(),
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Indefinite,
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let initial = apply(
        &fixture.context,
        &fixture.admin,
        &fixture.options,
        HistoryPolicyApplyRequest::new(proposal, None),
    )
    .await
    .unwrap();
    let context = fixture.context.clone();
    let user = fixture.admin.user.clone();
    let options = fixture.options.clone();
    let next = HistoryPolicyChange::new(
        initial.proposal().key(),
        initial.revision(),
        HistoryPolicyBinding::DisabledExpiry,
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let refused = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let attempted = refused.clone();
    *observer.action.lock().unwrap() = Some(Box::new(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(apply(
            &context,
            &AdminAccess { user },
            &options,
            HistoryPolicyApplyRequest::new(next, None),
        ));
        attempted.store(
            matches!(result, Err(ApiError::ServiceUnavailable(_))),
            std::sync::atomic::Ordering::SeqCst,
        );
    }));
    let page = list(
        &fixture.context,
        &fixture.admin,
        HistoryPolicyListRequest {
            after_revision: HistoryPolicyRevision::new(0).unwrap(),
            limit: 100,
        },
        &fixture.options,
    )
    .await
    .unwrap();
    assert!(matches!(
        page.protection,
        HistoryAuthorityProtection::Protected
    ));
    assert_eq!(
        page.protected_authority_revision,
        Some(page.authority_revision)
    );
    assert!(
        refused.load(std::sync::atomic::Ordering::SeqCst),
        "Concurrent checkpoint publication must remain excluded during capture"
    );
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[rstest::rstest]
#[case::apply("apply")]
#[case::create_hold("create_hold")]
#[case::release_hold("release_hold")]
#[actix_web::test]
async fn revoked_administrator_cannot_observe_scope_or_recovery_state(#[case] operation: &str) {
    use crate::models::history_lifecycle::HistoryPolicyScopeRequest;
    use crate::permissions::test_support::MockTreetopBackend;

    let fixture = Fixture::new().await;
    // AdminAccess was issued earlier. The configured policy now denies admin,
    // while both the recovery repository and selected scope are unavailable.
    let context = AppContext::new(
        storage_handle(&fixture.context).clone(),
        Arc::new(MockTreetopBackend::new()),
    );
    let unavailable = ControlRecoveryOptions::default();
    let result = match operation {
        "apply" => apply(
            &context,
            &fixture.admin,
            &unavailable,
            HistoryPolicyApplyRequest::new(fixture.proposal(), None),
        )
        .await
        .map(|_| ()),
        "create_hold" => create_hold(
            &context,
            &fixture.admin,
            &unavailable,
            HistoryRetentionHoldRequest {
                identity: Uuid::new_v4(),
                family: StorageIdentityHistoryKind::Object,
                scope: HistoryPolicyScopeRequest::Collection {
                    identity: hubuum_domain::ResourceIdentity::generate(),
                    current_id: Some(hubuum_domain::ResourceId::new(i32::MAX).unwrap()),
                },
                expected_authority_revision: HistoryAuthorityRevision::new(0).unwrap(),
                expires_at: None,
            },
        )
        .await
        .map(|_| ()),
        "release_hold" => release_hold(
            &context,
            &fixture.admin,
            &unavailable,
            Uuid::new_v4(),
            HistoryAuthorityRevision::new(0).unwrap(),
        )
        .await
        .map(|_| ()),
        _ => unreachable!("declared operation case"),
    };
    assert!(matches!(result, Err(ApiError::Forbidden(_))));
}

#[actix_web::test]
async fn natural_hold_expiry_is_settled_and_protected_once() {
    let fixture = Fixture::new().await;
    let storage = storage_handle(&fixture.context);
    let snapshot = storage.capture_history_lifecycle().await.unwrap();
    let deadline = snapshot.observed_at() - Duration::seconds(1);
    let identity = Uuid::new_v4();
    let hold = HistoryRetentionHold::new(
        identity,
        fixture.proposal().key(),
        deadline - Duration::seconds(60),
        Some(deadline),
        principal(&fixture.context, &fixture.admin).await.unwrap(),
    )
    .unwrap();
    let change = snapshot
        .control()
        .create_hold(hold, snapshot.fence())
        .unwrap();
    commit_protected(
        &fixture.context,
        session(&fixture.options).await.unwrap(),
        change,
    )
    .await
    .unwrap();

    assert!(settle_expired_hold(&fixture.context).await.unwrap());
    let settled = storage.capture_history_lifecycle().await.unwrap();
    assert!(!settle_expired_hold(&fixture.context).await.unwrap());
    let repeated = storage.capture_history_lifecycle().await.unwrap();
    assert_eq!(settled.control(), repeated.control());
    let expiry = settled
        .control()
        .authority()
        .hold_revisions()
        .last()
        .unwrap();
    assert_eq!(expiry.transition(), HistoryHoldTransition::Expired);
    assert_eq!(expiry.hold().expires_at(), Some(deadline));
    let protected = session(&fixture.options).await.unwrap();
    assert_eq!(
        protected.checkpoint().history_lifecycle(),
        Some(settled.control().authority())
    );
}

#[actix_web::test]
async fn review_apply_is_independently_protected_with_original_audit_identity() {
    let fixture = Fixture::new().await;
    let issued = preview(&fixture.context, &fixture.admin, fixture.proposal())
        .await
        .unwrap();
    let applied = apply(
        &fixture.context,
        &fixture.admin,
        &fixture.options,
        HistoryPolicyApplyRequest::new(issued.proposal().clone(), Some(issued.reference().clone())),
    )
    .await
    .unwrap();
    let session = session(&fixture.options).await.unwrap();
    let protected = session.checkpoint().history_lifecycle().unwrap();
    assert_eq!(
        protected.policies().records(),
        std::slice::from_ref(&applied)
    );
    assert_eq!(applied.review().unwrap().reference(), issued.reference());
    assert_eq!(applied.review().unwrap().expires_at(), issued.expires_at());
    let recovered: HistoryLifecycleAuthority =
        serde_json::from_slice(&serde_json::to_vec(protected).unwrap()).unwrap();
    assert_eq!(
        recovered
            .audit_events_since(&HistoryLifecycleAuthority::empty())
            .unwrap()[0]
            .event_id(),
        protected
            .audit_events_since(&HistoryLifecycleAuthority::empty())
            .unwrap()[0]
            .event_id()
    );
}

#[actix_web::test]
async fn a_review_without_current_independent_protection_cannot_acknowledge_application() {
    let fixture = Fixture::new().await;
    let issued = preview(&fixture.context, &fixture.admin, fixture.proposal())
        .await
        .unwrap();
    let result = apply(
        &fixture.context,
        &fixture.admin,
        &ControlRecoveryOptions::default(),
        HistoryPolicyApplyRequest::new(issued.proposal().clone(), Some(issued.reference().clone())),
    )
    .await;
    assert!(matches!(result, Err(ApiError::ServiceUnavailable(_))));
    assert_eq!(
        storage_handle(&fixture.context)
            .capture_history_lifecycle()
            .await
            .unwrap()
            .control()
            .authority()
            .revision()
            .value(),
        0
    );
}

#[actix_web::test]
async fn emitted_policy_metadata_matches_the_reviewed_audit_schema() {
    let fixture = Fixture::new().await;
    let issued = preview(&fixture.context, &fixture.admin, fixture.proposal())
        .await
        .unwrap();
    apply(
        &fixture.context,
        &fixture.admin,
        &fixture.options,
        HistoryPolicyApplyRequest::new(issued.proposal().clone(), Some(issued.reference().clone())),
    )
    .await
    .unwrap();
    let snapshot = storage_handle(&fixture.context)
        .capture_history_lifecycle()
        .await
        .unwrap();
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../docs/contracts/lifecycle-audit/audit-event.schema.json"
    ))
    .unwrap();
    let events = snapshot
        .control()
        .audit_events_since(&hubuum_storage_core::HistoryLifecycleControl::empty())
        .unwrap();
    for event in events {
        hubuum_domain::validate_json_value(&schema["properties"]["metadata"], event.metadata())
            .unwrap();
    }
}

#[actix_web::test]
async fn authenticated_http_preview_apply_protects_the_reviewed_policy() {
    use actix_web::{App, http::StatusCode, test, web};
    let fixture = Fixture::new().await;
    let bearer = fixture.bearer(false).await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(fixture.context.clone()))
            .app_data(web::JsonConfig::default().error_handler(crate::errors::json_error_handler))
            .configure(crate::api::config),
    )
    .await;
    let proposal = serde_json::to_value(fixture.proposal()).unwrap();
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/admin/history-lifecycle/policy-previews")
            .insert_header(("Authorization", format!("Bearer {bearer}")))
            .set_json(serde_json::json!({"change":proposal}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let issued: serde_json::Value = test::read_body_json(response).await;
    let response = test::call_service(
        &app,
        test::TestRequest::put()
            .uri("/api/v1/admin/history-lifecycle/policies")
            .insert_header(("Authorization", format!("Bearer {bearer}")))
            .set_json(serde_json::json!({"proposal":proposal,"review":issued["reference"]}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("Cache-Control").unwrap(), "no-store");
    let applied: serde_json::Value = test::read_body_json(response).await;
    let session = session(&fixture.options).await.unwrap();
    assert_eq!(
        serde_json::to_value(
            session
                .checkpoint()
                .history_lifecycle()
                .unwrap()
                .policies()
                .records()
                .last()
                .unwrap()
        )
        .unwrap(),
        applied
    );
}

#[rstest::rstest]
#[case(false, false, 401)]
#[case(true, true, 403)]
#[actix_web::test]
async fn policy_preview_requires_an_unscoped_administrator(
    #[case] authenticated: bool,
    #[case] scoped: bool,
    #[case] status: u16,
) {
    use actix_web::{App, test, web};
    let fixture = Fixture::new().await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(fixture.context.clone()))
            .configure(crate::api::config),
    )
    .await;
    let mut request = test::TestRequest::post()
        .uri("/api/v1/admin/history-lifecycle/policy-previews")
        .set_json(serde_json::json!({"change":fixture.proposal()}));
    if authenticated {
        request = request.insert_header((
            "Authorization",
            format!("Bearer {}", fixture.bearer(scoped).await),
        ));
    }
    let response = test::call_service(&app, request.to_request()).await;
    assert_eq!(response.status().as_u16(), status);
}

#[actix_web::test]
async fn policy_preview_rejects_unknown_write_fields_at_http_boundary() {
    use actix_web::{App, http::StatusCode, test, web};
    let fixture = Fixture::new().await;
    let bearer = fixture.bearer(false).await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(fixture.context.clone()))
            .app_data(web::JsonConfig::default().error_handler(crate::errors::json_error_handler))
            .configure(crate::api::config),
    )
    .await;
    let mut proposal = serde_json::to_value(fixture.proposal()).unwrap();
    proposal["approve_without_review"] = serde_json::json!(true);
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/admin/history-lifecycle/policy-previews")
            .insert_header(("Authorization", format!("Bearer {bearer}")))
            .set_json(serde_json::json!({"change":proposal}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[rstest::rstest]
#[case(false)]
#[case(true)]
#[actix_web::test]
async fn optional_write_fields_reject_explicit_null(#[case] review: bool) {
    use actix_web::{App, http::StatusCode, test, web};
    let fixture = Fixture::new().await;
    let bearer = fixture.bearer(false).await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(fixture.context.clone()))
            .configure(crate::api::config),
    )
    .await;
    let mut proposal = serde_json::to_value(fixture.proposal()).unwrap();
    let (request, body) = if review {
        proposal["application"] = serde_json::json!({"kind":"prospective"});
        (
            test::TestRequest::put().uri("/api/v1/admin/history-lifecycle/policies"),
            serde_json::json!({"proposal":proposal,"review":null}),
        )
    } else {
        proposal["scope"] =
            serde_json::json!({"kind":"class","identity":Uuid::new_v4(),"current_id":null});
        (
            test::TestRequest::post().uri("/api/v1/admin/history-lifecycle/policy-previews"),
            serde_json::json!({"change":proposal}),
        )
    };
    let response = test::call_service(
        &app,
        request
            .insert_header(("Authorization", format!("Bearer {bearer}")))
            .set_json(body)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn policy_scope_enforces_its_json_body_limit() {
    use actix_web::{App, http::StatusCode, test, web};
    let fixture = Fixture::new().await;
    let bearer = fixture.bearer(false).await;
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(fixture.context.clone()))
            .configure(crate::api::config),
    )
    .await;
    let mut proposal = serde_json::to_value(fixture.proposal()).unwrap();
    proposal["expected_policy_revision"] = serde_json::json!(
        "1".repeat(crate::models::history_lifecycle::HISTORY_LIFECYCLE_MAX_REQUEST_BYTES)
    );
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/admin/history-lifecycle/policy-previews")
            .insert_header(("Authorization", format!("Bearer {bearer}")))
            .set_json(serde_json::json!({"change":proposal}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn online_expiry_metadata_matches_the_reviewed_audit_schema() {
    use chrono::DateTime;
    use hubuum_domain::{ResourceIdentity, VersionIdentity};
    use hubuum_storage_core::{
        DeletionCheckpointIdentity, HistoryDeletionRevision, HistoryExpiryCandidate,
        HistoryExpiryInputBytes, HistoryExpiryJournal, HistoryExpiryOrigin, HistoryExpiryReadSpan,
        HistoryExpirySelection, HistoryExpiryTrigger, HistoryLifecycleControl,
        HistoryRetentionAnchor, HistoryRetentionSeconds, HistoryScopeAttribution,
        StorageRecordIdentity,
    };
    let at = |seconds| DateTime::from_timestamp(seconds, 0).unwrap();
    let principal = PrincipalIdentity::generate();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Object,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let initial = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        Uuid::new_v4(),
        HistoryDeletionRevision::new(0).unwrap(),
        at(100),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        HistoryPolicyRevision::new(0).unwrap(),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::Age {
                age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                deleted_age_seconds: None,
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let applied = initial
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            principal,
            initial.fence(),
            at(100),
        )
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        applied.next().clone(),
        initial.fence().incarnation(),
        initial.fence().deletion_revision(),
        at(200),
    )
    .unwrap();
    let target =
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
    let candidate = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Object,
        target,
        Some(HistoryRetentionAnchor::Closed(at(150))),
        vec![HistoryScopeAttribution::new(
            ResourceIdentity::generate(),
            None,
        )],
        false,
    )
    .unwrap()
    .with_read_span(HistoryExpiryReadSpan::closed(at(110), at(150)).unwrap())
    .unwrap();
    let mut selection = HistoryExpirySelection::new(
        Uuid::new_v4(),
        HistoryExpiryOrigin::Manual { principal },
        HistoryLifecycleScan::new(key, 1).unwrap(),
        &snapshot,
    )
    .unwrap();
    selection
        .consider(candidate, HistoryExpiryInputBytes::new(100).unwrap(), &[])
        .unwrap();
    let manifest = selection.finish().unwrap();
    let identity = manifest.identity();
    let journal = HistoryExpiryJournal::default().admit(manifest).unwrap();
    let intent = journal
        .intent(identity)
        .unwrap()
        .request_cancellation(principal, at(210))
        .unwrap()
        .record_removed(
            HistoryExpiryExactDeletion::from_affected_rows(
                target,
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
                at(220),
                1,
            )
            .unwrap(),
        )
        .unwrap();
    let journal = journal.retain(intent).unwrap();
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../docs/contracts/lifecycle-audit/audit-event.schema.json"
    ))
    .unwrap();
    let events = journal
        .audit_events_since(&HistoryExpiryJournal::default())
        .unwrap();
    assert_eq!(events.len(), 4);
    for event in events {
        hubuum_domain::validate_json_value(&schema["properties"]["metadata"], event.metadata())
            .unwrap();
    }
}
