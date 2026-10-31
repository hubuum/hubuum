use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use hubuum_storage_core::{
    StorageAuthenticationTokenScope, StorageTaskCancellationRequest, StorageTaskCreateRequest,
    StorageTokenCreate, StorageTokenDigest, StorageTokenIssuancePolicy, TaskControlChange,
    TaskExecutionStorage, TaskQueueStorage, TaskReceiptActivation, TaskRetentionPolicy,
    TaskRetentionStorage, TokenStorage,
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::*;
use crate::config::ControlRecoveryOptions;
use crate::models::schema_profiling::SchemaProfileRequest;
use crate::models::{
    BackupRequest, ExportRequest, ExportTemplateRunRequest, ImportRequest,
    RemoteTargetInvokeRequest, Token,
};
use crate::services::deletion_recovery::TaskReceiptService;
use crate::services::tasks::TaskSubmissionOperation;

struct Fixture {
    root: PathBuf,
    context: AppContext,
    actor: PrincipalId,
    bearer: String,
}

impl Fixture {
    async fn new(scoped: bool) -> Self {
        let storage = StorageHandle::memory();
        let actor = actor(&storage).await;
        let bearer = Uuid::new_v4().to_string();
        let hash = Token::storage_hash_from_raw(&bearer);
        let token = storage
            .create_token(
                StorageTokenCreate::new(
                    actor,
                    StorageTokenDigest::legacy_unidentified(&hash),
                    StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                    EventContext::system(),
                )
                .scope(
                    scoped.then(|| StorageAuthenticationTokenScope::new(Some(Vec::new()), None)),
                ),
            )
            .await
            .unwrap()
            .into_value();
        assert_eq!(token.principal_id(), actor);
        let root = std::env::temp_dir().join(format!("hubuum-task-api-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [61; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        crate::services::deletion_recovery::checkpoint(&storage, &directory, &key, true)
            .await
            .unwrap();
        let options: ControlRecoveryOptions =
            serde_json::from_value(json!({"directory":directory,"key":key})).unwrap();
        let service = TaskReceiptService::configured(&options).await.unwrap();
        let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
        let context = AppContext::new(storage, permissions).with_task_receipts(service);
        Self {
            root,
            context,
            actor,
            bearer,
        }
    }

    async fn pending(
        &self,
        operation: TaskSubmissionOperation,
        body: &Value,
    ) -> TaskAdmissionTicket {
        let input = original(operation, body.clone());
        let owner = principal_identity(&self.context, &self.actor)
            .await
            .unwrap();
        let key = IdempotencyKey::new("original-key").unwrap();
        let binding = self
            .context
            .task_receipts()
            .prepare_submission(
                owner,
                &key,
                operation.kind(),
                operation.as_str(),
                &input,
                None,
            )
            .unwrap()
            .unwrap();
        let storage = storage_handle(&self.context);
        let mut state = storage.read_task_control().await.unwrap();
        let policy = TaskRetentionPolicy::new(
            state.policy().revision().next().unwrap(),
            Duration::from_secs(3600),
            Some(Duration::ZERO),
        )
        .unwrap();
        let next = state.with_policy(policy).unwrap();
        state = storage
            .commit_task_control(TaskControlChange::new(state, next).unwrap())
            .await
            .unwrap();
        for activation in [
            TaskReceiptActivation::Backfilling {
                key_fingerprint: binding.key_fingerprint().clone(),
            },
            TaskReceiptActivation::Protected {
                key_fingerprint: binding.key_fingerprint().clone(),
            },
        ] {
            let next = state.with_activation(activation).unwrap();
            state = storage
                .commit_task_control(TaskControlChange::new(state, next).unwrap())
                .await
                .unwrap();
        }
        let request = StorageTaskCreateRequest::builder(operation.kind(), self.actor, json!({}), 1)
            .idempotency_key(Some(key))
            .request_hash(Some(crate::tasks::request_hash(&input).unwrap()))
            .receipt_binding(Some(binding))
            .try_build()
            .unwrap();
        let StorageTaskSubmissionOutcome::PendingProtection(ticket) =
            storage.create_task(request).await.unwrap()
        else {
            panic!("new keyed task must require protection")
        };
        ticket
    }

    async fn admit(&self, operation: TaskSubmissionOperation, body: &Value) -> (TaskIdentity, i32) {
        let ticket = self.pending(operation, body).await;
        let storage = storage_handle(&self.context);
        let identity = ticket.receipt().task();
        let TaskActivationOutcome::Activated(task) = self
            .context
            .task_receipts()
            .protect_admission(&self.context, ticket)
            .await
            .unwrap()
        else {
            panic!("fixture activation must win")
        };
        storage
            .request_task_cancellation(StorageTaskCancellationRequest::new(
                &task,
                EventContext::system(),
            ))
            .await
            .unwrap();
        (identity, task.id().id())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn normalize<T: DeserializeOwned + Serialize>(body: Value) -> Value {
    serde_json::to_value(serde_json::from_value::<T>(body).unwrap()).unwrap()
}

fn original(operation: TaskSubmissionOperation, body: Value) -> Value {
    match operation {
        TaskSubmissionOperation::Import => normalize::<ImportRequest>(body),
        TaskSubmissionOperation::Backup => normalize::<BackupRequest>(body),
        TaskSubmissionOperation::Export => normalize::<ExportRequest>(body),
        TaskSubmissionOperation::TemplateExport => {
            json!([999, normalize::<ExportTemplateRunRequest>(body)])
        }
        TaskSubmissionOperation::RemoteInvocation => {
            json!([999, normalize::<RemoteTargetInvokeRequest>(body)])
        }
        TaskSubmissionOperation::SchemaProfile => {
            json!([999, normalize::<SchemaProfileRequest>(body)])
        }
    }
}

fn request(operation: TaskSubmissionOperation, changed: bool) -> (&'static str, Value) {
    match operation {
        TaskSubmissionOperation::Import => (
            "/api/v1/imports",
            json!({"version":2,"dry_run":changed,"graph":{"principals":[{"name":"credential-bearing","kind":"human","identity_scope_key":{"name":"local"},"provider_managed":false,"password":"not-retained-password"}]}}),
        ),
        TaskSubmissionOperation::Backup => ("/api/v1/backups", json!({"include_history":!changed})),
        TaskSubmissionOperation::Export => (
            "/api/v1/exports",
            json!({"scope":{"kind":"classes"},"query":if changed { "name=changed" } else { "name=original" }}),
        ),
        TaskSubmissionOperation::TemplateExport => (
            "/api/v1/export-templates/999/exports",
            json!({"query":if changed { "name=changed" } else { "name=original" }}),
        ),
        TaskSubmissionOperation::RemoteInvocation => (
            "/api/v1/remote-targets/999/invoke",
            json!({"subject":{"type":"collection","collection_id":999},"parameters":{"changed":changed}}),
        ),
        TaskSubmissionOperation::SchemaProfile => (
            "/api/v1/classes/999/schema/profiles",
            json!({"sample_limit":if changed { 11 } else { 10 }}),
        ),
    }
}

#[derive(Clone, Copy)]
enum Retry {
    Retained,
    Changed,
    Expired,
}

#[rstest]
#[case::import(TaskSubmissionOperation::Import)]
#[case::backup(TaskSubmissionOperation::Backup)]
#[case::export(TaskSubmissionOperation::Export)]
#[case::template(TaskSubmissionOperation::TemplateExport)]
#[case::remote(TaskSubmissionOperation::RemoteInvocation)]
#[case::profile(TaskSubmissionOperation::SchemaProfile)]
#[actix_web::test]
async fn protected_retry_precedes_approval_and_mutable_dependency_reads(
    #[case] operation: TaskSubmissionOperation,
    #[values(Retry::Retained, Retry::Changed, Retry::Expired)] retry: Retry,
) {
    let fixture = Fixture::new(matches!(retry, Retry::Expired)).await;
    let (_, body) = request(operation, false);
    let (identity, task_id) = fixture.admit(operation, &body).await;
    if matches!(retry, Retry::Expired) {
        fixture
            .context
            .task_receipts()
            .sweep_task_retention(&fixture.context)
            .await
            .unwrap();
    }
    let before = storage_handle(&fixture.context)
        .read_task_control()
        .await
        .unwrap();
    let (path, body) = request(operation, matches!(retry, Retry::Changed));
    let app = actix_test::init_service(
        App::new()
            .app_data(web::Data::new(fixture.context.clone()))
            .configure(crate::api::config),
    )
    .await;
    let response = actix_test::call_service(
        &app,
        actix_test::TestRequest::post()
            .uri(path)
            .insert_header(("Authorization", format!("Bearer {}", fixture.bearer)))
            .insert_header(("Idempotency-Key", "original-key"))
            .set_json(body)
            .to_request(),
    )
    .await;
    let expected = match retry {
        Retry::Retained => 202,
        Retry::Changed => 409,
        Retry::Expired => 410,
    };
    let status = response.status().as_u16();
    if !matches!(retry, Retry::Retained) {
        assert_eq!(response.headers().get("Cache-Control").unwrap(), "no-store");
        assert!(response.headers().get("Retry-After").is_none());
    }
    let body: Value = actix_test::read_body_json(response).await;
    assert_eq!(status, expected, "{body}");
    match retry {
        Retry::Retained => assert_eq!(body["id"], task_id),
        Retry::Expired => assert_eq!(body["task_identity"], json!(identity)),
        Retry::Changed => {
            let fixture: Value = serde_json::from_str(include_str!(
                "../../../../../tests/fixtures/lifecycle/task_submission_outcomes.json"
            ))
            .unwrap();
            assert_eq!(body, fixture["changed_request"]["body"]);
        }
    }
    assert_eq!(
        storage_handle(&fixture.context)
            .read_task_control()
            .await
            .unwrap(),
        before,
        "a retry must not create, reactivate or renew a receipt"
    );
}

#[cfg(feature = "permissions-treetop")]
#[rstest]
#[case::duplicate_winner(false, ActivationResponseAccess::RequiresTaskRead)]
#[case::duplicate_after_helper(true, ActivationResponseAccess::RequiresTaskRead)]
#[case::creator_winner(false, ActivationResponseAccess::CreationAuthorized)]
#[case::creator_after_helper(true, ActivationResponseAccess::CreationAuthorized)]
#[actix_web::test]
async fn activation_response_keeps_receipt_read_separate_from_task_read(
    #[case] helper_already_activated: bool,
    #[case] access: ActivationResponseAccess,
) {
    use actix_web::body::to_bytes;

    let fixture = Fixture::new(false).await;
    let (_, body) = request(TaskSubmissionOperation::Import, false);
    let ticket = fixture
        .pending(TaskSubmissionOperation::Import, &body)
        .await;
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = requests.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = HttpServer::new(move || {
        let captured = captured.clone();
        App::new()
            .route("/readyz", web::get().to(|| async { HttpResponse::Ok().finish() }))
            .default_service(web::post().to(move |body: web::Json<Value>| {
                let body = body.into_inner();
                let receipt = serde_json::to_string(&body).unwrap().contains("receipt:");
                captured.lock().unwrap().push(body);
                async move {
                    let version = json!({"hash":"test", "loaded_at":"2025-01-01T00:00:00Z", "label_set":null, "generation":1});
                    HttpResponse::Ok().json(json!({
                        "results":[{"index":0,"status":"success","result":{"decision":if receipt { "Allow" } else { "Deny" },"version":version,"policy_id":if receipt { "receipt-only" } else { "" }}}],
                        "version":version,"successful":1,"failed":0
                    }))
                }
            }))
    }).workers(1).listen(listener).unwrap().run();
    let stop = server.handle();
    let server = actix_web::rt::spawn(server);
    let storage = storage_handle(&fixture.context);
    let config = AppConfig::parse_from(["hubuum"]);
    let permissions = TreetopPermissionBackend::connect(&address, &config, storage.clone())
        .await
        .unwrap();
    let context = AppContext::new(storage, Arc::new(permissions))
        .with_task_receipts(fixture.context.task_receipts().clone());
    let attempt = TaskSubmissionAttempt::prepare(
        &context,
        &fixture.actor,
        Some(IdempotencyKey::new("original-key").unwrap()),
        TaskSubmissionOperation::Import,
        &original(TaskSubmissionOperation::Import, body),
        None,
    )
    .await
    .unwrap();
    let duplicate = matches!(access, ActivationResponseAccess::RequiresTaskRead);
    if duplicate {
        authorize_receipt(
            &context,
            &fixture.actor,
            ticket.receipt().submission().principal(),
            ticket.receipt().task(),
        )
        .await
        .unwrap();
    }
    if helper_already_activated {
        assert!(matches!(
            context
                .task_receipts()
                .protect_admission(&context, ticket.clone())
                .await
                .unwrap(),
            TaskActivationOutcome::Activated(_)
        ));
    }
    let outcome = context
        .task_receipts()
        .protect_admission(&context, ticket)
        .await
        .unwrap();
    assert_eq!(
        matches!(&outcome, TaskActivationOutcome::AlreadyActive(_)),
        helper_already_activated
    );
    // Use the production response boundary after a real protected activation,
    // without starting the process-global worker from this isolated fixture.
    let result = attempt
        .activation_response(&context, &fixture.actor, outcome, access)
        .await;
    stop.stop(true).await;
    server.await.unwrap().unwrap();
    if duplicate {
        let error = result.unwrap_err();
        let response = error.error_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{error}");
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap();
        assert!(
            body.get("id").is_none(),
            "receipt-only permission must not expose task details"
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            serde_json::to_string(&requests[0])
                .unwrap()
                .contains("receipt:")
        );
        let task_read = serde_json::to_string(&requests[1]).unwrap();
        assert!(task_read.contains("ReadTask"));
        assert!(!task_read.contains("receipt:"));
    } else {
        assert!(
            result.is_ok(),
            "creation authority must survive another helper winning activation"
        );
        assert!(requests.lock().unwrap().is_empty());
    }
}
