#[cfg(feature = "permissions-treetop")]
use std::net::TcpListener;
use std::sync::Arc;
#[cfg(feature = "permissions-treetop")]
use std::sync::Mutex;

use actix_web::{App, ResponseError, test as actix_test, web};
#[cfg(feature = "permissions-treetop")]
use actix_web::{HttpResponse, HttpServer, http::StatusCode};
#[cfg(feature = "permissions-treetop")]
use clap::Parser;
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    StorageTask, StorageTaskCreateRequest, StorageTaskKind, StorageTaskStatus, StorageUserCreate,
    TaskQueueStorage, UserStorage,
};
use rstest::rstest;
use serde_json::{Value, json};

use super::*;
#[cfg(feature = "permissions-treetop")]
use crate::config::AppConfig;
use crate::models::TaskKind;
use crate::permissions::LocalPermissionBackend;
#[cfg(feature = "permissions-treetop")]
use crate::permissions::treetop::TreetopPermissionBackend;
use crate::storage::StorageHandle;

async fn actor(storage: &StorageHandle) -> PrincipalId {
    let user = storage
        .create_user(StorageUserCreate::new(
            None,
            "receipt-owner",
            "test-hash",
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    PrincipalId::new(user.into_parts().id().id()).unwrap()
}

#[actix_web::test]
async fn expired_receipt_http_response_contains_only_authorized_minimal_evidence() {
    let storage = StorageHandle::memory();
    let actor = actor(&storage).await;
    let owner = principal_identity(&storage, &actor).await.unwrap();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/lifecycle/task_submission_outcomes.json"
    ))
    .unwrap();
    let expected = &fixture["expired_details"];
    let task = serde_json::from_value(expected["body"]["task_identity"].clone()).unwrap();
    let receipt = ExpiredTaskSubmission::new(task, TaskKind::Export, None, owner);
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let context = AppContext::new(storage, permissions);
    let app = actix_test::init_service(App::new().route(
        "/receipt",
        web::post().to(move || {
            let context = context.clone();
            let receipt = receipt.clone();
            async move {
                let error = expired_error(&context, &actor, receipt).await.unwrap();
                error.error_response()
            }
        }),
    ))
    .await;
    let response = actix_test::call_service(
        &app,
        actix_test::TestRequest::post().uri("/receipt").to_request(),
    )
    .await;
    assert_eq!(
        response.status().as_u16(),
        expected["status"].as_u64().unwrap() as u16
    );
    assert_eq!(
        response.headers().get("Cache-Control").unwrap(),
        expected["headers"]["cache-control"].as_str().unwrap()
    );
    assert!(response.headers().get("Retry-After").is_none());
    let body: Value = actix_test::read_body_json(response).await;
    assert_eq!(body, expected["body"]);
}

#[actix_web::test]
async fn identical_integer_principal_in_another_lineage_cannot_read_old_receipt() {
    let original = StorageHandle::memory();
    let old_actor = actor(&original).await;
    let old_owner = principal_identity(&original, &old_actor).await.unwrap();
    let replacement = StorageHandle::memory();
    let new_actor = actor(&replacement).await;
    assert_eq!(old_actor, new_actor);
    let permissions = Arc::new(LocalPermissionBackend::new(
        replacement.clone(),
        "admin".into(),
    ));
    let context = AppContext::new(replacement, permissions);
    let result = expired_error(
        &context,
        &new_actor,
        ExpiredTaskSubmission::new(TaskIdentity::generate(), TaskKind::Import, None, old_owner),
    )
    .await;
    assert!(matches!(result, Err(ApiError::NotFound(_))));
}

#[actix_web::test]
async fn duplicate_authorization_does_not_reload_a_reused_integer_task_id() {
    let storage = StorageHandle::memory();
    let owner = actor(&storage).await;
    let other = storage
        .create_user(StorageUserCreate::new(
            None,
            "replacement-task-owner",
            "test-hash",
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts();
    let replacement = crate::tests::asserts::expect_legacy_task(
        storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    PrincipalId::new(other.id().id()).unwrap(),
                    json!({}),
                    0,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    let original = StorageTask::builder(
        replacement.id(),
        StorageTaskKind::Import,
        StorageTaskStatus::Succeeded,
        replacement.created_at(),
        replacement.updated_at(),
    )
    .submitted_by(Some(owner))
    .summary(Some("original captured admission".into()))
    .finished_at(Some(replacement.updated_at()))
    .try_build()
    .unwrap();
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let context = AppContext::new(storage, permissions);
    assert!(matches!(
        crate::services::tasks::load_authorized_task(&context, &owner, replacement.id()).await,
        Err(ApiError::NotFound(_))
    ));
    let authorized = authorize_submission_task(&context, &owner, original)
        .await
        .unwrap();
    assert_eq!(
        authorized.summary.as_deref(),
        Some("original captured admission")
    );
    assert_eq!(authorized.submitted_by, Some(owner.id()));
}

#[cfg(feature = "permissions-treetop")]
#[rstest]
#[case::allowed("Allow", StatusCode::OK, 410)]
#[case::denied("Deny", StatusCode::OK, 403)]
#[case::unavailable("Allow", StatusCode::SERVICE_UNAVAILABLE, 503)]
#[actix_web::test]
async fn expired_receipt_uses_real_external_read_task_policy(
    #[case] decision: &'static str,
    #[case] status: StatusCode,
    #[case] expected: u16,
) {
    let storage = StorageHandle::memory();
    let actor = actor(&storage).await;
    let owner = principal_identity(&storage, &actor).await.unwrap();
    let task = TaskIdentity::generate();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = requests.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = HttpServer::new(move || {
        let captured = captured.clone();
        App::new().route("/readyz", web::get().to(|| async { HttpResponse::Ok().finish() }))
            .default_service(web::post().to(move |body: web::Json<Value>| {
                captured.lock().unwrap().push(body.into_inner());
                async move {
                    let version = json!({"hash":"test", "loaded_at":"2025-01-01T00:00:00Z", "label_set":null, "generation":1});
                    HttpResponse::build(status).json(json!({
                        "results":[{"index":0,"status":"success","result":{"decision":decision,"version":version,"policy_id":if decision == "Allow" { "receipt-policy" } else { "" }}}],
                        "version":version,"successful":1,"failed":0
                    }))
                }
            }))
    }).workers(1).listen(listener).unwrap().run();
    let stop = server.handle();
    let server = actix_web::rt::spawn(server);
    let config = AppConfig::parse_from(["hubuum"]);
    let permissions = TreetopPermissionBackend::connect(&address, &config, storage.clone())
        .await
        .unwrap();
    let context = AppContext::new(storage, Arc::new(permissions));
    let result = expired_error(
        &context,
        &actor,
        ExpiredTaskSubmission::new(task, TaskKind::Backup, None, owner),
    )
    .await;
    let error = result.unwrap_or_else(std::convert::identity);
    stop.stop(true).await;
    server.await.unwrap().unwrap();
    assert_eq!(error.status_code().as_u16(), expected, "{error}");
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = serde_json::to_string(&requests[0]).unwrap();
    assert!(request.contains("ReadTask"));
    assert!(request.contains("HubuumTask"));
    assert!(request.contains(&format!("receipt:{task}")));
}

mod http;
