#![cfg(feature = "integration-test-support")]

use actix_web::{http::StatusCode, test};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum::models::{TaskResponse, TaskStatus};
use hubuum::schema::tasks::dsl::{id, request_payload, tasks};
use hubuum::tests::api_operations::{credential_post_request_with_headers, get_request};
use hubuum::tests::asserts::assert_response_status;
use hubuum::tests::{TestContext, integration_test_config};
use hubuum_storage_postgres::with_connection;
use serde_json::{Value, json};
use std::process::Command;
use std::time::Duration;

// This binary has its own task queue worker. Cargo runs it separately from
// the API suites, whose workers use their own process-wide secret configuration.
#[actix_web::test]
async fn credential_import_queue_and_worker_use_the_configured_key() {
    const PHASE: &str = "HUBUUM_TEST_CREDENTIAL_IMPORT_KEY_PHASE";
    let Ok(phase) = std::env::var(PHASE) else {
        for phase in ["missing", "queued", "worker"] {
            let mut child = Command::new(std::env::current_exe().unwrap());
            child
                .args([
                    "--exact",
                    "credential_import_queue_and_worker_use_the_configured_key",
                ])
                .env(PHASE, phase)
                .env("HUBUUM_SECRET_SOURCE", "environment")
                .env_remove("HUBUUM_SECRET_FILE_ROOT")
                .env_remove("HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF");
            if phase != "missing" {
                child
                    .env("HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF", "importtest")
                    .env(
                        "HUBUUM_RUNTIME_SETTINGS_KEY_IMPORTTEST",
                        "01234567890123456789012345678901",
                    );
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
                "{phase}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };

    let mut config = integration_test_config().unwrap();
    config.task_workers = usize::from(phase == "worker");
    hubuum::tasks::initialize_task_worker_settings(config.task_worker_settings().unwrap()).unwrap();
    let context = TestContext::new().await;
    let password = "credential-import-fixture";
    let request = json!({
        "version": 2, "dry_run": true, "graph": {"principals": [{
            "name": context.scoped_name("credential_import"), "kind": "human",
            "identity_scope_key": {"name": "local"}, "provider_managed": false,
            "password": password
        }]}
    });
    let response = credential_post_request_with_headers(
        &context.pool,
        &context.admin_token,
        "/api/v1/imports",
        &request,
        vec![],
    )
    .await;
    if phase == "missing" {
        assert_response_status(response, StatusCode::SERVICE_UNAVAILABLE).await;
        return;
    }
    let response = assert_response_status(response, StatusCode::ACCEPTED).await;
    let task: TaskResponse = test::read_body_json(response).await;
    if phase == "queued" {
        let stored = with_connection(&context.pool, async |conn| {
            tasks
                .filter(id.eq(task.id))
                .select(request_payload)
                .first::<Option<Value>>(conn)
                .await
        })
        .await
        .unwrap()
        .expect("queued request should be present");
        assert!(stored["encrypted_import"].is_object());
        assert!(stored.get("graph").is_none());
        assert!(!stored.to_string().contains(password));
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task.id).unwrap(),
        )
        .await
        .unwrap();
        return;
    }
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response = get_request(
                &context.pool,
                &context.admin_token,
                &format!("/api/v1/tasks/{}", task.id),
            )
            .await;
            let response = assert_response_status(response, StatusCode::OK).await;
            let observed: TaskResponse = test::read_body_json(response).await;
            if observed.status.is_terminal() {
                break observed;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("credential import should complete");
    assert_eq!(
        completed.status,
        TaskStatus::Succeeded,
        "{:?}",
        completed.summary
    );
    assert_eq!(completed.progress.success_items, 1);
}
