//! Hermetic live-Treetop authorization conformance tests.
//!
//! These tests are deliberately ignored by the ordinary Rust test suite. The
//! dedicated `scripts/run-treetop-conformance.sh` runner supplies a pinned real
//! service, PostgreSQL, and private-CA TLS proxy, then runs every ignored test
//! in this module with one test thread. Missing fixture inputs are fatal; there
//! is no successful "not configured" path.

#![cfg(test)]

use std::env::var;
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use actix_web::test as actix_test;
use tokio::sync::{Mutex, MutexGuard};

use crate::config::{AppConfig, PermissionBackendKind, get_config};
use crate::errors::ApiError;
use crate::models::{CollectionID, GroupID, Permissions};
use crate::permissions::PermissionBackend;
use crate::permissions::treetop::TreetopPermissionBackend;
use crate::permissions::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};
use crate::permissions::visibility::{AuthorizationPage, paginate_authorized};
use crate::tests::get_test_pool;
use crate::tests::permissions::conformance::{
    ConformanceBackend, ConformanceFixture, assert_backend_conformance,
};

const LIVE_TEST_REASON: &str = "run by scripts/run-treetop-conformance.sh";
const TEST_USER_ID: i32 = 9_001;
const TEST_USER_ID_SECOND: i32 = 9_002;
const TEST_ADMIN_GROUP_ID: i32 = 9_100;
const TEST_NORMAL_GROUP_ID: i32 = 9_101;
const TEST_COLLECTION_ID: i32 = 9_201;
const TEST_DENIED_COLLECTION_ID: i32 = 9_202;
const TEST_CLASS_ID: i32 = 9_301;
const TEST_OBJECT_ID: i32 = 9_401;
const TEST_TASK_ID: i32 = 9_501;
const CONTROLLED_CONTAINER_PREFIX: &str = "hubuum-treetop-conformance-";

static LIVE_TEST_LOCK: Mutex<()> = Mutex::const_new(());

async fn live_test_guard() -> MutexGuard<'static, ()> {
    LIVE_TEST_LOCK.lock().await
}

fn required_fixture_value(name: &str) -> String {
    let value = var(name).unwrap_or_else(|_| {
        panic!("{name} is required; run the hermetic suite with scripts/run-treetop-conformance.sh")
    });
    assert!(!value.trim().is_empty(), "{name} must not be empty");
    value
}

fn live_config(url: &str) -> AppConfig {
    let mut config = get_config()
        .expect("failed to load the test configuration")
        .clone();
    config.treetop_url = Some(url.to_string());
    config.permission_backend = PermissionBackendKind::Treetop;
    config.treetop_accept_invalid_certs = false;
    config
}

async fn backend_with_config(config: &AppConfig) -> Result<TreetopPermissionBackend, ApiError> {
    let pool = get_test_pool().get_ref().clone();
    TreetopPermissionBackend::connect(
        config
            .treetop_url
            .as_deref()
            .expect("the live test URL must be configured"),
        config,
        pool,
    )
    .await
}

async fn live_backend() -> Result<TreetopPermissionBackend, ApiError> {
    let url = required_fixture_value("HUBUUM_TREETOP_TEST_URL");
    backend_with_config(&live_config(&url)).await
}

fn fixture() -> ConformanceFixture {
    ConformanceFixture {
        normal: PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]),
        administrator: PrincipalRef::new(TEST_USER_ID_SECOND, [TEST_ADMIN_GROUP_ID]),
        unprivileged: PrincipalRef::new(TEST_USER_ID_SECOND + 1, []),
        granted_collection_id: TEST_COLLECTION_ID,
        denied_collection_id: TEST_DENIED_COLLECTION_ID,
        class_id: TEST_CLASS_ID,
        object_id: TEST_OBJECT_ID,
        task_id: TEST_TASK_ID,
    }
}

fn permission_request(collection_id: i32, permissions: Vec<Permissions>) -> PermissionRequest {
    PermissionRequest {
        resource: ResourceRef::collection(collection_id),
        permissions,
    }
}

fn controlled_container_name() -> String {
    let name = required_fixture_value("HUBUUM_TREETOP_TEST_CONTAINER_NAME");
    assert!(
        name.starts_with(CONTROLLED_CONTAINER_PREFIX),
        "refusing to control a container outside the hermetic fixture namespace"
    );
    name
}

fn docker_fixture_command(action: &str) -> Output {
    let container_name = controlled_container_name();
    Command::new("docker")
        .args([action, &container_name])
        .output()
        .unwrap_or_else(|error| panic!("failed to run docker {action}: {error}"))
}

fn assert_command_succeeded(action: &str, output: &Output) {
    assert!(
        output.status.success(),
        "docker {action} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn wait_for_fixture_policy(backend: &TreetopPermissionBackend) {
    let principal = PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]);
    let request = permission_request(TEST_COLLECTION_ID, vec![Permissions::ReadCollection]);
    let mut last_result = None;
    for _ in 0..120 {
        match backend.authorize(&principal, request.clone()).await {
            Ok(PermissionDecision::Allow) => return,
            result => last_result = Some(result),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("Treetop policy did not recover within 60 seconds: {last_result:?}");
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_fixture_metadata_is_immutable() {
    let _guard = live_test_guard().await;
    assert_eq!(
        LIVE_TEST_REASON,
        "run by scripts/run-treetop-conformance.sh"
    );
    let image = required_fixture_value("HUBUUM_TREETOP_TEST_IMAGE");
    let revision = required_fixture_value("HUBUUM_TREETOP_TEST_REVISION");
    assert!(
        image.contains("@sha256:") && image.split("@sha256:").nth(1).unwrap().len() == 64,
        "the Treetop image must be pinned by SHA-256 digest"
    );
    assert_eq!(revision.len(), 40, "the source revision must be a full SHA");
    assert!(
        revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "the source revision must be hexadecimal"
    );
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_health_check_succeeds() {
    let _guard = live_test_guard().await;
    let backend = live_backend()
        .await
        .expect("connect and health check failed");
    assert_eq!(backend.kind(), "treetop");
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_treetop_satisfies_the_shared_authorization_corpus() {
    let _guard = live_test_guard().await;
    let backend = live_backend().await.expect("connect failed");
    assert_backend_conformance(&backend, ConformanceBackend::Treetop, &fixture()).await;
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_expanded_permission_checks_preserve_order_across_wire_batches() {
    let _guard = live_test_guard().await;
    let backend = live_backend().await.expect("connect failed");
    let principal = PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]);
    let requests = (0..300)
        .map(|index| {
            let collection_id = if index % 2 == 0 {
                TEST_COLLECTION_ID
            } else {
                TEST_DENIED_COLLECTION_ID
            };
            permission_request(
                collection_id,
                vec![Permissions::ReadCollection, Permissions::UpdateCollection],
            )
        })
        .collect::<Vec<_>>();
    let expected = (0..300)
        .map(|index| {
            if index % 2 == 0 {
                PermissionDecision::Allow
            } else {
                PermissionDecision::Deny
            }
        })
        .collect::<Vec<_>>();

    let decisions = backend
        .authorize_many(&principal, requests)
        .await
        .expect("authorize_many failed");

    assert_eq!(decisions, expected);
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_paginated_authorization_counts_all_candidates_but_bounds_the_page() {
    let _guard = live_test_guard().await;
    let backend = live_backend().await.expect("connect failed");
    let principal = PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]);
    let candidates = (0..700).collect::<Vec<_>>();

    let page = paginate_authorized(
        &backend,
        &principal,
        candidates,
        None,
        vec![Permissions::ReadCollection],
        AuthorizationPage::new(50, 11),
        |index| {
            let collection_id = if *index % 3 == 0 {
                TEST_COLLECTION_ID
            } else {
                TEST_DENIED_COLLECTION_ID
            };
            ResourceRef::collection(collection_id)
        },
    )
    .await
    .expect("candidate pagination failed");

    assert_eq!(page.total_count, 234);
    assert_eq!(page.rows, (150..=180).step_by(3).collect::<Vec<_>>());
    assert_eq!(page.rows.len(), 11, "the retained page must remain bounded");
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_collection_reverse_query_reflects_external_policy() {
    let _guard = live_test_guard().await;
    seed_collection_if_missing(TEST_COLLECTION_ID).await;
    seed_collection_if_missing(TEST_DENIED_COLLECTION_ID).await;
    let backend = live_backend().await.expect("connect failed");
    let principal = PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]);

    let visible = backend
        .collections_user_can(&principal, &[Permissions::ReadCollection])
        .await
        .expect("collections_user_can failed");

    assert!(
        visible
            .iter()
            .any(|collection| collection.id == TEST_COLLECTION_ID),
        "the granted collection must be visible"
    );
    assert!(
        visible
            .iter()
            .all(|collection| collection.id != TEST_DENIED_COLLECTION_ID),
        "the ungranted collection must remain hidden"
    );
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_group_permission_query_returns_the_fixture_grant() {
    let _guard = live_test_guard().await;
    let backend = live_backend().await.expect("connect failed");
    let permission = backend
        .group_permission_on(
            CollectionID::new(TEST_COLLECTION_ID).unwrap(),
            GroupID::new(TEST_NORMAL_GROUP_ID).unwrap(),
        )
        .await
        .expect("group_permission_on failed")
        .expect("the fixture must grant at least one permission");

    assert!(permission.has_read_collection);
    assert!(permission.has_read_class);
    assert!(permission.has_read_object);
    assert!(permission.has_execute_remote_target);
    assert!(permission.has_read_audit);
    assert_eq!(permission.collection_id, TEST_COLLECTION_ID);
    assert_eq!(permission.group_id, TEST_NORMAL_GROUP_ID);
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_private_ca_tls_connection_succeeds() {
    let _guard = live_test_guard().await;
    let url = required_fixture_value("HUBUUM_TREETOP_TLS_TEST_URL");
    let mut config = live_config(&url);
    config.treetop_ca_cert = Some(required_fixture_value("HUBUUM_TREETOP_TEST_CA_CERT"));

    let backend = backend_with_config(&config)
        .await
        .expect("private-CA Treetop connection failed");
    assert_eq!(backend.kind(), "treetop");
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_private_ca_tls_connection_fails_without_trust() {
    let _guard = live_test_guard().await;
    let url = required_fixture_value("HUBUUM_TREETOP_TLS_TEST_URL");
    let error = backend_with_config(&live_config(&url))
        .await
        .err()
        .expect("an untrusted private CA must fail closed");
    assert!(matches!(error, ApiError::PermissionBackendUnavailable(_)));
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_missing_ca_file_fails_before_authorization() {
    let _guard = live_test_guard().await;
    let url = required_fixture_value("HUBUUM_TREETOP_TLS_TEST_URL");
    let mut config = live_config(&url);
    config.treetop_ca_cert = Some("/definitely/missing/hubuum-treetop-ca.pem".to_string());
    let error = backend_with_config(&config)
        .await
        .err()
        .expect("missing CA material must fail closed");
    assert!(matches!(error, ApiError::InternalServerError(_)));
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_connection_refusal_fails_closed() {
    let _guard = live_test_guard().await;
    let mut config = live_config("http://127.0.0.1:1");
    config.treetop_connect_timeout_ms = 250;
    config.treetop_request_timeout_ms = 250;

    let error = backend_with_config(&config)
        .await
        .err()
        .expect("connection refusal must fail closed");
    assert!(matches!(error, ApiError::PermissionBackendUnavailable(_)));
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_credential_bearing_url_is_rejected_and_sanitized() {
    let _guard = live_test_guard().await;
    let canary = "treetop-conformance-secret-canary";
    let url = format!("http://canary-user:{canary}@127.0.0.1:1");
    let mut config = live_config(&url);
    config.treetop_connect_timeout_ms = 250;
    config.treetop_request_timeout_ms = 250;

    let error = backend_with_config(&config)
        .await
        .err()
        .expect("credential-bearing URLs must be rejected");
    assert!(matches!(error, ApiError::InternalServerError(_)));
    let rendered = format!("{error:?} {error}");
    assert!(
        !rendered.contains(canary) && !rendered.contains("canary-user"),
        "credential-bearing URLs must not appear in diagnostics: {rendered}"
    );
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_slow_service_times_out_and_fails_closed() {
    let _guard = live_test_guard().await;
    let url = required_fixture_value("HUBUUM_TREETOP_TEST_URL");
    let mut config = live_config(&url);
    config.treetop_request_timeout_ms = 250;
    let backend = backend_with_config(&config).await.expect("connect failed");
    let pause = docker_fixture_command("pause");
    assert_command_succeeded("pause", &pause);

    let result = backend
        .authorize(
            &PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]),
            permission_request(TEST_COLLECTION_ID, vec![Permissions::ReadCollection]),
        )
        .await;

    let unpause = docker_fixture_command("unpause");
    assert_command_succeeded("unpause", &unpause);
    assert!(matches!(
        result,
        Err(ApiError::PermissionBackendUnavailable(_))
    ));
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_existing_client_recovers_after_service_restart() {
    let _guard = live_test_guard().await;
    let backend = live_backend().await.expect("connect failed");
    wait_for_fixture_policy(&backend).await;

    let restart = docker_fixture_command("restart");
    assert_command_succeeded("restart", &restart);

    wait_for_fixture_policy(&backend).await;
}

#[actix_test]
#[ignore = "run by scripts/run-treetop-conformance.sh"]
async fn live_in_flight_termination_fails_closed_and_service_recovers() {
    let _guard = live_test_guard().await;
    let backend = Arc::new(live_backend().await.expect("connect failed"));
    let pause = docker_fixture_command("pause");
    assert_command_succeeded("pause", &pause);

    let pending_backend = Arc::clone(&backend);
    let pending = tokio::spawn(async move {
        let repeated = permission_request(TEST_COLLECTION_ID, vec![Permissions::ReadCollection]);
        pending_backend
            .authorize_many(
                &PrincipalRef::new(TEST_USER_ID, [TEST_NORMAL_GROUP_ID]),
                vec![repeated; 512],
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let kill = docker_fixture_command("kill");
    assert_command_succeeded("kill", &kill);
    let result = pending.await.expect("authorization task panicked");

    let start = docker_fixture_command("start");
    assert_command_succeeded("start", &start);
    wait_for_fixture_policy(&backend).await;

    assert!(matches!(
        result,
        Err(ApiError::PermissionBackendUnavailable(_))
    ));
}

async fn seed_collection_if_missing(collection_id: i32) {
    use crate::db::prelude::*;
    use crate::db::with_connection;
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};
    use crate::schema::collections::{description, name};
    use diesel::dsl::exists;
    use diesel::result::Error as DieselError;
    use diesel::{insert_into, select};

    let pool = get_test_pool();
    let exists: bool = with_connection(&pool, async |connection| {
        select(exists(collections.filter(id.eq(collection_id))))
            .get_result(connection)
            .await
    })
    .await
    .expect("collections existence check failed");

    if !exists {
        let root_collection_id: i32 = with_connection(&pool, async |connection| {
            collections
                .filter(parent_collection_id.is_null())
                .select(id)
                .first(connection)
                .await
        })
        .await
        .expect("root collection lookup failed");

        with_connection(&pool, async |connection| -> Result<usize, DieselError> {
            insert_into(collections)
                .values((
                    id.eq(collection_id),
                    name.eq(format!("treetop_conformance_collection_{collection_id}")),
                    description.eq("hermetic Treetop conformance fixture"),
                    parent_collection_id.eq(root_collection_id),
                ))
                .execute(connection)
                .await
        })
        .await
        .expect("collection insert failed");
    }
}
