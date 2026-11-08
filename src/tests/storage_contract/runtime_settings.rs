//! Runtime settings through the administrator API and replica application.
//!
//! Each test owns an in-memory store so shared settings never leak between
//! parallel tests. PostgreSQL transaction, notification, migration, and
//! concurrency behavior is covered by the adapter's isolated-schema tests.
use super::*;
use crate::config::running::RunningConfig;
use crate::pagination::{deployment_page_limits, page_limits};
use crate::tests::integration_test_config;
use actix_web::{HttpResponse, middleware::from_fn, web};
use hubuum_query::parse_query_parameter;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageAuthenticationTokenScope, StoragePaginationPolicy,
    StorageQueryObservationPolicy, StorageQueryObservationSettings, StorageQueryUsageScope,
    StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup,
    StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsUpdate,
    StorageRuntimeSettingsUpdateOutcome, StorageRuntimeSettingsValue,
};
use serde_json::{Value, json};

fn replica(storage: &MemoryStorage, enabled: bool) -> StorageHandle {
    StorageHandle::from_registered_backend(storage.clone()).with_query_observations(
        StorageQueryObservationSettings::try_new(enabled, 1, 64, 64, 60, 16).unwrap(),
    )
}

fn context(backend: &StorageHandle) -> Data<AppContext> {
    Data::new(AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(
            backend.clone(),
            "admin".to_owned(),
        )),
    ))
}

fn running_config() -> Data<RunningConfig> {
    Data::new(RunningConfig::from(&integration_test_config().unwrap()))
}

async fn admin(backend: &StorageHandle) -> BackendUserFixture {
    let user = create_backend_user(backend, &prefix("settings_admin")).await;
    let _ = backend
        .add_group_member(user.principal_id, group_id(1), &EventContext::system())
        .await
        .unwrap();
    user
}

async fn send(
    backend: &StorageHandle,
    token: Option<&str>,
    request: test::TestRequest,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .app_data(context(backend))
            .app_data(running_config())
            .configure(crate::api::config),
    )
    .await;
    let request = match token {
        Some(token) => {
            request.insert_header((http::header::AUTHORIZATION, format!("Bearer {token}")))
        }
        None => request,
    };
    request.send_request(&app).await
}

async fn send_json(
    backend: &StorageHandle,
    token: &str,
    request: test::TestRequest,
) -> (http::StatusCode, Value) {
    let response = send(backend, Some(token), request).await;
    let status = response.status();
    (status, test::read_body_json(response).await)
}

fn put(path: &str, body: Value) -> test::TestRequest {
    test::TestRequest::put().uri(path).set_json(body)
}

fn revision(value: i64) -> ResourceRevision {
    ResourceRevision::new(value).unwrap()
}

async fn current_revision(
    backend: &StorageHandle,
    group: StorageRuntimeSettingsGroup,
) -> ResourceRevision {
    backend
        .load_runtime_settings()
        .await
        .unwrap()
        .record(group)
        .revision()
}

async fn history_len(backend: &StorageHandle, group: StorageRuntimeSettingsGroup) -> usize {
    backend
        .list_runtime_settings_changes(
            StorageRuntimeSettingsHistoryQuery::try_new(group, None, 100).unwrap(),
        )
        .await
        .unwrap()
        .len()
}

fn pagination(limit: u64) -> StorageRuntimeSettingsAssignment {
    StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
        StoragePaginationPolicy::try_new(limit).unwrap(),
    ))
}

async fn commit(
    backend: &StorageHandle,
    assignment: StorageRuntimeSettingsAssignment,
) -> ResourceRevision {
    let expected = current_revision(backend, assignment.group()).await;
    backend
        .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
            assignment,
            expected,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .record()
        .revision()
}

async fn scoped_admin_token(backend: &StorageHandle) -> String {
    let user = admin(backend).await;
    let raw = prefix("scoped_settings_token");
    let _ = backend
        .create_token(
            StorageTokenCreate::new(
                user.principal_id,
                StorageTokenDigest::legacy_unidentified(
                    crate::models::Token::storage_hash_from_raw(&raw),
                ),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                EventContext::system(),
            )
            .scope(Some(StorageAuthenticationTokenScope::new(
                Some(vec![StorageAuthorizationPermission::ReadClass]),
                None,
            ))),
        )
        .await
        .unwrap();
    raw
}

#[rstest::rstest]
#[actix_web::test]
async fn runtime_settings_require_an_unscoped_administrator(
    #[values(
        ("GET", "/api/v1/admin/runtime-settings"),
        ("POST", "/api/v1/admin/runtime-settings/authentication/preflight"),
        ("PUT", "/api/v1/admin/runtime-settings/authorization"),
        ("PUT", "/api/v1/admin/runtime-settings/pagination"),
        ("PUT", "/api/v1/admin/runtime-settings/query_observations"),
        ("DELETE", "/api/v1/admin/runtime-settings/authentication?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/authorization?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/pagination?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/query_observations?expected_revision=1"),
        ("GET", "/api/v1/admin/runtime-settings/history/authentication"),
        ("GET", "/api/v1/admin/runtime-settings/history/authorization"),
        ("GET", "/api/v1/admin/runtime-settings/history/pagination")
    )]
    endpoint: (&str, &str),
    #[values(("anonymous", 401), ("user", 403), ("scoped_admin", 403))] identity: (&str, u16),
) {
    let backend = StorageHandle::memory();
    let token = match identity.0 {
        "anonymous" => None,
        "user" => Some(
            create_backend_user(&backend, &prefix("settings_user"))
                .await
                .raw_token,
        ),
        _ => Some(scoped_admin_token(&backend).await),
    };
    let request = test::TestRequest::default()
        .method(endpoint.0.parse().unwrap())
        .uri(endpoint.1)
        .set_json(json!({"expected_revision": 1, "enabled": true, "default_page_limit": 5}));
    let response = send(&backend, token.as_deref(), request).await;
    let revisions = backend
        .load_runtime_settings()
        .await
        .unwrap()
        .records()
        .iter()
        .map(|record| record.revision())
        .collect::<Vec<_>>();
    assert_eq!(
        (response.status().as_u16(), revisions),
        (identity.1, vec![ResourceRevision::INITIAL; 4])
    );
}

#[cfg(not(feature = "permissions-treetop"))]
#[actix_web::test]
async fn unsupported_authorization_backend_is_not_committed() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        put(
            "/api/v1/admin/runtime-settings/authorization",
            json!({"expected_revision": 1, "backend": "treetop"}),
        ),
    )
    .await;

    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(
        current_revision(&backend, StorageRuntimeSettingsGroup::Authorization).await,
        ResourceRevision::INITIAL
    );
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Authorization).await,
        0
    );
}

#[cfg(not(feature = "permissions-treetop"))]
#[actix_web::test]
async fn stale_authorization_update_is_rejected_before_backend_preflight() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        put(
            "/api/v1/admin/runtime-settings/authorization",
            json!({"expected_revision": 2, "backend": "treetop"}),
        ),
    )
    .await;

    assert_eq!(response.status(), http::StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Authorization).await,
        0
    );
}

#[actix_web::test]
async fn supported_authorization_backend_is_committed() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        put(
            "/api/v1/admin/runtime-settings/authorization",
            json!({"expected_revision": 1, "backend": "local"}),
        ),
    )
    .await;

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        current_revision(&backend, StorageRuntimeSettingsGroup::Authorization).await,
        revision(2)
    );
}

#[actix_web::test]
async fn runtime_settings_are_revisioned_and_audited() {
    let backend = StorageHandle::memory();
    let initial = backend.load_runtime_settings().await.unwrap();
    let committed = backend
        .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
            pagination(8),
            ResourceRevision::INITIAL,
            EventContext::user(principal_id(7), None, None),
        ))
        .await
        .unwrap();
    let changes = backend
        .list_runtime_settings_changes(
            StorageRuntimeSettingsHistoryQuery::try_new(
                StorageRuntimeSettingsGroup::Pagination,
                None,
                10,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let current = backend.load_runtime_settings().await.unwrap();
    assert_eq!(
        (
            StorageRuntimeSettingsGroup::ALL
                .map(|group| initial.record(group).assignment().clone()),
            committed.is_committed(),
            current
                .record(StorageRuntimeSettingsGroup::Pagination)
                .revision(),
            current.pagination(),
            current
                .record(StorageRuntimeSettingsGroup::QueryObservations)
                .revision(),
            changes
                .iter()
                .map(|change| (
                    change.revision(),
                    change.previous().clone(),
                    change.current().clone(),
                    change.actor()
                ))
                .collect::<Vec<_>>(),
        ),
        (
            StorageRuntimeSettingsGroup::ALL.map(StorageRuntimeSettingsAssignment::Deployment),
            true,
            revision(2),
            Some(StoragePaginationPolicy::try_new(8).unwrap()),
            ResourceRevision::INITIAL,
            vec![(
                revision(2),
                StorageRuntimeSettingsAssignment::Deployment(
                    StorageRuntimeSettingsGroup::Pagination
                ),
                pagination(8),
                Some(principal_id(7)),
            )],
        )
    );
}

fn above_ceiling() -> Value {
    json!(deployment_page_limits().unwrap().maximum_limit() + 1)
}

#[rstest::rstest]
#[case::empty("pagination", json!({}))]
#[case::missing_revision("pagination", json!({"default_page_limit": 5}))]
#[case::non_positive_revision("pagination", json!({"expected_revision": 0, "default_page_limit": 5}))]
#[case::zero_limit("pagination", json!({"expected_revision": 1, "default_page_limit": 0}))]
#[case::negative_limit("pagination", json!({"expected_revision": 1, "default_page_limit": -1}))]
#[case::above_ceiling("pagination", json!({"expected_revision": 1, "default_page_limit": above_ceiling()}))]
#[case::string_limit("pagination", json!({"expected_revision": 1, "default_page_limit": "5"}))]
#[case::unknown_field("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "max_page_limit": 9}))]
#[case::observation_missing_value("query_observations", json!({"expected_revision": 1}))]
#[case::observation_null("query_observations", json!({"expected_revision": 1, "enabled": null}))]
#[case::observation_string("query_observations", json!({"expected_revision": 1, "enabled": "true"}))]
#[case::observation_invalid_sample_interval("query_observations", json!({"expected_revision": 1, "enabled": true, "sample_every": 0}))]
#[case::observation_unknown_field("query_observations", json!({"expected_revision": 1, "enabled": true, "unknown": 2}))]
#[actix_web::test]
async fn invalid_updates_are_rejected_before_persistence(#[case] group: &str, #[case] body: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        put(&format!("/api/v1/admin/runtime-settings/{group}"), body),
    )
    .await;
    assert_eq!(
        (
            response.status(),
            history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await
                + history_len(&backend, StorageRuntimeSettingsGroup::QueryObservations).await
        ),
        (http::StatusCode::BAD_REQUEST, 0)
    );
}

#[rstest::rstest]
#[case::put(put(
    "/api/v1/admin/runtime-settings/pagination",
    json!({"expected_revision": 1, "default_page_limit": 9})
))]
#[case::release(test::TestRequest::delete().uri("/api/v1/admin/runtime-settings/pagination?expected_revision=1"))]
#[actix_web::test]
async fn stale_revisions_are_rejected_with_the_current_revision(
    #[case] request: test::TestRequest,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination(7)).await;
    let (status, body) = send_json(&backend, &user.raw_token, request).await;
    assert_eq!(
        (
            status,
            body["current_revision"].as_i64(),
            backend.load_runtime_settings().await.unwrap().pagination()
        ),
        (
            http::StatusCode::PRECONDITION_FAILED,
            Some(2),
            Some(StoragePaginationPolicy::try_new(7).unwrap())
        )
    );
}

#[actix_web::test]
async fn committed_updates_are_audited_with_actor_and_values() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 1, "default_page_limit": 7}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let (_, history) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/runtime-settings/history/pagination"),
    )
    .await;
    let change = &history["changes"][0];
    assert_eq!(
        (
            history["changes"].as_array().map(Vec::len),
            &change["revision"],
            &change["previous"],
            &change["current"],
            &change["actor"],
        ),
        (
            Some(1),
            &json!(2),
            &json!({"source": "deployment", "value": null}),
            &json!({"source": "runtime", "value": {"default_page_limit": 7}}),
            &json!({"kind": "user", "principal_id": user.principal_id.id()}),
        )
    );
}

#[actix_web::test]
async fn committed_update_reports_commit_and_responding_replica_application() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (_, body) = send_json(
        &backend,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/query_observations",
            json!({"expected_revision": 1, "enabled": true}),
        ),
    )
    .await;
    let group = &body["settings"]["query_observations"];
    assert_eq!(
        (
            &body["committed_revision"],
            &body["changed"],
            &body["applied_by_responding_replica"],
            &group["source"],
            &group["revision"],
            &group["effective"],
            &body["settings"]["replica"]["status"],
        ),
        (
            &json!(2),
            &json!(true),
            &json!(true),
            &json!("runtime"),
            &json!(2),
            &json!({
                "enabled": true,
                "sample_every": 16,
                "max_patterns": 2048,
                "max_patterns_per_class": 64,
                "retention_seconds": 86400,
                "max_predicates_per_query": 16
            }),
            &json!("current"),
        )
    );
}

#[actix_web::test]
async fn query_observation_limits_are_clamped_and_reported_per_replica() {
    let storage = MemoryStorage::new();
    let writer = StorageHandle::from_registered_backend(storage.clone())
        .with_query_observations(StorageQueryObservationSettings::default());
    let user = admin(&writer).await;
    let (status, _) = send_json(
        &writer,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/query_observations",
            json!({
                "expected_revision": 1,
                "enabled": true,
                "sample_every": 1,
                "max_patterns": 256,
                "max_patterns_per_class": 128,
                "retention_seconds": 1200,
                "max_predicates_per_query": 20
            }),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);

    let ceilings =
        hubuum_storage_core::StorageQueryObservationCeilings::try_new(4, 64, 32, 300, 8).unwrap();
    let deployment =
        StorageQueryObservationSettings::try_new_with_ceilings(false, 4, 32, 16, 300, 8, ceilings)
            .unwrap();
    let reader =
        StorageHandle::from_registered_backend(storage).with_query_observations(deployment);
    reader.refresh_runtime_settings().await.unwrap();
    let (status, body) = send_json(
        &reader,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/runtime-settings"),
    )
    .await;
    let reported = &body["query_observations"];
    assert_eq!(
        (
            status,
            &reported["runtime"],
            &reported["effective"],
            &reported["constrained_by_ceiling"],
            &reported["ceilings"],
        ),
        (
            http::StatusCode::OK,
            &json!({
                "enabled": true,
                "sample_every": 1,
                "max_patterns": 256,
                "max_patterns_per_class": 128,
                "retention_seconds": 1200,
                "max_predicates_per_query": 20
            }),
            &json!({
                "enabled": true,
                "sample_every": 4,
                "max_patterns": 64,
                "max_patterns_per_class": 32,
                "retention_seconds": 300,
                "max_predicates_per_query": 8
            }),
            &json!(true),
            &json!({
                "min_sample_every": 4,
                "max_patterns": 64,
                "max_patterns_per_class": 32,
                "max_retention_seconds": 300,
                "max_predicates_per_query": 8
            }),
        )
    );
}

#[actix_web::test]
async fn failed_authentication_preflight_does_not_persist_the_candidate() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::post()
            .uri("/api/v1/admin/runtime-settings/authentication/preflight")
            .set_json(json!({
                "expected_revision": 1,
                "providers": [{
                    "name": "corporate",
                    "kind": "ldap",
                    "configuration": {
                        "scope": "corporate",
                        "url": "http://ldap.example.org",
                        "user_base_dn": "ou=people,dc=example,dc=org",
                        "user_filter": "(uid={username})"
                    }
                }]
            })),
    )
    .await;

    let snapshot = backend.load_runtime_settings().await.unwrap();
    let record = snapshot.record(StorageRuntimeSettingsGroup::Authentication);
    assert_eq!(
        (status, record.revision(), record.assignment()),
        (
            http::StatusCode::BAD_REQUEST,
            ResourceRevision::INITIAL,
            &StorageRuntimeSettingsAssignment::Deployment(
                StorageRuntimeSettingsGroup::Authentication
            )
        )
    );
}

#[rstest::rstest]
#[case::top_level(json!({"group_filter": "admins"}))]
#[case::nested(json!({
    "group_rules": [{"pattern": "admins", "name": "admins", "unexpected": true}]
}))]
#[actix_web::test]
async fn authentication_preflight_does_not_persist_unknown_fields(#[case] extra: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let mut configuration = json!({
        "scope": "corporate",
        "url": "ldap://ldap.example.org",
        "user_base_dn": "ou=people,dc=example,dc=org",
        "user_filter": "(uid={username})"
    });
    configuration
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::post()
            .uri("/api/v1/admin/runtime-settings/authentication/preflight")
            .set_json(json!({
                "expected_revision": 1,
                "providers": [{
                    "name": "corporate",
                    "kind": "ldap",
                    "configuration": configuration
                }]
            })),
    )
    .await;

    let snapshot = backend.load_runtime_settings().await.unwrap();
    let record = snapshot.record(StorageRuntimeSettingsGroup::Authentication);
    assert_eq!(
        (status, record.revision(), record.assignment()),
        (
            http::StatusCode::BAD_REQUEST,
            ResourceRevision::INITIAL,
            &StorageRuntimeSettingsAssignment::Deployment(
                StorageRuntimeSettingsGroup::Authentication
            )
        )
    );
}

#[actix_web::test]
async fn unchanged_update_keeps_the_revision_and_writes_no_audit() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination(7)).await;
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 2, "default_page_limit": 7}),
        ),
    )
    .await;
    assert_eq!(
        (
            status,
            &body["changed"],
            &body["committed_revision"],
            history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await
        ),
        (http::StatusCode::OK, &json!(false), &json!(2), 1)
    );
}

#[actix_web::test]
async fn releasing_returns_ownership_to_deployment_as_a_new_revision() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination(7)).await;
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::delete()
            .uri("/api/v1/admin/runtime-settings/pagination?expected_revision=2"),
    )
    .await;
    let group = &body["settings"]["pagination"];
    assert_eq!(
        (
            status,
            &body["committed_revision"],
            &group["source"],
            &group["runtime"],
            group["effective"]["default_page_limit"].as_u64(),
            history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await,
        ),
        (
            http::StatusCode::OK,
            &json!(3),
            &json!("deployment"),
            &Value::Null,
            Some(deployment_page_limits().unwrap().default_limit() as u64),
            2,
        )
    );
}

#[actix_web::test]
async fn concurrent_edits_of_one_revision_commit_exactly_once() {
    let backend = StorageHandle::memory();
    let update = |limit| {
        backend.update_runtime_settings(StorageRuntimeSettingsUpdate::new(
            pagination(limit),
            ResourceRevision::INITIAL,
            EventContext::system(),
        ))
    };
    let (first, second) = tokio::join!(update(5), update(6));
    let outcomes = [first, second];
    assert_eq!(
        (
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    Ok(StorageRuntimeSettingsUpdateOutcome::Committed(_))
                ))
                .count(),
            outcomes
                .iter()
                .filter_map(|outcome| outcome.as_ref().err())
                .map(|error| (error.kind(), error.current_revision()))
                .collect::<Vec<_>>(),
            history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await,
        ),
        (
            1,
            vec![(StorageErrorKind::RevisionConflict, Some(revision(2)))],
            1
        )
    );
}

#[actix_web::test]
async fn history_is_paged_newest_first() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    for limit in [3, 4, 5] {
        commit(&backend, pagination(limit)).await;
    }
    let revisions = |body: &Value| {
        body["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|change| change["revision"].as_i64().unwrap())
            .collect::<Vec<_>>()
    };
    let (_, first) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/runtime-settings/history/pagination?limit=2"),
    )
    .await;
    let (_, second) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri(&format!(
            "/api/v1/admin/runtime-settings/history/pagination?limit=2&before_revision={}",
            first["next_before_revision"]
        )),
    )
    .await;
    assert_eq!(
        (
            revisions(&first),
            revisions(&second),
            &second["next_before_revision"]
        ),
        (vec![4, 3], vec![2], &Value::Null)
    );
}

#[rstest::rstest]
#[case::zero("limit=0")]
#[case::too_large("limit=101")]
#[case::invalid_revision("before_revision=0")]
#[case::unknown_parameter("offset=1")]
#[actix_web::test]
async fn invalid_history_parameters_are_rejected(#[case] query: &str) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::get().uri(&format!(
            "/api/v1/admin/runtime-settings/history/pagination?{query}"
        )),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
}

async fn page_limit_header(backend: &StorageHandle, token: &str) -> Option<String> {
    let response = send(
        backend,
        Some(token),
        test::TestRequest::get().uri("/api/v1/collections"),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    response
        .headers()
        .get(crate::pagination::PAGE_LIMIT_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

#[actix_web::test]
async fn pagination_default_applies_to_the_next_request_on_each_replica() {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let reader = replica(&storage, false);
    let user = admin(&writer).await;
    let deployment = deployment_page_limits()
        .unwrap()
        .default_limit()
        .to_string();
    let (status, _) = send_json(
        &writer,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 1, "default_page_limit": 2}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let writer_limit = page_limit_header(&writer, &user.raw_token).await;
    // The reader has not reconciled yet, so its deployment value still applies.
    let reader_before = page_limit_header(&reader, &user.raw_token).await;
    reader.refresh_runtime_settings().await.unwrap();
    let reader_after = page_limit_header(&reader, &user.raw_token).await;
    assert_eq!(
        (writer_limit, reader_before, reader_after),
        (Some("2".into()), Some(deployment), Some("2".into()))
    );
}

#[actix_web::test]
async fn a_request_keeps_the_page_limits_it_started_with() {
    let backend = StorageHandle::memory();
    backend.refresh_runtime_settings().await.unwrap();
    let app = test::init_service(
        App::new().app_data(context(&backend)).service(
            web::scope("/probe")
                .wrap(from_fn(crate::middlewares::apply_runtime_settings))
                .route(
                    "",
                    web::get().to(|context: Data<AppContext>| async move {
                        let before = page_limits().unwrap().default_limit();
                        let storage = context.backend();
                        commit(storage, pagination(3)).await;
                        storage.refresh_runtime_settings().await.unwrap();
                        let after = page_limits().unwrap().default_limit();
                        HttpResponse::Ok().json(json!([before, after]))
                    }),
                ),
        ),
    )
    .await;
    let in_flight: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri("/probe").to_request())
            .await;
    let deployment = deployment_page_limits().unwrap().default_limit();
    assert_eq!(in_flight, json!([deployment, deployment]));
}

#[actix_web::test]
async fn client_configuration_reports_the_applied_default() {
    let backend = StorageHandle::memory();
    commit(&backend, pagination(4)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let response = send(
        &backend,
        None,
        test::TestRequest::get().uri("/api/v1/config"),
    )
    .await;
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["pagination"]["default_page_limit"], 4);
}

#[actix_web::test]
async fn admin_configuration_reports_effective_runtime_settings() {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let user = admin(&writer).await;
    commit(&writer, pagination(6)).await;
    let reader = replica(&storage, false);
    let (_, body) = send_json(
        &reader,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/config"),
    )
    .await;
    let settings = &body["runtime_settings"];
    assert_eq!(
        (
            &body["pagination"]["default_page_limit"],
            &settings["pagination"]["source"],
            &settings["pagination"]["revision"],
            &settings["pagination"]["mutable"],
            &settings["pagination"]["restart_required"],
            &settings["pagination"]["takes_effect"],
            &settings["query_observations"]["source"],
            &settings["replica"]["status"],
        ),
        (
            &json!(6),
            &json!("runtime"),
            &json!(2),
            &json!(true),
            &json!(false),
            &json!("next_request"),
            &json!("deployment"),
            &json!("current"),
        )
    );
}

#[rstest::rstest]
#[case::enable(true)]
#[case::disable(false)]
#[actix_web::test]
async fn toggle_changes_collection_on_another_replica(#[case] enabled: bool) {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, !enabled);
    let reader = replica(&storage, !enabled);
    let user = admin(&writer).await;
    let fixture = create_backend_object_fixture(&writer, &prefix("toggle"), vec![]).await;
    let query = parse_query_parameter(&format!(
        "class_id={}&json_data=serial=sample",
        fixture.class.id()
    ))
    .unwrap();
    reader.refresh_runtime_settings().await.unwrap();
    reader
        .observe_query(&query, None, async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    let response = send(
        &writer,
        Some(&user.raw_token),
        put(
            "/api/v1/admin/runtime-settings/query_observations",
            json!({"expected_revision": 1, "enabled": enabled}),
        ),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    // A notification or reconcile poll applies the committed change.
    reader.refresh_runtime_settings().await.unwrap();
    reader
        .observe_query(&query, None, async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
    let report =
        serde_json::to_value(reader.analyze_query_usage(scope, vec![]).await.unwrap()).unwrap();
    // Exactly one operation is collected, whether enabled first or enabled last.
    assert_eq!(report["observations"]["patterns"][0]["sampled_queries"], 1);
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[actix_web::test]
async fn admin_configuration_reports_shared_enablement_on_a_new_replica(#[case] enabled: bool) {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let user = admin(&writer).await;
    commit(
        &writer,
        StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::QueryObservations(
            StorageQueryObservationPolicy::new(enabled),
        )),
    )
    .await;
    let reader = replica(&storage, !enabled);
    let (status, body) = send_json(
        &reader,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/config"),
    )
    .await;
    let reported = &body["runtime_settings"]["query_observations"];
    assert_eq!(
        (
            status,
            &body["query_observations"]["enabled"],
            &reported["source"],
            &reported["runtime"],
            &reported["deployment"],
        ),
        (
            http::StatusCode::OK,
            &json!(enabled),
            &json!("runtime"),
            &json!({
                "enabled": enabled,
                "sample_every": 1,
                "max_patterns": 64,
                "max_patterns_per_class": 64,
                "retention_seconds": 60,
                "max_predicates_per_query": 16
            }),
            &json!({
                "enabled": !enabled,
                "sample_every": 1,
                "max_patterns": 64,
                "max_patterns_per_class": 64,
                "retention_seconds": 60,
                "max_predicates_per_query": 16
            }),
        )
    );
}
