//! Runtime settings through the administrator API and replica application.
//!
//! Each test owns an in-memory store so shared settings never leak between
//! parallel tests. PostgreSQL transaction, notification, migration, and
//! concurrency behavior is covered by the adapter's isolated-schema tests.
use super::*;
use crate::config::running::RunningConfig;
use crate::pagination::{deployment_page_limits, page_limits};
use crate::tests::integration_test_config;
use actix_web::{HttpRequest, HttpResponse, middleware::from_fn, web};
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

fn pagination_limits(default: u64, maximum: u64) -> StorageRuntimeSettingsAssignment {
    StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
        StoragePaginationPolicy::try_with_maximum(default, maximum).unwrap(),
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
        ("GET", "/api/v1/admin/runtime-settings/schema"),
        ("GET", "/api/v1/admin/runtime-settings/pagination"),
        ("PATCH", "/api/v1/admin/runtime-settings/pagination"),
        ("POST", "/api/v1/admin/runtime-settings/authentication/preflight"),
        ("PUT", "/api/v1/admin/runtime-settings/authorization"),
        ("PUT", "/api/v1/admin/runtime-settings/pagination"),
        ("PUT", "/api/v1/admin/runtime-settings/query_observations"),
        ("DELETE", "/api/v1/admin/runtime-settings/authentication?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/authorization?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/pagination?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/query_observations?expected_revision=1"),
        ("DELETE", "/api/v1/admin/runtime-settings/exports?expected_revision=1"),
        ("PATCH", "/api/v1/admin/runtime-settings/exports"),
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
        .set_json(if endpoint.0 == "PATCH" {
            json!({"expected_revision": 1, "changes": {"default_page_limit": 5}})
        } else {
            json!({"expected_revision": 1, "enabled": true, "default_page_limit": 5})
        });
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
        (
            identity.1,
            vec![ResourceRevision::INITIAL; StorageRuntimeSettingsGroup::ALL.len()]
        )
    );
}

#[rstest::rstest]
#[case::select(false)]
#[case::release(true)]
#[actix_web::test]
async fn authorization_change_cannot_remove_the_callers_administrator_access(
    #[case] release: bool,
) {
    let backend = StorageHandle::memory();
    let user = create_backend_user(&backend, &prefix("previous_admin")).await;
    let group_name = prefix("previous_admin_group");
    let group = backend
        .create_group(
            StorageGroupCreate::new(None, &group_name, None),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    backend
        .add_group_member(user.principal_id, group.id(), &EventContext::system())
        .await
        .unwrap()
        .into_value();
    let context = AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(backend.clone(), group_name)),
    );
    let assignment = if release {
        StorageRuntimeSettingsAssignment::Deployment(StorageRuntimeSettingsGroup::Authorization)
    } else {
        StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authorization(
            hubuum_storage_core::StorageAuthorizationBackend::Local,
        ))
    };
    let result = crate::services::runtime_settings::update(
        &context,
        StorageRuntimeSettingsUpdate::new(
            assignment,
            ResourceRevision::INITIAL,
            EventContext::user(user.principal_id, None, None),
        ),
    )
    .await;
    assert!(
        matches!(result, Err(ApiError::Forbidden(message)) if message.contains("local recovery grants"))
    );
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Authorization).await,
        0
    );
}

#[rstest::rstest]
#[case::authentication(StorageRuntimeSettingsGroup::Authentication)]
#[case::authorization(StorageRuntimeSettingsGroup::Authorization)]
#[actix_web::test]
async fn external_administrators_cannot_change_security_settings(
    #[case] group: StorageRuntimeSettingsGroup,
) {
    let backend = StorageHandle::memory();
    let user = backend
        .sync_external_user(
            hubuum_storage_core::StorageExternalUserSync::builder(
                "corporate",
                "ldap",
                "subject",
                "alice",
            )
            .build(),
        )
        .await
        .unwrap()
        .into_value();
    let actor = principal_id(user.id().id());
    backend
        .add_group_member(actor, group_id(1), &EventContext::system())
        .await
        .unwrap()
        .into_value();
    let context = context(&backend);
    let result = crate::services::runtime_settings::update(
        &context,
        StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Deployment(group),
            ResourceRevision::INITIAL,
            EventContext::user(actor, None, None),
        ),
    )
    .await;
    assert!(matches!(result, Err(ApiError::Forbidden(_))));
    assert_eq!(history_len(&backend, group).await, 0);
}

#[actix_web::test]
async fn local_authentication_cannot_be_disabled_through_the_api() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::post()
            .uri("/api/v1/admin/runtime-settings/security-candidates")
            .set_json(
                json!({"group": "authentication", "expected_revision": 1, "providers": [
                    {"source": "deployment", "name": "local", "kind": "ldap", "enabled": false}
                ]}),
            ),
    )
    .await;
    assert_eq!(
        (
            status,
            history_len(&backend, StorageRuntimeSettingsGroup::Authentication).await
        ),
        (http::StatusCode::BAD_REQUEST, 0)
    );
}

#[rstest::rstest]
#[case::simple("corporate")]
#[case::dotted("corp.example.org")]
#[case::unicode("Bedrift Øst")]
#[actix_web::test]
async fn disabled_deployment_provider_can_be_staged_without_connectivity(#[case] name: &str) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, candidate) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::post()
            .uri("/api/v1/admin/runtime-settings/security-candidates")
            .set_json(
                json!({"group": "authentication", "expected_revision": 1, "providers": [
                    {"source": "deployment", "name": name, "kind": "ldap", "enabled": false}
                ]}),
            ),
    )
    .await;
    assert_eq!(status, http::StatusCode::CREATED, "{candidate}");
    assert_eq!(candidate["state"], "verified");
    assert_eq!(candidate["providers"][0]["name"], name);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Authentication).await,
        0
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
#[case::unknown_field("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "unknown": 9}))]
#[case::zero_maximum("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "max_page_limit": 0}))]
#[case::negative_maximum("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "max_page_limit": -1}))]
#[case::maximum_above_ceiling("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "max_page_limit": above_ceiling()}))]
#[case::default_above_maximum("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "max_page_limit": 4}))]
#[case::string_maximum("pagination", json!({"expected_revision": 1, "default_page_limit": 5, "max_page_limit": "9"}))]
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

#[rstest::rstest]
#[case::legacy(json!({"default_page_limit": 7}))]
#[case::with_maximum(json!({"default_page_limit": 7, "max_page_limit": 12}))]
#[actix_web::test]
async fn committed_updates_are_audited_with_actor_and_values(#[case] values: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let mut request = values.clone();
    request["expected_revision"] = json!(1);
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        put("/api/v1/admin/runtime-settings/pagination", request),
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
            &json!({"source": "runtime", "value": values}),
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
async fn invalid_authentication_candidate_does_not_change_live_settings() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::post()
            .uri("/api/v1/admin/runtime-settings/security-candidates")
            .set_json(json!({
                "group": "authentication", "expected_revision": 1,
                "providers": [{
                    "name": "corporate",
                    "kind": "ldap",
                    "source": "runtime",
                    "enabled": true,
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
async fn authentication_candidate_rejects_unknown_fields(
    #[case] extra: Value,
    #[values(true, false)] enabled: bool,
) {
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
            .uri("/api/v1/admin/runtime-settings/security-candidates")
            .set_json(json!({
                "group": "authentication", "expected_revision": 1,
                "providers": [{
                    "name": "corporate",
                    "kind": "ldap",
                    "source": "runtime",
                    "enabled": enabled,
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

async fn page_limit_header(backend: &StorageHandle, token: &str, query: &str) -> Option<String> {
    let response = send(
        backend,
        Some(token),
        test::TestRequest::get().uri(&format!("/api/v1/collections{query}")),
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
    let writer_limit = page_limit_header(&writer, &user.raw_token, "").await;
    // The reader has not reconciled yet, so its deployment value still applies.
    let reader_before = page_limit_header(&reader, &user.raw_token, "").await;
    reader.refresh_runtime_settings().await.unwrap();
    let reader_after = page_limit_header(&reader, &user.raw_token, "").await;
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
                        let before = page_limits().unwrap();
                        let storage = context.backend();
                        commit(storage, pagination_limits(3, 5)).await;
                        storage.refresh_runtime_settings().await.unwrap();
                        let after = page_limits().unwrap();
                        HttpResponse::Ok().json(json!([
                            [before.default_limit(), before.maximum_limit()],
                            [after.default_limit(), after.maximum_limit()]
                        ]))
                    }),
                ),
        ),
    )
    .await;
    let in_flight: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri("/probe").to_request())
            .await;
    let deployment = deployment_page_limits().unwrap();
    let limits = [deployment.default_limit(), deployment.maximum_limit()];
    assert_eq!(in_flight, json!([limits, limits]));
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

#[rstest::rstest]
#[case::client("/api/v1/config")]
#[case::admin("/api/v1/admin/config")]
#[actix_web::test]
async fn configuration_reports_the_effective_runtime_maximum(#[case] path: &str) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 8)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri(path),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(body["pagination"]["max_page_limit"], 8);
}

#[actix_web::test]
async fn runtime_maximum_clamps_explicit_requests_after_replica_refresh() {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let reader = replica(&storage, false);
    reader.refresh_runtime_settings().await.unwrap();
    let user = admin(&writer).await;
    let (status, _) = send_json(
        &writer,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 1, "default_page_limit": 2, "max_page_limit": 3}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let writer_limit = page_limit_header(&writer, &user.raw_token, "?limit=9").await;
    let reader_before = page_limit_header(&reader, &user.raw_token, "?limit=9").await;
    reader.refresh_runtime_settings().await.unwrap();
    let reader_after = page_limit_header(&reader, &user.raw_token, "?limit=9").await;
    assert_eq!(
        (writer_limit, reader_before, reader_after),
        (Some("3".into()), Some("9".into()), Some("3".into()))
    );
}

#[actix_web::test]
async fn runtime_maximum_bounds_returned_rows() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let scope = prefix("pagination");
    let mut expected = Vec::new();
    for index in 0..5 {
        let collection = backend
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(format!("{scope}_{index}"), "", group_id(1), None),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        expected.push(collection.id().id());
    }
    commit(&backend, pagination_limits(1, 2)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::get().uri(&format!(
            "/api/v1/collections?limit=9&sort=id.asc&name__startswith={scope}"
        )),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let has_next = response
        .headers()
        .contains_key(crate::pagination::NEXT_CURSOR_HEADER);
    let body: Value = test::read_body_json(response).await;
    let ids = body
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_i64().unwrap() as i32)
        .collect::<Vec<_>>();
    assert_eq!((ids, has_next), (expected[..2].to_vec(), true));
}

#[rstest::rstest]
#[case::default_only(json!({"default_page_limit": 3}), 3, 10)]
#[case::maximum_only(json!({"max_page_limit": 8}), 4, 8)]
#[case::both(json!({"default_page_limit": 2, "max_page_limit": 3}), 2, 3)]
#[actix_web::test]
async fn pagination_patch_preserves_omitted_limits(
    #[case] changes: Value,
    #[case] default: u64,
    #[case] maximum: u64,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 10)).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/pagination")
            .set_json(json!({"expected_revision": 2, "changes": changes})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        backend.load_runtime_settings().await.unwrap().pagination(),
        Some(StoragePaginationPolicy::try_with_maximum(default, maximum).unwrap())
    );
}

#[rstest::rstest]
#[case::default_above_maximum(json!({"default_page_limit": 11}))]
#[case::maximum_below_default(json!({"max_page_limit": 3}))]
#[actix_web::test]
async fn pagination_patch_validates_the_combined_limits(#[case] changes: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 10)).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/pagination")
            .set_json(json!({"expected_revision": 2, "changes": changes})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await,
        1
    );
}

#[actix_web::test]
async fn releasing_pagination_restores_the_deployment_maximum() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 8)).await;
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::delete()
            .uri("/api/v1/admin/runtime-settings/pagination?expected_revision=2"),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(
        body["settings"]["pagination"]["effective"]["max_page_limit"],
        deployment_page_limits().unwrap().maximum_limit()
    );
}

#[actix_web::test]
async fn default_only_put_restores_deployment_maximum_inheritance() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 8)).await;
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 2, "default_page_limit": 5}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(
        body["settings"]["pagination"]["effective"]["max_page_limit"],
        deployment_page_limits().unwrap().maximum_limit()
    );
}

#[rstest::rstest]
#[case::runtime_default(None, false, 2)]
#[case::updated_before_body(None, true, 2)]
#[case::explicit_limit(Some(1), true, 1)]
#[actix_web::test]
async fn structured_search_stream_keeps_request_page_limits(
    #[case] requested_limit: Option<usize>,
    #[case] update_before_body: bool,
    #[case] expected_limit: usize,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    for index in 0..4 {
        backend
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(
                    format!("stream_collection_{index}"),
                    "runtime pagination stream fixture",
                    group_id(1),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
    commit(&backend, pagination(2)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let mut request = json!({"version": 1, "target": {"kind": "collection"}});
    if let Some(limit) = requested_limit {
        request["limit"] = json!(limit);
    }
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::post()
            .uri("/api/v1/search/stream")
            .set_json(request),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);

    // The handler has returned, but the search runs when its body is polled.
    if update_before_body {
        commit(&backend, pagination(3)).await;
        backend.refresh_runtime_settings().await.unwrap();
    }
    let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
    let done = body
        .split("\n\n")
        .find_map(|event| event.strip_prefix("event: done\ndata: "))
        .expect("stream should finish with pagination metadata");
    let done: Value = serde_json::from_str(done).unwrap();
    assert_eq!(
        (
            body.matches("event: result\n").count(),
            done["page_limit"].as_u64(),
            done["next"].is_string(),
        ),
        (expected_limit, Some(expected_limit as u64), true),
    );
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

#[actix_web::test]
async fn discovery_reports_all_groups_and_deployment_bounds() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, catalogue) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/runtime-settings/schema"),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let groups = catalogue["groups"].as_array().unwrap();
    assert_eq!(groups.len(), StorageRuntimeSettingsGroup::ALL.len());
    let pagination = groups
        .iter()
        .find(|group| group["key"] == "pagination")
        .unwrap();
    assert_eq!(
        pagination["schema"]["properties"]["default_page_limit"]["maximum"],
        deployment_page_limits().unwrap().maximum_limit()
    );
    let maximum = &pagination["schema"]["properties"]["max_page_limit"];
    assert_eq!(maximum["type"], "integer");
    assert_eq!(maximum["minimum"], 1);
    assert_eq!(
        maximum["maximum"],
        deployment_page_limits().unwrap().maximum_limit()
    );
    assert_eq!(
        maximum["default"],
        deployment_page_limits().unwrap().maximum_limit()
    );
    let observations = groups
        .iter()
        .find(|group| group["key"] == "query_observations")
        .unwrap();
    assert_eq!(
        observations["schema"]["properties"]["retention_seconds"]["type"],
        "integer"
    );
    assert!(
        observations["schema"]["properties"]
            .get("expected_revision")
            .is_none()
    );
}

#[actix_web::test]
async fn discovery_documents_every_advertised_field_and_enum_choice() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, catalogue) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/runtime-settings/schema"),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    for group in catalogue["groups"].as_array().unwrap() {
        let key = group["key"].as_str().unwrap();
        for annotation in ["title", "description"] {
            assert!(
                group[annotation]
                    .as_str()
                    .is_some_and(|text| !text.trim().is_empty()),
                "{key}: missing {annotation}"
            );
        }
        for (name, field) in group["schema"]["properties"].as_object().unwrap() {
            for annotation in ["title", "description"] {
                assert!(
                    field[annotation]
                        .as_str()
                        .is_some_and(|text| !text.trim().is_empty()),
                    "{key}.{name}: missing {annotation}"
                );
            }
            if let Some(choices) = field["enum"].as_array() {
                let descriptions = field["x-enum-descriptions"]
                    .as_object()
                    .expect("enum choices require help");
                assert_eq!(
                    choices.len(),
                    descriptions.len(),
                    "{key}.{name}: describe only supported choices"
                );
                for choice in choices {
                    assert!(
                        descriptions[choice.as_str().unwrap()]
                            .as_str()
                            .is_some_and(|text| !text.trim().is_empty()),
                        "{key}.{name}: missing help for {choice}"
                    );
                }
            }
        }
    }
}

#[actix_web::test]
async fn generic_patch_preserves_unedited_requested_settings() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let policy =
        StorageQueryObservationPolicy::try_with_resource_limits(true, 8, 100, 20, 120, 8).unwrap();
    let expected = commit(
        &backend,
        StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::QueryObservations(
            policy,
        )),
    )
    .await;
    let (status, _) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/query_observations")
            .set_json(json!({"expected_revision": expected, "changes": {"enabled": false}})),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let updated = backend
        .load_runtime_settings()
        .await
        .unwrap()
        .query_observations()
        .unwrap();
    assert_eq!(
        updated,
        StorageQueryObservationPolicy::try_with_resource_limits(false, 8, 100, 20, 120, 8).unwrap()
    );
}

#[actix_web::test]
async fn generic_patch_rejects_a_stale_editor_without_overwriting() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination(7)).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/pagination")
            .set_json(json!({"expected_revision": 1, "changes": {"default_page_limit": 5}})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        backend
            .load_runtime_settings()
            .await
            .unwrap()
            .pagination()
            .unwrap()
            .default_page_limit(),
        7
    );
}

#[rstest::rstest]
#[case::empty(json!({}))]
#[case::unknown(json!({"typo": 10}))]
#[case::null(json!({"default_page_limit": null}))]
#[case::wrong_type(json!({"default_page_limit": "10"}))]
#[case::zero(json!({"default_page_limit": 0}))]
#[case::ceiling(json!({"default_page_limit": 4_294_967_295u64}))]
#[case::zero_maximum(json!({"max_page_limit": 0}))]
#[case::maximum_ceiling(json!({"max_page_limit": above_ceiling()}))]
#[case::inverted_limits(json!({"default_page_limit": 10, "max_page_limit": 5}))]
#[case::null_maximum(json!({"max_page_limit": null}))]
#[case::revision_in_values(json!({"expected_revision": 1}))]
#[actix_web::test]
async fn invalid_generic_patch_is_not_audited(#[case] changes: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/pagination")
            .set_json(json!({"expected_revision": 1, "changes": changes})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await,
        0
    );
}

#[actix_web::test]
async fn generic_patch_cannot_bypass_security_candidate_activation() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/authentication")
            .set_json(json!({"expected_revision": 1, "changes": {"providers": []}})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::CONFLICT);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Authentication).await,
        0
    );
}

#[rstest::rstest]
#[case::default_only(false)]
#[case::maximum(true)]
#[actix_web::test]
async fn group_state_exposes_requested_and_effective_values_separately(#[case] with_maximum: bool) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let ceiling = deployment_page_limits().unwrap().maximum_limit() as u64;
    let policy = if with_maximum {
        pagination_limits(1, ceiling + 1)
    } else {
        pagination(ceiling + 1)
    };
    let expected = commit(&backend, policy).await;
    let (status, state) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri("/api/v1/admin/runtime-settings/pagination"),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(state["revision"], json!(expected));
    assert_eq!(
        state["effective"]["default_page_limit"],
        if with_maximum { 1 } else { ceiling }
    );
    assert_eq!(
        state["runtime"]["default_page_limit"],
        if with_maximum { 1 } else { ceiling + 1 }
    );
    assert_eq!(state["effective"]["max_page_limit"], ceiling);
    assert_eq!(
        state["runtime"]["max_page_limit"],
        if with_maximum {
            json!(ceiling + 1)
        } else {
            Value::Null
        }
    );
    assert_eq!(state["constrained_by_ceiling"], true);
}

#[rstest::rstest]
#[case::observability("observability", json!({"log_level":"debug", "metrics_path":"/telemetry/runtime"}))]
#[case::network("network", json!({"client_allowlist":"127.0.0.1/32", "trusted_proxies":"10.0.0.0/8", "trust_ip_headers":true}))]
#[case::queries("query_limits", json!({"max_transitive_depth": 7}))]
#[case::tasks("tasks", json!({"lease_seconds": 90, "heartbeat_seconds": 25, "computed_reindex_batch_size": 12}))]
#[case::remote("remote_calls", json!({"timeout_ms": 42, "max_response_bytes": 1234}))]
#[case::tokens("tokens", json!({"lifetime_hours": 2, "purge_interval_seconds": 17}))]
#[case::login("login_rate_limit", json!({"attempts_per_identity": 7, "window_seconds": 123}))]
#[case::events("events", json!({"fanout_batch_size": 7, "delivery_batch_size": 12, "purge_enabled": true}))]
#[case::exports("exports", json!({"template_fuel": 7654, "storage_query_budget_ms": 123}))]
#[case::backups("backups", json!({"max_active_per_user": 2, "max_output_bytes": 12345}))]
#[case::restores("restores", json!({"max_upload_bytes": 12345, "staging_retention_minutes": 12}))]
#[actix_web::test]
async fn operational_patch_is_persisted_audited_and_reported(
    #[case] group: &str,
    #[case] changes: Value,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri(&format!("/api/v1/admin/runtime-settings/{group}"))
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .set_json(json!({"expected_revision": 1, "changes": changes})),
    )
    .await;
    let status = response.status();
    let body: Value = test::read_body_json(response).await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    let key = StorageRuntimeSettingsGroup::parse(group).unwrap();
    let stored = backend
        .load_runtime_settings()
        .await
        .unwrap()
        .record(key)
        .assignment()
        .to_document()
        .unwrap();
    for (field, expected) in changes.as_object().unwrap() {
        assert_eq!(&stored[field], expected, "{group}.{field}");
        assert_eq!(
            &body["settings"]["policies"][group]["effective"][field], expected,
            "{group}.{field}"
        );
    }
    assert_eq!(history_len(&backend, key).await, 1);
}

#[rstest::rstest]
#[case::lease("tasks", json!({"lease_seconds":10, "heartbeat_seconds":20}))]
#[case::backoff("login_rate_limit", json!({"backoff_base_seconds":100, "backoff_max_seconds":99}))]
#[case::metrics_collision("observability", json!({"metrics_path":"/healthz"}))]
#[case::invalid_log_filter("observability", json!({"log_level":"invalid[filter"}))]
#[case::network_lockout("network", json!({"client_allowlist":"192.0.2.0/24"}))]
#[case::no_archive_path("events", json!({"file_archive_enabled":true}))]
#[case::oversized_delivery_timeout("events", json!({"delivery_transport_timeout_ms":2147483647}))]
#[actix_web::test]
async fn invalid_operational_change_has_no_audit_record(
    #[case] group: &str,
    #[case] changes: Value,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri(&format!("/api/v1/admin/runtime-settings/{group}"))
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .set_json(json!({"expected_revision":1,"changes":changes})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::parse(group).unwrap()).await,
        0
    );
}

async fn operational_assignment(
    group: StorageRuntimeSettingsGroup,
    changes: Value,
) -> StorageRuntimeSettingsAssignment {
    let mut document =
        crate::config::runtime::deployment_document(&integration_test_config().unwrap(), group)
            .unwrap();
    for (key, value) in changes.as_object().unwrap() {
        document[key] = value.clone();
    }
    StorageRuntimeSettingsAssignment::Managed(
        StorageRuntimeSettingsValue::from_document(group, &document).unwrap(),
    )
}

#[actix_web::test]
async fn operation_snapshot_survives_policy_change() {
    let backend = StorageHandle::memory();
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Exports,
            json!({"max_output_bytes":1234}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    let config = crate::config::runtime::for_storage(&backend).unwrap();
    crate::config::runtime::scope(config, async {
        commit(
            &backend,
            operational_assignment(
                StorageRuntimeSettingsGroup::Exports,
                json!({"max_output_bytes":9876}),
            )
            .await,
        )
        .await;
        backend.refresh_runtime_settings().await.unwrap();
        assert_eq!(
            crate::config::runtime::current()
                .unwrap()
                .export_max_output_bytes,
            1234
        );
    })
    .await;
    assert_eq!(
        crate::config::runtime::for_storage(&backend)
            .unwrap()
            .export_max_output_bytes,
        9876
    );
}

#[actix_web::test]
async fn token_lifetime_change_preserves_legacy_authentication_cutoff() {
    let backend = StorageHandle::memory();
    let legacy = crate::models::configured_token_lifetime().unwrap().hours();
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Tokens,
            json!({"lifetime_hours":1}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    crate::config::runtime::scope(
        crate::config::runtime::for_storage(&backend).unwrap(),
        async {
            assert_eq!(
                crate::models::configured_token_issuance_policy()
                    .unwrap()
                    .default_lifetime()
                    .hours(),
                1
            );
            assert_eq!(
                crate::models::configured_token_lifetime().unwrap().hours(),
                legacy
            );
        },
    )
    .await;
}

#[actix_web::test]
async fn metrics_path_moves_and_enablement_changes_without_rebuilding_routes() {
    let backend = StorageHandle::memory();
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Observability,
            json!({"metrics_enabled":true,"metrics_path":"/telemetry/before"}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&backend))
            .wrap(from_fn(crate::middlewares::apply_runtime_settings))
            .default_service(web::to(crate::observability::metrics::runtime_endpoint)),
    )
    .await;
    let before = test::TestRequest::get()
        .uri("/telemetry/before")
        .send_request(&app)
        .await;
    assert_eq!(before.status(), http::StatusCode::OK);
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Observability,
            json!({"metrics_enabled":true,"metrics_path":"/telemetry/after"}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    assert_eq!(
        test::TestRequest::get()
            .uri("/telemetry/before")
            .send_request(&app)
            .await
            .status(),
        http::StatusCode::NOT_FOUND
    );
    assert_eq!(
        test::TestRequest::get()
            .uri("/telemetry/after")
            .send_request(&app)
            .await
            .status(),
        http::StatusCode::OK
    );
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Observability,
            json!({"metrics_enabled":false,"metrics_path":"/telemetry/after"}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    assert_eq!(
        test::TestRequest::get()
            .uri("/telemetry/after")
            .send_request(&app)
            .await
            .status(),
        http::StatusCode::NOT_FOUND
    );
}

#[actix_web::test]
async fn network_snapshot_controls_allowlisting_and_client_attribution() {
    use crate::config::ClientAllowlist;
    use crate::middlewares::{ClientAllowlistMiddleware, ProxyTrust, TracingMiddleware};
    let backend = StorageHandle::memory();
    commit(&backend, operational_assignment(StorageRuntimeSettingsGroup::Network,
        json!({"trust_ip_headers":true,"trusted_proxies":"127.0.0.1/32","client_allowlist":"192.0.2.1/32"})).await).await;
    backend.refresh_runtime_settings().await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&backend))
            .app_data(Data::new(ProxyTrust::peer_only()))
            .wrap(ClientAllowlistMiddleware::new(ClientAllowlist::Any))
            .wrap(TracingMiddleware::new_with_trust(ProxyTrust::peer_only()))
            .wrap(from_fn(crate::middlewares::apply_runtime_settings))
            .route(
                "/source",
                web::get().to(|request: HttpRequest| async move {
                    HttpResponse::Ok().json(
                        crate::middlewares::rate_limit::client_ip_for_request(&request)
                            .map(|ip| ip.to_string()),
                    )
                }),
            ),
    )
    .await;
    let request = || {
        test::TestRequest::get()
            .uri("/source")
            .peer_addr("127.0.0.1:1234".parse().unwrap())
            .insert_header(("x-forwarded-for", "192.0.2.1"))
    };
    let response = request().send_request(&app).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        test::read_body_json::<Value, _>(response).await,
        json!("192.0.2.1")
    );
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Network,
            json!({"trust_ip_headers":false,"client_allowlist":"192.0.2.1/32"}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    assert_eq!(
        request().send_request(&app).await.status(),
        http::StatusCode::FORBIDDEN
    );
}

#[actix_web::test]
async fn restore_runtime_upload_limit_rejects_oversized_body() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Restores,
            json!({"max_upload_bytes":1}),
        )
        .await,
    )
    .await;
    backend.refresh_runtime_settings().await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&backend))
            .app_data(Data::new(
                crate::restores::RestoreSettings::new(60, 1024).unwrap(),
            ))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::post()
        .uri("/api/v1/restores")
        .insert_header((
            http::header::AUTHORIZATION,
            format!("Bearer {}", user.raw_token),
        ))
        .set_payload("{}")
        .send_request(&app)
        .await;
    assert_eq!(response.status(), http::StatusCode::PAYLOAD_TOO_LARGE);
}

#[rstest::rstest]
#[case::released(true)]
#[case::adopted(false)]
#[actix_web::test]
async fn restore_settings_keep_captured_ownership(#[case] initially_managed: bool) {
    let backend = StorageHandle::memory();
    let group = StorageRuntimeSettingsGroup::Restores;
    let managed = operational_assignment(
        group,
        json!({"max_upload_bytes":123, "staging_retention_minutes":12}),
    )
    .await;
    if initially_managed {
        commit(&backend, managed.clone()).await;
    }
    backend.refresh_runtime_settings().await.unwrap();
    let captured = crate::config::runtime::for_storage(&backend).unwrap();
    let deployment = crate::restores::RestoreSettings::new(60, 1024).unwrap();
    crate::config::runtime::scope(captured, async {
        // Authentication can yield while another request changes ownership.
        let assignment = if initially_managed {
            StorageRuntimeSettingsAssignment::Deployment(group)
        } else {
            managed
        };
        commit(&backend, assignment).await;
        backend.refresh_runtime_settings().await.unwrap();
        let settings = deployment
            .with_runtime(&crate::config::runtime::current().unwrap())
            .unwrap();
        let expected = if initially_managed {
            (123, 12)
        } else {
            (1024, 60)
        };
        assert_eq!(
            (
                settings.max_upload_bytes(),
                settings.stage_retention_minutes()
            ),
            expected
        );
    })
    .await;
}

#[rstest::rstest]
#[case::released(true)]
#[case::adopted(false)]
#[actix_web::test]
async fn backup_settings_keep_captured_ownership(#[case] initially_managed: bool) {
    let backend = StorageHandle::memory();
    let group = StorageRuntimeSettingsGroup::Backups;
    let managed = operational_assignment(
        group,
        json!({"max_active_per_user":2, "max_output_bytes":123, "max_capture_rows":45, "retention_hours":12}),
    ).await;
    if initially_managed {
        commit(&backend, managed.clone()).await;
    }
    backend.refresh_runtime_settings().await.unwrap();
    let captured = crate::config::runtime::for_storage(&backend).unwrap();
    let deployment =
        crate::backups::BackupSettings::new(24, 3, StorageBackupBudget::new(1024, 100).unwrap())
            .unwrap();
    crate::config::runtime::scope(captured, async {
        let assignment = if initially_managed {
            StorageRuntimeSettingsAssignment::Deployment(group)
        } else {
            managed
        };
        commit(&backend, assignment).await;
        backend.refresh_runtime_settings().await.unwrap();
        let settings = deployment
            .with_runtime(&crate::config::runtime::current().unwrap())
            .unwrap();
        let expected = if initially_managed {
            (2, 123, 45, 12)
        } else {
            (3, 1024, 100, 24)
        };
        assert_eq!(
            (
                settings.max_active_tasks_per_user(),
                settings.budget().max_bytes(),
                settings.budget().max_rows(),
                settings.output_retention_hours()
            ),
            expected
        );
    })
    .await;
}

#[actix_web::test]
async fn releasing_operational_policy_restores_deployment_values() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Exports,
            json!({"template_fuel":123}),
        )
        .await,
    )
    .await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::delete()
            .uri("/api/v1/admin/runtime-settings/exports?expected_revision=2"),
    )
    .await;
    let status = response.status();
    let body: Value = test::read_body_json(response).await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    assert_eq!(
        body["settings"]["policies"]["exports"]["effective"]["template_fuel"],
        integration_test_config().unwrap().export_template_fuel
    );
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Exports).await,
        2
    );
}

#[actix_web::test]
async fn stale_runtime_policy_blocks_new_delivery_batches() {
    let backend = StorageHandle::memory();
    backend.refresh_runtime_settings().await.unwrap();
    commit(
        &backend,
        operational_assignment(
            StorageRuntimeSettingsGroup::Observability,
            json!({"log_level":"invalid[filter"}),
        )
        .await,
    )
    .await;
    assert!(backend.refresh_runtime_settings().await.is_err());
    let result = crate::events::process_event_delivery_batch_for_test(
        &backend,
        integration_test_config()
            .unwrap()
            .event_delivery_settings()
            .unwrap(),
        &crate::events::NoopSinkResolver,
    )
    .await;
    assert!(matches!(result, Err(ApiError::ServiceUnavailable(_))));
}

mod security_candidates;
