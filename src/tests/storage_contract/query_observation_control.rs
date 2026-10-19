use super::*;
use crate::config::running::RunningConfig;
use crate::tests::integration_test_config;
use hubuum_query::parse_query_parameter;
use hubuum_storage_core::{
    QueryUsageStorage, StorageAuthenticationTokenScope, StorageQueryObservationControl,
    StorageQueryObservationSettings, StorageQueryUsageScope,
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

async fn admin(backend: &StorageHandle) -> BackendUserFixture {
    let user = create_backend_user(backend, &prefix("observation_admin")).await;
    let _ = backend
        .add_group_member(user.principal_id, group_id(1), &EventContext::system())
        .await
        .unwrap();
    user
}

#[rstest::rstest]
#[case::enable(true)]
#[case::disable(false)]
#[actix_web::test]
async fn api_toggle_changes_collection_on_another_replica(#[case] enabled: bool) {
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
    // Prime the reader's independent cache before changing shared storage.
    reader
        .observe_query(&query, None, async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&writer))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::put()
        .uri("/api/v1/admin/query-observations")
        .insert_header((
            http::header::AUTHORIZATION,
            format!("Bearer {}", user.raw_token),
        ))
        .set_json(json!({"enabled": enabled}))
        .send_request(&app)
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(1010)).await;
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
#[case::dedicated("/api/v1/admin/query-observations")]
#[case::configuration("/api/v1/admin/config")]
#[actix_web::test]
async fn admin_reads_show_override_on_a_new_replica(#[case] path: &str) {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let user = admin(&writer).await;
    writer
        .set_query_observation_control(StorageQueryObservationControl::Enabled)
        .await
        .unwrap();
    let reader = replica(&storage, false);
    let app = test::init_service(
        App::new()
            .app_data(context(&reader))
            .app_data(Data::new(RunningConfig::from(
                &integration_test_config().unwrap(),
            )))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::get()
        .uri(path)
        .insert_header((
            http::header::AUTHORIZATION,
            format!("Bearer {}", user.raw_token),
        ))
        .send_request(&app)
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    let config = if path.ends_with("/config") {
        &body["query_observations"]
    } else {
        &body
    };
    assert_eq!(
        (
            config["enabled"].as_bool(),
            config["runtime_override"].as_bool()
        ),
        (Some(true), Some(true))
    );
}

#[rstest::rstest]
#[case::anonymous_get("GET", "anonymous", 401)]
#[case::anonymous_put("PUT", "anonymous", 401)]
#[case::user_get("GET", "user", 403)]
#[case::user_put("PUT", "user", 403)]
#[case::scoped_admin_get("GET", "scoped_admin", 403)]
#[case::scoped_admin_put("PUT", "scoped_admin", 403)]
#[actix_web::test]
async fn control_requires_unscoped_administrator(
    #[case] method: &str,
    #[case] identity: &str,
    #[case] status: u16,
) {
    let backend = StorageHandle::memory();
    let user = if identity == "scoped_admin" {
        admin(&backend).await
    } else {
        create_backend_user(&backend, &prefix("toggle_user")).await
    };
    let bearer = if identity == "scoped_admin" {
        let raw = prefix("scoped_toggle_token");
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
    } else {
        user.raw_token
    };
    let app = test::init_service(
        App::new()
            .app_data(context(&backend))
            .configure(crate::api::config),
    )
    .await;
    let mut request = test::TestRequest::default()
        .method(method.parse().unwrap())
        .uri("/api/v1/admin/query-observations")
        .set_json(json!({"enabled": true}));
    if identity != "anonymous" {
        request = request.insert_header((http::header::AUTHORIZATION, format!("Bearer {bearer}")));
    }
    let response = request.send_request(&app).await;
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(
        backend.query_observation_control().await.unwrap(),
        StorageQueryObservationControl::Startup
    );
}

#[rstest::rstest]
#[case(json!({}))]
#[case(json!({"enabled": null}))]
#[case(json!({"enabled": "true"}))]
#[case(json!({"enabled": true, "sample_every": 1}))]
#[actix_web::test]
async fn invalid_control_requests_are_rejected(#[case] body: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let app = test::init_service(
        App::new()
            .app_data(context(&backend))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::put()
        .uri("/api/v1/admin/query-observations")
        .insert_header((
            http::header::AUTHORIZATION,
            format!("Bearer {}", user.raw_token),
        ))
        .set_json(body)
        .send_request(&app)
        .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
}
