use super::*;

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
