use super::*;
use crate::models::Permissions;
use crate::{
    models::schema_evolution::SchemaRepairReportRequest, services::schema_evolution as service,
};

fn generation(fixture: &SchemaFixture, work: StorageSchemaWork) -> service::RepairReportGeneration {
    service::RepairReportGeneration::new(
        work,
        "Repair class <script>".into(),
        fixture.collection_id(),
        serde_json::from_value::<SchemaRepairReportRequest>(
            json!({"object_url_template":"https://inventory.example/ui/objects/{object_id}"}),
        )
        .unwrap(),
    )
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn saved_diagnostics_describe_multiple_repairs(#[case] backend: StorageBackendKind) {
    let fixture = SchemaFixture::new(
        backend,
        vec![json!({"interfaces":[{},{},{},{"address":false}]})],
    )
    .await;
    let revision = fixture.stage(json!({"required":["hostname"],"properties":{"interfaces":{"items":{"properties":{"address":{"type":"string"}}}}}}),true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let work = fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let report = service::get_work(&fixture.backend, work.task_id())
        .await
        .unwrap();
    let finding = &report.impact.as_ref().unwrap().findings[0];
    let snapshot = finding.snapshot.as_ref().unwrap();
    assert_eq!(
        snapshot.object_revision,
        fixture.resources.objects[0].revision()
    );
    let diagnostics = serde_json::to_value(&snapshot.diagnostics).unwrap();
    assert!(
        diagnostics["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["instance_path"] == "/interfaces/3/address")
    );
    assert!(
        diagnostics["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["reason"]["missing_property"] == "hostname")
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn html_report_contains_all_groups_and_retained_object_links(
    #[case] backend: StorageBackendKind,
) {
    let documents = (0..21)
        .flat_map(|index| (0..6).map(move |_| json!({format!("field{index}"):"private-value"})))
        .collect();
    let properties = (0..21)
        .map(|index| (format!("field{index}"), json!({"type":"integer"})))
        .collect::<serde_json::Map<_, _>>();
    let fixture = SchemaFixture::new(backend, documents).await;
    let revision = fixture.stage(json!({"properties":properties}), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let work = fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let report = service::generate_repair_report(&fixture.backend, generation(&fixture, work))
        .await
        .unwrap();
    // Browsers decode MiniJinja's numeric character references in href attributes.
    let html = report.html().replace("&#x2f;", "/");
    for object in &fixture.resources.objects {
        assert!(html.contains(&format!(
            "href=\"https://inventory.example/ui/objects/{}\"",
            object.id()
        )));
    }
    assert_eq!(report.html().matches("<article ").count(), 126);
    assert!(!report.html().contains("private-value"));
    assert!(!report.html().contains("<script>"));
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn repair_report_persistence_does_not_change_source_analysis(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({})]).await;
    let revision = fixture.stage(json!(false), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let report = StorageSchemaRepairReport::try_new(
        &work,
        chrono::Utc::now(),
        "<p>Saved partial analysis</p>".into(),
    )
    .unwrap();
    fixture
        .backend
        .save_schema_repair_report(StorageSchemaRepairReportWrite::new(
            report.clone(),
            fixture.collection_id(),
        ))
        .await
        .unwrap();
    let restored = fixture
        .backend
        .get_schema_repair_report(work.task_id())
        .await
        .unwrap();
    assert_eq!(restored.html(), report.html());
    let after = fixture
        .backend
        .get_schema_work(work.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(work).unwrap()
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn retained_html_does_not_follow_later_analysis_progress(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({}), json!({})]).await;
    let revision = fixture.stage(json!(false), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let limits = StorageSchemaBatchLimits::try_new(1, 2048, 1024).unwrap();
    let partial = fixture
        .backend
        .process_schema_work(lease.clone(), limits)
        .await
        .unwrap();
    let report = service::generate_repair_report(&fixture.backend, generation(&fixture, partial))
        .await
        .unwrap();
    fixture.finish_claimed(lease, limits).await;
    let retained =
        service::retained_repair_report(&fixture.backend, revision.reference(), work.task_id())
            .await
            .unwrap();
    assert_eq!(retained.html(), report.html());
    assert!(retained.html().contains("Partial analysis: running"));
    assert_eq!(retained.html().matches("<article ").count(), 1);
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn repair_report_storage_rechecks_authorized_collection(#[case] backend: StorageBackendKind) {
    let fixture = SchemaFixture::new(backend, vec![]).await;
    let revision = fixture.stage(json!(false), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let report =
        StorageSchemaRepairReport::try_new(&work, chrono::Utc::now(), "<p>Report</p>".into())
            .unwrap();
    let error = fixture
        .backend
        .save_schema_repair_report(StorageSchemaRepairReportWrite::new(
            report,
            CollectionId::new(fixture.collection_id().id() + 1).unwrap(),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::NotFound);
    assert!(
        fixture
            .backend
            .get_schema_repair_report(work.task_id())
            .await
            .is_err()
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::generate("POST", "")]
#[case::view("GET", "")]
#[case::download("GET", "?download=true")]
#[actix_web::test]
async fn repair_report_endpoints_enforce_source_analysis_access(
    #[case] method: &str,
    #[case] suffix: &str,
    #[values(StorageBackendKind::Memory, StorageBackendKind::Postgres)] kind: StorageBackendKind,
    #[values("member", "administrator", "scoped_administrator")] access: &str,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({})]).await;
    let user = create_backend_user(&fixture.backend, &prefix("repair_reader")).await;
    fixture
        .backend
        .add_group_member(
            user.principal_id,
            fixture.resources.owned_group.as_ref().unwrap().id(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    if access != "member" {
        let group = match &fixture.environment {
            BackendTestEnvironment::Memory { .. } => group_id(1),
            BackendTestEnvironment::Postgres { pool } => {
                group_id(crate::tests::ensure_admin_group(pool).await.id)
            }
        };
        fixture
            .backend
            .add_group_member(user.principal_id, group, &EventContext::system())
            .await
            .unwrap()
            .into_value();
    }
    let revision = fixture.stage(json!({"required":["hostname"]}), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let work = fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let saved =
        service::generate_repair_report(&fixture.backend, generation(&fixture, work.clone()))
            .await
            .unwrap();
    let bearer = if access == "scoped_administrator" {
        let raw = prefix("repair_scoped");
        let digest = crate::models::Token::storage_hash_from_raw(&raw);
        fixture
            .backend
            .create_token(
                StorageTokenCreate::new(
                    user.principal_id,
                    StorageTokenDigest::legacy_unidentified(&digest),
                    StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                    EventContext::system(),
                )
                .scope(Some(StorageAuthenticationTokenScope::new(
                    Some(vec![StorageAuthorizationPermission::ReadClass]),
                    None,
                ))),
            )
            .await
            .unwrap()
            .into_value();
        raw
    } else {
        user.raw_token.clone()
    };
    let permissions = Arc::new(LocalPermissionBackend::new(
        fixture.backend.clone(),
        crate::tests::integration_test_config()
            .unwrap()
            .admin_groupname,
    ));
    let app = test::init_service(
        App::new()
            .app_data(Data::new(AppContext::new(
                fixture.backend.clone(),
                permissions,
            )))
            .configure(crate::api::config),
    )
    .await;
    let uri = format!(
        "/api/v1/classes/{}/schema/tasks/{}/report{suffix}",
        fixture.class_id(),
        work.task_id()
    );
    let mut request = test::TestRequest::default()
        .method(method.parse().unwrap())
        .uri(&uri)
        .insert_header((http::header::AUTHORIZATION, format!("Bearer {bearer}")));
    if method == "POST" {
        request = request.set_json(
            json!({"object_url_template":"https://inventory.example/ui/objects/{object_id}"}),
        );
    }
    let response = test::call_service(&app, request.to_request()).await;
    if access == "administrator" {
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap(),
            "text/html; charset=utf-8"
        );
        if !suffix.is_empty() {
            assert!(
                response
                    .headers()
                    .get("Content-Disposition")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("attachment;")
            );
        }
        if method == "GET" {
            assert_eq!(
                test::read_body(response).await.as_ref(),
                saved.html().as_bytes()
            );
        }
    } else {
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        let body = test::read_body(response).await;
        assert!(!String::from_utf8_lossy(&body).contains("hostname"));
    }
    delete_backend_user(&fixture.backend, user).await;
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn later_object_changes_do_not_rewrite_saved_diagnostics(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({"number":"wrong"})]).await;
    let revision = fixture
        .stage(json!({"properties":{"number":{"type":"integer"}}}), true)
        .await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let work = fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let before = service::get_work(&fixture.backend, work.task_id())
        .await
        .unwrap();
    fixture.update(0, json!({"number":42})).await.unwrap();
    let after = service::get_work(&fixture.backend, work.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(before.impact).unwrap(),
        serde_json::to_value(after.impact).unwrap()
    );
    assert!(matches!(
        after.readiness,
        Some(crate::models::schema_evolution::SchemaImpactReadiness::Inconclusive)
    ));
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::included("<header>Repair team</header>{{ report_content }}", true)]
#[case::omitted("<header>Oops</header>", false)]
#[case::duplicated("{{ report_content }}{{ report_content }}", false)]
#[actix_web::test]
async fn stored_layouts_must_retain_the_complete_report(
    #[case] source: &str,
    #[case] succeeds: bool,
) {
    let fixture = SchemaFixture::new(StorageBackendKind::Memory, vec![json!({})]).await;
    let revision = fixture.stage(json!({"required":["hostname"]}), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let work = fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let previous =
        service::generate_repair_report(&fixture.backend, generation(&fixture, work.clone()))
            .await
            .unwrap();
    let template: crate::models::ExportTemplate = serde_json::from_value(json!({
        "id":1,"collection_id":fixture.collection_id(),"name":"repair-layout.html","description":"Repair layout","content_type":"text/html","template":source,"kind":"fragment","created_at":"2026-09-14T00:00:00","updated_at":"2026-09-14T00:00:00","revision":1
    })).unwrap();
    let input = generation(&fixture, work.clone()).with_layout(Some(
        service::RepairReportLayout::try_new(template, vec![]).unwrap(),
    ));
    let result = service::generate_repair_report(&fixture.backend, input).await;
    if succeeds {
        let report = result.unwrap();
        assert!(report.html().contains("<header>Repair team</header>"));
        assert_eq!(report.html().matches("<article ").count(), 1);
    } else {
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
        assert_eq!(
            fixture
                .backend
                .get_schema_repair_report(work.task_id())
                .await
                .unwrap()
                .html(),
            previous.html()
        );
    }
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::class_denied(false, true)]
#[case::admin_denied(true, false)]
#[case::allowed(true, true)]
#[actix_web::test]
async fn retained_reports_use_configured_authorization(
    #[case] read_class: bool,
    #[case] external_admin: bool,
) {
    use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::{ResourceFields, ResourceKind};
    let fixture = SchemaFixture::new(StorageBackendKind::Memory, vec![json!({})]).await;
    let user = create_backend_user(&fixture.backend, &prefix("repair_delegated")).await;
    let group = fixture.resources.owned_group.as_ref().unwrap().id();
    fixture
        .backend
        .add_group_member(user.principal_id, group, &EventContext::system())
        .await
        .unwrap()
        .into_value();
    // Local administrator membership must not override the configured backend.
    fixture
        .backend
        .add_group_member(user.principal_id, group_id(1), &EventContext::system())
        .await
        .unwrap()
        .into_value();
    let revision = fixture.stage(json!(false), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    service::generate_repair_report(&fixture.backend, generation(&fixture, work.clone()))
        .await
        .unwrap();
    let policy = MockTreetopBackend::new();
    if read_class {
        policy.add_rule(MockAllowRule {
            group_id: group.id(),
            action: Permissions::ReadClass,
            resource_kind: ResourceKind::Class,
            resource_id: Some(fixture.class_id().id()),
            attrs: ResourceFields::default(),
        });
    }
    if external_admin {
        policy.add_admin_rule(group.id());
    }
    let app = test::init_service(
        App::new()
            .app_data(Data::new(AppContext::new(
                fixture.backend.clone(),
                Arc::new(policy),
            )))
            .configure(crate::api::config),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(&format!(
                "/api/v1/classes/{}/schema/tasks/{}/report",
                fixture.class_id(),
                work.task_id()
            ))
            .insert_header((
                http::header::AUTHORIZATION,
                format!("Bearer {}", user.raw_token),
            ))
            .to_request(),
    )
    .await;
    assert_eq!(
        response.status(),
        if read_class && external_admin {
            http::StatusCode::OK
        } else {
            http::StatusCode::FORBIDDEN
        }
    );
    delete_backend_user(&fixture.backend, user).await;
    fixture.cleanup().await;
}
