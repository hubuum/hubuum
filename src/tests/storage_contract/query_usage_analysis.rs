use super::*;
use hubuum_query::parse_query_parameter;
use hubuum_storage_core::{
    QueryUsageStorage, StorageQueryObservationSettings, StorageQueryUsageCreate,
    StorageQueryUsageOperation, StorageQueryUsagePattern, StorageQueryUsageScope,
    StorageQueryUsageValueType,
};
use serde_json::{Value, json};

fn pattern(path: &str) -> StorageQueryUsagePattern {
    StorageQueryUsagePattern::try_new(
        path,
        StorageQueryUsageValueType::String,
        vec![StorageQueryUsageOperation::Equals],
    )
    .unwrap()
}

async fn observe(backend: &StorageHandle, class: ClassId, path: &str) {
    for _ in 0..5 {
        let options = parse_query_parameter(&format!(
            "class_id={}&include_total=true&json_data={path}=secret-example",
            class.id()
        ))
        .unwrap();
        let _ = backend
            .list_objects(StorageCatalogListQuery::new(
                options,
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            ))
            .await
            .unwrap();
    }
}

#[rstest::rstest]
#[case::suggestion(false)]
#[case::existing_declaration(true)]
#[actix_web::test]
async fn analysis_distinguishes_availability_and_avoids_declared_suggestions(
    #[case] declared: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let backend = backend.with_query_observations(
            StorageQueryObservationSettings::try_new(true, 1, 64, 64, 60, 16).unwrap(),
        );
        let fixture =
            create_backend_object_fixture(&backend, &prefix("usage_analysis"), vec![]).await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        let path = prefix("usage_field");
        if declared {
            let _ = backend
                .create_query_usage(StorageQueryUsageCreate::new(
                    scope,
                    pattern(&path),
                    EventContext::system(),
                ))
                .await
                .unwrap();
        }
        observe(&backend, scope.class_id(), &path).await;
        let report =
            serde_json::to_value(backend.analyze_query_usage(scope, vec![]).await.unwrap())
                .unwrap();
        assert_eq!(report["observations"]["patterns"][0]["sampled_queries"], 5);
        assert!(!report.to_string().contains("secret-example"));
        if backend.descriptor().kind() == StorageBackendKind::Postgres {
            assert_eq!(report["status"], "complete");
            assert_eq!(
                report["suggestions"].as_array().unwrap().len(),
                usize::from(!declared)
            );
        } else {
            assert_eq!(report["status"], "unavailable");
            assert_eq!(report["suggestions"], json!([]));
        }
        // Read-only analysis must neither adopt a suggestion nor alter a declaration.
        assert_eq!(
            backend.list_query_usage(scope).await.unwrap().len(),
            usize::from(declared)
        );
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn disabled_collection_is_insufficient_evidence_on_postgres() {
    let _permit = postgres_permit().await;
    let backend = StorageHandle::postgres(pool().get_ref().clone());
    let fixture = create_backend_object_fixture(&backend, &prefix("usage_disabled"), vec![]).await;
    let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
    let report = serde_json::to_value(
        backend
            .analyze_query_usage(scope, vec![pattern("serial")])
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(report["status"], "insufficient_evidence");
    assert_eq!(report["assessments"].as_array().unwrap().len(), 1);
    assert_eq!(report["suggestions"], json!([]));
    delete_backend_object_fixture(&backend, fixture).await;
}

#[actix_web::test]
async fn analysis_api_requires_administrator_access() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("usage_review_acl"), vec![]).await;
        let user = create_backend_user(&backend, &prefix("usage_review_user")).await;
        let config = crate::tests::integration_test_config().unwrap();
        let permissions = Arc::new(LocalPermissionBackend::new(
            backend.clone(),
            config.admin_groupname,
        ));
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(backend.clone(), permissions)))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!(
                    "/api/v1/classes/{}/query-usage/analysis",
                    fixture.class.id()
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .set_json(json!({"proposed":[]}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn independently_managed_matching_index_suppresses_suggestions() {
    let _permit = postgres_permit().await;
    let backend = StorageHandle::postgres(pool().get_ref().clone()).with_query_observations(
        StorageQueryObservationSettings::try_new(true, 1, 64, 64, 60, 16).unwrap(),
    );
    let fixture = create_backend_object_fixture(&backend, &prefix("usage_external"), vec![]).await;
    let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
    let path = prefix("usage_external_path");
    let index_name = prefix("usage_external_index");
    let native_pool = if hubuum_storage_postgres::test_support::database_role_tests_enabled() {
        hubuum_storage_postgres::test_support::integration_test_migration_pool(1)
    } else {
        hubuum_storage_postgres::test_support::integration_test_pool(1)
    };
    native_ddl(
        &native_pool,
        format!("CREATE INDEX {index_name} ON hubuumobject USING hash ((data #>> '{{{path}}}'))"),
    )
    .await
    .unwrap();
    observe(&backend, scope.class_id(), &path).await;
    let report: Value = serde_json::to_value(
        backend
            .analyze_query_usage(scope, vec![pattern(&path)])
            .await
            .unwrap(),
    )
    .unwrap();
    let cleanup = native_ddl(&native_pool, format!("DROP INDEX {index_name}")).await;
    delete_backend_object_fixture(&backend, fixture).await;
    cleanup.unwrap();
    assert_eq!(report["suggestions"], json!([]));
    assert_eq!(
        report["assessments"][0]["resources"][0]["ownership"],
        "independently_managed"
    );
    assert_eq!(
        report["assessments"][0]["resources"][0]["cleanup_eligible_if_removed"],
        false
    );
}

async fn native_ddl(
    pool: &PostgresPool,
    sql: String,
) -> Result<usize, hubuum_storage_postgres::PostgresStorageError> {
    use diesel::{sql_query, sql_types::Text};
    use hubuum_storage_postgres::diesel_async_prelude::*;
    use hubuum_storage_postgres::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
    };
    hubuum_storage_postgres::with_connection(pool, async |connection| {
        connection
            .transaction::<_, diesel::result::Error, _>(async |connection| {
                if database_role_tests_enabled() {
                    sql_query("SELECT set_config('role', $1, true)")
                        .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                        .execute(connection)
                        .await?;
                }
                sql_query(sql).execute(connection).await
            })
            .await
    })
    .await
}
