use super::*;
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    QueryUsageStorage, StorageErrorKind, StorageQueryUsageCreate, StorageQueryUsageDelete,
    StorageQueryUsageOperation as Operation, StorageQueryUsagePattern, StorageQueryUsageReplace,
    StorageQueryUsageScope, StorageQueryUsageValueType as ValueType,
};
use serde_json::json;

fn pattern(path: &str) -> StorageQueryUsagePattern {
    StorageQueryUsagePattern::try_new(path, ValueType::String, vec![Operation::Equals]).unwrap()
}

#[rstest::rstest]
#[case::read("GET")]
#[case::create("POST")]
#[case::replace("PUT")]
#[case::delete("DELETE")]
#[actix_web::test]
async fn query_usage_api_requires_class_permission(#[case] method: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("usage_permission"), vec![]).await;
        let user = create_backend_user(&backend, &prefix("usage_denied")).await;
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
        let suffix = if matches!(method, "PUT" | "DELETE") {
            "/1?expected_revision=1"
        } else {
            ""
        };
        let response = test::call_service(
            &app,
            test::TestRequest::default()
                .method(method.parse().unwrap())
                .uri(&format!(
                    "/api/v1/classes/{}/query-usage{suffix}",
                    fixture.class.id()
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .set_json(if method == "PUT" {
                    json!({"pattern":pattern("serial"),"expected_revision":1})
                } else {
                    json!({"pattern":pattern("serial")})
                })
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        delete_backend_user(&backend, user).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn schema_changes_reassess_and_preserve_declarations() {
    use hubuum_domain::SchemaRevision;
    use hubuum_storage_core::schema_evolution::*;
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("usage_schema"), vec![]).await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        let record = backend
            .create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("serial"),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let schema = json!({"type":"object", "properties":{"serial":{"type":"number"}}});
        let staged = backend
            .stage_schema_revision(StorageSchemaStage::new(
                scope.authorized_collection(),
                scope.class_id(),
                StorageValidatedSchemaPolicy::try_new_with_limits(
                    StorageClassSchemaPolicy::try_from_parts(Some(schema.clone()), false).unwrap(),
                    backend.schema_limits(),
                )
                .unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let _ = backend
            .activate_schema_revision(StorageSchemaActivation::new(
                scope.authorized_collection(),
                staged.reference(),
                SchemaRevision::INITIAL,
                StorageSchemaActivationPolicy::RejectIncompatible,
                EventContext::system(),
            ))
            .await
            .unwrap();
        let records = backend.list_query_usage(scope).await.unwrap();
        assert_eq!(records, vec![record]);
        assert_eq!(
            records[0].pattern().schema_compatibility(Some(&schema)),
            hubuum_storage_core::StorageQueryUsageSchemaCompatibility::Incompatible
        );
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn declaration_capture_preserves_intent_and_provenance() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("usage_backup"), vec![]).await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        let record = backend
            .create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("serial"),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let snapshot = backend
            .capture_backup_snapshot(
                false,
                hubuum_storage_core::StorageBackupBudget::new(128 * 1024 * 1024, 100_000).unwrap(),
            )
            .await
            .unwrap();
        let (sections, _) = snapshot.into_parts();
        let rows =
            &sections[&hubuum_storage_core::StorageBackupStateSection::QueryUsageDeclarations];
        let saved = rows
            .iter()
            .find(|row| {
                row.get("id").and_then(serde_json::Value::as_i64)
                    == Some(i64::from(record.metadata().id().id()))
            })
            .unwrap();
        assert_eq!(saved.clone().into_value(), record.snapshot());
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn postgres_declaration_rolls_back_with_its_audit_event() {
    let _permit = postgres_permit().await;
    let backend = StorageHandle::from_registered_backend(
        hubuum_storage_postgres::PostgresStorage::unobserved(pool().get_ref().clone()),
    );
    let fixture = create_backend_object_fixture(&backend, &prefix("usage_rollback"), vec![]).await;
    let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
    let result = PostgresFaultController::failing(PostgresFaultPoint::TransactionBeforeCommit)
        .run(backend.create_query_usage(StorageQueryUsageCreate::new(
            scope,
            pattern("serial"),
            EventContext::system(),
        )))
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Backend);
    assert!(backend.list_query_usage(scope).await.unwrap().is_empty());
    let (events, _) = backend
        .list_audit_events(StorageAuditEventListQuery::new(
            vec![scope.authorized_collection()],
            false,
            StorageAuditEventFilters::new().entity_type(Some(EntityType::QueryUsageDeclaration)),
            QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true).unwrap(),
        ))
        .await
        .unwrap()
        .into_parts();
    assert!(
        events.is_empty(),
        "the declaration audit must roll back too"
    );
    delete_backend_object_fixture(&backend, fixture).await;
}

#[rstest::rstest]
#[case::unsafe_path(json!({"path":"x');DROP TABLE hubuumobject;--", "value_type":"string","operations":["equals"]}))]
#[case::wrong_type(json!({"path":"serial", "value_type":null,"operations":["equals"]}))]
#[case::invalid_operator(json!({"path":"serial", "value_type":"string","operations":["gt"]}))]
#[actix_web::test]
async fn postgres_declaration_constraints_reject_invalid_intent(
    #[case] pattern: serde_json::Value,
) {
    use diesel::sql_types::{Integer, Jsonb};
    use hubuum_storage_postgres::diesel_async_prelude::RunQueryDsl;
    let _permit = postgres_permit().await;
    let native_pool = pool();
    let backend = StorageHandle::from_registered_backend(
        hubuum_storage_postgres::PostgresStorage::unobserved(native_pool.get_ref().clone()),
    );
    let fixture =
        create_backend_object_fixture(&backend, &prefix("usage_constraints"), vec![]).await;
    let result =
        hubuum_storage_postgres::with_connection(native_pool.get_ref(), async |connection| {
            diesel::sql_query(
                "INSERT INTO query_usage_declarations(class_id,pattern) VALUES($1,$2)",
            )
            .bind::<Integer, _>(fixture.class.id().id())
            .bind::<Jsonb, _>(pattern)
            .execute(connection)
            .await
        })
        .await;
    assert!(result.is_err());
    delete_backend_object_fixture(&backend, fixture).await;
}

#[actix_web::test]
async fn declarations_have_audited_revision_checked_lifecycle() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("usage_lifecycle"),
            vec![json!({"serial":"abc"})],
        )
        .await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        let created = backend
            .create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("serial"),
                EventContext::system(),
            ))
            .await
            .unwrap();
        assert!(created.is_committed());
        let before = created.into_value();
        let replaced = backend
            .replace_query_usage(StorageQueryUsageReplace::new(
                StorageQueryUsageCreate::new(
                    scope,
                    pattern("hardware,serial"),
                    EventContext::system(),
                ),
                before.metadata().id(),
                before.metadata().revision(),
            ))
            .await
            .unwrap()
            .into_value();
        assert_eq!(replaced.metadata().revision().get(), 2);
        let stale = backend
            .delete_query_usage(StorageQueryUsageDelete::new(
                scope,
                before.metadata().id(),
                before.metadata().revision(),
                EventContext::system(),
            ))
            .await
            .unwrap_err();
        assert_eq!(stale.kind(), StorageErrorKind::Conflict);
        assert_eq!(
            backend.list_query_usage(scope).await.unwrap(),
            vec![replaced.clone()]
        );
        let deleted = backend
            .delete_query_usage(StorageQueryUsageDelete::new(
                scope,
                replaced.metadata().id(),
                replaced.metadata().revision(),
                EventContext::system(),
            ))
            .await
            .unwrap();
        assert!(deleted.is_committed());
        assert!(backend.list_query_usage(scope).await.unwrap().is_empty());
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest::rstest]
#[case::list("list")]
#[case::create("create")]
#[case::replace("replace")]
#[case::delete("delete")]
#[actix_web::test]
async fn declarations_recheck_the_authorized_collection(#[case] operation: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(&backend, &prefix("usage_scope"), vec![]).await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        let record = backend
            .create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("serial"),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let wrong =
            StorageQueryUsageScope::new(scope.class_id(), CollectionId::new(i32::MAX).unwrap());
        let error = match operation {
            "list" => backend.list_query_usage(wrong).await.unwrap_err(),
            "create" => backend
                .create_query_usage(StorageQueryUsageCreate::new(
                    wrong,
                    pattern("other"),
                    EventContext::system(),
                ))
                .await
                .unwrap_err(),
            "replace" => backend
                .replace_query_usage(StorageQueryUsageReplace::new(
                    StorageQueryUsageCreate::new(wrong, pattern("other"), EventContext::system()),
                    record.metadata().id(),
                    record.metadata().revision(),
                ))
                .await
                .unwrap_err(),
            _ => backend
                .delete_query_usage(StorageQueryUsageDelete::new(
                    wrong,
                    record.metadata().id(),
                    record.metadata().revision(),
                    EventContext::system(),
                ))
                .await
                .unwrap_err(),
        };
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn declaration_quota_is_serialized_with_concurrent_creates() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(&backend, &prefix("usage_quota"), vec![]).await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        for index in 0..31 {
            let _ = backend
                .create_query_usage(StorageQueryUsageCreate::new(
                    scope,
                    pattern(&format!("field_{index}")),
                    EventContext::system(),
                ))
                .await
                .unwrap();
        }
        let (first, second) = tokio::join!(
            backend.create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("first"),
                EventContext::system()
            )),
            backend.create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("second"),
                EventContext::system()
            ))
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert_eq!(backend.list_query_usage(scope).await.unwrap().len(), 32);
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn declaration_noop_preserves_revision_and_has_no_audit() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(&backend, &prefix("usage_noop"), vec![]).await;
        let scope = StorageQueryUsageScope::new(fixture.class.id(), fixture.collection.id());
        let record = backend
            .create_query_usage(StorageQueryUsageCreate::new(
                scope,
                pattern("serial"),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let result = backend
            .replace_query_usage(StorageQueryUsageReplace::new(
                StorageQueryUsageCreate::new(scope, pattern("serial"), EventContext::system()),
                record.metadata().id(),
                ResourceRevision::INITIAL,
            ))
            .await
            .unwrap();
        assert!(!result.is_committed());
        assert_eq!(result.into_value(), record);
        delete_backend_object_fixture(&backend, fixture).await;
    }
}
