use super::history_fixtures::{owner_user, update};
use super::*;
use crate::models::{Permissions, TokenResourceScope, TokenScope};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use crate::services::history::computed;
use chrono::{DateTime, Utc};
use hubuum_storage_core::{StorageComputedFieldHistoryQuery, StorageHistoricalComputedFields};
use hubuum_storage_postgres::capture_queries;
use rstest::rstest;
use serde_json::{Value, json};

fn definition() -> StorageComputedFieldDefinitionInput {
    StorageComputedFieldDefinitionInput::new(
        serde_json::from_value(json!({
            "key": "historical_value", "label": "Historical value", "description": "",
            "operation": {"type": "first_non_null", "paths": ["/value"]},
            "result_type": "integer", "enabled": true
        }))
        .unwrap(),
    )
}

async fn create(
    backend: &StorageHandle,
    fixture: &BackendObjectFixture,
    owner: PrincipalId,
) -> ComputedFieldDefinitionId {
    let (field, _) = backend
        .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
            fixture.class.id(),
            fixture.collection.id(),
            owner,
            definition(),
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts();
    ComputedFieldDefinitionId::from(field.metadata().id())
}

async fn history(
    backend: &StorageHandle,
    class: ClassId,
    at: DateTime<Utc>,
) -> StorageHistoricalComputedFields {
    backend
        .get_computed_field_history(StorageComputedFieldHistoryQuery::new(class, at))
        .await
        .unwrap()
        .unwrap()
}

fn policy(fixture: &BackendObjectFixture, object: bool, class: bool) -> Arc<MockTreetopBackend> {
    let policy = Arc::new(MockTreetopBackend::new());
    for (allowed, action, resource_kind) in [
        (object, Permissions::ReadObject, ResourceKind::Object),
        (class, Permissions::ReadClass, ResourceKind::Class),
    ] {
        if allowed {
            policy.add_rule(MockAllowRule {
                group_id: fixture.owned_group.as_ref().unwrap().id().id(),
                action,
                resource_kind,
                resource_id: None,
                attrs: ResourceFields {
                    collection_id: Some(fixture.collection.id().id()),
                    ..Default::default()
                },
            });
        }
    }
    policy
}

#[actix_web::test]
async fn historical_computation_token_scope_must_allow_class_definitions() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("computed_history_scope"),
            vec![json!({"value":7})],
        )
        .await;
        let user = owner_user(&backend, &fixture).await;
        create(&backend, &fixture, user.principal_id).await;
        let context = AppContext::new(backend.clone(), policy(&fixture, true, true));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let scope = TokenScope::from_request_parts(
            Some(vec![Permissions::ReadObject]),
            Some(vec![TokenResourceScope::Collection(
                fixture.collection.id(),
            )]),
        )
        .unwrap()
        .unwrap();
        let (result, queries) = capture_queries(computed::as_of(
            &context,
            &principal,
            Some(&scope),
            fixture.class.id(),
            fixture.objects[0].id(),
            Utc::now(),
        ))
        .await;
        assert!(matches!(
            result,
            Err(ApiError::Forbidden(_) | ApiError::NotFound(_))
        ));
        assert_eq!(queries.queries_matching("computed_field_history"), 0);
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn class_deletion_closes_shared_history_without_erasing_it() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("computed_history_delete"), vec![])
                .await;
        let user = create_backend_user(&backend, &prefix("computed_history_deleted_owner")).await;
        let id = create(&backend, &fixture, user.principal_id).await;
        let class = fixture.class.id();
        let at = Utc::now();
        delete_backend_object_fixture(&backend, fixture).await;
        assert_eq!(
            history(&backend, class, at).await.record().definitions()[0].id(),
            id
        );
        assert!(
            backend
                .get_computed_field_history(StorageComputedFieldHistoryQuery::new(
                    class,
                    Utc::now()
                ))
                .await
                .unwrap()
                .is_none()
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn concurrent_shared_definitions_are_recorded_as_complete_sets() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("computed_history_concurrent"), vec![])
                .await;
        let owner = create_backend_user(&backend, &prefix("computed_history_writer")).await;
        let request = |key: &str| {
            let mut value = serde_json::to_value(definition().into_definition()).unwrap();
            value["key"] = json!(key);
            StorageSharedComputedFieldCreate::new(
                fixture.class.id(),
                fixture.collection.id(),
                owner.principal_id,
                StorageComputedFieldDefinitionInput::new(serde_json::from_value(value).unwrap()),
                EventContext::system(),
            )
        };
        let (first, second) = tokio::join!(
            backend.create_shared_computed_field(request("first")),
            backend.create_shared_computed_field(request("second"))
        );
        first.unwrap().into_value();
        second.unwrap().into_value();
        let current = history(&backend, fixture.class.id(), Utc::now()).await;
        assert_eq!(current.record().definitions().len(), 2);
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, owner).await;
    }
}

#[actix_web::test]
async fn known_empty_shared_history_is_available_before_the_first_definition() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("computed_history_empty"), vec![])
                .await;
        let at = Utc::now();
        let owner = create_backend_user(&backend, &prefix("computed_history_empty_owner")).await;
        create(&backend, &fixture, owner.principal_id).await;
        assert!(
            history(&backend, fixture.class.id(), at)
                .await
                .record()
                .definitions()
                .is_empty()
        );
        assert!(
            backend
                .get_computed_field_history(StorageComputedFieldHistoryQuery::new(
                    fixture.class.id(),
                    DateTime::UNIX_EPOCH
                ))
                .await
                .unwrap()
                .is_none()
        );
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, owner).await;
    }
}

#[rstest]
#[case::operation("operation")]
#[case::disable("disable")]
#[case::delete("delete")]
#[case::recreate("recreate")]
#[actix_web::test]
async fn historical_computation_retains_definition_revisions_independent_of_object_changes(
    #[case] change: &str,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("computed_history"),
            vec![json!({"value": 7, "other": 9})],
        )
        .await;
        let user = owner_user(&backend, &fixture).await;
        let id = create(&backend, &fixture, user.principal_id).await;
        let at = Utc::now();
        let before = history(&backend, fixture.class.id(), at).await;
        let context = AppContext::new(backend.clone(), policy(&fixture, true, true));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let original = serde_json::to_value(
            computed::as_of(
                &context,
                &principal,
                None,
                fixture.class.id(),
                fixture.objects[0].id(),
                at,
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(original["shared"]["fields"][0]["value"], 7);
        match change {
            "operation" | "disable" => {
                let patch = if change == "disable" {
                    StorageComputedFieldDefinitionPatch::new().with_enabled(Some(false))
                } else {
                    StorageComputedFieldDefinitionPatch::new()
                        .with_operation(Some(json!({"type":"first_non_null", "paths":["/other"]})))
                };
                backend
                    .update_shared_computed_field(StorageSharedComputedFieldUpdate::new(
                        fixture.class.id(),
                        fixture.collection.id(),
                        id,
                        user.principal_id,
                        patch,
                        EventContext::system(),
                    ))
                    .await
                    .unwrap()
                    .into_value();
            }
            _ => {
                backend
                    .delete_shared_computed_field(StorageSharedComputedFieldDelete::new(
                        fixture.class.id(),
                        fixture.collection.id(),
                        id,
                        user.principal_id,
                        EventContext::system(),
                    ))
                    .await
                    .unwrap()
                    .into_value();
                if change == "recreate" {
                    assert_ne!(create(&backend, &fixture, user.principal_id).await, id);
                }
            }
        }
        let after_definition = Utc::now();
        update(
            &backend,
            fixture.objects[0].id(),
            StorageObjectUpdate::builder()
                .data(Some(json!({"value": 70, "other": 90})))
                .build(),
        )
        .await;
        let old = serde_json::to_value(
            computed::as_of(
                &context,
                &principal,
                None,
                fixture.class.id(),
                fixture.objects[0].id(),
                at,
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(old["shared"]["fields"][0]["value"], 7);
        assert_eq!(
            old["shared"]["fields"][0]["revision"],
            original["shared"]["fields"][0]["revision"]
        );
        assert_eq!(
            old["object"]["history_id"],
            original["object"]["history_id"]
        );
        assert_eq!(
            old["shared"]["definition_history_id"],
            before.record().history_entry_id().id()
        );
        let changed = serde_json::to_value(
            computed::as_of(
                &context,
                &principal,
                None,
                fixture.class.id(),
                fixture.objects[0].id(),
                after_definition,
            )
            .await
            .unwrap(),
        )
        .unwrap();
        match change {
            "operation" => assert_eq!(changed["shared"]["fields"][0]["value"], 9),
            "disable" => assert_eq!(changed["shared"]["fields"][0]["status"], "disabled"),
            "delete" => assert_eq!(changed["shared"]["fields"], json!([])),
            _ => assert_ne!(changed["shared"]["fields"][0]["definition_id"], id.id()),
        }
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::missing_object(false, true)]
#[case::missing_class(true, false)]
#[actix_web::test]
async fn historical_computation_requires_both_current_permissions(
    #[case] object: bool,
    #[case] class: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("computed_history_access"),
            vec![json!({"value":7})],
        )
        .await;
        let user = owner_user(&backend, &fixture).await;
        create(&backend, &fixture, user.principal_id).await;
        let context = AppContext::new(backend.clone(), policy(&fixture, object, class));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let (result, queries) = capture_queries(computed::as_of(
            &context,
            &principal,
            None,
            fixture.class.id(),
            fixture.objects[0].id(),
            Utc::now(),
        ))
        .await;
        for table in [
            "computed_field_history",
            "hubuumobject_history",
            "hubuumclass_history",
        ] {
            assert_eq!(
                queries.queries_matching(table),
                0,
                "denied request accessed historical source"
            );
        }
        assert!(matches!(
            result,
            Err(ApiError::Forbidden(_) | ApiError::NotFound(_))
        ));
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn historical_computation_api_is_private_and_has_no_computation_or_object_side_effects() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("computed_history_api"),
            vec![json!({"value":7})],
        )
        .await;
        let user = owner_user(&backend, &fixture).await;
        create(&backend, &fixture, user.principal_id).await;
        let at = Utc::now();
        let context = AppContext::new(backend.clone(), policy(&fixture, true, true));
        let principal = backend
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let (result, queries) = capture_queries(computed::as_of(
            &context,
            &principal,
            None,
            fixture.class.id(),
            fixture.objects[0].id(),
            at,
        ))
        .await;
        result.unwrap();
        assert!(queries.query_counts().keys().all(|sql| {
            !["INSERT", "UPDATE", "DELETE"]
                .iter()
                .any(|verb| sql.trim_start().starts_with(verb))
        }));
        assert_eq!(
            queries.queries_matching("FROM computed_field_history"),
            usize::from(backend.descriptor().kind() == StorageBackendKind::Postgres)
        );
        let before_state = backend
            .get_computed_field_state(fixture.class.id())
            .await
            .unwrap();
        let before_object = backend
            .object_store()
            .get_object(fixture.objects[0].id())
            .await
            .unwrap();
        let before_history = history(&backend, fixture.class.id(), at).await;
        let app = test::init_service(
            App::new()
                .app_data(Data::new(AppContext::new(
                    backend.clone(),
                    policy(&fixture, true, true),
                )))
                .configure(crate::api::config),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!(
                    "/api/v1/classes/{}/{}/history/computed/as-of?at={}",
                    fixture.class.id(),
                    fixture.objects[0].id(),
                    at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
                ))
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get(http::header::CACHE_CONTROL).unwrap(),
            "private, no-store"
        );
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["mode"], "historical_recomputation");
        assert_eq!(body["personal"], "unsupported");
        assert_eq!(body["shared"]["fields"][0]["value"], 7);
        assert!(
            before_state
                == backend
                    .get_computed_field_state(fixture.class.id())
                    .await
                    .unwrap()
        );
        assert!(
            before_object
                == backend
                    .object_store()
                    .get_object(fixture.objects[0].id())
                    .await
                    .unwrap()
        );
        assert!(before_history == history(&backend, fixture.class.id(), at).await);
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::deleted(false)]
#[case::anonymized(true)]
#[actix_web::test]
async fn shared_history_never_retains_personal_definitions_or_owner_identity(
    #[case] anonymize: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("computed_history_privacy"), vec![])
                .await;
        let user = create_backend_user(&backend, &prefix("computed_history_owner")).await;
        create(&backend, &fixture, user.principal_id).await;
        let at = Utc::now();
        let before = history(&backend, fixture.class.id(), at).await;
        backend
            .create_personal_computed_field(StoragePersonalComputedFieldCreate::new(
                fixture.class.id(),
                user.principal_id,
                definition(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        if anonymize {
            backend
                .anonymize_user(StorageUserAnonymize::new(user.id, EventContext::system()))
                .await
                .unwrap()
                .into_value();
        } else {
            backend
                .delete_user(StorageUserDelete::new(user.id, EventContext::system()))
                .await
                .unwrap()
                .into_value();
        }
        let retained = history(&backend, fixture.class.id(), at).await;
        assert!(before.record().definitions() == retained.record().definitions());
        let serialized = serde_json::to_string(retained.record()).unwrap();
        for private in ["owner_user_id", "created_by", "updated_by", "personal"] {
            assert!(!serialized.contains(private));
        }
        delete_backend_object_fixture(&backend, fixture).await;
        if anonymize {
            delete_backend_user(&backend, user).await;
        }
    }
}
