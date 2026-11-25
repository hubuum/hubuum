use super::*;

#[actix_web::test]
async fn every_registered_backend_satisfies_the_audited_mutation_contract() {
    let _permit = postgres_permit().await;

    for environment in available_backend_environments() {
        let report = environment
            .verify_audit_contract()
            .await
            .expect("certified backend must satisfy the audit contract");
        assert_eq!(report.checks(), 6);
    }
}

#[rstest::rstest]
#[case::class_from(false, 0)]
#[case::class_to(false, 1)]
#[case::object_from(true, 0)]
#[case::object_to(true, 1)]
#[actix_web::test]
async fn relation_audit_timelines_survive_relation_deletion(
    #[case] object: bool,
    #[case] endpoint: usize,
) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_audit")).await;
        let collection = fixture.collection.id();
        let (subject, entity_type, relation_type, entity_id, relation_id) = if object {
            let id = fixture.objects[endpoint].id();
            (
                StorageAuditSubject::Object(id),
                EntityType::Object,
                EntityType::ObjectRelation,
                id.id(),
                fixture.object_relation_id.id(),
            )
        } else {
            let id = fixture.classes[endpoint].id();
            (
                StorageAuditSubject::Class(id),
                EntityType::Class,
                EntityType::ClassRelation,
                id.id(),
                fixture.class_relation_id.id(),
            )
        };
        delete_backend_relation_fixture(&backend, fixture).await;

        let (events, total) = backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![collection],
                false,
                StorageAuditEventFilters::new().involving(Some(subject)),
                QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts();
        let mut actual = events
            .iter()
            .map(|recorded| {
                let (event, _, _) = recorded.as_parts();
                (
                    event.entity_type().as_str(),
                    event.entity_id().unwrap().get(),
                    event.action().as_str(),
                )
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let mut expected = vec![
            (entity_type.as_str(), entity_id, "created"),
            (entity_type.as_str(), entity_id, "deleted"),
            (relation_type.as_str(), relation_id, "created"),
            (relation_type.as_str(), relation_id, "deleted"),
        ];
        expected.sort_unstable();
        assert_eq!((actual, total), (expected, Some(4)));
    }
}

#[rstest::rstest]
#[case::class(false)]
#[case::object(true)]
#[actix_web::test]
async fn relation_audit_timelines_redact_indirectly_visible_snapshots(#[case] object: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let fixture =
            create_backend_relation_fixture(&backend, &prefix("relation_redaction")).await;
        let subject = if object {
            StorageAuditSubject::Object(fixture.objects[0].id())
        } else {
            StorageAuditSubject::Class(fixture.classes[0].id())
        };
        let (events, _) = backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![fixture.collection.id()],
                false,
                StorageAuditEventFilters::new().involving(Some(subject)),
                QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts();
        let relation = events
            .iter()
            .map(|event| event.as_parts().0)
            .find(|event| {
                matches!(
                    event.entity_type(),
                    EntityType::ClassRelation | EntityType::ObjectRelation
                )
            })
            .expect("relation event is visible through either related collection");
        assert_eq!((relation.before(), relation.after()), (None, None));
        delete_backend_relation_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn backend_registration_does_not_require_optional_application_providers() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::from_registered_backend(AdapterPostgresStorage::unobserved(
        pool.get_ref().clone(),
    ));

    assert!(backend.database_pool_state().is_none());
    assert!(!backend.has_worker_notification_provider());
    assert!(
        backend
            .database_storage_snapshot()
            .await
            .expect("missing optional diagnostics must not be a backend failure")
            .is_none()
    );
    assert!(backend.get_readiness_snapshot().await.is_ok());
}

struct RegisteredApplicationCompatibilityFixture {
    environment: BackendTestEnvironment,
}

#[async_trait(?Send)]
impl ApplicationCompatibilityFixture for RegisteredApplicationCompatibilityFixture {
    async fn application_compatibility_probe(
        &self,
    ) -> Result<ApplicationCompatibilityProbe, FixtureError> {
        let fixture_error = |error: crate::errors::ApiError| -> FixtureError {
            std::io::Error::other(error.to_string()).into()
        };
        let config = crate::tests::integration_test_config().map_err(fixture_error)?;
        let fixture = backend_application_fixture(&self.environment).await;
        let backend = fixture.backend.clone();
        let descriptor = backend.descriptor();

        let services = Services::from_storage(backend.clone());
        let root = services
            .collections()
            .get(CollectionID::new(1).expect("valid root collection id"))
            .await
            .map_err(fixture_error)?;

        let permissions = Arc::new(LocalPermissionBackend::new(
            backend.clone(),
            config.admin_groupname.clone(),
        ));
        let app = test::init_service(
            App::new()
                .wrap(actix_web::middleware::from_fn(
                    crate::middlewares::actor_context,
                ))
                .app_data(Data::new(AppContext::new(backend, permissions)))
                .configure(crate::api::config),
        )
        .await;

        let ready =
            test::call_service(&app, test::TestRequest::get().uri("/readyz").to_request()).await;
        let readiness_status = ready.status().as_u16();

        let authorization = (
            http::header::AUTHORIZATION,
            format!("Bearer {}", fixture.bearer_token),
        );
        let point = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/collections/1")
                .insert_header(authorization.clone())
                .to_request(),
        )
        .await;
        let point_status = point.status().as_u16();
        let point_id = if point.status() == http::StatusCode::OK {
            test::read_body_json::<crate::models::Collection, _>(point)
                .await
                .id
        } else {
            -1
        };

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/collections?limit=10")
                .insert_header(authorization)
                .to_request(),
        )
        .await;
        let list_status = list.status().as_u16();
        let listed = if list.status() == http::StatusCode::OK {
            test::read_body_json::<Vec<crate::models::Collection>, _>(list).await
        } else {
            Vec::new()
        };

        drop(app);
        fixture.cleanup(&self.environment).await;

        ApplicationCompatibilityProbe::builder(descriptor.kind().as_str())
            .service_resource_id(root.id)
            .readiness_status(readiness_status)
            .point(point_status, point_id)
            .list(
                list_status,
                listed.into_iter().map(|collection| collection.id).collect(),
            )
            .build()
    }
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_services_and_http() {
    let _permit = postgres_permit().await;

    for environment in available_backend_environments() {
        let expectations = ApplicationCompatibilityExpectations::new(
            environment.kind().as_str(),
            1,
            http::StatusCode::OK.as_u16(),
        );
        let fixture = RegisteredApplicationCompatibilityFixture { environment };
        let report = verify_application_compatibility(&fixture, &expectations)
            .await
            .expect("registered backend must satisfy application compatibility");
        assert_eq!(report.checks(), 7);
    }
}
