use super::*;
use hubuum_domain::{SchemaReference, SchemaRevision, TaskId};
use hubuum_storage_core::schema_evolution::*;
use hubuum_storage_core::{StorageAuthenticationTokenScope, StorageMutationOutcome};
use hubuum_storage_postgres::test_support::claim_task_by_id_with_lease;
use serde_json::{Value, json};

struct SchemaFixture {
    environment: BackendTestEnvironment,
    backend: StorageHandle,
    resources: BackendObjectFixture,
    _permit: OwnedSemaphorePermit,
}

impl SchemaFixture {
    async fn new(kind: StorageBackendKind, documents: Vec<Value>) -> Self {
        let permit = postgres_permit().await;
        let environment = match kind {
            StorageBackendKind::Memory => BackendTestEnvironment::Memory {
                storage: MemoryStorage::new(),
            },
            StorageBackendKind::Postgres => BackendTestEnvironment::Postgres {
                pool: pool().get_ref().clone(),
            },
        };
        let backend = environment.storage();
        let resources =
            create_backend_object_fixture(&backend, &prefix("schema_evolution"), documents).await;
        Self {
            environment,
            backend,
            resources,
            _permit: permit,
        }
    }
    fn class_id(&self) -> ClassId {
        self.resources.class.id()
    }
    fn collection_id(&self) -> CollectionId {
        self.resources.collection.id()
    }
    async fn stage(&self, schema: Value, enforced: bool) -> StorageSchemaRevision {
        self.backend
            .stage_schema_revision(StorageSchemaStage::new(
                self.collection_id(),
                self.class_id(),
                StorageValidatedSchemaPolicy::try_new(
                    StorageClassSchemaPolicy::try_from_parts(Some(schema), enforced).unwrap(),
                )
                .unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
    }
    async fn request(
        &self,
        target: SchemaReference,
        kind: StorageSchemaWorkKind,
    ) -> StorageSchemaWork {
        self.backend
            .request_schema_work(StorageSchemaWorkRequest::new(
                self.collection_id(),
                target,
                kind,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
    }
    async fn claim(&self, task_id: TaskId, millis: i64) -> StorageTaskLease {
        let duration = StorageTaskLeaseDuration::from_milliseconds(millis).unwrap();
        let claim = match &self.environment {
            BackendTestEnvironment::Memory { storage } => storage
                .claim_next_task(duration)
                .await
                .unwrap()
                .expect("queued task"),
            BackendTestEnvironment::Postgres { pool } => {
                claim_task_by_id_with_lease(pool, task_id, duration)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(claim.task().id(), task_id);
        claim.lease().clone()
    }
    async fn finish(
        &self,
        work: StorageSchemaWork,
        limits: StorageSchemaBatchLimits,
    ) -> StorageSchemaWork {
        let lease = self.claim(work.task_id(), 60_000).await;
        self.finish_claimed(lease, limits).await
    }
    async fn finish_claimed(
        &self,
        lease: StorageTaskLease,
        limits: StorageSchemaBatchLimits,
    ) -> StorageSchemaWork {
        for _ in 0..200 {
            let work = self
                .backend
                .process_schema_work(lease.clone(), limits)
                .await
                .unwrap();
            if work.status() != StorageSchemaWorkStatus::Running {
                return work;
            }
        }
        panic!("schema work exceeded fixture batch bound")
    }
    async fn activate(
        &self,
        revision: &StorageSchemaRevision,
        expected: SchemaRevision,
        policy: StorageSchemaActivationPolicy,
        proof: Option<TaskId>,
    ) -> Result<StorageSchemaActivationResult, StorageError> {
        self.backend
            .activate_schema_revision(
                StorageSchemaActivation::new(
                    self.collection_id(),
                    revision.reference(),
                    expected,
                    policy,
                    EventContext::system(),
                )
                .with_proof_task(proof),
            )
            .await
            .map(StorageMutationOutcome::into_value)
    }
    async fn update(&self, index: usize, data: Value) -> Result<StorageObject, StorageError> {
        let object = self
            .backend
            .object_store()
            .get_object(self.resources.objects[index].id())
            .await?;
        self.backend
            .object_store()
            .update_object(
                &object,
                StorageObjectUpdate::builder().data(Some(data)).build(),
                &EventContext::system(),
            )
            .await
            .map(StorageMutationOutcome::into_value)
    }
    async fn compliance(&self) -> Vec<StorageObjectCompliance> {
        self.backend
            .list_schema_compliance(
                StorageSchemaPage::try_new(self.class_id(), 0, 100).unwrap(),
                None,
            )
            .await
            .unwrap()
    }
    async fn cleanup(self) {
        delete_backend_object_fixture(&self.backend, self.resources).await;
    }
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn immutable_schema_revisions_are_monotonic_and_equivalent_staging_is_idempotent(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![]).await;
    let first = fixture.stage(json!({"type":"object"}), true).await;
    let duplicate = fixture.stage(json!({"type":"object"}), true).await;
    assert_eq!(first.reference(), duplicate.reference());
    fixture
        .backend
        .abandon_schema_revision(
            first.reference(),
            fixture.collection_id(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    let next = fixture.stage(json!({"type":"object"}), true).await;
    assert!(next.reference().revision() > first.reference().revision());
    let retained = fixture
        .backend
        .list_schema_revisions(StorageSchemaPage::try_new(fixture.class_id(), 0, 100).unwrap())
        .await
        .unwrap();
    assert_eq!(retained[1].status(), StorageSchemaRevisionStatus::Abandoned);
    assert_eq!(retained[1].policy().policy(), first.policy().policy());
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn permissive_schema_activation_records_object_effects_without_changing_data(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(
        backend,
        vec![json!({"age":"secret-value"}), json!({"age":5})],
    )
    .await;
    let revision = fixture
        .stage(
            json!({"properties":{"age":{"type":"integer"}},"required":["age"]}),
            true,
        )
        .await;
    let activation = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    assert!(
        fixture
            .compliance()
            .await
            .iter()
            .all(|row| row.status() == StorageComplianceStatus::Pending)
    );
    let work = fixture
        .backend
        .get_schema_work(activation.task_id().unwrap())
        .await
        .unwrap();
    fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let compliance = fixture.compliance().await;
    assert_eq!(
        compliance
            .iter()
            .map(StorageObjectCompliance::status)
            .collect::<Vec<_>>(),
        [
            StorageComplianceStatus::Invalid,
            StorageComplianceStatus::Valid
        ]
    );
    let current = fixture
        .backend
        .object_store()
        .get_object(fixture.resources.objects[0].id())
        .await
        .unwrap();
    assert_eq!(current.object().data(), &json!({"age":"secret-value"}));
    let events = fixture
        .backend
        .list_audit_events(StorageAuditEventListQuery::new(
            vec![fixture.collection_id()],
            false,
            StorageAuditEventFilters::new().entity_type(Some(EntityType::ObjectValidation)),
            QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    let envelopes = events
        .into_iter()
        .map(|event| event.into_parts().0)
        .collect::<Vec<_>>();
    assert!(
        envelopes
            .iter()
            .any(|event| event.action() == Action::Failed)
    );
    assert!(
        envelopes
            .iter()
            .any(|event| event.action() == Action::Succeeded)
    );
    assert!(
        !serde_json::to_string(&envelopes)
            .unwrap()
            .contains("secret-value")
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn object_writes_validate_and_replace_schema_evidence_atomically(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({})]).await;
    let revision = fixture.stage(json!({"required":["ok"]}), true).await;
    fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    assert!(fixture.update(0, json!({"bad":"redacted"})).await.is_err());
    let object = fixture.update(0, json!({"ok":true})).await.unwrap();
    let evidence = serde_json::to_value(&fixture.compliance().await[0]).unwrap();
    assert_eq!(evidence["status"], "valid");
    assert_eq!(
        evidence["evidence"]["object_revision"],
        json!(object.revision())
    );
    assert_eq!(evidence["evidence"]["schema"], json!(revision.reference()));
    fixture.cleanup().await;
}

#[derive(Clone, Copy)]
enum ProofCase {
    Missing,
    Incompatible,
    Stale,
    Current,
}
#[rstest::rstest]
#[case::missing(ProofCase::Missing)]
#[case::incompatible(ProofCase::Incompatible)]
#[case::stale(ProofCase::Stale)]
#[case::current(ProofCase::Current)]
#[actix_web::test]
async fn strict_activation_requires_current_complete_compatible_proof(#[case] scenario: ProofCase) {
    for backend in StorageBackendKind::ALL {
        let fixture = SchemaFixture::new(backend, vec![json!({"n":1})]).await;
        let revision = fixture
            .stage(
                if matches!(scenario, ProofCase::Incompatible) {
                    json!({"required":["absent"]})
                } else {
                    json!({"required":["n"]})
                },
                true,
            )
            .await;
        let proof = if matches!(scenario, ProofCase::Missing) {
            None
        } else {
            let work = fixture
                .request(revision.reference(), StorageSchemaWorkKind::Impact)
                .await;
            Some(
                fixture
                    .finish(work, StorageSchemaBatchLimits::default())
                    .await
                    .task_id(),
            )
        };
        if matches!(scenario, ProofCase::Stale) {
            fixture.update(0, json!({"n":2})).await.unwrap();
        }
        let result = fixture
            .activate(
                &revision,
                SchemaRevision::INITIAL,
                StorageSchemaActivationPolicy::RejectIncompatible,
                proof,
            )
            .await;
        if matches!(scenario, ProofCase::Current) {
            assert_eq!(result.unwrap().active().reference(), revision.reference());
        } else {
            assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        }
        fixture.cleanup().await;
    }
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_impact_is_bounded_and_oversized_objects_cannot_prove_compatibility(
    #[case] backend: StorageBackendKind,
) {
    let mut data = (0..25).map(|_| json!({"n":"secret"})).collect::<Vec<_>>();
    data.push(json!({"large":"x".repeat(8192)}));
    let fixture = SchemaFixture::new(backend, data).await;
    let revision = fixture
        .stage(json!({"properties":{"n":{"type":"integer"}}}), true)
        .await;
    let limits = StorageSchemaBatchLimits::try_new(4, 2048, 1024).unwrap();
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let batch = fixture
        .backend
        .process_schema_work(lease.clone(), limits)
        .await
        .unwrap();
    assert_eq!(batch.examined(), 4);
    let complete = fixture.finish_claimed(lease, limits).await;
    let report = serde_json::to_value(&complete).unwrap();
    assert_eq!(complete.examined(), 26);
    assert_eq!(complete.uninspectable(), 1);
    assert_eq!(report["invalid_samples"].as_array().unwrap().len(), 20);
    assert!(!complete.proves_compatible(revision.reference(), 0));
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn cancelled_schema_worker_cannot_commit_another_batch(#[case] backend: StorageBackendKind) {
    let fixture = SchemaFixture::new(backend, vec![json!({}), json!({})]).await;
    let revision = fixture.stage(json!({"required":["missing"]}), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let limits = StorageSchemaBatchLimits::try_new(1, 2048, 1024).unwrap();
    fixture
        .backend
        .process_schema_work(lease.clone(), limits)
        .await
        .unwrap();
    let cancelled = fixture
        .backend
        .cancel_schema_work(
            work.task_id(),
            fixture.collection_id(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    assert_eq!(cancelled.examined(), 1);
    assert!(
        fixture
            .backend
            .process_schema_work(lease, limits)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .backend
            .get_schema_work(work.task_id())
            .await
            .unwrap()
            .status(),
        StorageSchemaWorkStatus::Cancelled
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn expired_schema_lease_resumes_from_committed_checkpoint(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({}), json!({})]).await;
    let revision = fixture.stage(json!({"type":"object"}), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let limits = StorageSchemaBatchLimits::try_new(1, 2048, 1024).unwrap();
    fixture
        .backend
        .process_schema_work(lease.clone(), limits)
        .await
        .unwrap();
    fixture
        .backend
        .renew_task_lease(
            lease.clone(),
            StorageTaskLeaseDuration::from_milliseconds(1).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let recovered = fixture
        .backend
        .recover_expired_task_leases(100)
        .await
        .unwrap();
    assert!(
        recovered
            .iter()
            .any(|task| task.id() == work.task_id() && task.status() == StorageTaskStatus::Queued)
    );
    assert!(
        fixture
            .backend
            .process_schema_work(lease, limits)
            .await
            .is_err()
    );
    let complete = fixture
        .finish(
            fixture
                .backend
                .get_schema_work(work.task_id())
                .await
                .unwrap(),
            limits,
        )
        .await;
    assert_eq!(complete.examined(), 2);
    assert_eq!(complete.batches(), 2);
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_mutations_preserve_the_authorized_collection_boundary(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![]).await;
    let result = fixture
        .backend
        .stage_schema_revision(StorageSchemaStage::new(
            CollectionId::new(i32::MAX).unwrap(),
            fixture.class_id(),
            StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Absent).unwrap(),
            EventContext::system(),
        ))
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::NotFound);
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn concurrent_schema_activation_allows_one_expected_revision_to_win(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![]).await;
    let a = fixture.stage(json!({"type":"object"}), true).await;
    let b = fixture.stage(json!({"type":"array"}), true).await;
    let (a, b) = tokio::join!(
        fixture.activate(
            &a,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::RejectIncompatible,
            None
        ),
        fixture.activate(
            &b,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::RejectIncompatible,
            None
        )
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_compliance_reports_and_filters_share_active_revision(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({}), json!({"n":3})]).await;
    let revision = fixture.stage(json!({"required":["n"]}), true).await;
    let activation = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let state = fixture
        .backend
        .get_schema_state(fixture.class_id())
        .await
        .unwrap();
    assert_eq!(state.counts().pending(), 2);
    let totals = fixture.backend.schema_compliance_counts().await.unwrap();
    assert!(totals.pending() >= 2);
    fixture
        .finish(
            fixture
                .backend
                .get_schema_work(activation.task_id().unwrap())
                .await
                .unwrap(),
            StorageSchemaBatchLimits::default(),
        )
        .await;
    let invalid = fixture
        .backend
        .list_schema_compliance(
            StorageSchemaPage::try_new(fixture.class_id(), 0, 10).unwrap(),
            Some(StorageComplianceStatus::Invalid),
        )
        .await
        .unwrap();
    assert_eq!(
        invalid
            .iter()
            .map(StorageObjectCompliance::object_id)
            .collect::<Vec<_>>(),
        [fixture.resources.objects[0].id()]
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::class_counts("schema")]
#[case::impact("schema/revisions/1/impact")]
#[actix_web::test]
async fn class_managers_cannot_read_unrestricted_schema_reports(#[case] suffix: &str) {
    for kind in StorageBackendKind::ALL {
        let fixture = SchemaFixture::new(kind, vec![json!({})]).await;
        let user = create_backend_user(&fixture.backend, &prefix("schema_manager")).await;
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
        let config = crate::tests::integration_test_config().unwrap();
        let permissions = Arc::new(LocalPermissionBackend::new(
            fixture.backend.clone(),
            config.admin_groupname,
        ));
        let app = test::init_service(
            App::new()
                .wrap(actix_web::middleware::from_fn(
                    crate::middlewares::actor_context,
                ))
                .app_data(Data::new(AppContext::new(
                    fixture.backend.clone(),
                    permissions,
                )))
                .configure(crate::api::config),
        )
        .await;
        let uri = format!("/api/v1/classes/{}/{suffix}", fixture.class_id());
        let request = if suffix.ends_with("impact") {
            test::TestRequest::post()
        } else {
            test::TestRequest::get()
        };
        let response = test::call_service(
            &app,
            request
                .uri(&uri)
                .insert_header((
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", user.raw_token),
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        delete_backend_user(&fixture.backend, user).await;
        fixture.cleanup().await;
    }
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_routes_stage_and_activate_an_empty_class(#[case] backend: StorageBackendKind) {
    let fixture = SchemaFixture::new(backend, vec![]).await;
    let admin = backend_application_fixture(&fixture.environment).await;
    let config = crate::tests::integration_test_config().unwrap();
    let permissions = Arc::new(LocalPermissionBackend::new(
        fixture.backend.clone(),
        config.admin_groupname,
    ));
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .app_data(Data::new(AppContext::new(
                fixture.backend.clone(),
                permissions,
            )))
            .configure(crate::api::config),
    )
    .await;
    let auth = (
        http::header::AUTHORIZATION,
        format!("Bearer {}", admin.bearer_token),
    );
    let uri = format!("/api/v1/classes/{}/schema/revisions", fixture.class_id());
    let staged = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&uri)
            .insert_header(auth.clone())
            .set_json(json!({"json_schema":{"type":"object"},"validate_schema":true}))
            .to_request(),
    )
    .await;
    assert_eq!(staged.status(), http::StatusCode::CREATED);
    let document: Value = test::read_body_json(staged).await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&format!("{uri}/{}/activate", document["revision"]))
            .insert_header(auth)
            .set_json(json!({"expected_active_revision":1,"policy":"reject_incompatible"}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let activated: Value = test::read_body_json(response).await;
    assert_eq!(activated["active"]["revision"], document["revision"]);
    assert!(activated["task_id"].as_i64().is_some());
    admin.cleanup(&fixture.environment).await;
    fixture.cleanup().await;
}

mod native;

mod imports;

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn superseded_schema_work_cannot_replace_not_required_object_effects(
    #[case] kind: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let enforced = fixture.stage(json!({"required":["n"]}), true).await;
    let first = fixture
        .activate(
            &enforced,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let old_lease = fixture.claim(first.task_id().unwrap(), 60_000).await;
    let advisory = fixture.stage(json!({}), false).await;
    let disabled = fixture
        .activate(
            &advisory,
            enforced.reference().revision(),
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let old = fixture
        .backend
        .process_schema_work(old_lease, StorageSchemaBatchLimits::default())
        .await
        .unwrap();
    assert_eq!(old.status(), StorageSchemaWorkStatus::Superseded);
    let work = fixture
        .backend
        .get_schema_work(disabled.task_id().unwrap())
        .await
        .unwrap();
    let finished = fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    assert_eq!(finished.not_required(), 1);
    assert_eq!(
        fixture.compliance().await[0].status(),
        StorageComplianceStatus::NotRequired
    );
    let events = fixture
        .backend
        .list_audit_events(StorageAuditEventListQuery::new(
            vec![fixture.collection_id()],
            false,
            StorageAuditEventFilters::new().entity_type(Some(EntityType::ObjectValidation)),
            QueryOptions::new(vec![], vec![], Some(100), None, false).unwrap(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    assert!(
        events
            .into_iter()
            .any(|event| event.into_parts().0.action() == Action::Updated)
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_backup_retains_supported_evidence_and_lifecycle_documents(
    #[case] kind: StorageBackendKind,
) {
    use hubuum_storage_core::{
        StorageBackupHistorySection, StorageBackupSnapshot, StorageBackupStateSection,
    };
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let revision = fixture.stage(json!({"required":["n"]}), true).await;
    let active = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let work = fixture
        .backend
        .get_schema_work(active.task_id().unwrap())
        .await
        .unwrap();
    fixture
        .finish(work, StorageSchemaBatchLimits::default())
        .await;
    let (state, history) = fixture
        .backend
        .capture_backup_snapshot(true)
        .await
        .unwrap()
        .into_parts();
    let evidence = state[&StorageBackupStateSection::ObjectSchemaEvidence]
        .iter()
        .find(|row| {
            row.get("object_id").and_then(Value::as_i64)
                == Some(i64::from(fixture.resources.objects[0].id().id()))
        })
        .unwrap();
    assert_eq!(
        evidence.get("schema_revision"),
        Some(&json!(revision.reference().revision()))
    );
    assert_eq!(evidence.get("valid"), Some(&json!(true)));
    let history = history.unwrap();
    assert!(
        history[&StorageBackupHistorySection::ClassSchemaHistory]
            .iter()
            .any(|row| row.get("class_id").and_then(Value::as_i64)
                == Some(i64::from(fixture.class_id().id()))
                && row
                    .get("snapshot")
                    .and_then(|snapshot| snapshot.get("status"))
                    == Some(&json!("retired")))
    );
    StorageBackupSnapshot::try_new(state, Some(history)).unwrap();
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_history_preserves_explicit_mutation_actor(#[case] kind: StorageBackendKind) {
    use hubuum_storage_core::StorageBackupHistorySection;
    let fixture = SchemaFixture::new(kind, vec![]).await;
    let user = create_backend_user(&fixture.backend, &prefix("schema_history_actor")).await;
    let context = EventContext::user(user.principal_id, None, None);
    let revision = fixture
        .backend
        .stage_schema_revision(StorageSchemaStage::new(
            fixture.collection_id(),
            fixture.class_id(),
            StorageValidatedSchemaPolicy::try_new(
                StorageClassSchemaPolicy::try_from_parts(Some(json!({"type":"object"})), true)
                    .unwrap(),
            )
            .unwrap(),
            context.clone(),
        ))
        .await
        .unwrap()
        .into_value();
    fixture
        .backend
        .abandon_schema_revision(revision.reference(), fixture.collection_id(), &context)
        .await
        .unwrap()
        .into_value();
    let (_, history) = fixture
        .backend
        .capture_backup_snapshot(true)
        .await
        .unwrap()
        .into_parts();
    let history = history.unwrap();
    let rows = history[&StorageBackupHistorySection::ClassSchemaHistory]
        .iter()
        .filter(|row| {
            row.get("class_id") == Some(&json!(fixture.class_id()))
                && row.get("revision") == Some(&json!(revision.reference().revision()))
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|row| row.get("actor_principal_id") == Some(&json!(user.principal_id)))
    );
    delete_backend_user(&fixture.backend, user).await;
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::external_grants_non_local_admin(false, true)]
#[case::external_denies_local_admin(true, false)]
#[actix_web::test]
async fn schema_reports_use_configured_administrator_policy(
    #[case] local_admin: bool,
    #[case] external_admin: bool,
) {
    use crate::models::Permissions;
    use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::{ResourceFields, ResourceKind};
    let fixture = SchemaFixture::new(StorageBackendKind::Memory, vec![json!({})]).await;
    let user = create_backend_user(&fixture.backend, &prefix("schema_external_admin")).await;
    let group = fixture.resources.owned_group.as_ref().unwrap().id();
    fixture
        .backend
        .add_group_member(user.principal_id, group, &EventContext::system())
        .await
        .unwrap()
        .into_value();
    if local_admin {
        fixture
            .backend
            .add_group_member(user.principal_id, group_id(1), &EventContext::system())
            .await
            .unwrap()
            .into_value();
    }
    let policy = MockTreetopBackend::new();
    policy.add_rule(MockAllowRule {
        group_id: group.id(),
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(fixture.class_id().id()),
        attrs: ResourceFields::default(),
    });
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
            .uri(&format!("/api/v1/classes/{}/schema", fixture.class_id()))
            .insert_header((
                http::header::AUTHORIZATION,
                format!("Bearer {}", user.raw_token),
            ))
            .to_request(),
    )
    .await;
    assert_eq!(
        response.status(),
        if external_admin {
            http::StatusCode::OK
        } else {
            http::StatusCode::FORBIDDEN
        }
    );
    delete_backend_user(&fixture.backend, user).await;
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_activation_queues_a_fenced_shared_computed_rebuild(
    #[case] kind: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let owner = create_backend_user(&fixture.backend, &prefix("schema_computed_owner")).await;
    fixture
        .backend
        .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
            fixture.class_id(),
            fixture.collection_id(),
            owner.principal_id,
            StorageComputedFieldDefinitionInput::new(
                Definition::new(
                    FieldKey::new("schema_n").unwrap(),
                    "N",
                    "",
                    Operation::FirstNonNull {
                        paths: vec![JsonPointer::new("/n").unwrap()],
                    },
                    ResultType::Number,
                    true,
                )
                .unwrap(),
            ),
            EventContext::user(owner.principal_id, None, None),
        ))
        .await
        .unwrap()
        .into_value();
    let revision = fixture.stage(json!({"required":["n"]}), true).await;
    let activated = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    assert!(activated.dependent_rebuild_task_id().is_some());
    assert_ne!(activated.dependent_rebuild_task_id(), activated.task_id());
    let backend = fixture.backend.clone();
    fixture.cleanup().await;
    delete_backend_user(&backend, owner).await;
}

#[rstest::rstest]
#[case::create(false)]
#[case::update(true)]
#[actix_web::test]
async fn rejected_memory_class_policy_leaves_no_persisted_changes(
    #[case] update: bool,
    #[values(false, true)] enforced: bool,
) {
    let fixture = SchemaFixture::new(StorageBackendKind::Memory, vec![]).await;
    let before = fixture.backend.capture_backup_snapshot(true).await.unwrap();
    let result = if update {
        let target = fixture
            .backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(fixture.class_id()))
            .await
            .unwrap();
        fixture
            .backend
            .class_store()
            .update_class(
                &target,
                StorageClassUpdate::builder()
                    .json_schema(Some(json!({"type":7})))
                    .validate_schema(Some(enforced))
                    .build(),
                &EventContext::system(),
            )
            .await
    } else {
        fixture
            .backend
            .class_store()
            .create_class(
                StorageClassCreate::builder(
                    prefix("invalid_policy"),
                    fixture.collection_id(),
                    "invalid",
                )
                .schema_policy(
                    StorageClassSchemaPolicy::try_from_parts(Some(json!({"type":7})), enforced)
                        .unwrap(),
                )
                .build(),
                &EventContext::system(),
            )
            .await
    };
    assert!(result.is_err());
    let after = fixture.backend.capture_backup_snapshot(true).await.unwrap();
    assert_eq!(before.into_parts(), after.into_parts());
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::worker_error("Schema worker failed")]
#[case::shutdown("Task interrupted by graceful shutdown")]
#[actix_web::test]
async fn failed_schema_workers_publish_terminal_checkpoints(
    #[case] reason: &str,
    #[values(StorageBackendKind::Memory, StorageBackendKind::Postgres)] kind: StorageBackendKind,
    #[values(false, true)] process_batch: bool,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({}), json!({})]).await;
    let revision = fixture.stage(json!(true), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let limits = StorageSchemaBatchLimits::try_new(1, 1024, 1024).unwrap();
    if process_batch {
        fixture
            .backend
            .process_schema_work(lease.clone(), limits)
            .await
            .unwrap();
    }
    let before = fixture
        .backend
        .get_schema_work(work.task_id())
        .await
        .unwrap();
    let failed = fixture
        .backend
        .fail_task(StorageTaskFailure::new(
            lease.clone(),
            reason,
            StorageTaskEventInput::new("failed", reason),
        ))
        .await
        .unwrap();
    let after = fixture
        .backend
        .get_schema_work(work.task_id())
        .await
        .unwrap();
    assert_eq!(failed.status(), StorageTaskStatus::Failed);
    assert_eq!(after.status(), StorageSchemaWorkStatus::Failed);
    assert_eq!(after.examined(), before.examined());
    assert!(
        fixture
            .backend
            .process_schema_work(lease, limits)
            .await
            .is_err()
    );
    let replacement = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    assert_ne!(replacement.task_id(), work.task_id());
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::detail("detail")]
#[case::events("events")]
#[case::listing("list")]
#[case::filtered_listing("filtered")]
#[actix_web::test]
async fn generic_schema_tasks_require_administrator_report_access(
    #[case] endpoint: &str,
    #[values(StorageBackendKind::Memory, StorageBackendKind::Postgres)] kind: StorageBackendKind,
    #[values("member", "administrator", "scoped_administrator")] access: &str,
    #[values(false, true)] delegated: bool,
) {
    // These read tests claim work explicitly; automatic workers would consume
    // other tests' queued schema fixtures in the shared PostgreSQL database.
    static WORKER_SETTINGS: std::sync::Once = std::sync::Once::new();
    WORKER_SETTINGS.call_once(|| {
        let mut config = crate::tests::integration_test_config().unwrap();
        config.task_workers = 0;
        crate::tasks::initialize_task_worker_settings(config.task_worker_settings().unwrap())
            .unwrap();
    });
    let admin = access != "member";
    let scoped = access == "scoped_administrator";
    let allowed = admin && !scoped;
    let fixture = SchemaFixture::new(kind, vec![json!({})]).await;
    let user = create_backend_user(&fixture.backend, &prefix("schema_task_manager")).await;
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
    if admin != delegated {
        let administrator_group = match &fixture.environment {
            BackendTestEnvironment::Memory { .. } => group_id(1),
            BackendTestEnvironment::Postgres { pool } => {
                group_id(crate::tests::ensure_admin_group(pool).await.id)
            }
        };
        fixture
            .backend
            .add_group_member(
                user.principal_id,
                administrator_group,
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
    let revision = fixture.stage(json!(true), true).await;
    let proof = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let proof = fixture
        .finish(proof, StorageSchemaBatchLimits::default())
        .await;
    let activation = fixture
        .backend
        .activate_schema_revision(
            StorageSchemaActivation::new(
                fixture.collection_id(),
                revision.reference(),
                SchemaRevision::INITIAL,
                StorageSchemaActivationPolicy::RejectIncompatible,
                EventContext::user(user.principal_id, None, None),
            )
            .with_proof_task(Some(proof.task_id())),
        )
        .await
        .unwrap()
        .into_value();
    let task_id = activation.task_id().unwrap();
    let bearer = if scoped {
        let raw = prefix("scoped_schema_report_token");
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

    let permissions: Arc<dyn crate::permissions::PermissionBackend> = if delegated {
        use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
        let policy = MockTreetopBackend::new();
        let group = fixture.resources.owned_group.as_ref().unwrap().id().id();
        policy.add_task_read_rule(group, Some(task_id.id()));
        if admin {
            policy.add_admin_rule(group);
        }
        Arc::new(policy)
    } else {
        let config = crate::tests::integration_test_config().unwrap();
        Arc::new(LocalPermissionBackend::new(
            fixture.backend.clone(),
            config.admin_groupname,
        ))
    };
    let app = test::init_service(
        App::new()
            .app_data(Data::new(AppContext::new(
                fixture.backend.clone(),
                permissions,
            )))
            .configure(crate::api::config),
    )
    .await;
    let uri = match endpoint {
        "detail" => format!("/api/v1/tasks/{task_id}"),
        "events" => format!("/api/v1/tasks/{task_id}/events"),
        "filtered" => format!(
            "/api/v1/tasks?kind=schema_validation&submitted_by={}",
            user.principal_id
        ),
        _ => format!("/api/v1/tasks?submitted_by={}", user.principal_id),
    };
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(&uri)
            .insert_header((http::header::AUTHORIZATION, format!("Bearer {bearer}")))
            .to_request(),
    )
    .await;
    if matches!(endpoint, "detail" | "events") {
        assert_eq!(
            response.status(),
            if allowed {
                http::StatusCode::OK
            } else {
                http::StatusCode::NOT_FOUND
            }
        );
    } else {
        assert_eq!(response.status(), http::StatusCode::OK);
        if !allowed {
            assert_eq!(response.headers().get("X-Total-Count").unwrap(), "0");
            assert!(response.headers().get("X-Next-Cursor").is_none());
        }
        let tasks: Vec<Value> = test::read_body_json(response).await;
        assert_eq!(
            tasks.iter().any(|task| task["id"] == json!(task_id.id())),
            allowed
        );
        if !allowed {
            assert!(tasks.is_empty());
        }
    }
    delete_backend_user(&fixture.backend, user).await;
    fixture.cleanup().await;
}
