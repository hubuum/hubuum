use super::*;
use hubuum_storage_core::{
    StorageImportClassKey, StorageImportClassKeyParts, StorageImportCollectionKey,
    StorageImportCollectionKeyParts, StorageImportObject, StorageImportObjectParts,
    StorageImportOperation, StorageImportPlan, StorageImportPlanItem, TransactionStorage,
};

#[derive(Clone, Copy, Debug)]
enum WritePath {
    Direct,
    Transaction,
    Import,
}

#[rstest::rstest]
#[case::memory_strict(StorageBackendKind::Memory, 1024, false)]
#[case::memory_relaxed(StorageBackendKind::Memory, 4096, true)]
#[case::postgres_strict(StorageBackendKind::Postgres, 1024, false)]
#[case::postgres_relaxed(StorageBackendKind::Postgres, 4096, true)]
#[actix_web::test]
async fn configured_instance_limits_apply_to_object_writes(
    #[case] kind: StorageBackendKind,
    #[case] bytes: usize,
    #[case] accepted: bool,
    #[values(WritePath::Direct, WritePath::Transaction, WritePath::Import)] path: WritePath,
) {
    let limits = JsonSchemaLimits::builder()
        .instance_bytes(bytes)
        .build()
        .unwrap();
    let fixture = SchemaFixture::with_limits(kind, vec![json!("small")], limits).await;
    let revision = fixture.stage(json!({"type":"string"}), true).await;
    fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let value = json!("x".repeat(2048));
    let result = match path {
        WritePath::Transaction => {
            let selector = StorageObjectSelector::Ids {
                class_id: fixture.class_id(),
                object_id: fixture.resources.objects[0].id(),
            };
            fixture
                .backend
                .with_transaction(EventContext::system(), move |transaction| {
                    Box::pin(async move {
                        let object = transaction.objects().resolve(selector).await?;
                        transaction
                            .objects()
                            .update(
                                &object,
                                StorageObjectUpdate::builder().data(Some(value)).build(),
                            )
                            .await
                            .map(StorageMutationOutcome::into_value)
                    })
                })
                .await
        }
        WritePath::Direct => fixture.update(0, value).await,
        WritePath::Import => {
            let original = &fixture.resources.objects[0];
            let input = StorageImportObject::from_parts(StorageImportObjectParts {
                reference: None,
                name: original.name().into(),
                description: "budget import".into(),
                data: value,
                class_ref: None,
                class_key: Some(StorageImportClassKey::from_parts(
                    StorageImportClassKeyParts {
                        name: fixture.resources.class.name().into(),
                        collection_ref: None,
                        collection_key: Some(StorageImportCollectionKey::from_parts(
                            StorageImportCollectionKeyParts {
                                name: fixture.resources.collection.name().into(),
                                path: None,
                            },
                        )),
                    },
                )),
                condition: None,
                timestamps: None,
            });
            let plan = StorageImportPlan::try_new(vec![StorageImportPlanItem::new(
                0,
                StorageImportOperation::UpdateObject {
                    object_id: original.id(),
                    input,
                },
            )])
            .unwrap();
            fixture
                .backend
                .apply_import_strict(plan)
                .await
                .map(|()| original.clone())
        }
    };
    assert_eq!(result.is_ok(), accepted, "{path:?}: {result:?}");
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn configured_work_limits_apply_to_background_evidence(#[case] kind: StorageBackendKind) {
    let limits = JsonSchemaLimits::builder()
        .instance_work(1024)
        .build()
        .unwrap();
    let fixture = SchemaFixture::with_limits(kind, vec![json!("x".repeat(2048))], limits).await;
    let revision = fixture.stage(json!({"type":"string"}), true).await;
    let activation = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let lease = fixture.claim(activation.task_id().unwrap(), 60_000).await;
    fixture
        .finish_claimed(lease, StorageSchemaBatchLimits::default())
        .await;
    assert_eq!(
        fixture.compliance().await[0].status(),
        StorageComplianceStatus::Invalid
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_proofs_cannot_bypass_deployment_budgets(#[case] kind: StorageBackendKind) {
    let limits = JsonSchemaLimits::builder()
        .schema_bytes(1024)
        .build()
        .unwrap();
    let fixture = SchemaFixture::with_limits(kind, vec![], limits).await;
    let before = fixture
        .backend
        .list_schema_revisions(StorageSchemaPage::try_new(fixture.class_id(), 0, 100).unwrap())
        .await
        .unwrap()
        .len();
    let policy = StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Advisory(
        json!({"description":"x".repeat(2048)}),
    ))
    .unwrap();
    let result = fixture
        .backend
        .stage_schema_revision(StorageSchemaStage::new(
            fixture.collection_id(),
            fixture.class_id(),
            policy,
            EventContext::system(),
        ))
        .await;
    assert!(result.is_err());
    assert_eq!(
        fixture
            .backend
            .list_schema_revisions(StorageSchemaPage::try_new(fixture.class_id(), 0, 100).unwrap())
            .await
            .unwrap()
            .len(),
        before
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn configured_schema_limits_survive_backup_history_validation(
    #[case] kind: StorageBackendKind,
) {
    let limits = JsonSchemaLimits::builder()
        .schema_bytes(96 * 1024)
        .build()
        .unwrap();
    let fixture = SchemaFixture::with_limits(kind, vec![], limits).await;
    let revision = fixture
        .stage(json!({"description":"x".repeat(2048)}), false)
        .await;
    fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::RejectIncompatible,
            None,
        )
        .await
        .unwrap();
    let snapshot = fixture.backend.capture_backup_snapshot(true).await.unwrap();
    assert_eq!(snapshot.schema_limits(), limits);
    // Retained PostgreSQL history also participates in other suite backups.
    // Keep these rows readable under the suite defaults; the independent memory
    // restore regression exercises a document above the default schema ceiling.
    let strict = JsonSchemaLimits::builder()
        .schema_bytes(1024)
        .build()
        .unwrap();
    assert!(snapshot.with_schema_limits(strict).is_err());
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn worker_admits_objects_above_the_former_one_mib_ceiling(#[case] kind: StorageBackendKind) {
    let fixture = SchemaFixture::new(kind, vec![json!("x".repeat(1024 * 1024 + 1024))]).await;
    let revision = fixture.stage(json!({"type":"string"}), true).await;
    let activation = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let lease = fixture.claim(activation.task_id().unwrap(), 60_000).await;
    fixture
        .finish_claimed(
            lease,
            StorageSchemaBatchLimits::for_schema_limits(fixture.backend.schema_limits()),
        )
        .await;
    assert_eq!(
        fixture.compliance().await[0].status(),
        StorageComplianceStatus::Valid
    );
    fixture.cleanup().await;
}
