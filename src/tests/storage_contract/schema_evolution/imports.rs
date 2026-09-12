use super::*;
use hubuum_storage_core::{
    StorageImportAtomicity, StorageImportClass, StorageImportClassKey, StorageImportClassKeyParts,
    StorageImportClassParts, StorageImportCollectionKey, StorageImportCollectionKeyParts,
    StorageImportCollisionPolicy, StorageImportMode, StorageImportObject, StorageImportObjectParts,
    StorageImportOperation, StorageImportPermissionPolicy, StorageImportPlan,
    StorageImportPlanItem,
};

impl SchemaFixture {
    fn import_activation(&self, revision: &StorageSchemaRevision) -> StorageImportPlan {
        let collection_key =
            StorageImportCollectionKey::from_parts(StorageImportCollectionKeyParts {
                name: self.resources.collection.name().into(),
                path: None,
            });
        StorageImportPlan::try_new(vec![
            StorageImportPlanItem::new(
                0,
                StorageImportOperation::UpdateClass {
                    class_id: self.class_id(),
                    input: StorageImportClass::from_parts(StorageImportClassParts {
                        schema_activation: Some(StorageImportSchemaActivation::new(
                            revision.reference().revision(),
                            SchemaRevision::INITIAL,
                            StorageSchemaActivationPolicy::AllowPending,
                            None,
                        )),
                        reference: None,
                        name: self.resources.class.name().into(),
                        description: "imported policy".into(),
                        schema_policy: revision.policy().policy().clone(),
                        collection_ref: None,
                        collection_key: Some(collection_key.clone()),
                        condition: None,
                        timestamps: None,
                    }),
                },
            ),
            StorageImportPlanItem::new(
                1,
                StorageImportOperation::UpdateObject {
                    object_id: self.resources.objects[0].id(),
                    input: StorageImportObject::from_parts(StorageImportObjectParts {
                        reference: None,
                        name: self.resources.objects[0].name().into(),
                        description: "invalid import".into(),
                        data: json!({"n":"invalid"}),
                        class_ref: None,
                        class_key: Some(StorageImportClassKey::from_parts(
                            StorageImportClassKeyParts {
                                name: self.resources.class.name().into(),
                                collection_ref: None,
                                collection_key: Some(collection_key),
                            },
                        )),
                        condition: None,
                        timestamps: None,
                    }),
                },
            ),
        ])
        .unwrap()
    }
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn strict_schema_import_rolls_back_activation_and_queued_work(
    #[case] kind: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let revision = fixture
        .stage(json!({"properties":{"n":{"type":"integer"}}}), true)
        .await;
    let error = fixture
        .backend
        .apply_import_strict(fixture.import_activation(&revision))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
    let state = fixture
        .backend
        .get_schema_state(fixture.class_id())
        .await
        .unwrap();
    assert_eq!(
        state.active().reference().revision(),
        SchemaRevision::INITIAL
    );
    assert_eq!(
        fixture.compliance().await[0].status(),
        StorageComplianceStatus::NotRequired
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn best_effort_schema_import_commits_policy_and_reports_object_failure(
    #[case] kind: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let revision = fixture
        .stage(json!({"properties":{"n":{"type":"integer"}}}), true)
        .await;
    let (results, aborted) = fixture
        .backend
        .apply_import_best_effort(
            fixture.import_activation(&revision),
            StorageImportMode::new(
                StorageImportAtomicity::BestEffort,
                StorageImportCollisionPolicy::Overwrite,
                StorageImportPermissionPolicy::Continue,
            ),
        )
        .await
        .unwrap()
        .into_parts();
    assert!(!aborted);
    assert!(results[0].error().is_none(), "{:?}", results[0]);
    assert!(results[1].error().is_some());
    let state = fixture
        .backend
        .get_schema_state(fixture.class_id())
        .await
        .unwrap();
    assert_eq!(state.active().reference(), revision.reference());
    assert_eq!(
        fixture.compliance().await[0].status(),
        StorageComplianceStatus::Pending
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn failed_schema_import_item_leaves_the_policy_unchanged_for_later_items(
    #[case] kind: StorageBackendKind,
    #[values(false, true)] preflight: bool,
) {
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let conflicting = fixture
        .backend
        .class_store()
        .create_class(
            StorageClassCreate::builder(prefix("conflicting"), fixture.collection_id(), "").build(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    let revision = fixture
        .stage(json!({"properties":{"n":{"type":"integer"}}}), true)
        .await;
    let events_query = StorageAuditEventListQuery::new(
        vec![fixture.collection_id()],
        false,
        StorageAuditEventFilters::new().entity_type(Some(EntityType::ClassSchema)),
        QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
    );
    let before_events = fixture
        .backend
        .list_audit_events(events_query.clone())
        .await
        .unwrap();
    let items = fixture
        .import_activation(&revision)
        .into_items()
        .into_iter()
        .map(|item| {
            let (index, operation) = item.into_parts();
            let operation = match operation {
                StorageImportOperation::UpdateClass { class_id, input } => {
                    let mut parts = input.into_parts();
                    parts.name = conflicting.name().into();
                    StorageImportOperation::UpdateClass {
                        class_id,
                        input: StorageImportClass::from_parts(parts),
                    }
                }
                other => other,
            };
            StorageImportPlanItem::new(index, operation)
        })
        .collect();
    let plan = StorageImportPlan::try_new(items).unwrap();
    let mode = StorageImportMode::new(
        StorageImportAtomicity::BestEffort,
        StorageImportCollisionPolicy::Overwrite,
        StorageImportPermissionPolicy::Continue,
    );
    let errors = if preflight {
        let (results, aborted) = fixture
            .backend
            .preflight_import(plan, mode)
            .await
            .unwrap()
            .into_parts();
        assert!(!aborted);
        results
            .into_iter()
            .map(|item| item.into_parts().2)
            .collect::<Vec<_>>()
    } else {
        let (results, aborted) = fixture
            .backend
            .apply_import_best_effort(plan, mode)
            .await
            .unwrap()
            .into_parts();
        assert!(!aborted);
        results
            .into_iter()
            .map(|item| item.into_parts().1)
            .collect::<Vec<_>>()
    };
    assert_eq!(errors.len(), 2);
    assert_eq!(
        errors[0].as_ref().unwrap().kind(),
        StorageErrorKind::Conflict
    );
    assert!(
        errors[1].is_none(),
        "later object must use the original policy: {:?}",
        errors[1]
    );
    let state = fixture
        .backend
        .get_schema_state(fixture.class_id())
        .await
        .unwrap();
    assert_eq!(
        state.active().reference().revision(),
        SchemaRevision::INITIAL
    );
    let revisions = fixture
        .backend
        .list_schema_revisions(StorageSchemaPage::try_new(fixture.class_id(), 0, 100).unwrap())
        .await
        .unwrap();
    assert_eq!(revisions[1].status(), StorageSchemaRevisionStatus::Staged);
    let after_events = fixture
        .backend
        .list_audit_events(events_query)
        .await
        .unwrap();
    assert_eq!(
        before_events.into_parts().0.len(),
        after_events.into_parts().0.len()
    );
    if let BackendTestEnvironment::Memory { storage } = &fixture.environment {
        assert!(
            storage
                .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
    let target = fixture
        .backend
        .class_store()
        .resolve_class(StorageClassSelector::Id(conflicting.id()))
        .await
        .unwrap();
    fixture
        .backend
        .class_store()
        .delete_class(&target, &EventContext::system())
        .await
        .unwrap()
        .into_value();
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn schema_import_preflight_does_not_publish_activation(#[case] kind: StorageBackendKind) {
    let fixture = SchemaFixture::new(kind, vec![json!({"n":1})]).await;
    let revision = fixture
        .stage(json!({"properties":{"n":{"type":"integer"}}}), true)
        .await;
    let (results, _) = fixture
        .backend
        .preflight_import(
            fixture.import_activation(&revision),
            StorageImportMode::new(
                StorageImportAtomicity::BestEffort,
                StorageImportCollisionPolicy::Overwrite,
                StorageImportPermissionPolicy::Continue,
            ),
        )
        .await
        .unwrap()
        .into_parts();
    let errors = results
        .into_iter()
        .map(|item| item.into_parts().2)
        .collect::<Vec<_>>();
    assert!(errors[0].is_none(), "{:?}", errors[0]);
    assert!(errors[1].is_some());
    assert_eq!(
        fixture
            .backend
            .get_schema_state(fixture.class_id())
            .await
            .unwrap()
            .active()
            .reference()
            .revision(),
        SchemaRevision::INITIAL
    );
    fixture.cleanup().await;
}
