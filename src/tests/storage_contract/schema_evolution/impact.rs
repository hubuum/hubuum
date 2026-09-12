use super::*;
use crate::services::schema_evolution as service;

async fn install_baseline(fixture: &SchemaFixture, schema: Value) -> StorageSchemaRevision {
    let baseline = fixture.stage(schema, true).await;
    let activated = fixture
        .activate(
            &baseline,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    if let Some(task) = activated.task_id() {
        let work = fixture.backend.get_schema_work(task).await.unwrap();
        fixture
            .finish(work, StorageSchemaBatchLimits::default())
            .await;
    }
    baseline
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn an_older_staged_revision_requires_a_new_candidate_before_activation(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({})]).await;
    let candidate = fixture.stage(json!({"type":"object"}), true).await;
    install_baseline(&fixture, json!({"type":"object","maxProperties":10})).await;
    let queued = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let complete = fixture
        .finish(queued, StorageSchemaBatchLimits::default())
        .await;
    let report = service::get_work(&fixture.backend, complete.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(report).unwrap()["readiness"],
        "inconclusive"
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn impact_compares_both_policies_without_publishing_candidate_evidence(
    #[case] backend: StorageBackendKind,
) {
    let fixture = SchemaFixture::new(
        backend,
        vec![
            json!({"n":15}),
            json!({"n":5}),
            json!({"n":"private-value"}),
            json!({"n":10}),
        ],
    )
    .await;
    let baseline = install_baseline(
        &fixture,
        json!({"properties":{"n":{"type":"integer","minimum":10}}}),
    )
    .await;
    let before = serde_json::to_value(fixture.compliance().await).unwrap();
    let candidate = fixture
        .stage(
            json!({"properties":{"n":{"type":"integer","maximum":10}}}),
            true,
        )
        .await;
    let queued = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let boundary = fixture
        .backend
        .get_schema_impact_boundary(candidate.reference())
        .await
        .unwrap();
    assert_eq!(boundary.active(), baseline.reference());
    let complete = fixture
        .finish(
            queued,
            StorageSchemaBatchLimits::try_new(1, 8192, 4096).unwrap(),
        )
        .await;
    let report = serde_json::to_value(
        service::get_work(&fixture.backend, complete.task_id())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        report["impact"]["baseline"],
        serde_json::to_value(baseline.reference()).unwrap()
    );
    assert_eq!(
        report["impact"]["counts"],
        json!({"newly_invalid":1,"newly_valid":1,"still_invalid":1,"still_valid":1,"newly_required_valid":0,"no_longer_required":0,"unchanged_not_required":0,"uninspectable":0})
    );
    assert_eq!(report["readiness"], "incompatible");
    assert_eq!(
        serde_json::to_value(fixture.compliance().await).unwrap(),
        before
    );
    assert!(!report.to_string().contains("private-value"));
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory_add(StorageBackendKind::Memory, false)]
#[case::postgres_add(StorageBackendKind::Postgres, false)]
#[case::memory_remove(StorageBackendKind::Memory, true)]
#[case::postgres_remove(StorageBackendKind::Postgres, true)]
#[actix_web::test]
async fn impact_explains_adding_and_removing_enforcement(
    #[case] backend: StorageBackendKind,
    #[case] remove: bool,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({"hostname":"host"}), json!({})]).await;
    let schema = json!({"required":["hostname"],"properties":{"hostname":{"type":"string"}}});
    if remove {
        install_baseline(&fixture, schema.clone()).await;
    }
    let candidate = fixture.stage(schema, !remove).await;
    let queued = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let complete = fixture
        .finish(queued, StorageSchemaBatchLimits::default())
        .await;
    let report = serde_json::to_value(
        service::get_work(&fixture.backend, complete.task_id())
            .await
            .unwrap(),
    )
    .unwrap();
    if remove {
        assert_eq!(report["impact"]["counts"]["no_longer_required"], 2);
        assert_eq!(report["readiness"], "compatible");
    } else {
        assert_eq!(report["impact"]["counts"]["newly_required_valid"], 1);
        assert_eq!(report["impact"]["counts"]["newly_invalid"], 1);
        assert_eq!(
            report["impact"]["failures"][0]["reason"],
            json!({"keyword":"required","schema_path":"/required","missing_property":"hostname"})
        );
        assert_eq!(report["readiness"], "incompatible");
    }
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory_population(StorageBackendKind::Memory, "population")]
#[case::postgres_population(StorageBackendKind::Postgres, "population")]
#[case::memory_baseline(StorageBackendKind::Memory, "baseline")]
#[case::postgres_baseline(StorageBackendKind::Postgres, "baseline")]
#[case::memory_abandoned(StorageBackendKind::Memory, "abandoned")]
#[case::postgres_abandoned(StorageBackendKind::Postgres, "abandoned")]
#[actix_web::test]
async fn completed_impact_readiness_tracks_current_state(
    #[case] backend: StorageBackendKind,
    #[case] change: &str,
) {
    let fixture = SchemaFixture::new(backend, vec![json!({"n":1})]).await;
    let alternative = fixture.stage(json!({"type":"object"}), true).await;
    let candidate = fixture
        .stage(json!({"properties":{"n":{"type":"integer"}}}), true)
        .await;
    let queued = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let complete = fixture
        .finish(queued, StorageSchemaBatchLimits::default())
        .await;
    let report = service::get_work(&fixture.backend, complete.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(report).unwrap()["readiness"],
        "compatible"
    );
    match change {
        "population" => {
            fixture.update(0, json!({"n":2})).await.unwrap();
        }
        "baseline" => {
            fixture
                .activate(
                    &alternative,
                    SchemaRevision::INITIAL,
                    StorageSchemaActivationPolicy::AllowPending,
                    None,
                )
                .await
                .unwrap();
        }
        "abandoned" => {
            let abandoned = fixture
                .backend
                .abandon_schema_revision(
                    candidate.reference(),
                    fixture.collection_id(),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value();
            assert_eq!(abandoned.status(), StorageSchemaRevisionStatus::Abandoned);
        }
        _ => unreachable!(),
    }
    let report = service::get_work(&fixture.backend, complete.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(report).unwrap()["readiness"],
        "inconclusive"
    );
    assert!(
        fixture
            .activate(
                &candidate,
                fixture
                    .backend
                    .get_schema_state(fixture.class_id())
                    .await
                    .unwrap()
                    .active()
                    .reference()
                    .revision(),
                StorageSchemaActivationPolicy::RejectIncompatible,
                Some(complete.task_id())
            )
            .await
            .is_err()
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn impact_failure_groups_and_samples_remain_bounded_across_batches(
    #[case] backend: StorageBackendKind,
) {
    let mut properties = serde_json::Map::new();
    let mut values = Vec::new();
    for index in 0..25 {
        let key = format!("field{index:02}");
        properties.insert(key.clone(), json!({"type":"integer"}));
        for _ in 0..7 {
            values.push(json!({key.clone():"private-instance-value"}));
        }
    }
    let fixture = SchemaFixture::new(backend, values).await;
    let candidate = fixture.stage(json!({"properties":properties}), true).await;
    let queued = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let complete = fixture
        .finish(
            queued,
            StorageSchemaBatchLimits::try_new(4, 8192, 4096).unwrap(),
        )
        .await;
    let report = serde_json::to_value(&complete).unwrap();
    let groups = report["impact"]["failures"].as_array().unwrap();
    assert_eq!(groups.len(), 20);
    for group in groups {
        assert_eq!(group["objects"], 7);
        assert_eq!(group["samples"].as_array().unwrap().len(), 5);
    }
    assert_eq!(report["impact"]["ungrouped_failures"], 35);
    assert!(!report.to_string().contains("private-instance-value"));
    let restored: StorageSchemaWork = serde_json::from_value(report).unwrap();
    assert_eq!(restored.examined(), 175);
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::memory(StorageBackendKind::Memory)]
#[case::postgres(StorageBackendKind::Postgres)]
#[actix_web::test]
async fn impact_budget_rejection_is_inconclusive_without_false_mismatches(
    #[case] backend: StorageBackendKind,
) {
    let limits = JsonSchemaLimits::builder()
        .instance_work(1024)
        .build()
        .unwrap();
    let fixture =
        SchemaFixture::with_limits(backend, vec![json!({"payload":"x".repeat(2048)})], limits)
            .await;
    let candidate = fixture
        .stage(json!({"properties":{"payload":{"type":"string"}}}), true)
        .await;
    let queued = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let complete = fixture
        .finish(queued, StorageSchemaBatchLimits::default())
        .await;
    let report = serde_json::to_value(
        service::get_work(&fixture.backend, complete.task_id())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(report["readiness"], "inconclusive");
    assert_eq!(report["impact"]["counts"]["uninspectable"], 1);
    assert_eq!(report["invalid"], 0);
    assert_eq!(report["uninspectable"], 1);
    fixture.cleanup().await;
}
