use super::super::*;
use crate::models::{NewHubuumClass, NewHubuumObject};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use crate::tests::{TestContext, create_test_group};
use crate::traits::CanSave;
use hubuum_storage_postgres::capture_queries;
use std::sync::Arc;

#[actix_web::test]
async fn external_policy_export_crosses_bounded_pages_with_sparse_grants() {
    let context = TestContext::new().await;
    let fixture = context.collection_fixture("sparse_export_evidence").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("sparse_export_class"),
        description: "export evidence".into(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut allowed_ids = Vec::new();
    for index in 0..300 {
        let object = NewHubuumObject {
            collection_id: fixture.collection.id,
            hubuum_class_id: class.id,
            name: context.scoped_name(&format!("sparse_export_{index:03}")),
            description: "export evidence".into(),
            data: json!({"payload":"x".repeat(8192)}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        if (256..259).contains(&index) {
            allowed_ids.push(object.id);
        }
    }
    let policy = Arc::new(MockTreetopBackend::new());
    for id in &allowed_ids {
        policy.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(*id),
            attrs: ResourceFields::default(),
        });
    }
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        policy.clone(),
    );
    let exporter = PermissionAwareExport::new(&backend, &context.normal_user, None)
        .await
        .unwrap();
    let query = parse_query_parameter(&format!("class_id={}&sort=id&limit=3", class.id)).unwrap();
    let started = std::time::Instant::now();
    let (result, queries) = capture_queries(exporter.objects(query)).await;
    let objects = result.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        objects.iter().map(|object| object.id).collect::<Vec<_>>(),
        allowed_ids
    );
    let batches = policy.authorization_batch_sizes();
    assert_eq!(batches, vec![128, 128, 44]);
    eprintln!(
        "PERFORMANCE_EVIDENCE {}",
        json!({"scenario":"external_policy_sparse_export", "candidates":300, "candidate_payload_bytes":8192, "policy_batches":batches,
        "output_rows":objects.len(), "elapsed_us":elapsed.as_micros(), "queries":queries.total_queries(), "policy_transport":"in-process deterministic backend"})
    );
    fixture.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}
