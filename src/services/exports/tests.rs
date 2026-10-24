use hubuum_domain::{ClassId, TemplateAlias};

mod performance;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::models::search::QueryOptions;
use crate::models::{
    ExportContentType, ExportInclude, ExportIncludeRelatedDirection, ExportIncludeRelatedObject,
    ExportIncludeRelatedQuery, ExportIncludeRelatedSort, ExportLimits, ExportMissingDataPolicy,
    ExportRelationContext, ExportRequest, ExportScope, ExportScopeKind, ExportTemplate,
    ExportTemplateKind, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    NewHubuumObjectRelation, Permissions,
};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};

use super::{
    AuthorizationCandidates, ExportRuntime, HydrationBudget, ObjectGraphEdge,
    PermissionAwareExport, RelationHydrationPlan, build_template_items, inferred_relation_alias,
    load_hydration_class_metadata, normalize_alias_segment, object_with_root_path, pluralize_alias,
    take_related_within_budget, validate_export_limits, validate_export_submission,
};
use crate::errors::ApiError;
use crate::tests::{TestContext, create_test_group};
use crate::traits::CanSave;
use hubuum_storage_postgres::capture_queries;

fn test_timestamp() -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .naive_utc()
}

#[test]
fn authorization_candidates_reject_mismatched_resources() {
    let result = AuthorizationCandidates::new(
        vec![1, 2],
        vec![crate::permissions::ResourceRef::collection(1)],
    );

    assert!(matches!(result, Err(ApiError::InternalServerError(_))));
}

#[test]
fn object_graph_edge_is_independent_of_relation_direction() {
    let forward = ObjectGraphEdge::new(3, 7).unwrap();
    let reverse = ObjectGraphEdge::new(7, 3).unwrap();

    assert_eq!(forward, reverse);
}

#[tokio::test]
async fn permission_aware_export_uses_external_backend_visibility() {
    let context = TestContext::new().await;
    let visible = context.collection_fixture("external_export_visible").await;
    let hidden = context.collection_fixture("external_export_hidden").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();

    let visible_class = NewHubuumClass {
        collection_id: visible.collection.id,
        name: context.scoped_name("external_export_visible_class"),
        description: "visible class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let hidden_class = NewHubuumClass {
        collection_id: hidden.collection.id,
        name: context.scoped_name("external_export_hidden_class"),
        description: "hidden class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let visible_object = NewHubuumObject {
        collection_id: visible.collection.id,
        hubuum_class_id: visible_class.id,
        name: context.scoped_name("external_export_visible_object"),
        description: "visible object".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    NewHubuumObject {
        collection_id: hidden.collection.id,
        hubuum_class_id: hidden_class.id,
        name: context.scoped_name("external_export_hidden_object"),
        description: "hidden object".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let permission_backend = MockTreetopBackend::new();
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: None,
        attrs: ResourceFields {
            collection_id: Some(visible.collection.id),
            ..Default::default()
        },
    });
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        Arc::new(permission_backend),
    );
    let exporter = PermissionAwareExport::new(&backend, &context.normal_user, None)
        .await
        .unwrap();
    let objects = exporter
        .objects(
            QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, false)
                .expect("test query must be valid"),
        )
        .await
        .unwrap();

    assert_eq!(
        objects.iter().map(|object| object.id).collect::<Vec<_>>(),
        vec![visible_object.id]
    );

    visible.cleanup().await.unwrap();
    hidden.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[tokio::test]
async fn export_observes_one_query_across_authorization_batches() {
    use crate::permissions::AppContext;
    use crate::storage::StorageHandle;
    use hubuum_domain::{ClassId, CollectionId};
    use hubuum_storage_core::{StorageQueryObservationSettings, StorageQueryUsageScope};

    let context = TestContext::new().await;
    let fixture = context.collection_fixture("observed_export").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("observed_export_class"),
        description: String::new(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    for index in 0..140 {
        NewHubuumObject {
            collection_id: fixture.collection.id,
            hubuum_class_id: class.id,
            name: context.scoped_name(&format!("observed_export_{index}")),
            description: String::new(),
            data: serde_json::json!({"serial": "sample"}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
    }
    let policy = Arc::new(MockTreetopBackend::new());
    policy.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: None,
        attrs: ResourceFields::default(),
    });
    let storage = StorageHandle::postgres(context.pool.get_ref().clone()).with_query_observations(
        StorageQueryObservationSettings::try_new(true, 1, 64, 64, 60, 16).unwrap(),
    );
    let backend = AppContext::new(storage.clone(), policy.clone());
    let exporter = PermissionAwareExport::new(&backend, &context.normal_user, None)
        .await
        .unwrap();
    let options = crate::models::search::parse_query_parameter(&format!(
        "class_id={}&json_data=serial=sample&limit=140",
        class.id,
    ))
    .unwrap();
    let objects = exporter.objects(options).await.unwrap();
    assert_eq!(objects.len(), 140);
    assert!(policy.authorization_batch_sizes().len() > 1);
    let report = serde_json::to_value(
        storage
            .analyze_query_usage(
                StorageQueryUsageScope::new(
                    ClassId::new(class.id).unwrap(),
                    CollectionId::new(fixture.collection.id).unwrap(),
                ),
                vec![],
            )
            .await
            .unwrap(),
    )
    .unwrap();
    fixture.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
    assert_eq!(report["observations"]["patterns"][0]["sampled_queries"], 1);
}

#[tokio::test]
async fn hydration_metadata_hides_unauthorized_class_relations() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("external_export_hydration_metadata")
        .await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();

    let source_class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("external_export_source_class"),
        description: "source class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let hidden_adjacent_class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("external_export_hidden_adjacent_class"),
        description: "hidden adjacent class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(source_class.id).expect("positive fixture class id"),
        to_hubuum_class_id: ClassId::new(hidden_adjacent_class.id)
            .expect("positive fixture class id"),
        forward_label: None,
        reverse_label: None,
        forward_template_alias: Some(
            TemplateAlias::try_from("secret_alias".to_string()).expect("valid fixture alias"),
        ),
        reverse_template_alias: Some(
            TemplateAlias::try_from("secret_reverse_alias".to_string())
                .expect("valid fixture alias"),
        ),
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let source_object = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: source_class.id,
        name: context.scoped_name("external_export_source_object"),
        description: "source object".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let permission_backend = MockTreetopBackend::new();
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: Some(source_object.id),
        attrs: ResourceFields::default(),
    });
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        Arc::new(permission_backend),
    );
    let exporter = PermissionAwareExport::new(&backend, &context.normal_user, None)
        .await
        .unwrap();
    let objects = BTreeMap::from([(source_object.id, object_with_root_path(&source_object))]);

    let metadata = load_hydration_class_metadata(&exporter, &objects)
        .await
        .unwrap();

    assert_eq!(
        metadata.resolve_class_names.get(&source_class.id),
        Some(&source_class.name)
    );
    assert!(
        !metadata
            .resolve_class_names
            .contains_key(&hidden_adjacent_class.id)
    );
    assert!(
        metadata
            .class_relations_by_object_class
            .get(&source_class.id)
            .is_none_or(Vec::is_empty)
    );

    fixture.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[tokio::test]
async fn external_graph_authorization_preserves_allowed_alternative_path() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("external_export_alternative_path")
        .await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();

    let endpoint_class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("external_export_endpoint_class"),
        description: "endpoint class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let intermediate_class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("external_export_intermediate_class"),
        description: "intermediate class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let outbound_class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(endpoint_class.id).expect("positive fixture class id"),
        to_hubuum_class_id: ClassId::new(intermediate_class.id).expect("positive fixture class id"),
        forward_label: None,
        reverse_label: None,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let root = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: endpoint_class.id,
        name: context.scoped_name("external_export_path_root"),
        description: "path root".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let denied_intermediate = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: intermediate_class.id,
        name: context.scoped_name("external_export_denied_intermediate"),
        description: "denied intermediate".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let allowed_intermediate = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: intermediate_class.id,
        name: context.scoped_name("external_export_allowed_intermediate"),
        description: "allowed intermediate".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let target = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: endpoint_class.id,
        name: context.scoped_name("external_export_path_target"),
        description: "path target".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let _denied_first_edge = NewHubuumObjectRelation {
        from_hubuum_object_id: root.id,
        to_hubuum_object_id: denied_intermediate.id,
        class_relation_id: outbound_class_relation.id.id(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let _denied_second_edge = NewHubuumObjectRelation {
        from_hubuum_object_id: denied_intermediate.id,
        to_hubuum_object_id: target.id,
        class_relation_id: outbound_class_relation.id.id(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let allowed_first_edge = NewHubuumObjectRelation {
        from_hubuum_object_id: root.id,
        to_hubuum_object_id: allowed_intermediate.id,
        class_relation_id: outbound_class_relation.id.id(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let allowed_second_edge = NewHubuumObjectRelation {
        from_hubuum_object_id: allowed_intermediate.id,
        to_hubuum_object_id: target.id,
        class_relation_id: outbound_class_relation.id.id(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let permission_backend = MockTreetopBackend::new();
    for object_id in [root.id, allowed_intermediate.id, target.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object_id),
            attrs: ResourceFields::default(),
        });
    }
    for relation_id in [allowed_first_edge.id, allowed_second_edge.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObjectRelation,
            resource_kind: ResourceKind::ObjectRelation,
            resource_id: Some(relation_id),
            attrs: ResourceFields::default(),
        });
    }
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        Arc::new(permission_backend),
    );
    let exporter = PermissionAwareExport::new(&backend, &context.normal_user, None)
        .await
        .unwrap();

    let included = exporter
        .related_objects_for_roots(
            &[root.id],
            ExportIncludeRelatedQuery {
                class_id: endpoint_class.id,
                class_relation_id: None,
                direction: ExportIncludeRelatedDirection::Outgoing,
                sort: ExportIncludeRelatedSort::Path,
                max_depth: 2,
                limit: 10,
            },
        )
        .await
        .unwrap();
    let included_target = included
        .iter()
        .find(|candidate| candidate.descendant_object_id == target.id)
        .unwrap();
    assert_eq!(
        included_target.path,
        vec![root.id, allowed_intermediate.id, target.id]
    );

    let hydrated = exporter
        .bidirectionally_related_objects_for_roots(&[root.id], 2, 10)
        .await
        .unwrap();
    let hydrated_target = hydrated
        .iter()
        .find(|candidate| candidate.descendant_object_id == target.id)
        .unwrap();
    assert_eq!(
        hydrated_target.path,
        vec![root.id, allowed_intermediate.id, target.id]
    );

    fixture.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

fn test_object_with_path(id: i32) -> crate::models::HubuumObjectWithPath {
    crate::models::HubuumObjectWithPath {
        id,
        name: format!("object-{id}"),
        collection_id: 1,
        hubuum_class_id: 1,
        data: serde_json::json!({}),
        description: String::new(),
        created_at: test_timestamp(),
        updated_at: test_timestamp(),
        revision: crate::models::ResourceRevision::INITIAL,
        path: vec![id],
    }
}

#[test]
fn take_related_within_budget_allows_within_capacity() {
    let mut budget = HydrationBudget::new(5);
    budget.count_object().unwrap(); // hydrated=1, remaining=4, cap=3
    let kept = take_related_within_budget(
        &budget,
        vec![test_object_with_path(10), test_object_with_path(11)],
    )
    .unwrap();
    assert_eq!(kept.len(), 2);
}

#[test]
fn take_related_within_budget_errors_when_second_root_exceeds_remaining() {
    let mut budget = HydrationBudget::new(5);
    // Simulate the first root consuming three objects.
    budget.count_object().unwrap();
    budget.count_object().unwrap();
    budget.count_object().unwrap(); // hydrated=3, remaining=2, cap=1
    let err = take_related_within_budget(
        &budget,
        vec![
            test_object_with_path(10),
            test_object_with_path(11),
            test_object_with_path(12),
        ],
    )
    .unwrap_err();
    match err {
        ApiError::BadRequest(message) => assert_eq!(
            message,
            "Hydrated template object limit exceeded (2 related objects > 1 remaining related capacity)"
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn take_related_within_budget_errors_when_exhausted() {
    let mut budget = HydrationBudget::new(2);
    budget.count_object().unwrap();
    budget.count_object().unwrap(); // hydrated=2, remaining=0
    let err = take_related_within_budget(&budget, vec![test_object_with_path(10)]).unwrap_err();
    match err {
        ApiError::BadRequest(message) => {
            assert_eq!(message, "Hydrated template object limit exceeded (2 >= 2)")
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

fn export_with_limits(limits: ExportLimits) -> ExportRequest {
    ExportRequest {
        scope: ExportScope {
            kind: ExportScopeKind::ObjectsInClass,
            class_id: Some(1),
            object_id: None,
        },
        query: None,
        missing_data_policy: None,
        limits: Some(limits),
        include: None,
        relation_context: None,
    }
}

fn templated_export_with_include(
    related_objects: HashMap<String, ExportIncludeRelatedObject>,
) -> ExportRequest {
    ExportRequest {
        scope: ExportScope {
            kind: ExportScopeKind::ObjectsInClass,
            class_id: Some(1),
            object_id: None,
        },
        query: None,
        missing_data_policy: None,
        limits: None,
        include: Some(ExportInclude {
            related_objects: Some(related_objects),
        }),
        relation_context: Some(ExportRelationContext { depth: Some(1) }),
    }
}

fn export_runtime(export: ExportRequest) -> ExportRuntime {
    let scope = export.scope.validate().unwrap();
    ExportRuntime {
        export,
        scope,
        missing_data_policy: ExportMissingDataPolicy::Strict,
        template: Some(ExportTemplate {
            id: 1,
            collection_id: 1,
            name: "summary".to_string(),
            description: String::new(),
            content_type: ExportContentType::TextPlain,
            template: "{{ items|length }}".to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
            class_id: Some(1),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
            created_at: test_timestamp(),
            updated_at: test_timestamp(),
            revision: crate::models::ResourceRevision::INITIAL,
        }),
        collection_templates: Vec::new(),
    }
}

#[actix_web::test]
async fn object_hydration_query_count_is_constant_with_root_count() {
    let context = TestContext::new().await;
    let scope = &context.scope;
    let fixture = context
        .collection_fixture("query_budget_export_hydration")
        .await;
    fixture
        .owner_group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .expect("normal user should join the fixture owner group");
    let host_class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("query_budget_export_host"),
        description: "query budget export host".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("host class should save");
    let room_class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("query_budget_export_room"),
        description: "query budget export room".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("room class should save");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(host_class.id).expect("positive fixture class id"),
        to_hubuum_class_id: ClassId::new(room_class.id).expect("positive fixture class id"),
        forward_label: None,
        reverse_label: None,
        forward_template_alias: Some(
            TemplateAlias::try_from("rooms".to_string()).expect("valid fixture alias"),
        ),
        reverse_template_alias: Some(
            TemplateAlias::try_from("hosts".to_string()).expect("valid fixture alias"),
        ),
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class relation should save");

    let mut hosts = Vec::new();
    for index in 0..10 {
        let host = NewHubuumObject {
            collection_id: fixture.collection.id,
            hubuum_class_id: host_class.id,
            name: scope.scoped_name(&format!("query_budget_export_host_{index:02}")),
            description: "query budget export host".to_string(),
            data: serde_json::json!({"index": index}),
        }
        .save_without_events(&scope.pool)
        .await
        .expect("host should save");
        let room = NewHubuumObject {
            collection_id: fixture.collection.id,
            hubuum_class_id: room_class.id,
            name: scope.scoped_name(&format!("query_budget_export_room_{index:02}")),
            description: "query budget export room".to_string(),
            data: serde_json::json!({"index": index}),
        }
        .save_without_events(&scope.pool)
        .await
        .expect("room should save");
        NewHubuumObjectRelation {
            from_hubuum_object_id: host.id,
            to_hubuum_object_id: room.id,
            class_relation_id: class_relation.id.id(),
        }
        .save_without_events(&scope.pool)
        .await
        .expect("object relation should save");
        hosts.push(serde_json::to_value(host).expect("host should serialize"));
    }

    let runtime = export_runtime(ExportRequest {
        scope: ExportScope {
            kind: ExportScopeKind::ObjectsInClass,
            class_id: Some(host_class.id),
            object_id: None,
        },
        query: None,
        missing_data_policy: None,
        limits: None,
        include: None,
        relation_context: Some(ExportRelationContext { depth: Some(1) }),
    });
    let exporter = PermissionAwareExport::new(&context.pool, &context.normal_user, None)
        .await
        .expect("permission-aware exporter should initialize");

    let (small_hydration, small_queries) = capture_queries(build_template_items(
        &exporter,
        &runtime,
        &hosts[..1],
        Some(RelationHydrationPlan {
            depth_limit: 1,
            enabled_for_scope: true,
        }),
    ))
    .await;
    assert_eq!(
        small_hydration
            .expect("small hydration should succeed")
            .0
            .len(),
        1
    );

    let (large_hydration, large_queries) = capture_queries(build_template_items(
        &exporter,
        &runtime,
        &hosts,
        Some(RelationHydrationPlan {
            depth_limit: 1,
            enabled_for_scope: true,
        }),
    ))
    .await;
    assert_eq!(
        large_hydration
            .expect("large hydration should succeed")
            .0
            .len(),
        10
    );

    assert_eq!(large_queries.total_queries(), small_queries.total_queries());
    assert_eq!(
        large_queries.domain_queries(),
        small_queries.domain_queries()
    );
    assert_eq!(
        large_queries.control_queries(),
        small_queries.control_queries()
    );
    assert_eq!(
        large_queries.connection_checkouts(),
        small_queries.connection_checkouts()
    );
    assert_eq!(
        large_queries.total_queries(),
        7,
        "{:#?}",
        large_queries.query_counts()
    );
    assert_eq!(large_queries.domain_queries(), 7);
    assert_eq!(large_queries.control_queries(), 0);
    assert_eq!(large_queries.connection_checkouts(), 4);
    assert_eq!(
        large_queries.queries_matching("FROM \"hubuumclass_relation\""),
        1
    );

    fixture.cleanup().await.expect("export fixture cleanup");
}

#[test]
fn normalizes_relation_alias_segments_predictably() {
    assert_eq!(normalize_alias_segment("Access Policy"), "access_policy");
    assert_eq!(normalize_alias_segment("Person async"), "person_async");
    assert_eq!(normalize_alias_segment("Host"), "host");
}

#[test]
fn pluralizes_relation_aliases_predictably() {
    assert_eq!(pluralize_alias("room"), "rooms");
    assert_eq!(pluralize_alias("person"), "persons");
    assert_eq!(pluralize_alias("policy"), "policies");
    assert_eq!(pluralize_alias("class"), "classes");
}

#[test]
fn infers_relation_aliases_from_class_names() {
    assert_eq!(inferred_relation_alias("Room"), "rooms");
    assert_eq!(inferred_relation_alias("Person"), "persons");
    assert_eq!(inferred_relation_alias("Access Policy"), "access_policies");
    assert_eq!(inferred_relation_alias("Person async"), "person_asyncs");
}

#[test]
fn rejects_relation_context_with_related_object_includes() {
    let mut related_objects = HashMap::new();
    related_objects.insert(
        "owners".to_string(),
        ExportIncludeRelatedObject {
            class_id: 2,
            class_relation_id: None,
            direction: None,
            sort: None,
            max_depth: None,
            limit: None,
        },
    );
    let runtime = export_runtime(templated_export_with_include(related_objects));

    let error = validate_export_submission(&runtime).unwrap_err();

    assert_eq!(
        error.to_string(),
        "include.related_objects cannot be combined with relation_context"
    );
}

#[test]
fn allows_relation_context_with_empty_related_object_includes() {
    let runtime = export_runtime(templated_export_with_include(HashMap::new()));

    validate_export_submission(&runtime).unwrap();
}

#[test]
fn rejects_zero_export_limits() {
    let max_items_error = validate_export_limits(&export_with_limits(ExportLimits {
        max_items: Some(0),
        max_output_bytes: None,
    }))
    .unwrap_err();

    assert_eq!(
        max_items_error.to_string(),
        "max_items must be greater than 0"
    );

    let max_output_error = validate_export_limits(&export_with_limits(ExportLimits {
        max_items: None,
        max_output_bytes: Some(0),
    }))
    .unwrap_err();

    assert_eq!(
        max_output_error.to_string(),
        "max_output_bytes must be greater than 0"
    );
}

#[test]
fn rejects_export_output_limit_above_server_cap() {
    let error = validate_export_limits(&export_with_limits(ExportLimits {
        max_items: None,
        max_output_bytes: Some(usize::MAX),
    }))
    .unwrap_err();

    assert!(error.to_string().contains("exceeds server maximum"));
}

#[test]
fn hydration_budget_reserves_capacity_for_root_object() {
    let budget = HydrationBudget::new(2);

    assert_eq!(budget.remaining(), 2);
    assert_eq!(budget.remaining_related_capacity().unwrap(), 1);
}

#[test]
fn hydration_budget_rejects_when_export_budget_is_exhausted() {
    let mut budget = HydrationBudget::new(1);

    assert_eq!(budget.remaining_related_capacity().unwrap(), 0);
    budget.count_object().unwrap();

    let error = budget.remaining_related_capacity().unwrap_err();

    assert_eq!(
        error.to_string(),
        "Hydrated template object limit exceeded (1 >= 1)"
    );
}
