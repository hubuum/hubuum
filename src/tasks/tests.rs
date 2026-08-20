use chrono::NaiveDateTime;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use rstest::rstest;
use std::sync::Arc;

use super::execution::{execute_import_best_effort, execute_import_strict};
use super::helpers::{
    class_to_resolution, planned_result, sanitize_error_for_storage,
    should_abort_best_effort_execution,
};
use super::planning::{
    plan_class, plan_collection, plan_import, plan_object, plan_runtime_admin_import,
};
use super::request_hash;
use super::resolution::{
    remember_class, remember_collection, resolve_class_planning, resolve_collection_by_id_planning,
    resolve_collection_planning, resolve_object_planning,
};
use super::types::{
    CollectionResolution, ExecutionAccumulator, FailureKind, PlannedExecution, PlannedItem,
    PlanningFailure, PlanningState, WorkerLoopAction,
};
use super::worker::{
    background_worker_action, mark_claimed_task_failed, process_claimed_task_for_test,
};
use crate::errors::ApiError;
use crate::models::{
    CURRENT_IMPORT_VERSION, ClassKey, CollectionID, CollectionKey, ExportContentType,
    ExportScopeKind, ExportTemplateKind, GroupID, GroupKey, IdentityScopeKey, ImportAtomicity,
    ImportClassInput, ImportClassRelationInput, ImportCollectionInput, ImportCollisionPolicy,
    ImportExportTemplateInput, ImportGraph, ImportGroupMembershipInput, ImportIdentityScopeInput,
    ImportMembershipSourceInput, ImportMode, ImportObjectInput, ImportObjectRelationInput,
    ImportPermissionPolicy, ImportRemoteTargetInput, ImportRequest, ImportWriteCondition,
    NewCollectionWithAssignee, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    NewHubuumObjectRelation, ObjectKey, Permissions, PrincipalKey, RemoteAuthConfig,
    RemoteHttpMethod, RemoteTargetSubjectType, ResourceRevision, RestoreTimestamps,
    TaskResultCounts, TaskStatus,
};
use crate::permissions::PermissionBackend;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::types::{ResourceAttrs, ResourceKind};
use crate::schema::collections::dsl::{collections, name as collection_name};
use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name};
use crate::services::tasks::{ClaimedTask, TaskStateChange, find_task, update_task_state};
use crate::storage::{
    CollectionStorage, ImportStorage, StorageImportPlan, StorageImportPlanItem,
    StorageImportResult, StorageTaskCreateRequest, StorageTaskKind, StorageTaskScopeSnapshot,
    TaskQueueStorage,
};
use crate::tests::{TestContext, create_test_group};
use crate::traits::CanSave;
use hubuum_storage_postgres::PostgresStorage;
use hubuum_storage_postgres::{capture_queries, with_connection};

async fn create_worker_test_task(
    context: &TestContext,
    kind: StorageTaskKind,
    payload: serde_json::Value,
    total_items: i32,
    label: &str,
) -> crate::models::TaskRecord {
    let backend = crate::storage::storage_handle(&context.pool);
    let task = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                kind,
                hubuum_domain::PrincipalId::new(context.admin_user.id)
                    .expect("persisted test principal id must be positive"),
                payload,
                total_items,
            )
            .idempotency_key(Some(
                hubuum_task_core::IdempotencyKey::new(context.scoped_name(label))
                    .expect("test idempotency key must be valid"),
            ))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .build(100),
        )
        .await
        .expect("worker test task should be created");
    hubuum_storage_postgres::test_support::prioritize_task(context.pool.get_ref(), task.id())
        .await
        .expect("worker test task should be prioritized");
    find_task(
        &context.pool,
        crate::models::TaskID::new(task.id().id()).expect("persisted task id must be positive"),
    )
    .await
    .expect("worker test task should be loadable")
}

async fn claim_worker_test_task(
    context: &TestContext,
    task_id: i32,
) -> crate::services::tasks::ClaimedTask {
    let task_id = hubuum_domain::TaskId::new(task_id).expect("persisted task id must be positive");
    let claim =
        hubuum_storage_postgres::test_support::claim_task_by_id(context.pool.get_ref(), task_id)
            .await
            .expect("the exact worker test task should be claimable");
    ClaimedTask::from_storage(claim).expect("the claimed worker test task should be valid")
}

#[tokio::test]
async fn import_planning_query_growth_is_bounded_per_object_in_one_class() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("query_budget_import_preload")
        .await;
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("query_budget_import_class"),
        description: "query budget import class".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&context.pool)
    .await
    .expect("import class should save");
    let mut objects = Vec::new();
    for index in 0..20 {
        objects.push(
            NewHubuumObject {
                collection_id: fixture.collection.id,
                hubuum_class_id: class.id,
                name: context.scoped_name(&format!("query_budget_import_object_{index:02}")),
                description: "existing import object".to_string(),
                data: serde_json::json!({"index": index}),
            }
            .save_without_events(&context.pool)
            .await
            .expect("import object should save"),
        );
    }

    let request_for = |count: usize| ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: Some(ImportMode {
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            ..ImportMode::default()
        }),
        graph: ImportGraph {
            objects: objects
                .iter()
                .take(count)
                .enumerate()
                .map(|(index, object)| ImportObjectInput {
                    ref_: Some(format!("object:query-budget-{index}")),
                    name: object.name.clone(),
                    description: "planned import update".to_string(),
                    condition: None,
                    timestamps: None,
                    data: serde_json::json!({"index": index, "planned": true}),
                    class_ref: None,
                    class_key: Some(ClassKey {
                        name: class.name.clone(),
                        collection_ref: None,
                        collection_key: Some(CollectionKey {
                            name: fixture.collection.name.clone(),
                            path: None,
                        }),
                    }),
                })
                .collect(),
            ..ImportGraph::default()
        },
    };

    let small_request = request_for(1);
    let (small_plan, small_queries) = capture_queries(plan_runtime_admin_import(
        &context.pool,
        &context.admin_user,
        &small_request,
    ))
    .await;
    assert!(!small_plan.aborted);
    assert!(small_plan.failures.is_empty());
    assert_eq!(small_plan.planned_items.len(), 1);

    let large_request = request_for(20);
    let (large_plan, large_queries) = capture_queries(plan_runtime_admin_import(
        &context.pool,
        &context.admin_user,
        &large_request,
    ))
    .await;
    assert!(!large_plan.aborted);
    assert!(large_plan.failures.is_empty());
    assert_eq!(large_plan.planned_items.len(), 20);

    // Current before-refactor shape: three fixed batch queries (collection,
    // class, object), plus three collection-name resolutions per object across
    // class preload, object preload, and object planning. Keep that slope
    // explicit so a storage-boundary rewrite cannot make it worse unnoticed.
    let fixed_queries = 3;
    let queries_per_object = 3;
    assert_eq!(
        small_queries.total_queries(),
        fixed_queries + queries_per_object,
        "{:#?}",
        small_queries.query_counts()
    );
    assert_eq!(
        large_queries.total_queries(),
        fixed_queries + queries_per_object * 20,
        "{:#?}",
        large_queries.query_counts()
    );
    assert_eq!(
        small_queries.domain_queries(),
        small_queries.total_queries()
    );
    assert_eq!(
        large_queries.domain_queries(),
        large_queries.total_queries()
    );
    assert_eq!(small_queries.control_queries(), 0);
    assert_eq!(large_queries.control_queries(), 0);
    assert_eq!(
        small_queries.connection_checkouts(),
        small_queries.total_queries()
    );
    assert_eq!(
        large_queries.connection_checkouts(),
        large_queries.total_queries()
    );
    assert_eq!(large_queries.queries_matching("FROM \"hubuumclass\""), 1);
    assert_eq!(large_queries.queries_matching("FROM \"hubuumobject\""), 1);
    assert_eq!(
        large_queries.queries_matching("FROM \"collections\""),
        queries_per_object * 20 + 1
    );

    fixture.cleanup().await.expect("import fixture cleanup");
}

#[tokio::test]
async fn import_planning_uses_the_task_execution_permission_backend() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("external_task_authorization")
        .await;
    let permissions: Arc<dyn PermissionBackend> = Arc::new(MockTreetopBackend::new());
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        permissions,
    );
    let request = ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(true),
        mode: Some(ImportMode {
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            ..ImportMode::default()
        }),
        graph: ImportGraph {
            collections: vec![ImportCollectionInput {
                ref_: Some("collection:existing".to_string()),
                name: fixture.collection.name.clone(),
                description: "updated by import".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            }],
            ..ImportGraph::default()
        },
    };

    let planning = plan_import(&backend, &context.admin_user, None, &request).await;

    assert!(planning.aborted);
    assert_eq!(planning.failures.len(), 1);
    assert!(matches!(planning.failures[0].kind, FailureKind::Permission));
}

#[derive(Clone, Copy, Debug)]
enum TimestampRelationKind {
    Class,
    Object,
}

#[rstest]
#[case::class_update(TimestampRelationKind::Class, Permissions::UpdateClassRelation, true)]
#[case::class_create(TimestampRelationKind::Class, Permissions::CreateClassRelation, false)]
#[case::object_update(TimestampRelationKind::Object, Permissions::UpdateObjectRelation, true)]
#[case::object_create(
    TimestampRelationKind::Object,
    Permissions::CreateObjectRelation,
    false
)]
#[tokio::test]
async fn relation_timestamp_overwrite_requires_update_permission(
    #[case] kind: TimestampRelationKind,
    #[case] granted_permission: Permissions,
    #[case] expected_allowed: bool,
) {
    let context = TestContext::new().await;
    let fixtures = context
        .collection_fixtures("relation_timestamp_permission", 2)
        .await;
    let mut classes = Vec::new();
    let mut objects = Vec::new();
    for (index, fixture) in fixtures.iter().enumerate() {
        let class = NewHubuumClass {
            collection_id: fixture.collection.id,
            name: context.scoped_name(&format!("relation_timestamp_class_{index}")),
            description: "Relation timestamp permission class".to_string(),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let object = NewHubuumObject {
            collection_id: fixture.collection.id,
            hubuum_class_id: class.id,
            name: context.scoped_name(&format!("relation_timestamp_object_{index}")),
            description: "Relation timestamp permission object".to_string(),
            data: serde_json::json!({"index": index}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        classes.push(class);
        objects.push(object);
    }
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: classes[0].id,
        to_hubuum_class_id: classes[1].id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    NewHubuumObjectRelation {
        from_hubuum_object_id: objects[0].id,
        to_hubuum_object_id: objects[1].id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let policy_group = create_test_group(&context.pool).await;
    policy_group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let permission_backend = MockTreetopBackend::new();
    let resource_kind = match kind {
        TimestampRelationKind::Class => ResourceKind::ClassRelation,
        TimestampRelationKind::Object => ResourceKind::ObjectRelation,
    };
    for fixture in &fixtures {
        permission_backend.add_rule(MockAllowRule {
            group_id: policy_group.id,
            action: granted_permission,
            resource_kind: resource_kind.clone(),
            resource_id: Some(0),
            attrs: ResourceAttrs {
                collection_id: Some(fixture.collection.id),
                from_collection_id: Some(fixture.collection.id),
                to_collection_id: Some(fixture.collection.id),
                ..ResourceAttrs::default()
            },
        });
    }
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        Arc::new(permission_backend),
    );

    let collection_key = |index: usize| CollectionKey {
        name: fixtures[index].collection.name.clone(),
        path: None,
    };
    let class_key = |index: usize| ClassKey {
        name: classes[index].name.clone(),
        collection_ref: None,
        collection_key: Some(collection_key(index)),
    };
    let object_key = |index: usize| ObjectKey {
        name: objects[index].name.clone(),
        class_ref: None,
        class_key: Some(class_key(index)),
    };
    let timestamps = restore_timestamps("2020-01-02T03:04:05", "2021-02-03T04:05:06");
    let mut graph = ImportGraph::default();
    match kind {
        TimestampRelationKind::Class => {
            graph.class_relations.push(ImportClassRelationInput {
                ref_: Some("class-relation:timestamp-permission".to_string()),
                from_class_ref: None,
                from_class_key: Some(class_key(0)),
                to_class_ref: None,
                to_class_key: Some(class_key(1)),
                forward_template_alias: None,
                reverse_template_alias: None,
                from_max_relations: None,
                to_max_relations: None,
                condition: None,
                timestamps: Some(timestamps),
            });
        }
        TimestampRelationKind::Object => {
            graph.object_relations.push(ImportObjectRelationInput {
                ref_: Some("object-relation:timestamp-permission".to_string()),
                from_object_ref: None,
                from_object_key: Some(object_key(0)),
                to_object_ref: None,
                to_object_key: Some(object_key(1)),
                condition: None,
                timestamps: Some(timestamps),
            });
        }
    }
    let request = ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: Some(ImportMode {
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            ..ImportMode::default()
        }),
        graph,
    };

    let planning = plan_import(&backend, &context.normal_user, None, &request).await;

    assert_eq!(planning.aborted, !expected_allowed);
    if expected_allowed {
        assert!(planning.failures.is_empty());
        assert!(matches!(
            planning.planned_items.as_slice(),
            [PlannedItem {
                execution: Some(
                    PlannedExecution::UpdateClassRelationTimestamps { .. }
                        | PlannedExecution::UpdateObjectRelationTimestamps { .. }
                ),
                ..
            }]
        ));
    } else {
        assert!(matches!(
            planning.failures.as_slice(),
            [PlanningFailure {
                kind: FailureKind::Permission,
                ..
            }]
        ));
    }
}

fn extended_import_request(name: String) -> ImportRequest {
    ImportRequest {
        version: crate::models::CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: None,
        graph: ImportGraph {
            identity_scopes: vec![ImportIdentityScopeInput {
                ref_: Some("identity:backend-test".to_string()),
                name,
                provider_kind: "local".to_string(),
                condition: None,
                timestamps: None,
            }],
            ..ImportGraph::default()
        },
    }
}

#[derive(Clone, Copy, Debug)]
enum ClassBoundImport {
    ExportTemplate,
    RemoteTarget,
}

#[derive(Clone, Copy, Debug)]
enum TimestampOverwrite {
    Omitted,
    Identical,
}

#[derive(Clone, Copy, Debug)]
enum CoreTemporalEntity {
    Collection,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
}

#[derive(Clone, Copy, Debug)]
enum TemplateDependency {
    Existing,
    SameImport,
    Missing,
}

fn restore_timestamps(created_at_value: &str, updated_at_value: &str) -> RestoreTimestamps {
    RestoreTimestamps::new(
        created_at_value.parse().expect("created_at test timestamp"),
        updated_at_value.parse().expect("updated_at test timestamp"),
    )
    .expect("ordered restore timestamps")
}

async fn apply_import_operations(
    context: &TestContext,
    operations: impl IntoIterator<Item = PlannedExecution>,
) -> Result<(), ApiError> {
    let items = operations
        .into_iter()
        .enumerate()
        .map(|(index, operation)| {
            crate::services::import_boundary::import_operation_to_storage(operation)
                .map(|operation| StorageImportPlanItem::new(index, operation))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let plan = StorageImportPlan::new(items).map_err(ApiError::from)?;
    PostgresStorage::unobserved(context.pool.get_ref().clone())
        .apply_import_strict(plan)
        .await
        .map_err(ApiError::from)
}

#[tokio::test]
async fn imported_collection_timestamps_are_written_in_the_initial_history_entry() {
    let context = TestContext::new().await;
    let parent = context.collection_fixture("import_history_parent").await;
    let timestamps = restore_timestamps("2020-01-02T03:04:05", "2020-02-03T04:05:06");
    let imported_name = context.scoped_name("import_history_collection");
    apply_import_operations(
        &context,
        [PlannedExecution::CreateCollection(ImportCollectionInput {
            ref_: None,
            name: imported_name.clone(),
            description: "Imported collection history".to_string(),
            parent_collection_ref: None,
            parent_collection_key: Some(CollectionKey {
                name: parent.collection.name.clone(),
                path: None,
            }),
            condition: None,
            timestamps: Some(timestamps.clone()),
        })],
    )
    .await
    .unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    let collection = backend
        .import_collection_child_by_name(
            hubuum_domain::CollectionId::new(parent.collection.id).unwrap(),
            &imported_name,
        )
        .await
        .unwrap()
        .expect("imported collection should exist");

    let history = with_connection(&context.pool, async |conn| {
        use crate::schema::collections_history::dsl as h;
        h::collections_history
            .filter(h::id.eq(collection.id().id()))
            .order(h::history_id.asc())
            .select((h::op, h::created_at, h::updated_at))
            .load::<(String, NaiveDateTime, NaiveDateTime)>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(
        history,
        vec![(
            "I".to_string(),
            timestamps.created_at(),
            timestamps.updated_at()
        )]
    );

    backend
        .delete_collection(collection.id(), &crate::events::EventContext::system())
        .await
        .unwrap()
        .into_value();
    parent.cleanup().await.unwrap();
}

#[rstest]
#[case::export_omitted(ClassBoundImport::ExportTemplate, TimestampOverwrite::Omitted)]
#[case::export_identical(ClassBoundImport::ExportTemplate, TimestampOverwrite::Identical)]
#[case::remote_omitted(ClassBoundImport::RemoteTarget, TimestampOverwrite::Omitted)]
#[case::remote_identical(ClassBoundImport::RemoteTarget, TimestampOverwrite::Identical)]
#[tokio::test]
async fn unchanged_temporal_import_overwrite_does_not_append_history(
    #[case] kind: ClassBoundImport,
    #[case] overwrite: TimestampOverwrite,
) {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("unchanged_temporal_import")
        .await;
    let timestamps = restore_timestamps("2020-01-02T03:04:05", "2020-02-03T04:05:06");
    let overwrite_timestamps = match overwrite {
        TimestampOverwrite::Omitted => None,
        TimestampOverwrite::Identical => Some(timestamps.clone()),
    };

    let collection_key = Some(CollectionKey {
        name: fixture.collection.name.clone(),
        path: None,
    });
    let history_count = match kind {
        ClassBoundImport::ExportTemplate => {
            let input = ImportExportTemplateInput {
                ref_: None,
                collection_ref: None,
                collection_key: collection_key.clone(),
                class_ref: None,
                class_key: None,
                name: context.scoped_name("unchanged_export_template"),
                description: "Unchanged export template".to_string(),
                content_type: ExportContentType::TextPlain,
                template: "unchanged".to_string(),
                kind: ExportTemplateKind::Fragment,
                scope_kind: None,
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
                condition: None,
                timestamps: Some(timestamps.clone()),
            };
            let mut overwrite_input = input.clone();
            overwrite_input.timestamps = overwrite_timestamps;
            apply_import_operations(
                &context,
                [
                    PlannedExecution::UpsertExportTemplate {
                        input: input.clone(),
                        overwrite: false,
                    },
                    PlannedExecution::UpsertExportTemplate {
                        input: overwrite_input,
                        overwrite: true,
                    },
                ],
            )
            .await
            .unwrap();

            let name = input.name;
            use crate::schema::export_templates::dsl as t;
            use crate::schema::export_templates_history::dsl as h;
            with_connection(&context.pool, async |conn| {
                let id = t::export_templates
                    .filter(t::name.eq(name))
                    .select(t::id)
                    .first::<i32>(conn)
                    .await?;
                h::export_templates_history
                    .filter(h::id.eq(id))
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
            .unwrap()
        }
        ClassBoundImport::RemoteTarget => {
            let input = ImportRemoteTargetInput {
                ref_: None,
                collection_ref: None,
                collection_key,
                class_ref: None,
                class_key: None,
                name: context.scoped_name("unchanged_remote_target"),
                description: "Unchanged remote target".to_string(),
                method: RemoteHttpMethod::Get,
                url_template: "https://example.test/static".to_string(),
                headers_template: serde_json::json!({}),
                body_template: None,
                auth_config: RemoteAuthConfig::None,
                allowed_subject_types: vec![RemoteTargetSubjectType::Collection],
                timeout_ms: 1_000,
                enabled: true,
                condition: None,
                timestamps: Some(timestamps.clone()),
            };
            let mut overwrite_input = input.clone();
            overwrite_input.timestamps = overwrite_timestamps;
            apply_import_operations(
                &context,
                [
                    PlannedExecution::UpsertRemoteTarget {
                        input: input.clone(),
                        overwrite: false,
                    },
                    PlannedExecution::UpsertRemoteTarget {
                        input: overwrite_input,
                        overwrite: true,
                    },
                ],
            )
            .await
            .unwrap();

            let name = input.name;
            use crate::schema::remote_targets::dsl as t;
            use crate::schema::remote_targets_history::dsl as h;
            with_connection(&context.pool, async |conn| {
                let id = t::remote_targets
                    .filter(t::name.eq(name))
                    .select(t::id)
                    .first::<i32>(conn)
                    .await?;
                h::remote_targets_history
                    .filter(h::id.eq(id))
                    .count()
                    .get_result::<i64>(conn)
                    .await
            })
            .await
            .unwrap()
        }
    };

    assert_eq!(history_count, 1);

    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::collection(CoreTemporalEntity::Collection)]
#[case::class(CoreTemporalEntity::Class)]
#[case::object(CoreTemporalEntity::Object)]
#[case::class_relation(CoreTemporalEntity::ClassRelation)]
#[case::object_relation(CoreTemporalEntity::ObjectRelation)]
#[tokio::test]
async fn unchanged_core_import_overwrite_returns_current_row_without_history(
    #[case] entity: CoreTemporalEntity,
) {
    let context = TestContext::new().await;
    let parent = context
        .collection_fixture("unchanged_core_import_parent")
        .await;
    let timestamps = restore_timestamps("2020-01-02T03:04:05", "2020-02-03T04:05:06");
    let collection_input = ImportCollectionInput {
        ref_: Some("collection:current".to_string()),
        name: context.scoped_name("unchanged_core_import_collection"),
        description: "Unchanged core import collection".to_string(),
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent.collection.name.clone(),
            path: None,
        }),
        condition: None,
        timestamps: Some(timestamps.clone()),
    };
    let class_inputs = [0, 1].map(|index| ImportClassInput {
        ref_: Some(format!("class:{index}")),
        name: context.scoped_name(&format!("unchanged_core_import_class_{index}")),
        description: format!("Unchanged core import class {index}"),
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:current".to_string()),
        collection_key: None,
        condition: None,
        timestamps: Some(timestamps.clone()),
    });
    let object_inputs = [0, 1].map(|index| ImportObjectInput {
        ref_: Some(format!("object:{index}")),
        name: context.scoped_name(&format!("unchanged_core_import_object_{index}")),
        description: format!("Unchanged core import object {index}"),
        data: serde_json::json!({"index": index}),
        class_ref: Some(format!("class:{index}")),
        class_key: None,
        condition: None,
        timestamps: Some(timestamps.clone()),
    });
    let class_relation_input = ImportClassRelationInput {
        ref_: Some("class-relation:current".to_string()),
        from_class_ref: Some("class:0".to_string()),
        from_class_key: None,
        to_class_ref: Some("class:1".to_string()),
        to_class_key: None,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
        condition: None,
        timestamps: Some(timestamps.clone()),
    };
    let object_relation_input = ImportObjectRelationInput {
        ref_: Some("object-relation:current".to_string()),
        from_object_ref: Some("object:0".to_string()),
        from_object_key: None,
        to_object_ref: Some("object:1".to_string()),
        to_object_key: None,
        condition: None,
        timestamps: Some(timestamps.clone()),
    };
    apply_import_operations(
        &context,
        vec![
            PlannedExecution::CreateCollection(collection_input.clone()),
            PlannedExecution::CreateClass(class_inputs[0].clone()),
            PlannedExecution::CreateClass(class_inputs[1].clone()),
            PlannedExecution::CreateObject(object_inputs[0].clone()),
            PlannedExecution::CreateObject(object_inputs[1].clone()),
            PlannedExecution::CreateClassRelation(class_relation_input.clone()),
            PlannedExecution::CreateObjectRelation(object_relation_input.clone()),
        ],
    )
    .await
    .unwrap();

    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    let collection = backend
        .import_collection_child_by_name(
            hubuum_domain::CollectionId::new(parent.collection.id).unwrap(),
            &collection_input.name,
        )
        .await
        .unwrap()
        .expect("imported collection should exist");
    let classes = [
        backend
            .import_class_by_name(collection.id(), &class_inputs[0].name)
            .await
            .unwrap()
            .expect("first imported class should exist"),
        backend
            .import_class_by_name(collection.id(), &class_inputs[1].name)
            .await
            .unwrap()
            .expect("second imported class should exist"),
    ];
    let objects = [
        backend
            .import_object_by_name(classes[0].id(), &object_inputs[0].name)
            .await
            .unwrap()
            .expect("first imported object should exist"),
        backend
            .import_object_by_name(classes[1].id(), &object_inputs[1].name)
            .await
            .unwrap()
            .expect("second imported object should exist"),
    ];
    let (class_relation_id, object_relation_id) = with_connection(&context.pool, async |conn| {
        use crate::schema::hubuumclass_relation::dsl as cr;
        use crate::schema::hubuumobject_relation::dsl as or;
        let class_relation_id = cr::hubuumclass_relation
            .filter(cr::from_hubuum_class_id.eq(classes[0].id().id()))
            .filter(cr::to_hubuum_class_id.eq(classes[1].id().id()))
            .select(cr::id)
            .first::<i32>(conn)
            .await?;
        let object_relation_id = or::hubuumobject_relation
            .filter(or::from_hubuum_object_id.eq(objects[0].id().id()))
            .filter(or::to_hubuum_object_id.eq(objects[1].id().id()))
            .select(or::id)
            .first::<i32>(conn)
            .await?;
        Ok::<_, diesel::result::Error>((class_relation_id, object_relation_id))
    })
    .await
    .unwrap();

    // Import refs are local to one plan. This update runs in a second plan, so
    // address the existing relation endpoints through their durable keys.
    let collection_key = || CollectionKey {
        name: collection.name().to_string(),
        path: None,
    };
    let class_key = |index: usize| ClassKey {
        name: classes[index].name().to_string(),
        collection_ref: None,
        collection_key: Some(collection_key()),
    };
    let object_key = |index: usize| ObjectKey {
        name: objects[index].name().to_string(),
        class_ref: None,
        class_key: Some(class_key(index)),
    };

    let update = match entity {
        CoreTemporalEntity::Collection => PlannedExecution::UpdateCollection {
            collection_id: collection.id().id(),
            input: collection_input,
        },
        CoreTemporalEntity::Class => PlannedExecution::UpdateClass {
            class_id: classes[0].id().id(),
            input: class_inputs[0].clone(),
        },
        CoreTemporalEntity::Object => PlannedExecution::UpdateObject {
            object_id: objects[0].id().id(),
            input: object_inputs[0].clone(),
        },
        CoreTemporalEntity::ClassRelation => PlannedExecution::UpdateClassRelationTimestamps {
            input: ImportClassRelationInput {
                from_class_ref: None,
                from_class_key: Some(class_key(0)),
                to_class_ref: None,
                to_class_key: Some(class_key(1)),
                ..class_relation_input
            },
            timestamps: timestamps.clone(),
        },
        CoreTemporalEntity::ObjectRelation => PlannedExecution::UpdateObjectRelationTimestamps {
            input: ImportObjectRelationInput {
                from_object_ref: None,
                from_object_key: Some(object_key(0)),
                to_object_ref: None,
                to_object_key: Some(object_key(1)),
                ..object_relation_input
            },
            timestamps,
        },
    };
    apply_import_operations(&context, [update]).await.unwrap();

    let history_count = with_connection(&context.pool, async |conn| match entity {
        CoreTemporalEntity::Collection => {
            use crate::schema::collections_history::dsl as h;
            h::collections_history
                .filter(h::id.eq(collection.id().id()))
                .count()
                .get_result::<i64>(conn)
                .await
        }
        CoreTemporalEntity::Class => {
            use crate::schema::hubuumclass_history::dsl as h;
            h::hubuumclass_history
                .filter(h::id.eq(classes[0].id().id()))
                .count()
                .get_result::<i64>(conn)
                .await
        }
        CoreTemporalEntity::Object => {
            use crate::schema::hubuumobject_history::dsl as h;
            h::hubuumobject_history
                .filter(h::id.eq(objects[0].id().id()))
                .count()
                .get_result::<i64>(conn)
                .await
        }
        CoreTemporalEntity::ClassRelation => {
            use crate::schema::hubuumclass_relation_history::dsl as h;
            h::hubuumclass_relation_history
                .filter(h::id.eq(class_relation_id))
                .count()
                .get_result::<i64>(conn)
                .await
        }
        CoreTemporalEntity::ObjectRelation => {
            use crate::schema::hubuumobject_relation_history::dsl as h;
            h::hubuumobject_relation_history
                .filter(h::id.eq(object_relation_id))
                .count()
                .get_result::<i64>(conn)
                .await
        }
    })
    .await
    .unwrap();

    assert_eq!(history_count, 1);

    backend
        .delete_collection(collection.id(), &crate::events::EventContext::system())
        .await
        .unwrap()
        .into_value();
    parent.cleanup().await.unwrap();
}

#[tokio::test]
async fn core_imports_without_timestamps_use_database_transaction_time() {
    let context = TestContext::new().await;
    let parent = context
        .collection_fixture("database_timestamp_parent")
        .await;
    let imported_collection_name = context.scoped_name("database_timestamp_collection");
    let resolve_class_names = [
        context.scoped_name("database_timestamp_class_one"),
        context.scoped_name("database_timestamp_class_two"),
    ];
    let object_names = [
        context.scoped_name("database_timestamp_object_one"),
        context.scoped_name("database_timestamp_object_two"),
    ];

    let collection_input = ImportCollectionInput {
        ref_: Some("collection:current".to_string()),
        name: imported_collection_name.clone(),
        description: "Database timestamp collection".to_string(),
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent.collection.name.clone(),
            path: None,
        }),
        condition: None,
        timestamps: None,
    };
    let class_inputs = [0, 1].map(|index| ImportClassInput {
        ref_: Some(format!("class:{index}")),
        name: resolve_class_names[index].clone(),
        description: format!("Database timestamp class {index}"),
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:current".to_string()),
        collection_key: None,
        condition: None,
        timestamps: None,
    });
    let object_inputs = [0, 1].map(|index| ImportObjectInput {
        ref_: Some(format!("object:{index}")),
        name: object_names[index].clone(),
        description: format!("Database timestamp object {index}"),
        data: serde_json::json!({"index": index}),
        class_ref: Some(format!("class:{index}")),
        class_key: None,
        condition: None,
        timestamps: None,
    });
    apply_import_operations(
        &context,
        vec![
            PlannedExecution::CreateCollection(collection_input),
            PlannedExecution::CreateClass(class_inputs[0].clone()),
            PlannedExecution::CreateClass(class_inputs[1].clone()),
            PlannedExecution::CreateObject(object_inputs[0].clone()),
            PlannedExecution::CreateObject(object_inputs[1].clone()),
            PlannedExecution::CreateClassRelation(ImportClassRelationInput {
                ref_: None,
                from_class_ref: Some("class:0".to_string()),
                from_class_key: None,
                to_class_ref: Some("class:1".to_string()),
                to_class_key: None,
                forward_template_alias: None,
                reverse_template_alias: None,
                from_max_relations: None,
                to_max_relations: None,
                condition: None,
                timestamps: None,
            }),
            PlannedExecution::CreateObjectRelation(ImportObjectRelationInput {
                ref_: None,
                from_object_ref: Some("object:0".to_string()),
                from_object_key: None,
                to_object_ref: Some("object:1".to_string()),
                to_object_key: None,
                condition: None,
                timestamps: None,
            }),
        ],
    )
    .await
    .unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    let collection = backend
        .import_collection_child_by_name(
            hubuum_domain::CollectionId::new(parent.collection.id).unwrap(),
            &imported_collection_name,
        )
        .await
        .unwrap()
        .expect("imported collection should exist");
    let classes = [
        backend
            .import_class_by_name(collection.id(), &resolve_class_names[0])
            .await
            .unwrap()
            .expect("first imported class should exist"),
        backend
            .import_class_by_name(collection.id(), &resolve_class_names[1])
            .await
            .unwrap()
            .expect("second imported class should exist"),
    ];
    let objects = [
        backend
            .import_object_by_name(classes[0].id(), &object_names[0])
            .await
            .unwrap()
            .expect("first imported object should exist"),
        backend
            .import_object_by_name(classes[1].id(), &object_names[1])
            .await
            .unwrap()
            .expect("second imported object should exist"),
    ];
    let relation_timestamps = with_connection(&context.pool, async |conn| {
        use crate::schema::hubuumclass_relation::dsl as cr;
        use crate::schema::hubuumobject_relation::dsl as or;
        let class_relation = cr::hubuumclass_relation
            .filter(cr::from_hubuum_class_id.eq(classes[0].id().id()))
            .filter(cr::to_hubuum_class_id.eq(classes[1].id().id()))
            .select((cr::created_at, cr::updated_at))
            .first::<(NaiveDateTime, NaiveDateTime)>(conn)
            .await?;
        let object_relation = or::hubuumobject_relation
            .filter(or::from_hubuum_object_id.eq(objects[0].id().id()))
            .filter(or::to_hubuum_object_id.eq(objects[1].id().id()))
            .select((or::created_at, or::updated_at))
            .first::<(NaiveDateTime, NaiveDateTime)>(conn)
            .await?;
        Ok::<_, diesel::result::Error>((class_relation, object_relation))
    })
    .await
    .unwrap();
    let actual = [
        (collection.created_at(), collection.updated_at()),
        (classes[0].created_at(), classes[0].updated_at()),
        (classes[1].created_at(), classes[1].updated_at()),
        (objects[0].created_at(), objects[0].updated_at()),
        (objects[1].created_at(), objects[1].updated_at()),
        relation_timestamps.0,
        relation_timestamps.1,
    ];
    let expected = actual[0].0;

    assert_eq!(actual, [(expected, expected); 7]);

    backend
        .delete_collection(collection.id(), &crate::events::EventContext::system())
        .await
        .unwrap()
        .into_value();
    parent.cleanup().await.unwrap();
}

#[tokio::test]
async fn test_extended_import_uses_backend_denial_for_sql_administrator() {
    let test = TestContext::new().await;
    let context = crate::tests::app_context_with_permission_backend(
        test.pool.get_ref().clone(),
        Arc::new(MockTreetopBackend::new()),
    );
    let request = extended_import_request(test.scoped_name("backend_denied_import"));

    let planning = plan_import(&context, &test.admin_user, None, &request).await;

    assert!(planning.aborted);
    assert!(matches!(
        planning.failures.as_slice(),
        [failure] if matches!(failure.kind, FailureKind::Permission)
    ));
}

#[tokio::test]
async fn test_extended_import_uses_backend_grant_for_non_sql_administrator() {
    let test = TestContext::new().await;
    let policy_group = create_test_group(&test.pool).await;
    policy_group
        .add_member_without_events(&test.pool, &test.normal_user)
        .await
        .unwrap();
    let backend = MockTreetopBackend::new();
    backend.add_admin_rule(policy_group.id);
    let context = crate::tests::app_context_with_permission_backend(
        test.pool.get_ref().clone(),
        Arc::new(backend),
    );
    let request = extended_import_request(test.scoped_name("backend_allowed_import"));

    let planning = plan_import(&context, &test.normal_user, None, &request).await;

    assert!(!planning.aborted);
    assert!(planning.failures.is_empty());
    assert_eq!(planning.planned_items.len(), 1);
}

#[tokio::test]
async fn test_identity_scope_overwrite_preserves_imported_timestamps() {
    let context = (TestContext::new()).await;
    let name = context.scoped_name("identity_scope_timestamp_overwrite");
    let initial = restore_timestamps("2020-01-02T03:04:05", "2020-02-03T04:05:06");
    let restored = restore_timestamps("2019-04-05T06:07:08", "2021-06-07T08:09:10");

    apply_import_operations(
        &context,
        [
            PlannedExecution::UpsertIdentityScope {
                input: ImportIdentityScopeInput {
                    ref_: None,
                    name: name.clone(),
                    provider_kind: "local".to_string(),
                    condition: None,
                    timestamps: Some(initial),
                },
                overwrite: false,
            },
            PlannedExecution::UpsertIdentityScope {
                input: ImportIdentityScopeInput {
                    ref_: None,
                    name: name.clone(),
                    provider_kind: "oidc".to_string(),
                    condition: None,
                    timestamps: Some(restored.clone()),
                },
                overwrite: true,
            },
        ],
    )
    .await
    .unwrap();

    let row = with_connection(&context.pool, async |conn| {
        use crate::schema::identity_scopes::dsl::{
            created_at, id as scope_id, identity_scopes, name as scope_name, updated_at,
        };
        identity_scopes
            .filter(scope_name.eq(name))
            .select((scope_id, created_at, updated_at))
            .first::<(i32, NaiveDateTime, NaiveDateTime)>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(row.1, restored.created_at());
    assert_eq!(row.2, restored.updated_at());

    with_connection(&context.pool, async |conn| {
        use crate::schema::identity_scopes::dsl::{id as scope_id, identity_scopes};
        diesel::delete(identity_scopes.filter(scope_id.eq(row.0)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn identity_scope_import_rejects_a_stale_expected_revision() {
    let context = TestContext::new().await;
    let name = context.scoped_name("identity_scope_stale_revision");

    apply_import_operations(
        &context,
        [PlannedExecution::UpsertIdentityScope {
            input: ImportIdentityScopeInput {
                ref_: None,
                name: name.clone(),
                provider_kind: "local".to_string(),
                condition: None,
                timestamps: None,
            },
            overwrite: false,
        }],
    )
    .await
    .unwrap();
    let error = apply_import_operations(
        &context,
        [PlannedExecution::UpsertIdentityScope {
            input: ImportIdentityScopeInput {
                ref_: None,
                name,
                provider_kind: "oidc".to_string(),
                condition: Some(ImportWriteCondition::IfRevision {
                    expected_revision: ResourceRevision::new(2).unwrap(),
                }),
                timestamps: None,
            },
            overwrite: true,
        }],
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ApiError::RevisionConflict(message, revision)
            if message.contains("stale_revision") && revision.get() == 1
    ));
}

#[rstest]
#[case::export_template(ClassBoundImport::ExportTemplate)]
#[case::remote_target(ClassBoundImport::RemoteTarget)]
#[tokio::test]
async fn imported_class_binding_must_match_target_collection(#[case] kind: ClassBoundImport) {
    let context = TestContext::new().await;
    let target = context
        .collection_fixture("import_class_scope_target")
        .await;
    let class_owner = context.collection_fixture("import_class_scope_owner").await;
    let class = NewHubuumClass {
        collection_id: class_owner.collection.id,
        name: context.scoped_name("import_class_scope_class"),
        description: "Class in another collection".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let collection_key = Some(CollectionKey {
        name: target.collection.name.clone(),
        path: None,
    });
    let class_key = Some(ClassKey {
        name: class.name,
        collection_ref: None,
        collection_key: Some(CollectionKey {
            name: class_owner.collection.name.clone(),
            path: None,
        }),
    });
    let execution = match kind {
        ClassBoundImport::ExportTemplate => PlannedExecution::UpsertExportTemplate {
            input: ImportExportTemplateInput {
                ref_: None,
                collection_ref: None,
                collection_key,
                class_ref: None,
                class_key,
                name: context.scoped_name("cross_collection_export_template"),
                description: "Invalid class binding".to_string(),
                content_type: ExportContentType::TextPlain,
                template: "{{ items|length }}".to_string(),
                kind: ExportTemplateKind::Export,
                scope_kind: Some(ExportScopeKind::ObjectsInClass),
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
                condition: None,
                timestamps: None,
            },
            overwrite: false,
        },
        ClassBoundImport::RemoteTarget => PlannedExecution::UpsertRemoteTarget {
            input: ImportRemoteTargetInput {
                ref_: None,
                collection_ref: None,
                collection_key,
                class_ref: None,
                class_key,
                name: context.scoped_name("cross_collection_remote_target"),
                description: "Invalid class binding".to_string(),
                method: RemoteHttpMethod::Get,
                url_template: "https://example.test/{{ subject.id }}".to_string(),
                headers_template: serde_json::json!({}),
                body_template: None,
                auth_config: RemoteAuthConfig::None,
                allowed_subject_types: vec![RemoteTargetSubjectType::Object],
                timeout_ms: 1_000,
                enabled: true,
                condition: None,
                timestamps: None,
            },
            overwrite: false,
        },
    };
    let execution =
        crate::services::import_boundary::import_operation_to_storage(execution).unwrap();

    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    let plan = StorageImportPlan::new(vec![StorageImportPlanItem::new(0, execution)]).unwrap();
    let result = backend.apply_import_strict(plan).await;

    assert!(result.is_err_and(|error| error.to_string().contains("not target collection")));
}

#[rstest]
#[case::existing(TemplateDependency::Existing, true)]
#[case::same_import(TemplateDependency::SameImport, true)]
#[case::missing(TemplateDependency::Missing, false)]
#[tokio::test]
async fn imported_templates_use_effective_collection_loader(
    #[case] dependency: TemplateDependency,
    #[case] expected_valid: bool,
) {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("import_template_composition")
        .await;
    let fragment_name = context.scoped_name("fragment.txt");
    let fragment = ImportExportTemplateInput {
        ref_: Some("template:fragment".to_string()),
        collection_ref: None,
        collection_key: Some(CollectionKey {
            name: fixture.collection.name.clone(),
            path: None,
        }),
        class_ref: None,
        class_key: None,
        name: fragment_name.clone(),
        description: "Reusable fragment".to_string(),
        content_type: ExportContentType::TextPlain,
        template: "fragment".to_string(),
        kind: ExportTemplateKind::Fragment,
        scope_kind: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        condition: None,
        default_limits: None,
        timestamps: None,
    };
    let export = ImportExportTemplateInput {
        ref_: Some("template:export".to_string()),
        name: context.scoped_name("composed_export.txt"),
        description: "Composed export".to_string(),
        template: format!("{{% include \"{fragment_name}\" %}}"),
        kind: ExportTemplateKind::Export,
        scope_kind: Some(ExportScopeKind::Collections),
        ..fragment.clone()
    };

    if matches!(dependency, TemplateDependency::Existing) {
        apply_import_operations(
            &context,
            [PlannedExecution::UpsertExportTemplate {
                input: fragment.clone(),
                overwrite: false,
            }],
        )
        .await
        .unwrap();
    }
    let executions = match dependency {
        TemplateDependency::SameImport => vec![export.clone(), fragment],
        TemplateDependency::Existing | TemplateDependency::Missing => vec![export.clone()],
    }
    .into_iter()
    .enumerate()
    .map(|(index, input)| {
        crate::services::import_boundary::import_operation_to_storage(
            PlannedExecution::UpsertExportTemplate {
                input,
                overwrite: false,
            },
        )
        .map(|operation| StorageImportPlanItem::new(index, operation))
    })
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let executions = StorageImportPlan::new(executions).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    let result = backend.apply_import_strict(executions).await;

    assert_eq!(result.is_ok(), expected_valid);
}

#[rstest]
#[case::abort(false, "conflict")]
#[case::overwrite(true, "updated")]
#[tokio::test]
async fn membership_import_honors_collision_policy(
    #[case] overwrite: bool,
    #[case] expected_outcome: &str,
) {
    let context = TestContext::new().await;
    let group = create_test_group(&context.pool).await;
    let principal_name = context.admin_user.name(&context.pool).await.unwrap();
    let initial = restore_timestamps("2020-01-02T03:04:05", "2020-02-03T04:05:06");
    let restored = restore_timestamps("2019-04-05T06:07:08", "2021-06-07T08:09:10");
    let membership = |timestamps: RestoreTimestamps| ImportGroupMembershipInput {
        ref_: None,
        principal_ref: None,
        principal_key: Some(PrincipalKey {
            identity_scope: None,
            name: principal_name.clone(),
        }),
        group_ref: None,
        group_key: Some(GroupKey {
            identity_scope: None,
            groupname: group.groupname.clone(),
        }),
        sources: vec![ImportMembershipSourceInput {
            source: "oidc".to_string(),
            source_scope_ref: None,
            source_scope_key: Some(IdentityScopeKey {
                name: crate::models::identity::LOCAL_IDENTITY_SCOPE.to_string(),
            }),
            source_key: "operators".to_string(),
            timestamps: Some(timestamps.clone()),
        }],
        condition: None,
        timestamps: Some(timestamps),
    };

    apply_import_operations(
        &context,
        [PlannedExecution::UpsertGroupMembership {
            input: membership(initial.clone()),
            overwrite: false,
        }],
    )
    .await
    .unwrap();
    let collision = apply_import_operations(
        &context,
        [PlannedExecution::UpsertGroupMembership {
            input: membership(restored.clone()),
            overwrite,
        }],
    )
    .await;
    let (stored_membership, stored_source) = with_connection(&context.pool, async |conn| {
        use crate::schema::group_membership_sources::dsl as s;
        use crate::schema::group_memberships::dsl as m;
        let stored_membership = m::group_memberships
            .filter(m::principal_id.eq(context.admin_user.id))
            .filter(m::group_id.eq(group.id))
            .select((m::created_at, m::updated_at))
            .first::<(NaiveDateTime, NaiveDateTime)>(conn)
            .await?;
        let stored_source = s::group_membership_sources
            .filter(s::principal_id.eq(context.admin_user.id))
            .filter(s::group_id.eq(group.id))
            .filter(s::source.eq("oidc"))
            .filter(s::source_scope_id.eq(group.identity_scope_id))
            .filter(s::source_key.eq("operators"))
            .select((s::created_at, s::updated_at))
            .first::<(NaiveDateTime, NaiveDateTime)>(conn)
            .await?;
        Ok::<_, diesel::result::Error>((stored_membership, stored_source))
    })
    .await
    .unwrap();
    let actual_outcome = match collision {
        Ok(()) => "updated",
        Err(ApiError::Conflict(_)) => "conflict",
        Err(error) => panic!("unexpected membership collision result: {error}"),
    };
    // Timestamp-only imports are semantic no-ops. Imported timestamps are
    // preserved only when creation or an effective domain change occurs.
    let expected_timestamps = initial;

    assert_eq!(
        (actual_outcome, stored_membership, stored_source),
        (
            expected_outcome,
            (
                expected_timestamps.created_at(),
                expected_timestamps.updated_at()
            ),
            (
                expected_timestamps.created_at(),
                expected_timestamps.updated_at()
            )
        )
    );
}

#[tokio::test]
async fn test_execute_import_strict_rolls_back_on_runtime_failure() {
    let context = (TestContext::new()).await;
    let collection = context.scoped_name("strict_rollback_collection");
    let class = context.scoped_name("strict_rollback_class");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:ok".to_string()),
                Some(collection.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:ok".to_string()),
                name: collection.clone(),
                description: "Rollback collection".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "class",
                "create",
                Some("class:bad".to_string()),
                Some(class.clone()),
            ),
            execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                ref_: Some("class:bad".to_string()),
                name: class.clone(),
                description: "Fails at runtime".to_string(),
                condition: None,
                timestamps: None,
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    let result = (execute_import_strict(&context.pool, 1, &planned_items, &mut accumulator)).await;
    assert!(result.is_err());

    let collection_exists = with_connection(&context.pool, async |conn| {
        collections
            .filter(collection_name.eq(&collection))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    let class_exists = with_connection(&context.pool, async |conn| {
        hubuumclass
            .filter(class_name.eq(&class))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(collection_exists, 0);
    assert_eq!(class_exists, 0);
    assert_eq!(accumulator.processed, 0);
}

#[tokio::test]
async fn test_execute_import_best_effort_keeps_successful_items() {
    let context = (TestContext::new()).await;
    let collection_one = context.scoped_name("best_effort_collection_one");
    let collection_two = context.scoped_name("best_effort_collection_two");
    let class_bad = context.scoped_name("best_effort_class_bad");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:one".to_string()),
                Some(collection_one.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:one".to_string()),
                name: collection_one.clone(),
                description: "Best effort collection one".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "class",
                "create",
                Some("class:bad".to_string()),
                Some(class_bad),
            ),
            execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                ref_: Some("class:bad".to_string()),
                name: "bad".to_string(),
                description: "Fails at runtime".to_string(),
                condition: None,
                timestamps: None,
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:two".to_string()),
                Some(collection_two.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:two".to_string()),
                name: collection_two.clone(),
                description: "Best effort collection two".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    (execute_import_best_effort(
        &context.pool,
        1,
        &planned_items,
        &ImportMode {
            atomicity: Some(ImportAtomicity::BestEffort),
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            permission_policy: Some(ImportPermissionPolicy::Continue),
        },
        &mut accumulator,
    ))
    .await
    .unwrap();

    let collection_count = with_connection(&context.pool, async |conn| {
        collections
            .filter(collection_name.eq_any([collection_one.clone(), collection_two.clone()]))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(collection_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[tokio::test]
async fn test_execute_import_best_effort_continues_after_non_policy_runtime_error() {
    let context = (TestContext::new()).await;
    let collection_one = context.scoped_name("best_effort_runtime_collection_one");
    let collection_two = context.scoped_name("best_effort_runtime_collection_two");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:one".to_string()),
                Some(collection_one.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:one".to_string()),
                name: collection_one.clone(),
                description: "Best effort collection one".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "class",
                "create",
                Some("class:bad".to_string()),
                Some("bad".to_string()),
            ),
            execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                ref_: Some("class:bad".to_string()),
                name: "bad".to_string(),
                description: "Fails at runtime".to_string(),
                condition: None,
                timestamps: None,
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:two".to_string()),
                Some(collection_two.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:two".to_string()),
                name: collection_two.clone(),
                description: "Best effort collection two".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    (execute_import_best_effort(
        &context.pool,
        1,
        &planned_items,
        &ImportMode {
            atomicity: Some(ImportAtomicity::BestEffort),
            collision_policy: Some(ImportCollisionPolicy::Abort),
            permission_policy: Some(ImportPermissionPolicy::Abort),
        },
        &mut accumulator,
    ))
    .await
    .unwrap();

    let collection_count = with_connection(&context.pool, async |conn| {
        collections
            .filter(collection_name.eq_any([collection_one.clone(), collection_two.clone()]))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(collection_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[tokio::test]
async fn test_execute_import_strict_preserves_underlying_error_variant() {
    let context = (TestContext::new()).await;
    let planned_items = vec![PlannedItem {
        result: planned_result(
            "collection",
            "update",
            Some("collection:missing".to_string()),
            Some("missing".to_string()),
        ),
        execution: Some(PlannedExecution::UpdateCollection {
            collection_id: i32::MAX,
            input: ImportCollectionInput {
                ref_: Some("collection:missing".to_string()),
                name: "missing".to_string(),
                description: "missing".to_string(),
                condition: None,
                timestamps: None,
                parent_collection_ref: None,
                parent_collection_key: None,
            },
        }),
    }];

    let mut accumulator = ExecutionAccumulator::default();
    let result = (execute_import_strict(&context.pool, 1, &planned_items, &mut accumulator)).await;

    assert!(matches!(result, Err(ApiError::NotFound(_))));
}

#[test]
fn test_background_worker_continues_immediately_after_processing_a_task() {
    let result = Ok(true);
    assert_eq!(
        background_worker_action(&result),
        WorkerLoopAction::Continue
    );
}

#[test]
fn test_remember_collection_populates_collection_id_index() {
    let mut state = PlanningState::new();
    let collection = CollectionResolution {
        id: -42,
        name: "planned".to_string(),
        description: "planned collection".to_string(),
        parent_collection_id: None,
        exists_in_db: false,
    };

    remember_collection(
        &mut state,
        Some("collection:planned".to_string()),
        collection.clone(),
    );

    assert_eq!(
        state.collections_by_id.get(&collection.id).unwrap().name,
        collection.name
    );
}

#[tokio::test]
async fn test_plan_collection_rejects_duplicate_name_within_request() {
    let context = (TestContext::new()).await;
    let mut state = PlanningState::new();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportCollectionInput {
        ref_: Some("collection:one".to_string()),
        name: context.scoped_name("duplicate_collection"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: None,
    };

    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        ..input
    };
    let err = (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate collection name"));
}

#[tokio::test]
async fn test_plan_collection_allows_duplicate_names_under_different_parents() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("duplicate_import_parent_one")).await;
    let parent_two = (context.collection_fixture("duplicate_import_parent_two")).await;
    let mut state = PlanningState::new();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let child_name = context.scoped_name("duplicate_import_child");
    let input_one = ImportCollectionInput {
        ref_: Some("collection:one".to_string()),
        name: child_name.clone(),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_one.collection.name.clone(),
            path: None,
        }),
    };
    let input_two = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        name: child_name,
        description: "second".to_string(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_two.collection.name.clone(),
            path: None,
        }),
    };

    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_one,
    ))
    .await
    .unwrap();
    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_two,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn test_plan_class_rejects_duplicate_name_against_virtual_planned_class() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_virtual_class")).await;
    let mut state = PlanningState::new();
    remember_collection(
        &mut state,
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportClassInput {
        ref_: Some("class:one".to_string()),
        name: context.scoped_name("duplicate_class"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportClassInput {
        ref_: Some("class:two".to_string()),
        ..input
    };
    let err = (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class name"));
}

#[tokio::test]
async fn test_plan_object_rejects_duplicate_name_against_virtual_planned_object() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_virtual_object")).await;
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("duplicate_virtual_object_class"),
        description: "existing class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::new();
    remember_class(
        &mut state,
        Some("class:existing".to_string()),
        class_to_resolution(class.clone()),
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportObjectInput {
        ref_: Some("object:one".to_string()),
        name: context.scoped_name("duplicate_object"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportObjectInput {
        ref_: Some("object:two".to_string()),
        ..input
    };
    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object name"));
}

#[tokio::test]
async fn test_plan_class_rejects_duplicate_ref_against_virtual_planned_class() {
    let context = (TestContext::new()).await;
    let fixture_one = (context.collection_fixture("duplicate_class_ref_one")).await;
    let fixture_two = (context.collection_fixture("duplicate_class_ref_two")).await;
    let mut state = PlanningState::new();
    remember_collection(
        &mut state,
        Some("collection:one".to_string()),
        CollectionResolution {
            id: fixture_one.collection.id,
            name: fixture_one.collection.name.clone(),
            description: fixture_one.collection.description.clone(),
            parent_collection_id: fixture_one.collection.parent_collection_id,
            exists_in_db: true,
        },
    );
    remember_collection(
        &mut state,
        Some("collection:two".to_string()),
        CollectionResolution {
            id: fixture_two.collection.id,
            name: fixture_two.collection.name.clone(),
            description: fixture_two.collection.description.clone(),
            parent_collection_id: fixture_two.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportClassInput {
        ref_: Some("class:shared".to_string()),
        name: context.scoped_name("duplicate_class_ref_one"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:one".to_string()),
        collection_key: None,
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportClassInput {
        name: context.scoped_name("duplicate_class_ref_two"),
        collection_ref: Some("collection:two".to_string()),
        ..input
    };
    let err = (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class ref"));
}

#[tokio::test]
async fn test_plan_object_rejects_duplicate_ref_against_virtual_planned_object() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_object_ref")).await;
    let class_one = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("duplicate_object_ref_class_one"),
        description: "first class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let class_two = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("duplicate_object_ref_class_two"),
        description: "second class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::new();
    remember_class(
        &mut state,
        Some("class:one".to_string()),
        class_to_resolution(class_one.clone()),
    );
    remember_class(
        &mut state,
        Some("class:two".to_string()),
        class_to_resolution(class_two.clone()),
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportObjectInput {
        ref_: Some("object:shared".to_string()),
        name: context.scoped_name("duplicate_object_ref_one"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:one".to_string()),
        class_key: None,
    };

    (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportObjectInput {
        name: context.scoped_name("duplicate_object_ref_two"),
        class_ref: Some("class:two".to_string()),
        ..input
    };
    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object ref"));
}

#[tokio::test]
async fn test_resolve_collection_planning_backfills_caches_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_collection_cache")).await;
    let mut state = PlanningState::new();

    let resolved = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: fixture.collection.name.clone(),
            path: None,
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, fixture.collection.id);
    assert_eq!(
        state
            .collections_by_name
            .get(&fixture.collection.name)
            .unwrap()
            .iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>(),
        vec![fixture.collection.id]
    );
    assert_eq!(
        state
            .collections_by_id
            .get(&fixture.collection.id)
            .unwrap()
            .name,
        fixture.collection.name
    );
}

#[tokio::test]
async fn test_resolve_collection_planning_rejects_ambiguous_bare_name() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("ambiguous_parent_one")).await;
    let parent_two = (context.collection_fixture("ambiguous_parent_two")).await;
    let child_name = context.scoped_name("ambiguous_child");

    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "first ambiguous child".to_string(),
        group_id: GroupID::new(parent_one.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();
    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "second ambiguous child".to_string(),
        group_id: GroupID::new(parent_two.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();

    let mut state = PlanningState::new();
    let err = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: None,
        }),
    ))
    .await
    .unwrap_err();

    assert!(err.contains("ambiguous"));
    assert!(err.contains("collection_key.path"));
}

#[tokio::test]
async fn test_resolve_collection_planning_uses_path_to_disambiguate_name() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("path_parent_one")).await;
    let parent_two = (context.collection_fixture("path_parent_two")).await;
    let child_name = context.scoped_name("path_child");

    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "first path child".to_string(),
        group_id: GroupID::new(parent_one.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();
    let target_child = ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "second path child".to_string(),
        group_id: GroupID::new(parent_two.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();

    let mut state = PlanningState::new();
    let resolved = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: Some(vec![parent_two.collection.name.clone(), child_name]),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, target_child.id);
}

#[tokio::test]
async fn test_resolve_collection_by_id_planning_backfills_caches_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_collection_id_cache")).await;
    let mut state = PlanningState::new();

    let resolved =
        (resolve_collection_by_id_planning(&context.pool, &mut state, fixture.collection.id))
            .await
            .unwrap();

    assert_eq!(resolved.name, fixture.collection.name);
    assert_eq!(
        state
            .collections_by_name
            .get(&fixture.collection.name)
            .unwrap()
            .iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>(),
        vec![fixture.collection.id]
    );
    assert_eq!(
        state
            .collections_by_id
            .get(&fixture.collection.id)
            .unwrap()
            .name,
        fixture.collection.name
    );
}

#[tokio::test]
async fn test_resolve_class_planning_backfills_cache_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_class_cache")).await;
    let class_name_value = context.scoped_name("planning_class_cache_value");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "cached class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::new();

    let resolved = (resolve_class_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ClassKey {
            name: class.name.clone(),
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, class.id);
    assert_eq!(
        state
            .classes_by_key
            .get(&(fixture.collection.id, class.name.clone()))
            .unwrap()
            .id,
        class.id
    );
}

#[tokio::test]
async fn test_resolve_object_planning_backfills_cache_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_object_cache")).await;
    let class_name_value = context.scoped_name("planning_object_cache_class");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "cached class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let object_name_value = context.scoped_name("planning_object_cache_value");
    let object = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        name: object_name_value.clone(),
        description: "cached object".to_string(),
        data: serde_json::json!({"hostname":"cached"}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::new();

    let resolved = (resolve_object_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ObjectKey {
            name: object.name.clone(),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            }),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, object.id);
    assert_eq!(
        state
            .objects_by_key
            .get(&(class.id, object.name.clone()))
            .unwrap()
            .id,
        object.id
    );
}

#[tokio::test]
async fn test_update_collection_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_collection_ref")).await;
    let updated_description = context.scoped_name("updated_collection_description");
    let execution = PlannedExecution::UpdateCollection {
        collection_id: fixture.collection.id,
        input: ImportCollectionInput {
            ref_: Some("collection:existing".to_string()),
            name: fixture.collection.name.clone(),
            description: updated_description.clone(),
            condition: None,
            timestamps: None,
            parent_collection_ref: None,
            parent_collection_key: None,
        },
    };

    let class_input = ImportClassInput {
        ref_: Some("class:child".to_string()),
        name: context.scoped_name("class_after_collection_update"),
        description: "child".to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    let operations = [execution, PlannedExecution::CreateClass(class_input)]
        .into_iter()
        .enumerate()
        .map(|(index, operation)| {
            crate::services::import_boundary::import_operation_to_storage(operation)
                .map(|operation| StorageImportPlanItem::new(index, operation))
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let operations = StorageImportPlan::new(operations).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    backend.apply_import_strict(operations).await.unwrap();
    let collection = backend
        .import_collection_by_id(hubuum_domain::CollectionId::new(fixture.collection.id).unwrap())
        .await
        .unwrap();

    let collection = collection.expect("collection should remain available after update");
    assert_eq!(collection.id().id(), fixture.collection.id);
    assert_eq!(collection.description(), updated_description);
}

#[tokio::test]
async fn test_update_class_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_class_ref")).await;
    let class_name_value = context.scoped_name("existing_class_for_update");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "existing class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let execution = PlannedExecution::UpdateClass {
        class_id: class.id,
        input: ImportClassInput {
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated class".to_string(),
            condition: None,
            timestamps: None,
            json_schema: None,
            validate_schema: Some(false),
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        },
    };

    let object_input = ImportObjectInput {
        ref_: Some("object:child".to_string()),
        name: context.scoped_name("object_after_class_update"),
        description: "child".to_string(),
        condition: None,
        timestamps: None,
        data: serde_json::json!({"hostname":"child"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    let operations = [execution, PlannedExecution::CreateObject(object_input)]
        .into_iter()
        .enumerate()
        .map(|(index, operation)| {
            crate::services::import_boundary::import_operation_to_storage(operation)
                .map(|operation| StorageImportPlanItem::new(index, operation))
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let operations = StorageImportPlan::new(operations).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    backend.apply_import_strict(operations).await.unwrap();
    let updated = backend
        .import_class_by_name(
            hubuum_domain::CollectionId::new(fixture.collection.id).unwrap(),
            &class.name,
        )
        .await
        .unwrap();

    let updated = updated.expect("class should remain available after update");
    assert_eq!(updated.id().id(), class.id);
    assert_eq!(updated.name(), class.name);
}

#[tokio::test]
async fn test_plan_class_update_preserves_existing_schema_for_following_objects() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_class_schema_ref")).await;
    let schema = serde_json::json!({
        "type": "object",
        "required": ["hostname"],
        "properties": {
            "hostname": {"type": "string"}
        }
    });
    let class_name_value = context.scoped_name("existing_class_with_schema");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "existing class".to_string(),
        json_schema: Some(schema.clone()),
        validate_schema: Some(true),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let mut state = PlanningState::new();
    remember_collection(
        &mut state,
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportClassInput {
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated description".to_string(),
            condition: None,
            timestamps: None,
            json_schema: None,
            validate_schema: None,
            collection_ref: Some("collection:existing".to_string()),
            collection_key: None,
        },
    ))
    .await
    .unwrap();

    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportObjectInput {
            ref_: Some("object:invalid".to_string()),
            name: context.scoped_name("invalid_object_after_class_update"),
            description: "invalid".to_string(),
            condition: None,
            timestamps: None,
            data: serde_json::json!({"hostname": 42}),
            class_ref: Some("class:existing".to_string()),
            class_key: None,
        },
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
}

#[tokio::test]
async fn test_update_object_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_object_ref")).await;
    let class_name_value = context.scoped_name("existing_class_for_object_update");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "existing class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let object_name_value = context.scoped_name("existing_object_for_update");
    let object = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        name: object_name_value.clone(),
        description: "existing object".to_string(),
        data: serde_json::json!({"hostname":"existing"}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let execution = PlannedExecution::UpdateObject {
        object_id: object.id,
        input: ImportObjectInput {
            ref_: Some("object:existing".to_string()),
            name: object.name.clone(),
            description: "updated object".to_string(),
            condition: None,
            timestamps: None,
            data: serde_json::json!({"hostname":"updated"}),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            }),
        },
    };

    let operation = crate::services::import_boundary::import_operation_to_storage(execution)
        .map(|operation| StorageImportPlanItem::new(0, operation))
        .unwrap();
    let plan = StorageImportPlan::new(vec![operation]).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    backend.apply_import_strict(plan).await.unwrap();
    let resolved = backend
        .import_object_by_name(hubuum_domain::ClassId::new(class.id).unwrap(), &object.name)
        .await
        .unwrap()
        .expect("updated object should remain addressable by name");

    assert_eq!(resolved.id().id(), object.id);
    assert_eq!(resolved.description(), "updated object");
}

#[test]
fn test_request_hash_is_stable_for_reordered_json_objects() {
    let first = serde_json::json!({
        "version": 1,
        "dry_run": true,
        "graph": {
            "objects": [{
                "ref": "object:one",
                "name": "server-1",
                "description": "server",
                "data": {"a": 1, "b": {"x": 1, "y": 2}},
                "class_ref": "class:one"
            }]
        }
    });
    let second = serde_json::json!({
        "graph": {
            "objects": [{
                "class_ref": "class:one",
                "description": "server",
                "name": "server-1",
                "ref": "object:one",
                "data": {"b": {"y": 2, "x": 1}, "a": 1}
            }]
        },
        "dry_run": true,
        "version": 1
    });

    assert_eq!(
        request_hash(&first).unwrap(),
        request_hash(&second).unwrap()
    );
}

#[test]
fn test_sanitize_error_for_storage_masks_database_details() {
    let sanitized = sanitize_error_for_storage(&ApiError::DatabaseError(
        "relation users does not exist".to_string(),
    ));
    assert_eq!(sanitized, "Database operation failed");
}

#[test]
fn test_runtime_planning_failures_are_sanitized_for_storage() {
    let failure = PlanningFailure {
        kind: FailureKind::Runtime,
        item: planned_result(
            "collection",
            "lookup",
            Some("collection:one".to_string()),
            None,
        ),
        message: "relation users does not exist".to_string(),
    };

    assert_eq!(failure.message_for_storage(), "An internal error occurred");

    let stored = failure.into_result(1);
    assert_eq!(stored.error(), Some("An internal error occurred"));
}

#[test]
fn test_best_effort_execution_only_aborts_for_matching_policy_failures() {
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Abort),
        permission_policy: Some(ImportPermissionPolicy::Abort),
    };

    assert!(should_abort_best_effort_execution(
        &ApiError::Conflict("collision".to_string()),
        &mode,
    ));
    assert!(should_abort_best_effort_execution(
        &ApiError::Forbidden("permission".to_string()),
        &mode,
    ));
    assert!(!should_abort_best_effort_execution(
        &ApiError::NotFound("missing runtime ref".to_string()),
        &mode,
    ));
    assert!(!should_abort_best_effort_execution(
        &ApiError::DatabaseError("db error".to_string()),
        &mode,
    ));
}

#[tokio::test]
async fn test_process_one_task_export_failure_marks_single_failed_item() {
    let context = (TestContext::new()).await;
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Export,
        serde_json::json!({"export": "demo"}),
        0,
        "unimplemented-export-task",
    )
    .await;
    let task_id = crate::models::TaskID::new(task.id).expect("persisted task id must be positive");

    let claimed = claim_worker_test_task(&context, task.id).await;
    process_claimed_task_for_test(&context.pool, &claimed)
        .await
        .unwrap();
    let stored = find_task(&context.pool, task_id).await.unwrap();
    assert_eq!(stored.status, TaskStatus::Failed.as_str());
    assert_eq!(stored.total_items, 0);
    assert_eq!(stored.processed_items, 1);
    assert_eq!(stored.failed_items, 1);
}

#[tokio::test]
async fn test_mark_claimed_task_failed_uses_recorded_result_counts() {
    let context = (TestContext::new()).await;
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({"version": 1}),
        3,
        "fallback-count-task",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    assert_eq!(claimed.id, task.id);

    let storage_task_id =
        hubuum_domain::TaskId::new(task.id).expect("persisted task id must be positive");
    crate::storage::storage_handle(&context.pool)
        .record_import_results(vec![
            StorageImportResult::builder(storage_task_id, "collection", "create", "succeeded")
                .item_ref(Some("a".to_string()))
                .identifier(Some("a".to_string()))
                .build(),
            StorageImportResult::builder(storage_task_id, "class", "create", "failed")
                .item_ref(Some("b".to_string()))
                .identifier(Some("b".to_string()))
                .error(Some("failed".to_string()))
                .build(),
        ])
        .await
        .expect("import results should be recorded");

    (mark_claimed_task_failed(
        &context.pool,
        &claimed,
        &ApiError::InternalServerError("boom".to_string()),
    ))
    .await
    .unwrap();

    let stored = find_task(
        &context.pool,
        crate::models::TaskID::new(task.id).expect("persisted task id must be positive"),
    )
    .await
    .unwrap();
    assert_eq!(stored.processed_items, 2);
    assert_eq!(stored.success_items, 1);
    assert_eq!(stored.failed_items, 1);
}

#[tokio::test]
async fn test_reindex_failure_finalization_reloads_persisted_progress() {
    let context = TestContext::new().await;
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Reindex,
        serde_json::json!({"class_id": 1}),
        5,
        "reindex-progress-task",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    assert_eq!(claimed.id, task.id);
    update_task_state(
        &context.pool,
        &claimed,
        TaskStateChange::new(
            TaskStatus::Running,
            TaskResultCounts::from_stored(3, 3, 0).expect("test progress must be valid"),
        ),
    )
    .await
    .expect("reindex progress should be persisted");

    mark_claimed_task_failed(
        &context.pool,
        &claimed,
        &ApiError::InternalServerError("batch failed".to_string()),
    )
    .await
    .unwrap();

    let stored = find_task(
        &context.pool,
        crate::models::TaskID::new(task.id).expect("persisted task id must be positive"),
    )
    .await
    .unwrap();
    assert_eq!(stored.processed_items, 3);
    assert_eq!(stored.success_items, 3);
    assert_eq!(stored.failed_items, 1);
}
