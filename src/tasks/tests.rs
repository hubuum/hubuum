mod artifact_pipeline;
use crate::models::HubuumClassID;
use crate::tests::asserts::expect_legacy_task;
use chrono::NaiveDateTime;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::ClassId;
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
    resolve_class_planning, resolve_collection_by_id_planning, resolve_collection_planning,
    resolve_object_planning,
};
use super::types::{
    CacheLookup, CollectionResolution, ExecutionAccumulator, FailureKind, ObjectResolution,
    PlannedExecution, PlannedItem, PlanningFailure, PlanningState, WorkerLoopAction,
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
    NewHubuumObjectRelation, NewTaskEventRecord, ObjectKey, Permissions, PrincipalKey,
    RemoteAuthConfig, RemoteHttpMethod, RemoteTargetSubjectType, ResourceRevision,
    RestoreTimestamps, TaskResultCounts, TaskStatus,
};
use crate::permissions::PermissionBackend;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::types::{ResourceFields, ResourceKind};
use crate::schema::collections::dsl::{collections, name as collection_name};
use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name};
use crate::services::tasks::{
    ClaimedTask, TaskStateChange, append_task_event, find_task, update_task_state,
};
use crate::storage::{
    CollectionStorage, ImportStorage, StorageImportPlan, StorageImportPlanItem,
    StorageImportResult, StorageTaskCreateRequest, StorageTaskKind, StorageTaskScopeSnapshot,
    TaskQueueStorage, with_mutation_provenance,
};
use crate::tests::{TestContext, create_test_group};
use crate::traits::CanSave;
use hubuum_events_core::{Action, EntityType, EventEntityId, MutationProvenance, TraceLink};
use hubuum_storage_postgres::PostgresStorage;
use hubuum_storage_postgres::{capture_queries, with_connection};

pub(super) async fn create_worker_test_task(
    context: &TestContext,
    kind: StorageTaskKind,
    payload: serde_json::Value,
    total_items: i32,
    label: &str,
) -> crate::models::TaskRecord {
    create_worker_test_task_with_trace(context, kind, payload, total_items, label, None).await
}

async fn create_worker_test_task_with_trace(
    context: &TestContext,
    kind: StorageTaskKind,
    payload: serde_json::Value,
    total_items: i32,
    label: &str,
    trace_link: Option<TraceLink>,
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
            .trace_link(trace_link)
            .try_build()
            .expect("worker test task request should be valid"),
        )
        .await
        .map(expect_legacy_task)
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

pub(super) async fn claim_worker_test_task(
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

#[rstest]
#[case::edited_binding(false)]
#[case::legacy_unbound_invocation(true)]
#[tokio::test]
async fn remote_worker_rejects_changed_or_unbound_target_before_resolving_credentials(
    #[case] legacy: bool,
) {
    let context = TestContext::new().await;
    let backend = crate::tests::app_context(&context.pool);
    let fixture = context.collection_fixture("remote_binding_revision").await;
    let target = crate::services::remote_targets::create_remote_target(&backend,
        serde_json::from_value(serde_json::json!({
            "collection_id": fixture.collection_id(), "name":"revision-bound-target", "description":"test",
            "method":"post", "url_template":"https://service.example/hook", "allowed_subject_types":["collection"],
            "auth_config":{"type":"bearer_secret", "secret":"missing_revision_test_secret"}
        })).unwrap(), crate::events::EventContext::system()).await.unwrap();
    let mut payload = serde_json::json!({
        "target_id":target.id, "target_revision":target.revision,
        "subject":{"type":"collection", "collection_id":fixture.collection_id()},
        "parameters":{}, "body_override":{}
    });
    if legacy {
        payload.as_object_mut().unwrap().remove("target_revision");
    } else {
        crate::services::remote_targets::update_remote_target(&backend, target.id,
            serde_json::from_value(serde_json::json!({"url_template":"https://replacement.example/hook", "enabled":true})).unwrap(),
            &target, crate::events::EventContext::system()).await.unwrap();
    }
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::RemoteCall,
        payload,
        1,
        "remote_revision_task",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let principal = crate::services::authentication::ExecutionPrincipal::resolve(
        &backend,
        context.admin_user.id,
    )
    .await
    .unwrap();
    let status = super::remote_call::execute_remote_call_task(&backend, &claimed, &principal, None)
        .await
        .unwrap();
    assert_eq!(status, TaskStatus::Failed);
    let finished = find_task(&backend, crate::models::TaskID::new(task.id).unwrap())
        .await
        .unwrap();
    assert!(
        finished
            .summary
            .unwrap()
            .contains("submit a new invocation")
    );
    fixture.cleanup().await.unwrap();
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

#[test]
fn test_background_worker_continues_immediately_after_processing_a_task() {
    let result = Ok(true);
    assert_eq!(
        background_worker_action(&result),
        WorkerLoopAction::Continue
    );
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

#[rstest]
#[case::nested_and_escaped(
    r#"{"z":18446744073709551617,"a":[true,"line\n🫐",{"y":1,"x":2}]}"#,
    "f3a705dc6c3f0d178d19b6ae8bd346c9df4f54673059768eb16edc1be82a669e"
)]
#[case::decimal(
    r#"{"b":1.2300,"a":[false,null]}"#,
    "87691c5d9d60a92c51762ec8a60a3c70dae8edc1af50b917383ef3f18b4aca27"
)]
#[case::reordered_array(
    r#"{"b":1.2300,"a":[null,false]}"#,
    "08879a9479bf9040f3dfdbc552b789b7dd8252e03704594962a07d279bb383ab"
)]
fn task_request_hash_matches_persisted_digest(#[case] raw: &str, #[case] expected: &str) {
    let input = serde_json::from_str(raw).unwrap();
    assert_eq!(request_hash(&input).unwrap(), expected);
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

fn task_test_trace_link(trace_id: &str, span_id: &str) -> TraceLink {
    TraceLink::new(trace_id, span_id, 1, 0).expect("test trace link must be valid")
}

#[tokio::test]
async fn task_queued_event_persists_the_admission_trace_link() {
    let context = TestContext::new().await;
    let admission_link =
        task_test_trace_link("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    let task = create_worker_test_task_with_trace(
        &context,
        StorageTaskKind::Export,
        serde_json::json!({}),
        0,
        "queued-trace-link",
        Some(admission_link.clone()),
    )
    .await;

    let events = hubuum_storage_postgres::test_support::list_events(
        context.pool.get_ref(),
        EntityType::Task,
        EventEntityId::new(task.id).expect("persisted task id must be positive"),
        Some(Action::Queued),
    )
    .await
    .expect("queued task event should be readable");
    let envelope = events
        .into_iter()
        .next()
        .expect("queued task event should exist")
        .into_parts()
        .0;

    assert_eq!(envelope.trace_link(), Some(&admission_link));

    hubuum_storage_postgres::test_support::delete_task(
        context.pool.get_ref(),
        hubuum_domain::TaskId::new(task.id).expect("persisted task id must be positive"),
    )
    .await
    .expect("queued trace-link task fixture should be removed");
}

#[tokio::test]
async fn worker_task_event_prefers_the_current_execution_trace_link() {
    let context = TestContext::new().await;
    let admission_link =
        task_test_trace_link("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    let execution_link =
        task_test_trace_link("6bf92f3577b34da6a3ce929d0e0e4738", "10f067aa0ba902b8");
    let task = create_worker_test_task_with_trace(
        &context,
        StorageTaskKind::Export,
        serde_json::json!({}),
        0,
        "execution-trace-link",
        Some(admission_link),
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let provenance = MutationProvenance::worker(
        Some(
            hubuum_domain::PrincipalId::new(context.admin_user.id)
                .expect("persisted test principal id must be positive"),
        ),
        hubuum_domain::TaskId::new(task.id).expect("persisted task id must be positive"),
    )
    .with_trace_link(Some(execution_link.clone()));

    with_mutation_provenance(&context.pool, Some(provenance), async {
        append_task_event(
            &context.pool,
            &claimed,
            NewTaskEventRecord {
                event_type: TaskStatus::Running.as_str().to_string(),
                message: "Task execution started".to_string(),
                data: None,
            },
        )
        .await
    })
    .await
    .expect("worker task event should be appended");

    let events = hubuum_storage_postgres::test_support::list_events(
        context.pool.get_ref(),
        EntityType::Task,
        EventEntityId::new(task.id).expect("persisted task id must be positive"),
        Some(Action::Running),
    )
    .await
    .expect("running task event should be readable");
    let envelope = events
        .into_iter()
        .next()
        .expect("running task event should exist")
        .into_parts()
        .0;

    assert_eq!(envelope.trace_link(), Some(&execution_link));

    hubuum_storage_postgres::test_support::delete_task(
        context.pool.get_ref(),
        hubuum_domain::TaskId::new(task.id).expect("persisted task id must be positive"),
    )
    .await
    .expect("execution trace-link task fixture should be removed");
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

mod fencing;

mod import_execution;
mod import_planning;
