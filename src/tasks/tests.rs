use chrono::NaiveDate;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use futures::executor::block_on;

use super::execution::{execute_import_best_effort, execute_import_strict, execute_planned_item};
use super::helpers::{
    class_to_resolution, planned_result, sanitize_error_for_storage,
    should_abort_best_effort_execution,
};
use super::planning::{plan_class, plan_namespace, plan_object};
use super::request_hash;
use super::resolution::{
    remember_class, remember_namespace, resolve_class_planning, resolve_namespace_by_id_planning,
    resolve_namespace_planning, resolve_object_planning, resolve_object_runtime,
};
use super::types::{
    ExecutionAccumulator, FailureKind, NamespaceResolution, PlannedExecution, PlannedItem,
    PlanningFailure, PlanningState, RuntimeState, WorkerLoopAction,
};
use super::worker::{background_worker_action, mark_claimed_task_failed, process_one_task};
use crate::db::traits::task::{
    count_import_results_summary, create_task_record, find_task_record, insert_import_results,
    list_task_events_with_total_count,
};
use crate::db::traits::task_import::{create_class_db, create_object_db};
use crate::db::with_connection;
use crate::errors::ApiError;
use crate::models::{
    ClassKey, ImportAtomicity, ImportClassInput, ImportCollisionPolicy, ImportMode,
    ImportNamespaceInput, ImportObjectInput, ImportPermissionPolicy, NamespaceKey,
    NewImportTaskResultRecord, NewTaskRecord, ObjectKey, TaskKind, TaskStatus,
};
use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name};
use crate::schema::namespaces::dsl::{name as namespace_name, namespaces};
use crate::schema::tasks::dsl::{created_at, id as task_id, tasks};
use crate::tests::TestContext;

#[test]
fn test_execute_import_strict_rolls_back_on_runtime_failure() {
    let context = block_on(TestContext::new());
    let namespace = context.scoped_name("strict_rollback_ns");
    let class = context.scoped_name("strict_rollback_class");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "namespace",
                "create",
                Some("ns:ok".to_string()),
                Some(namespace.clone()),
            ),
            execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                ref_: Some("ns:ok".to_string()),
                name: namespace.clone(),
                description: "Rollback namespace".to_string(),
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
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: Some("ns:missing".to_string()),
                namespace_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    let result = block_on(execute_import_strict(
        &context.pool,
        1,
        &planned_items,
        &mut accumulator,
    ));
    assert!(result.is_err());

    let namespace_exists = with_connection(&context.pool, |conn| {
        namespaces
            .filter(namespace_name.eq(&namespace))
            .count()
            .get_result::<i64>(conn)
    })
    .unwrap();
    let class_exists = with_connection(&context.pool, |conn| {
        hubuumclass
            .filter(class_name.eq(&class))
            .count()
            .get_result::<i64>(conn)
    })
    .unwrap();

    assert_eq!(namespace_exists, 0);
    assert_eq!(class_exists, 0);
    assert_eq!(accumulator.processed, 0);
}

#[test]
fn test_execute_import_best_effort_keeps_successful_items() {
    let context = block_on(TestContext::new());
    let namespace_one = context.scoped_name("best_effort_ns_one");
    let namespace_two = context.scoped_name("best_effort_ns_two");
    let class_bad = context.scoped_name("best_effort_class_bad");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "namespace",
                "create",
                Some("ns:one".to_string()),
                Some(namespace_one.clone()),
            ),
            execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                ref_: Some("ns:one".to_string()),
                name: namespace_one.clone(),
                description: "Best effort namespace one".to_string(),
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
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: Some("ns:missing".to_string()),
                namespace_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "namespace",
                "create",
                Some("ns:two".to_string()),
                Some(namespace_two.clone()),
            ),
            execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                ref_: Some("ns:two".to_string()),
                name: namespace_two.clone(),
                description: "Best effort namespace two".to_string(),
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    block_on(execute_import_best_effort(
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
    .unwrap();

    let namespace_count = with_connection(&context.pool, |conn| {
        namespaces
            .filter(namespace_name.eq_any([namespace_one.clone(), namespace_two.clone()]))
            .count()
            .get_result::<i64>(conn)
    })
    .unwrap();

    assert_eq!(namespace_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[test]
fn test_execute_import_best_effort_continues_after_non_policy_runtime_error() {
    let context = block_on(TestContext::new());
    let namespace_one = context.scoped_name("best_effort_runtime_ns_one");
    let namespace_two = context.scoped_name("best_effort_runtime_ns_two");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "namespace",
                "create",
                Some("ns:one".to_string()),
                Some(namespace_one.clone()),
            ),
            execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                ref_: Some("ns:one".to_string()),
                name: namespace_one.clone(),
                description: "Best effort namespace one".to_string(),
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
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: Some("ns:missing".to_string()),
                namespace_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "namespace",
                "create",
                Some("ns:two".to_string()),
                Some(namespace_two.clone()),
            ),
            execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                ref_: Some("ns:two".to_string()),
                name: namespace_two.clone(),
                description: "Best effort namespace two".to_string(),
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    block_on(execute_import_best_effort(
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
    .unwrap();

    let namespace_count = with_connection(&context.pool, |conn| {
        namespaces
            .filter(namespace_name.eq_any([namespace_one.clone(), namespace_two.clone()]))
            .count()
            .get_result::<i64>(conn)
    })
    .unwrap();

    assert_eq!(namespace_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[test]
fn test_execute_import_strict_preserves_underlying_error_variant() {
    let context = block_on(TestContext::new());
    let planned_items = vec![PlannedItem {
        result: planned_result(
            "namespace",
            "update",
            Some("ns:missing".to_string()),
            Some("missing".to_string()),
        ),
        execution: Some(PlannedExecution::UpdateNamespace {
            namespace_id: -999,
            input: ImportNamespaceInput {
                ref_: Some("ns:missing".to_string()),
                name: "missing".to_string(),
                description: "missing".to_string(),
            },
        }),
    }];

    let mut accumulator = ExecutionAccumulator::default();
    let result = block_on(execute_import_strict(
        &context.pool,
        1,
        &planned_items,
        &mut accumulator,
    ));

    assert!(matches!(result, Err(ApiError::NotFound(_))));
}

#[test]
fn test_process_one_task_marks_claimed_task_failed_when_execution_setup_errors() {
    let context = block_on(TestContext::new());
    let task = block_on(create_task_record(
        &context.pool,
        NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            idempotency_key: Some(context.scoped_name("missing-payload-task")),
            request_hash: None,
            request_payload: None,
            summary: None,
            total_items: 1,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        },
    ))
    .unwrap();

    let earliest = NaiveDate::from_ymd_opt(2000, 1, 1)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid timestamp");
    with_connection(&context.pool, |conn| {
        diesel::update(tasks.filter(task_id.eq(task.id)))
            .set(created_at.eq(earliest))
            .execute(conn)
    })
    .unwrap();

    for _ in 0..20 {
        let _ = block_on(process_one_task(&context.pool)).unwrap();

        let stored = block_on(find_task_record(&context.pool, task.id)).unwrap();
        if stored.status == TaskStatus::Failed.as_str() {
            assert!(stored.finished_at.is_some());
            assert!(stored.request_redacted_at.is_some());

            let (events, _) = block_on(list_task_events_with_total_count(
                &context.pool,
                task.id,
                &crate::models::search::QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                },
            ))
            .unwrap();
            let event_types = events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>();
            assert!(event_types.contains(&"validating"));
            assert!(event_types.contains(&"failed"));
            return;
        }
    }

    let stored = block_on(find_task_record(&context.pool, task.id)).unwrap();
    panic!(
        "Task {} did not reach failed state after repeated processing attempts; current status: {}",
        task.id, stored.status
    );
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
fn test_remember_namespace_populates_namespace_id_index() {
    let mut state = PlanningState::new();
    let namespace = NamespaceResolution {
        id: -42,
        name: "planned".to_string(),
        description: "planned namespace".to_string(),
        exists_in_db: false,
    };

    remember_namespace(
        &mut state,
        Some("ns:planned".to_string()),
        namespace.clone(),
    );

    assert_eq!(
        state.namespaces_by_id.get(&namespace.id).unwrap().name,
        namespace.name
    );
}

#[test]
fn test_plan_namespace_rejects_duplicate_name_within_request() {
    let context = block_on(TestContext::new());
    let mut state = PlanningState::new();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportNamespaceInput {
        ref_: Some("ns:one".to_string()),
        name: context.scoped_name("duplicate_namespace"),
        description: "first".to_string(),
    };

    block_on(plan_namespace(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .unwrap();

    let duplicate = ImportNamespaceInput {
        ref_: Some("ns:two".to_string()),
        ..input
    };
    let err = block_on(plan_namespace(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate namespace name"));
}

#[test]
fn test_plan_class_rejects_duplicate_name_against_virtual_planned_class() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("duplicate_virtual_class"));
    let mut state = PlanningState::new();
    remember_namespace(
        &mut state,
        Some("ns:existing".to_string()),
        NamespaceResolution {
            id: fixture.namespace.id,
            name: fixture.namespace.name.clone(),
            description: fixture.namespace.description.clone(),
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
        json_schema: None,
        validate_schema: Some(false),
        namespace_ref: Some("ns:existing".to_string()),
        namespace_key: None,
    };

    block_on(plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .unwrap();

    let duplicate = ImportClassInput {
        ref_: Some("class:two".to_string()),
        ..input
    };
    let err = block_on(plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class name"));
}

#[test]
fn test_plan_object_rejects_duplicate_name_against_virtual_planned_object() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("duplicate_virtual_object"));
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_virtual_object_class"),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
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
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    block_on(plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .unwrap();

    let duplicate = ImportObjectInput {
        ref_: Some("object:two".to_string()),
        ..input
    };
    let err = block_on(plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object name"));
}

#[test]
fn test_plan_class_rejects_duplicate_ref_against_virtual_planned_class() {
    let context = block_on(TestContext::new());
    let fixture_one = block_on(context.namespace_fixture("duplicate_class_ref_one"));
    let fixture_two = block_on(context.namespace_fixture("duplicate_class_ref_two"));
    let mut state = PlanningState::new();
    remember_namespace(
        &mut state,
        Some("ns:one".to_string()),
        NamespaceResolution {
            id: fixture_one.namespace.id,
            name: fixture_one.namespace.name.clone(),
            description: fixture_one.namespace.description.clone(),
            exists_in_db: true,
        },
    );
    remember_namespace(
        &mut state,
        Some("ns:two".to_string()),
        NamespaceResolution {
            id: fixture_two.namespace.id,
            name: fixture_two.namespace.name.clone(),
            description: fixture_two.namespace.description.clone(),
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
        json_schema: None,
        validate_schema: Some(false),
        namespace_ref: Some("ns:one".to_string()),
        namespace_key: None,
    };

    block_on(plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .unwrap();

    let duplicate = ImportClassInput {
        name: context.scoped_name("duplicate_class_ref_two"),
        namespace_ref: Some("ns:two".to_string()),
        ..input
    };
    let err = block_on(plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class ref"));
}

#[test]
fn test_plan_object_rejects_duplicate_ref_against_virtual_planned_object() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("duplicate_object_ref"));
    let class_one = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_object_ref_class_one"),
                description: "first class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
    .unwrap();
    let class_two = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_object_ref_class_two"),
                description: "second class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
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
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:one".to_string()),
        class_key: None,
    };

    block_on(plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .unwrap();

    let duplicate = ImportObjectInput {
        name: context.scoped_name("duplicate_object_ref_two"),
        class_ref: Some("class:two".to_string()),
        ..input
    };
    let err = block_on(plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object ref"));
}

#[test]
fn test_resolve_namespace_planning_backfills_caches_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("planning_namespace_cache"));
    let mut state = PlanningState::new();

    let resolved = block_on(resolve_namespace_planning(
        &context.pool,
        &mut state,
        None,
        Some(&NamespaceKey {
            name: fixture.namespace.name.clone(),
        }),
    ))
    .unwrap();

    assert_eq!(resolved.id, fixture.namespace.id);
    assert_eq!(
        state
            .namespaces_by_name
            .get(&fixture.namespace.name)
            .unwrap()
            .id,
        fixture.namespace.id
    );
    assert_eq!(
        state
            .namespaces_by_id
            .get(&fixture.namespace.id)
            .unwrap()
            .name,
        fixture.namespace.name
    );
}

#[test]
fn test_resolve_namespace_by_id_planning_backfills_caches_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("planning_namespace_id_cache"));
    let mut state = PlanningState::new();

    let resolved = block_on(resolve_namespace_by_id_planning(
        &context.pool,
        &mut state,
        fixture.namespace.id,
    ))
    .unwrap();

    assert_eq!(resolved.name, fixture.namespace.name);
    assert_eq!(
        state
            .namespaces_by_name
            .get(&fixture.namespace.name)
            .unwrap()
            .id,
        fixture.namespace.id
    );
    assert_eq!(
        state
            .namespaces_by_id
            .get(&fixture.namespace.id)
            .unwrap()
            .name,
        fixture.namespace.name
    );
}

#[test]
fn test_resolve_class_planning_backfills_cache_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("planning_class_cache"));
    let class_name_value = context.scoped_name("planning_class_cache_value");
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "cached class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
    .unwrap();
    let mut state = PlanningState::new();

    let resolved = block_on(resolve_class_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ClassKey {
            name: class.name.clone(),
            namespace_ref: None,
            namespace_key: Some(NamespaceKey {
                name: fixture.namespace.name.clone(),
            }),
        }),
    ))
    .unwrap();

    assert_eq!(resolved.id, class.id);
    assert_eq!(
        state
            .classes_by_key
            .get(&(fixture.namespace.id, class.name.clone()))
            .unwrap()
            .id,
        class.id
    );
}

#[test]
fn test_resolve_object_planning_backfills_cache_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("planning_object_cache"));
    let class_name_value = context.scoped_name("planning_object_cache_class");
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "cached class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
    .unwrap();
    let object_name_value = context.scoped_name("planning_object_cache_value");
    let object = with_connection(&context.pool, |conn| {
        create_object_db(
            conn,
            &ImportObjectInput {
                ref_: None,
                name: object_name_value.clone(),
                description: "cached object".to_string(),
                data: serde_json::json!({"hostname":"cached"}),
                class_ref: None,
                class_key: Some(ClassKey {
                    name: class.name.clone(),
                    namespace_ref: None,
                    namespace_key: Some(NamespaceKey {
                        name: fixture.namespace.name.clone(),
                    }),
                }),
            },
            &class,
        )
    })
    .unwrap();
    let mut state = PlanningState::new();

    let resolved = block_on(resolve_object_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ObjectKey {
            name: object.name.clone(),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            }),
        }),
    ))
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

#[test]
fn test_update_namespace_refreshes_runtime_ref_for_following_items() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("update_namespace_ref"));
    let updated_description = context.scoped_name("updated_namespace_description");
    let execution = PlannedExecution::UpdateNamespace {
        namespace_id: fixture.namespace.id,
        input: ImportNamespaceInput {
            ref_: Some("ns:existing".to_string()),
            name: fixture.namespace.name.clone(),
            description: updated_description.clone(),
        },
    };

    let class_input = ImportClassInput {
        ref_: Some("class:child".to_string()),
        name: context.scoped_name("class_after_namespace_update"),
        description: "child".to_string(),
        json_schema: None,
        validate_schema: Some(false),
        namespace_ref: Some("ns:existing".to_string()),
        namespace_key: None,
    };

    let result = with_connection(&context.pool, |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution)?;
        execute_planned_item(
            conn,
            &mut runtime,
            &PlannedExecution::CreateClass(class_input.clone()),
        )?;
        Ok::<_, ApiError>(runtime.namespaces_by_ref.get("ns:existing").cloned())
    })
    .unwrap();

    let namespace = result.expect("namespace ref should be available after update");
    assert_eq!(namespace.id, fixture.namespace.id);
    assert_eq!(namespace.description, updated_description);
}

#[test]
fn test_update_class_refreshes_runtime_ref_for_following_items() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("update_class_ref"));
    let class_name_value = context.scoped_name("existing_class_for_update");
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
    .unwrap();

    let execution = PlannedExecution::UpdateClass {
        class_id: class.id,
        input: ImportClassInput {
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated class".to_string(),
            json_schema: None,
            validate_schema: Some(false),
            namespace_ref: None,
            namespace_key: Some(NamespaceKey {
                name: fixture.namespace.name.clone(),
            }),
        },
    };

    let object_input = ImportObjectInput {
        ref_: Some("object:child".to_string()),
        name: context.scoped_name("object_after_class_update"),
        description: "child".to_string(),
        data: serde_json::json!({"hostname":"child"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    let result = with_connection(&context.pool, |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution)?;
        execute_planned_item(
            conn,
            &mut runtime,
            &PlannedExecution::CreateObject(object_input.clone()),
        )?;
        Ok::<_, ApiError>(runtime.classes_by_ref.get("class:existing").cloned())
    })
    .unwrap();

    let updated = result.expect("class ref should be available after update");
    assert_eq!(updated.id, class.id);
    assert_eq!(updated.name, class.name);
}

#[test]
fn test_plan_class_update_preserves_existing_schema_for_following_objects() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("update_class_schema_ref"));
    let schema = serde_json::json!({
        "type": "object",
        "required": ["hostname"],
        "properties": {
            "hostname": {"type": "string"}
        }
    });
    let class_name_value = context.scoped_name("existing_class_with_schema");
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "existing class".to_string(),
                json_schema: Some(schema.clone()),
                validate_schema: Some(true),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
    .unwrap();

    let mut state = PlanningState::new();
    remember_namespace(
        &mut state,
        Some("ns:existing".to_string()),
        NamespaceResolution {
            id: fixture.namespace.id,
            name: fixture.namespace.name.clone(),
            description: fixture.namespace.description.clone(),
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };

    block_on(plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportClassInput {
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated description".to_string(),
            json_schema: None,
            validate_schema: None,
            namespace_ref: Some("ns:existing".to_string()),
            namespace_key: None,
        },
    ))
    .unwrap();

    let err = block_on(plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportObjectInput {
            ref_: Some("object:invalid".to_string()),
            name: context.scoped_name("invalid_object_after_class_update"),
            description: "invalid".to_string(),
            data: serde_json::json!({"hostname": 42}),
            class_ref: Some("class:existing".to_string()),
            class_key: None,
        },
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
}

#[test]
fn test_update_object_refreshes_runtime_ref_for_following_items() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.namespace_fixture("update_object_ref"));
    let class_name_value = context.scoped_name("existing_class_for_object_update");
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            },
            fixture.namespace.id,
        )
    })
    .unwrap();

    let object_name_value = context.scoped_name("existing_object_for_update");
    let object = with_connection(&context.pool, |conn| {
        create_object_db(
            conn,
            &ImportObjectInput {
                ref_: None,
                name: object_name_value.clone(),
                description: "existing object".to_string(),
                data: serde_json::json!({"hostname":"existing"}),
                class_ref: None,
                class_key: Some(ClassKey {
                    name: class.name.clone(),
                    namespace_ref: None,
                    namespace_key: Some(NamespaceKey {
                        name: fixture.namespace.name.clone(),
                    }),
                }),
            },
            &class,
        )
    })
    .unwrap();

    let execution = PlannedExecution::UpdateObject {
        object_id: object.id,
        input: ImportObjectInput {
            ref_: Some("object:existing".to_string()),
            name: object.name.clone(),
            description: "updated object".to_string(),
            data: serde_json::json!({"hostname":"updated"}),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                namespace_ref: None,
                namespace_key: Some(NamespaceKey {
                    name: fixture.namespace.name.clone(),
                }),
            }),
        },
    };

    let resolved = with_connection(&context.pool, |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution)?;
        resolve_object_runtime(conn, &runtime, Some("object:existing"), None::<&ObjectKey>)
    })
    .unwrap();

    assert_eq!(resolved.id, object.id);
    assert_eq!(resolved.description, "updated object");
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
        item: planned_result("namespace", "lookup", Some("ns:one".to_string()), None),
        message: "relation users does not exist".to_string(),
    };

    assert_eq!(failure.message_for_storage(), "An internal error occurred");

    let stored = failure.into_result(1);
    assert_eq!(stored.error.as_deref(), Some("An internal error occurred"));
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

#[test]
fn test_process_one_task_report_failure_marks_single_failed_item() {
    let context = block_on(TestContext::new());
    let task = block_on(create_task_record(
        &context.pool,
        NewTaskRecord {
            kind: TaskKind::Report.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            idempotency_key: Some(context.scoped_name("unimplemented-report-task")),
            request_hash: None,
            request_payload: Some(serde_json::json!({"report": "demo"})),
            summary: None,
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        },
    ))
    .unwrap();

    let earliest = NaiveDate::from_ymd_opt(2000, 1, 1)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid timestamp");
    with_connection(&context.pool, |conn| {
        diesel::update(tasks.filter(task_id.eq(task.id)))
            .set(created_at.eq(earliest))
            .execute(conn)
    })
    .unwrap();

    for _ in 0..20 {
        let _ = block_on(process_one_task(&context.pool)).unwrap();
        let stored = block_on(find_task_record(&context.pool, task.id)).unwrap();
        if stored.status == TaskStatus::Failed.as_str() {
            assert_eq!(stored.total_items, 0);
            assert_eq!(stored.processed_items, 1);
            assert_eq!(stored.failed_items, 1);
            return;
        }
    }

    let stored = block_on(find_task_record(&context.pool, task.id)).unwrap();
    panic!(
        "Task {} did not reach failed state after repeated processing attempts; current status: {}",
        task.id, stored.status
    );
}

#[test]
fn test_mark_claimed_task_failed_uses_recorded_result_counts() {
    let context = block_on(TestContext::new());
    let task = block_on(create_task_record(
        &context.pool,
        NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            idempotency_key: Some(context.scoped_name("fallback-count-task")),
            request_hash: None,
            request_payload: Some(serde_json::json!({"version": 1})),
            summary: None,
            total_items: 3,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        },
    ))
    .unwrap();

    block_on(insert_import_results(
        &context.pool,
        &[
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("a".to_string()),
                entity_kind: "namespace".to_string(),
                action: "create".to_string(),
                identifier: Some("a".to_string()),
                outcome: "succeeded".to_string(),
                error: None,
                details: None,
            },
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("b".to_string()),
                entity_kind: "class".to_string(),
                action: "create".to_string(),
                identifier: Some("b".to_string()),
                outcome: "failed".to_string(),
                error: Some("failed".to_string()),
                details: None,
            },
        ],
    ))
    .unwrap();

    block_on(mark_claimed_task_failed(
        &context.pool,
        &task,
        &ApiError::InternalServerError("boom".to_string()),
    ))
    .unwrap();

    let stored = block_on(find_task_record(&context.pool, task.id)).unwrap();
    assert_eq!(stored.processed_items, 2);
    assert_eq!(stored.success_items, 1);
    assert_eq!(stored.failed_items, 1);
}

#[test]
fn test_count_import_results_summary_counts_success_and_failure_rows() {
    let context = block_on(TestContext::new());
    let task = block_on(create_task_record(
        &context.pool,
        NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            idempotency_key: Some(context.scoped_name("aggregate-count-task")),
            request_hash: None,
            request_payload: Some(serde_json::json!({"version": 1})),
            summary: None,
            total_items: 4,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        },
    ))
    .unwrap();

    block_on(insert_import_results(
        &context.pool,
        &[
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("one".to_string()),
                entity_kind: "namespace".to_string(),
                action: "create".to_string(),
                identifier: Some("one".to_string()),
                outcome: "succeeded".to_string(),
                error: None,
                details: None,
            },
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("two".to_string()),
                entity_kind: "class".to_string(),
                action: "create".to_string(),
                identifier: Some("two".to_string()),
                outcome: "failed".to_string(),
                error: Some("failed".to_string()),
                details: None,
            },
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("three".to_string()),
                entity_kind: "object".to_string(),
                action: "update".to_string(),
                identifier: Some("three".to_string()),
                outcome: "planned".to_string(),
                error: None,
                details: None,
            },
        ],
    ))
    .unwrap();

    let counts = block_on(count_import_results_summary(&context.pool, task.id)).unwrap();

    assert_eq!(counts.processed, 3);
    assert_eq!(counts.success, 2);
    assert_eq!(counts.failed, 1);
}
