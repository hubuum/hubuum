use chrono::NaiveDate;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use futures::executor::block_on;

use super::execution::{execute_import_best_effort, execute_import_strict, execute_planned_item};
use super::helpers::{
    class_to_resolution, planned_result, sanitize_error_for_storage,
    should_abort_best_effort_execution,
};
use super::planning::{plan_class, plan_collection, plan_object};
use super::request_hash;
use super::resolution::{
    remember_class, remember_collection, resolve_class_planning, resolve_collection_by_id_planning,
    resolve_collection_planning, resolve_object_planning, resolve_object_runtime,
};
use super::types::{
    CollectionResolution, ExecutionAccumulator, FailureKind, PlannedExecution, PlannedItem,
    PlanningFailure, PlanningState, RuntimeState, WorkerLoopAction,
};
use super::worker::{background_worker_action, mark_claimed_task_failed, process_one_task};
use crate::db::traits::task::{TaskBackend, insert_import_results};
use crate::db::traits::task_import::{create_class_db, create_object_db};
use crate::db::with_connection;
use crate::errors::ApiError;
use crate::models::{
    ClassKey, CollectionID, CollectionKey, ImportAtomicity, ImportClassInput,
    ImportCollectionInput, ImportCollisionPolicy, ImportMode, ImportObjectInput,
    ImportPermissionPolicy, NewCollectionWithAssignee, NewImportTaskResultRecord, NewTaskRecord,
    ObjectKey, TaskKind, TaskStatus,
};
use crate::schema::collections::dsl::{collections, name as collection_name};
use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name};
use crate::schema::tasks::dsl::{created_at, id as task_id, tasks};
use crate::tests::TestContext;
use crate::traits::CanSave;

#[test]
fn test_execute_import_strict_rolls_back_on_runtime_failure() {
    let context = block_on(TestContext::new());
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
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
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

    let collection_exists = with_connection(&context.pool, |conn| {
        collections
            .filter(collection_name.eq(&collection))
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

    assert_eq!(collection_exists, 0);
    assert_eq!(class_exists, 0);
    assert_eq!(accumulator.processed, 0);
}

#[test]
fn test_execute_import_best_effort_keeps_successful_items() {
    let context = block_on(TestContext::new());
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
                parent_collection_ref: None,
                parent_collection_key: None,
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

    let collection_count = with_connection(&context.pool, |conn| {
        collections
            .filter(collection_name.eq_any([collection_one.clone(), collection_two.clone()]))
            .count()
            .get_result::<i64>(conn)
    })
    .unwrap();

    assert_eq!(collection_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[test]
fn test_execute_import_best_effort_continues_after_non_policy_runtime_error() {
    let context = block_on(TestContext::new());
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
                parent_collection_ref: None,
                parent_collection_key: None,
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

    let collection_count = with_connection(&context.pool, |conn| {
        collections
            .filter(collection_name.eq_any([collection_one.clone(), collection_two.clone()]))
            .count()
            .get_result::<i64>(conn)
    })
    .unwrap();

    assert_eq!(collection_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[test]
fn test_execute_import_strict_preserves_underlying_error_variant() {
    let context = block_on(TestContext::new());
    let planned_items = vec![PlannedItem {
        result: planned_result(
            "collection",
            "update",
            Some("collection:missing".to_string()),
            Some("missing".to_string()),
        ),
        execution: Some(PlannedExecution::UpdateCollection {
            collection_id: -999,
            input: ImportCollectionInput {
                ref_: Some("collection:missing".to_string()),
                name: "missing".to_string(),
                description: "missing".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
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
    let task = block_on(
        NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
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
        }
        .create(&context.pool),
    )
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

        let stored = block_on(task.find_record(&context.pool)).unwrap();
        if stored.status == TaskStatus::Failed.as_str() {
            assert!(stored.finished_at.is_some());
            assert!(stored.request_redacted_at.is_some());

            let (events, _) = block_on(task.list_events_with_total_count(
                &context.pool,
                &crate::models::search::QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                    include_total: true,
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

    let stored = block_on(task.find_record(&context.pool)).unwrap();
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

#[test]
fn test_plan_collection_rejects_duplicate_name_within_request() {
    let context = block_on(TestContext::new());
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
        parent_collection_ref: None,
        parent_collection_key: None,
    };

    block_on(plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .unwrap();

    let duplicate = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        ..input
    };
    let err = block_on(plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate collection name"));
}

#[test]
fn test_plan_collection_allows_duplicate_names_under_different_parents() {
    let context = block_on(TestContext::new());
    let parent_one = block_on(context.collection_fixture("duplicate_import_parent_one"));
    let parent_two = block_on(context.collection_fixture("duplicate_import_parent_two"));
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
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_two.collection.name.clone(),
            path: None,
        }),
    };

    block_on(plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_one,
    ))
    .unwrap();
    block_on(plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_two,
    ))
    .unwrap();
}

#[test]
fn test_plan_class_rejects_duplicate_name_against_virtual_planned_class() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("duplicate_virtual_class"));
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
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
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
    let fixture = block_on(context.collection_fixture("duplicate_virtual_object"));
    let class = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_virtual_object_class"),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
    let fixture_one = block_on(context.collection_fixture("duplicate_class_ref_one"));
    let fixture_two = block_on(context.collection_fixture("duplicate_class_ref_two"));
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
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:one".to_string()),
        collection_key: None,
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
        collection_ref: Some("collection:two".to_string()),
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
    let fixture = block_on(context.collection_fixture("duplicate_object_ref"));
    let class_one = with_connection(&context.pool, |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_object_ref_class_one"),
                description: "first class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
fn test_resolve_collection_planning_backfills_caches_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("planning_collection_cache"));
    let mut state = PlanningState::new();

    let resolved = block_on(resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: fixture.collection.name.clone(),
            path: None,
        }),
    ))
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

#[test]
fn test_resolve_collection_planning_rejects_ambiguous_bare_name() {
    let context = block_on(TestContext::new());
    let parent_one = block_on(context.collection_fixture("ambiguous_parent_one"));
    let parent_two = block_on(context.collection_fixture("ambiguous_parent_two"));
    let child_name = context.scoped_name("ambiguous_child");

    block_on(
        (NewCollectionWithAssignee {
            name: child_name.clone(),
            description: "first ambiguous child".to_string(),
            group_id: parent_one.owner_group.id,
            parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
        })
        .save_without_events(&context.pool),
    )
    .unwrap();
    block_on(
        (NewCollectionWithAssignee {
            name: child_name.clone(),
            description: "second ambiguous child".to_string(),
            group_id: parent_two.owner_group.id,
            parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
        })
        .save_without_events(&context.pool),
    )
    .unwrap();

    let mut state = PlanningState::new();
    let err = block_on(resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: None,
        }),
    ))
    .unwrap_err();

    assert!(err.contains("ambiguous"));
    assert!(err.contains("collection_key.path"));
}

#[test]
fn test_resolve_collection_planning_uses_path_to_disambiguate_name() {
    let context = block_on(TestContext::new());
    let parent_one = block_on(context.collection_fixture("path_parent_one"));
    let parent_two = block_on(context.collection_fixture("path_parent_two"));
    let child_name = context.scoped_name("path_child");

    block_on(
        (NewCollectionWithAssignee {
            name: child_name.clone(),
            description: "first path child".to_string(),
            group_id: parent_one.owner_group.id,
            parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
        })
        .save_without_events(&context.pool),
    )
    .unwrap();
    let target_child = block_on(
        (NewCollectionWithAssignee {
            name: child_name.clone(),
            description: "second path child".to_string(),
            group_id: parent_two.owner_group.id,
            parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
        })
        .save_without_events(&context.pool),
    )
    .unwrap();

    let mut state = PlanningState::new();
    let resolved = block_on(resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: Some(vec![parent_two.collection.name.clone(), child_name]),
        }),
    ))
    .unwrap();

    assert_eq!(resolved.id, target_child.id);
}

#[test]
fn test_resolve_collection_by_id_planning_backfills_caches_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("planning_collection_id_cache"));
    let mut state = PlanningState::new();

    let resolved = block_on(resolve_collection_by_id_planning(
        &context.pool,
        &mut state,
        fixture.collection.id,
    ))
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

#[test]
fn test_resolve_class_planning_backfills_cache_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("planning_class_cache"));
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        }),
    ))
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

#[test]
fn test_resolve_object_planning_backfills_cache_after_db_lookup() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("planning_object_cache"));
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
                    collection_ref: None,
                    collection_key: Some(CollectionKey {
                        name: fixture.collection.name.clone(),
                        path: None,
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
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
fn test_update_collection_refreshes_runtime_ref_for_following_items() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("update_collection_ref"));
    let updated_description = context.scoped_name("updated_collection_description");
    let execution = PlannedExecution::UpdateCollection {
        collection_id: fixture.collection.id,
        input: ImportCollectionInput {
            ref_: Some("collection:existing".to_string()),
            name: fixture.collection.name.clone(),
            description: updated_description.clone(),
            parent_collection_ref: None,
            parent_collection_key: None,
        },
    };

    let class_input = ImportClassInput {
        ref_: Some("class:child".to_string()),
        name: context.scoped_name("class_after_collection_update"),
        description: "child".to_string(),
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    let result = with_connection(&context.pool, |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution)?;
        execute_planned_item(
            conn,
            &mut runtime,
            &PlannedExecution::CreateClass(class_input.clone()),
        )?;
        Ok::<_, ApiError>(
            runtime
                .collections_by_ref
                .get("collection:existing")
                .cloned(),
        )
    })
    .unwrap();

    let collection = result.expect("collection ref should be available after update");
    assert_eq!(collection.id, fixture.collection.id);
    assert_eq!(collection.description, updated_description);
}

#[test]
fn test_update_class_refreshes_runtime_ref_for_following_items() {
    let context = block_on(TestContext::new());
    let fixture = block_on(context.collection_fixture("update_class_ref"));
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
    let fixture = block_on(context.collection_fixture("update_class_schema_ref"));
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
    })
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
            collection_ref: Some("collection:existing".to_string()),
            collection_key: None,
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
    let fixture = block_on(context.collection_fixture("update_object_ref"));
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
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
                    collection_ref: None,
                    collection_key: Some(CollectionKey {
                        name: fixture.collection.name.clone(),
                        path: None,
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
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
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
fn test_process_one_task_export_failure_marks_single_failed_item() {
    let context = block_on(TestContext::new());
    let task = block_on(
        NewTaskRecord {
            kind: TaskKind::Export.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            idempotency_key: Some(context.scoped_name("unimplemented-export-task")),
            request_hash: None,
            request_payload: Some(serde_json::json!({"export": "demo"})),
            summary: None,
            total_items: 0,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
        }
        .create(&context.pool),
    )
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
        let stored = block_on(task.find_record(&context.pool)).unwrap();
        if stored.status == TaskStatus::Failed.as_str() {
            assert_eq!(stored.total_items, 0);
            assert_eq!(stored.processed_items, 1);
            assert_eq!(stored.failed_items, 1);
            return;
        }
    }

    let stored = block_on(task.find_record(&context.pool)).unwrap();
    panic!(
        "Task {} did not reach failed state after repeated processing attempts; current status: {}",
        task.id, stored.status
    );
}

#[test]
fn test_mark_claimed_task_failed_uses_recorded_result_counts() {
    let context = block_on(TestContext::new());
    let task = block_on(
        NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
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
        }
        .create(&context.pool),
    )
    .unwrap();

    block_on(insert_import_results(
        &context.pool,
        &[
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("a".to_string()),
                entity_kind: "collection".to_string(),
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

    let stored = block_on(task.find_record(&context.pool)).unwrap();
    assert_eq!(stored.processed_items, 2);
    assert_eq!(stored.success_items, 1);
    assert_eq!(stored.failed_items, 1);
}

#[test]
fn test_count_import_results_summary_counts_success_and_failure_rows() {
    let context = block_on(TestContext::new());
    let task = block_on(
        NewTaskRecord {
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Queued.as_str().to_string(),
            submitted_by: Some(context.admin_user.id),
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
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
        }
        .create(&context.pool),
    )
    .unwrap();

    block_on(insert_import_results(
        &context.pool,
        &[
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("one".to_string()),
                entity_kind: "collection".to_string(),
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

    let counts = block_on(task.count_import_results(&context.pool)).unwrap();

    assert_eq!(counts.processed, 3);
    assert_eq!(counts.success, 2);
    assert_eq!(counts.failed, 1);
}
