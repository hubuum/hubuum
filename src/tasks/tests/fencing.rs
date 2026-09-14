use super::*;
use crate::storage::TaskExecutionStorage;
use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint};

fn collection_item(name: &str) -> PlannedItem {
    PlannedItem {
        result: planned_result(
            "collection",
            "create",
            Some("collection:fenced".into()),
            Some(name.into()),
        ),
        execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
            ref_: Some("collection:fenced".into()),
            name: name.into(),
            description: "fenced import".into(),
            condition: None,
            timestamps: None,
            parent_collection_ref: None,
            parent_collection_key: None,
        })),
    }
}
async fn expire(context: &TestContext, task_id: i32) {
    with_connection(&context.pool, async |connection| {
        diesel::sql_query("UPDATE tasks SET lease_expires_at = clock_timestamp() - interval '1 minute' WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(task_id).execute(connection).await
    }).await.unwrap();
}
async fn exists(context: &TestContext, name: &str) -> bool {
    with_connection(&context.pool, async |connection| {
        collections
            .filter(collection_name.eq(name))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .unwrap()
        > 0
}

#[tokio::test]
async fn best_effort_import_does_not_publish_references_from_failed_commits() {
    let context = TestContext::new().await;
    let name = context.scoped_name("uncommitted_import_reference");
    let mut child = ImportCollectionInput {
        ref_: None,
        name: context.scoped_name("uncommitted_import_child"),
        description: "depends on a rolled-back parent".into(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: None,
    };
    let parent = child.clone();
    child.parent_collection_ref = Some("parent".into());
    let parent = ImportCollectionInput {
        ref_: Some("parent".into()),
        name: name.clone(),
        ..parent
    };
    let plan = StorageImportPlan::try_new(
        [parent, child]
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                StorageImportPlanItem::new(
                    index,
                    crate::services::import_boundary::import_operation_to_storage(
                        crate::storage::ApplicationImportOperation::CreateCollection(input),
                    )
                    .unwrap(),
                )
            })
            .collect(),
    )
    .unwrap();
    let storage = crate::storage::storage_handle(&context.pool);
    let mode = crate::services::import_boundary::import_mode_to_storage(ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    });
    let (outcomes, _) =
        PostgresFaultController::failing(PostgresFaultPoint::TransactionBeforeCommit)
            .run(storage.apply_import_best_effort(plan, mode))
            .await
            .unwrap()
            .into_parts();
    assert!(!exists(&context, &name).await, "the parent must roll back");
    let (_, error) = outcomes.into_iter().nth(1).unwrap().into_parts();
    assert!(
        error
            .unwrap()
            .to_string()
            .contains("Unknown collection ref 'parent'"),
        "the child must fail reference resolution before attempting a database write"
    );
}

#[tokio::test]
async fn expired_claim_rolls_back_import_domain_mutations() {
    let context = TestContext::new().await;
    let name = context.scoped_name("expired_import");
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "expired_import",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    expire(&context, task.id).await;
    let result = execute_import_strict(
        &context.pool,
        &claimed,
        &[collection_item(&name)],
        &mut ExecutionAccumulator::default(),
    )
    .await;
    assert!(result.is_err());
    assert!(!exists(&context, &name).await);
    hubuum_storage_postgres::test_support::delete_task(
        &context.pool,
        hubuum_domain::TaskId::new(task.id).unwrap(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_reconciles_import_committed_before_worker_loss() {
    let context = TestContext::new().await;
    let name = context.scoped_name("committed_import_receipt");
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "committed_import_receipt",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let result = PostgresFaultController::failing(PostgresFaultPoint::ImportAfterCommit)
        .run(execute_import_strict(
            &context.pool,
            &claimed,
            &[collection_item(&name)],
            &mut ExecutionAccumulator::default(),
        ))
        .await;
    assert!(result.is_err(), "worker should stop after commit");
    assert!(
        exists(&context, &name).await,
        "the receipt must describe committed effects"
    );
    expire(&context, task.id).await;
    let storage = crate::storage::storage_handle(&context.pool);
    storage.recover_expired_task_leases(1000).await.unwrap();
    let recovered = find_task(&context.pool, crate::models::TaskID::new(task.id).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.status, TaskStatus::Succeeded.as_str());
    assert_eq!(recovered.success_items, 1);
    with_connection(&context.pool, async |connection| {
        diesel::delete(collections.filter(collection_name.eq(&name)))
            .execute(connection)
            .await
    })
    .await
    .unwrap();
    hubuum_storage_postgres::test_support::delete_task(
        &context.pool,
        hubuum_domain::TaskId::new(task.id).unwrap(),
    )
    .await
    .unwrap();
}

#[rstest]
#[case::domain_effects(true)]
#[case::planning_results(false)]
#[tokio::test]
async fn lease_expiring_after_the_final_application_check_rejects_the_commit(
    #[case] domain_effects: bool,
) {
    let context = TestContext::new().await;
    let name = context.scoped_name("deferred_import_fence");
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "deferred_import_fence",
    )
    .await;
    let claim = hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
        &context.pool,
        hubuum_domain::TaskId::new(task.id).unwrap(),
        crate::storage::StorageTaskLeaseDuration::from_milliseconds(2000).unwrap(),
    )
    .await
    .unwrap();
    let claimed = ClaimedTask::from_storage(claim).unwrap();
    let controller = PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
    let mut accumulator = ExecutionAccumulator::default();
    let items = [collection_item(&name)];
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            controller.run(async {
                if domain_effects {
                    execute_import_strict(&context.pool, &claimed, &items, &mut accumulator).await
                } else {
                    let results = crate::storage::FencedImportResults::try_new(
                        claimed.lease().clone(),
                        vec![
                            crate::storage::StorageImportResult::builder(
                                claimed.lease().task_id(),
                                "collection",
                                "create",
                                "failed",
                            )
                            .error(Some("planning failure".into()))
                            .build(),
                        ],
                    )?;
                    crate::storage::storage_handle(&context.pool)
                        .record_claimed_import_results(results)
                        .await
                        .map_err(ApiError::from)
                }
            }),
            async {
                controller.wait_until_reached().await;
                tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
                controller.resume();
            }
        )
    })
    .await
    .expect("the import must reach its commit fence");
    assert!(
        result.is_err(),
        "the deferred database fence must reject the expired claim"
    );
    assert!(!exists(&context, &name).await);
    let receipt_count = with_connection(&context.pool, async |connection| {
        crate::schema::import_task_results::table
            .filter(crate::schema::import_task_results::task_id.eq(task.id))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(
        receipt_count, 0,
        "an expired commit cannot leave result rows"
    );
    hubuum_storage_postgres::test_support::delete_task(
        &context.pool,
        hubuum_domain::TaskId::new(task.id).unwrap(),
    )
    .await
    .unwrap();
}

#[rstest]
#[case::before_commit(PostgresFaultPoint::ImportBeforeCommitFence, false)]
#[case::after_commit(PostgresFaultPoint::ImportAfterCommit, true)]
#[tokio::test]
async fn cancellation_arbitrates_against_strict_import_commit(
    #[case] point: PostgresFaultPoint,
    #[case] committed: bool,
) {
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::{StorageTaskCancellationRequest, StorageTaskStatus};
    let context = TestContext::new().await;
    let name = context.scoped_name("cancel_commit_fence");
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "cancel_commit_fence",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let backend = crate::storage::storage_handle(&context.pool);
    let (stored, _) = backend
        .get_task_access(claimed.lease().task_id())
        .await
        .unwrap()
        .into_parts();
    let controller = PostgresFaultController::pausing(point);
    let mut accumulator = ExecutionAccumulator::default();
    let items = [collection_item(&name)];
    let (execution, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            controller.run(execute_import_strict(
                &context.pool,
                &claimed,
                &items,
                &mut accumulator
            )),
            async {
                controller.wait_until_reached().await;
                backend
                    .request_task_cancellation(StorageTaskCancellationRequest::new(
                        &stored,
                        EventContext::system(),
                    ))
                    .await
                    .unwrap();
                controller.resume();
            },
        )
    })
    .await
    .expect("cancellation must not deadlock on import receipt foreign keys");
    if committed {
        assert!(execution.is_ok());
    } else {
        assert!(matches!(execution, Err(ApiError::TaskStopped(_))));
    }
    assert_eq!(exists(&context, &name).await, committed);
    let finished = backend
        .acknowledge_task_stop(claimed.lease().clone())
        .await
        .unwrap();
    assert_eq!(
        finished.status(),
        if committed {
            StorageTaskStatus::Succeeded
        } else {
            StorageTaskStatus::Cancelled
        }
    );
    with_connection(&context.pool, async |connection| {
        diesel::delete(collections.filter(collection_name.eq(&name)))
            .execute(connection)
            .await
    })
    .await
    .unwrap();
    hubuum_storage_postgres::test_support::delete_task(&context.pool, stored.id())
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_stop_cancels_inflight_query_and_drains_before_connection_reuse() {
    use crate::storage::{StorageExecutionScope, with_storage_execution_scope};
    use diesel::sql_types::{Bool, Text};
    use hubuum_task_core::{TaskExecutionContext, TaskStopReason};
    use std::time::Duration;
    let context = TestContext::new().await;
    let sql = format!(
        "SELECT pg_sleep(30) /* {} */",
        context.scoped_name("cancel_inflight_query")
    );
    let (execution, stop) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            with_storage_execution_scope(
                &context.pool,
                StorageExecutionScope::default().with_task_execution(Some(execution)),
                with_connection(&context.pool, async |connection| {
                    diesel::sql_query(&sql).execute(connection).await
                })
            ),
            async {
                loop {
                    let running = with_connection(&context.pool, async |connection| {
                        diesel::select(
                            diesel::dsl::sql::<Bool>(
                                "EXISTS (SELECT 1 FROM pg_stat_activity WHERE query = ",
                            )
                            .bind::<Text, _>(&sql)
                            .sql(" AND state = 'active' AND wait_event = 'PgSleep')"),
                        )
                        .get_result::<bool>(connection)
                        .await
                    })
                    .await
                    .unwrap();
                    if running {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                stop.request_stop(TaskStopReason::Cancelled);
            }
        )
    })
    .await
    .expect("SQL cancellation must stop actual work well before pg_sleep completes");
    let error = result.unwrap_err();
    assert_eq!(
        error.kind(),
        hubuum_storage_core::StorageErrorKind::TaskCancelled
    );
    let healthy = with_connection(&context.pool, async |connection| {
        diesel::select(diesel::dsl::sql::<Bool>("true"))
            .get_result::<bool>(connection)
            .await
    })
    .await
    .unwrap();
    assert!(healthy, "drained connections must remain usable");
}

#[tokio::test]
async fn maximum_task_duration_fits_postgres_statement_timeout() {
    use crate::storage::{StorageExecutionScope, with_storage_execution_scope};
    use diesel::sql_types::Text;
    use hubuum_task_core::{TaskExecutionContext, TaskExecutionLimit};
    let context = TestContext::new().await;
    let limit =
        TaskExecutionLimit::from_milliseconds(TaskExecutionLimit::MAX_MILLISECONDS).unwrap();
    let (execution, _) = TaskExecutionContext::new(limit.duration()).unwrap();
    let milliseconds = with_storage_execution_scope(
        &context.pool,
        StorageExecutionScope::default().with_task_execution(Some(execution)),
        with_connection(&context.pool, async |connection| {
            diesel::select(diesel::dsl::sql::<Text>(
                "(SELECT setting FROM pg_settings WHERE name='statement_timeout')",
            ))
            .get_result::<String>(connection)
            .await
        }),
    )
    .await
    .unwrap();
    assert_eq!(milliseconds.parse::<i32>().unwrap(), i32::MAX);
}

async fn resume_after_database_waiter(
    context: &TestContext,
    controller: &PostgresFaultController,
    blocker: i32,
) {
    use diesel::sql_types::{Bool, Integer};
    loop {
        let blocked = with_connection(&context.pool, async |connection| {
            diesel::select(
                diesel::dsl::sql::<Bool>("EXISTS (SELECT 1 FROM pg_stat_activity WHERE ")
                    .bind::<Integer, _>(blocker)
                    .sql(" = ANY(pg_blocking_pids(pid)))"),
            )
            .get_result::<bool>(connection)
            .await
        })
        .await
        .unwrap();
        if blocked {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    controller.resume();
}

#[tokio::test]
async fn queued_cancellation_lock_prevents_a_concurrent_claim() {
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::{StorageTaskCancellationRequest, StorageTaskStatus};
    let context = TestContext::new().await;
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "cancel_claim_race",
    )
    .await;
    let backend = crate::storage::storage_handle(&context.pool);
    let task_id = hubuum_domain::TaskId::new(task.id).unwrap();
    let (stored, _) = backend.get_task_access(task_id).await.unwrap().into_parts();
    let controller = PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
    let (cancel, (claim, ())) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            controller.run(
                backend.request_task_cancellation(StorageTaskCancellationRequest::new(
                    &stored,
                    EventContext::system()
                ))
            ),
            async {
                let blocker = controller.wait_until_reached().await.backend_pid().unwrap();
                tokio::join!(
                    hubuum_storage_postgres::test_support::claim_task_by_id(&context.pool, task_id),
                    resume_after_database_waiter(&context, &controller, blocker),
                )
            },
        )
    })
    .await
    .expect("claim and cancellation must serialize without deadlock");
    assert_eq!(
        cancel.unwrap().task().status(),
        StorageTaskStatus::Cancelled
    );
    assert!(
        claim.is_err(),
        "claim predicate must be rechecked after the cancellation lock releases"
    );
    hubuum_storage_postgres::test_support::delete_task(&context.pool, task_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn success_finalization_lock_wins_a_concurrent_cancel_request() {
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::{
        StorageTaskCancellationChange, StorageTaskCancellationRequest, StorageTaskCompletion,
        StorageTaskCompletionPayload, StorageTaskEventInput, StorageTaskResultCounts,
        StorageTaskStatus, StorageTaskTerminalStatus, StorageTaskTerminalUpdate,
    };
    let context = TestContext::new().await;
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "cancel_success_race",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let backend = crate::storage::storage_handle(&context.pool);
    let (stored, _) = backend
        .get_task_access(claimed.lease().task_id())
        .await
        .unwrap()
        .into_parts();
    let controller = PostgresFaultController::pausing(PostgresFaultPoint::TaskFinalizeAfterEvent);
    let (success, (cancel, ())) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            controller.run(backend.complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    claimed.lease().clone(),
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap()
                ),
                StorageTaskEventInput::new("succeeded", "success won"),
                StorageTaskCompletionPayload::Import,
            ))),
            async {
                let blocker = controller.wait_until_reached().await.backend_pid().unwrap();
                tokio::join!(
                    backend.request_task_cancellation(StorageTaskCancellationRequest::new(
                        &stored,
                        EventContext::system(),
                    )),
                    resume_after_database_waiter(&context, &controller, blocker),
                )
            },
        )
    })
    .await
    .expect("success and cancellation must serialize without deadlock");
    assert_eq!(success.unwrap().status(), StorageTaskStatus::Succeeded);
    assert_eq!(
        cancel.unwrap().change(),
        StorageTaskCancellationChange::Unchanged
    );
    hubuum_storage_postgres::test_support::delete_task(&context.pool, stored.id())
        .await
        .unwrap();
}

#[rstest]
#[case::durable_request(false)]
#[case::execution_deadline(true)]
#[tokio::test]
async fn control_monitor_stops_executor_from_durable_backend_state(#[case] deadline: bool) {
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::StorageTaskCancellationRequest;
    use hubuum_task_core::{TaskExecutionLimit, TaskStopReason};
    use std::time::Duration;
    let context = TestContext::new().await;
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::json!({}),
        1,
        "monitor_stop",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let backend = crate::storage::storage_handle(&context.pool);
    let app = crate::permissions::AppContext::new(
        backend.clone(),
        Arc::new(crate::permissions::LocalPermissionBackend::new(
            backend.clone(),
            "admin".to_string(),
        )),
    );
    let (stored, _) = backend
        .get_task_access(claimed.lease().task_id())
        .await
        .unwrap()
        .into_parts();
    let started = tokio::sync::Notify::new();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            super::super::control::execute(
                &app,
                &claimed,
                TaskExecutionLimit::from_milliseconds(if deadline { 1000 } else { 60_000 })
                    .unwrap(),
                async {
                    started.notify_one();
                    loop {
                        if let Err(error) = super::super::control::checkpoint() {
                            break Err::<(), ApiError>(error);
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            ),
            async {
                started.notified().await;
                if !deadline {
                    backend
                        .request_task_cancellation(StorageTaskCancellationRequest::new(
                            &stored,
                            EventContext::system(),
                        ))
                        .await
                        .unwrap();
                }
            },
        )
    })
    .await
    .expect("control monitor must stop the executor");
    assert!(
        matches!(result, Err(ApiError::TaskStopped(reason)) if reason == if deadline { TaskStopReason::DeadlineExceeded } else { TaskStopReason::Cancelled })
    );
    backend
        .acknowledge_task_stop(claimed.lease().clone())
        .await
        .unwrap();
    hubuum_storage_postgres::test_support::delete_task(&context.pool, stored.id())
        .await
        .unwrap();
}
