use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_task_queue() {
    let _permit = postgres_permit().await;
    let options = || {
        QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
            .expect("contract query must be valid")
    };

    for environment in available_backend_environments() {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("task_queue_user")).await;
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    user.principal_id,
                    serde_json::json!({"items": []}),
                    0,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("task_queue_key"))
                        .expect("compatibility idempotency key should be valid"),
                ))
                .request_hash(Some(prefix("task_queue_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(10)
                .expect("task queue request should be valid"),
            )
            .await
            .expect("certified backend should create a task");
        let task_id = task.id();
        assert_eq!(task.kind(), StorageTaskKind::Import);
        assert_eq!(task.status(), StorageTaskStatus::Queued);

        let access = backend
            .get_task_access(task_id)
            .await
            .expect("certified backend should return task access facts");
        assert_eq!(access.into_parts().0.id(), task_id);

        let (tasks, total) = backend
            .list_tasks(StorageTaskListQuery::new(
                Some(user.principal_id),
                Some(StorageTaskKind::Import),
                Some(StorageTaskStatus::Queued),
                options(),
            ))
            .await
            .expect("certified backend should list tasks")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(tasks.len(), 1);

        let (events, event_total) = backend
            .list_task_events(StorageTaskChildListQuery::new(task_id, options()))
            .await
            .expect("certified backend should list task events")
            .into_parts();
        assert_eq!(event_total, Some(1));
        assert_eq!(events.len(), 1);

        let (results, result_total) = backend
            .list_import_task_results(StorageTaskChildListQuery::new(task_id, options()))
            .await
            .expect("certified backend should list import results")
            .into_parts();
        assert_eq!(result_total, Some(0));
        assert!(results.is_empty());

        backend
            .record_import_results(vec![
                StorageImportResult::builder(task_id, "compatibility", "verify", "succeeded")
                    .item_ref(Some("compatibility:item".to_string()))
                    .build(),
            ])
            .await
            .expect("certified backend should persist import results");
        let (results, result_total) = backend
            .list_import_task_results(StorageTaskChildListQuery::new(task_id, options()))
            .await
            .expect("certified backend should return persisted import results")
            .into_parts();
        assert_eq!(result_total, Some(1));
        assert_eq!(results.len(), 1);

        assert!(
            backend
                .list_export_output_summaries(vec![task_id])
                .await
                .expect("certified backend should list export output summaries")
                .is_empty()
        );
        assert!(
            backend
                .list_backup_output_summaries(vec![task_id])
                .await
                .expect("certified backend should list backup output summaries")
                .is_empty()
        );
        assert!(matches!(
            backend.get_export_output_summary(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));
        assert!(matches!(
            backend.get_backup_output_summary(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));
        assert!(matches!(
            backend.get_export_output(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));
        assert!(matches!(
            backend.get_backup_output(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));

        if let BackendTestEnvironment::Postgres { pool } = &environment {
            hubuum_storage_postgres::test_support::delete_task(pool, task_id)
                .await
                .expect("task queue compatibility fixture should be removed");
        }
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_task_state_machine() {
    let _permit = postgres_permit().await;

    for environment in available_backend_environments() {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("task_execution_user")).await;
        let mut fixture_ids = Vec::new();
        for task_kind in StorageTaskKind::ALL {
            let task = backend
                .create_task(
                    StorageTaskCreateRequest::builder(
                        task_kind,
                        user.principal_id,
                        serde_json::json!({"compatibility": true}),
                        1,
                    )
                    .idempotency_key(Some(
                        IdempotencyKey::new(prefix(&format!(
                            "task_execution_{}",
                            task_kind.as_str()
                        )))
                        .expect("compatibility idempotency key should be valid"),
                    ))
                    .request_hash(Some(prefix(&format!(
                        "task_execution_hash_{}",
                        task_kind.as_str()
                    ))))
                    .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                    .try_build(10)
                    .expect("task execution request should be valid"),
                )
                .await
                .expect("certified backend should create an executable task");
            fixture_ids.push(task.id());
        }
        if let BackendTestEnvironment::Postgres { pool } = &environment {
            for task_id in &fixture_ids {
                hubuum_storage_postgres::test_support::prioritize_task(pool, *task_id)
                    .await
                    .expect("compatibility tasks should be made claim-first");
            }
        }

        let lease_duration = StorageTaskLeaseDuration::from_milliseconds(60_000)
            .expect("compatibility lease duration should be valid");
        assert!(
            backend
                .recover_expired_task_leases(0)
                .await
                .expect("certified backend should recover expired claims")
                .is_empty()
        );

        let mut completed_ids = HashSet::new();
        let mut completed_kinds = HashSet::new();
        for completed_index in 0..StorageTaskKind::ALL.len() {
            let claimed = backend
                .claim_next_task(lease_duration)
                .await
                .expect("certified backend should claim the next task")
                .expect("a compatibility task should be claimable");
            assert!(fixture_ids.contains(&claimed.task().id()));
            assert!(completed_ids.insert(claimed.task().id()));
            assert!(completed_kinds.insert(claimed.task().kind()));
            if completed_index == 0 {
                assert!(
                    backend
                        .renew_task_lease(claimed.lease().clone(), lease_duration)
                        .await
                        .expect("certified backend should renew a live claim")
                );
                backend
                    .append_task_event(StorageTaskEventAppend::new(
                        claimed.lease().clone(),
                        StorageTaskEventInput::new("running", "Compatibility event"),
                    ))
                    .await
                    .expect("certified backend should append a claim-owned event");
            }
            backend
                .update_task_state(StorageTaskActiveUpdate::new(
                    claimed.lease().clone(),
                    StorageTaskActiveStatus::Running,
                    StorageTaskResultCounts::try_new(0, 0, 0)
                        .expect("non-negative task counts should be valid"),
                ))
                .await
                .expect("certified backend should update claimed task state");
            let payload = compatibility_completion_payload(claimed.task().kind());
            backend
                .complete_task(StorageTaskCompletion::new(
                    StorageTaskTerminalUpdate::new(
                        claimed.lease().clone(),
                        StorageTaskTerminalStatus::Succeeded,
                        StorageTaskResultCounts::try_new(1, 1, 0)
                            .expect("non-negative task counts should be valid"),
                    ),
                    StorageTaskEventInput::new("succeeded", "Compatibility completed"),
                    payload,
                ))
                .await
                .expect("certified backend should complete a claimed task");
            match claimed.task().kind() {
                StorageTaskKind::SchemaProfiling => assert!(matches!(
                    backend.get_schema_profile_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Export => assert!(matches!(
                    backend.get_export_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Backup => assert!(matches!(
                    backend.get_backup_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Import
                | StorageTaskKind::Reindex
                | StorageTaskKind::RemoteCall
                | StorageTaskKind::SchemaValidation => {}
            }
        }
        assert_eq!(completed_kinds.len(), StorageTaskKind::ALL.len());

        let mismatched_kind_task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Export,
                    user.principal_id,
                    serde_json::json!({"compatibility_kind_mismatch": true}),
                    1,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("task_execution_kind_mismatch"))
                        .expect("compatibility idempotency key should be valid"),
                ))
                .request_hash(Some(prefix("task_execution_kind_mismatch_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(10)
                .expect("task kind mismatch request should be valid"),
            )
            .await
            .expect("certified backend should create a kind mismatch fixture");
        fixture_ids.push(mismatched_kind_task.id());
        // This subcase verifies completion validation, not queue scheduling. Claim the
        // exact fixture so a concurrent task from another contract cannot be consumed.
        let mismatched_kind_claim = match &environment {
            BackendTestEnvironment::Postgres { pool } => {
                hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
                    pool,
                    mismatched_kind_task.id(),
                    lease_duration,
                )
                .await
                .expect("certified backend should claim the kind mismatch fixture")
            }
            BackendTestEnvironment::Memory { .. } => backend
                .claim_next_task(lease_duration)
                .await
                .expect("memory backend should claim the kind mismatch fixture")
                .expect("the kind mismatch fixture should be queued"),
        };
        let mismatched_kind_error = backend
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    mismatched_kind_claim.lease().clone(),
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0)
                        .expect("non-negative task counts should be valid"),
                ),
                StorageTaskEventInput::new("succeeded", "Mismatched kind must fail"),
                StorageTaskCompletionPayload::Import,
            ))
            .await
            .expect_err("a completion kind that differs from storage must be rejected");
        assert_eq!(mismatched_kind_error.kind(), StorageErrorKind::InvalidInput);
        backend
            .fail_task(StorageTaskFailure::new(
                mismatched_kind_claim.lease().clone(),
                "Compatibility cleanup",
                StorageTaskEventInput::new("failed", "Compatibility cleanup"),
            ))
            .await
            .expect("the rejected completion claim should remain valid");

        let mut failure_fixture_ids = Vec::new();
        for task_kind in StorageTaskKind::ALL {
            let task = backend
                .create_task(
                    StorageTaskCreateRequest::builder(
                        task_kind,
                        user.principal_id,
                        serde_json::json!({"compatibility_failure": true}),
                        1,
                    )
                    .idempotency_key(Some(
                        IdempotencyKey::new(prefix(&format!(
                            "task_execution_failure_{}",
                            task_kind.as_str()
                        )))
                        .expect("compatibility idempotency key should be valid"),
                    ))
                    .request_hash(Some(prefix(&format!(
                        "task_execution_failure_hash_{}",
                        task_kind.as_str()
                    ))))
                    .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                    .try_build(10)
                    .expect("task execution failure request should be valid"),
                )
                .await
                .expect("certified backend should create a failure fixture");
            failure_fixture_ids.push(task.id());
            fixture_ids.push(task.id());
        }
        if let BackendTestEnvironment::Postgres { pool } = &environment {
            for task_id in &failure_fixture_ids {
                hubuum_storage_postgres::test_support::prioritize_task(pool, *task_id)
                    .await
                    .expect("failure fixtures should be made claim-first");
            }
        }
        let failed = backend
            .claim_next_task(lease_duration)
            .await
            .expect("certified backend should claim a failure fixture")
            .expect("a compatibility failure fixture should be claimable");
        assert!(failure_fixture_ids.contains(&failed.task().id()));
        backend
            .fail_task(StorageTaskFailure::new(
                failed.lease().clone(),
                "Compatibility failure",
                StorageTaskEventInput::new("failed", "Compatibility failure"),
            ))
            .await
            .expect("certified backend should fail a claimed task");

        backend
            .purge_expired_export_outputs()
            .await
            .expect("certified backend should purge expired export outputs");
        backend
            .purge_expired_backup_outputs()
            .await
            .expect("certified backend should purge expired backup outputs");

        if let BackendTestEnvironment::Postgres { pool } = &environment {
            hubuum_storage_postgres::test_support::delete_tasks(pool, &fixture_ids)
                .await
                .expect("task execution compatibility fixtures should be removed");
        }
        delete_backend_user(&backend, user).await;
    }
}
