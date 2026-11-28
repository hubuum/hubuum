use super::*;

#[actix_web::test]
async fn postgres_rolls_back_a_compound_collection_create_at_an_injected_failure() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                prefix("collection_failpoint_group"),
                Some("collection rollback owner".to_string()),
            ),
            &EventContext::system(),
        )
        .await
        .expect("collection rollback owner group should be created")
        .into_value();
    let collection_name = prefix("collection_failpoint");
    let command = StorageCollectionCreate::new(
        collection_name.clone(),
        "must be rolled back",
        group.id(),
        Some(collection_id(
            CollectionID::new(1).expect("root id should be valid").id(),
        )),
    );

    let result = PostgresFaultController::failing(PostgresFaultPoint::CollectionCreateAfterRecords)
        .run(
            backend
                .collection_store()
                .create_collection(command, &EventContext::system()),
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("injected failure should abort collection creation"),
    };
    assert_eq!(error.kind(), StorageErrorKind::Backend);

    let persisted = hubuum_storage_postgres::test_support::count_collections_by_name(
        pool.get_ref(),
        &collection_name,
    )
    .await
    .expect("collection rollback should remain queryable");
    assert_eq!(persisted, 0, "all collection records must roll back");

    let _ = backend
        .delete_group(group.id(), &EventContext::system())
        .await
        .expect("collection rollback owner group should be removed")
        .into_value();
}

#[actix_web::test]
async fn postgres_rolls_back_task_finalization_at_an_injected_failure() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_failpoint_user"),
        "testpassword",
    )
    .await;
    let task = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                principal_id(user.id),
                serde_json::json!({"failpoint": true}),
                1,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_failpoint_key"))
                    .expect("failpoint idempotency key should be valid"),
            ))
            .request_hash(Some(prefix("task_failpoint_hash")))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .try_build()
            .expect("failpoint task request should be valid"),
        )
        .await
        .map(expect_legacy_task)
        .expect("task rollback fixture should be created");
    let claim = hubuum_storage_postgres::test_support::claim_task_by_id(pool.get_ref(), task.id())
        .await
        .expect("task rollback fixture should receive a live fenced claim");
    let lease = claim.lease().clone();

    let error = PostgresFaultController::failing(PostgresFaultPoint::TaskFinalizeAfterEvent)
        .run(
            backend.complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    lease,
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0)
                        .expect("non-negative task counts should be valid"),
                ),
                StorageTaskEventInput::new("succeeded", "Must be rolled back"),
                StorageTaskCompletionPayload::Import,
            )),
        )
        .await
        .expect_err("injected failure should abort task finalization");
    assert_eq!(error.kind(), StorageErrorKind::Backend);

    let (persisted, _) = backend
        .get_task_access(task.id())
        .await
        .expect("rolled-back task should remain readable")
        .into_parts();
    assert_eq!(persisted.status(), StorageTaskStatus::Validating);
    let events = backend
        .list_task_events(StorageTaskChildListQuery::new(
            task.id(),
            QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                .expect("contract query must be valid"),
        ))
        .await
        .expect("rolled-back task events should remain readable");
    assert_eq!(
        events.into_parts().0.len(),
        1,
        "terminal event must roll back"
    );

    hubuum_storage_postgres::test_support::delete_task(pool.get_ref(), task.id())
        .await
        .expect("task rollback fixture should be removed");
    user.delete_without_events(pool.get_ref())
        .await
        .expect("task rollback user should be removed");
}

#[actix_web::test]
async fn postgres_task_page_count_and_rows_share_one_snapshot() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_snapshot_user"),
        "testpassword",
    )
    .await;
    let first = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                principal_id(user.id),
                serde_json::json!({"snapshot": "first"}),
                0,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_snapshot_first"))
                    .expect("snapshot idempotency key should be valid"),
            ))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .try_build()
            .expect("initial snapshot task request should be valid"),
        )
        .await
        .map(expect_legacy_task)
        .expect("initial snapshot task should be created");

    let controller = PostgresFaultController::pausing(PostgresFaultPoint::PageAfterCount);
    let listing_controller = controller.clone();
    let listing_backend = backend.clone();
    let query = StorageTaskListQuery::new(
        Some(principal_id(user.id)),
        Some(StorageTaskKind::Import),
        Some(StorageTaskStatus::Queued),
        QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
            .expect("snapshot query should be valid"),
    );
    let listing = tokio::spawn(async move {
        listing_controller
            .run(listing_backend.list_tasks(query))
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        controller.wait_until_reached(),
    )
    .await
    .expect("task page should pause after its count");

    let second = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                principal_id(user.id),
                serde_json::json!({"snapshot": "second"}),
                0,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_snapshot_second"))
                    .expect("snapshot idempotency key should be valid"),
            ))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .try_build()
            .expect("concurrent snapshot task request should be valid"),
        )
        .await
        .map(expect_legacy_task)
        .expect("concurrent snapshot task should be created");
    controller.resume();

    let page = tokio::time::timeout(std::time::Duration::from_secs(5), listing)
        .await
        .expect("task page should finish after resuming")
        .expect("task page future should not panic")
        .expect("task page should succeed");
    let (tasks, total) = page.into_parts();
    assert_eq!(total, Some(1));
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id(), first.id());

    for task in [first, second] {
        hubuum_storage_postgres::test_support::delete_task(pool.get_ref(), task.id())
            .await
            .expect("snapshot task should be removed");
    }
    user.delete_without_events(pool.get_ref())
        .await
        .expect("snapshot user should be removed");
}

#[actix_web::test]
async fn postgres_delivery_faults_preserve_claim_acknowledgement_and_retry_recovery() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture = PostgresAuditContractFixture::new(pool.get_ref().clone())
        .await
        .expect("delivery fault fixture should initialize");
    let report = verify_delivery_fault_contract(&fixture)
        .await
        .expect("PostgreSQL must satisfy the portable delivery fault contract");
    assert_eq!(report.checks(), 8);

    fixture
        .cleanup()
        .await
        .expect("delivery fault fixture should clean up");
}

#[actix_web::test]
async fn postgres_restore_faults_roll_back_coordination_transitions() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture = PostgresRestoreCoordinationFaultFixture {
        pool: pool.get_ref().clone(),
    };
    let report = verify_restore_coordination_fault_contract(&fixture)
        .await
        .expect("PostgreSQL must satisfy the portable restore coordination fault contract");
    assert_eq!(report.checks(), 5);
}

#[actix_web::test]
async fn postgres_lease_renewal_loss_expires_and_finalizes_without_replay() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture = PostgresLeaseLossFaultFixture {
        pool: pool.get_ref().clone(),
    };
    let report = verify_lease_loss_fault_contract(&fixture)
        .await
        .expect("PostgreSQL must satisfy the portable lease-loss fault contract");
    assert_eq!(report.checks(), 5);
}
