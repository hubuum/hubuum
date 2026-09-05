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
