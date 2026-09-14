use super::*;

#[rstest::rstest]
#[case::single_batch(3, 3)]
#[case::split_batches(3, 2)]
#[case::single_object_batches(3, 1)]
#[tokio::test]
async fn read_repair_bounds_transactions_and_loads_definitions_once_per_batch(
    #[future(awt)] test_context: TestContext,
    #[case] object_count: usize,
    #[case] batch_size: usize,
) {
    use std::num::NonZeroUsize;

    use hubuum_domain::ObjectId;
    use hubuum_storage_core::{
        ComputedObjectStorage, ObjectStorage, StorageComputedObjectEnrichmentQuery,
    };
    use hubuum_storage_postgres::PostgresStorage;

    let mut fixture = fixture(&test_context, "computed repair batch budget").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    for index in 1..object_count {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(&format!("batch repair object {index}")),
                description: "Batch repair query budget".to_string(),
                data: fixture.objects[0].data.clone(),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }
    let storage = PostgresStorage::unobserved(test_context.pool.get_ref().clone())
        .with_computed_reindex_batch_size(NonZeroUsize::new(batch_size).unwrap());
    let mut objects = Vec::new();
    for object in &fixture.objects {
        remove_computed_cache(&test_context, object.id).await;
        objects.push(
            storage
                .get_object(ObjectId::new(object.id).unwrap())
                .await
                .unwrap()
                .object()
                .clone(),
        );
    }
    let (result, queries) = capture_queries(
        storage
            .enrich_objects_with_computed(StorageComputedObjectEnrichmentQuery::new(objects, None)),
    )
    .await;
    result.unwrap();
    let batches = object_count.div_ceil(batch_size);
    // One read snapshot plus one transaction per bounded repair batch. Only
    // materialization upserts grow with the number of objects inside a batch.
    assert_eq!(queries.connection_checkouts(), 1 + batches);
    assert_eq!(
        queries.queries_matching("FROM \"computed_field_definitions\""),
        1 + batches
    );
    assert_eq!(queries.queries_matching("FOR UPDATE"), batches);
    assert_eq!(queries.queries_matching("FOR KEY SHARE"), batches);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"object_computed_data\""),
        object_count
    );
    let persisted = with_connection(&test_context.pool, async |connection| {
        use crate::schema::object_computed_data::dsl::{class_id, object_computed_data};
        object_computed_data
            .filter(class_id.eq(fixture.class.id))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .unwrap();
    fixture.cleanup().await.unwrap();
    assert_eq!(persisted, object_count as i64);
}

#[rstest::rstest]
#[case::commit(true)]
#[case::rollback(false)]
#[tokio::test]
async fn computed_batch_materialization_obeys_its_transaction_outcome(
    #[future(awt)] test_context: TestContext,
    #[case] commit: bool,
) {
    use hubuum_storage_postgres::PostgresStorageError;
    use hubuum_storage_postgres::test_support::computed_lock_protocol::{
        PostgresRuntime, with_computed_transaction,
    };

    let fixture = fixture(&test_context, "computed batch transaction outcome").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    remove_computed_cache(&test_context, fixture.objects[0].id).await;
    let runtime = PostgresRuntime::unobserved(test_context.pool.get_ref().clone());
    let result = with_computed_transaction(&runtime, async |transaction| {
        transaction
            .lock_class(fixture.class.id)
            .await?
            .lock_objects(&[fixture.objects[0].id])
            .await?
            .repair()
            .await?;
        if commit {
            Ok(())
        } else {
            Err(PostgresStorageError::internal("abort repaired batch"))
        }
    })
    .await;
    assert_eq!(result.is_ok(), commit);
    let persisted = with_connection(&test_context.pool, async |connection| {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
        object_computed_data
            .filter(object_id.eq(fixture.objects[0].id))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .unwrap();
    fixture.cleanup().await.unwrap();
    assert_eq!(persisted, i64::from(commit));
}

#[rstest::rstest]
#[tokio::test]
async fn computed_class_capability_cannot_materialize_another_class_objects(
    #[future(awt)] test_context: TestContext,
) {
    use hubuum_storage_postgres::test_support::computed_lock_protocol::{
        PostgresRuntime, with_computed_transaction,
    };

    let first = fixture(&test_context, "computed capability first class").await;
    let second = fixture(&test_context, "computed capability second class").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", second.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, second.class.id).await;
    let runtime = PostgresRuntime::unobserved(test_context.pool.get_ref().clone());
    with_computed_transaction(&runtime, async |transaction| {
        transaction
            .lock_class(first.class.id)
            .await?
            .lock_objects(&[second.objects[0].id])
            .await?
            .repair()
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    let cached = persisted_computed_value(&test_context, second.objects[0].id).await;
    first.cleanup().await.unwrap();
    second.cleanup().await.unwrap();
    assert_eq!(cached["display_name"], "inventory.example");
}

async fn wait_for_computed_lock_wait(context: &TestContext, blocker_pid: i32) {
    use diesel::sql_types::{Bool, Integer};
    use tokio::time::{Duration, sleep, timeout};

    #[derive(QueryableByName)]
    struct BlockedTransaction {
        #[diesel(sql_type = Bool)]
        blocked: bool,
    }

    timeout(Duration::from_secs(10), async {
        loop {
            let blocked = with_connection(&context.pool, async |connection| {
                diesel::sql_query(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                     WHERE $1 = ANY(pg_blocking_pids(pid))) AS blocked",
                )
                .bind::<Integer, _>(blocker_pid)
                .get_result::<BlockedTransaction>(connection)
                .await
            })
            .await
            .unwrap();
            if blocked.blocked {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("computed repair should reach the held lock");
}

async fn remove_computed_cache(context: &TestContext, target_object: i32) {
    with_connection(&context.pool, async |connection| {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
        diesel::delete(object_computed_data.filter(object_id.eq(target_object)))
            .execute(connection)
            .await
    })
    .await
    .unwrap();
}

async fn persisted_computed_value(context: &TestContext, target_object: i32) -> serde_json::Value {
    with_connection(&context.pool, async |connection| {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id, values};
        object_computed_data
            .filter(object_id.eq(target_object))
            .select(values)
            .first::<serde_json::Value>(connection)
            .await
    })
    .await
    .unwrap()
}

#[rstest::rstest]
#[case::class_row(false)]
#[case::definition_advisory(true)]
#[tokio::test]
async fn read_repair_locks_the_class_before_objects_and_reloads_current_data(
    #[future(awt)] test_context: TestContext,
    #[case] definition_lock: bool,
) {
    use diesel::sql_types::Integer;
    use hubuum_storage_postgres::test_support::computed_lock_protocol::acquire_computed_class_exclusive_lock;
    use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint, with_transaction};
    use tokio::time::{Duration, timeout};

    use crate::schema::{hubuumclass, hubuumobject};

    let fixture = fixture(&test_context, "computed repair lock order").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    remove_computed_cache(&test_context, fixture.objects[0].id).await;

    // Pause after the read-only snapshot so a definition blocker exercises
    // repair's acquisition order, rather than merely blocking the snapshot.
    let pause = PostgresFaultController::pausing(PostgresFaultPoint::ComputedRepairBeforeLocks);
    let endpoint = format!(
        "/api/v1/classes/{}/{}?include=computed",
        fixture.class.id, fixture.objects[0].id
    );
    let repair = pause.run(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
    ));
    let writer = async {
        pause.wait_until_reached().await;
        with_transaction(&test_context.pool, async |connection| {
            if definition_lock {
                acquire_computed_class_exclusive_lock(connection, fixture.class.id).await?;
            } else {
                hubuumclass::table
                    .filter(hubuumclass::id.eq(fixture.class.id))
                    .for_update()
                    .select(hubuumclass::id)
                    .first::<i32>(connection)
                    .await?;
            }
            let blocker_pid = diesel::select(diesel::dsl::sql::<Integer>("pg_backend_pid()"))
                .get_result::<i32>(connection)
                .await?;
            pause.resume();
            wait_for_computed_lock_wait(&test_context, blocker_pid).await;
            hubuumobject::table
                .filter(hubuumobject::id.eq(fixture.objects[0].id))
                .for_update()
                .no_wait()
                .select(hubuumobject::id)
                .first::<i32>(connection)
                .await?;
            // Simulate a writer outside the materialization protocol. Repair
            // must use the committed source, not the earlier response snapshot.
            diesel::update(hubuumobject::table.find(fixture.objects[0].id))
                .set(hubuumobject::data.eq(serde_json::json!({
                    "manual": {"hostname": "after-repair-wait.example"}
                })))
                .execute(connection)
                .await?;
            Ok::<_, hubuum_storage_postgres::PostgresStorageError>(())
        })
        .await
    };
    let (response, writer_result) = timeout(Duration::from_secs(20), async {
        tokio::join!(repair, writer)
    })
    .await
    .expect("repair and writer should finish");
    writer_result.expect("repair must not hold objects while waiting for class capabilities");
    assert_response_status(response, StatusCode::OK).await;
    // Inspect persistence directly: a second enriched GET could hide a failed
    // first repair by repairing again.
    let cached = persisted_computed_value(&test_context, fixture.objects[0].id).await;
    fixture.cleanup().await.unwrap();
    assert_eq!(cached["display_name"], "after-repair-wait.example");
}

#[rstest::rstest]
#[tokio::test]
async fn read_repair_reloads_definitions_changed_after_its_snapshot(
    #[future(awt)] test_context: TestContext,
) {
    use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint};
    use tokio::time::{Duration, timeout};

    let fixture = fixture(&test_context, "computed repair definition change").await;
    let definitions_endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &definitions_endpoint,
        definition("display_name"),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let created: serde_json::Value = test::read_body_json(response).await;
    let definition_id = created["definition"]["id"].as_i64().unwrap();
    finish_active_rebuild(&test_context, fixture.class.id).await;
    remove_computed_cache(&test_context, fixture.objects[0].id).await;

    let pause = PostgresFaultController::pausing(PostgresFaultPoint::ComputedRepairBeforeLocks);
    let endpoint = format!(
        "/api/v1/classes/{}/{}?include=computed",
        fixture.class.id, fixture.objects[0].id
    );
    let repair = pause.run(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
    ));
    let definition_change = async {
        pause.wait_until_reached().await;
        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("{definitions_endpoint}/{definition_id}"),
            serde_json::json!({
                "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]}
            }),
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;
        pause.resume();
    };
    let (response, ()) = timeout(Duration::from_secs(20), async {
        tokio::join!(repair, definition_change)
    })
    .await
    .expect("definition change and repair should finish");
    assert_response_status(response, StatusCode::OK).await;
    let cached = persisted_computed_value(&test_context, fixture.objects[0].id).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
    assert_eq!(cached["display_name"], "manual.example");
}
