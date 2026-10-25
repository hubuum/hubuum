#![cfg(feature = "integration-test-support")]

pub use hubuum::*;
pub mod tests;

use crate::models::{HubuumClassID, NewHubuumClass, NewHubuumObject};
use crate::services::computed_fields::{class_computation_state_for, request_class_rebuild};
use crate::services::tasks::{ClaimedTask, execute_computed_field_rebuild};
use crate::tests::api_operations::{
    credential_post_request as post_request, get_request, patch_request,
};
use crate::tests::asserts::assert_response_status;
use crate::tests::{TestContext, test_context};
use crate::traits::{CanSave, SelfAccessors};
use actix_web::{App, http::StatusCode, test};
use hubuum_domain::{ClassId, CollectionId, PrincipalId};
use hubuum_storage_core::{ComputedFieldStorage, StorageTaskClaim};
use hubuum_storage_postgres::diesel_async_prelude::*;
use hubuum_storage_postgres::{capture_queries, with_connection};

// Queue-wide behavior uses a dedicated integration-test binary. Serializing
// these tests isolates the global durable queue without serializing API tests.
static REPAIR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn reset_read_repairs(context: &TestContext) {
    with_connection(&context.pool, async |connection| {
        diesel::sql_query(
            "DELETE FROM tasks WHERE request_payload->>'type' = 'computed_read_repairs'",
        )
        .execute(connection)
        .await
    })
    .await
    .unwrap();
}

async fn queued_repair(context: &TestContext, endpoint: &str) -> StorageTaskClaim {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let response = get_request(&context.pool, &context.admin_token, endpoint).await;
            assert_response_status(response, StatusCode::OK).await;
            let task_id = with_connection(&context.pool, async |connection| {
                use crate::schema::tasks::dsl as tasks;
                tasks::tasks
                    .filter(tasks::status.eq("queued"))
                    .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
                        "request_payload->>'type' = 'computed_read_repairs'",
                    ))
                    .select(tasks::id)
                    .first::<i32>(connection)
                    .await
                    .optional()
            })
            .await
            .unwrap();
            if let Some(id) = task_id {
                return hubuum_storage_postgres::test_support::claim_task_by_id(
                    context.pool.get_ref(),
                    hubuum_domain::TaskId::new(id).unwrap(),
                )
                .await
                .unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stale read should eventually enqueue repair")
}

async fn execute_read_repair(
    context: &TestContext,
    task: &StorageTaskClaim,
) -> Result<hubuum_storage_core::StorageTask, hubuum_storage_core::StorageError> {
    hubuum_storage_postgres::PostgresStorage::unobserved(context.pool.get_ref().clone())
        .execute_computed_field_rebuild(task.lease().clone())
        .await
}

fn definition(key: &str) -> serde_json::Value {
    serde_json::json!({
        "key": key,
        "label": "Display name",
        "description": "",
        "operation": {
            "type": "first_non_null",
            "paths": ["/inventory/hostname", "/manual/hostname"]
        },
        "result_type": "string",
        "enabled": true
    })
}

fn typed_class_id(id: i32) -> ClassId {
    ClassId::new(id).expect("test fixture class ids are positive")
}

fn typed_collection_id(id: i32) -> CollectionId {
    CollectionId::new(id).expect("test fixture collection ids are positive")
}

fn typed_principal_id(id: i32) -> PrincipalId {
    PrincipalId::new(id).expect("test fixture principal ids are positive")
}

async fn fixture(context: &TestContext, label: &str) -> crate::tests::ObjectFixture {
    context
        .object_fixture(
            label,
            NewHubuumClass {
                collection_id: 0,
                name: context.scoped_name(&format!("computed class {label}")),
                description: "Computed field test class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            vec![NewHubuumObject {
                collection_id: 0,
                hubuum_class_id: 0,
                name: context.scoped_name(&format!("computed object {label}")),
                description: "Computed field test object".to_string(),
                data: serde_json::json!({
                    "inventory": {"hostname": "inventory.example"},
                    "manual": {"hostname": "manual.example"}
                }),
            }],
        )
        .await
        .unwrap()
}

async fn active_rebuild_task(context: &TestContext, class_id: i32) -> ClaimedTask {
    for _ in 0..50 {
        let state = class_computation_state_for(&context.pool, typed_class_id(class_id))
            .await
            .unwrap();
        let task_id = match state.active_task_id {
            Some(task_id) => task_id,
            None => {
                let class = HubuumClassID::new(class_id)
                    .unwrap()
                    .instance(&context.pool)
                    .await
                    .unwrap();
                request_class_rebuild(
                    &context.pool,
                    typed_class_id(class_id),
                    typed_collection_id(class.collection_id),
                    Some(typed_principal_id(context.admin_user.id)),
                )
                .await
                .unwrap()
                .active_task_id
                .expect("manual rebuild task")
            }
        };
        if let Ok(task) =
            crate::test_support::claim_persisted_test_task(context.pool.get_ref(), task_id).await
        {
            return task;
        }
        tokio::task::yield_now().await;
    }
    panic!("could not load an active computed-field rebuild task");
}

async fn finish_active_rebuild(context: &TestContext, class_id: i32) {
    for _ in 0..50 {
        let state = class_computation_state_for(&context.pool, typed_class_id(class_id))
            .await
            .unwrap();
        if state.active_task_id.is_none() && state.rebuild_status == "ready" {
            return;
        }
        let task = active_rebuild_task(context, class_id).await;
        let _ = execute_computed_field_rebuild(&context.pool, &task).await;
        tokio::task::yield_now().await;
    }
    panic!("computed-field rebuild did not reach ready state");
}

#[rstest::rstest]
#[tokio::test]
async fn shared_definition_live_fallback_repairs_stale_materialization(
    #[future(awt)] test_context: TestContext,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let fixture = fixture(&test_context, "shared fallback").await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        definition("display_name"),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let mutation: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(mutation["state"]["rebuild_status"], "rebuilding");
    assert!(mutation["state"]["active_task_id"].is_number());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    with_connection(&test_context.pool, async |conn| {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};

        diesel::delete(object_computed_data.filter(object_id.eq(fixture.objects[0].id)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let object_endpoint = format!(
        "/api/v1/classes/{}/{}?include=computed",
        fixture.class.id, fixture.objects[0].id
    );
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let first: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        first["computed"]["shared"]["values"]["display_name"],
        "inventory.example"
    );
    assert_eq!(first["computed"]["shared"]["materialization_stale"], true);
    let task = queued_repair(&test_context, &object_endpoint).await;
    execute_read_repair(&test_context, &task).await.unwrap();

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let repaired: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        repaired["computed"]["shared"]["materialization_stale"],
        false
    );

    fixture.cleanup().await.unwrap();
}

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
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
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
    assert_eq!(
        queries.queries_matching("INSERT INTO \"object_computed_data\""),
        0
    );
    assert_eq!(
        queries.queries_matching("FROM \"computed_field_definitions\""),
        1
    );
    let endpoint = format!("/api/v1/classes/{}/?include=computed", fixture.class.id);
    let task = queued_repair(&test_context, &endpoint).await;
    let (result, queries) =
        capture_queries(storage.execute_computed_field_rebuild(task.lease().clone())).await;
    result.unwrap();
    let batches = object_count.div_ceil(batch_size);
    assert_eq!(
        queries.queries_matching("FROM \"computed_field_definitions\""),
        batches
    );
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
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
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
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
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
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
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
    let task = queued_repair(&test_context, &endpoint).await;
    let repair = pause.run(execute_read_repair(&test_context, &task));
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
    response.unwrap();
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
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
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
    let task = queued_repair(&test_context, &endpoint).await;
    let repair = pause.run(execute_read_repair(&test_context, &task));
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
    response.unwrap();
    let cached = persisted_computed_value(&test_context, fixture.objects[0].id).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
    assert_eq!(cached["display_name"], "manual.example");
}

async fn stale_fixture(
    context: &TestContext,
    label: &str,
) -> (crate::tests::ObjectFixture, String) {
    let fixture = fixture(context, label).await;
    let response = post_request(
        &context.pool,
        &context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(context, fixture.class.id).await;
    remove_computed_cache(context, fixture.objects[0].id).await;
    let endpoint = format!(
        "/api/v1/classes/{}/{}?include=computed",
        fixture.class.id, fixture.objects[0].id
    );
    (fixture, endpoint)
}

#[rstest::rstest]
#[tokio::test]
async fn stale_response_completes_while_background_repair_holds_its_transaction(
    #[future(awt)] test_context: TestContext,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (fixture, endpoint) = stale_fixture(&test_context, "response before repair commit").await;
    let task = queued_repair(&test_context, &endpoint).await;
    let pause = hubuum_storage_postgres::PostgresFaultController::pausing(
        hubuum_storage_postgres::PostgresFaultPoint::ComputedRepairBeforeCommit,
    );
    let repair = pause.run(execute_read_repair(&test_context, &task));
    let reader = async {
        pause.wait_until_reached().await;
        // Completion before resume proves the read does not await repair's locks
        // or commit. The generous timeout detects deadlock without timing claims.
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            get_request(&test_context.pool, &test_context.admin_token, &endpoint),
        )
        .await
        .unwrap();
        let response = assert_response_status(response, StatusCode::OK).await;
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            body["computed"]["shared"]["values"]["display_name"],
            "inventory.example"
        );
        assert_eq!(body["computed"]["shared"]["errors"], serde_json::json!({}));
        assert_eq!(body["computed"]["shared"]["materialization_stale"], true);
        pause.resume();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        tokio::join!(repair, reader)
    })
    .await
    .expect("paused repair and reader must finish");
    result.unwrap();
    let response = get_request(&test_context.pool, &test_context.admin_token, &endpoint).await;
    let body: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(body["computed"]["shared"]["materialization_stale"], false);
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn concurrent_duplicate_readers_keep_one_active_repair_identity(
    #[future(awt)] test_context: TestContext,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (fixture, endpoint) = stale_fixture(&test_context, "duplicate readers").await;
    let claim = queued_repair(&test_context, &endpoint).await;
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .app_data(crate::tests::app_context(&test_context.pool))
            .configure(crate::api::config),
    )
    .await;
    let reads = (0..8).map(|_| async {
        let response = test::TestRequest::get()
            .insert_header((
                actix_web::http::header::AUTHORIZATION,
                format!("Bearer {}", test_context.admin_token),
            ))
            .uri(&endpoint)
            .send_request(&app)
            .await
            .map_into_boxed_body();
        let response = assert_response_status(response, StatusCode::OK).await;
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["computed"]["shared"]["materialization_stale"], true);
    });
    futures::future::join_all(reads).await;
    let payloads = with_connection(&test_context.pool, async |connection| {
        use crate::schema::tasks::dsl as tasks;
        tasks::tasks
            .filter(tasks::status.eq_any(["queued", "validating", "running"]))
            .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
                "request_payload->>'type' = 'computed_read_repairs'",
            ))
            .select(tasks::request_payload)
            .load::<Option<serde_json::Value>>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(payloads.len(), 1);
    assert_eq!(
        payloads[0].as_ref().unwrap()["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    execute_read_repair(&test_context, &claim).await.unwrap();
    let response = get_request(&test_context.pool, &test_context.admin_token, &endpoint).await;
    let body: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(body["computed"]["shared"]["materialization_stale"], false);
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn expired_repair_claim_is_fenced_and_queue_resumes_after_restart(
    #[future(awt)] test_context: TestContext,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (fixture, endpoint) = stale_fixture(&test_context, "repair restart").await;
    let previous = queued_repair(&test_context, &endpoint).await;
    let id = previous.task().id();
    with_connection(&test_context.pool, async |connection| {
        diesel::sql_query("UPDATE tasks SET lease_expires_at = (clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '1 second' WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(id.id()).execute(connection).await
    }).await.unwrap();
    crate::services::tasks::recover_expired_task_leases(&test_context.pool, 1000)
        .await
        .unwrap();
    let replacement =
        hubuum_storage_postgres::test_support::claim_task_by_id(test_context.pool.get_ref(), id)
            .await
            .unwrap();
    assert!(execute_read_repair(&test_context, &previous).await.is_err());
    execute_read_repair(&test_context, &replacement)
        .await
        .unwrap();
    let cached = persisted_computed_value(&test_context, fixture.objects[0].id).await;
    assert_eq!(cached["display_name"], "inventory.example");
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::object_deleted(false)]
#[case::class_deleted(true)]
#[tokio::test]
async fn repair_skips_identities_deleted_after_the_read(
    #[future(awt)] test_context: TestContext,
    #[case] delete_class: bool,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (mut fixture, endpoint) = stale_fixture(&test_context, "repair deleted identity").await;
    let task = queued_repair(&test_context, &endpoint).await;
    with_connection(&test_context.pool, async |connection| {
        diesel::delete(crate::schema::hubuumobject::table.find(fixture.objects[0].id))
            .execute(connection)
            .await?;
        if delete_class {
            diesel::delete(crate::schema::hubuumclass::table.find(fixture.class.id))
                .execute(connection)
                .await?;
        }
        Ok::<_, hubuum_storage_postgres::PostgresStorageError>(())
    })
    .await
    .unwrap();
    let completed = execute_read_repair(&test_context, &task).await.unwrap();
    assert_eq!(
        completed.status(),
        hubuum_storage_core::StorageTaskStatus::Succeeded
    );
    fixture.objects.clear();
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::failure(false)]
#[case::timeout(true)]
#[tokio::test]
async fn enqueue_failure_keeps_snapshot_values_and_later_reads_retry(
    #[future(awt)] test_context: TestContext,
    #[case] timeout: bool,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (fixture, endpoint) = stale_fixture(&test_context, "repair admission failure").await;
    let point = hubuum_storage_postgres::PostgresFaultPoint::ComputedRepairBeforeEnqueue;
    let fault = if timeout {
        hubuum_storage_postgres::PostgresFaultController::pausing(point)
    } else {
        hubuum_storage_postgres::PostgresFaultController::failing(point)
    };
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fault.run(get_request(
            &test_context.pool,
            &test_context.admin_token,
            &endpoint,
        )),
    )
    .await
    .unwrap();
    let response = assert_response_status(response, StatusCode::OK).await;
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        body["computed"]["shared"]["values"]["display_name"],
        "inventory.example"
    );
    assert_eq!(body["computed"]["shared"]["materialization_stale"], true);
    let retry = queued_repair(&test_context, &endpoint).await;
    execute_read_repair(&test_context, &retry).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn repair_rolls_back_materialization_if_its_claim_expires_before_commit(
    #[future(awt)] test_context: TestContext,
) {
    use hubuum_storage_core::StorageTaskLeaseDuration;
    use hubuum_storage_postgres::test_support::claim_task_by_id_with_lease;
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (fixture, endpoint) = stale_fixture(&test_context, "repair commit fence").await;
    let first = queued_repair(&test_context, &endpoint).await;
    // Return the fixture claim to queued, then claim with a short real DB lease.
    with_connection(&test_context.pool, async |connection| {
        diesel::sql_query("UPDATE tasks SET status = 'queued', lease_token = NULL, lease_expires_at = NULL WHERE id = $1")
            .bind::<diesel::sql_types::Integer, _>(first.task().id().id()).execute(connection).await
    }).await.unwrap();
    let claim = claim_task_by_id_with_lease(
        test_context.pool.get_ref(),
        first.task().id(),
        StorageTaskLeaseDuration::from_milliseconds(5000).unwrap(),
    )
    .await
    .unwrap();
    let pause = hubuum_storage_postgres::PostgresFaultController::pausing(
        hubuum_storage_postgres::PostgresFaultPoint::ComputedRepairBeforeCommit,
    );
    let worker = pause.run(execute_read_repair(&test_context, &claim));
    let expire = async {
        pause.wait_until_reached().await;
        tokio::time::sleep(std::time::Duration::from_millis(5100)).await;
        pause.resume();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        tokio::join!(worker, expire)
    })
    .await
    .expect("claim expiry must release the paused worker");
    assert!(result.is_err());
    let count = with_connection(&test_context.pool, async |connection| {
        crate::schema::object_computed_data::table
            .filter(crate::schema::object_computed_data::object_id.eq(fixture.objects[0].id))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(count, 0);
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn saturated_queue_does_not_fail_reads_or_grow_and_later_reads_retry(
    #[future(awt)] test_context: TestContext,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let (fixture, endpoint) = stale_fixture(&test_context, "repair saturation").await;
    let task = queued_repair(&test_context, &endpoint).await;
    let payload = serde_json::json!({"type":"computed_read_repairs", "objects":
        (1..=4096).map(|offset| serde_json::json!({"class_id":i32::MAX,"object_id":i32::MAX-offset})).collect::<Vec<_>>()});
    with_connection(&test_context.pool, async |connection| {
        diesel::update(crate::schema::tasks::table.find(task.task().id().id()))
            .set((
                crate::schema::tasks::request_payload.eq(Some(payload.clone())),
                crate::schema::tasks::total_items.eq(4096),
            ))
            .execute(connection)
            .await
    })
    .await
    .unwrap();
    let response = get_request(&test_context.pool, &test_context.admin_token, &endpoint).await;
    let body: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(
        body["computed"]["shared"]["values"]["display_name"],
        "inventory.example"
    );
    let retained = with_connection(&test_context.pool, async |connection| {
        crate::schema::tasks::table
            .find(task.task().id().id())
            .select(crate::schema::tasks::request_payload)
            .first::<Option<serde_json::Value>>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(retained, Some(payload));
    execute_read_repair(&test_context, &task).await.unwrap();
    let retry = queued_repair(&test_context, &endpoint).await;
    execute_read_repair(&test_context, &retry).await.unwrap();
    assert_eq!(
        persisted_computed_value(&test_context, fixture.objects[0].id).await["display_name"],
        "inventory.example"
    );
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn repair_skips_objects_reclassified_after_the_read(
    #[future(awt)] test_context: TestContext,
) {
    let _guard = REPAIR_TEST_LOCK.lock().await;
    reset_read_repairs(&test_context).await;
    let destination = fixture(&test_context, "repair destination class").await;
    let (fixture, endpoint) = stale_fixture(&test_context, "repair old class").await;
    let task = queued_repair(&test_context, &endpoint).await;
    hubuum_storage_postgres::test_support::reclassify_object_history_fixture(
        test_context.pool.get_ref(),
        hubuum_domain::ObjectId::new(fixture.objects[0].id).unwrap(),
        ClassId::new(destination.class.id).unwrap(),
    )
    .await
    .unwrap();
    execute_read_repair(&test_context, &task).await.unwrap();
    let count = with_connection(&test_context.pool, async |connection| {
        crate::schema::object_computed_data::table
            .filter(crate::schema::object_computed_data::object_id.eq(fixture.objects[0].id))
            .filter(crate::schema::object_computed_data::class_id.eq(fixture.class.id))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(count, 0);
    fixture.cleanup().await.unwrap();
    destination.cleanup().await.unwrap();
}
