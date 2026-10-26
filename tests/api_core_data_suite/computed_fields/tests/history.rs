use super::*;
use diesel::sql_types::{Integer, Text};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};

#[rstest::rstest]
#[case::missing_history(false)]
#[case::unsupported_evaluator(true)]
#[tokio::test]
async fn historical_computed_values_never_fall_back_to_live_definitions(
    #[case] unsupported: bool,
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "historical unavailable").await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    let at = chrono::Utc::now();
    let migration_pool = integration_test_migration_pool(1);
    with_transaction(&migration_pool, async |connection| {
        // Only the fixture setup may alter authoritative history; requests below
        // still use the ordinary runtime role.
        if database_role_tests_enabled() {
            let roles = integration_test_database_roles();
            diesel::sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(roles.owner().as_str())
                .execute(connection)
                .await?;
        }
        let query = if unsupported {
            "UPDATE computed_field_history SET evaluator_version = 99 WHERE class_id = $1"
        } else {
            "DELETE FROM computed_field_history WHERE class_id = $1"
        };
        diesel::sql_query(query)
            .bind::<Integer, _>(fixture.class.id)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/{}/history/computed/as-of?at={}",
            fixture.class.id,
            fixture.objects[0].id(),
            at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let result: serde_json::Value = test::read_body_json(response).await;
    if unsupported {
        assert_eq!(result["shared"]["fields"][0]["status"], "unavailable");
        assert_eq!(
            result["shared"]["fields"][0]["reason"],
            "unsupported_evaluator_version"
        );
        assert!(result["shared"]["fields"][0].get("value").is_none());
    } else {
        assert_eq!(result["shared"]["status"], "unavailable");
        assert_eq!(result["shared"]["reason"], "definition_history_unavailable");
        assert!(result["shared"].get("fields").is_none());
    }
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::class(false)]
#[case::collection(true)]
#[tokio::test]
async fn deletion_closes_history_without_waiting_for_a_computed_worker(
    #[case] delete_collection: bool,
    #[future(awt)] test_context: TestContext,
) {
    use diesel::sql_types::{BigInt, Bool};
    use tokio::sync::oneshot;

    #[derive(QueryableByName)]
    struct HistoryClosure {
        #[diesel(sql_type = BigInt)]
        snapshots: i64,
        #[diesel(sql_type = Bool)]
        closed: bool,
    }

    let fixture = fixture(&test_context, "computed history delete lock").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    let (locked, wait_for_lock) = oneshot::channel();
    let (deleted, wait_for_delete) = oneshot::channel();

    // A worker takes the shared advisory lock before the class key-share lock.
    // Hold that first lock until deletion finishes. The trigger must not wait
    // for it while owning the deleted row; a timeout catches the old inversion.
    let worker = with_transaction(&test_context.pool, async |connection| {
        diesel::sql_query("SELECT pg_advisory_xact_lock_shared(1133113, $1)")
            .bind::<Integer, _>(fixture.class.id)
            .execute(connection)
            .await?;
        locked.send(()).expect("deletion is waiting for the worker");
        wait_for_delete.await.expect("deletion signals completion");
        let remaining = crate::schema::hubuumclass::table
            .find(fixture.class.id)
            .for_key_share()
            .select(crate::schema::hubuumclass::id)
            .first::<i32>(connection)
            .await
            .optional()?;
        Ok::<_, PostgresStorageError>(remaining)
    });
    let deletion = async {
        wait_for_lock
            .await
            .expect("worker acquired its advisory lock");
        let result = with_transaction(&test_context.pool, async |connection| {
            diesel::sql_query("SET LOCAL lock_timeout = '2s'")
                .execute(connection)
                .await?;
            let (query, id) = if delete_collection {
                (
                    "DELETE FROM collections WHERE id = $1",
                    fixture.collection.collection.id,
                )
            } else {
                ("DELETE FROM hubuumclass WHERE id = $1", fixture.class.id)
            };
            diesel::sql_query(query)
                .bind::<Integer, _>(id)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await;
        deleted.send(()).expect("worker is waiting for deletion");
        result
    };
    let (worker, deletion) = tokio::join!(worker, deletion);
    deletion.expect("deletion must not wait for the computed advisory lock");
    assert_eq!(worker.unwrap(), None);
    let history = with_connection(&test_context.pool, async |connection| {
        diesel::sql_query(
            "SELECT count(*) AS snapshots, bool_and(valid_to IS NOT NULL) AS closed \
             FROM computed_field_history WHERE class_id = $1",
        )
        .bind::<Integer, _>(fixture.class.id)
        .get_result::<HistoryClosure>(connection)
        .await
    })
    .await
    .unwrap();
    assert!(history.snapshots >= 2 && history.closed);
    if !delete_collection {
        fixture
            .collection
            .collection
            .delete_without_events(&test_context.pool)
            .await
            .unwrap();
    }
    fixture
        .collection
        .owner_group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
