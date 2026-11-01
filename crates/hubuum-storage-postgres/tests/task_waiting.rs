#![cfg(feature = "integration-test-support")]

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text, Uuid as SqlUuid};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_domain::TaskId;
use hubuum_storage_core::{
    StorageErrorKind, StorageTaskCheckpoint, StorageTaskClaimToken, StorageTaskLease,
    StorageTaskResumeOutcome, StorageTaskResumeRequest, StorageTaskStatus, StorageTaskWaitRequest,
    StorageTaskWaitToken, TaskExecutionStorage,
};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{
    PostgresConnection, PostgresPool, PostgresStorage, PostgresStorageError, with_transaction,
};
use hubuum_task_core::TaskStopReason;
use rstest::rstest;
use serde_json::json;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[derive(QueryableByName)]
struct Id {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(QueryableByName)]
struct LeaseToken {
    #[diesel(sql_type = SqlUuid)]
    lease_token: Uuid,
}

#[derive(QueryableByName)]
struct Accounting {
    #[diesel(sql_type = Bool)]
    outstanding: bool,
    #[diesel(sql_type = Bool)]
    executing: bool,
    #[diesel(sql_type = BigInt)]
    resident_input_bytes: i64,
    #[diesel(sql_type = BigInt)]
    cumulative_input_bytes: i64,
    #[diesel(sql_type = Bool)]
    checkpoint_present: bool,
}

async fn start_fixture(connection: &mut PostgresConnection) -> Result<i32, PostgresStorageError> {
    if database_role_tests_enabled() {
        connection
            .batch_execute(&format!(
                "SET LOCAL ROLE \"{}\"",
                integration_test_database_roles().owner().as_str(),
            ))
            .await?;
    }
    connection
        .batch_execute("SAVEPOINT waiting_fixture")
        .await?;
    Ok(diesel::sql_query(
        "INSERT INTO tasks (kind,status,request_payload,started_at,attempt_count,lease_token,lease_expires_at,execution_deadline_at)
         VALUES ('export','running','{}',clock_timestamp() AT TIME ZONE 'UTC',1,$1,
            (clock_timestamp() AT TIME ZONE 'UTC') + INTERVAL '1 hour',
            (clock_timestamp() AT TIME ZONE 'UTC') + INTERVAL '2 hours') RETURNING id",
    )
    .bind::<SqlUuid, _>(Uuid::new_v4())
    .get_result::<Id>(connection)
    .await?
    .id)
}

async fn accounting(
    connection: &mut PostgresConnection,
    id: i32,
) -> Result<Accounting, PostgresStorageError> {
    diesel::sql_query(
        "SELECT a.outstanding, a.lease_token IS NOT NULL AS executing,
            a.resident_input_bytes,a.cumulative_input_bytes,t.task_checkpoint IS NOT NULL AS checkpoint_present
         FROM hubuum_control.task_accounting a JOIN tasks t ON t.id=a.task_id WHERE t.id=$1",
    )
    .bind::<Integer, _>(id)
    .get_result::<Accounting>(connection)
    .await
    .map_err(Into::into)
}

async fn delete_committed_fixture(pool: &PostgresPool, id: i32) {
    // Remove the detached accounting history these native-only tasks otherwise
    // leave behind, as well as the task itself.
    with_transaction(
        pool,
        async |connection| -> Result<(), PostgresStorageError> {
            if database_role_tests_enabled() {
                connection
                    .batch_execute(&format!(
                        "SET LOCAL ROLE \"{}\"",
                        integration_test_database_roles().owner().as_str(),
                    ))
                    .await?;
            }
            diesel::sql_query("DELETE FROM hubuum_control.task_accounting WHERE task_id=$1")
                .bind::<Integer, _>(id)
                .execute(connection)
                .await?;
            diesel::sql_query("DELETE FROM tasks WHERE id=$1")
                .bind::<Integer, _>(id)
                .execute(connection)
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn uncontended_expired_notification_returns_stopped() {
    let owner_pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let storage = PostgresStorage::unobserved(integration_test_pool(1));
    let token = Uuid::new_v4();
    let checkpoint = StorageTaskCheckpoint::try_new("native.test", 1, json!({})).unwrap();
    let id = with_transaction(&owner_pool, async |connection| -> Result<_, PostgresStorageError> {
        let id = start_fixture(connection).await?;
        // Represent an admitted execution whose deadline has already elapsed.
        diesel::sql_query("UPDATE tasks SET status='waiting',task_checkpoint=$2,wait_token=$3,lease_token=NULL,lease_expires_at=NULL,
            created_at=(clock_timestamp() AT TIME ZONE 'UTC')-INTERVAL '3 seconds',
            started_at=(clock_timestamp() AT TIME ZONE 'UTC')-INTERVAL '2 seconds',
            execution_deadline_at=(clock_timestamp() AT TIME ZONE 'UTC')-INTERVAL '1 second' WHERE id=$1")
            .bind::<Integer, _>(id).bind::<Text, _>(checkpoint.encoded())
            .bind::<SqlUuid, _>(token).execute(connection).await?;
        Ok(id)
    }).await.unwrap();
    // This native binary has no recovery sweeps. The notification itself must
    // settle the expired wait, rather than observe another executor's result.
    let result = storage
        .resume_task(StorageTaskResumeRequest::new(
            TaskId::new(id).unwrap(),
            StorageTaskWaitToken::try_new(token.to_string()).unwrap(),
        ))
        .await;
    delete_committed_fixture(&owner_pool, id).await;
    let StorageTaskResumeOutcome::Stopped(task) = result.unwrap() else {
        panic!("an uncontended expired notification must return Stopped");
    };
    assert_eq!(task.status(), StorageTaskStatus::Cancelled);
    assert_eq!(
        task.control().terminal_reason(),
        Some(TaskStopReason::DeadlineExceeded)
    );
    assert!(task.checkpoint().is_none());
    assert!(!task.has_lease());
}

#[rstest]
#[case::waiting(0)]
#[case::queued(1)]
#[case::reclaimed(2)]
#[case::terminal(3)]
#[tokio::test]
async fn checkpoint_accounting_retains_admission_and_charges_each_claim(#[case] transitions: u8) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
        let id = start_fixture(connection).await?;
        let initial = accounting(connection, id).await?;
        let checkpoint = StorageTaskCheckpoint::try_new("native.test", 1, json!({"cursor": "opaque"})).unwrap();
        diesel::sql_query("UPDATE tasks SET status='waiting',task_checkpoint=$2,wait_token=$3,lease_token=NULL,lease_expires_at=NULL WHERE id=$1")
            .bind::<Integer,_>(id).bind::<Text,_>(checkpoint.encoded()).bind::<SqlUuid,_>(Uuid::new_v4()).execute(connection).await?;
        if transitions >= 1 {
            diesel::sql_query("UPDATE tasks SET status='queued',wait_token=NULL WHERE id=$1").bind::<Integer,_>(id).execute(connection).await?;
        }
        if transitions >= 2 {
            diesel::sql_query("UPDATE tasks SET status='validating',lease_token=$2,lease_expires_at=(clock_timestamp() AT TIME ZONE 'UTC')+INTERVAL '1 hour',attempt_count=attempt_count+1 WHERE id=$1")
                .bind::<Integer,_>(id).bind::<SqlUuid,_>(Uuid::new_v4()).execute(connection).await?;
        }
        if transitions == 3 {
            diesel::sql_query("UPDATE tasks SET status='cancelled',lease_token=NULL,lease_expires_at=NULL,request_payload=NULL,finished_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=$1")
                .bind::<Integer,_>(id).execute(connection).await?;
        }
        let observed = accounting(connection, id).await?;
        let retained = initial.resident_input_bytes + checkpoint.byte_size() as i64;
        assert_eq!(observed.outstanding, transitions != 3);
        assert_eq!(observed.executing, transitions == 2);
        assert_eq!(observed.checkpoint_present, transitions != 3);
        assert_eq!(observed.resident_input_bytes, if transitions == 3 { 0 } else { retained });
        assert_eq!(observed.cumulative_input_bytes, initial.cumulative_input_bytes + if transitions >= 2 { retained } else { 0 });
        connection.batch_execute("ROLLBACK TO SAVEPOINT waiting_fixture").await?;
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn rejected_checkpoint_growth_preserves_the_live_reservation() {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
        let id = start_fixture(connection).await?;
        let initial = accounting(connection, id).await?;
        connection.batch_execute("UPDATE hubuum_control.task_policy SET policy=jsonb_set(policy,'{deployment,resident_bytes}','1'); SAVEPOINT rejected_wait").await?;
        let checkpoint = StorageTaskCheckpoint::try_new("native.test", 1, json!({"cursor": "opaque"})).unwrap();
        let rejected = diesel::sql_query("UPDATE tasks SET status='waiting',task_checkpoint=$2,wait_token=$3,lease_token=NULL,lease_expires_at=NULL WHERE id=$1")
            .bind::<Integer,_>(id).bind::<Text,_>(checkpoint.encoded()).bind::<SqlUuid,_>(Uuid::new_v4()).execute(connection).await;
        assert!(rejected.unwrap_err().to_string().contains("task_capacity_resident_bytes"));
        connection.batch_execute("ROLLBACK TO SAVEPOINT rejected_wait").await?;
        let observed = accounting(connection, id).await?;
        assert!(observed.outstanding && observed.executing && !observed.checkpoint_present);
        assert_eq!(observed.resident_input_bytes, initial.resident_input_bytes);
        assert_eq!(observed.cumulative_input_bytes, initial.cumulative_input_bytes);
        connection.batch_execute("ROLLBACK TO SAVEPOINT waiting_fixture").await?;
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn rollback_refuses_to_discard_a_durable_continuation() {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
        let id = start_fixture(connection).await?;
        let checkpoint = StorageTaskCheckpoint::try_new("native.test", 1, json!({})).unwrap();
        diesel::sql_query("UPDATE tasks SET status='waiting',task_checkpoint=$2,wait_token=$3,lease_token=NULL,lease_expires_at=NULL WHERE id=$1")
            .bind::<Integer,_>(id).bind::<Text,_>(checkpoint.encoded()).bind::<SqlUuid,_>(Uuid::new_v4()).execute(connection).await?;
        connection.batch_execute("SAVEPOINT rejected_rollback").await?;
        let rejected = connection.batch_execute(include_str!("../migrations/2026-11-01-000001_task_waiting/down.sql")).await;
        assert!(rejected.unwrap_err().to_string().contains("Drain or cancel checkpointed tasks"));
        connection.batch_execute("ROLLBACK TO SAVEPOINT rejected_rollback").await?;
        assert!(accounting(connection, id).await?.checkpoint_present);
        connection.batch_execute("ROLLBACK TO SAVEPOINT waiting_fixture").await?;
        Ok(())
    }).await.unwrap();
}

#[rstest]
#[case::unsupported_kind("import", true, false, true, "{}")]
#[case::missing_deadline("export", false, false, true, "{}")]
#[case::retained_execution_lease("export", true, true, true, "{}")]
#[case::missing_wait_token("export", true, false, false, "{}")]
#[case::non_object_checkpoint_data("export", true, false, true, "null")]
#[tokio::test]
async fn database_rejects_invalid_wait_states(
    #[case] kind: &str,
    #[case] deadline: bool,
    #[case] retain_lease: bool,
    #[case] token: bool,
    #[case] data: &str,
) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            let id = start_fixture(connection).await?;
            connection.batch_execute("SAVEPOINT invalid_wait").await?;
            let checkpoint =
                format!("{{\"format\":\"native.test\",\"version\":1,\"data\":{data}}}");
            let result = diesel::sql_query(
                "UPDATE tasks SET kind=$2,status='waiting',task_checkpoint=$3,wait_token=$4,
            execution_deadline_at=CASE WHEN $5 THEN execution_deadline_at ELSE NULL END,
            lease_token=CASE WHEN $6 THEN lease_token ELSE NULL END,
            lease_expires_at=CASE WHEN $6 THEN lease_expires_at ELSE NULL END WHERE id=$1",
            )
            .bind::<Integer, _>(id)
            .bind::<Text, _>(kind)
            .bind::<Text, _>(checkpoint)
            .bind::<Nullable<SqlUuid>, _>(token.then(Uuid::new_v4))
            .bind::<Bool, _>(deadline)
            .bind::<Bool, _>(retain_lease)
            .execute(connection)
            .await;
            assert!(result.unwrap_err().to_string().contains("tasks_wait_shape"));
            connection
                .batch_execute(
                    "ROLLBACK TO SAVEPOINT invalid_wait; ROLLBACK TO SAVEPOINT waiting_fixture",
                )
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[rstest]
#[case::checkpoint(false, false)]
#[case::resume(true, false)]
#[case::stale_resume(true, true)]
#[tokio::test]
async fn wait_operations_do_not_block_task_first_writers(
    #[case] resume: bool,
    #[case] stale_token: bool,
) {
    let owner_pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let storage = PostgresStorage::unobserved(integration_test_pool(1));
    let checkpoint = StorageTaskCheckpoint::try_new("native.test", 1, json!({})).unwrap();
    let wait_token = Uuid::new_v4();
    let (id, lease_token) = with_transaction(
        &owner_pool,
        async |connection| -> Result<_, PostgresStorageError> {
            let id = start_fixture(connection).await?;
            let lease_token = diesel::sql_query("SELECT lease_token FROM tasks WHERE id=$1")
                .bind::<Integer, _>(id)
                .get_result::<LeaseToken>(connection)
                .await?
                .lease_token;
            if resume {
                diesel::sql_query("UPDATE tasks SET status='waiting',task_checkpoint=$2,wait_token=$3,lease_token=NULL,lease_expires_at=NULL WHERE id=$1")
                    .bind::<Integer, _>(id)
                    .bind::<Text, _>(checkpoint.encoded())
                    .bind::<SqlUuid, _>(wait_token)
                    .execute(connection)
                    .await?;
            }
            Ok((id, lease_token))
        },
    )
    .await
    .unwrap();

    let observed = with_transaction(
        &owner_pool,
        async |connection| -> Result<_, PostgresStorageError> {
            if database_role_tests_enabled() {
                connection
                    .batch_execute(&format!(
                        "SET LOCAL ROLE \"{}\"",
                        integration_test_database_roles().owner().as_str(),
                    ))
                    .await?;
            }
            // Progress/completion starts with this row lock, then enters the
            // accounting trigger's global policy lock. The new operation must
            // return while this lock remains held, rather than invert the pair.
            diesel::sql_query("SELECT id FROM tasks WHERE id=$1 FOR UPDATE")
                .bind::<Integer, _>(id)
                .get_result::<Id>(connection)
                .await?;
            let task_id = TaskId::new(id).unwrap();
            let operation = async {
                if resume {
                    storage
                        .resume_task(StorageTaskResumeRequest::new(
                            task_id,
                            StorageTaskWaitToken::try_new(
                                if stale_token {
                                    Uuid::new_v4()
                                } else {
                                    wait_token
                                }
                                .to_string(),
                            )
                            .unwrap(),
                        ))
                        .await
                        .map(|outcome| matches!(outcome, StorageTaskResumeOutcome::Unchanged))
                } else {
                    storage
                        .wait_task(StorageTaskWaitRequest::new(
                            StorageTaskLease::new(
                                task_id,
                                StorageTaskClaimToken::new(lease_token.to_string()),
                            ),
                            checkpoint.clone(),
                        ))
                        .await
                        .map(|_| false)
                }
            };
            let result = timeout(Duration::from_secs(5), operation).await;
            // Also prove the rejected operation released its policy lock, so
            // the original row owner can finish its accounting write.
            timeout(
                Duration::from_secs(5),
                diesel::sql_query("UPDATE tasks SET total_items=total_items+1,processed_items=processed_items+1,success_items=success_items+1 WHERE id=$1")
                    .bind::<Integer, _>(id)
                    .execute(connection),
            )
            .await
            .expect("checkpoint/resume retained the capacity lock")?;
            Ok(result)
        },
    )
    .await;

    // Clean committed fixtures before asserting.
    delete_committed_fixture(&owner_pool, id).await;

    let result = observed
        .unwrap()
        .expect("checkpoint/resume waited for the busy task row");
    if stale_token {
        assert!(
            result.unwrap(),
            "stale wake must remain unchanged even while locked"
        );
    } else {
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
    }
}
