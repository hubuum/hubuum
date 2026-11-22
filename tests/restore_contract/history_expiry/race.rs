use super::*;
use diesel::sql_types::{Bool, Integer, Nullable, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::SimpleAsyncConnection;
use hubuum::errors::ApiError;
use hubuum_storage_postgres::{
    PostgresConnection, PostgresPool, PostgresStorage, PostgresStorageError, with_transaction,
};
use tokio::sync::oneshot;

#[derive(QueryableByName)]
struct BackendPid {
    #[diesel(sql_type = Integer)]
    pid: i32,
}

async fn backend_pid(connection: &mut PostgresConnection) -> i32 {
    sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<BackendPid>(connection)
        .await
        .unwrap()
        .pid
}

async fn recovery_session() -> (PostgresStorage, i32) {
    let pool = postgres_test_pool_with_timeout(&database_url(), 1, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let pid = backend_pid(&mut pool.get().await.unwrap()).await;
    (PostgresStorage::unobserved(pool), pid)
}

enum Waiter {
    Backend(i32),
    RestoreReplacingTables,
    RestoreRotatingApplication,
}

async fn wait_for_waiter(connection: &mut PostgresConnection, blocker: i32, waiter: Waiter) -> i32 {
    let (pid, condition) = match waiter {
        Waiter::Backend(pid) => (Some(pid), "TRUE"),
        Waiter::RestoreReplacingTables => (
            None,
            "locktype='relation' AND relation='public.export_templates_history'::regclass AND mode='AccessExclusiveLock'",
        ),
        Waiter::RestoreRotatingApplication => (
            None,
            "pid IN (SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND query LIKE '%hubuum_rotate_application_for_restore()%')",
        ),
    };
    let query = format!(
        "SELECT DISTINCT pid FROM pg_locks WHERE NOT granted
        AND $1=ANY(pg_blocking_pids(pid)) AND ($2::integer IS NULL OR pid=$2) AND {condition}"
    );
    timeout(Duration::from_secs(30), async {
        loop {
            let waits = sql_query(&query)
                .bind::<Integer, _>(blocker)
                .bind::<Nullable<Integer>, _>(pid)
                .load::<BackendPid>(connection)
                .await
                .unwrap();
            if let [wait] = waits.as_slice() {
                return wait.pid;
            }
            assert!(waits.is_empty(), "unexpected competing restore sessions");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("expected PostgreSQL lock dependency")
}

async fn with_table_lock<T, F: Future<Output = T>>(
    pool: &PostgresPool,
    statement: &str,
    race: impl FnOnce(i32, oneshot::Sender<()>) -> F,
) -> T {
    let (locked, gate_pid) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let gate = with_transaction(pool, async move |gate| {
        gate.batch_execute(statement).await?;
        locked.send(backend_pid(gate).await).unwrap();
        released.await.unwrap();
        Ok::<_, PostgresStorageError>(())
    });
    let (gate, result) = tokio::join!(gate, async { race(gate_pid.await.unwrap(), release).await });
    gate.unwrap();
    result
}

async fn copy_present(connection: &mut PostgresConnection, target: StorageRecordIdentity) -> bool {
    #[derive(QueryableByName)]
    struct Copy {
        #[diesel(sql_type = Bool)]
        present: bool,
    }
    sql_query(
        "SELECT EXISTS(SELECT 1 FROM public.collections_history
        WHERE logical_identity #>> '{subject,identity,identity}'=$1
          AND logical_identity #>> '{subject,version}'=$2) AS present",
    )
    .bind::<Text, _>(target.resource().to_string())
    .bind::<Text, _>(target.version().to_string())
    .get_result::<Copy>(connection)
    .await
    .unwrap()
    .present
}

pub(super) async fn restore_with_queued_replay<T>(
    kind: StorageBackendKind,
    pool: &PostgresPool,
    restore: impl Future<Output = Result<T, ApiError>>,
    request: HistoryExpiryReapplication,
) {
    if kind == StorageBackendKind::Memory {
        restore.await.unwrap();
        return;
    }
    let (recovery, recovery_pid) = recovery_session().await;
    let target = request.target();
    let replay = recovery.reapply_history_expiry(request);
    tokio::pin!(restore, replay);
    let mut observer = pool.get().await.unwrap();
    // Backup reads can proceed. Restore rotates the application before requesting
    // exclusive locks to replace the backed-up tables.
    let (restored, stale) = with_table_lock(pool, "LOCK TABLE public.export_templates_history IN ACCESS SHARE MODE", async |gate, release| {
        let restore_pid = tokio::select! {
            result = &mut restore => panic!("restore finished before replacing history: {:?}", result.err()),
            pid = wait_for_waiter(&mut observer, gate, Waiter::RestoreReplacingTables) => pid,
        };
        tokio::select! {
            _ = &mut restore => panic!("restore escaped its table lock"),
            result = &mut replay => panic!("expiry replay escaped the restore: {result:?}"),
            _ = wait_for_waiter(&mut observer, restore_pid, Waiter::Backend(recovery_pid)) => {},
        }
        release.send(()).unwrap();
        tokio::join!(restore, replay)
    }).await;
    restored.unwrap();
    let stale = stale.unwrap_err();
    assert_eq!(stale.kind(), StorageErrorKind::ValidationFailed, "{stale}");
    assert!(
        stale
            .message()
            .contains("Protected recovery authority or application image changed"),
        "{stale}"
    );
    assert!(!copy_present(&mut observer, target).await);
}

pub(super) async fn replay_with_queued_restore(
    pool: &PostgresPool,
    restore: impl Future<Output = Result<(), ApiError>>,
    request: HistoryExpiryReapplication,
    trace: &ReplayTrace,
) {
    let (recovery, recovery_pid) = recovery_session().await;
    let target = request.target();
    let batch = request.batch();
    let checkpoint = request.protection().checkpoint().clone();
    let original = request.protection().fence();
    assert!(!request.has_receipt());
    let replay = recovery.reapply_history_expiry(request);
    tokio::pin!(replay, restore);
    let mut observer = pool.get().await.unwrap();
    assert!(copy_present(&mut observer, target).await);
    // The required first-removal audit holds expiry inside its transaction.
    // Only poll restore through the observed wait until we read the first receipt.
    with_table_lock(pool, "LOCK TABLE public.events IN SHARE MODE", async |gate, release| {
        tokio::select! {
            result = &mut replay => panic!("expiry escaped the audit lock: {result:?}"),
            _ = wait_for_waiter(&mut observer, gate, Waiter::Backend(recovery_pid)) => {},
        }
        tokio::select! {
            _ = &mut replay => panic!("expiry escaped its audit lock"),
            result = &mut restore => panic!("restore escaped expiry: {result:?}"),
            _ = wait_for_waiter(&mut observer, recovery_pid, Waiter::RestoreRotatingApplication) => {},
        }
        release.send(()).unwrap();
        assert!((&mut replay).await.unwrap());
    }).await;
    let removed = recovery.capture_history_lifecycle().await.unwrap();
    assert_eq!(removed.fence().incarnation(), original.incarnation());
    let [first] = removed
        .control()
        .authority()
        .expiry()
        .intent(batch)
        .unwrap()
        .removals()
    else {
        panic!("expected one receipt before restore replaced the data")
    };
    assert_eq!(first.target(), target);
    assert_eq!(first.checkpoint(), &checkpoint);
    assert!(!copy_present(&mut observer, target).await);

    // The coordinator yields to read control after its data transaction commits.
    // Inspect that committed image before allowing signed reapplication to run.
    tokio::select! {
        biased;
        () = trace.data_replaced.notified() => {},
        result = &mut restore => panic!("expected to observe the restored data before release: {result:?}"),
    }
    let application = recovery.read_application_control().await.unwrap();
    assert_ne!(application.fence().incarnation(), original.incarnation());
    assert!(!application.is_released_for(&recovery.read_deletion_control().await.unwrap()));
    assert!(copy_present(&mut observer, target).await);
    restore.await.unwrap();
    assert!(!copy_present(&mut observer, target).await);
    let after = recovery.capture_history_lifecycle().await.unwrap();
    assert_eq!(
        after
            .control()
            .authority()
            .expiry()
            .intent(batch)
            .unwrap()
            .removals()
            .iter()
            .find(|receipt| receipt.target() == target),
        Some(first)
    );
}
