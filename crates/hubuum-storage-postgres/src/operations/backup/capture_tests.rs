use super::*;
use crate::test_support::integration_test_pool;
use crate::{with_connection, with_transaction};
use diesel::sql_types::BigInt;
use diesel_async::SimpleAsyncConnection;
use hubuum_storage_core::{
    StorageBackupBudget, StorageBackupCaptureProgress, StorageError, StorageErrorKind,
};
use rstest::rstest;

#[derive(QueryableByName)]
struct Probe {
    #[diesel(sql_type = BigInt)]
    calls: i64,
}

#[rstest]
#[case::bytes(2500, 1000, 2)]
#[case::rows(100_000, 2, 2)]
#[case::single_large_row(100, 1000, 0)]
#[tokio::test]
async fn oversized_capture_stops_database_work_early(
    #[case] max_bytes: usize,
    #[case] max_rows: usize,
    #[case] retained_rows: usize,
) {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |conn| -> Result<(), PostgresStorageError> {
        // The volatile probe prevents view flattening. Preserve key ordering
        // inside the view so instrumentation does not introduce a full sort.
        conn.batch_execute(
            "CREATE TEMP TABLE backup_corpus (id integer PRIMARY KEY, data text) ON COMMIT DROP;
             INSERT INTO backup_corpus SELECT n, repeat('x', 1024) FROM generate_series(1, 1000) n;
             CREATE TEMP SEQUENCE backup_probe;
             CREATE TEMP VIEW hubuumobject AS SELECT id, data, nextval('backup_probe') AS probe FROM backup_corpus ORDER BY id;
             SET LOCAL enable_sort = off;"
        ).await?;
        let mut progress = StorageBackupCaptureProgress::new(StorageBackupBudget::new(max_bytes, max_rows).unwrap());
        let error = load_capture_rows(conn, "hubuumobject", SnapshotFilter::All, &mut progress, |_| Ok(())).await.unwrap_err();
        assert_eq!(StorageError::from(error).kind(), StorageErrorKind::InputTooLarge);
        let calls = diesel::sql_query("SELECT last_value AS calls FROM backup_probe").get_result::<Probe>(conn).await?.calls;
        // The database-side sequence proves enumeration itself stopped, rather
        // than only the application stopping consumption of a completed SELECT.
        assert_eq!(calls, i64::try_from(retained_rows + 1).unwrap());
        assert_eq!(progress.retained_rows(), retained_rows);
        assert!(progress.retained_bytes() <= max_bytes);
        assert!(progress.scanned_rows() <= max_rows);
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn excluded_history_rows_consume_the_work_budget() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |conn| -> Result<(), PostgresStorageError> {
        conn.batch_execute(
            "CREATE TEMP TABLE tasks (id integer PRIMARY KEY, status text) ON COMMIT DROP;
             INSERT INTO tasks SELECT n, 'running' FROM generate_series(1, 1000) n;
             SET LOCAL enable_sort = off;",
        )
        .await?;
        let mut progress =
            StorageBackupCaptureProgress::new(StorageBackupBudget::new(4096, 3).unwrap());
        let error = load_capture_rows(
            conn,
            "tasks",
            SnapshotFilter::TerminalTasks,
            &mut progress,
            |_| Ok(()),
        )
        .await
        .unwrap_err();
        assert_eq!(
            StorageError::from(error).kind(),
            StorageErrorKind::InputTooLarge
        );
        assert_eq!(progress.scanned_rows(), 3);
        assert_eq!(progress.retained_rows(), 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn capture_orders_by_numeric_primary_keys() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |conn| -> Result<(), PostgresStorageError> {
        conn.batch_execute(
            "CREATE TEMP TABLE hubuumobject (id integer PRIMARY KEY, data text) ON COMMIT DROP;
             INSERT INTO hubuumobject VALUES (10, 'a'), (2, 'z'), (1, 'm');",
        )
        .await?;
        let mut progress =
            StorageBackupCaptureProgress::new(StorageBackupBudget::new(4096, 3).unwrap());
        let rows = load_capture_rows(
            conn,
            "hubuumobject",
            SnapshotFilter::All,
            &mut progress,
            |_| Ok(()),
        )
        .await?;
        assert_eq!(
            rows.iter()
                .map(|r| r.get("id").unwrap().as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 10]
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn successive_capture_cursors_preserve_the_transaction_snapshot() {
    use diesel::sql_types::Text;
    use uuid::Uuid;

    let pool = integration_test_pool(2);
    let name = format!("backup_snapshot_{}", Uuid::new_v4());
    with_connection(&pool, async |conn| -> Result<(), PostgresStorageError> {
        diesel::sql_query("INSERT INTO groups (groupname, description) VALUES ($1, 'before')")
            .bind::<Text, _>(&name)
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    let runtime = PostgresRuntime::unobserved(pool.clone());
    let result = runtime
        .with_read_only_snapshot(async |conn| -> Result<_, PostgresStorageError> {
            let mut progress = StorageBackupCaptureProgress::new(
                StorageBackupBudget::new(1024 * 1024, 10000).unwrap(),
            );
            let before =
                load_capture_rows(conn, "groups", SnapshotFilter::All, &mut progress, |_| {
                    Ok(())
                })
                .await?;
            with_connection(&pool, async |writer| -> Result<(), PostgresStorageError> {
                diesel::sql_query("UPDATE groups SET description = 'after' WHERE groupname = $1")
                    .bind::<Text, _>(&name)
                    .execute(writer)
                    .await?;
                Ok(())
            })
            .await?;
            let after =
                load_capture_rows(conn, "groups", SnapshotFilter::All, &mut progress, |_| {
                    Ok(())
                })
                .await?;
            let find = |rows: Vec<StorageBackupRow>| {
                rows.into_iter()
                    .find(|r| r.get("groupname").and_then(Value::as_str) == Some(name.as_str()))
                    .ok_or_else(|| {
                        PostgresStorageError::database("Missing scoped snapshot fixture")
                    })
            };
            Ok((find(before)?, find(after)?))
        })
        .await;
    with_connection(&pool, async |conn| -> Result<(), PostgresStorageError> {
        diesel::sql_query("DELETE FROM groups WHERE groupname = $1")
            .bind::<Text, _>(&name)
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    let (before, after) = result.unwrap();
    assert_eq!(before, after);
    assert_eq!(
        after.get("description"),
        Some(&Value::String("before".into()))
    );
}

#[tokio::test]
async fn capture_keys_match_every_snapshot_tables_primary_key() {
    #[derive(QueryableByName)]
    struct PrimaryKey {
        #[diesel(sql_type = Text)]
        table_name: String,
        #[diesel(sql_type = Text)]
        columns: String,
    }
    let pool = integration_test_pool(1);
    let keys = with_connection(&pool, async |conn| -> Result<_, PostgresStorageError> {
        Ok(diesel::sql_query(
            "SELECT c.relname::text AS table_name, string_agg(a.attname::text, ', ' ORDER BY k.ordinality) AS columns
             FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ordinality)
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
             WHERE i.indisprimary AND n.nspname = 'public' GROUP BY c.relname"
        ).load::<PrimaryKey>(conn).await?)
    }).await.unwrap().into_iter().map(|key| (key.table_name, key.columns)).collect::<BTreeMap<_, _>>();
    let configured = StorageBackupStateSection::ALL
        .iter()
        .copied()
        .map(state_table)
        .chain(
            StorageBackupHistorySection::ALL
                .iter()
                .copied()
                .map(history_table),
        );
    for table in configured {
        assert_eq!(
            Some(snapshot_key(table).unwrap()),
            keys.get(table).map(String::as_str),
            "{table}"
        );
    }
}
