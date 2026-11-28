//! PostgreSQL shared-buffer counters for one complete service lookup.
use crate::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS;
use crate::test_support::{
    archive_test_observer_url, archive_test_runtime_database_url, postgres_test_pool_with_timeout,
};
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Text, Timestamptz};
use diesel::{QueryableByName, sql_query};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::PostgresPool;
use hubuum_storage_postgres::test_support::{IsolatedRestoreTarget, integration_test_database_url};
use reqwest::Url;
use serde_json::{Value, json};

pub(super) struct PageProbe {
    observer: PostgresPool,
    database: IsolatedRestoreTarget,
    runtime_login: String,
}

#[derive(QueryableByName)]
pub(super) struct Counters {
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    hits: i64,
    #[diesel(sql_type = BigInt)]
    reads: i64,
    #[diesel(sql_type = BigInt)]
    temp_reads: i64,
    #[diesel(sql_type = BigInt)]
    temp_writes: i64,
    #[diesel(sql_type = BigInt)]
    block_size: i64,
    #[diesel(sql_type = BigInt)]
    dealloc: i64,
    #[diesel(sql_type = Timestamptz)]
    stats_reset: DateTime<Utc>,
}

impl PageProbe {
    pub(super) async fn new() -> Self {
        let url = Url::parse(&archive_test_runtime_database_url()).unwrap();
        let database = IsolatedRestoreTarget::new(url.as_str(), &integration_test_database_url())
            .expect("page diagnostics require the separate archive fixture database");
        let runtime_login = url.username().to_owned();
        let observer = postgres_test_pool_with_timeout(
            archive_test_observer_url(&url).as_str(),
            1,
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        );
        {
            let mut connection = observer.get().await.unwrap();
            database.validate_connection(&mut connection).await.unwrap();
            // Install after application migrations have reconciled the split-role
            // grants. These functions belong to the administrative observer.
            connection
                .batch_execute(
                    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;
                 CREATE EXTENSION IF NOT EXISTS pg_buffercache WITH SCHEMA public",
                )
                .await
                .expect("install diagnostic extensions in the isolated archive database");
        }
        let probe = Self {
            observer,
            database,
            runtime_login,
        };
        // Fail before fixture setup if the server did not load pg_stat_statements.
        probe.counters().await;
        probe
    }

    pub(super) async fn low_compression_description(&self) -> String {
        #[derive(QueryableByName)]
        struct Description {
            #[diesel(sql_type = Text)]
            value: String,
        }
        let row: Description = sql_query(
            "SELECT string_agg(md5(i::text), '' ORDER BY i) AS value FROM generate_series(1,98304) i",
        )
        .get_result(&mut self.observer.get().await.unwrap())
        .await
        .unwrap();
        assert_eq!(row.value.len(), 3 * 1024 * 1024);
        row.value
    }

    async fn counters(&self) -> Counters {
        sql_query(
            "SELECT totals.*, current_setting('block_size')::bigint AS block_size,
                    info.dealloc, info.stats_reset
             FROM public.pg_stat_statements_info info CROSS JOIN LATERAL (
               SELECT COALESCE(sum(calls),0)::bigint AS calls,
                      COALESCE(sum(shared_blks_hit),0)::bigint AS hits,
                      COALESCE(sum(shared_blks_read),0)::bigint AS reads,
                      COALESCE(sum(temp_blks_read),0)::bigint AS temp_reads,
                      COALESCE(sum(temp_blks_written),0)::bigint AS temp_writes
               FROM public.pg_stat_statements
               WHERE toplevel AND dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
                 AND userid=(SELECT oid FROM pg_roles WHERE rolname=$1)
             ) totals",
        )
        .bind::<Text, _>(&self.runtime_login)
        .get_result(&mut self.observer.get().await.unwrap())
        .await
        .expect("install pg_stat_statements in the disposable fixture")
    }

    pub(super) async fn start_cold(&self) -> Counters {
        #[derive(QueryableByName)]
        struct Eviction {
            #[diesel(sql_type = BigInt)]
            evicted: i64,
            #[diesel(sql_type = BigInt)]
            skipped: i64,
        }
        {
            let mut connection = self.observer.get().await.unwrap();
            self.database
                .validate_connection(&mut connection)
                .await
                .unwrap();
            let result: Eviction = sql_query(
                "WITH RECURSIVE relations(oid) AS (
                   SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname IN ('public','hubuum_control') AND c.relkind IN ('r','m')
                   UNION
                   SELECT child FROM relations r JOIN pg_class c ON c.oid=r.oid
                   CROSS JOIN LATERAL (
                      SELECT c.reltoastrelid AS child WHERE c.reltoastrelid<>0
                      UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid=c.oid
                   ) children
                 )
                 SELECT sum(buffers_evicted)::bigint AS evicted, sum(buffers_skipped)::bigint AS skipped
                 FROM relations CROSS JOIN LATERAL public.pg_buffercache_evict_relation(oid::regclass)",
            )
            .get_result(&mut connection)
            .await
            .expect("install pg_buffercache in the disposable PostgreSQL 18 fixture");
            assert!(result.evicted > 0);
            assert_eq!(result.skipped, 0, "all selected relations must start cold");
        }
        self.counters().await
    }

    pub(super) async fn finish(&self, before: Counters) -> Value {
        let after = self.counters().await;
        assert_eq!(before.stats_reset, after.stats_reset);
        assert_eq!(
            before.dealloc, after.dealloc,
            "statement counters were evicted"
        );
        assert!(after.calls > before.calls);
        let delta = |new: i64, old: i64| u64::try_from(new - old).expect("counter must increase");
        let reads = delta(after.reads, before.reads);
        assert!(
            reads > 0,
            "the runtime must fetch pages from the cold relations"
        );
        json!({
            "statements": after.calls - before.calls,
            "block_bytes": after.block_size,
            "shared_hits": delta(after.hits,before.hits),
            "shared_reads": reads,
            "shared_read_bytes": reads * u64::try_from(after.block_size).unwrap(),
            "temp_reads": delta(after.temp_reads,before.temp_reads),
            "temp_writes": delta(after.temp_writes,before.temp_writes),
        })
    }
}
