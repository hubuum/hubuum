//! PostgreSQL-only fixture construction and storage accounting for issue #504.

use async_trait::async_trait;
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Jsonb, Text};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_scale_core::lifecycle::{
    LifecycleBenchmarkBackend, LifecycleError, LifecycleFixture, LifecycleInventory,
    LogicalMeasurement, MeasuredBytes, PhysicalMeasurement, RelationMeasurement,
};
use serde_json::json;

use super::{PostgresScaleBackend, initialize_benchmark_identities, load_raw_dataset_with_pool};
use crate::{PostgresConnection, PostgresStorageError, with_transaction};

#[derive(QueryableByName)]
struct RelationRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = BigInt)]
    table_bytes: i64,
    #[diesel(sql_type = BigInt)]
    index_bytes: i64,
}

#[derive(QueryableByName)]
struct LogicalRow {
    #[diesel(sql_type = BigInt)]
    rows: i64,
    #[diesel(sql_type = BigInt)]
    encoded_bytes: i64,
    #[diesel(sql_type = BigInt)]
    largest_row_bytes: i64,
}

fn measurement_error(error: impl std::fmt::Display) -> LifecycleError {
    LifecycleError::Measurement(error.to_string())
}

fn count(value: i64) -> Result<u64, LifecycleError> {
    u64::try_from(value).map_err(measurement_error)
}

impl TryFrom<LogicalRow> for LogicalMeasurement {
    type Error = LifecycleError;

    fn try_from(value: LogicalRow) -> Result<Self, Self::Error> {
        Self::new(
            count(value.rows)?,
            MeasuredBytes::new(count(value.encoded_bytes)?),
            MeasuredBytes::new(count(value.largest_row_bytes)?),
        )
    }
}

async fn logical_measurement(
    connection: &mut PostgresConnection,
    quoted_relation: &str,
) -> Result<LogicalRow, PostgresStorageError> {
    // Only a catalog-quoted identifier or an internal test constant reaches this
    // helper. The aggregate retains one encoded row at a time in PostgreSQL and
    // returns three counts, never payloads, to the benchmark process.
    Ok(diesel::sql_query(format!(
        "SELECT count(*)::bigint AS rows, \
           coalesce(sum(octet_length(to_jsonb(r)::text)), 0)::bigint AS encoded_bytes, \
           coalesce(max(octet_length(to_jsonb(r)::text)), 0)::bigint AS largest_row_bytes \
         FROM {quoted_relation} r"
    ))
    .get_result(connection)
    .await?)
}

#[async_trait]
impl LifecycleBenchmarkBackend for PostgresScaleBackend {
    async fn load_lifecycle_fixture(
        &self,
        fixture: &LifecycleFixture,
    ) -> Result<LifecycleInventory, LifecycleError> {
        let profile = fixture.profile();
        let workload = fixture.workload();
        // This already rejects anything but a freshly migrated disposable DB.
        load_raw_dataset_with_pool(profile, &self.pool)
            .await
            .map_err(measurement_error)?;
        let before = self.lifecycle_inventory().await?;

        // Fixed-size batches keep fixture generation independent of corpus size.
        let mut tasks = workload.tasks();
        loop {
            let batch: Vec<_> = tasks
                .by_ref()
                .take(128)
                .map(|task| {
                    json!({
                        "id": task.ordinal(), "principal": task.principal_ordinal(),
                        "duration_ms": task.duration().as_millis(),
                        "output_bytes": task.output_bytes().map_or(0, MeasuredBytes::bytes),
                    })
                })
                .collect();
            if batch.is_empty() {
                break;
            }
            with_transaction(&self.pool, async |connection| {
                // These are unpublished synthetic rows. Capture immutable
                // ownership once, after the complete recipe is assembled.
                connection.batch_execute("SET LOCAL session_replication_role=replica").await?;
                let batch = json!(batch);
                diesel::sql_query(
                    "UPDATE tasks SET kind = CASE WHEN fixture.output_bytes > 0 THEN 'export' ELSE tasks.kind END, \
                       status = 'succeeded', submitted_by = fixture.principal, \
                       finished_at = started_at + fixture.duration_ms * interval '1 millisecond' \
                     FROM jsonb_to_recordset($1) AS fixture(id int, principal int, duration_ms bigint, output_bytes int) \
                     WHERE tasks.id = fixture.id",
                ).bind::<Jsonb, _>(&batch).execute(connection).await?;
                diesel::sql_query(
                    "INSERT INTO export_task_outputs (task_id, content_type, text_output, meta_json, warnings_json, \
                       warning_count, truncated, output_expires_at, total_duration_ms, query_duration_ms, \
                       hydration_duration_ms, render_duration_ms) \
                     SELECT id, 'text/plain', \
                       (SELECT left(string_agg(md5(id::text || ':' || block::text), '' ORDER BY block), output_bytes) \
                        FROM generate_series(1, (output_bytes + 31) / 32) block), \
                       '{}'::jsonb, '[]'::jsonb, 0, false, timestamp '2030-01-01', duration_ms, 0, 0, 0 \
                     FROM jsonb_to_recordset($1) AS fixture(id int, duration_ms int, output_bytes int) \
                     WHERE output_bytes > 0",
                ).bind::<Jsonb, _>(&batch).execute(connection).await?;
                Ok::<_, PostgresStorageError>(())
            }).await.map_err(measurement_error)?;
        }
        initialize_benchmark_identities(&self.pool)
            .await
            .map_err(measurement_error)?;
        for start in (1..=profile.totals.objects).step_by(128) {
            let end = (start + 127).min(profile.totals.objects);
            let batch: Vec<_> = (start..=end)
                .map(|id| {
                    json!({
                        "id": id, "bytes": workload.row_bytes(id).bytes(),
                    })
                })
                .collect();
            with_transaction(&self.pool, async |connection| {
                // Real triggers retain the previous version. The deterministic
                // hexadecimal payload avoids an unrealistically compressible
                // repeated-character fixture while remaining entirely synthetic.
                diesel::sql_query(
                    "UPDATE hubuumobject SET data = data || jsonb_build_object('lifecycle_payload', \
                       (SELECT left(string_agg(md5(fixture.id::text || ':' || block::text), '' ORDER BY block), bytes) \
                        FROM generate_series(1, (bytes + 31) / 32) block)) \
                     FROM jsonb_to_recordset($1) AS fixture(id int, bytes int) \
                     WHERE hubuumobject.id = fixture.id",
                ).bind::<Jsonb, _>(json!(batch)).execute(connection).await?;
                Ok::<_, PostgresStorageError>(())
            }).await.map_err(measurement_error)?;
        }
        Ok(before)
    }

    async fn lifecycle_inventory(&self) -> Result<LifecycleInventory, LifecycleError> {
        let measurements = with_transaction(&self.pool, async |connection| {
            connection
                .batch_execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; \
                 SET LOCAL statement_timeout = '120s'; SET LOCAL lock_timeout = '5s'",
                )
                .await?;
            // pg_table_size includes TOAST and auxiliary forks; pg_indexes_size
            // includes this relation's indexes. Do not count TOAST again.
            let relations = diesel::sql_query(
                "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name, \
                   pg_table_size(c.oid)::bigint AS table_bytes, \
                   pg_indexes_size(c.oid)::bigint AS index_bytes \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY c.relname",
            )
            .load::<RelationRow>(connection)
            .await?;
            let mut measurements = Vec::with_capacity(relations.len());
            for relation in relations {
                let logical = logical_measurement(connection, &relation.name).await?;
                measurements.push((relation, logical));
            }
            Ok::<_, PostgresStorageError>(measurements)
        })
        .await
        .map_err(measurement_error)?;
        measurements
            .into_iter()
            .map(|(relation, logical)| {
                Ok(RelationMeasurement::new(
                    relation.name,
                    logical.try_into()?,
                    PhysicalMeasurement::new(
                        MeasuredBytes::new(count(relation.table_bytes)?),
                        MeasuredBytes::new(count(relation.index_bytes)?),
                    ),
                ))
            })
            .collect::<Result<Vec<_>, LifecycleError>>()
            .map(LifecycleInventory::new)
    }
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests {
    use super::super::{CountRow, load_database_settings};
    use super::*;
    use crate::test_support::integration_test_pool;
    use rstest::rstest;

    #[tokio::test]
    async fn reported_buffer_bytes_match_postgres_blocks() {
        let pool = integration_test_pool(1);
        let settings = load_database_settings(&pool).await.unwrap();
        let difference = with_transaction(&pool, async |connection| {
            diesel::sql_query(
                "SELECT pg_size_bytes($1) - setting::bigint * current_setting('block_size')::bigint AS value \
                 FROM pg_settings WHERE name = 'shared_buffers'",
            )
            .bind::<Text, _>(&settings["shared_buffers"])
            .get_result::<CountRow>(connection)
            .await
        })
        .await
        .unwrap();
        assert_eq!(difference.value, 0);
    }

    #[rstest]
    #[case::empty(0, 0)]
    #[case::utf8(1, 21)]
    #[case::multiple(3, 63)]
    #[tokio::test]
    async fn inventory_counts_encoded_utf8_rows(#[case] rows: i64, #[case] expected: u64) {
        let pool = integration_test_pool(1);
        let measured = with_transaction(&pool, async |connection| {
            connection
                .batch_execute(
                    "CREATE TEMP TABLE lifecycle_measurement (payload text) ON COMMIT DROP",
                )
                .await?;
            diesel::sql_query(
                "INSERT INTO lifecycle_measurement SELECT 'æøå' FROM generate_series(1, $1)",
            )
            .bind::<BigInt, _>(rows)
            .execute(connection)
            .await?;
            logical_measurement(connection, "pg_temp.lifecycle_measurement").await
        })
        .await
        .unwrap();
        let measured = LogicalMeasurement::try_from(measured).unwrap();
        assert_eq!(measured.encoded_bytes().bytes(), expected);
    }
}
