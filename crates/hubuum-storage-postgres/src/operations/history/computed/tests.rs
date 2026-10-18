use super::*;
use crate::{test_support::integration_test_pool, with_transaction};
use diesel::sql_types::{Json, Text};
use diesel_async::SimpleAsyncConnection;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json)]
    #[diesel(column_name = "QUERY PLAN")]
    plan: Value,
}

#[derive(QueryableByName)]
struct ClassRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[tokio::test]
async fn computed_history_trigger_rolls_back_with_runtime_definition_writes() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        connection.batch_execute("SAVEPOINT history_fixture").await?;
        let name = format!("computed_history_{}", Uuid::new_v4().simple());
        let class = sql_query("WITH collection AS (
            INSERT INTO public.collections(name, description, parent_collection_id)
            SELECT $1, 'computed history test', id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id
        ) INSERT INTO public.hubuumclass(name, collection_id, description, validate_schema)
            SELECT $1, id, 'computed history test', false FROM collection RETURNING id")
            .bind::<Text, _>(&name).get_result::<ClassRow>(connection).await?;
        connection.batch_execute("SAVEPOINT definition_write").await?;
        sql_query("INSERT INTO public.computed_field_definitions(class_id, visibility, key, label, operation, result_type)
            VALUES ($1, 'shared', 'value', 'Value', '{\"type\":\"first_non_null\",\"paths\":[\"/value\"]}', 'integer')")
            .bind::<Integer, _>(class.id).execute(connection).await?;
        connection.batch_execute("ROLLBACK TO SAVEPOINT definition_write").await?;
        let row = sql_query("SELECT jsonb_build_object('records', count(*), 'fields', sum(jsonb_array_length(definitions))) AS snapshot FROM public.computed_field_history WHERE class_id = $1")
            .bind::<Integer, _>(class.id).get_result::<HistoryRow>(connection).await?;
        assert_eq!(row.snapshot, serde_json::json!({"records":1,"fields":0}));
        connection.batch_execute("ROLLBACK TO SAVEPOINT history_fixture").await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn computed_history_old_dates_seek_past_newer_versions() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        sql_query("CREATE TEMP TABLE computed_field_history (LIKE public.computed_field_history INCLUDING INDEXES) ON COMMIT DROP").execute(connection).await?;
        sql_query("INSERT INTO pg_temp.computed_field_history (history_id, class_id, valid_from, valid_to, evaluator_version, definitions)
            SELECT n, 42, '2026-01-01'::timestamptz + n * interval '1 second',
            '2026-01-01'::timestamptz + (n + 1) * interval '1 second', 1, '[]'::jsonb
            FROM generate_series(1, 12000) n").execute(connection).await?;
        sql_query("ANALYZE pg_temp.computed_field_history").execute(connection).await?;
        let plan = sql_query("EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM pg_temp.computed_field_history
            WHERE class_id = 42 AND valid_from <= '2026-01-01 00:00:02+00' ORDER BY valid_from DESC LIMIT 1")
            .get_result::<Explain>(connection).await?.plan;
        let scan = &plan[0]["Plan"]["Plans"][0];
        assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
        assert!(scan["Index Cond"].as_str().unwrap().contains("valid_from"), "{plan:#}");
        assert_eq!(scan["Actual Rows"].as_f64().unwrap(), 1.0, "{plan:#}");
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
