use super::*;
use crate::{test_support::integration_test_pool, with_transaction};
use diesel::sql_types::Json;

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json)]
    #[diesel(column_name = "QUERY PLAN")]
    plan: Value,
}

#[tokio::test]
async fn computed_history_trigger_rolls_back_with_definition_writes() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        use diesel_async::SimpleAsyncConnection;
        connection.batch_execute("CREATE TEMP TABLE hubuumclass (id integer) ON COMMIT DROP;
            CREATE TEMP TABLE computed_field_history (LIKE public.computed_field_history INCLUDING ALL) ON COMMIT DROP;
            CREATE TEMP TABLE computed_field_definitions (
                id integer, class_id integer, revision bigint, visibility text, key text, label text,
                description text, operation jsonb, result_type text, enabled boolean, semantics_version smallint
            ) ON COMMIT DROP;
            CREATE TRIGGER computed_history AFTER INSERT OR UPDATE OR DELETE ON computed_field_definitions
                FOR EACH ROW EXECUTE FUNCTION public.track_shared_computed_field_history();
            INSERT INTO hubuumclass VALUES (42);
            SELECT public.record_shared_computed_field_history(42);
            SAVEPOINT definition_write;
            INSERT INTO computed_field_definitions VALUES (1, 42, 1, 'shared', 'value', 'Value', '',
                '{\"type\":\"first_non_null\",\"paths\":[\"/value\"]}', 'integer', true, 1);
            ROLLBACK TO SAVEPOINT definition_write;").await?;
        let row = sql_query("SELECT jsonb_build_object('records', count(*), 'fields', sum(jsonb_array_length(definitions))) AS snapshot FROM pg_temp.computed_field_history")
            .get_result::<HistoryRow>(connection).await?;
        assert_eq!(row.snapshot, serde_json::json!({"records":1,"fields":0}));
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
