use super::*;
use crate::{
    test_support::{database_role_tests_enabled, integration_test_pool},
    with_transaction,
};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment};
use diesel::result::QueryResult;
use diesel::sql_types::{Integer, Json, Jsonb, Text, Untyped};
use diesel::{QueryableByName, sql_query};
use diesel_async::SimpleAsyncConnection;
use rstest::rstest;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json)]
    #[diesel(column_name = "QUERY PLAN")]
    plan: Value,
}

#[derive(diesel::QueryId)]
struct ExplainQuery<Q>(Q);

impl<Q> Query for ExplainQuery<Q> {
    type SqlType = Untyped;
}

impl<Q: QueryFragment<Pg>> QueryFragment<Pg> for ExplainQuery<Q> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("EXPLAIN (ANALYZE, FORMAT JSON) ");
        self.0.walk_ast(out.reborrow())
    }
}

#[derive(QueryableByName)]
struct SnapshotRow {
    #[diesel(sql_type = Jsonb)]
    snapshot: Value,
}

#[derive(QueryableByName)]
struct ClassRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[rstest]
#[case::ordinary(false)]
#[case::forged_restore(true)]
#[tokio::test]
async fn computed_history_trigger_rolls_back_with_runtime_definition_writes(
    #[case] forged_restore: bool,
) {
    if forged_restore && !database_role_tests_enabled() {
        return;
    }
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        connection.batch_execute("SAVEPOINT history_fixture").await?;
        if forged_restore {
            connection.batch_execute("SET LOCAL hubuum.restore_history = 'on'").await?;
        }
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
        let before_rollback = sql_query("SELECT jsonb_build_object('records', count(*), 'fields', sum(jsonb_array_length(definitions))) AS snapshot FROM public.computed_field_history WHERE class_id = $1")
            .bind::<Integer, _>(class.id).get_result::<SnapshotRow>(connection).await?;
        assert_eq!(before_rollback.snapshot, serde_json::json!({"records":2,"fields":1}));
        connection.batch_execute("ROLLBACK TO SAVEPOINT definition_write").await?;
        let row = sql_query("SELECT jsonb_build_object('records', count(*), 'fields', sum(jsonb_array_length(definitions))) AS snapshot FROM public.computed_field_history WHERE class_id = $1")
            .bind::<Integer, _>(class.id).get_result::<SnapshotRow>(connection).await?;
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
        let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:02Z").unwrap().with_timezone(&Utc);
        let row = computed_history_query!(42, at).get_result::<HistoryRow>(connection).await?;
        assert!(row.visible);
        let selected = row.project()?;
        assert_eq!(selected.history_entry_id().id(), 2);
        assert!(selected.is_effective_at(at));
        let plan = ExplainQuery(computed_history_query!(42, at))
            .get_result::<Explain>(connection).await?.plan;
        let scan = &plan[0]["Plan"]["Plans"][0];
        assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
        assert!(scan["Index Cond"].as_str().unwrap().contains("valid_from"), "{plan:#}");
        assert_eq!(scan["Actual Rows"].as_f64().unwrap(), 1.0, "{plan:#}");
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
