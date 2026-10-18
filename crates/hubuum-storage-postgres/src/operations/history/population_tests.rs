use super::*;
use diesel::QueryableByName;
use diesel::sql_types::Json;
use hubuum_query::{CursorValue, SortParam, encode_cursor_values};
use rstest::rstest;

use crate::test_support::integration_test_pool;
use crate::with_transaction;

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json)]
    #[diesel(column_name = "QUERY PLAN")]
    plan: Value,
}

#[rstest]
#[case::older(true, vec![5999, 5998])]
#[case::newer(false, vec![6001, 6002])]
#[tokio::test]
async fn historical_population_seeks_large_class_without_loading_earlier_pages(
    #[case] descending: bool,
    #[case] expected: Vec<i64>,
) {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        diesel::sql_query("CREATE TEMP TABLE hubuumobject_history (LIKE public.hubuumobject_history INCLUDING INDEXES) ON COMMIT DROP").execute(connection).await?;
        diesel::sql_query("INSERT INTO pg_temp.hubuumobject_history
            (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision)
            SELECT n, 'object-' || n, 1, 42, jsonb_build_object('payload', repeat('x', 1024)), '',
            '2026-01-01'::timestamp, '2026-01-01'::timestamp,
            'I', '2026-01-01'::timestamptz, NULL, n, 1
            FROM generate_series(1, 12000) n").execute(connection).await?;
        diesel::sql_query("ANALYZE pg_temp.hubuumobject_history").execute(connection).await?;
        let sorts = vec![SortParam::new(FilterField::HistoryId, descending)];
        let cursor = encode_cursor_values(&sorts, vec![CursorValue::Integer(6000)]).unwrap();
        let options = QueryOptions::new(vec![], sorts, Some(2), Some(cursor), false).unwrap();
        let at = DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z").unwrap().with_timezone(&Utc);
        let query = StorageClassObjectsAsOfQuery::try_new(ClassId::new(42).unwrap(), at, options).unwrap()
            .visibility(StorageHistoryCollectionScope::All, true);
        let options = query.options();
        let fields = history_cursor_fields(options, "hubuumobject_history")?;
        let mut records = class_objects_as_of_query(&query)?;
        crate::apply_query_options_with_fields!(records, options, fields,
            crate::cursor::CursorTieBreaker::new(FilterField::HistoryId, false,
                history_cursor_field(&FilterField::HistoryId, "hubuumobject_history")?));
        let rows = records.load::<ObjectHistoryRow>(connection).await?;
        assert_eq!(rows.iter().map(|row| row.history_id).collect::<Vec<_>>(), expected);
        let comparison = if descending { "<" } else { ">" };
        let order = if descending { "DESC" } else { "ASC" };
        let plan = diesel::sql_query(format!("EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT * FROM pg_temp.hubuumobject_history
            WHERE hubuum_class_id = 42 AND op <> 'D'
            AND tstzrange(valid_from, valid_to, '[)') @> '2026-01-02'::timestamptz
            AND history_id {comparison} 6000 ORDER BY history_id {order} LIMIT 2"))
            .get_result::<Explain>(connection).await?.plan;
        let scan = &plan[0]["Plan"]["Plans"][0];
        assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
        assert!(scan["Index Cond"].as_str().unwrap().contains("history_id"), "{plan:#}");
        assert!(scan["Actual Rows"].as_f64().unwrap() <= 2.0, "{plan:#}");
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn historical_population_indexes_sparse_validity_intervals() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        diesel::sql_query("CREATE TEMP TABLE hubuumobject_history (LIKE public.hubuumobject_history INCLUDING INDEXES) ON COMMIT DROP").execute(connection).await?;
        diesel::sql_query("INSERT INTO pg_temp.hubuumobject_history
            (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision)
            SELECT n, 'object-' || n, 1, 42, '{}'::jsonb, '',
            '2026-01-01'::timestamp, '2026-01-01'::timestamp,
            'I', '2026-01-01'::timestamptz,
            CASE WHEN n = 12000 THEN NULL ELSE '2026-01-02'::timestamptz END,
            n, 1 FROM generate_series(1, 12000) n").execute(connection).await?;
        diesel::sql_query("ANALYZE pg_temp.hubuumobject_history").execute(connection).await?;
        let plan = diesel::sql_query("EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT count(*) FROM pg_temp.hubuumobject_history
            WHERE hubuum_class_id = 42 AND op <> 'D'
            AND tstzrange(valid_from, valid_to, '[)') @> '2026-01-03'::timestamptz")
            .get_result::<Explain>(connection).await?.plan;
        let scan = &plan[0]["Plan"]["Plans"][0];
        assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
        assert!(scan["Index Cond"].as_str().unwrap().contains("@>"), "{plan:#}");
        assert_eq!(scan["Actual Rows"].as_f64().unwrap(), 1.0, "{plan:#}");
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
