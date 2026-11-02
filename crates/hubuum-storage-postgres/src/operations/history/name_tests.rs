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
#[case(false, vec![7000, 8000])]
#[case(true, vec![5000, 4000])]
#[tokio::test]
async fn name_history_seeks_installed_index_with_timestamp_ties(
    #[case] descending: bool,
    #[case] expected: Vec<i64>,
) {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        diesel::sql_query("CREATE TEMP TABLE hubuumobject_history (LIKE public.hubuumobject_history INCLUDING INDEXES) ON COMMIT DROP").execute(connection).await?;
        diesel::sql_query("INSERT INTO pg_temp.hubuumobject_history
            (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision)
            SELECT n, CASE WHEN n % 1000 = 0 THEN 'server01' ELSE 'other' END,
            1, 42, '{}'::jsonb, '', '2026-01-01'::timestamp, '2026-01-01'::timestamp,
            'D', '2026-01-01'::timestamptz, '2026-01-01'::timestamptz, n, 1
            FROM generate_series(1, 12000) n").execute(connection).await?;
        diesel::sql_query("ANALYZE pg_temp.hubuumobject_history").execute(connection).await?;
        let sorts = vec![SortParam::new(FilterField::HistoryId, descending)];
        let cursor = encode_cursor_values(&sorts, vec![CursorValue::Integer(6000)]).unwrap();
        let options = QueryOptions::new(vec![], sorts, Some(2), Some(cursor), true).unwrap();
        let query = StorageObjectHistoryNameQuery::try_new(ClassId::new(42).unwrap(), "server01".into(), options).unwrap()
            .visibility(StorageHistoryCollectionScope::All, true);
        let options = query.options();
        let fields = history_cursor_fields(options, "hubuumobject_history")?;
        let mut records = object_name_history_query(&query)?;
        crate::apply_query_options_with_fields!(records, options, fields,
            crate::cursor::CursorTieBreaker::new(FilterField::HistoryId, false,
                history_cursor_field(&FilterField::HistoryId, "hubuumobject_history")?));
        let rows = records.load::<ObjectHistoryRow>(connection).await?;
        assert_eq!(rows.iter().map(|row| row.history_id).collect::<Vec<_>>(), expected);
        assert!(rows.windows(2).all(|pair| pair[0].valid_from == pair[1].valid_from));

        let comparison = if descending { "<" } else { ">" };
        let order = if descending { "DESC" } else { "ASC" };
        let plan = diesel::sql_query(format!("EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT * FROM pg_temp.hubuumobject_history
            WHERE hubuum_class_id = 42 AND name = 'server01' AND history_id {comparison} 6000
            ORDER BY history_id {order} LIMIT 2"))
            .get_result::<Explain>(connection).await?.plan;
        let scan = &plan[0]["Plan"]["Plans"][0];
        assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
        let condition = scan["Index Cond"].as_str().unwrap();
        assert!(condition.contains("hubuum_class_id") && condition.contains("name") && condition.contains("history_id"), "{plan:#}");
        assert!(scan["Actual Rows"].as_f64().expect("actual row count") <= 2.0, "{plan:#}");
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
