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

#[tokio::test]
async fn resource_identity_predicate_keeps_index_in_generic_plans() {
    use diesel::pg::Pg;
    use diesel::query_builder::{AstPass, QueryFragment};
    use diesel::result::QueryResult;
    use diesel_async::SimpleAsyncConnection;

    diesel::table! {
        history_identity_lookup_fixture (id) {
            id -> Integer,
            logical_identity -> Nullable<Jsonb>,
        }
    }

    // Prepare the actual Diesel predicate, with the requested identity left as
    // a server parameter. EXPLAIN EXECUTE then exercises a generic cached plan,
    // rather than specializing a fresh EXPLAIN SELECT to its current bindings.
    #[derive(diesel::QueryId)]
    struct PrepareIdentityLookup<Q>(Q);
    impl<Q: QueryFragment<Pg>> QueryFragment<Pg> for PrepareIdentityLookup<Q> {
        fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            out.push_sql("PREPARE history_identity_lookup(TEXT) AS ");
            self.0.walk_ast(out.reborrow())
        }
    }

    let pool = integration_test_pool(1);
    let plan = with_transaction(&pool, async |connection| {
        connection.batch_execute(
            "CREATE TEMP TABLE history_identity_lookup_fixture (id INTEGER PRIMARY KEY, logical_identity JSONB) ON COMMIT DROP;
             INSERT INTO history_identity_lookup_fixture
               SELECT n, jsonb_build_object('subject',jsonb_build_object('identity',jsonb_build_object('identity',n::TEXT)))
               FROM generate_series(1,20000) n;
             CREATE INDEX history_identity_lookup_fixture_identity
               ON history_identity_lookup_fixture ((logical_identity #>> '{subject,identity,identity}'));
             ANALYZE history_identity_lookup_fixture;
             SET LOCAL plan_cache_mode = force_generic_plan;"
        ).await?;
        let query = history_identity_lookup_fixture::table
            .filter(history_identity_lookup_fixture::logical_identity
                .retrieve_by_path_as_text(resource_identity_path())
                .eq(sql::<Nullable<Text>>("$1")))
            .select(history_identity_lookup_fixture::id);
        PrepareIdentityLookup(query).execute(connection).await?;
        let plan = diesel::sql_query(
            "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE history_identity_lookup('15000')"
        ).get_result::<Explain>(connection).await?.plan;
        connection.batch_execute("DEALLOCATE history_identity_lookup").await?;
        Ok::<_, PostgresStorageError>(plan)
    }).await.unwrap();
    let scan = &plan[0]["Plan"];
    assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
    assert_eq!(scan["Actual Rows"], 1, "{plan:#}");
    assert_eq!(
        scan["Rows Removed by Filter"].as_u64().unwrap_or(0),
        0,
        "{plan:#}"
    );
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
        let rows = records.select(ObjectHistoryRow::as_select()).load::<ObjectHistoryRow>(connection).await?;
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
