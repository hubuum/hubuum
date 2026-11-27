use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;
use diesel::sql_types::{Integer, Json};
use diesel::{QueryableByName, dsl::sql, sql_query};
use diesel_async::SimpleAsyncConnection;
use rstest::rstest;

use super::*;
use crate::schema::hubuumobject_history as h;
use crate::test_support::integration_test_pool;
use crate::with_transaction;

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json, column_name = "QUERY PLAN")]
    plan: Value,
}

#[derive(QueryableByName)]
struct Position {
    #[diesel(sql_type = BigInt)]
    history_id: i64,
}

#[derive(diesel::QueryId)]
struct PreparePositionLookup<Q>(Q);

impl<Q: QueryFragment<Pg>> QueryFragment<Pg> for PreparePositionLookup<Q> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("PREPARE history_position_lookup(TIMESTAMPTZ, BIGINT) AS ");
        self.0.walk_ast(out.reborrow())?;
        out.push_sql(" LIMIT 2");
        Ok(())
    }
}

#[rstest]
#[case::older(true, [5999, 5998])]
#[case::newer(false, [6001, 6002])]
#[tokio::test]
async fn position_comparison_seeks_timestamp_ties_in_generic_plans(
    #[case] descending: bool,
    #[case] expected: [i64; 2],
) {
    let pool = integration_test_pool(1);
    let (rows, plan) = with_transaction(&pool, async |connection| {
        connection.batch_execute(
            "CREATE TEMP TABLE hubuumobject_history (LIKE public.hubuumobject_history INCLUDING INDEXES) ON COMMIT DROP;
             INSERT INTO pg_temp.hubuumobject_history
               (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision)
               SELECT 42, 'timeline', 1, 7, '{}'::jsonb, '', '2026-01-01'::timestamp, '2026-01-01'::timestamp,
               'D', '2026-01-01'::timestamptz, '2026-01-01'::timestamptz, n, 1
               FROM generate_series(1, 12000) n;
             ANALYZE pg_temp.hubuumobject_history;
             SET LOCAL plan_cache_mode = force_generic_plan;"
        ).await?;
        let mut query = h::table
            .filter(h::id.eq(sql::<Integer>("42")))
            .filter(h::hubuum_class_id.eq(sql::<Integer>("7")))
            .select(h::history_id)
            .into_boxed();
        // Exercise the production expression with server parameters, including
        // the history ID tie breaker, rather than a hand-written equivalent.
        let anchor = (sql::<Timestamptz>("$1"), sql::<BigInt>("$2"));
        query = if descending {
            query.filter(history_position!(h).lt(anchor))
                .order((h::valid_from.desc(), h::history_id.desc()))
        } else {
            query.filter(history_position!(h).gt(anchor))
                .order((h::valid_from.asc(), h::history_id.asc()))
        };
        PreparePositionLookup(query).execute(connection).await?;
        let rows = sql_query("EXECUTE history_position_lookup('2026-01-01', 6000)")
            .load::<Position>(connection).await?;
        let plan = sql_query("EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE history_position_lookup('2026-01-01', 6000)")
            .get_result::<Explain>(connection).await?.plan;
        connection.batch_execute("DEALLOCATE history_position_lookup").await?;
        Ok::<_, PostgresStorageError>((rows, plan))
    }).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.history_id).collect::<Vec<_>>(),
        expected
    );
    let scan = &plan[0]["Plan"]["Plans"][0];
    assert!(
        matches!(
            scan["Node Type"].as_str(),
            Some("Index Scan" | "Index Only Scan")
        ),
        "{plan:#}"
    );
    let condition = scan["Index Cond"].as_str().unwrap();
    assert!(
        condition.contains("valid_from") && condition.contains("history_id"),
        "{plan:#}"
    );
    assert_eq!(scan["Actual Rows"].as_f64().unwrap(), 2.0, "{plan:#}");
    assert_eq!(
        scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0),
        0.0,
        "{plan:#}"
    );
}
