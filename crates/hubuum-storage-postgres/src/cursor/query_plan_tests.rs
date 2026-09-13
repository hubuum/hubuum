//! Plans use the installed migration indexes, copied into isolated temporary
//! tables. Locale ordering, deep seeks, both directions, and both cursor forms
//! are checked without changing shared planner settings or persistent fixtures.

use diesel::QueryableByName;
use diesel::sql_types::{Integer, Json, Text};
use diesel_async::RunQueryDsl;
use hubuum_query::{
    CursorValue, FilterField, QueryContinuation, QueryOptions, SortParam, encode_cursor_values,
};
use rstest::rstest;
use serde_json::Value;

use super::{
    CursorSqlField, CursorSqlType, cursor_filter_sql_for_fields, order_sql_clause_for_field,
};
use crate::test_support::integration_test_pool;
use crate::{PostgresStorageError, with_transaction};

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(QueryableByName)]
struct Boundary {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json)]
    #[diesel(column_name = "QUERY PLAN")]
    value: Value,
}

#[derive(Clone, Copy, Debug)]
enum Position {
    First,
    Token,
    Continuation,
}

fn assert_bounded_plan(plan: &Value, continued: bool) {
    fn visit(node: &Value, visited: &mut u64, index_conditions: &mut usize) {
        let kind = node["Node Type"].as_str().expect("plan node type");
        assert!(
            !matches!(
                kind,
                "Sort" | "Incremental Sort" | "Seq Scan" | "Bitmap Heap Scan"
            ),
            "cursor page must preserve index ordering: {node:#}"
        );
        *visited += node["Actual Rows"].as_u64().unwrap_or_default()
            + node["Rows Removed by Filter"].as_u64().unwrap_or_default();
        *index_conditions += usize::from(node.get("Index Cond").is_some());
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                visit(child, visited, index_conditions);
            }
        }
    }
    let root = &plan[0]["Plan"];
    let mut visited = 0;
    let mut index_conditions = 0;
    visit(root, &mut visited, &mut index_conditions);
    assert!(visited <= 8, "a tiny page visited {visited} rows: {plan:#}");
    let blocks = root["Local Hit Blocks"].as_u64().unwrap_or_default()
        + root["Local Read Blocks"].as_u64().unwrap_or_default();
    assert!(
        blocks <= 24,
        "a tiny page visited {blocks} buffers: {plan:#}"
    );
    if continued {
        assert!(
            index_conditions > 0,
            "continued page must seek within an index: {plan:#}"
        );
    }
}

#[rstest]
#[case::collections_name_c("collections", "name", &[], "collections_name_c", "")]
#[case::collections_parent_name_c("collections", "name", &["parent_collection_id"], "collections_parent_name_c", "")]
#[case::hubuumclass_name_c("hubuumclass", "name", &[], "hubuumclass_name_c", "")]
#[case::hubuumclass_collection_name_c("hubuumclass", "name", &["collection_id"], "hubuumclass_collection_name_c", "")]
#[case::hubuumobject_name_c("hubuumobject", "name", &[], "hubuumobject_name_c", "")]
#[case::hubuumobject_class_name_c("hubuumobject", "name", &["hubuum_class_id"], "hubuumobject_class_name_c", "")]
#[case::hubuumobject_collection_name_c("hubuumobject", "name", &["collection_id"], "hubuumobject_collection_name_c", "")]
#[case::groups_name_c("groups", "groupname", &[], "groups_name_c", "")]
#[case::groups_scope_name_c("groups", "groupname", &["identity_scope_id"], "groups_scope_name_c", "")]
#[case::principals_name_c("principals", "name", &[], "principals_name_c", "")]
#[case::principals_scope_name_c("principals", "name", &["identity_scope_id"], "principals_scope_name_c", "")]
#[case::identity_scopes_name_c("identity_scopes", "name", &[], "identity_scopes_name_c", "")]
#[case::export_templates_collection_name_c("export_templates", "name", &["collection_id"], "export_templates_collection_name_c", "")]
#[case::remote_targets_collection_name_c("remote_targets", "name", &["collection_id"], "remote_targets_collection_name_c", "")]
#[case::event_sinks_name_c("event_sinks", "name", &[], "event_sinks_name_c", "")]
#[case::event_subscriptions_collection_name_c("event_subscriptions", "name", &["collection_id"], "event_subscriptions_collection_name_c", "")]
#[case::computed_fields_shared_name_c("computed_field_definitions", "key", &["class_id"], "computed_fields_shared_name_c", "visibility = 'shared'")]
#[case::computed_fields_personal_name_c("computed_field_definitions", "key", &["owner_user_id", "class_id"], "computed_fields_personal_name_c", "visibility = 'personal'")]
#[tokio::test]
async fn name_cursor_pages_use_installed_indexes(
    #[case] table: &str,
    #[case] column: &str,
    #[case] scope: &[&str],
    #[case] index_prefix: &str,
    #[case] partial_predicate: &str,
    #[values(false, true)] descending: bool,
    #[values(Position::First, Position::Token, Position::Continuation)] position: Position,
) {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        let index_name = format!("{index_prefix}{}_id_idx", if descending { "_desc" } else { "" });
        let index = diesel::sql_query(
            "SELECT indexdef AS value FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1 AND indexname = $2",
        ).bind::<Text, _>(table).bind::<Text, _>(&index_name)
            .get_result::<TextRow>(connection).await?;
        assert!(index.value.contains("COLLATE \"C\""), "{index_name} lost byte ordering");

        // LIKE copies the real indexes, including scoped/partial and descending
        // definitions. No triggers, foreign keys, or production sequences run.
        diesel::sql_query(format!(
            "CREATE TEMP TABLE cursor_plan_rows (LIKE public.{table} INCLUDING INDEXES) ON COMMIT DROP",
        )).execute(connection).await?;
        let required = diesel::sql_query(
            "SELECT attname::text AS value FROM pg_attribute WHERE attrelid = 'pg_temp.cursor_plan_rows'::regclass AND attnum > 0 AND attnotnull AND attname <> 'id'",
        ).load::<TextRow>(connection).await?;
        for attribute in required {
            diesel::sql_query(format!(
                "ALTER TABLE cursor_plan_rows ALTER COLUMN \"{}\" DROP NOT NULL",
                attribute.value.replace('"', "\"\""),
            )).execute(connection).await?;
        }
        let locale = diesel::sql_query(
            "SELECT collname AS value FROM pg_collation WHERE collname IN ('en-x-icu', 'en-US-x-icu', 'en_US.utf8', 'en_US.UTF-8', 'en_US') ORDER BY collname LIMIT 1",
        ).get_result::<TextRow>(connection).await?;
        diesel::sql_query(format!(
            "ALTER TABLE cursor_plan_rows ALTER COLUMN {column} TYPE varchar COLLATE \"{}\"",
            locale.value.replace('"', "\"\""),
        )).execute(connection).await?;
        let mut columns = vec!["id", column];
        columns.extend_from_slice(scope);
        let mut values = vec!["n".to_string(), "CASE WHEN n % 2 = 0 THEN 'Z' ELSE 'a' END || lpad(n::text, 8, '0')".to_string()];
        // Sparse scope membership makes a global name scan measurably worse
        // than seeking through the scope-prefixed index.
        values.extend(scope.iter().map(|_| "CASE WHEN n % 100 = 0 THEN 9 ELSE 10 END".to_string()));
        if table == "collections" && scope.is_empty() {
            // Preserve the copied single-root unique index: these are children.
            columns.push("parent_collection_id");
            values.push("9".to_string());
        }
        if !partial_predicate.is_empty() {
            columns.push("visibility");
            values.push(partial_predicate.strip_prefix("visibility = ").unwrap().to_string());
        }
        diesel::sql_query(format!(
            "INSERT INTO cursor_plan_rows ({}) SELECT {} FROM generate_series(1, 12000) n",
            columns.join(", "), values.join(", "),
        )).execute(connection).await?;
        diesel::sql_query("ANALYZE cursor_plan_rows").execute(connection).await?;

        let sorts = [SortParam::new(FilterField::Name, descending), SortParam::new(FilterField::Id, false)];
        let fields = [
            CursorSqlField { column, sql_type: CursorSqlType::String, nullable: false },
            CursorSqlField { column: "id", sql_type: CursorSqlType::Integer, nullable: false },
        ];
        let order = sorts.iter().zip(&fields).map(|(sort, field)| order_sql_clause_for_field(sort, field))
            .collect::<Vec<_>>().join(", ");
        let mut filters = scope.iter().map(|column| format!("{column} = 9")).collect::<Vec<_>>();
        if !partial_predicate.is_empty() {
            filters.push(partial_predicate.to_string());
        }
        let base_filter = if filters.is_empty() { "TRUE".to_string() } else { filters.join(" AND ") };
        let mut options = QueryOptions::new(vec![], sorts.to_vec(), Some(2), None, false).unwrap();
        if !matches!(position, Position::First) {
            let offset = if scope.is_empty() { 11996 } else { 116 };
            let boundary = diesel::sql_query(format!(
                "SELECT id, {column}::text AS value FROM cursor_plan_rows WHERE {base_filter} ORDER BY {order} OFFSET {offset} LIMIT 1",
            )).get_result::<Boundary>(connection).await?;
            let values = vec![CursorValue::String(boundary.value), CursorValue::Integer(i64::from(boundary.id))];
            match position {
                Position::Token => options.set_cursor(Some(encode_cursor_values(&sorts, values).unwrap())).unwrap(),
                Position::Continuation => options.set_continuation(QueryContinuation::new(&sorts, values).unwrap()),
                Position::First => unreachable!(),
            }
        }
        let cursor = cursor_filter_sql_for_fields(&options, &sorts, &fields)?.unwrap_or_else(|| "TRUE".to_string());
        let explain = diesel::sql_query(format!(
            "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF, FORMAT JSON) SELECT id FROM cursor_plan_rows WHERE {base_filter} AND {cursor} ORDER BY {order} LIMIT 2",
        )).get_result::<Explain>(connection).await?;
        assert_bounded_plan(&explain.value, !matches!(position, Position::First));
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
