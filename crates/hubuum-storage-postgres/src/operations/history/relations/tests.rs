use super::*;
use crate::{test_support::integration_test_pool, with_transaction};
use diesel::sql_types::Json;
use rstest::rstest;

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json, column_name = "QUERY PLAN")]
    plan: Value,
}

fn scans<'a>(node: &'a Value, found: &mut Vec<&'a Value>) {
    if node["Node Type"].as_str().is_some_and(|kind| {
        matches!(
            kind,
            "Index Scan"
                | "Index Only Scan"
                | "Seq Scan"
                | "Bitmap Heap Scan"
                | "Bitmap Index Scan"
        )
    }) {
        found.push(node);
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans(child, found);
    }
}

#[rstest]
#[case::older(true, vec![5999, 5998])]
#[case::newer(false, vec![6001, 6002])]
#[tokio::test]
async fn relationship_history_seeks_each_endpoint_index(
    #[values(false, true)] objects: bool,
    #[case] descending: bool,
    #[case] expected: Vec<i64>,
) {
    let pool = integration_test_pool(1);
    let resource = if objects {
        StorageRelationHistoryResource::Object(ObjectId::new(7).unwrap())
    } else {
        StorageRelationHistoryResource::Class(ClassId::new(7).unwrap())
    };
    let (tables, root) = RelationTables::for_resource(resource);
    with_transaction(&pool, async |connection| {
        sql_query(format!("CREATE TEMP TABLE {} (LIKE public.{} INCLUDING INDEXES) ON COMMIT DROP", tables.edges, tables.edges)).execute(connection).await?;
        let extra_column = if objects { ", class_relation_id" } else { "" };
        let extra_value = if objects { ", 1" } else { "" };
        sql_query(format!("INSERT INTO {} (id, {}, {}, created_at, updated_at, op, valid_from, history_id, revision{extra_column})
            SELECT n, CASE WHEN n <= 6000 THEN 7 ELSE 1 END, CASE WHEN n <= 6000 THEN n + 8 ELSE 7 END,
            '2026-01-01'::timestamp, '2026-01-01'::timestamp, 'I', '2026-01-01'::timestamptz, n, 1{extra_value}
            FROM generate_series(1, 12000) n", tables.edges, tables.from, tables.to)).execute(connection).await?;
        sql_query(format!("ANALYZE {}", tables.edges)).execute(connection).await?;
        let at = DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z").unwrap().to_utc();
        let rows = sql_query(tables.page_sql(descending, true)).bind::<Integer, _>(root).bind::<Nullable<Timestamptz>, _>(Some(at)).bind::<Nullable<BigInt>, _>(Some(6000i64)).bind::<BigInt, _>(2i64).load::<RelationRow>(connection).await?;
        assert_eq!(rows.iter().map(|row| row.metadata.history_id).collect::<Vec<_>>(), expected);
        let plan = sql_query(format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", tables.page_sql(descending, true)))
            .bind::<Integer, _>(root).bind::<Nullable<Timestamptz>, _>(Some(at)).bind::<Nullable<BigInt>, _>(Some(6000i64)).bind::<BigInt, _>(2i64).get_result::<Explain>(connection).await?.plan;
        let mut found = Vec::new();
        scans(&plan[0]["Plan"], &mut found);
        assert_eq!(found.len(), 2, "{plan:#}");
        for scan in found {
            assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
            assert!(scan["Index Cond"].as_str().unwrap().contains("history_id"), "{plan:#}");
            let condition = scan["Index Cond"].as_str().unwrap();
            assert!(condition.contains("from_hubuum_") || condition.contains("to_hubuum_"), "{plan:#}");
            assert!(scan["Actual Rows"].as_f64().unwrap() <= 2.0, "{plan:#}");
            assert!(scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0) <= 2.0, "{plan:#}");
        }
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn endpoint_batch_omits_unavailable_history_without_live_fallback() {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        sql_query("CREATE TEMP TABLE hubuumclass_history (LIKE public.hubuumclass_history INCLUDING INDEXES) ON COMMIT DROP").execute(connection).await?;
        sql_query("INSERT INTO hubuumclass_history (id, name, collection_id, validate_schema, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision)
            VALUES (42, 'old', 1, false, '', '2026-01-01', '2026-01-01', 'I', '2026-01-01', '2026-01-02', 1, 1),
                   (42, 'new', 2, false, '', '2026-01-02', '2026-01-02', 'U', '2026-01-02', NULL, 2, 2)").execute(connection).await?;
        let (tables, _) = RelationTables::for_resource(StorageRelationHistoryResource::Class(ClassId::new(42).unwrap()));
        let times = ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z"].into_iter().map(|value| DateTime::parse_from_rfc3339(value).unwrap().to_utc()).collect::<Vec<_>>();
        let rows = sql_query(tables.endpoint_sql()).bind::<Array<Integer>, _>(vec![42, 42, 43]).bind::<Array<Timestamptz>, _>(times).load::<EndpointRow>(connection).await?;
        assert_eq!(rows.into_iter().map(|row| (row.ordinal, row.name, row.collection_id)).collect::<Vec<_>>(), [(1, "old".into(), 1), (2, "new".into(), 2)]);
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[tokio::test]
async fn endpoint_hydration_seeks_the_selected_version_in_long_history(#[case] objects: bool) {
    let pool = integration_test_pool(1);
    let resource = if objects {
        StorageRelationHistoryResource::Object(ObjectId::new(42).unwrap())
    } else {
        StorageRelationHistoryResource::Class(ClassId::new(42).unwrap())
    };
    let (tables, _) = RelationTables::for_resource(resource);
    with_transaction(&pool, async |connection| {
        sql_query(format!("CREATE TEMP TABLE {} (LIKE public.{} INCLUDING INDEXES) ON COMMIT DROP", tables.endpoints, tables.endpoints)).execute(connection).await?;
        let (columns, values) = if objects { ("hubuum_class_id, data", "1, '{}'::jsonb") } else { ("validate_schema", "false") };
        sql_query(format!("INSERT INTO {} (id, name, collection_id, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision, {columns})
            SELECT 42, 'version-' || n, 1, '', '2026-01-01'::timestamp, '2026-01-01'::timestamp, 'U',
            '2026-01-01T00:00:00Z'::timestamptz + n * interval '1 second',
            '2026-01-01T00:00:00Z'::timestamptz + (n+1) * interval '1 second', n, n, {values}
            FROM generate_series(1, 12000) n", tables.endpoints)).execute(connection).await?;
        sql_query(format!("ANALYZE {}", tables.endpoints)).execute(connection).await?;
        let at = DateTime::parse_from_rfc3339("2026-01-01T01:40:00.5Z").unwrap().to_utc();
        let rows = sql_query(tables.endpoint_sql()).bind::<Array<Integer>, _>(vec![42]).bind::<Array<Timestamptz>, _>(vec![at]).load::<EndpointRow>(connection).await?;
        assert_eq!(rows.into_iter().map(|row| row.metadata.history_id).collect::<Vec<_>>(), [6000]);
        let plan = sql_query(format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", tables.endpoint_sql())).bind::<Array<Integer>, _>(vec![42]).bind::<Array<Timestamptz>, _>(vec![at]).get_result::<Explain>(connection).await?.plan;
        let mut found = Vec::new();
        scans(&plan[0]["Plan"], &mut found);
        assert_eq!(found.len(), 1, "{plan:#}");
        let scan = found[0];
        assert_eq!(scan["Node Type"], "Index Scan", "{plan:#}");
        assert!(scan["Index Cond"].as_str().unwrap().contains("valid_from"), "{plan:#}");
        assert_eq!(scan["Actual Rows"].as_f64().unwrap(), 1.0, "{plan:#}");
        assert!(scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0) <= 1.0, "{plan:#}");
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
