use super::*;
use crate::{test_support::integration_test_pool, with_transaction};
use diesel::sql_types::Json;
use rstest::rstest;

const ENDPOINT_IDENTITY: &str = "11111111-1111-4111-8111-111111111111";

async fn assign_endpoint_identities(
    connection: &mut crate::PostgresConnection,
    table: &str,
) -> Result<(), PostgresStorageError> {
    sql_query(format!("UPDATE {table} SET logical_identity = jsonb_build_object('subject', jsonb_build_object('kind', 'resource', 'identity', jsonb_build_object('state', 'known', 'identity', $1::text), 'version', gen_random_uuid()), 'references', '{{}}'::jsonb)"))
        .bind::<Text, _>(ENDPOINT_IDENTITY).execute(connection).await?;
    Ok(())
}

#[derive(QueryableByName)]
struct Explain {
    #[diesel(sql_type = Json, column_name = "QUERY PLAN")]
    plan: Value,
}

#[tokio::test]
async fn relationship_validity_keeps_index_in_generic_plans() {
    use diesel::pg::Pg;
    use diesel::query_builder::{AstPass, QueryFragment};
    use diesel::result::QueryResult;
    use diesel_async::SimpleAsyncConnection;

    diesel::table! {
        relation_validity_fixture (id) {
            id -> Integer,
            op -> Text,
            valid_from -> Timestamptz,
            valid_to -> Nullable<Timestamptz>,
        }
    }

    #[derive(diesel::QueryId)]
    struct PrepareValidityLookup<Q>(Q);
    impl<Q: QueryFragment<Pg>> QueryFragment<Pg> for PrepareValidityLookup<Q> {
        fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            out.push_sql("PREPARE relation_validity_lookup(TIMESTAMPTZ) AS ");
            self.0.walk_ast(out.reborrow())
        }
    }

    let pool = integration_test_pool(1);
    let plan = with_transaction(&pool, async |connection| {
        connection.batch_execute(
            "CREATE TEMP TABLE relation_validity_fixture
               (id INTEGER PRIMARY KEY, op TEXT NOT NULL, valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ) ON COMMIT DROP;
             INSERT INTO relation_validity_fixture
               SELECT n, 'U', '2000-01-01'::TIMESTAMPTZ + n * INTERVAL '1 day',
                 '2000-01-01'::TIMESTAMPTZ + (n + 1) * INTERVAL '1 day'
               FROM generate_series(1,20000) n;
             CREATE INDEX relation_validity_fixture_validity
               ON relation_validity_fixture USING gist (tstzrange(valid_from, valid_to, '[)'))
               WHERE op <> 'D';
             ANALYZE relation_validity_fixture;
             SET LOCAL plan_cache_mode = force_generic_plan;"
        ).await?;
        // Exercise the production Diesel predicate, retaining the requested
        // timestamp as a server parameter to prevent a custom plan hiding an
        // expression-index mismatch.
        let query = relation_validity_fixture::table
            .filter(relation_valid_at!(relation_validity_fixture, sql::<Timestamptz>("$1")))
            .count();
        PrepareValidityLookup(query).execute(connection).await?;
        let plan = sql_query(
            "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE relation_validity_lookup('2001-01-01 00:00:00+00')"
        ).get_result::<Explain>(connection).await?.plan;
        connection.batch_execute("DEALLOCATE relation_validity_lookup").await?;
        Ok::<_, PostgresStorageError>(plan)
    }).await.unwrap();
    let mut nodes = Vec::new();
    scans(&plan[0]["Plan"], &mut nodes);
    let index = nodes
        .iter()
        .find(|node| node["Index Name"] == "relation_validity_fixture_validity")
        .expect("the generic count plan must use the validity index");
    assert_eq!(index["Actual Rows"], 1, "{plan:#}");
    assert!(
        index["Index Cond"].as_str().unwrap().contains("$1"),
        "{plan:#}"
    );
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

async fn relation_page_fixture(
    connection: &mut crate::PostgresConnection,
    tables: &RelationTables,
    expired: Option<(i64, i64)>,
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::Uuid as SqlUuid;
    use uuid::Uuid;
    let objects = tables.family == "object_relation";
    sql_query(format!(
        "CREATE TEMP TABLE {} (LIKE public.{} INCLUDING INDEXES) ON COMMIT DROP",
        tables.edges, tables.edges
    ))
    .execute(connection)
    .await?;
    let extra_column = if objects { ", class_relation_id" } else { "" };
    let extra_value = if objects { ", 1" } else { "" };
    sql_query(format!("INSERT INTO {} (id,{}, {},created_at,updated_at,op,valid_from,valid_to,history_id,revision,logical_identity{extra_column})
            SELECT n,7,n+100,'2000-01-01'::timestamp,'2000-01-01'::timestamp,'U','2000-01-01'::timestamptz,'2000-01-03'::timestamptz,n,1,
            jsonb_build_object('subject',jsonb_build_object('kind','resource','identity',jsonb_build_object('state','known','identity',gen_random_uuid()),'version',gen_random_uuid()),'references','{{}}'::jsonb){extra_value}
            FROM generate_series(1,6)n",tables.edges,tables.from,tables.to)).execute(connection).await?;
    if let Some((first, last)) = expired {
        sql_query(format!("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to)
            SELECT $1,(logical_identity #>> '{{subject,identity,identity}}')::UUID,(logical_identity #>> '{{subject,version}}')::UUID,$2,valid_from,valid_to
            FROM {} WHERE history_id BETWEEN $3 AND $4",tables.edges))
            .bind::<Text,_>(tables.family).bind::<SqlUuid,_>(Uuid::new_v4())
            .bind::<BigInt,_>(first)
            .bind::<BigInt,_>(last)
            .execute(connection).await?;
    }
    Ok(())
}

#[rstest]
#[case::older(true, vec![4, 3])]
#[case::newer(false, vec![3, 4])]
#[tokio::test]
async fn relationship_history_filters_expired_candidates_before_page_limit(
    #[values(false, true)] objects: bool,
    #[case] descending: bool,
    #[case] expected: Vec<i64>,
) {
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    };
    use diesel_async::SimpleAsyncConnection;
    let resource = if objects {
        StorageRelationHistoryResource::Object(ObjectId::new(7).unwrap())
    } else {
        StorageRelationHistoryResource::Class(ClassId::new(7).unwrap())
    };
    let (tables, root) = RelationTables::for_resource(resource);
    let pool = integration_test_migration_pool(1);
    let rows = with_transaction(&pool, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection)
                .await?;
        }
        connection
            .batch_execute("SAVEPOINT relationship_visibility_fixture")
            .await?;
        // Predicate fixture only; no protected intent or native removal.
        relation_page_fixture(
            connection,
            &tables,
            Some(if descending { (5, 6) } else { (1, 2) }),
        )
        .await?;
        let at = DateTime::parse_from_rfc3339("2000-01-02T00:00:00Z")
            .unwrap()
            .to_utc();
        let selected = sql_query(tables.page_sql(descending, true))
            .bind::<Integer, _>(root)
            .bind::<Nullable<Timestamptz>, _>(Some(at))
            .bind::<Nullable<BigInt>, _>(Some(if descending { 7i64 } else { 0i64 }))
            .bind::<BigInt, _>(2i64)
            .load::<RelationRow>(connection)
            .await;
        // Roll back exact membership and temporary rows before assertions,
        // including a rejected query, so parallel fixtures retain no state.
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT relationship_visibility_fixture")
            .await?;
        Ok::<_, PostgresStorageError>(
            selected?
                .into_iter()
                .map(|row| row.metadata.history_id)
                .collect::<Vec<_>>(),
        )
    })
    .await
    .unwrap();
    assert_eq!(rows, expected);
}

#[rstest]
#[case::retained(true)]
#[case::clear(false)]
#[tokio::test]
async fn relationship_history_empty_page_preserves_as_of_gap(
    #[values(false, true)] objects: bool,
    #[case] retained: bool,
) {
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    };
    use diesel_async::SimpleAsyncConnection;
    let resource = if objects {
        StorageRelationHistoryResource::Object(ObjectId::new(7).unwrap())
    } else {
        StorageRelationHistoryResource::Class(ClassId::new(7).unwrap())
    };
    let (tables, root) = RelationTables::for_resource(resource);
    let pool = integration_test_migration_pool(1);
    let rows = with_transaction(&pool, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection)
                .await?;
        }
        connection
            .batch_execute("SAVEPOINT relationship_gap_fixture")
            .await?;
        relation_page_fixture(connection, &tables, retained.then_some((1, 2))).await?;
        let at = DateTime::parse_from_rfc3339("2000-01-02T00:00:00Z")
            .unwrap()
            .to_utc();
        let selected = sql_query(tables.page_with_gap_sql(false, true))
            .bind::<Integer, _>(root)
            .bind::<Nullable<Timestamptz>, _>(Some(at))
            .bind::<Nullable<BigInt>, _>(Some(i64::MAX))
            .bind::<BigInt, _>(2i64)
            .load::<RelationPageRow>(connection)
            .await;
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT relationship_gap_fixture")
            .await?;
        Ok::<_, PostgresStorageError>(selected?)
    })
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "Empty pages must retain one complete gap verdict"
    );
    assert_eq!(rows[0].history_expired, retained);
    assert!(
        rows.iter()
            .all(|row| !row.has_record && row.record.is_none())
    );
}

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[tokio::test]
async fn relationship_history_partial_page_is_not_an_empty_page(#[case] objects: bool) {
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    };
    use diesel_async::SimpleAsyncConnection;
    use hubuum_storage_core::StorageErrorKind;
    let resource = if objects {
        StorageRelationHistoryResource::Object(ObjectId::new(7).unwrap())
    } else {
        StorageRelationHistoryResource::Class(ClassId::new(7).unwrap())
    };
    let (tables, root) = RelationTables::for_resource(resource);
    let pool = integration_test_migration_pool(1);
    let rows = with_transaction(&pool, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection)
                .await?;
        }
        connection
            .batch_execute("SAVEPOINT relationship_partial_fixture")
            .await?;
        relation_page_fixture(connection, &tables, None).await?;
        // Corrupt only the temporary selected-row projection: required payload
        // fields may not silently become an absent left-join record.
        sql_query(format!(
            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
            tables.edges, tables.to
        ))
        .execute(connection)
        .await?;
        sql_query(format!(
            "UPDATE {} SET {}=NULL WHERE history_id=6",
            tables.edges, tables.to
        ))
        .execute(connection)
        .await?;
        let at = DateTime::parse_from_rfc3339("2000-01-02T00:00:00Z")
            .unwrap()
            .to_utc();
        let selected = sql_query(tables.page_with_gap_sql(true, true))
            .bind::<Integer, _>(root)
            .bind::<Nullable<Timestamptz>, _>(Some(at))
            .bind::<Nullable<BigInt>, _>(Some(7i64))
            .bind::<BigInt, _>(1i64)
            .load::<RelationPageRow>(connection)
            .await;
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT relationship_partial_fixture")
            .await?;
        Ok::<_, PostgresStorageError>(selected?)
    })
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(row) = rows.into_iter().next() else {
        unreachable!()
    };
    assert!(matches!(row.into_record(),Err(error) if error.kind()==StorageErrorKind::Backend));
}

#[rstest]
#[case::missing(43, Some(ENDPOINT_IDENTITY))]
#[case::unresolved(42, None)]
#[case::reused_integer(42, Some("22222222-2222-4222-8222-222222222222"))]
#[tokio::test]
async fn endpoint_batch_omits_unavailable_history_without_live_fallback(
    #[case] unavailable_id: i32,
    #[case] unavailable_identity: Option<&str>,
) {
    let pool = integration_test_pool(1);
    with_transaction(&pool, async |connection| {
        sql_query("CREATE TEMP TABLE hubuumclass_history (LIKE public.hubuumclass_history INCLUDING INDEXES) ON COMMIT DROP").execute(connection).await?;
        sql_query("INSERT INTO hubuumclass_history (id, name, collection_id, validate_schema, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision)
            VALUES (42, 'old', 1, false, '', '2026-01-01', '2026-01-01', 'I', '2026-01-01', '2026-01-02', 1, 1),
                   (42, 'new', 2, false, '', '2026-01-02', '2026-01-02', 'U', '2026-01-02', NULL, 2, 2)").execute(connection).await?;
        assign_endpoint_identities(connection, "hubuumclass_history").await?;
        let (tables, _) = RelationTables::for_resource(StorageRelationHistoryResource::Class(ClassId::new(42).unwrap()));
        let times = ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z"].into_iter().map(|value| DateTime::parse_from_rfc3339(value).unwrap().to_utc()).collect::<Vec<_>>();
        let rows = sql_query(tables.endpoint_sql()).bind::<Array<Integer>, _>(vec![42, 42, unavailable_id]).bind::<Array<Timestamptz>, _>(times).bind::<Array<Nullable<Text>>, _>(vec![Some(ENDPOINT_IDENTITY), Some(ENDPOINT_IDENTITY), unavailable_identity]).load::<EndpointRow>(connection).await?;
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
        assign_endpoint_identities(connection, tables.endpoints).await?;
        sql_query(format!("ANALYZE {}", tables.endpoints)).execute(connection).await?;
        let at = DateTime::parse_from_rfc3339("2026-01-01T01:40:00.5Z").unwrap().to_utc();
        let rows = sql_query(tables.endpoint_sql()).bind::<Array<Integer>, _>(vec![42]).bind::<Array<Timestamptz>, _>(vec![at]).bind::<Array<Nullable<Text>>, _>(vec![Some(ENDPOINT_IDENTITY)]).load::<EndpointRow>(connection).await?;
        assert_eq!(rows.into_iter().map(|row| row.metadata.history_id).collect::<Vec<_>>(), [6000]);
        let plan = sql_query(format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", tables.endpoint_sql())).bind::<Array<Integer>, _>(vec![42]).bind::<Array<Timestamptz>, _>(vec![at]).bind::<Array<Nullable<Text>>, _>(vec![Some(ENDPOINT_IDENTITY)]).get_result::<Explain>(connection).await?.plan;
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

#[rstest]
#[case::class(false)]
#[case::object(true)]
#[tokio::test]
async fn endpoint_expiry_preserves_native_selection(#[case] objects: bool) {
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    };
    use diesel::sql_types::Uuid as SqlUuid;
    use hubuum_domain::{IdentityReference, ResourceIdentity, VersionIdentity};
    use hubuum_storage_core::StorageIdentitySubject;
    use uuid::Uuid;

    let pool = integration_test_migration_pool(1);
    let resource = if objects {
        StorageRelationHistoryResource::Object(ObjectId::new(42).unwrap())
    } else {
        StorageRelationHistoryResource::Class(ClassId::new(42).unwrap())
    };
    let (tables, _) = RelationTables::for_resource(resource);
    let identity = ResourceIdentity::generate();
    let versions = [
        VersionIdentity::generate(),
        VersionIdentity::generate(),
        VersionIdentity::generate(),
    ];
    let intent = Uuid::new_v4();
    with_transaction(&pool, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        sql_query(format!("CREATE TEMP TABLE {} (LIKE public.{} INCLUDING INDEXES) ON COMMIT DROP", tables.endpoints, tables.endpoints)).execute(connection).await?;
        let (columns, values) = if objects { ("hubuum_class_id, data", "1, '{}'::jsonb") } else { ("validate_schema", "false") };
        for (index, version) in versions.into_iter().enumerate() {
            let evidence = StorageIdentityEvidence::try_new(
                Some(StorageIdentitySubject::Resource { identity: IdentityReference::Known { identity }, version }),
                Default::default(),
            ).unwrap();
            sql_query(format!("INSERT INTO {} (id,name,collection_id,description,created_at,updated_at,op,valid_from,valid_to,history_id,revision,logical_identity,{columns})
                VALUES (42,'selected',1,'','2000-01-01','2000-01-01','U',
                '2000-01-01'::timestamptz + $1 * interval '1 day',
                CASE WHEN $1=2 THEN NULL ELSE '2000-01-01'::timestamptz + ($1+1) * interval '1 day' END,$1+1,$1+1,$2,{values})", tables.endpoints))
                .bind::<Integer, _>(index as i32)
                .bind::<Jsonb, _>(serde_json::to_value(evidence).unwrap()).execute(connection).await?;
        }
        // Owner-only predicate fixture. This grants no executable expiry authority.
        sql_query("INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to) VALUES($1,$2,$3,$4,'2000-01-02','2000-01-03')")
            .bind::<Text, _>(tables.endpoint_family)
            .bind::<SqlUuid, _>(Uuid::from_bytes(*identity.as_bytes()))
            .bind::<SqlUuid, _>(Uuid::from_bytes(*versions[1].as_bytes()))
            .bind::<SqlUuid, _>(intent).execute(connection).await?;
        let at = ["2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z", "2000-01-03T00:00:00Z"]
            .into_iter().map(|value| DateTime::parse_from_rfc3339(value).unwrap().to_utc()).collect::<Vec<_>>();
        let rows = sql_query(tables.endpoint_sql())
            .bind::<Array<Integer>, _>(vec![42; 3])
            .bind::<Array<Timestamptz>, _>(at)
            .bind::<Array<Nullable<Text>>, _>(vec![Some(identity.to_string()); 3])
            .load::<EndpointRow>(connection).await?;
        assert_eq!(rows.iter().map(|row| row.metadata.history_id).collect::<Vec<_>>(), [1, 2, 3]);
        assert_eq!(rows.into_iter().map(|row| row.project().is_ok()).collect::<Vec<_>>(), [true, false, true]);
        sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
            .bind::<SqlUuid, _>(intent).execute(connection).await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
}
