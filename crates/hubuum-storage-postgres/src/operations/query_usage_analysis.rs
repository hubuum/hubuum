//! Read-only, bounded PostgreSQL assessments and evidence-based suggestions.
use chrono::{DateTime, Utc};
use diesel::{
    prelude::*,
    sql_types::{Array, BigInt, Bool, Integer, Jsonb, Nullable, Oid, Text, Timestamptz},
};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageQueryUsageAdapterProgress, StorageQueryUsageAnalysis, StorageQueryUsageAnalysisRequest,
    StorageQueryUsageAnalysisStatus as Status, StorageQueryUsageAssessment,
    StorageQueryUsageDeclaration, StorageQueryUsageOperation, StorageQueryUsagePattern,
    StorageQueryUsageResource, StorageQueryUsageSuggestion, StorageQueryUsageValueType,
};
use serde_json::Value;
use std::collections::BTreeMap;

use super::class::ClassRow;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

const CATALOG_LIMIT: i64 = 128;
const MIN_SAMPLED_QUERIES: u64 = 5;

#[derive(QueryableByName)]
struct DeclarationRow {
    #[diesel(sql_type=Jsonb)]
    value: Value,
}

#[derive(QueryableByName)]
struct IndexRow {
    #[diesel(sql_type=Oid)]
    oid: u32,
    #[diesel(sql_type=Nullable<Integer>)]
    pattern: Option<i32>,
    #[diesel(sql_type=Bool)]
    usable: bool,
    #[diesel(sql_type=Nullable<BigInt>)]
    bytes: Option<i64>,
    #[diesel(sql_type=Nullable<BigInt>)]
    scans: Option<i64>,
}

pub(crate) fn supported(pattern: &StorageQueryUsagePattern) -> bool {
    pattern.value_type() == StorageQueryUsageValueType::String && pattern.operations() == [StorageQueryUsageOperation::Equals]
        // The current query compiler interprets this unquoted array element as SQL NULL.
        && !pattern.path().segments().any(|segment| segment.eq_ignore_ascii_case("null"))
}

pub(crate) fn expression(pattern: &StorageQueryUsagePattern) -> String {
    format!("((data #>> '{{{}}}'::text[]))", pattern.path().canonical())
}

pub(crate) async fn native_resources(
    connection: &mut PostgresConnection,
    patterns: &[String],
) -> Result<(BTreeMap<String, Vec<StorageQueryUsageResource>>, bool), PostgresStorageError> {
    // The LIMIT applies before expression inspection. Neither arbitrary native
    // SQL nor constants from independently managed expressions cross this boundary.
    let rows = diesel::sql_query(
        r#"
        WITH candidates AS MATERIALIZED (
            SELECT * FROM pg_index WHERE indrelid='public.hubuumobject'::regclass
            ORDER BY indexrelid LIMIT $2
        )
        SELECT i.indexrelid AS oid,
            array_position($1::text[], pg_get_indexdef(i.indexrelid,1,false)) AS pattern,
            (i.indisvalid AND i.indisready AND i.indislive AND i.indpred IS NULL
             AND i.indnkeyatts=1 AND am.amname IN ('hash','btree')
             AND op.opcname='text_ops' AND ns.nspname='pg_catalog'
             AND i.indcollation[0]='pg_catalog."default"'::regcollation) AS usable,
            pg_relation_size(i.indexrelid) AS bytes, stats.idx_scan AS scans
        FROM candidates i JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_am am ON am.oid=idx.relam
        JOIN pg_opclass op ON op.oid=i.indclass[0]
        JOIN pg_namespace ns ON ns.oid=op.opcnamespace
        LEFT JOIN pg_stat_all_indexes stats ON stats.indexrelid=i.indexrelid
        ORDER BY i.indexrelid
    "#,
    )
    .bind::<Array<Text>, _>(patterns)
    .bind::<BigInt, _>(CATALOG_LIMIT + 1)
    .load::<IndexRow>(connection)
    .await?;
    let truncated = rows.len() > CATALOG_LIMIT as usize;
    let mut resources = BTreeMap::<String, Vec<StorageQueryUsageResource>>::new();
    for row in rows
        .into_iter()
        .take(CATALOG_LIMIT as usize)
        .filter(|row| row.usable)
    {
        // Catalog snapshots can retain an index after concurrent DROP INDEX.
        // Missing live metadata is not evidence of usable native coverage.
        let Some(bytes) = row.bytes else {
            continue;
        };
        let Some(index) = row
            .pattern
            .and_then(|index| usize::try_from(index - 1).ok())
        else {
            continue;
        };
        let Some(pattern) = patterns.get(index) else {
            return Err(PostgresStorageError::database(
                "Native resource pattern reference is invalid",
            ));
        };
        let resource = StorageQueryUsageResource::external(
            format!("postgresql:{}", row.oid),
            u64::try_from(bytes).unwrap_or(0),
            row.scans.and_then(|value| u64::try_from(value).ok()),
        );
        resources.entry(pattern.clone()).or_default().push(resource);
    }
    Ok((resources, truncated))
}

pub(crate) async fn analyze(
    runtime: &PostgresRuntime,
    request: StorageQueryUsageAnalysisRequest,
) -> Result<StorageQueryUsageAnalysis, PostgresStorageError> {
    runtime.with_read_only_snapshot::<_, _, PostgresStorageError>(async move |connection| {
        diesel::sql_query("SET LOCAL statement_timeout = '2s'").execute(connection).await?;
        use crate::schema::hubuumclass::dsl as classes;
        let class = classes::hubuumclass.filter(classes::id.eq(request.scope().class_id().id())).filter(classes::collection_id.eq(request.scope().authorized_collection().id())).select(ClassRow::as_select()).first::<ClassRow>(connection).await?;
        let declarations = diesel::sql_query("SELECT to_jsonb(d) AS value FROM query_usage_declarations d WHERE class_id=$1 ORDER BY id").bind::<Integer,_>(class.id).load::<DeclarationRow>(connection).await?.into_iter().map(|row| StorageQueryUsageDeclaration::from_snapshot(row.value).map_err(|error| PostgresStorageError::invalid_persisted_value("query usage declaration", error))).collect::<Result<Vec<_>,_>>()?;
        let mut expressions = declarations.iter().map(|value| value.pattern()).chain(request.proposed()).chain(request.observations().patterns().iter().map(|value| value.pattern())).filter(|pattern| supported(pattern)).map(expression).collect::<Vec<_>>();
        expressions.sort(); expressions.dedup();
        let (resources, truncated) = native_resources(connection, &expressions).await?;
        let evidence = request.observations().settings().enabled() && request.observations().patterns().iter().any(|value| supported(value.pattern()) && value.sampled_queries() >= MIN_SAMPLED_QUERIES);
        let status = if evidence && !truncated { Status::Complete } else { Status::InsufficientEvidence };
        let managed = managed_resources(connection, class.id).await?;
        let mut assessments = Vec::new();
        for (declaration, pattern) in declarations.iter().map(|value| (Some(value),value.pattern())).chain(request.proposed().iter().map(|value| (None,value))) {
            let mut covered = if supported(pattern) { resources.get(&expression(pattern)).cloned().unwrap_or_default() } else { Vec::new() };
            let managed = managed.iter().find(|resource| supported(pattern) && resource.path == pattern.path().canonical());
            if let Some(managed) = managed {
                for resource in &mut covered {
                    if managed.verified && managed.index_oid.is_some_and(|oid| resource.reference() == format!("postgresql:{oid}")) {
                        let last_owner = managed.owners == 1 && declaration.is_some_and(|declaration| managed.class_owners.contains(&declaration.metadata().id().id()));
                        *resource = resource.clone().with_declaration_ownership(managed.owners as u64, last_owner);
                    }
                }
            }
            let rationale = if !supported(pattern) { "This PostgreSQL analysis currently supports plain string equality only; other intent remains recorded." } else if !covered.is_empty() { "A valid matching native expression resource already exists. Its observed scan count is backend-wide and does not establish use by this class." } else { "String equality can use a matching text expression resource. Creation has write and storage costs; this read-only assessment does not prepare anything." };
            let mut assessment = StorageQueryUsageAssessment::new(pattern.clone(), pattern.schema_compatibility(class.json_schema.as_ref()), supported(pattern), rationale).resources(covered);
            if let Some(declaration) = declaration { assessment = assessment.declaration(declaration); }
            if let Some(managed) = managed { assessment = assessment.adapter_progress(managed.progress()); }
            assessments.push(assessment);
        }
        let mut candidates = request.observations().patterns().iter().filter(|observed| supported(observed.pattern()) && observed.sampled_queries() >= MIN_SAMPLED_QUERIES && !declarations.iter().any(|declaration| declaration.pattern() == observed.pattern()) && !resources.contains_key(&expression(observed.pattern()))).cloned().collect::<Vec<_>>();
        candidates.sort_by(|a,b| b.sampled_queries().cmp(&a.sampled_queries()).then_with(|| a.pattern().path().canonical().cmp(b.pattern().path().canonical())));
        let mut report = StorageQueryUsageAnalysis::new(status, request.into_observations());
        for assessment in assessments { report.assessment(assessment); }
        if !truncated {
            for observed in candidates.into_iter().take(32) {
                report.suggestion(StorageQueryUsageSuggestion::new(observed.pattern().clone(), vec![observed], "At least five sampled successful requests used this undeclared text equality pattern, and no matching native resource was found within a complete bounded catalog review. Consider recording this intent; benefit is unknown."));
            }
        } else { report.limitation("Native catalog inspection reached its 128-index budget; absence of coverage is unknown, and suggestions are withheld."); }
        report.limitation("Native counters are cumulative and can be reset independently of application observations. They cover all classes using a shared resource, not a single declaration.");
        report.limitation("Only direct single-class scalar filters are observed. Related, computed, structured-only, negated, case-insensitive and unattributable patterns are omitted.");
        report.limitation("Resource sizes and valid index coverage are observations; future cost, maintenance overhead and performance benefit are unknown. Native preparation requires a separately scheduled privileged executor; support does not establish that it is running or that allocation budgets permit this declaration.");
        Ok(report)
    }).await
}

#[derive(QueryableByName)]
struct ManagedResource {
    #[diesel(sql_type=BigInt)]
    id: i64,
    #[diesel(sql_type=Text)]
    path: String,
    #[diesel(sql_type=Nullable<Oid>)]
    index_oid: Option<u32>,
    #[diesel(sql_type=Text)]
    state: String,
    #[diesel(sql_type=Nullable<Text>)]
    last_error: Option<String>,
    #[diesel(sql_type=Timestamptz)]
    updated_at: DateTime<Utc>,
    #[diesel(sql_type=BigInt)]
    owners: i64,
    #[diesel(sql_type=Array<Integer>)]
    class_owners: Vec<i32>,
    #[diesel(sql_type=Bool)]
    verified: bool,
}
impl ManagedResource {
    fn progress(&self) -> StorageQueryUsageAdapterProgress {
        StorageQueryUsageAdapterProgress::new(
            format!("postgresql:query_usage:{}", self.id),
            self.state.clone(),
            self.owners as u64,
            self.last_error.clone(),
            self.updated_at,
        )
    }
}
async fn managed_resources(
    connection: &mut PostgresConnection,
    class: i32,
) -> Result<Vec<ManagedResource>, PostgresStorageError> {
    Ok(diesel::sql_query("SELECT r.*,
        (SELECT count(*) FROM query_usage_resource_owners o WHERE o.resource_id=r.id) AS owners,
        ARRAY(SELECT o.declaration_id FROM query_usage_resource_owners o JOIN query_usage_declarations d ON d.id=o.declaration_id WHERE o.resource_id=r.id AND d.class_id=$1 ORDER BY o.declaration_id LIMIT 32) AS class_owners,
        EXISTS(SELECT 1 FROM pg_class idx JOIN pg_namespace ns ON ns.oid=idx.relnamespace WHERE idx.oid=r.index_oid AND ns.nspname='public' AND idx.relname='hubuum_usage_' || r.id::text AND obj_description(idx.oid,'pg_class')='hubuum-query-usage:' || r.identity::text) AS verified
        FROM query_usage_resources r ORDER BY id LIMIT 8")
        .bind::<Integer,_>(class).load::<ManagedResource>(connection).await?)
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests {
    use super::*;
    use crate::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    };
    use diesel_async::SimpleAsyncConnection;
    use rstest::rstest;
    use uuid::Uuid;

    #[derive(QueryableByName)]
    struct CatalogState {
        #[diesel(sql_type = Oid)]
        oid: u32,
        #[diesel(sql_type = Nullable<BigInt>)]
        bytes: Option<i64>,
        #[diesel(sql_type = Nullable<Text>)]
        definition: Option<String>,
    }

    #[rstest]
    #[case::retained(false)]
    #[case::disappeared(true)]
    #[tokio::test]
    async fn native_resources_handles_index_disappearance(#[case] disappear: bool) {
        let pool = integration_test_migration_pool(2);
        let mut reader = pool.get().await.unwrap();
        let mut writer = pool.get().await.unwrap();
        if database_role_tests_enabled() {
            let roles = integration_test_database_roles();
            let role = format!("SET ROLE \"{}\"", roles.owner().as_str());
            reader.batch_execute(&role).await.unwrap();
            writer.batch_execute(&role).await.unwrap();
        }
        let name = format!("catalog_race_{}", Uuid::new_v4().simple());
        let path = format!("catalog_path_{}", Uuid::new_v4().simple());
        let pattern = format!("((data #>> '{{{path}}}'::text[]))");
        writer.batch_execute(&format!(
            "CREATE INDEX {name} ON public.hubuumobject USING btree ((data #>> '{{{path}}}'::text[]))"
        )).await.unwrap();

        // Read only catalog rows to establish the snapshot without holding an
        // index relation lock that would prevent the second connection's DROP.
        reader
            .batch_execute(
                "BEGIN ISOLATION LEVEL REPEATABLE READ; SET LOCAL statement_timeout = '5s'",
            )
            .await
            .unwrap();
        let oid = diesel::sql_query("SELECT indexrelid AS oid, NULL::bigint AS bytes, NULL::text AS definition FROM pg_index WHERE indexrelid=to_regclass($1)")
            .bind::<Text, _>(format!("public.{name}"))
            .get_result::<CatalogState>(&mut reader).await.unwrap().oid;
        if disappear {
            writer
                .batch_execute(&format!(
                    "SET statement_timeout = '5s'; DROP INDEX public.{name}"
                ))
                .await
                .unwrap();
        }
        let state = diesel::sql_query("SELECT indexrelid AS oid, pg_relation_size(indexrelid) AS bytes, pg_get_indexdef(indexrelid,1,false) AS definition FROM pg_index WHERE indexrelid=$1")
            .bind::<Oid, _>(oid).get_result::<CatalogState>(&mut reader).await.unwrap();
        let result = native_resources(&mut reader, std::slice::from_ref(&pattern)).await;
        reader.batch_execute("ROLLBACK").await.unwrap();
        writer
            .batch_execute(&format!("DROP INDEX IF EXISTS public.{name}"))
            .await
            .unwrap();

        // The catalog row survives either way; live metadata only survives when
        // the index does. Assert after cleanup so a regression leaves no index.
        assert_eq!(state.oid, oid);
        assert_eq!(state.bytes.is_none(), disappear);
        assert_eq!(state.definition.is_none(), disappear);
        let (resources, truncated) = result.unwrap();
        assert!(!truncated);
        if disappear {
            assert!(resources.is_empty());
        } else {
            let resources = resources.get(&pattern).unwrap();
            assert_eq!(resources.len(), 1);
            assert_eq!(resources[0].reference(), format!("postgresql:{oid}"));
            let resource = serde_json::to_value(&resources[0]).unwrap();
            assert_eq!(resource["bytes"], state.bytes.unwrap());
            assert_eq!(state.definition.as_deref(), Some(pattern.as_str()));
        }
    }
}
