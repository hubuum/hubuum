//! Read-only, bounded PostgreSQL assessments and evidence-based suggestions.
use diesel::{
    prelude::*,
    sql_types::{Array, BigInt, Bool, Integer, Jsonb, Nullable, Oid, Text},
};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageQueryUsageAnalysis, StorageQueryUsageAnalysisRequest,
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
    #[diesel(sql_type=BigInt)]
    bytes: i64,
    #[diesel(sql_type=Nullable<BigInt>)]
    scans: Option<i64>,
}

pub(crate) fn supported(pattern: &StorageQueryUsagePattern) -> bool {
    pattern.value_type() == StorageQueryUsageValueType::String && pattern.operations() == [StorageQueryUsageOperation::Equals]
        // The current query compiler interprets this unquoted array element as SQL NULL.
        && !pattern.path().segments().any(|segment| segment.eq_ignore_ascii_case("null"))
}

fn expression(pattern: &StorageQueryUsagePattern) -> String {
    format!("((data #>> '{{{}}}'::text[]))", pattern.path().canonical())
}

async fn native_resources(
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
            u64::try_from(row.bytes).unwrap_or(0),
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
        let mut assessments = Vec::new();
        for (declaration, pattern) in declarations.iter().map(|value| (Some(value),value.pattern())).chain(request.proposed().iter().map(|value| (None,value))) {
            let covered = if supported(pattern) { resources.get(&expression(pattern)).cloned().unwrap_or_default() } else { Vec::new() };
            let rationale = if !supported(pattern) { "This PostgreSQL analysis currently supports plain string equality only; other intent remains recorded." } else if !covered.is_empty() { "A valid matching native expression resource already exists. Its observed scan count is backend-wide and does not establish use by this class." } else { "String equality can use a matching text expression resource. Creation has write and storage costs; this read-only assessment does not prepare anything." };
            let mut assessment = StorageQueryUsageAssessment::new(pattern.clone(), pattern.schema_compatibility(class.json_schema.as_ref()), false, rationale).resources(covered);
            if let Some(declaration) = declaration { assessment = assessment.declaration(declaration); }
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
        report.limitation("Resource sizes and valid index coverage are observations; future cost, maintenance overhead and performance benefit are unknown. This build records declarations without native preparation.");
        Ok(report)
    }).await
}
