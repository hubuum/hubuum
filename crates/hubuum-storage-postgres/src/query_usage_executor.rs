//! Privileged, bounded reconciliation of PostgreSQL-owned query usage resources.
//!
//! Every DDL action and ownership update commits in one transaction. Invocation
//! is deliberately separate from request handling and the normal task worker.
use crate::operations::query_usage_analysis::{expression, native_resources, supported};
use crate::{
    DatabaseRoleName, NoopPostgresObserver, PostgresConnection, PostgresPool, PostgresRuntime,
    PostgresStorageError,
};
use chrono::{DateTime, Utc};
use diesel::{
    prelude::*,
    sql_types::{BigInt, Bool, Integer, Jsonb, Nullable, Oid, Text, Timestamptz, Uuid as SqlUuid},
};
use diesel_async::RunQueryDsl;
use hubuum_domain::ClassId;
use hubuum_storage_core::{StorageQueryUsageDeclaration, StorageQueryUsagePattern};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

const MAX_RESOURCES: usize = 8;
const MAX_DECLARATIONS: i64 = 64;
const MAX_SOURCE_BYTES: i64 = 536_870_912;
const EXECUTOR_LOCK: i64 = 7_214_100_041;

#[derive(Default, Debug, Serialize)]
pub struct QueryUsageExecutionReport {
    attempted: usize,
    prepared: usize,
    removed: usize,
    deferred: usize,
    resources: Vec<QueryUsageResourceProgress>,
}
#[derive(Debug, Serialize)]
pub struct QueryUsageResourceProgress {
    reference: String,
    state: String,
    declaration_owners: i64,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}
#[derive(QueryableByName)]
struct Resource {
    #[diesel(sql_type=BigInt)]
    id: i64,
    #[diesel(sql_type=Text)]
    path: String,
    #[diesel(sql_type=SqlUuid)]
    identity: Uuid,
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
}
impl Resource {
    fn name(&self) -> String {
        format!("hubuum_usage_{}", self.id)
    }
    fn marker(&self) -> String {
        format!("hubuum-query-usage:{}", self.identity)
    }
    fn pattern(&self) -> Result<StorageQueryUsagePattern, PostgresStorageError> {
        use hubuum_storage_core::{StorageQueryUsageOperation, StorageQueryUsageValueType};
        StorageQueryUsagePattern::try_new(
            &self.path,
            StorageQueryUsageValueType::String,
            vec![StorageQueryUsageOperation::Equals],
        )
        .map_err(|error| {
            PostgresStorageError::invalid_persisted_value("native query usage path", error)
        })
    }
    fn progress(self) -> QueryUsageResourceProgress {
        QueryUsageResourceProgress {
            reference: format!("postgresql:query_usage:{}", self.id),
            state: self.state,
            declaration_owners: self.owners,
            last_error: self.last_error,
            updated_at: self.updated_at,
        }
    }
}
#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type=Jsonb)]
    value: Value,
}
#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type=BigInt)]
    id: i64,
}
#[derive(QueryableByName)]
struct NativeRow {
    #[diesel(sql_type=Oid)]
    oid: u32,
    #[diesel(sql_type=Bool)]
    matches: bool,
}
#[derive(QueryableByName)]
struct SizeRow {
    #[diesel(sql_type=BigInt)]
    bytes: i64,
}

#[derive(QueryableByName)]
struct MaintenanceRow {
    #[diesel(sql_type=Bool)]
    ready: bool,
}

/// A dedicated executor composed with a privileged connection. Never attached to
/// the application's ordinary StorageHandle or used by the normal task worker.
pub struct QueryUsageExecutor {
    runtime: PostgresRuntime,
    owner: Option<DatabaseRoleName>,
}
impl QueryUsageExecutor {
    pub fn new(pool: PostgresPool, owner: Option<DatabaseRoleName>) -> Self {
        Self {
            runtime: PostgresRuntime::new(pool, Arc::new(NoopPostgresObserver)),
            owner,
        }
    }

    /// One bounded pass. Schedule this command externally for retry and restart
    /// recovery. An optional class restricts planning; cleanup remains global.
    pub async fn reconcile(
        &self,
        class: Option<ClassId>,
    ) -> Result<QueryUsageExecutionReport, PostgresStorageError> {
        let planning_deferred = self.plan(class).await?;
        let ids = self
            .runtime
            .with_transaction::<_, _, PostgresStorageError>(async |connection| {
                self.prepare(connection).await?;
                Ok(
                    diesel::sql_query("SELECT id FROM query_usage_resources ORDER BY id LIMIT 8")
                        .load::<IdRow>(connection)
                        .await?,
                )
            })
            .await?;
        let mut report = QueryUsageExecutionReport {
            deferred: planning_deferred,
            ..QueryUsageExecutionReport::default()
        };
        for id in ids {
            report.attempted += 1;
            match self.apply(id.id).await {
                Ok(Action::Prepared) => report.prepared += 1,
                Ok(Action::Removed) => report.removed += 1,
                Ok(Action::Deferred) => report.deferred += 1,
                Ok(Action::Unchanged) => {}
                Err(_) => {
                    // No raw SQL or values are retained. The transaction rolled
                    // back; a durable generic diagnostic makes retry visible.
                    report.deferred += 1;
                    self.record_failure(id.id).await?;
                }
            }
        }
        report.resources = self
            .runtime
            .with_transaction::<_, _, PostgresStorageError>(async |connection| {
                self.prepare(connection).await?;
                Ok(resources(connection)
                    .await?
                    .into_iter()
                    .map(Resource::progress)
                    .collect())
            })
            .await?;
        Ok(report)
    }

    async fn plan(&self, class: Option<ClassId>) -> Result<usize, PostgresStorageError> {
        self.runtime.with_transaction::<_,_,PostgresStorageError>(async |connection| {
            self.prepare(connection).await?;
            let after = if class.is_none() {
                diesel::sql_query("SELECT after_declaration_id::bigint AS id FROM query_usage_executor_cursor WHERE singleton").get_result::<IdRow>(connection).await?.id as i32
            } else { 0 };
            let mut declarations = candidates(connection,class,after).await?;
            if declarations.is_empty() && after > 0 { declarations = candidates(connection,class,0).await?; }
            let mut next_after = 0;
            let mut deferred = 0;
            let mut existing = resources(connection).await?;
            for row in declarations {
                let declaration = StorageQueryUsageDeclaration::from_snapshot(row.value).map_err(|error| PostgresStorageError::invalid_persisted_value("query usage declaration",error))?;
                next_after = declaration.metadata().id().id();
                if !supported(declaration.pattern()) { continue; }
                let path = declaration.pattern().path().canonical();
                let resource_id = if let Some(resource) = existing.iter().find(|resource| resource.path == path) { resource.id } else {
                    if existing.len() >= MAX_RESOURCES { deferred += 1; continue; }
                    let expr = expression(declaration.pattern());
                    let (coverage, truncated) = native_resources(connection, std::slice::from_ref(&expr)).await?;
                    if truncated { deferred += 1; continue; }
                    if coverage.contains_key(&expr) { continue; }
                    let id = diesel::sql_query("INSERT INTO query_usage_resources(path) VALUES($1) RETURNING id").bind::<Text,_>(path).get_result::<IdRow>(connection).await?.id;
                    existing = resources(connection).await?;
                    id
                };
                let attached = diesel::sql_query("INSERT INTO query_usage_resource_owners(declaration_id,resource_id) SELECT $1,$2 WHERE (SELECT count(*) FROM query_usage_resource_owners WHERE resource_id=$2) < 128 ON CONFLICT(declaration_id) DO NOTHING").bind::<Integer,_>(declaration.metadata().id().id()).bind::<BigInt,_>(resource_id).execute(connection).await?;
                if attached == 0 { deferred += 1; }
                diesel::sql_query("UPDATE query_usage_resources SET state=CASE WHEN index_oid IS NULL THEN 'pending' ELSE 'ready' END, updated_at=clock_timestamp() WHERE id=$1").bind::<BigInt,_>(resource_id).execute(connection).await?;
            }
            if class.is_none() {
                diesel::sql_query("UPDATE query_usage_executor_cursor SET after_declaration_id=$1 WHERE singleton").bind::<Integer,_>(next_after).execute(connection).await?;
            }
            Ok(deferred)
        }).await
    }

    async fn prepare(
        &self,
        connection: &mut PostgresConnection,
    ) -> Result<(), PostgresStorageError> {
        if let Some(owner) = &self.owner {
            diesel::sql_query("SELECT set_config('role',$1,true)")
                .bind::<Text, _>(owner.as_str())
                .execute(connection)
                .await?;
        }
        diesel::sql_query("SET LOCAL lock_timeout='100ms'")
            .execute(connection)
            .await?;
        diesel::sql_query("SET LOCAL statement_timeout='2s'")
            .execute(connection)
            .await?;
        let maintenance = diesel::sql_query(
            "SELECT state='normal' AS ready FROM system_maintenance WHERE id=1 FOR SHARE",
        )
        .get_result::<MaintenanceRow>(connection)
        .await?;
        if !maintenance.ready {
            return Err(PostgresStorageError::conflict(
                "Query usage reconciliation is deferred while restoration is active",
            ));
        }
        diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
            .bind::<BigInt, _>(EXECUTOR_LOCK)
            .execute(connection)
            .await?;
        Ok(())
    }

    async fn apply(&self, id: i64) -> Result<Action, PostgresStorageError> {
        self.runtime.with_transaction::<_,_,PostgresStorageError>(async |connection| {
            self.prepare(connection).await?;
            // Lock live declaration rows before the resource, matching withdrawal
            // order. Re-read ownership after waiting; queued work has no authority.
            diesel::sql_query("SELECT d.id::bigint AS id FROM query_usage_declarations d JOIN query_usage_resource_owners o ON o.declaration_id=d.id WHERE o.resource_id=$1 ORDER BY d.id LIMIT 128 FOR UPDATE OF d")
                .bind::<BigInt,_>(id).load::<IdRow>(connection).await?;
            let row = diesel::sql_query("SELECT r.*, (SELECT count(*) FROM query_usage_resource_owners o WHERE o.resource_id=r.id) AS owners FROM query_usage_resources r WHERE r.id=$1 FOR UPDATE").bind::<BigInt,_>(id).get_result::<Resource>(connection).await.optional()?;
            let Some(resource) = row else { return Ok(Action::Unchanged); };
            let native = native_identity(connection,&resource).await?;
            if resource.owners == 0 {
                if let Some(native) = native {
                    if !native.matches || Some(native.oid) != resource.index_oid {
                        diagnostic(connection,id,"identity_mismatch").await?;
                        return Ok(Action::Deferred);
                    }
                    diesel::sql_query(format!("DROP INDEX public.\"{}\"",resource.name())).execute(connection).await?;
                }
                diesel::sql_query("DELETE FROM query_usage_resources WHERE id=$1").bind::<BigInt,_>(id).execute(connection).await?;
                return Ok(Action::Removed);
            }
            if let Some(native) = native {
                if native.matches && Some(native.oid) == resource.index_oid {
                    diesel::sql_query("UPDATE query_usage_resources SET state='ready',last_error=NULL WHERE id=$1").bind::<BigInt,_>(id).execute(connection).await?;
                    return Ok(Action::Unchanged);
                }
                diagnostic(connection,id,"identity_mismatch").await?;
                return Ok(Action::Deferred);
            }
            let pattern = resource.pattern()?;
            let expr = expression(&pattern);
            let (coverage,truncated) = native_resources(connection,std::slice::from_ref(&expr)).await?;
            if truncated { diagnostic(connection,id,"catalog_budget_exceeded").await?; return Ok(Action::Deferred); }
            if coverage.contains_key(&expr) {
                // An independently managed resource now covers this intent.
                // Discard only our allocation record, never that native resource.
                diesel::sql_query("DELETE FROM query_usage_resources WHERE id=$1").bind::<BigInt,_>(id).execute(connection).await?;
                return Ok(Action::Removed);
            }
            let size = diesel::sql_query("SELECT pg_table_size('public.hubuumobject'::regclass) AS bytes").get_result::<SizeRow>(connection).await?;
            if size.bytes > MAX_SOURCE_BYTES { diagnostic(connection,id,"source_budget_exceeded").await?; return Ok(Action::Deferred); }
            // Hash entries have bounded key storage even for arbitrarily long
            // text. Unlike a btree expression index, they cannot reject future
            // valid object writes because a text key exceeds a page-size limit.
            diesel::sql_query(format!("CREATE INDEX \"{}\" ON public.hubuumobject USING hash ((data #>> '{{{}}}'))",resource.name(),pattern.path().canonical())).execute(connection).await?;
            diesel::sql_query(format!("COMMENT ON INDEX public.\"{}\" IS '{}'",resource.name(),resource.marker())).execute(connection).await?;
            let native = native_identity(connection,&resource).await?.ok_or_else(|| PostgresStorageError::database("Created query usage index is missing"))?;
            diesel::sql_query("UPDATE query_usage_resources SET index_oid=$2,state='ready',last_error=NULL,updated_at=clock_timestamp() WHERE id=$1").bind::<BigInt,_>(id).bind::<Oid,_>(native.oid).execute(connection).await?;
            Ok(Action::Prepared)
        }).await
    }

    async fn record_failure(&self, id: i64) -> Result<(), PostgresStorageError> {
        self.runtime
            .with_transaction::<_, _, PostgresStorageError>(async |connection| {
                self.prepare(connection).await?;
                diagnostic(connection, id, "native_operation_failed").await
            })
            .await
    }
}
#[derive(Clone, Copy)]
enum Action {
    Prepared,
    Removed,
    Deferred,
    Unchanged,
}
async fn resources(
    connection: &mut PostgresConnection,
) -> Result<Vec<Resource>, PostgresStorageError> {
    Ok(diesel::sql_query("SELECT r.*, (SELECT count(*) FROM query_usage_resource_owners o WHERE o.resource_id=r.id) AS owners FROM query_usage_resources r ORDER BY id LIMIT 8").load::<Resource>(connection).await?)
}
async fn diagnostic(
    connection: &mut PostgresConnection,
    id: i64,
    reason: &str,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query(
        "UPDATE query_usage_resources SET last_error=$2,updated_at=clock_timestamp() WHERE id=$1",
    )
    .bind::<BigInt, _>(id)
    .bind::<Text, _>(reason)
    .execute(connection)
    .await?;
    Ok(())
}
async fn native_identity(
    connection: &mut PostgresConnection,
    resource: &Resource,
) -> Result<Option<NativeRow>, PostgresStorageError> {
    Ok(diesel::sql_query(r#"SELECT idx.oid,
        COALESCE((obj_description(idx.oid,'pg_class')=$2 AND idx.relname=$1 AND ns.nspname='public' AND i.indrelid='public.hubuumobject'::regclass
        AND pg_get_indexdef(idx.oid,1,false)=$3 AND am.amname='hash'
        AND i.indisvalid AND i.indisready AND i.indnkeyatts=1 AND i.indpred IS NULL),false) AS matches
        FROM pg_class idx JOIN pg_namespace ns ON ns.oid=idx.relnamespace
        JOIN pg_index i ON i.indexrelid=idx.oid JOIN pg_am am ON am.oid=idx.relam
        WHERE (ns.nspname='public' AND idx.relname=$1) OR idx.oid=$4 ORDER BY idx.oid"#)
        .bind::<Text,_>(resource.name()).bind::<Text,_>(resource.marker()).bind::<Text,_>(expression(&resource.pattern()?)).bind::<Nullable<Oid>,_>(resource.index_oid).load::<NativeRow>(connection).await?
        .into_iter().reduce(|mut first,_| { first.matches=false; first }))
}

async fn candidates(
    connection: &mut PostgresConnection,
    class: Option<ClassId>,
    after: i32,
) -> Result<Vec<JsonRow>, PostgresStorageError> {
    Ok(diesel::sql_query("SELECT to_jsonb(d) AS value FROM query_usage_declarations d WHERE ($1::integer IS NULL OR class_id=$1) AND d.id>$3 AND pattern->>'value_type'='string' AND pattern->'operations'='[\"equals\"]'::jsonb AND NOT EXISTS (SELECT 1 FROM query_usage_resource_owners o WHERE o.declaration_id=d.id) ORDER BY id LIMIT $2 FOR UPDATE")
        .bind::<Nullable<Integer>,_>(class.map(ClassId::id)).bind::<BigInt,_>(MAX_DECLARATIONS).bind::<Integer,_>(after).load::<JsonRow>(connection).await?)
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;
