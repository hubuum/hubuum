//! Bounded observation of a fixed synthetic cohort. Never returns matching keys,
//! fingerprints, request bodies, output payloads, or replacement integer owners.

use std::collections::BTreeSet;
use std::time::Instant;

use diesel::QueryableByName;
use diesel::sql_types::{Array, BigInt, Bool, Integer, Nullable, Text, Uuid as SqlUuid};
use diesel_async::RunQueryDsl;
use hubuum_domain::{TaskId, TaskIdentity};
use hubuum_scale_core::{Result, invalid_data};
use serde::Serialize;
use uuid::Uuid;

use super::PostgresScaleBackend;
use crate::{PostgresStorageError, with_connection};

#[derive(Clone, Debug, Serialize)]
pub struct PostgresTaskRetentionObservation {
    identity: TaskIdentity,
    task_id: Option<TaskId>,
    terminal_at_ms: Option<i64>,
    receipt_expires_at_ms: Option<i64>,
    details_expires_at_ms: Option<i64>,
    output_expires_at_ms: Option<i64>,
    receipt_present: bool,
    child_rows: u64,
    output_rows: u64,
    details_removed: Option<bool>,
    outputs_removed: Option<bool>,
}
impl PostgresTaskRetentionObservation {
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub const fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }
    pub const fn terminal_at_ms(&self) -> Option<i64> {
        self.terminal_at_ms
    }
    pub const fn receipt_expires_at_ms(&self) -> Option<i64> {
        self.receipt_expires_at_ms
    }
    pub const fn details_expires_at_ms(&self) -> Option<i64> {
        self.details_expires_at_ms
    }
    /// Latest deadline of the physical expiring outputs still present. Preserve
    /// an observation from before cleanup when measuring the subsequent lag.
    pub const fn output_expires_at_ms(&self) -> Option<i64> {
        self.output_expires_at_ms
    }
    pub const fn receipt_present(&self) -> bool {
        self.receipt_present
    }
    pub const fn child_rows(&self) -> u64 {
        self.child_rows
    }
    pub const fn output_rows(&self) -> u64 {
        self.output_rows
    }
    pub const fn details_removed(&self) -> Option<bool> {
        self.details_removed
    }
    pub const fn outputs_removed(&self) -> Option<bool> {
        self.outputs_removed
    }
}

#[derive(QueryableByName)]
struct IdentityRow {
    #[diesel(sql_type=Integer)]
    id: i32,
    #[diesel(sql_type=Nullable<Text>)]
    identity: Option<String>,
}
#[derive(QueryableByName)]
struct ObservationRow {
    #[diesel(sql_type=Text)]
    identity: String,
    #[diesel(sql_type=Nullable<Integer>)]
    task_id: Option<i32>,
    #[diesel(sql_type=Nullable<BigInt>)]
    terminal_at_ms: Option<i64>,
    #[diesel(sql_type=Nullable<BigInt>)]
    receipt_expires_at_ms: Option<i64>,
    #[diesel(sql_type=Nullable<BigInt>)]
    details_expires_at_ms: Option<i64>,
    #[diesel(sql_type=Nullable<BigInt>)]
    output_expires_at_ms: Option<i64>,
    #[diesel(sql_type=Bool)]
    receipt_present: bool,
    #[diesel(sql_type=BigInt)]
    child_rows: i64,
    #[diesel(sql_type=BigInt)]
    output_rows: i64,
    #[diesel(sql_type=Nullable<Bool>)]
    details_removed: Option<bool>,
    #[diesel(sql_type=Nullable<Bool>)]
    outputs_removed: Option<bool>,
}
impl TryFrom<ObservationRow> for PostgresTaskRetentionObservation {
    type Error = hubuum_scale_core::Error;
    fn try_from(row: ObservationRow) -> Result<Self> {
        Ok(Self {
            identity: row.identity.parse()?,
            task_id: row.task_id.map(TaskId::new).transpose()?,
            terminal_at_ms: row.terminal_at_ms,
            receipt_expires_at_ms: row.receipt_expires_at_ms,
            details_expires_at_ms: row.details_expires_at_ms,
            output_expires_at_ms: row.output_expires_at_ms,
            receipt_present: row.receipt_present,
            child_rows: u64::try_from(row.child_rows)?,
            output_rows: u64::try_from(row.output_rows)?,
            details_removed: row.details_removed,
            outputs_removed: row.outputs_removed,
        })
    }
}

fn validate_cohort<T: Ord + Copy>(values: &[T]) -> Result<()> {
    if values.is_empty()
        || values.len() > 1000
        || values.iter().copied().collect::<BTreeSet<_>>().len() != values.len()
    {
        return Err(invalid_data(
            "Task retention observation requires 1..=1000 unique cohort identities",
        ));
    }
    Ok(())
}

impl PostgresScaleBackend {
    /// Ten SELECT 1 round trips on one acquired connection, after an excluded
    /// warmup. Includes transport and server execution, but not pool acquisition.
    pub async fn task_retention_round_trip_nanos(&self) -> Result<Vec<u64>> {
        Ok(with_connection(
            &self.pool,
            async |connection| -> std::result::Result<Vec<u64>, PostgresStorageError> {
                diesel::select(diesel::dsl::sql::<Integer>("1"))
                    .get_result::<i32>(connection)
                    .await?;
                let mut samples = Vec::with_capacity(10);
                for _ in 0..10 {
                    let started = Instant::now();
                    diesel::select(diesel::dsl::sql::<Integer>("1"))
                        .get_result::<i32>(connection)
                        .await?;
                    samples.push(u64::try_from(started.elapsed().as_nanos()).map_err(|_| {
                        PostgresStorageError::database("Database round trip exceeded timer range")
                    })?);
                }
                Ok(samples)
            },
        )
        .await?)
    }

    /// Resolve immediately after initial admission, before cleanup or restore.
    /// Callers retain the returned UUIDs for every subsequent observation.
    pub async fn resolve_task_retention_cohort(
        &self,
        ids: &[TaskId],
    ) -> Result<Vec<(TaskId, TaskIdentity)>> {
        validate_cohort(ids)?;
        let raw = ids.iter().map(|id| id.id()).collect::<Vec<_>>();
        let rows=with_connection(&self.pool,async move |connection| {
            diesel::sql_query("SELECT t.id,t.logical_identity #>> '{subject,identity,identity}' AS identity FROM unnest($1::integer[]) WITH ORDINALITY wanted(id,position) JOIN public.tasks t ON t.id=wanted.id ORDER BY wanted.position")
                .bind::<Array<Integer>,_>(raw).load::<IdentityRow>(connection).await.map_err(PostgresStorageError::from)
        }).await?;
        if rows.len() != ids.len() {
            return Err(invalid_data(
                "Initial task cohort mapping is incomplete; never resolve replacement integers after cleanup",
            ));
        }
        rows.into_iter()
            .map(|row| {
                Ok((
                    TaskId::new(row.id)?,
                    row.identity
                        .ok_or_else(|| invalid_data("Task cohort logical identity is unresolved"))?
                        .parse()?,
                ))
            })
            .collect()
    }

    /// One statement observes all clocks and physical child presence at the
    /// same snapshot, including tasks already physically absent.
    pub async fn observe_task_retention(
        &self,
        identities: &[TaskIdentity],
    ) -> Result<Vec<PostgresTaskRetentionObservation>> {
        validate_cohort(identities)?;
        let raw = identities
            .iter()
            .map(|identity| Uuid::from_bytes(*identity.as_bytes()))
            .collect::<Vec<_>>();
        let rows=with_connection(&self.pool,async move |connection| {
            diesel::sql_query("SELECT wanted.identity::text AS identity,t.id AS task_id, \
                (extract(epoch FROM COALESCE((r.material #>> '{outcome,finished_at}')::timestamptz,(r.material #>> '{outcome,settled_at}')::timestamptz,t.finished_at AT TIME ZONE 'UTC'))*1000)::bigint AS terminal_at_ms, \
                (extract(epoch FROM r.expires_at)*1000)::bigint AS receipt_expires_at_ms, \
                (extract(epoch FROM COALESCE(t.detail_expires_at AT TIME ZONE 'UTC',COALESCE((r.material #>> '{outcome,finished_at}')::timestamptz,(r.material #>> '{outcome,settled_at}')::timestamptz)+make_interval(secs=>(r.material->>'details_after_terminal_secs')::double precision)))*1000)::bigint AS details_expires_at_ms, \
                (extract(epoch FROM GREATEST((SELECT output_expires_at FROM public.export_task_outputs WHERE task_id=t.id),(SELECT output_expires_at FROM public.backup_task_outputs WHERE task_id=t.id),(SELECT output_expires_at FROM public.schema_profile_outputs WHERE task_id=t.id)) AT TIME ZONE 'UTC')*1000)::bigint AS output_expires_at_ms, \
                r.task_identity IS NOT NULL AS receipt_present, \
                ((SELECT count(*) FROM public.import_task_results WHERE task_id=t.id)+(SELECT count(*) FROM public.remote_call_results WHERE task_id=t.id)+(SELECT count(*) FROM public.schema_validation_work WHERE task_id=t.id)+(SELECT count(*) FROM public.schema_impact_findings WHERE task_id=t.id))::bigint AS child_rows, \
                ((SELECT count(*) FROM public.export_task_outputs WHERE task_id=t.id)+(SELECT count(*) FROM public.backup_task_outputs WHERE task_id=t.id)+(SELECT count(*) FROM public.schema_profile_outputs WHERE task_id=t.id)+(SELECT count(*) FROM public.schema_repair_reports WHERE task_id=t.id))::bigint AS output_rows, \
                CASE WHEN details.id IS NOT NULL THEN details.material->>'removed' IS NOT NULL END AS details_removed, \
                CASE WHEN outputs.id IS NOT NULL THEN outputs.material->>'removed' IS NOT NULL END AS outputs_removed \
                FROM unnest($1::uuid[]) WITH ORDINALITY wanted(identity,position) \
                LEFT JOIN public.tasks t ON t.logical_identity #>> '{subject,identity,identity}'=wanted.identity::text \
                LEFT JOIN hubuum_control.task_receipts r ON r.task_identity=wanted.identity \
                LEFT JOIN hubuum_control.task_purge_targets dp ON dp.task_identity=wanted.identity AND dp.scope='task_details' \
                LEFT JOIN hubuum_control.task_purge_batches details ON details.id=dp.batch_id \
                LEFT JOIN hubuum_control.task_purge_targets op ON op.task_identity=wanted.identity AND op.scope='expired_outputs' \
                LEFT JOIN hubuum_control.task_purge_batches outputs ON outputs.id=op.batch_id ORDER BY wanted.position")
                .bind::<Array<SqlUuid>,_>(raw).load::<ObservationRow>(connection).await.map_err(PostgresStorageError::from)
        }).await?;
        if rows.len() != identities.len() {
            return Err(invalid_data("Task UUID cohort is ambiguous"));
        }
        rows.into_iter()
            .map(PostgresTaskRetentionObservation::try_from)
            .collect()
    }
}
