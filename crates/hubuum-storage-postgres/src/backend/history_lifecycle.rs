use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Bool, Integer, Jsonb, Nullable, Timestamptz, Uuid as SqlUuid};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use futures_util::TryStreamExt;
use hubuum_storage_core::*;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::PostgresStorage;
use crate::{PostgresConnection, PostgresStorageError};

#[derive(QueryableByName)]
struct MaterialRow {
    #[diesel(sql_type = Jsonb)]
    material: Value,
}
#[derive(QueryableByName)]
struct CommittedRow {
    #[diesel(sql_type = Bool)]
    committed: bool,
}
#[derive(QueryableByName)]
struct HistoryRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    logical_identity: Option<Value>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    anchor: Option<DateTime<Utc>>,
    #[diesel(sql_type = Bool)]
    deleted: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capture {
    observed_at: DateTime<Utc>,
    control: HistoryLifecycleControl,
    incarnation: Uuid,
    deletion_revision: HistoryDeletionRevision,
}
async fn capture_on(
    connection: &mut PostgresConnection,
) -> Result<HistoryLifecycleSnapshot, PostgresStorageError> {
    let row = sql_query("SELECT public.hubuum_history_lifecycle_capture() AS material")
        .get_result::<MaterialRow>(connection)
        .await?;
    let capture: Capture = serde_json::from_value(row.material).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("history lifecycle control", error)
    })?;
    HistoryLifecycleSnapshot::new(
        capture.control,
        capture.incarnation,
        capture.deletion_revision,
        capture.observed_at,
    )
    .map_err(Into::into)
}

#[async_trait]
impl HistoryLifecycleStorage for PostgresStorage {
    async fn register_history_runtime(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        let registration = RuntimeRegistration::generate();
        self.runtime()
            .with_control_transaction(async move |connection| {
                let result = sql_query(
                    "SELECT public.hubuum_register_history_runtime($1,$2,$3) AS committed",
                )
                .bind::<SqlUuid, _>(registration.id())
                .bind::<SqlUuid, _>(registration.ownership_token())
                .bind::<Nullable<Jsonb>, _>(
                    isolation
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|_| {
                            PostgresStorageError::invalid_input("Cannot encode runtime isolation")
                        })?,
                )
                .get_result::<CommittedRow>(connection)
                .await?;
                if !result.committed {
                    return Err(PostgresStorageError::unavailable(
                        "Application quarantine or runtime capacity prevents history-aware startup",
                    ));
                }
                let authority =
                    sql_query("SELECT public.hubuum_deletion_control_read() AS material")
                        .get_result::<MaterialRow>(connection)
                        .await?;
                serde_json::from_value::<DeletionControlState>(authority.material).map_err(
                    |error| {
                        PostgresStorageError::invalid_persisted_value("deletion authority", error)
                    },
                )?;
                capture_on(connection).await?;
                Ok::<_, PostgresStorageError>(registration)
            })
            .await
            .map_err(Into::into)
    }
    async fn scan_history_lifecycle(
        &self,
        scan: HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError> {
        self.runtime().with_connection(async move |connection| {
            let family = scan.key().family();
            let (table, columns) = match family {
                StorageIdentityHistoryKind::Collection => ("collections_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::Class => ("hubuumclass_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::Object => ("hubuumobject_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::ClassRelation => ("hubuumclass_relation_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::ObjectRelation => ("hubuumobject_relation_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::ExportTemplate => ("export_templates_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::RemoteTarget => ("remote_targets_history", "CASE WHEN lower(op)='delete' THEN valid_from ELSE valid_to END AS anchor, lower(op)='delete' AS deleted"),
                StorageIdentityHistoryKind::ComputedDefinitions => ("computed_field_history", "valid_to AS anchor, FALSE AS deleted"),
                StorageIdentityHistoryKind::ClassSchema => ("class_schema_history", "occurred_at AS anchor, FALSE AS deleted"),
            };
            // Both identifiers are selected from the exhaustive static family map.
            let order = if family == StorageIdentityHistoryKind::ClassSchema { "id" } else { "history_id" };
            let query = format!("SELECT logical_identity, {columns} FROM public.{table} ORDER BY {order} LIMIT $1");
            let rows = sql_query(query).bind::<Integer,_>(scan.limit() as i32).load_stream::<HistoryRow>(connection).await?;
            futures_util::pin_mut!(rows);
            let mut candidates = Vec::new();
            let mut scanned = 0;
            let mut unresolved = 0;
            let mut input_bytes = 0;
            let mut exhausted = false;
            while let Some(row) = rows.try_next().await? {
                let Some(value) = row.logical_identity else { scanned += 1; unresolved += 1; continue; };
                input_bytes += serde_json::to_vec(&value).map_err(|_| PostgresStorageError::database("Cannot size historical identity"))?.len();
                if input_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES { exhausted = true; break; }
                scanned += 1;
                let evidence: StorageIdentityEvidence = serde_json::from_value(value).map_err(|error| PostgresStorageError::invalid_persisted_value("historical identity", error))?;
                let anchor = row.anchor.map(|at| if family == StorageIdentityHistoryKind::ClassSchema { HistoryRetentionAnchor::SchemaEvent(at) } else if row.deleted { HistoryRetentionAnchor::Deleted(at) } else { HistoryRetentionAnchor::Closed(at) });
                match HistoryExpiryCandidate::from_evidence(family, &evidence, anchor, false)? {
                    Some(candidate) if scan.includes(&candidate) => candidates.push(candidate),
                    Some(_) => {},
                    None => unresolved += 1,
                }
            }
            HistoryLifecycleScanResult::new(candidates, scanned, !exhausted && scanned < scan.limit(), unresolved).map_err(PostgresStorageError::from)
        }).await.map_err(Into::into)
    }

    async fn recover_history_lifecycle(
        &self,
        recovery: HistoryLifecycleRecovery,
    ) -> Result<(), StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                sql_query("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE")
                    .execute(connection)
                    .await?;
                let exposed =
                    sql_query("SELECT public.hubuum_deletion_exposure_allowed() AS committed")
                        .get_result::<CommittedRow>(connection)
                        .await?
                        .committed;
                if exposed {
                    return Err(PostgresStorageError::conflict(
                        "History authority recovery requires quarantine",
                    ));
                }
                let current = capture_on(connection).await?;
                if current.control() != recovery.expected() || current.fence() != recovery.fence() {
                    return Err(PostgresStorageError::conflict(
                        "History authority changed during recovery",
                    ));
                }
                let material = serde_json::to_value(recovery.recovered()).map_err(|_| {
                    PostgresStorageError::invalid_input("Cannot encode recovered history authority")
                })?;
                sql_query(
                    "UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1",
                )
                .bind::<Jsonb, _>(material)
                .execute(connection)
                .await?;
                sql_query("SELECT public.hubuum_history_expiry_sync()")
                    .execute(connection)
                    .await?;
                for event in recovery
                    .recovered()
                    .authority()
                    .audit_events_since(&HistoryLifecycleAuthority::empty())?
                {
                    crate::operations::event_record::append_retained_lifecycle_event(
                        connection, &event,
                    )
                    .await?;
                }
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(Into::into)
    }
    async fn capture_history_lifecycle(&self) -> Result<HistoryLifecycleSnapshot, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| capture_on(connection).await)
            .await
            .map_err(Into::into)
    }
    async fn commit_history_lifecycle(
        &self,
        change: HistoryLifecycleControlChange,
    ) -> Result<HistoryLifecycleSnapshot, StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                for scope in change.scopes_to_validate() {
                    if !change.expected().retains_scope(scope) {
                        let (current, history, identity) = match scope {
                            HistoryPolicyScope::Deployment => continue,
                            HistoryPolicyScope::Collection { identity } => ("collections", "collections_history", identity),
                            HistoryPolicyScope::Class { identity } => ("hubuumclass", "hubuumclass_history", identity),
                        };
                        let query = format!("SELECT EXISTS(SELECT 1 FROM public.{current} WHERE (logical_identity #>> '{{subject,identity,identity}}')::uuid=$1 UNION ALL SELECT 1 FROM public.{history} WHERE (logical_identity #>> '{{subject,identity,identity}}')::uuid=$1) AS committed");
                        let exists = sql_query(query).bind::<SqlUuid,_>(Uuid::from_bytes(*identity.as_bytes())).get_result::<CommittedRow>(connection).await?.committed;
                        if !exists { return Err(PostgresStorageError::not_found("History policy scope was not found")); }
                    }
                }
                let expected = serde_json::to_value(change.expected()).map_err(|_| {
                    PostgresStorageError::invalid_input(
                        "Cannot encode history lifecycle predecessor",
                    )
                })?;
                let replacement = serde_json::to_value(change.next()).map_err(|_| {
                    PostgresStorageError::invalid_input(
                        "Cannot encode history lifecycle transition",
                    )
                })?;
                let committed = sql_query(
                    "SELECT public.hubuum_history_lifecycle_commit($1,$2,$3,$4) AS committed",
                )
                .bind::<Jsonb, _>(expected)
                .bind::<Jsonb, _>(replacement)
                .bind::<SqlUuid, _>(change.fence().incarnation())
                .bind::<BigInt, _>(change.fence().deletion_revision().value() as i64)
                .get_result::<CommittedRow>(connection)
                .await?
                .committed;
                if !committed {
                    return Err(PostgresStorageError::conflict(
                        "History policy authority or application incarnation changed",
                    ));
                }
                for event in change.next().audit_events_since(change.expected())? {
                    crate::operations::event_record::append_retained_lifecycle_event(
                        connection, &event,
                    )
                    .await?;
                }
                capture_on(connection).await
            })
            .await
            .map_err(Into::into)
    }
}
