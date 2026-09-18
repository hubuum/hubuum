//! Bounded projections: never deserialize schema checkpoints or assemble reports.
use crate::{PostgresConnection, PostgresStorageError};
use diesel::sql_types::{Array, Bool, Integer, Nullable, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{StorageTask, StorageTaskDiscoveryState};

#[derive(QueryableByName)]
struct DiscoveryRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Text>)]
    work_kind: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    work_status: Option<String>,
    #[diesel(sql_type = Bool)]
    output_present: bool,
}

pub(super) async fn enrich(
    connection: &mut PostgresConnection,
    tasks: &mut [StorageTask],
) -> Result<(), PostgresStorageError> {
    if tasks.is_empty() {
        return Ok(());
    }
    let rows = sql_query(
        "SELECT t.id, w.kind AS work_kind, w.checkpoint->>'status' AS work_status,
        (EXISTS (SELECT 1 FROM export_task_outputs e WHERE e.task_id = t.id)
         OR EXISTS (SELECT 1 FROM backup_task_outputs b WHERE b.task_id = t.id)) AS output_present
        FROM tasks t LEFT JOIN schema_validation_work w ON w.task_id = t.id WHERE t.id = ANY($1)",
    )
    .bind::<Array<Integer>, _>(tasks.iter().map(|t| t.id().id()).collect::<Vec<_>>())
    .load::<DiscoveryRow>(connection)
    .await?;
    let mut states = std::collections::HashMap::new();
    for row in rows {
        let work = match (row.work_kind, row.work_status) {
            (Some(kind), Some(status)) => Some((
                serde_json::from_value(serde_json::Value::String(kind)).map_err(|e| {
                    PostgresStorageError::invalid_persisted_value("schema work kind", e)
                })?,
                serde_json::from_value(serde_json::Value::String(status)).map_err(|e| {
                    PostgresStorageError::invalid_persisted_value("schema work status", e)
                })?,
            )),
            (None, None) => None,
            _ => {
                return Err(PostgresStorageError::database(
                    "Incomplete retained schema work discovery",
                ));
            }
        };
        states.insert(
            row.id,
            StorageTaskDiscoveryState::new(work, row.output_present),
        );
    }
    for task in tasks {
        if let Some(state) = states.remove(&task.id().id()) {
            task.set_discovery_state(state);
        }
    }
    Ok(())
}

/// Expressions are a closed adapter-owned allow-list; values are always bound.
pub(super) fn predicate_expression(
    predicate: &hubuum_storage_core::TaskDiscoveryPredicate,
) -> Option<(&'static str, String)> {
    use hubuum_storage_core::TaskDiscoveryPredicate as P;
    fn wire(value: impl serde::Serialize) -> String {
        let value = serde_json::to_value(value).expect("scalar discovery value");
        value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string())
    }
    Some(match predicate {
        P::Class(v) => (
            "COALESCE(tasks.discovery_metadata->'data'->>'class_id', tasks.discovery_metadata->'data'->'target'->>'class_id')",
            v.id().to_string(),
        ),
        P::Object(v) => (
            "tasks.discovery_metadata->'data'->'target'->>'object_id'",
            v.id().to_string(),
        ),
        P::Collection(v) => (
            "tasks.discovery_metadata->'data'->'target'->>'collection_id'",
            v.id().to_string(),
        ),
        P::ClassRelation(v) => (
            "(tasks.discovery_metadata->'data'->'target'->>'type') || ':' || (tasks.discovery_metadata->'data'->'target'->>'relation_id')",
            format!("class_relation:{}", v.id()),
        ),
        P::ObjectRelation(v) => (
            "(tasks.discovery_metadata->'data'->'target'->>'type') || ':' || (tasks.discovery_metadata->'data'->'target'->>'relation_id')",
            format!("object_relation:{}", v.id()),
        ),
        P::SchemaRevision(v) => (
            "tasks.discovery_metadata->'data'->>'schema_revision'",
            wire(v),
        ),
        P::SchemaWorkKind(v) => (
            "(SELECT w.kind FROM schema_validation_work w WHERE w.task_id = tasks.id)",
            wire(v),
        ),
        P::SchemaWorkStatus(v) => (
            "(SELECT w.checkpoint->>'status' FROM schema_validation_work w WHERE w.task_id = tasks.id)",
            wire(v),
        ),
        P::ComputationRevision(v) => (
            "tasks.discovery_metadata->'data'->>'computation_revision'",
            wire(v),
        ),
        P::RemoteTarget(v) => (
            "tasks.discovery_metadata->'data'->>'remote_target_id'",
            v.id().to_string(),
        ),
        P::RemoteSideEffect(v) => (
            "CASE WHEN tasks.remote_dispatched_at IS NOT NULL THEN 'possibly_sent' WHEN tasks.execution_deadline_at IS NOT NULL OR (tasks.started_at IS NULL AND tasks.attempt_count = 0) THEN 'not_sent' ELSE 'legacy_unknown' END",
            match v {
                hubuum_storage_core::TaskRemoteSideEffectState::NotSent => "not_sent",
                hubuum_storage_core::TaskRemoteSideEffectState::PossiblySent => "possibly_sent",
                hubuum_storage_core::TaskRemoteSideEffectState::LegacyUnknown => "legacy_unknown",
            }
            .into(),
        ),
        P::ExportScope(v) => ("tasks.discovery_metadata->'data'->>'scope_kind'", wire(v)),
        P::ExportTemplate(v) => (
            "tasks.discovery_metadata->'data'->>'template_id'",
            v.id().to_string(),
        ),
        P::ExportHasWarnings(v) => (
            "((tasks.discovery_metadata->'data'->>'warning_count')::integer > 0)::text",
            wire(v),
        ),
        P::ExportTruncated(v) => ("tasks.discovery_metadata->'data'->>'truncated'", wire(v)),
        P::ImportDryRun(v) => ("tasks.discovery_metadata->'data'->>'dry_run'", wire(v)),
        P::ImportAtomicity(v) => ("tasks.discovery_metadata->'data'->>'atomicity'", wire(v)),
        P::ImportCollisionPolicy(v) => (
            "tasks.discovery_metadata->'data'->>'collision_policy'",
            wire(v),
        ),
        P::ImportPermissionPolicy(v) => (
            "tasks.discovery_metadata->'data'->>'permission_policy'",
            wire(v),
        ),
        P::ImportHasFailedItems(v) => (
            "tasks.discovery_metadata->'data'->>'has_failed_items'",
            wire(v),
        ),
        P::BackupIncludeHistory(v) => (
            "tasks.discovery_metadata->'data'->>'include_history'",
            wire(v),
        ),
        P::OutputState(_) => return None,
    })
}
