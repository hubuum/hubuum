use std::collections::BTreeMap;

use diesel::sql_types::Jsonb;
use serde_json::Value;

use crate::db::prelude::*;
use crate::db::{DbConnection, DbPool, with_transaction};
use crate::errors::ApiError;
use crate::models::backup::{
    BACKUP_AUXILIARY_HISTORY_SECTIONS, BACKUP_STATE_SECTIONS, BACKUP_TEMPORAL_HISTORY_SECTIONS,
};
use crate::models::{BackupHistory, BackupState};

#[derive(QueryableByName)]
struct JsonRows {
    #[diesel(sql_type = Jsonb)]
    rows: Value,
}

fn validate_snapshot_table(table: &str) -> Result<(), ApiError> {
    let known_table = BACKUP_STATE_SECTIONS
        .iter()
        .chain(BACKUP_TEMPORAL_HISTORY_SECTIONS)
        .chain(BACKUP_AUXILIARY_HISTORY_SECTIONS)
        .any(|known| *known == table);
    if !known_table {
        return Err(ApiError::InternalServerError(
            "Refused an unknown backup snapshot table".to_string(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SnapshotFilter {
    All,
    TerminalTasks,
    TerminalTaskResults,
    HistoryEvents,
    TerminalDeliveries,
}

impl SnapshotFilter {
    fn sql(self, table: &str) -> Result<Option<&'static str>, ApiError> {
        match self {
            Self::All => Ok(None),
            Self::TerminalTasks if table == "tasks" => Ok(Some(
                "status IN ('succeeded', 'partially_succeeded', 'failed', 'cancelled')",
            )),
            Self::TerminalTaskResults
                if matches!(
                    table,
                    "import_task_results" | "export_task_outputs" | "remote_call_results"
                ) =>
            {
                Ok(Some(
                    "task_id IN (SELECT id FROM tasks WHERE status IN \
                     ('succeeded', 'partially_succeeded', 'failed', 'cancelled'))",
                ))
            }
            Self::HistoryEvents if table == "events" => Ok(Some(
                "entity_type <> 'task' OR entity_id IN \
                 (SELECT id FROM tasks WHERE status IN \
                 ('succeeded', 'partially_succeeded', 'failed', 'cancelled'))",
            )),
            Self::TerminalDeliveries if table == "event_deliveries" => Ok(Some(
                "status IN ('succeeded', 'dead') AND event_id IN \
                 (SELECT id FROM events WHERE entity_type <> 'task' OR entity_id IN \
                 (SELECT id FROM tasks WHERE status IN \
                 ('succeeded', 'partially_succeeded', 'failed', 'cancelled')))",
            )),
            _ => Err(ApiError::InternalServerError(
                "Refused an invalid backup snapshot filter/table combination".to_string(),
            )),
        }
    }
}

async fn load_json_rows(
    conn: &mut DbConnection,
    table: &str,
    filter: SnapshotFilter,
) -> Result<Vec<Value>, ApiError> {
    validate_snapshot_table(table)?;
    // The only formatted components are a table identifier from the closed
    // list above and a predicate selected from fixed internal variants.
    let predicate = filter
        .sql(table)?
        .map(|value| format!(" WHERE {value}"))
        .unwrap_or_default();
    let query = format!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(snapshot_row) ORDER BY to_jsonb(snapshot_row)::text), '[]'::jsonb) AS rows \
         FROM (SELECT * FROM {table}{predicate}) snapshot_row"
    );
    let value = diesel::sql_query(query)
        .get_result::<JsonRows>(conn)
        .await?
        .rows;
    value.as_array().cloned().ok_or_else(|| {
        ApiError::InternalServerError(format!("Backup query for {table} did not return an array"))
    })
}

async fn snapshot_state(conn: &mut DbConnection) -> Result<BackupState, ApiError> {
    let mut sections = BTreeMap::new();
    for table in BACKUP_STATE_SECTIONS {
        sections.insert(
            (*table).to_string(),
            load_json_rows(conn, table, SnapshotFilter::All).await?,
        );
    }
    Ok(BackupState { sections })
}

async fn snapshot_history(conn: &mut DbConnection) -> Result<BackupHistory, ApiError> {
    let mut sections = BTreeMap::new();
    for table in BACKUP_TEMPORAL_HISTORY_SECTIONS {
        sections.insert(
            (*table).to_string(),
            load_json_rows(conn, table, SnapshotFilter::All).await?,
        );
    }
    let mut tasks = load_json_rows(conn, "tasks", SnapshotFilter::TerminalTasks).await?;
    for task in &mut tasks {
        if let Some(object) = task.as_object_mut() {
            // Tokens are intentionally excluded. Historical tasks keep their
            // scope snapshot but not an invalid FK to an omitted credential.
            object.insert("submitted_token_id".to_string(), Value::Null);
            // A completed task's request-dedup key has no operational meaning
            // after restore and can otherwise block future submissions.
            object.insert("idempotency_key".to_string(), Value::Null);
        }
    }
    sections.insert("tasks".to_string(), tasks);
    sections.insert(
        "import_task_results".to_string(),
        load_json_rows(
            conn,
            "import_task_results",
            SnapshotFilter::TerminalTaskResults,
        )
        .await?,
    );
    sections.insert(
        "export_task_outputs".to_string(),
        load_json_rows(
            conn,
            "export_task_outputs",
            SnapshotFilter::TerminalTaskResults,
        )
        .await?,
    );
    sections.insert(
        "remote_call_results".to_string(),
        load_json_rows(
            conn,
            "remote_call_results",
            SnapshotFilter::TerminalTaskResults,
        )
        .await?,
    );
    let mut events = load_json_rows(conn, "events", SnapshotFilter::HistoryEvents).await?;
    for event in &mut events {
        if let Some(object) = event.as_object_mut() {
            if object.get("dispatched_at").is_none_or(Value::is_null) {
                object.insert(
                    "dispatched_at".to_string(),
                    object.get("occurred_at").cloned().unwrap_or(Value::Null),
                );
            }
            object.insert("fanout_locked_until".to_string(), Value::Null);
            object.insert("fanout_claim_token".to_string(), Value::Null);
        }
    }
    sections.insert("events".to_string(), events);
    sections.insert(
        "event_deliveries".to_string(),
        load_json_rows(conn, "event_deliveries", SnapshotFilter::TerminalDeliveries).await?,
    );
    Ok(BackupHistory { sections })
}

pub(crate) async fn snapshot_backup_db(
    pool: &DbPool,
    include_history: bool,
) -> Result<(BackupState, Option<BackupHistory>), ApiError> {
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        // This must be the first statement in the transaction. Every state and
        // history query below consequently observes one PostgreSQL snapshot.
        diesel::sql_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(conn)
            .await?;
        let state = snapshot_state(conn).await?;
        let history = if include_history {
            Some(snapshot_history(conn).await?)
        } else {
            None
        };
        Ok((state, history))
    })
    .await
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{SnapshotFilter, validate_snapshot_table};

    #[rstest]
    #[case::known("collections", true)]
    #[case::injected("collections; DROP TABLE users", false)]
    fn snapshot_tables_use_a_closed_allowlist(#[case] table: &str, #[case] accepted: bool) {
        assert_eq!(validate_snapshot_table(table).is_ok(), accepted);
    }

    #[rstest]
    #[case::all_collections(SnapshotFilter::All, "collections", true)]
    #[case::task_filter_on_collection(SnapshotFilter::TerminalTasks, "collections", false)]
    fn snapshot_filters_are_validated_for_their_table(
        #[case] filter: SnapshotFilter,
        #[case] table: &str,
        #[case] accepted: bool,
    ) {
        assert_eq!(filter.sql(table).is_ok(), accepted);
    }
}
