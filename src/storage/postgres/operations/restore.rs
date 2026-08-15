use chrono::Utc;
use diesel::sql_types::Jsonb;
use serde_json::Value;

use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, NewEvent};
use crate::models::MaintenanceState;
use crate::models::backup::{
    BACKUP_AUXILIARY_HISTORY_SECTIONS, BACKUP_STATE_SECTIONS, BACKUP_TEMPORAL_HISTORY_SECTIONS,
    backup_history_sections,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, with_transaction};
use crate::storage::{StorageRestoreCompletion, StorageRestoreDocument, StorageRestoreJobStatus};

const TRUNCATE_TABLES: &[&str] = &[
    "object_computed_data",
    "class_computation_state",
    "computed_field_definitions",
    "event_deliveries",
    "events",
    "backup_task_outputs",
    "export_task_outputs",
    "remote_call_results",
    "import_task_results",
    "tasks",
    "token_scopes",
    "tokens",
    "event_subscriptions",
    "event_sinks",
    "remote_targets_history",
    "remote_targets",
    "export_templates_history",
    "export_templates",
    "permissions",
    "collection_authorization_state",
    "hubuumobject_relation_history",
    "hubuumobject_relation",
    "hubuumobject_history",
    "hubuumobject",
    "hubuumclass_relation_history",
    "hubuumclass_relation",
    "hubuumclass_reachability",
    "hubuumclass_history",
    "hubuumclass",
    "collection_closure",
    "collections_history",
    "collections",
    "group_membership_sources",
    "group_memberships",
    "service_accounts",
    "users",
    "principals",
    "groups",
    "identity_scopes",
];

const SERIAL_ID_TABLES: &[&str] = &[
    "identity_scopes",
    "groups",
    "principals",
    "collections",
    "permissions",
    "hubuumclass",
    "computed_field_definitions",
    "hubuumclass_relation",
    "hubuumobject",
    "hubuumobject_relation",
    "export_templates",
    "remote_targets",
    "event_sinks",
    "event_subscriptions",
    "tokens",
    "tasks",
    "import_task_results",
    "export_task_outputs",
    "remote_call_results",
];

const HISTORY_SEQUENCE_TABLES: &[&str] = &[
    "collections_history",
    "hubuumclass_history",
    "hubuumclass_relation_history",
    "hubuumobject_history",
    "hubuumobject_relation_history",
    "export_templates_history",
    "remote_targets_history",
];

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::restore_jobs)]
struct RestoreApplyRow {
    pub(crate) id: i64,
    pub(crate) status: String,
    pub(crate) requested_by: Option<i32>,
    pub(crate) requested_by_identity_scope: String,
    pub(crate) requested_by_name: String,
    pub(crate) sha256: String,
}

fn validate_restore_identifier(table: &str, column: Option<&str>) -> Result<(), ApiError> {
    let known_table = BACKUP_STATE_SECTIONS
        .iter()
        .chain(BACKUP_TEMPORAL_HISTORY_SECTIONS)
        .chain(BACKUP_AUXILIARY_HISTORY_SECTIONS)
        .chain(TRUNCATE_TABLES)
        .any(|known| *known == table);
    let known_column = column.is_none_or(|value| matches!(value, "id" | "history_id"));
    if known_table && known_column {
        Ok(())
    } else {
        Err(ApiError::InternalServerError(
            "Refused an unsafe restore SQL identifier".to_string(),
        ))
    }
}

async fn insert_rows(
    conn: &mut PostgresConnection,
    table: &str,
    rows: &[Value],
) -> Result<(), ApiError> {
    validate_restore_identifier(table, None)?;
    if rows.is_empty() {
        return Ok(());
    }
    let query = format!(
        "INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)"
    );
    diesel::sql_query(query)
        .bind::<Jsonb, _>(Value::Array(rows.to_vec()))
        .execute(conn)
        .await?;
    Ok(())
}

async fn reset_sequence(
    conn: &mut PostgresConnection,
    table: &str,
    column: &str,
) -> Result<(), ApiError> {
    validate_restore_identifier(table, Some(column))?;
    let query = format!(
        "SELECT setval(pg_get_serial_sequence('{table}', '{column}'), \
         COALESCE((SELECT MAX({column}) FROM {table}), 1), \
         (SELECT MAX({column}) IS NOT NULL FROM {table}))"
    );
    diesel::sql_query(query).execute(conn).await?;
    Ok(())
}

pub(crate) async fn apply_restore_db(
    pool: &crate::storage::postgres::PostgresPool,
    job_id: i64,
    document: StorageRestoreDocument,
) -> Result<StorageRestoreCompletion, ApiError> {
    let (metadata, snapshot) = document.into_parts();
    let (backup_version, backup_created_at, backup_source_version) = metadata.into_parts();
    let (state_sections, history_sections) = snapshot.into_parts();
    let includes_history = history_sections.is_some();
    with_transaction(pool, async move |conn| -> Result<StorageRestoreCompletion, ApiError> {
        diesel::sql_query("SELECT pg_advisory_xact_lock(4850188191125217)")
            .execute(conn)
            .await?;

        use crate::schema::restore_jobs::dsl::{id as restore_id, restore_jobs};
        use crate::schema::system_maintenance::dsl::{
            id as maintenance_id, restore_job_id, state, system_maintenance,
        };
        let job = restore_jobs
            .filter(restore_id.eq(job_id))
            .for_update()
            .select(RestoreApplyRow::as_select())
            .first::<RestoreApplyRow>(conn)
            .await?;
        let (maintenance_state_value, maintenance_restore_job_id) = system_maintenance
            .filter(maintenance_id.eq(1_i16))
            .select((state, restore_job_id))
            .first::<(String, Option<i64>)>(conn)
            .await?;
        let maintenance_state = MaintenanceState::try_from(maintenance_state_value.as_str())
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        if job.status != StorageRestoreJobStatus::Confirmed.as_str()
            || maintenance_state != MaintenanceState::Draining
            || maintenance_restore_job_id != Some(job.id)
        {
            return Err(ApiError::Conflict(format!(
                "Restore stage {} is no longer confirmed and draining",
                job.id
            )));
        }

        let started_at = Utc::now().naive_utc();
        diesel::sql_query("SELECT set_config('hubuum.restore_history', 'on', true)")
            .execute(conn)
            .await?;
        diesel::sql_query("SELECT set_config('hubuum.restore_events', 'on', true)")
            .execute(conn)
            .await?;
        diesel::sql_query("SELECT set_config('hubuum.restore_revisions', 'on', true)")
            .execute(conn)
            .await?;

        let lock_tables = TRUNCATE_TABLES.join(", ");
        for table in TRUNCATE_TABLES {
            validate_restore_identifier(table, None)?;
        }
        diesel::sql_query(format!("LOCK TABLE {lock_tables} IN ACCESS EXCLUSIVE MODE"))
            .execute(conn)
            .await?;
        diesel::sql_query(format!(
            "TRUNCATE TABLE {lock_tables} RESTART IDENTITY CASCADE"
        ))
        .execute(conn)
        .await?;

        for table in BACKUP_STATE_SECTIONS {
            let rows = state_sections
                .get(*table)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            insert_rows(conn, table, rows).await?;
        }
        if let Some(history) = &history_sections {
            for table in backup_history_sections() {
                let rows = history
                    .get(table)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                insert_rows(conn, table, rows).await?;
            }
        }

        for table in SERIAL_ID_TABLES {
            reset_sequence(conn, table, "id").await?;
        }
        reset_sequence(conn, "events", "id").await?;
        reset_sequence(conn, "event_deliveries", "id").await?;
        for table in HISTORY_SEQUENCE_TABLES {
            reset_sequence(conn, table, "history_id").await?;
        }

        crate::storage::postgres::operations::computed_field::enqueue_restored_computed_rebuilds(
            conn,
        )
        .await?;

        // Restored event rows must not fan out while they are inserted. This
        // new event is the one deliberate post-restore provenance record and
        // is delivered normally after the transaction commits.
        diesel::sql_query("SELECT set_config('hubuum.restore_events', 'off', true)")
            .execute(conn)
            .await?;
        let provenance = NewEvent::new(
            EntityType::Restore,
            Action::Succeeded,
            ActorKind::System,
            "System restore completed",
        )?
        .with_entity_name(job.sha256.clone())
        .with_metadata(serde_json::json!({
            "restore_job_id": job.id,
            "backup_sha256": job.sha256,
            "backup_version": backup_version,
            "backup_source_version": backup_source_version,
            "backup_created_at": backup_created_at,
            "includes_history": includes_history,
            "initiated_by": {
                "principal_id": job.requested_by,
                "identity_scope": job.requested_by_identity_scope,
                "name": job.requested_by_name,
            },
        }));
        emit_event(conn, &provenance).await?;

        let finished_at = Utc::now().naive_utc();
        diesel::sql_query(
            "UPDATE system_maintenance \
                 SET generation=0, state='normal', restore_job_id=NULL, \
                     entered_at=NULL, updated_at=$1 \
                 WHERE id=1",
        )
        .bind::<diesel::sql_types::Timestamp, _>(finished_at)
        .execute(conn)
        .await?;
        diesel::sql_query("DELETE FROM restore_jobs")
            .execute(conn)
            .await?;
        diesel::sql_query("DELETE FROM server_instances")
            .execute(conn)
            .await?;
        diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
            .execute(conn)
            .await?;
        Ok(StorageRestoreCompletion::new(started_at, finished_at))
    })
    .await
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::validate_restore_identifier;

    #[rstest]
    #[case::known("collections", Some("id"), true)]
    #[case::unknown_table("collections; DROP TABLE users", None, false)]
    #[case::unknown_column("collections", Some("id DESC"), false)]
    fn restore_sql_identifiers_come_from_closed_lists(
        #[case] table: &str,
        #[case] column: Option<&str>,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(
            validate_restore_identifier(table, column).is_ok(),
            expected_valid
        );
    }
}
