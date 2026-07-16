use chrono::{NaiveDateTime, Utc};
use diesel::dsl::sql;
use diesel::sql_types::{Jsonb, Timestamp};
use serde_json::Value;
use uuid::Uuid;

use crate::db::prelude::*;
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{NewEvent, emit_event};
use crate::models::backup::{
    BACKUP_AUXILIARY_HISTORY_SECTIONS, BACKUP_STATE_SECTIONS, BACKUP_TEMPORAL_HISTORY_SECTIONS,
    backup_history_sections,
};
use crate::models::{
    BackupDocument, NewRestoreJobRecord, RestoreJobRecord, RestoreJobStatus, ServerInstanceRecord,
};

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

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = crate::schema::restore_jobs)]
pub(crate) struct RestoreJobStatusRecord {
    pub(crate) id: i64,
    pub(crate) status: String,
    pub(crate) requested_by: Option<i32>,
    pub(crate) requested_by_identity_scope: String,
    pub(crate) requested_by_name: String,
    pub(crate) byte_size: i64,
    pub(crate) sha256: String,
    pub(crate) capability_hash: String,
    pub(crate) validation_summary: serde_json::Value,
    pub(crate) error: Option<String>,
    pub(crate) expires_at: NaiveDateTime,
    pub(crate) confirmed_at: Option<NaiveDateTime>,
    pub(crate) finished_at: Option<NaiveDateTime>,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RestoreCompletion {
    pub(crate) started_at: NaiveDateTime,
    pub(crate) finished_at: NaiveDateTime,
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

async fn insert_rows(conn: &mut DbConnection, table: &str, rows: &[Value]) -> Result<(), ApiError> {
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
    conn: &mut DbConnection,
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

pub(crate) async fn insert_restore_job_db(
    pool: &DbPool,
    input: NewRestoreJobRecord,
) -> Result<RestoreJobRecord, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::restore_jobs::dsl::restore_jobs;

        diesel::insert_into(restore_jobs)
            .values(input)
            .returning(RestoreJobRecord::as_returning())
            .get_result::<RestoreJobRecord>(conn)
            .await
    })
    .await
}

pub(crate) async fn load_restore_job_db(
    pool: &DbPool,
    job_id: i64,
) -> Result<RestoreJobRecord, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::restore_jobs::dsl::{id, restore_jobs};

        restore_jobs
            .filter(id.eq(job_id))
            .select(RestoreJobRecord::as_select())
            .first::<RestoreJobRecord>(conn)
            .await
            .optional()
    })
    .await?
    .ok_or_else(|| ApiError::NotFound(format!("Restore stage {job_id} was not found")))
}

pub(crate) async fn load_restore_status_job_db(
    pool: &DbPool,
    job_id: i64,
) -> Result<RestoreJobStatusRecord, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::restore_jobs::dsl::{id, restore_jobs};

        restore_jobs
            .filter(id.eq(job_id))
            .select(RestoreJobStatusRecord::as_select())
            .first::<RestoreJobStatusRecord>(conn)
            .await
            .optional()
    })
    .await?
    .ok_or_else(|| ApiError::NotFound(format!("Restore stage {job_id} was not found")))
}

pub(crate) async fn expire_restore_stage_db(pool: &DbPool, job_id: i64) -> Result<usize, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::restore_jobs::dsl::{document, id, restore_jobs, status};

        diesel::update(
            restore_jobs
                .filter(id.eq(job_id))
                .filter(status.eq(RestoreJobStatus::Validated.as_str())),
        )
        .set((
            status.eq(RestoreJobStatus::Expired.as_str()),
            document.eq(Vec::<u8>::new()),
        ))
        .execute(conn)
        .await
    })
    .await
}

pub(crate) async fn start_restore_draining_db(
    pool: &DbPool,
    job_id: i64,
) -> Result<NaiveDateTime, ApiError> {
    with_transaction(pool, async |conn| -> Result<NaiveDateTime, ApiError> {
        diesel::sql_query("SELECT pg_advisory_xact_lock(4850188191125217)")
            .execute(conn)
            .await?;
        use crate::schema::restore_jobs::dsl::{confirmed_at, error, id, restore_jobs, status};
        let confirmation_time = diesel::update(
            restore_jobs
                .filter(id.eq(job_id))
                .filter(status.eq(RestoreJobStatus::Validated.as_str())),
        )
        .set((
            status.eq(RestoreJobStatus::Confirmed.as_str()),
            confirmed_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL).nullable()),
            error.eq::<Option<String>>(None),
        ))
        .returning(confirmed_at)
        .get_result::<Option<NaiveDateTime>>(conn)
        .await
        .optional()?
        .flatten()
        .ok_or_else(|| {
            ApiError::Conflict("Restore stage was confirmed concurrently".to_string())
        })?;
        let maintenance_changed = diesel::sql_query(
            "UPDATE system_maintenance \
             SET generation=generation+1, state='draining', restore_job_id=$1, \
                 entered_at=now(), updated_at=now() \
             WHERE id=1 AND state='normal'",
        )
        .bind::<diesel::sql_types::BigInt, _>(job_id)
        .execute(conn)
        .await?;
        if maintenance_changed != 1 {
            return Err(ApiError::Conflict(
                "Another maintenance operation is already active".to_string(),
            ));
        }
        diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'draining')")
            .execute(conn)
            .await?;
        Ok(confirmation_time)
    })
    .await
}

pub(crate) async fn apply_restore_db(
    pool: &DbPool,
    job: &RestoreJobRecord,
    document: &BackupDocument,
    provenance: &NewEvent,
) -> Result<RestoreCompletion, ApiError> {
    with_transaction(pool, async |conn| -> Result<RestoreCompletion, ApiError> {
        diesel::sql_query("SELECT pg_advisory_xact_lock(4850188191125217)")
            .execute(conn)
            .await?;

        use crate::schema::restore_jobs::dsl::{id as restore_id, restore_jobs, status};
        use crate::schema::system_maintenance::dsl::{
            id as maintenance_id, restore_job_id, state, system_maintenance,
        };
        let current_status = restore_jobs
            .filter(restore_id.eq(job.id))
            .select(status)
            .first::<String>(conn)
            .await?;
        let (maintenance_state, maintenance_restore_job_id) = system_maintenance
            .filter(maintenance_id.eq(1_i16))
            .select((state, restore_job_id))
            .first::<(String, Option<i64>)>(conn)
            .await?;
        if current_status != RestoreJobStatus::Confirmed.as_str()
            || maintenance_state != "draining"
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
            let rows = document
                .state
                .sections
                .get(*table)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            insert_rows(conn, table, rows).await?;
        }
        if let Some(history) = &document.history {
            for table in backup_history_sections() {
                let rows = history
                    .sections
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

        crate::db::traits::computed_field::enqueue_restored_computed_rebuilds(conn).await?;

        // Restored event rows must not fan out while they are inserted. This
        // new event is the one deliberate post-restore provenance record and
        // is delivered normally after the transaction commits.
        diesel::sql_query("SELECT set_config('hubuum.restore_events', 'off', true)")
            .execute(conn)
            .await?;
        emit_event(conn, provenance).await?;

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
        Ok(RestoreCompletion {
            started_at,
            finished_at,
        })
    })
    .await
}

pub(crate) async fn fail_restore_and_resume_db(
    pool: &DbPool,
    job_id: i64,
    stored_error: &str,
) -> Result<(), ApiError> {
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        diesel::sql_query(
            "UPDATE restore_jobs \
             SET status='failed', error=$2, finished_at=now(), document=''::bytea \
             WHERE id=$1 AND status IN ('validated', 'confirmed')",
        )
        .bind::<diesel::sql_types::BigInt, _>(job_id)
        .bind::<diesel::sql_types::Text, _>(stored_error)
        .execute(conn)
        .await?;
        diesel::sql_query(
            "UPDATE system_maintenance \
             SET state='normal', restore_job_id=NULL, entered_at=NULL, updated_at=now() \
             WHERE id=1 AND restore_job_id=$1 AND state='draining'",
        )
        .bind::<diesel::sql_types::BigInt, _>(job_id)
        .execute(conn)
        .await?;
        diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

pub(crate) async fn maintenance_restore_reference_db(
    pool: &DbPool,
) -> Result<(String, Option<i64>, NaiveDateTime), ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::system_maintenance::dsl::{
            id, restore_job_id, state, system_maintenance,
        };

        system_maintenance
            .filter(id.eq(1_i16))
            .select((
                state,
                restore_job_id,
                sql::<Timestamp>(DATABASE_UTC_NOW_SQL),
            ))
            .first::<(String, Option<i64>, NaiveDateTime)>(conn)
            .await
    })
    .await
}

pub(crate) async fn resume_maintenance_without_job_db(pool: &DbPool) -> Result<(), ApiError> {
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        diesel::sql_query(
            "UPDATE system_maintenance \
             SET state='normal', entered_at=NULL, updated_at=now() \
             WHERE id=1 AND restore_job_id IS NULL AND state='draining'",
        )
        .execute(conn)
        .await?;
        diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

pub(crate) async fn resume_terminal_restore_db(pool: &DbPool, job_id: i64) -> Result<(), ApiError> {
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        diesel::sql_query(
            "UPDATE system_maintenance \
             SET state='normal', restore_job_id=NULL, entered_at=NULL, updated_at=now() \
             WHERE id=1 AND restore_job_id=$1 AND state='draining'",
        )
        .bind::<diesel::sql_types::BigInt, _>(job_id)
        .execute(conn)
        .await?;
        diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
            .execute(conn)
            .await?;
        Ok(())
    })
    .await
}

pub(crate) async fn expire_validated_restore_jobs_db(pool: &DbPool) -> Result<(), ApiError> {
    with_connection(pool, async |conn| {
        diesel::sql_query(
            "UPDATE restore_jobs \
             SET status='expired', document=''::bytea \
             WHERE status='validated' AND expires_at <= now()",
        )
        .execute(conn)
        .await
    })
    .await?;
    Ok(())
}

pub(crate) async fn maintenance_generation_and_state_db(
    pool: &DbPool,
) -> Result<(i64, String), ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::system_maintenance::dsl::{generation, id, state, system_maintenance};

        system_maintenance
            .filter(id.eq(1_i16))
            .select((generation, state))
            .first::<(i64, String)>(conn)
            .await
    })
    .await
}

pub(crate) async fn upsert_server_instance_db(
    pool: &DbPool,
    record: &ServerInstanceRecord,
) -> Result<(), ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::server_instances::dsl::{
            drained, instance_id as row_id, last_heartbeat_at, maintenance_generation,
            server_instances,
        };

        diesel::insert_into(server_instances)
            .values(record)
            .on_conflict(row_id)
            .do_update()
            .set((
                maintenance_generation.eq(record.maintenance_generation),
                drained.eq(record.drained),
                last_heartbeat_at.eq(record.last_heartbeat_at),
            ))
            .execute(conn)
            .await
    })
    .await?;
    Ok(())
}

pub(crate) async fn maintenance_generation_and_instances_db(
    pool: &DbPool,
    heartbeat_cutoff: NaiveDateTime,
) -> Result<(i64, Vec<ServerInstanceRecord>), ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::server_instances::dsl::{last_heartbeat_at, server_instances};
        use crate::schema::system_maintenance::dsl::{
            generation as generation_column, id, system_maintenance,
        };

        let current_generation = system_maintenance
            .filter(id.eq(1_i16))
            .select(generation_column)
            .first::<i64>(conn)
            .await?;
        let instances = server_instances
            .filter(last_heartbeat_at.gt(heartbeat_cutoff))
            .load::<ServerInstanceRecord>(conn)
            .await?;
        Ok::<_, diesel::result::Error>((current_generation, instances))
    })
    .await
}

pub(crate) async fn delete_server_instance_db(
    pool: &DbPool,
    instance_id: Uuid,
) -> Result<(), ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::server_instances::dsl::{instance_id as row_id, server_instances};

        diesel::delete(server_instances.filter(row_id.eq(instance_id)))
            .execute(conn)
            .await
    })
    .await?;
    Ok(())
}

pub(crate) async fn maintenance_state_db(pool: &DbPool) -> Result<String, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};

        system_maintenance
            .filter(id.eq(1_i16))
            .select(state)
            .first::<String>(conn)
            .await
    })
    .await
}

pub(crate) async fn identity_scope_name_db(
    pool: &DbPool,
    identity_scope_id: i32,
) -> Result<String, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::identity_scopes::dsl::{id, identity_scopes, name};

        identity_scopes
            .filter(id.eq(identity_scope_id))
            .select(name)
            .first::<String>(conn)
            .await
    })
    .await
}

#[cfg(test)]
mod tests {
    use diesel::prelude::*;
    use rstest::rstest;

    use super::{RestoreJobStatusRecord, validate_restore_identifier};

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

    #[rstest]
    #[case::document("\"restore_jobs\".\"document\"", false)]
    #[case::capability_hash("\"restore_jobs\".\"capability_hash\"", true)]
    fn restore_status_projection_fields(#[case] field: &str, #[case] expected: bool) {
        use crate::schema::restore_jobs::dsl::{id, restore_jobs};

        let query = restore_jobs
            .filter(id.eq(42_i64))
            .select(RestoreJobStatusRecord::as_select());
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();

        assert_eq!(sql.contains(field), expected);
    }
}
