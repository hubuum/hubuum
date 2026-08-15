//! Transitional PostgreSQL persistence for remote-call task results.
//!
//! Remote-target lifecycle operations live in `hubuum-storage-postgres`. This
//! result projection remains here only until the task-execution family moves
//! into the adapter crate.

#[cfg(test)]
use std::fmt;

#[cfg(test)]
use diesel::Insertable;
#[cfg(any(test, feature = "integration-test-support"))]
use diesel::prelude::ExpressionMethods;
#[cfg(feature = "integration-test-support")]
use diesel::prelude::QueryDsl;
#[cfg(feature = "integration-test-support")]
use diesel::{Queryable, Selectable};
#[cfg(any(test, feature = "integration-test-support"))]
use diesel_async::RunQueryDsl;

#[cfg(any(test, feature = "integration-test-support"))]
use crate::errors::ApiError;
#[cfg(feature = "integration-test-support")]
use crate::models::remote_target::RemoteCallResult;
#[cfg(test)]
use crate::models::{REDACTED_DEBUG_VALUE, redacted_debug_option};
#[cfg(feature = "integration-test-support")]
use crate::storage::postgres::with_connection;

#[cfg(feature = "integration-test-support")]
#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::remote_call_results)]
struct RemoteCallResultRow {
    id: i32,
    task_id: i32,
    target_id: Option<i32>,
    subject_type: String,
    subject_id: i32,
    method: String,
    rendered_url: String,
    response_status: Option<i32>,
    response_headers: Option<serde_json::Value>,
    response_body_preview: Option<String>,
    duration_ms: i32,
    success: bool,
    error: Option<String>,
    created_at: chrono::NaiveDateTime,
}

#[cfg(test)]
#[derive(Clone, Insertable)]
#[diesel(table_name = crate::schema::remote_call_results)]
pub(crate) struct NewRemoteCallResultRow {
    pub(crate) task_id: i32,
    pub(crate) target_id: Option<i32>,
    pub(crate) subject_type: String,
    pub(crate) subject_id: i32,
    pub(crate) method: String,
    pub(crate) rendered_url: String,
    pub(crate) response_status: Option<i32>,
    pub(crate) response_headers: Option<serde_json::Value>,
    pub(crate) response_body_preview: Option<String>,
    pub(crate) duration_ms: i32,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
}

#[cfg(test)]
impl fmt::Debug for NewRemoteCallResultRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewRemoteCallResultRow")
            .field("task_id", &self.task_id)
            .field("target_id", &self.target_id)
            .field("subject_type", &self.subject_type)
            .field("subject_id", &self.subject_id)
            .field("method", &self.method)
            .field("response_status", &self.response_status)
            .field("duration_ms", &self.duration_ms)
            .field("success", &self.success)
            .field("rendered_url", &REDACTED_DEBUG_VALUE)
            .field("response_headers", &REDACTED_DEBUG_VALUE)
            .field("response_body_preview", &REDACTED_DEBUG_VALUE)
            .field("error", &redacted_debug_option(&self.error))
            .finish()
    }
}

#[cfg(test)]
pub(crate) async fn upsert_remote_call_result_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
    entry: NewRemoteCallResultRow,
) -> Result<(), ApiError> {
    use crate::schema::remote_call_results::dsl::{remote_call_results, task_id};

    diesel::insert_into(remote_call_results)
        .values(&entry)
        .on_conflict(task_id)
        .do_update()
        .set((
            crate::schema::remote_call_results::target_id.eq(entry.target_id),
            crate::schema::remote_call_results::subject_type.eq(entry.subject_type.clone()),
            crate::schema::remote_call_results::subject_id.eq(entry.subject_id),
            crate::schema::remote_call_results::method.eq(entry.method.clone()),
            crate::schema::remote_call_results::rendered_url.eq(entry.rendered_url.clone()),
            crate::schema::remote_call_results::response_status.eq(entry.response_status),
            crate::schema::remote_call_results::response_headers.eq(entry.response_headers.clone()),
            crate::schema::remote_call_results::response_body_preview
                .eq(entry.response_body_preview.clone()),
            crate::schema::remote_call_results::duration_ms.eq(entry.duration_ms),
            crate::schema::remote_call_results::success.eq(entry.success),
            crate::schema::remote_call_results::error.eq(entry.error.clone()),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn load_remote_call_result_for_task(
    pool: &crate::storage::postgres::PostgresPool,
    task_id_value: i32,
) -> Result<RemoteCallResult, ApiError> {
    use crate::schema::remote_call_results::dsl::{remote_call_results, task_id};

    with_connection(pool, async |conn| {
        remote_call_results
            .filter(task_id.eq(task_id_value))
            .first::<RemoteCallResultRow>(conn)
            .await
    })
    .await
    .map(remote_call_result_to_model)
}

#[cfg(feature = "integration-test-support")]
fn remote_call_result_to_model(row: RemoteCallResultRow) -> RemoteCallResult {
    RemoteCallResult {
        id: row.id,
        task_id: row.task_id,
        target_id: row.target_id,
        subject_type: row.subject_type,
        subject_id: row.subject_id,
        method: row.method,
        rendered_url: row.rendered_url,
        response_status: row.response_status,
        response_headers: row.response_headers,
        response_body_preview: row.response_body_preview,
        duration_ms: row.duration_ms,
        success: row.success,
        error: row.error,
        created_at: row.created_at,
    }
}
