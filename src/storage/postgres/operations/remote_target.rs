use std::fmt;

use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::apply_query_options;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
#[cfg(feature = "integration-test-support")]
use crate::models::remote_target::RemoteCallResult;
use crate::models::remote_target::RemoteTargetID;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{REDACTED_DEBUG_VALUE, redacted_debug_option};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{with_connection, with_transaction};
use crate::{date_search, numeric_search, revision_search, string_search};

macro_rules! impl_redacted_remote_target_row_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("configuration", &REDACTED_DEBUG_VALUE)
                    .finish()
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::remote_targets)]
pub(crate) struct RemoteTargetRow {
    pub(crate) id: i32,
    pub(crate) collection_id: i32,
    pub(crate) class_id: Option<i32>,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) method: String,
    pub(crate) url_template: String,
    pub(crate) headers_template: serde_json::Value,
    pub(crate) body_template: Option<String>,
    pub(crate) auth_config: serde_json::Value,
    pub(crate) allowed_subject_types: serde_json::Value,
    pub(crate) timeout_ms: i32,
    pub(crate) enabled: bool,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl_redacted_remote_target_row_debug!(
    RemoteTargetRow,
    id,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
    created_at,
    updated_at,
);

impl RemoteTargetRow {
    pub(crate) fn audit_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "collection_id": self.collection_id,
            "class_id": self.class_id,
            "name": self.name,
            "description": self.description,
            "method": self.method,
            "url_template": self.url_template,
            "headers_template": self.headers_template,
            "body_template": self.body_template,
            "auth_config": "<redacted>",
            "allowed_subject_types": self.allowed_subject_types,
            "timeout_ms": self.timeout_ms,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

impl CursorPaginated for RemoteTargetRow {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::CollectionId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id.into())),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::Description => Ok(CursorValue::String(self.description.clone())),
            FilterField::CollectionId => Ok(CursorValue::Integer(self.collection_id.into())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::UpdatedAt => Ok(CursorValue::DateTime(self.updated_at)),
            FilterField::Revision => Ok(CursorValue::Integer(self.revision.get())),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{field}' for remote targets"
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for RemoteTargetRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "remote_targets.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "remote_targets.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "remote_targets.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CollectionId => CursorSqlField {
                column: "remote_targets.collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "remote_targets.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "remote_targets.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "remote_targets.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{field}' is not orderable for remote targets"
                )));
            }
        })
    }
}

#[derive(Clone, Insertable)]
#[diesel(table_name = crate::schema::remote_targets)]
pub(crate) struct NewRemoteTargetRow {
    pub(crate) collection_id: i32,
    pub(crate) class_id: Option<i32>,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) method: String,
    pub(crate) url_template: String,
    pub(crate) headers_template: serde_json::Value,
    pub(crate) body_template: Option<String>,
    pub(crate) auth_config: serde_json::Value,
    pub(crate) allowed_subject_types: serde_json::Value,
    pub(crate) timeout_ms: i32,
    pub(crate) enabled: bool,
}

impl_redacted_remote_target_row_debug!(
    NewRemoteTargetRow,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
);

#[derive(Clone, AsChangeset)]
#[diesel(table_name = crate::schema::remote_targets)]
pub(crate) struct UpdateRemoteTargetRow {
    pub(crate) collection_id: Option<i32>,
    pub(crate) class_id: Option<Option<i32>>,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) method: Option<String>,
    pub(crate) url_template: Option<String>,
    pub(crate) headers_template: Option<serde_json::Value>,
    pub(crate) body_template: Option<Option<String>>,
    pub(crate) auth_config: Option<serde_json::Value>,
    pub(crate) allowed_subject_types: Option<serde_json::Value>,
    pub(crate) timeout_ms: Option<i32>,
    pub(crate) enabled: Option<bool>,
}

impl_redacted_remote_target_row_debug!(
    UpdateRemoteTargetRow,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
);

impl UpdateRemoteTargetRow {
    pub(crate) fn has_changes(&self, current: &RemoteTargetRow) -> bool {
        self.collection_id
            .is_some_and(|value| value != current.collection_id)
            || self
                .class_id
                .as_ref()
                .is_some_and(|value| value != &current.class_id)
            || self
                .name
                .as_ref()
                .is_some_and(|value| value != &current.name)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
            || self
                .method
                .as_ref()
                .is_some_and(|value| value != &current.method)
            || self
                .url_template
                .as_ref()
                .is_some_and(|value| value != &current.url_template)
            || self
                .headers_template
                .as_ref()
                .is_some_and(|value| value != &current.headers_template)
            || self
                .body_template
                .as_ref()
                .is_some_and(|value| value != &current.body_template)
            || self
                .auth_config
                .as_ref()
                .is_some_and(|value| value != &current.auth_config)
            || self
                .allowed_subject_types
                .as_ref()
                .is_some_and(|value| value != &current.allowed_subject_types)
            || self
                .timeout_ms
                .is_some_and(|value| value != current.timeout_ms)
            || self.enabled.is_some_and(|value| value != current.enabled)
    }
}

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

fn remote_target_event(
    row: &RemoteTargetRow,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::RemoteTarget,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(row.id)
    .with_entity_name(row.name.clone())
    .with_collection_id(row.collection_id))
}

pub(crate) trait LoadRemoteTargetRecord {
    async fn load_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<RemoteTargetRow, ApiError>;
}

impl LoadRemoteTargetRecord for RemoteTargetID {
    async fn load_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, async |conn| {
            remote_targets
                .filter(id.eq(self.id()))
                .first::<RemoteTargetRow>(conn)
                .await
        })
        .await
    }
}

pub(crate) trait SaveRemoteTargetRecord {
    async fn save_remote_target_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<RemoteTargetRow, ApiError>;

    async fn save_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let _ = context;
        self.save_remote_target_record_without_events(pool).await
    }
}

impl SaveRemoteTargetRecord for NewRemoteTargetRow {
    async fn save_remote_target_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::remote_targets;

        with_connection(pool, async |conn| {
            diesel::insert_into(remote_targets)
                .values(self)
                .get_result::<RemoteTargetRow>(conn)
                .await
        })
        .await
    }

    async fn save_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let Some(context) = context else {
            return self.save_remote_target_record_without_events(pool).await;
        };

        use crate::schema::remote_targets::dsl::remote_targets;

        with_transaction(pool, async |conn| -> Result<RemoteTargetRow, ApiError> {
            let row = diesel::insert_into(remote_targets)
                .values(self)
                .get_result::<RemoteTargetRow>(conn)
                .await?;
            let event = remote_target_event(
                &row,
                Action::Created,
                context,
                format!("Remote target '{}' created", row.name),
            )?
            .with_after(row.audit_snapshot());
            emit_event(conn, &event).await?;
            Ok(row)
        })
        .await
    }
}

pub(crate) trait UpdateRemoteTargetRecord {
    async fn update_remote_target_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target_id: i32,
    ) -> Result<RemoteTargetRow, ApiError>;

    async fn update_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target_id: i32,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let _ = context;
        self.update_remote_target_record_without_events(pool, target_id)
            .await
    }
}

impl UpdateRemoteTargetRecord for UpdateRemoteTargetRow {
    async fn update_remote_target_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target_id: i32,
    ) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(remote_targets.filter(id.eq(target_id)))
                    .set(self)
                    .get_result::<RemoteTargetRow>(conn)
                    .await
                    .optional(),
                async || remote_targets.filter(id.eq(target_id)).first(conn).await,
            )
            .await
        })
        .await
    }

    async fn update_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target_id: i32,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let Some(context) = context else {
            return self
                .update_remote_target_record_without_events(pool, target_id)
                .await;
        };

        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_transaction(pool, async |conn| -> Result<RemoteTargetRow, ApiError> {
            let before = remote_targets
                .filter(id.eq(target_id))
                .for_update()
                .first::<RemoteTargetRow>(conn)
                .await?;
            crate::storage::postgres::assert_locked_revision_precondition(
                conn,
                &RevisionOwner::RemoteTarget.key(before.id),
                before.revision,
            )
            .await?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let after = diesel::update(remote_targets.filter(id.eq(target_id)))
                .set(self)
                .get_result::<RemoteTargetRow>(conn)
                .await?;
            let event = remote_target_event(
                &after,
                Action::Updated,
                context,
                format!("Remote target '{}' updated", after.name),
            )?
            .with_before(before.audit_snapshot())
            .with_after(after.audit_snapshot());
            emit_event(conn, &event).await?;
            Ok(after)
        })
        .await
    }
}

pub(crate) trait DeleteRemoteTargetRecord {
    async fn delete_remote_target_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;

    async fn delete_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_remote_target_record_without_events(pool).await
    }
}

impl DeleteRemoteTargetRecord for RemoteTargetID {
    async fn delete_remote_target_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, async |conn| {
            diesel::delete(remote_targets.filter(id.eq(self.id())))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_remote_target_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_remote_target_record_without_events(pool).await;
        };

        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let before = remote_targets
                .filter(id.eq(self.id()))
                .for_update()
                .first::<RemoteTargetRow>(conn)
                .await?;
            diesel::delete(remote_targets.filter(id.eq(self.id())))
                .execute(conn)
                .await?;
            let event = remote_target_event(
                &before,
                Action::Deleted,
                context,
                format!("Remote target '{}' deleted", before.name),
            )?
            .with_before(before.audit_snapshot());
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub(crate) async fn emit_remote_target_invoked_event(
    pool: &crate::storage::postgres::PostgresPool,
    target_id: i32,
    context: &EventContext,
    task_id: i32,
    subject_type: &str,
    subject_id: i32,
) -> Result<(), ApiError> {
    with_connection(pool, async |conn| -> Result<(), ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};
        let target = remote_targets
            .filter(id.eq(target_id))
            .first::<RemoteTargetRow>(conn)
            .await?;
        let event = NewEvent::new(
            EntityType::RemoteTarget,
            Action::Invoked,
            context.actor_kind(),
            format!("Remote target '{}' invoked", target.name),
        )?
        .with_context(context)
        .with_entity_id(target.id)
        .with_entity_name(target.name.clone())
        .with_collection_id(target.collection_id)
        .with_metadata(serde_json::json!({
            "task_id": task_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
        }));
        emit_event(conn, &event).await?;
        Ok(())
    })
    .await
}

pub(crate) async fn list_rows_with_total_count(
    pool: &crate::storage::postgres::PostgresPool,
    allowed_collection_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<(Vec<RemoteTargetRow>, i64), ApiError> {
    let query = build_list_query(allowed_collection_ids, query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut query = build_list_query(allowed_collection_ids, query_options)?;
    apply_query_options!(query, query_options, RemoteTargetRow);
    let rows =
        with_connection(pool, async |conn| query.load::<RemoteTargetRow>(conn).await).await?;

    Ok((rows, total_count))
}

fn build_list_query<'a>(
    allowed_collection_ids: &'a [i32],
    query_options: &'a QueryOptions,
) -> Result<crate::schema::remote_targets::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::remote_targets::dsl::{
        class_id, collection_id, created_at, description, id, method, name, remote_targets,
        revision, updated_at,
    };

    let mut query = remote_targets
        .into_boxed()
        .filter(collection_id.eq_any(allowed_collection_ids));

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, id),
            FilterField::Name => string_search!(query, param, operator, name),
            FilterField::Description => string_search!(query, param, operator, description),
            FilterField::CollectionId | FilterField::Collections => {
                numeric_search!(query, param, operator, collection_id)
            }
            FilterField::ClassId => numeric_search!(query, param, operator, class_id),
            FilterField::Kind => string_search!(query, param, operator, method),
            FilterField::CreatedAt => date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(query, param, operator, updated_at),
            FilterField::Revision => revision_search!(query, param, operator, revision),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for remote targets",
                    param.field
                )));
            }
        }
    }

    Ok(query)
}

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
