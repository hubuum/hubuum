use std::num::NonZeroI64;

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    ClassHistoryRecord, CollectionHistoryRecord, ExportTemplateHistoryRecord, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryPage, HistoryPrincipalName,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, RemoteTargetHistoryRecord,
};
use serde_json::Value;

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

const SKIPPED_TOTAL_COUNT: i64 = -1;

#[derive(diesel::Queryable)]
#[diesel(table_name = crate::schema::collections_history)]
struct CollectionHistoryRow {
    id: i32,
    name: String,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    parent_collection_id: Option<i32>,
    op: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: PostgresRevision,
}

#[derive(diesel::Queryable)]
#[diesel(table_name = crate::schema::hubuumclass_history)]
struct ClassHistoryRow {
    id: i32,
    name: String,
    collection_id: i32,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    op: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: PostgresRevision,
}

#[derive(diesel::Queryable)]
#[diesel(table_name = crate::schema::hubuumobject_history)]
struct ObjectHistoryRow {
    id: i32,
    name: String,
    collection_id: i32,
    hubuum_class_id: i32,
    data: Value,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    op: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: PostgresRevision,
}

#[derive(diesel::Queryable)]
#[diesel(table_name = crate::schema::export_templates_history)]
struct ExportTemplateHistoryRow {
    id: i32,
    collection_id: i32,
    name: String,
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<Value>,
    relation_context: Option<Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<Value>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    op: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: PostgresRevision,
}

#[derive(diesel::Queryable)]
#[diesel(table_name = crate::schema::remote_targets_history)]
struct RemoteTargetHistoryRow {
    id: i32,
    collection_id: i32,
    class_id: Option<i32>,
    name: String,
    description: String,
    method: String,
    url_template: String,
    headers_template: Value,
    body_template: Option<String>,
    auth_config: Value,
    allowed_subject_types: Value,
    timeout_ms: i32,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    op: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: PostgresRevision,
}

macro_rules! metadata {
    ($row:expr) => {{
        let row = &$row;
        HistoryMetadata::new(
            row.op.clone(),
            row.valid_from,
            row.valid_to,
            row.history_id,
            NonZeroI64::new(row.revision.get())
                .expect("PostgresRevision always contains a positive value"),
        )
        .actor(row.actor_id, row.actor_kind.clone())
        .initiator_principal_id(row.initiator_user_id)
        .task_id(row.task_id)
    }};
}

impl From<CollectionHistoryRow> for CollectionHistoryRecord {
    fn from(row: CollectionHistoryRow) -> Self {
        let metadata = metadata!(row);
        Self::new(
            row.id,
            row.name,
            row.description,
            row.created_at,
            row.updated_at,
            row.parent_collection_id,
            metadata,
        )
    }
}

impl From<ClassHistoryRow> for ClassHistoryRecord {
    fn from(row: ClassHistoryRow) -> Self {
        let metadata = metadata!(row);
        Self::new(
            row.id,
            row.name,
            row.collection_id,
            row.json_schema,
            row.validate_schema,
            row.description,
            row.created_at,
            row.updated_at,
            metadata,
        )
    }
}

impl From<ObjectHistoryRow> for ObjectHistoryRecord {
    fn from(row: ObjectHistoryRow) -> Self {
        let metadata = metadata!(row);
        Self::new(
            row.id,
            row.name,
            row.collection_id,
            row.hubuum_class_id,
            row.data,
            row.description,
            row.created_at,
            row.updated_at,
            metadata,
        )
    }
}

impl From<ExportTemplateHistoryRow> for ExportTemplateHistoryRecord {
    fn from(row: ExportTemplateHistoryRow) -> Self {
        let metadata = metadata!(row);
        Self::new(
            row.id,
            row.collection_id,
            row.name,
            row.description,
            row.content_type,
            row.template,
            row.kind,
            row.scope_kind,
            row.class_id,
            row.default_query,
            row.include,
            row.relation_context,
            row.default_missing_data_policy,
            row.default_limits,
            row.created_at,
            row.updated_at,
            metadata,
        )
    }
}

impl From<RemoteTargetHistoryRow> for RemoteTargetHistoryRecord {
    fn from(row: RemoteTargetHistoryRow) -> Self {
        let metadata = metadata!(row);
        Self::new(
            row.id,
            row.collection_id,
            row.class_id,
            row.name,
            row.description,
            row.method,
            row.url_template,
            row.headers_template,
            row.body_template,
            row.auth_config,
            row.allowed_subject_types,
            row.timeout_ms,
            row.enabled,
            row.created_at,
            row.updated_at,
            metadata,
        )
    }
}

pub async fn resolve_principal_names(
    runtime: &PostgresRuntime,
    mut principal_ids: Vec<i32>,
) -> Result<Vec<HistoryPrincipalName>, PostgresStorageError> {
    principal_ids.sort_unstable();
    principal_ids.dedup();
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    runtime
        .with_connection(async |connection| {
            use crate::schema::principals::dsl::{id, name as principal_name, principals};

            principals
                .filter(id.eq_any(principal_ids))
                .select((id, principal_name))
                .load::<(i32, String)>(connection)
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|(principal_id, name)| HistoryPrincipalName::new(principal_id, name))
                        .collect()
                })
        })
        .await
}

fn history_cursor_fields(
    options: &QueryOptions,
    table: &'static str,
) -> Result<Vec<CursorSqlField<String>>, PostgresStorageError> {
    options
        .sort
        .iter()
        .map(|sort| {
            let column = match sort.field {
                FilterField::HistoryId => "history_id",
                FilterField::Revision => "revision",
                ref field => {
                    return Err(PostgresStorageError::bad_request(format!(
                        "Field '{field}' is not orderable for history"
                    )));
                }
            };
            Ok(CursorSqlField {
                column: format!("{table}.{column}"),
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            })
        })
        .collect()
}

fn validate_history_filters(options: &QueryOptions) -> Result<(), PostgresStorageError> {
    for parameter in &options.filters {
        if parameter.field != FilterField::Revision {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{}' is not searchable for history",
                parameter.field
            )));
        }
    }
    Ok(())
}

macro_rules! history_operations {
    (
        $list_fn:ident,
        $as_of_fn:ident,
        $table:ident,
        $table_name:literal,
        $row:ty,
        $record:ty,
        $visibility_column:ident
    ) => {
        pub async fn $list_fn(
            runtime: &PostgresRuntime,
            query: HistoryListQuery,
        ) -> Result<HistoryPage<$record>, PostgresStorageError> {
            let (entity_id, options, scope) = query.into_parts();
            validate_history_filters(&options)?;
            let fields = history_cursor_fields(&options, $table_name)?;
            runtime
                .with_read_only_snapshot(async |connection| {
                    use crate::schema::$table::dsl::*;

                    let mut count_query = crate::schema::$table::table
                        .into_boxed()
                        .filter(id.eq(entity_id));
                    if let HistoryCollectionScope::Visible(collection_ids) = &scope {
                        count_query = count_query.filter($visibility_column.eq_any(collection_ids));
                    }
                    for parameter in &options.filters {
                        crate::postgres_revision_filter!(count_query, parameter, revision);
                    }
                    let total = if options.include_total {
                        count_query.count().get_result::<i64>(connection).await?
                    } else {
                        SKIPPED_TOTAL_COUNT
                    };

                    let mut records = crate::schema::$table::table
                        .into_boxed()
                        .filter(id.eq(entity_id));
                    if let HistoryCollectionScope::Visible(collection_ids) = &scope {
                        records = records.filter($visibility_column.eq_any(collection_ids));
                    }
                    for parameter in &options.filters {
                        crate::postgres_revision_filter!(records, parameter, revision);
                    }
                    crate::apply_query_options_with_fields!(records, options, fields);
                    let rows = records
                        .load::<$row>(connection)
                        .await?
                        .into_iter()
                        .map(<$record>::from)
                        .collect();
                    Ok::<_, PostgresStorageError>(HistoryPage::new(rows, total))
                })
                .await
        }

        pub async fn $as_of_fn(
            runtime: &PostgresRuntime,
            query: HistoryAsOfQuery,
        ) -> Result<Option<$record>, PostgresStorageError> {
            let (entity_id, at) = query.into_parts();
            runtime
                .with_connection(async |connection| {
                    use crate::schema::$table::dsl::*;

                    crate::schema::$table::table
                        .filter(id.eq(entity_id))
                        .filter(valid_from.le(at))
                        .filter(valid_to.is_null().or(valid_to.gt(at)))
                        .order(history_id.desc())
                        .first::<$row>(connection)
                        .await
                        .optional()
                        .map(|row| row.map(<$record>::from))
                })
                .await
        }
    };
}

history_operations!(
    list_collection_history,
    collection_history_as_of,
    collections_history,
    "collections_history",
    CollectionHistoryRow,
    CollectionHistoryRecord,
    id
);

history_operations!(
    list_class_history,
    class_history_as_of,
    hubuumclass_history,
    "hubuumclass_history",
    ClassHistoryRow,
    ClassHistoryRecord,
    collection_id
);

history_operations!(
    list_export_template_history,
    export_template_history_as_of,
    export_templates_history,
    "export_templates_history",
    ExportTemplateHistoryRow,
    ExportTemplateHistoryRecord,
    collection_id
);

history_operations!(
    list_remote_target_history,
    remote_target_history_as_of,
    remote_targets_history,
    "remote_targets_history",
    RemoteTargetHistoryRow,
    RemoteTargetHistoryRecord,
    collection_id
);

pub async fn list_object_history(
    runtime: &PostgresRuntime,
    query: ObjectHistoryListQuery,
) -> Result<HistoryPage<ObjectHistoryRecord>, PostgresStorageError> {
    let (object_id, class_id, options, scope) = query.into_parts();
    validate_history_filters(&options)?;
    let fields = history_cursor_fields(&options, "hubuumobject_history")?;
    runtime
        .with_read_only_snapshot(async |connection| {
            use crate::schema::hubuumobject_history::dsl as history;

            let mut count_query = history::hubuumobject_history
                .into_boxed()
                .filter(history::id.eq(object_id))
                .filter(history::hubuum_class_id.eq(class_id));
            if let HistoryCollectionScope::Visible(collection_ids) = &scope {
                count_query = count_query.filter(history::collection_id.eq_any(collection_ids));
            }
            for parameter in &options.filters {
                crate::postgres_revision_filter!(count_query, parameter, history::revision);
            }
            let total = if options.include_total {
                count_query.count().get_result::<i64>(connection).await?
            } else {
                SKIPPED_TOTAL_COUNT
            };

            let mut records = history::hubuumobject_history
                .into_boxed()
                .filter(history::id.eq(object_id))
                .filter(history::hubuum_class_id.eq(class_id));
            if let HistoryCollectionScope::Visible(collection_ids) = &scope {
                records = records.filter(history::collection_id.eq_any(collection_ids));
            }
            for parameter in &options.filters {
                crate::postgres_revision_filter!(records, parameter, history::revision);
            }
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .load::<ObjectHistoryRow>(connection)
                .await?
                .into_iter()
                .map(ObjectHistoryRecord::from)
                .collect();
            Ok::<_, PostgresStorageError>(HistoryPage::new(rows, total))
        })
        .await
}

pub async fn object_history_as_of(
    runtime: &PostgresRuntime,
    query: ObjectHistoryAsOfQuery,
) -> Result<Option<ObjectHistoryRecord>, PostgresStorageError> {
    let (object_id, class_id, at) = query.into_parts();
    runtime
        .with_connection(async |connection| {
            use crate::schema::hubuumobject_history::dsl as history;

            history::hubuumobject_history
                .filter(history::id.eq(object_id))
                .filter(history::hubuum_class_id.eq(class_id))
                .filter(history::valid_from.le(at))
                .filter(history::valid_to.is_null().or(history::valid_to.gt(at)))
                .order(history::history_id.desc())
                .first::<ObjectHistoryRow>(connection)
                .await
                .optional()
                .map(|row| row.map(ObjectHistoryRecord::from))
        })
        .await
}
