use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId, HistoryRecordId, PrincipalId, TaskId};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    ClassHistoryRecord, CollectionHistoryRecord, ExportTemplateHistoryRecord, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryPrincipalName,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, RemoteTargetHistoryRecord,
    StorageClassRecord, StorageCollection, StorageExportTemplate, StorageExportTemplateDefinition,
    StorageHistoryOperation, StorageObject, StoragePage, StorageRemoteTarget,
    StorageRemoteTargetDefinition, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
};
use serde_json::Value;

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::revision::record_metadata;
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

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
        Ok::<_, PostgresStorageError>(
            HistoryMetadata::try_new(
                history_operation(&row.op)?,
                row.valid_from,
                row.valid_to,
                HistoryRecordId::new(row.history_id)?,
                row.revision.into_domain(),
            )?
            .actor(
                row.actor_id.map(PrincipalId::new).transpose()?,
                row.actor_kind.clone(),
            )
            .initiator_principal_id(row.initiator_user_id.map(PrincipalId::new).transpose()?)
            .task_id(row.task_id.map(TaskId::new).transpose()?),
        )
    }};
}

fn history_operation(value: &str) -> Result<StorageHistoryOperation, PostgresStorageError> {
    match value {
        "I" => Ok(StorageHistoryOperation::Create),
        "U" => Ok(StorageHistoryOperation::Update),
        "D" => Ok(StorageHistoryOperation::Delete),
        _ => Err(PostgresStorageError::database(format!(
            "Invalid persisted history operation '{value}'"
        ))),
    }
}

impl TryFrom<CollectionHistoryRow> for CollectionHistoryRecord {
    type Error = PostgresStorageError;

    fn try_from(row: CollectionHistoryRow) -> Result<Self, Self::Error> {
        let metadata = metadata!(row)?;
        let record = StorageCollection::new(
            record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
            row.name,
            row.description,
            row.parent_collection_id
                .map(CollectionId::new)
                .transpose()?,
        );
        Ok(Self::new(record, metadata))
    }
}

impl TryFrom<ClassHistoryRow> for ClassHistoryRecord {
    type Error = PostgresStorageError;

    fn try_from(row: ClassHistoryRow) -> Result<Self, Self::Error> {
        let metadata = metadata!(row)?;
        let record = StorageClassRecord::builder(
            record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
            row.name,
            CollectionId::new(row.collection_id)?,
            row.description,
        )
        .json_schema(row.json_schema)
        .validate_schema(row.validate_schema)
        .build();
        Ok(Self::new(record, metadata))
    }
}

impl TryFrom<ObjectHistoryRow> for ObjectHistoryRecord {
    type Error = PostgresStorageError;

    fn try_from(row: ObjectHistoryRow) -> Result<Self, Self::Error> {
        let metadata = metadata!(row)?;
        let record = StorageObject::new(
            record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
            row.name,
            CollectionId::new(row.collection_id)?,
            ClassId::new(row.hubuum_class_id)?,
            row.data,
            row.description,
        );
        Ok(Self::new(record, metadata))
    }
}

impl TryFrom<ExportTemplateHistoryRow> for ExportTemplateHistoryRecord {
    type Error = PostgresStorageError;

    fn try_from(row: ExportTemplateHistoryRow) -> Result<Self, Self::Error> {
        let metadata = metadata!(row)?;
        let definition = StorageExportTemplateDefinition::new(
            row.description,
            row.content_type,
            row.template,
            row.kind,
        )
        .with_scope(row.scope_kind, row.class_id.map(ClassId::new).transpose()?)
        .with_default_query(row.default_query)
        .with_include(row.include)
        .with_relation_context(row.relation_context)
        .with_default_missing_data_policy(row.default_missing_data_policy)
        .with_default_limits(row.default_limits);
        let record = StorageExportTemplate::new(
            record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
            CollectionId::new(row.collection_id)?,
            row.name,
            definition,
        );
        Ok(Self::new(record, metadata))
    }
}

impl TryFrom<RemoteTargetHistoryRow> for RemoteTargetHistoryRecord {
    type Error = PostgresStorageError;

    fn try_from(row: RemoteTargetHistoryRow) -> Result<Self, Self::Error> {
        let metadata = metadata!(row)?;
        let allowed_subject_types =
            serde_json::from_value::<Vec<String>>(row.allowed_subject_types).map_err(|error| {
                PostgresStorageError::database(format!(
                    "Invalid persisted remote-target subject policy: {error}"
                ))
            })?;
        let definition = StorageRemoteTargetDefinition::new(
            row.description,
            StorageRemoteTargetTransport::try_new(
                row.method,
                row.url_template,
                row.headers_template,
                row.body_template,
                row.auth_config,
                row.timeout_ms,
            )?,
            StorageRemoteTargetPolicy::try_new(
                row.class_id.map(ClassId::new).transpose()?,
                allowed_subject_types,
                row.enabled,
            )?,
        );
        let record = StorageRemoteTarget::new(
            record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
            CollectionId::new(row.collection_id)?,
            row.name,
            definition,
        );
        Ok(Self::new(record, metadata))
    }
}

pub async fn resolve_principal_names(
    runtime: &PostgresRuntime,
    mut principal_ids: Vec<PrincipalId>,
) -> Result<Vec<HistoryPrincipalName>, PostgresStorageError> {
    principal_ids.sort_unstable();
    principal_ids.dedup();
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    let principal_ids = principal_ids
        .into_iter()
        .map(PrincipalId::id)
        .collect::<Vec<_>>();
    runtime
        .with_connection(async |connection| {
            use crate::schema::principals::dsl::{id, name as principal_name, principals};

            principals
                .filter(id.eq_any(principal_ids))
                .select((id, principal_name))
                .load::<(i32, String)>(connection)
                .await
                .map_err(PostgresStorageError::from)?
                .into_iter()
                .map(|(principal_id, name)| {
                    Ok::<_, PostgresStorageError>(HistoryPrincipalName::new(
                        PrincipalId::new(principal_id)?,
                        name,
                    ))
                })
                .collect()
        })
        .await
}

fn history_cursor_fields(
    options: &QueryOptions,
    table: &'static str,
) -> Result<Vec<CursorSqlField<String>>, PostgresStorageError> {
    options
        .sort()
        .iter()
        .map(|sort| {
            let column = match sort.field {
                FilterField::HistoryId => "history_id",
                FilterField::Revision => "revision",
                ref field => {
                    return Err(PostgresStorageError::invalid_input(format!(
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
    for parameter in options.filters() {
        if parameter.field != FilterField::Revision {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{}' is not searchable for history",
                parameter.field
            )));
        }
    }
    Ok(())
}

fn visible_collection_ids(scope: &HistoryCollectionScope) -> Option<Vec<i32>> {
    match scope {
        HistoryCollectionScope::All => None,
        HistoryCollectionScope::Visible(ids) => {
            Some(ids.iter().copied().map(CollectionId::id).collect())
        }
    }
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
        ) -> Result<StoragePage<$record>, PostgresStorageError> {
            let (entity_id, options, scope) = query.into_parts();
            let visible_collection_ids = visible_collection_ids(&scope);
            validate_history_filters(&options)?;
            let fields = history_cursor_fields(&options, $table_name)?;
            runtime
                .with_read_only_snapshot(async |connection| {
                    use crate::schema::$table::dsl::*;

                    let mut count_query = crate::schema::$table::table
                        .into_boxed()
                        .filter(id.eq(entity_id.id()));
                    if let Some(collection_ids) = &visible_collection_ids {
                        count_query = count_query.filter($visibility_column.eq_any(collection_ids));
                    }
                    for parameter in options.filters() {
                        crate::postgres_revision_filter!(count_query, parameter, revision);
                    }
                    let total = if options.include_total() {
                        Some(count_query.count().get_result::<i64>(connection).await?)
                    } else {
                        None
                    };

                    let mut records = crate::schema::$table::table
                        .into_boxed()
                        .filter(id.eq(entity_id.id()));
                    if let Some(collection_ids) = &visible_collection_ids {
                        records = records.filter($visibility_column.eq_any(collection_ids));
                    }
                    for parameter in options.filters() {
                        crate::postgres_revision_filter!(records, parameter, revision);
                    }
                    crate::apply_query_options_with_fields!(records, options, fields);
                    let rows = records
                        .load::<$row>(connection)
                        .await?
                        .into_iter()
                        .map(<$record>::try_from)
                        .collect::<Result<Vec<_>, _>>()?;
                    StoragePage::try_new(rows, total).map_err(PostgresStorageError::from)
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
                        .filter(id.eq(entity_id.id()))
                        .filter(valid_from.le(at))
                        .filter(valid_to.is_null().or(valid_to.gt(at)))
                        .order(history_id.desc())
                        .first::<$row>(connection)
                        .await
                        .optional()
                        .map_err(PostgresStorageError::from)?
                        .map(<$record>::try_from)
                        .transpose()
                })
                .await
        }
    };
}

history_operations!(
    list_collection_history,
    get_collection_history_as_of,
    collections_history,
    "collections_history",
    CollectionHistoryRow,
    CollectionHistoryRecord,
    id
);

history_operations!(
    list_class_history,
    get_class_history_as_of,
    hubuumclass_history,
    "hubuumclass_history",
    ClassHistoryRow,
    ClassHistoryRecord,
    collection_id
);

history_operations!(
    list_export_template_history,
    get_export_template_history_as_of,
    export_templates_history,
    "export_templates_history",
    ExportTemplateHistoryRow,
    ExportTemplateHistoryRecord,
    collection_id
);

history_operations!(
    list_remote_target_history,
    get_remote_target_history_as_of,
    remote_targets_history,
    "remote_targets_history",
    RemoteTargetHistoryRow,
    RemoteTargetHistoryRecord,
    collection_id
);

pub async fn list_object_history(
    runtime: &PostgresRuntime,
    query: ObjectHistoryListQuery,
) -> Result<StoragePage<ObjectHistoryRecord>, PostgresStorageError> {
    let (object_id, class_id, options, scope) = query.into_parts();
    let visible_collection_ids = visible_collection_ids(&scope);
    validate_history_filters(&options)?;
    let fields = history_cursor_fields(&options, "hubuumobject_history")?;
    runtime
        .with_read_only_snapshot(async |connection| {
            use crate::schema::hubuumobject_history::dsl as history;

            let mut count_query = history::hubuumobject_history
                .into_boxed()
                .filter(history::id.eq(object_id.id()))
                .filter(history::hubuum_class_id.eq(class_id.id()));
            if let Some(collection_ids) = &visible_collection_ids {
                count_query = count_query.filter(history::collection_id.eq_any(collection_ids));
            }
            for parameter in options.filters() {
                crate::postgres_revision_filter!(count_query, parameter, history::revision);
            }
            let total = if options.include_total() {
                Some(count_query.count().get_result::<i64>(connection).await?)
            } else {
                None
            };

            let mut records = history::hubuumobject_history
                .into_boxed()
                .filter(history::id.eq(object_id.id()))
                .filter(history::hubuum_class_id.eq(class_id.id()));
            if let Some(collection_ids) = &visible_collection_ids {
                records = records.filter(history::collection_id.eq_any(collection_ids));
            }
            for parameter in options.filters() {
                crate::postgres_revision_filter!(records, parameter, history::revision);
            }
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .load::<ObjectHistoryRow>(connection)
                .await?
                .into_iter()
                .map(ObjectHistoryRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(rows, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn get_object_history_as_of(
    runtime: &PostgresRuntime,
    query: ObjectHistoryAsOfQuery,
) -> Result<Option<ObjectHistoryRecord>, PostgresStorageError> {
    let (object_id, class_id, at) = query.into_parts();
    runtime
        .with_connection(async |connection| {
            use crate::schema::hubuumobject_history::dsl as history;

            history::hubuumobject_history
                .filter(history::id.eq(object_id.id()))
                .filter(history::hubuum_class_id.eq(class_id.id()))
                .filter(history::valid_from.le(at))
                .filter(history::valid_to.is_null().or(history::valid_to.gt(at)))
                .order(history::history_id.desc())
                .first::<ObjectHistoryRow>(connection)
                .await
                .optional()
                .map_err(PostgresStorageError::from)?
                .map(ObjectHistoryRecord::try_from)
                .transpose()
        })
        .await
}
