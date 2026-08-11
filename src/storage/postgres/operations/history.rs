use crate::errors::ApiError;
use crate::events::PrincipalNames;
use crate::models::search::QueryOptions;
use crate::models::{CollectionHistory, HubuumClassHistory, HubuumObjectHistory, ResourceRevision};
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::with_connection;
use crate::storage::{
    ClassHistoryRecord, CollectionHistoryRecord, ExportTemplateHistoryRecord, HistoryMetadata,
    HistoryPrincipalName, ObjectHistoryRecord, RemoteTargetHistoryRecord,
};
use chrono::{DateTime, Utc};
use std::num::NonZeroI64;

/// Collection visibility to apply before history rows are counted or paginated.
#[derive(Clone, Copy)]
pub enum HistoryCollectionFilter<'a> {
    All,
    Visible(&'a [i32]),
}

#[derive(Queryable)]
#[diesel(table_name = crate::schema::export_templates_history)]
pub(crate) struct ExportTemplateHistoryRow {
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
    include: Option<serde_json::Value>,
    relation_context: Option<serde_json::Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<serde_json::Value>,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    op: String,
    valid_from: chrono::DateTime<chrono::Utc>,
    valid_to: Option<chrono::DateTime<chrono::Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: ResourceRevision,
}

crate::impl_history_pagination!(ExportTemplateHistoryRow, "export_templates_history");

#[derive(Queryable)]
#[diesel(table_name = crate::schema::remote_targets_history)]
pub(crate) struct RemoteTargetHistoryRow {
    id: i32,
    collection_id: i32,
    class_id: Option<i32>,
    name: String,
    description: String,
    method: String,
    url_template: String,
    headers_template: serde_json::Value,
    body_template: Option<String>,
    auth_config: serde_json::Value,
    allowed_subject_types: serde_json::Value,
    timeout_ms: i32,
    enabled: bool,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    op: String,
    valid_from: chrono::DateTime<chrono::Utc>,
    valid_to: Option<chrono::DateTime<chrono::Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: ResourceRevision,
}

crate::impl_history_pagination!(RemoteTargetHistoryRow, "remote_targets_history");

/// Batch-resolve principal ids for provenance responses (anonymized users keep
/// their tombstoned principal name; ids with no matching principal are absent).
pub(crate) async fn resolve_principal_names(
    pool: &impl crate::storage::StorageContext,
    principal_ids: Vec<i32>,
) -> Result<PrincipalNames, ApiError> {
    Ok(resolve_principal_name_rows(pool, principal_ids)
        .await?
        .into_iter()
        .collect())
}

pub(crate) async fn resolve_principal_name_rows(
    pool: &impl crate::storage::StorageContext,
    mut principal_ids: Vec<i32>,
) -> Result<Vec<(i32, String)>, ApiError> {
    use crate::schema::principals::dsl::{id, name, principals};
    principal_ids.sort_unstable();
    principal_ids.dedup();
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    with_connection(pool, async |conn| {
        principals
            .filter(id.eq_any(&principal_ids))
            .select((id, name))
            .load(conn)
            .await
    })
    .await
}

pub(crate) fn principal_name_to_storage(row: (i32, String)) -> HistoryPrincipalName {
    HistoryPrincipalName::new(row.0, row.1)
}

macro_rules! metadata_to_storage {
    ($row:expr) => {
        HistoryMetadata::new(
            $row.op,
            $row.valid_from,
            $row.valid_to,
            $row.history_id,
            NonZeroI64::new($row.revision.get())
                .expect("ResourceRevision always contains a positive value"),
        )
        .actor($row.actor_id, $row.actor_kind)
        .initiator_principal_id($row.initiator_user_id)
        .task_id($row.task_id)
    };
}

pub(crate) fn collection_history_to_storage(row: CollectionHistory) -> CollectionHistoryRecord {
    CollectionHistoryRecord::new(
        row.id,
        row.name,
        row.description,
        row.created_at,
        row.updated_at,
        row.parent_collection_id,
        metadata_to_storage!(row),
    )
}

pub(crate) fn class_history_to_storage(row: HubuumClassHistory) -> ClassHistoryRecord {
    ClassHistoryRecord::new(
        row.id,
        row.name,
        row.collection_id,
        row.json_schema,
        row.validate_schema,
        row.description,
        row.created_at,
        row.updated_at,
        metadata_to_storage!(row),
    )
}

pub(crate) fn object_history_to_storage(row: HubuumObjectHistory) -> ObjectHistoryRecord {
    ObjectHistoryRecord::new(
        row.id,
        row.name,
        row.collection_id,
        row.hubuum_class_id,
        row.data,
        row.description,
        row.created_at,
        row.updated_at,
        metadata_to_storage!(row),
    )
}

pub(crate) fn export_template_history_to_storage(
    row: ExportTemplateHistoryRow,
) -> ExportTemplateHistoryRecord {
    ExportTemplateHistoryRecord::new(
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
        metadata_to_storage!(row),
    )
}

pub(crate) fn remote_target_history_to_storage(
    row: RemoteTargetHistoryRow,
) -> RemoteTargetHistoryRecord {
    RemoteTargetHistoryRecord::new(
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
        metadata_to_storage!(row),
    )
}

macro_rules! history_db_fns {
    (
        $paginate_fn:ident,
        $as_of_fn:ident,
        $($schema:tt)::+,
        $visibility_column:ident,
        $ty:ty
    ) => {
        pub(crate) async fn $paginate_fn(
            entity_id: i32,
            pool: &impl $crate::storage::StorageContext,
            query_options: &$crate::models::search::QueryOptions,
            collection_filter: $crate::storage::postgres::operations::history::HistoryCollectionFilter<'_>,
        ) -> Result<(Vec<$ty>, i64), $crate::errors::ApiError> {
            use $crate::storage::postgres::prelude::*;
            use $($schema)::+::dsl::*;
            let total = $crate::pagination::exact_count_or_skipped(query_options, async || {
                $crate::storage::postgres::with_connection(pool, async |conn| -> Result<i64, $crate::errors::ApiError> {
                    let mut query = $($schema)::+::table
                        .into_boxed()
                        .filter(id.eq(entity_id));
                    if let $crate::storage::postgres::operations::history::HistoryCollectionFilter::Visible(
                        collection_ids,
                    ) = collection_filter
                    {
                        query = query.filter($visibility_column.eq_any(collection_ids));
                    }
                    for param in &query_options.filters {
                        let operator = param.operator.clone();
                        match param.field {
                            $crate::models::search::FilterField::Revision => {
                                $crate::revision_search!(query, param, operator, revision)
                            }
                            _ => return Err($crate::errors::ApiError::BadRequest(format!(
                                "Field '{}' is not searchable for history",
                                param.field
                            ))),
                        }
                    }
                    Ok(query
                        .count()
                        .get_result::<i64>(conn)
                        .await?)
                })
                .await
            }).await?;
            let mut query = $($schema)::+::table.into_boxed().filter(id.eq(entity_id));
            if let $crate::storage::postgres::operations::history::HistoryCollectionFilter::Visible(collection_ids) =
                collection_filter
            {
                query = query.filter($visibility_column.eq_any(collection_ids));
            }
            for param in &query_options.filters {
                let operator = param.operator.clone();
                match param.field {
                    $crate::models::search::FilterField::Revision => {
                        $crate::revision_search!(query, param, operator, revision)
                    }
                    _ => return Err($crate::errors::ApiError::BadRequest(format!(
                        "Field '{}' is not searchable for history",
                        param.field
                    ))),
                }
            }
            $crate::apply_query_options!(query, query_options, $ty);
            let items = $crate::storage::postgres::with_connection(pool, async |conn| {
                query.load::<$ty>(conn).await
            }).await?;
            Ok((items, total))
        }

        pub(crate) async fn $as_of_fn(
            entity_id: i32,
            at: chrono::DateTime<chrono::Utc>,
            pool: &impl $crate::storage::StorageContext,
        ) -> Result<Option<$ty>, $crate::errors::ApiError> {
            use $crate::storage::postgres::prelude::*;
            use $($schema)::+::dsl::*;
            $crate::storage::postgres::with_connection(pool, async |conn| {
                $($schema)::+::table
                    .into_boxed()
                    .filter(id.eq(entity_id))
                    .filter(valid_from.le(at))
                    .filter(valid_to.is_null().or(valid_to.gt(at)))
                    .order(history_id.desc())
                    .first::<$ty>(conn)
                    .await
                    .optional()
            })
            .await
        }
    };
}

history_db_fns!(
    collection_history_paginated_with_total_count,
    collection_as_of,
    crate::schema::collections_history,
    id,
    crate::models::CollectionHistory
);

history_db_fns!(
    class_history_paginated_with_total_count,
    class_as_of,
    crate::schema::hubuumclass_history,
    collection_id,
    crate::models::HubuumClassHistory
);

history_db_fns!(
    export_template_history_paginated_with_total_count,
    export_template_as_of,
    crate::schema::export_templates_history,
    collection_id,
    ExportTemplateHistoryRow
);

history_db_fns!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    crate::schema::remote_targets_history,
    collection_id,
    RemoteTargetHistoryRow
);

pub async fn object_history_paginated_with_total_count(
    object_id: i32,
    class_id: i32,
    pool: &impl crate::storage::StorageContext,
    query_options: &QueryOptions,
    collection_filter: HistoryCollectionFilter<'_>,
) -> Result<(Vec<crate::models::HubuumObjectHistory>, i64), ApiError> {
    use crate::schema::hubuumobject_history::dsl as history;

    let total = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| -> Result<i64, ApiError> {
            let mut query = history::hubuumobject_history
                .into_boxed()
                .filter(history::id.eq(object_id))
                .filter(history::hubuum_class_id.eq(class_id));
            if let HistoryCollectionFilter::Visible(collection_ids) = collection_filter {
                query = query.filter(history::collection_id.eq_any(collection_ids));
            }
            for param in &query_options.filters {
                let operator = param.operator.clone();
                match param.field {
                    crate::models::search::FilterField::Revision => {
                        crate::revision_search!(query, param, operator, history::revision)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' is not searchable for history",
                            param.field
                        )));
                    }
                }
            }
            Ok(query.count().get_result::<i64>(conn).await?)
        })
        .await
    })
    .await?;
    let mut query = history::hubuumobject_history
        .into_boxed()
        .filter(history::id.eq(object_id))
        .filter(history::hubuum_class_id.eq(class_id));
    if let HistoryCollectionFilter::Visible(collection_ids) = collection_filter {
        query = query.filter(history::collection_id.eq_any(collection_ids));
    }
    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            crate::models::search::FilterField::Revision => {
                crate::revision_search!(query, param, operator, history::revision)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not searchable for history",
                    param.field
                )));
            }
        }
    }
    crate::apply_query_options!(query, query_options, crate::models::HubuumObjectHistory);
    let items = with_connection(pool, async |conn| {
        query.load::<crate::models::HubuumObjectHistory>(conn).await
    })
    .await?;
    Ok((items, total))
}

pub async fn object_as_of(
    object_id: i32,
    class_id: i32,
    at: DateTime<Utc>,
    pool: &impl crate::storage::StorageContext,
) -> Result<Option<crate::models::HubuumObjectHistory>, ApiError> {
    use crate::schema::hubuumobject_history::dsl as history;

    with_connection(pool, async |conn| {
        history::hubuumobject_history
            .into_boxed()
            .filter(history::id.eq(object_id))
            .filter(history::hubuum_class_id.eq(class_id))
            .filter(history::valid_from.le(at))
            .filter(history::valid_to.is_null().or(history::valid_to.gt(at)))
            .order(history::history_id.desc())
            .first::<crate::models::HubuumObjectHistory>(conn)
            .await
            .optional()
    })
    .await
}
