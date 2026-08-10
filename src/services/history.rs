use chrono::{DateTime, Utc};

use crate::errors::ApiError;
use crate::events::PrincipalNames;
use crate::models::search::QueryOptions;
use crate::models::{
    CollectionHistory, ExportTemplateHistory, HubuumClassHistory, HubuumObjectHistory,
    RemoteTargetHistory, ResourceRevision,
};
use crate::storage::{
    ClassHistoryRecord, CollectionHistoryRecord, ExportTemplateHistoryRecord, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryStorage,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, RemoteTargetHistoryRecord,
    StorageContext, storage_handle,
};

/// Collection visibility applied before history rows are counted or paged.
#[derive(Clone, Copy)]
pub enum HistoryCollectionFilter<'a> {
    All,
    Visible(&'a [i32]),
}

impl From<HistoryCollectionFilter<'_>> for HistoryCollectionScope {
    fn from(value: HistoryCollectionFilter<'_>) -> Self {
        match value {
            HistoryCollectionFilter::All => Self::All,
            HistoryCollectionFilter::Visible(collection_ids) => {
                Self::Visible(collection_ids.to_vec())
            }
        }
    }
}

struct AppHistoryMetadata {
    operation: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_principal_id: Option<i32>,
    task_id: Option<i32>,
    revision: ResourceRevision,
}

impl TryFrom<HistoryMetadata> for AppHistoryMetadata {
    type Error = ApiError;

    fn try_from(value: HistoryMetadata) -> Result<Self, Self::Error> {
        let (
            operation,
            valid_from,
            valid_to,
            actor_id,
            history_id,
            actor_kind,
            initiator_principal_id,
            task_id,
            revision,
        ) = value.into_parts();
        Ok(Self {
            operation,
            valid_from,
            valid_to,
            actor_id,
            history_id,
            actor_kind,
            initiator_principal_id,
            task_id,
            revision: ResourceRevision::new(revision)?,
        })
    }
}

fn collection_from_storage(row: CollectionHistoryRecord) -> Result<CollectionHistory, ApiError> {
    let (id, name, description, created_at, updated_at, parent_collection_id, metadata) =
        row.into_parts();
    let metadata = AppHistoryMetadata::try_from(metadata)?;
    Ok(CollectionHistory {
        id,
        name,
        description,
        created_at,
        updated_at,
        parent_collection_id,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        revision: metadata.revision,
    })
}

fn class_from_storage(row: ClassHistoryRecord) -> Result<HubuumClassHistory, ApiError> {
    let (
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        metadata,
    ) = row.into_parts();
    let metadata = AppHistoryMetadata::try_from(metadata)?;
    Ok(HubuumClassHistory {
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        revision: metadata.revision,
    })
}

fn object_from_storage(row: ObjectHistoryRecord) -> Result<HubuumObjectHistory, ApiError> {
    let (
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        metadata,
    ) = row.into_parts();
    let metadata = AppHistoryMetadata::try_from(metadata)?;
    Ok(HubuumObjectHistory {
        id,
        name,
        collection_id,
        hubuum_class_id,
        data,
        description,
        created_at,
        updated_at,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        revision: metadata.revision,
    })
}

fn export_template_from_storage(
    row: ExportTemplateHistoryRecord,
) -> Result<ExportTemplateHistory, ApiError> {
    let (
        id,
        collection_id,
        name,
        description,
        content_type,
        template,
        kind,
        scope_kind,
        class_id,
        default_query,
        include,
        relation_context,
        default_missing_data_policy,
        default_limits,
        created_at,
        updated_at,
        metadata,
    ) = row.into_parts();
    let metadata = AppHistoryMetadata::try_from(metadata)?;
    Ok(ExportTemplateHistory {
        id,
        collection_id,
        name,
        description,
        content_type,
        template,
        kind,
        scope_kind,
        class_id,
        default_query,
        include,
        relation_context,
        default_missing_data_policy,
        default_limits,
        created_at,
        updated_at,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        revision: metadata.revision,
    })
}

fn remote_target_from_storage(
    row: RemoteTargetHistoryRecord,
) -> Result<RemoteTargetHistory, ApiError> {
    let (
        id,
        collection_id,
        class_id,
        name,
        description,
        method,
        url_template,
        headers_template,
        body_template,
        auth_config,
        allowed_subject_types,
        timeout_ms,
        enabled,
        created_at,
        updated_at,
        metadata,
    ) = row.into_parts();
    let metadata = AppHistoryMetadata::try_from(metadata)?;
    Ok(RemoteTargetHistory {
        id,
        collection_id,
        class_id,
        name,
        description,
        method,
        url_template,
        headers_template,
        body_template,
        auth_config,
        allowed_subject_types,
        timeout_ms,
        enabled,
        created_at,
        updated_at,
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        revision: metadata.revision,
    })
}

pub async fn resolve_principal_names(
    backend: &impl StorageContext,
    principal_ids: Vec<i32>,
) -> Result<PrincipalNames, ApiError> {
    Ok(storage_handle(backend)
        .resolve_history_principal_names(principal_ids)
        .await?
        .into_iter()
        .map(|row| row.into_parts())
        .collect())
}

macro_rules! history_service {
    ($list:ident, $as_of:ident, $storage_list:ident, $storage_as_of:ident, $from:ident, $ty:ty) => {
        pub async fn $list(
            entity_id: i32,
            backend: &impl StorageContext,
            query_options: &QueryOptions,
            collection_filter: HistoryCollectionFilter<'_>,
        ) -> Result<(Vec<$ty>, i64), ApiError> {
            let (rows, total_count) = storage_handle(backend)
                .$storage_list(HistoryListQuery::new(
                    entity_id,
                    query_options.clone(),
                    collection_filter.into(),
                ))
                .await?
                .into_parts();
            Ok((
                rows.into_iter().map($from).collect::<Result<_, _>>()?,
                total_count,
            ))
        }

        pub async fn $as_of(
            entity_id: i32,
            at: DateTime<Utc>,
            backend: &impl StorageContext,
        ) -> Result<Option<$ty>, ApiError> {
            storage_handle(backend)
                .$storage_as_of(HistoryAsOfQuery::new(entity_id, at))
                .await?
                .map($from)
                .transpose()
        }
    };
}

history_service!(
    collection_history_paginated_with_total_count,
    collection_as_of,
    list_collection_history,
    collection_history_as_of,
    collection_from_storage,
    CollectionHistory
);
history_service!(
    class_history_paginated_with_total_count,
    class_as_of,
    list_class_history,
    class_history_as_of,
    class_from_storage,
    HubuumClassHistory
);
history_service!(
    export_template_history_paginated_with_total_count,
    export_template_as_of,
    list_export_template_history,
    export_template_history_as_of,
    export_template_from_storage,
    ExportTemplateHistory
);
history_service!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    list_remote_target_history,
    remote_target_history_as_of,
    remote_target_from_storage,
    RemoteTargetHistory
);

pub async fn object_history_paginated_with_total_count(
    object_id: i32,
    class_id: i32,
    backend: &impl StorageContext,
    query_options: &QueryOptions,
    collection_filter: HistoryCollectionFilter<'_>,
) -> Result<(Vec<HubuumObjectHistory>, i64), ApiError> {
    let (rows, total_count) = storage_handle(backend)
        .list_object_history(ObjectHistoryListQuery::new(
            object_id,
            class_id,
            query_options.clone(),
            collection_filter.into(),
        ))
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(object_from_storage)
            .collect::<Result<_, _>>()?,
        total_count,
    ))
}

pub async fn object_as_of(
    object_id: i32,
    class_id: i32,
    at: DateTime<Utc>,
    backend: &impl StorageContext,
) -> Result<Option<HubuumObjectHistory>, ApiError> {
    storage_handle(backend)
        .object_history_as_of(ObjectHistoryAsOfQuery::new(object_id, class_id, at))
        .await?
        .map(object_from_storage)
        .transpose()
}
