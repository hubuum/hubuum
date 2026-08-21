use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, CollectionId, ObjectId, PrincipalId, ResourceId};

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
    StorageContext, StorageHistoryOperation, StorageRemoteTargetTransportParts, storage_handle,
};

/// Collection visibility applied before history rows are counted or paged.
#[derive(Clone, Copy)]
pub enum HistoryCollectionFilter<'a> {
    All,
    Visible(&'a [i32]),
}

fn exact_history_total(total: Option<i64>) -> Result<i64, ApiError> {
    total.ok_or_else(|| {
        ApiError::InternalServerError(
            "Storage history query omitted its required exact total".to_string(),
        )
    })
}

impl TryFrom<HistoryCollectionFilter<'_>> for HistoryCollectionScope {
    type Error = ApiError;

    fn try_from(value: HistoryCollectionFilter<'_>) -> Result<Self, Self::Error> {
        match value {
            HistoryCollectionFilter::All => Ok(Self::All),
            HistoryCollectionFilter::Visible(collection_ids) => Ok(Self::Visible(
                collection_ids
                    .iter()
                    .copied()
                    .map(CollectionId::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
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

impl From<HistoryMetadata> for AppHistoryMetadata {
    fn from(value: HistoryMetadata) -> Self {
        let parts = value.into_parts();
        // Preserve the established HTTP history representation while the
        // storage contract uses semantic operation values.
        let operation = match parts.operation {
            StorageHistoryOperation::Create => "I",
            StorageHistoryOperation::Update => "U",
            StorageHistoryOperation::Delete => "D",
        }
        .to_string();
        Self {
            operation,
            valid_from: parts.valid_from,
            valid_to: parts.valid_to,
            actor_id: parts.actor_id.map(|id| id.id()),
            history_id: parts.history_entry_id.id(),
            actor_kind: parts.actor_kind,
            initiator_principal_id: parts.initiator_principal_id.map(|id| id.id()),
            task_id: parts.task_id.map(|id| id.id()),
            revision: parts.revision,
        }
    }
}

fn collection_from_storage(row: CollectionHistoryRecord) -> Result<CollectionHistory, ApiError> {
    let (record, metadata) = row.into_parts();
    let (id, name, description, created_at, updated_at, parent_collection_id, _) =
        record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(CollectionHistory {
        id: id.id(),
        name,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        parent_collection_id: parent_collection_id.map(|id| id.id()),
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
    let (record, metadata) = row.into_parts();
    let (
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        _,
    ) = record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(HubuumClassHistory {
        id: id.id(),
        name,
        collection_id: collection_id.id(),
        json_schema,
        validate_schema,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
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
    let (record, metadata) = row.into_parts();
    let (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, _) =
        record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(HubuumObjectHistory {
        id: id.id(),
        name,
        collection_id: collection_id.id(),
        hubuum_class_id: hubuum_class_id.id(),
        data,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
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
    let (record, metadata) = row.into_parts();
    let (record_metadata, collection_id, name, definition) = record.into_parts();
    let definition = definition.into_parts();
    let (id, created_at, updated_at, _) = record_metadata.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(ExportTemplateHistory {
        id: id.id(),
        collection_id: collection_id.id(),
        name,
        description: definition.description().to_string(),
        content_type: definition.content_type().to_string(),
        template: definition.template().to_string(),
        kind: definition.kind().to_string(),
        scope_kind: definition.scope_kind().map(str::to_string),
        class_id: definition.class_id().map(|id| id.id()),
        default_query: definition.default_query().map(str::to_string),
        include: definition.include().cloned(),
        relation_context: definition.relation_context().cloned(),
        default_missing_data_policy: definition.default_missing_data_policy().map(str::to_string),
        default_limits: definition.default_limits().cloned(),
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
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
    let (record, metadata) = row.into_parts();
    let (record_metadata, collection_id, name, definition) = record.into_parts();
    let (description, transport, policy) = definition.into_parts();
    let StorageRemoteTargetTransportParts {
        method,
        url_template,
        headers_template,
        body_template,
        auth_config,
        timeout_ms,
    } = transport.into_parts();
    let (class_id, allowed_subject_types, enabled) = policy.into_parts();
    let (id, created_at, updated_at, _) = record_metadata.into_parts();
    let allowed_subject_types = serde_json::to_value(
        allowed_subject_types
            .into_iter()
            .map(|subject_type| subject_type.as_str())
            .collect::<Vec<_>>(),
    )?;
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(RemoteTargetHistory {
        id: id.id(),
        collection_id: collection_id.id(),
        class_id: class_id.map(|id| id.id()),
        name,
        description,
        method: method.as_str().to_string(),
        url_template,
        headers_template,
        body_template,
        auth_config,
        allowed_subject_types,
        timeout_ms,
        enabled,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
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
    let principal_ids = principal_ids
        .into_iter()
        .map(PrincipalId::new)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(storage_handle(backend)
        .resolve_history_principal_names(principal_ids)
        .await?
        .into_iter()
        .map(|row| {
            let (principal_id, name) = row.into_parts();
            (principal_id.id(), name)
        })
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
                    ResourceId::new(entity_id)?,
                    query_options.clone(),
                    collection_filter.try_into()?,
                ))
                .await?
                .into_parts();
            Ok((
                rows.into_iter().map($from).collect::<Result<_, _>>()?,
                exact_history_total(total_count)?,
            ))
        }

        pub async fn $as_of(
            entity_id: i32,
            at: DateTime<Utc>,
            backend: &impl StorageContext,
        ) -> Result<Option<$ty>, ApiError> {
            storage_handle(backend)
                .$storage_as_of(HistoryAsOfQuery::new(ResourceId::new(entity_id)?, at))
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
    get_collection_history_as_of,
    collection_from_storage,
    CollectionHistory
);
history_service!(
    class_history_paginated_with_total_count,
    class_as_of,
    list_class_history,
    get_class_history_as_of,
    class_from_storage,
    HubuumClassHistory
);
history_service!(
    export_template_history_paginated_with_total_count,
    export_template_as_of,
    list_export_template_history,
    get_export_template_history_as_of,
    export_template_from_storage,
    ExportTemplateHistory
);
history_service!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    list_remote_target_history,
    get_remote_target_history_as_of,
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
            ObjectId::new(object_id)?,
            ClassId::new(class_id)?,
            query_options.clone(),
            collection_filter.try_into()?,
        ))
        .await?
        .into_parts();
    Ok((
        rows.into_iter()
            .map(object_from_storage)
            .collect::<Result<_, _>>()?,
        exact_history_total(total_count)?,
    ))
}

pub async fn object_as_of(
    object_id: i32,
    class_id: i32,
    at: DateTime<Utc>,
    backend: &impl StorageContext,
) -> Result<Option<HubuumObjectHistory>, ApiError> {
    storage_handle(backend)
        .get_object_history_as_of(ObjectHistoryAsOfQuery::new(
            ObjectId::new(object_id)?,
            ClassId::new(class_id)?,
            at,
        ))
        .await?
        .map(object_from_storage)
        .transpose()
}
