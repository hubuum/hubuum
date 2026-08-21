use std::str::FromStr;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::QueryOptions;
use crate::models::{
    NewRemoteTarget, RemoteHttpMethod, RemoteTarget, RemoteTargetSubjectType, UpdateRemoteTarget,
    validate_target_parts,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::services::storage_boundary::{
    class_id_to_storage, collection_id_to_storage, resource_id_to_storage,
};
use crate::storage::{
    RemoteTargetStorage, StorageContext, StorageRemoteHttpMethod, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDefinition, StorageRemoteTargetDelete,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetPatch,
    StorageRemoteTargetPolicy, StorageRemoteTargetSubjectType, StorageRemoteTargetTransport,
    StorageRemoteTargetTransportParts, StorageRemoteTargetUpdate, storage_handle,
};

pub(crate) async fn get_remote_target(
    backend: &impl StorageContext,
    target_id: i32,
) -> Result<RemoteTarget, ApiError> {
    let target = storage_handle(backend)
        .get_remote_target(
            hubuum_domain::RemoteTargetId::new(target_id)
                .expect("validated remote target id must be positive"),
        )
        .await?;
    target_from_storage(target)
}

pub(crate) async fn list_remote_targets(
    backend: &impl StorageContext,
    allowed_collection_ids: Vec<i32>,
    options: QueryOptions,
) -> Result<(Vec<RemoteTarget>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_remote_targets(StorageRemoteTargetListQuery::new(
            allowed_collection_ids
                .into_iter()
                .map(collection_id_to_storage)
                .collect(),
            options,
        ))
        .await?;
    let (targets, total) = page.into_parts();
    Ok((
        targets
            .into_iter()
            .map(target_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn create_remote_target(
    backend: &impl StorageContext,
    input: NewRemoteTarget,
    event_context: EventContext,
) -> Result<RemoteTarget, ApiError> {
    validate_target_parts(
        input.class_id.map(|class_id| class_id.id()),
        &input.url_template,
        &input.headers_template,
        input.body_template.as_deref(),
        &input.auth_config,
        &input.allowed_subject_types,
        input.timeout_ms,
    )?;
    let definition = StorageRemoteTargetDefinition::new(
        input.description,
        StorageRemoteTargetTransport::try_new(
            http_method_to_storage(input.method),
            input.url_template,
            input.headers_template,
            input.body_template,
            serde_json::to_value(input.auth_config)?,
            input.timeout_ms,
        )?,
        StorageRemoteTargetPolicy::try_new(
            input
                .class_id
                .map(|class_id| class_id_to_storage(class_id.id())),
            subject_types_to_storage(input.allowed_subject_types),
            input.enabled,
        )?,
    );
    let target = storage_handle(backend)
        .create_remote_target(StorageRemoteTargetCreate::new(
            collection_id_to_storage(input.collection_id.id()),
            input.name,
            definition,
            event_context,
        ))
        .await?;
    target_from_storage(target.into_value())
}

pub(crate) async fn update_remote_target(
    backend: &impl StorageContext,
    target_id: i32,
    update: UpdateRemoteTarget,
    existing: &RemoteTarget,
    event_context: EventContext,
) -> Result<RemoteTarget, ApiError> {
    let effective_url_template = update
        .url_template
        .as_deref()
        .unwrap_or(&existing.url_template);
    let effective_headers_template = update
        .headers_template
        .as_ref()
        .unwrap_or(&existing.headers_template);
    let effective_body_template = update
        .body_template
        .as_ref()
        .map_or(existing.body_template.as_deref(), |value| value.as_deref());
    let effective_auth_config = update.auth_config.as_ref().unwrap_or(&existing.auth_config);
    let effective_allowed_subject_types = update
        .allowed_subject_types
        .as_deref()
        .unwrap_or(&existing.allowed_subject_types);
    let effective_class_id = match update.class_id {
        Some(Some(class_id)) => Some(class_id.id()),
        Some(None) => None,
        None => existing.class_id,
    };
    validate_target_parts(
        effective_class_id,
        effective_url_template,
        effective_headers_template,
        effective_body_template,
        effective_auth_config,
        effective_allowed_subject_types,
        update.timeout_ms.unwrap_or(existing.timeout_ms),
    )?;

    let patch = StorageRemoteTargetPatch::new()
        .with_collection_id(
            update
                .collection_id
                .map(|collection_id| collection_id_to_storage(collection_id.id())),
        )
        .with_class_id(
            update
                .class_id
                .map(|class_id| class_id.map(|class_id| class_id_to_storage(class_id.id()))),
        )
        .with_name(update.name)
        .with_description(update.description)
        .with_method(update.method.map(http_method_to_storage))
        .with_url_template(update.url_template)
        .with_headers_template(update.headers_template)
        .with_body_template(update.body_template)
        .with_auth_config(update.auth_config.map(serde_json::to_value).transpose()?)
        .with_allowed_subject_types(update.allowed_subject_types.map(subject_types_to_storage))
        .with_timeout_ms(update.timeout_ms)
        .with_enabled(update.enabled);
    let target = storage_handle(backend)
        .update_remote_target(StorageRemoteTargetUpdate::new(
            hubuum_domain::RemoteTargetId::new(target_id)
                .expect("validated remote target id must be positive"),
            patch,
            event_context,
        ))
        .await?;
    target_from_storage(target.into_value())
}

pub(crate) async fn delete_remote_target(
    backend: &impl StorageContext,
    target_id: i32,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .delete_remote_target(StorageRemoteTargetDelete::new(
            hubuum_domain::RemoteTargetId::new(target_id)
                .expect("validated remote target id must be positive"),
            event_context,
        ))
        .await?
        .into_value();
    Ok(())
}

pub(crate) async fn record_remote_target_invocation(
    backend: &impl StorageContext,
    target_id: i32,
    task_id: i32,
    subject_type: RemoteTargetSubjectType,
    subject_id: i32,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .record_remote_target_invocation(StorageRemoteTargetInvocation::new(
            hubuum_domain::RemoteTargetId::new(target_id)
                .expect("validated remote target id must be positive"),
            hubuum_domain::TaskId::new(task_id).expect("validated task id must be positive"),
            subject_type_to_storage(subject_type),
            resource_id_to_storage(subject_id),
            event_context,
        ))
        .await?
        .into_value();
    Ok(())
}

fn http_method_to_storage(method: RemoteHttpMethod) -> StorageRemoteHttpMethod {
    match method {
        RemoteHttpMethod::Get => StorageRemoteHttpMethod::Get,
        RemoteHttpMethod::Post => StorageRemoteHttpMethod::Post,
        RemoteHttpMethod::Patch => StorageRemoteHttpMethod::Patch,
        RemoteHttpMethod::Delete => StorageRemoteHttpMethod::Delete,
    }
}

fn subject_type_to_storage(
    subject_type: RemoteTargetSubjectType,
) -> StorageRemoteTargetSubjectType {
    match subject_type {
        RemoteTargetSubjectType::Collection => StorageRemoteTargetSubjectType::Collection,
        RemoteTargetSubjectType::Class => StorageRemoteTargetSubjectType::Class,
        RemoteTargetSubjectType::Object => StorageRemoteTargetSubjectType::Object,
        RemoteTargetSubjectType::ClassRelation => StorageRemoteTargetSubjectType::ClassRelation,
        RemoteTargetSubjectType::ObjectRelation => StorageRemoteTargetSubjectType::ObjectRelation,
    }
}

fn subject_types_to_storage(
    subject_types: Vec<RemoteTargetSubjectType>,
) -> Vec<StorageRemoteTargetSubjectType> {
    subject_types
        .into_iter()
        .map(subject_type_to_storage)
        .collect()
}

fn target_from_storage(target: StorageRemoteTarget) -> Result<RemoteTarget, ApiError> {
    let (metadata, collection_id, name, definition) = target.into_parts();
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
    Ok(RemoteTarget {
        id: metadata.id().id(),
        collection_id: collection_id.id(),
        class_id: class_id.map(|id| id.id()),
        name,
        description,
        method: RemoteHttpMethod::from_str(method.as_str())?,
        url_template,
        headers_template,
        body_template,
        auth_config: serde_json::from_value(auth_config)?,
        allowed_subject_types: allowed_subject_types
            .into_iter()
            .map(|subject_type| RemoteTargetSubjectType::from_str(subject_type.as_str()))
            .collect::<Result<Vec<_>, _>>()?,
        timeout_ms,
        enabled,
        created_at: metadata.created_at().naive_utc(),
        updated_at: metadata.updated_at().naive_utc(),
        revision: metadata.revision(),
    })
}
