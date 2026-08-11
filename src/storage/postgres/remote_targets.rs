use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::{NewRemoteTargetRow, RemoteTargetID, RemoteTargetRow, UpdateRemoteTargetRow};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    RemoteTargetStorage, StorageError, StorageRecordMetadata, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDefinition, StorageRemoteTargetDelete,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetPage,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate,
};

use super::PostgresStorage;
use super::error::map_postgres_error;
use super::operations::remote_target::{
    DeleteRemoteTargetRecord, LoadRemoteTargetRecord, SaveRemoteTargetRecord,
    UpdateRemoteTargetRecord, emit_remote_target_invoked_event, list_rows_with_total_count,
};

fn target_to_storage(row: RemoteTargetRow) -> Result<StorageRemoteTarget, ApiError> {
    let allowed_subject_types = serde_json::from_value::<Vec<String>>(row.allowed_subject_types)?;
    Ok(StorageRemoteTarget::new(
        StorageRecordMetadata::new(row.id, row.created_at, row.updated_at, row.revision.get()),
        row.collection_id,
        row.name,
        StorageRemoteTargetDefinition::new(
            row.description,
            StorageRemoteTargetTransport::new(
                row.method,
                row.url_template,
                row.headers_template,
                row.body_template,
                row.auth_config,
                row.timeout_ms,
            ),
            StorageRemoteTargetPolicy::new(row.class_id, allowed_subject_types, row.enabled),
        ),
    ))
}

fn definition_into_row_parts(
    definition: StorageRemoteTargetDefinition,
) -> Result<RemoteTargetDefinitionRowParts, ApiError> {
    let (description, transport, policy) = definition.into_parts();
    let (method, url_template, headers_template, body_template, auth_config, timeout_ms) =
        transport.into_parts();
    let (class_id, allowed_subject_types, enabled) = policy.into_parts();
    Ok(RemoteTargetDefinitionRowParts {
        class_id,
        description,
        method,
        url_template,
        headers_template,
        body_template,
        auth_config,
        allowed_subject_types: serde_json::to_value(allowed_subject_types)?,
        timeout_ms,
        enabled,
    })
}

struct RemoteTargetDefinitionRowParts {
    class_id: Option<i32>,
    description: String,
    method: String,
    url_template: String,
    headers_template: serde_json::Value,
    body_template: Option<String>,
    auth_config: serde_json::Value,
    allowed_subject_types: serde_json::Value,
    timeout_ms: i32,
    enabled: bool,
}

fn patch_into_row(patch: StorageRemoteTargetPatch) -> Result<UpdateRemoteTargetRow, ApiError> {
    let (
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
    ) = patch.into_parts();
    Ok(UpdateRemoteTargetRow {
        collection_id,
        class_id,
        name,
        description,
        method,
        url_template,
        headers_template,
        body_template,
        auth_config,
        allowed_subject_types: allowed_subject_types
            .map(serde_json::to_value)
            .transpose()?,
        timeout_ms,
        enabled,
    })
}

#[async_trait]
impl RemoteTargetStorage for PostgresStorage {
    async fn get_remote_target(&self, target_id: i32) -> Result<StorageRemoteTarget, StorageError> {
        RemoteTargetID::new(target_id)
            .map_err(map_postgres_error)?
            .load_remote_target_record(self.pool())
            .await
            .and_then(target_to_storage)
            .map_err(map_postgres_error)
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StorageRemoteTargetPage, StorageError> {
        let (allowed_collection_ids, options) = query.into_parts();
        let (rows, total) =
            list_rows_with_total_count(self.pool(), &allowed_collection_ids, &options)
                .await
                .map_err(map_postgres_error)?;
        let targets = rows
            .into_iter()
            .map(target_to_storage)
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_postgres_error)?;
        Ok(StorageRemoteTargetPage::new(
            targets,
            (total != SKIPPED_TOTAL_COUNT).then_some(total),
        ))
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<StorageRemoteTarget, StorageError> {
        let (collection_id, name, definition, event_context) = request.into_parts();
        let definition = definition_into_row_parts(definition).map_err(map_postgres_error)?;
        NewRemoteTargetRow {
            collection_id,
            class_id: definition.class_id,
            name,
            description: definition.description,
            method: definition.method,
            url_template: definition.url_template,
            headers_template: definition.headers_template,
            body_template: definition.body_template,
            auth_config: definition.auth_config,
            allowed_subject_types: definition.allowed_subject_types,
            timeout_ms: definition.timeout_ms,
            enabled: definition.enabled,
        }
        .save_remote_target_record(self.pool(), Some(&event_context))
        .await
        .and_then(target_to_storage)
        .map_err(map_postgres_error)
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<StorageRemoteTarget, StorageError> {
        let (target_id, patch, event_context) = request.into_parts();
        patch_into_row(patch)
            .map_err(map_postgres_error)?
            .update_remote_target_record(self.pool(), target_id, Some(&event_context))
            .await
            .and_then(target_to_storage)
            .map_err(map_postgres_error)
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<(), StorageError> {
        let (target_id, event_context) = request.into_parts();
        RemoteTargetID::new(target_id)
            .map_err(map_postgres_error)?
            .delete_remote_target_record(self.pool(), Some(&event_context))
            .await
            .map_err(map_postgres_error)
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<(), StorageError> {
        let (target_id, task_id, subject_type, subject_id, event_context) = request.into_parts();
        emit_remote_target_invoked_event(
            self.pool(),
            target_id,
            &event_context,
            task_id,
            &subject_type,
            subject_id,
        )
        .await
        .map_err(map_postgres_error)
    }
}
