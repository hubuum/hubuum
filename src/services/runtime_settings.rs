//! Administrator use cases for database-managed runtime settings.
use chrono::Utc;
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsUpdate,
};

use crate::config::running::QueryObservationConfig;
use crate::errors::ApiError;
use crate::models::runtime_settings::{
    RuntimeSettingsDeployment, RuntimeSettingsGroup, RuntimeSettingsHistoryParams,
    RuntimeSettingsHistoryResponse, RuntimeSettingsReport, RuntimeSettingsUpdateResponse,
};
use crate::pagination::deployment_page_limits;
use crate::storage::runtime_settings::AppliedRuntimeSettings;
use crate::storage::{StorageContext, StorageHandle, storage_handle};

fn deployment(storage: &StorageHandle) -> Result<RuntimeSettingsDeployment, ApiError> {
    Ok(RuntimeSettingsDeployment {
        query_observations: storage.deployment_query_observation_settings(),
        pagination: deployment_page_limits()?,
    })
}

/// Reconcile the responding replica, then report what it applies.
///
/// A failed refresh is reported as staleness with the retained snapshot rather
/// than failing the read, so administrators can diagnose the replica.
pub async fn report(context: &impl StorageContext) -> Result<RuntimeSettingsReport, ApiError> {
    let storage = storage_handle(context);
    let _ = storage.refresh_runtime_settings().await;
    Ok(RuntimeSettingsReport::new(
        &storage.applied_runtime_settings(),
        deployment(&storage)?,
        Utc::now(),
    ))
}

/// Commit a revision-checked update, then apply it to the responding replica.
///
/// Success confirms the commit. Other replicas apply it asynchronously; the
/// response reports only the responding replica's applied revision.
pub async fn update(
    context: &impl StorageContext,
    request: StorageRuntimeSettingsUpdate,
) -> Result<RuntimeSettingsUpdateResponse, ApiError> {
    let group = RuntimeSettingsGroup::from(request.group());
    let outcome = storage_handle(context)
        .update_runtime_settings(request)
        .await?;
    let committed_revision = outcome.record().revision();
    let settings = report(context).await?;
    Ok(RuntimeSettingsUpdateResponse {
        group,
        committed_revision,
        changed: outcome.is_committed(),
        applied_by_responding_replica: settings
            .applied_revision(group)
            .is_some_and(|applied| applied >= committed_revision),
        settings,
    })
}

pub async fn history(
    context: &impl StorageContext,
    group: RuntimeSettingsGroup,
    params: &RuntimeSettingsHistoryParams,
) -> Result<RuntimeSettingsHistoryResponse, ApiError> {
    let query = StorageRuntimeSettingsHistoryQuery::try_new(
        group.into(),
        params.before_revision,
        params.limit(),
    )
    .map_err(|error| ApiError::BadRequest(error.message().to_string()))?;
    let changes = storage_handle(context)
        .list_runtime_settings_changes(query)
        .await?;
    // Revision 2 is the earliest possible audit record.
    let earliest = ResourceRevision::INITIAL
        .checked_advance()
        .expect("the second revision is representable");
    let next_before_revision = changes
        .last()
        .map(|change| change.revision())
        .filter(|revision| changes.len() == query.limit() && *revision > earliest);
    Ok(RuntimeSettingsHistoryResponse {
        changes: changes.into_iter().map(Into::into).collect(),
        next_before_revision,
    })
}

/// Effective query observation settings applied by this process.
pub(crate) fn query_observation_config(
    storage: &StorageHandle,
    applied: &AppliedRuntimeSettings,
) -> QueryObservationConfig {
    QueryObservationConfig::from(storage.query_observation_settings(applied))
}
