//! Administrator use cases for database-managed runtime settings.
use chrono::Utc;
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageAuthenticationProviderKind, StorageRuntimeSettingsAssignment,
    StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsUpdate, StorageRuntimeSettingsValue,
};

use crate::config::running::QueryObservationConfig;
use crate::errors::ApiError;
use crate::models::runtime_settings::{
    RuntimeAuthenticationProviderStatus, RuntimeSettingsDeployment, RuntimeSettingsGroup,
    RuntimeSettingsHistoryParams, RuntimeSettingsHistoryResponse, RuntimeSettingsReport,
    RuntimeSettingsUpdateResponse,
};
use crate::pagination::deployment_page_limits;
use crate::permissions::AppContext;
use crate::storage::runtime_settings::AppliedRuntimeSettings;
use crate::storage::{StorageContext, StorageHandle, storage_handle};

fn deployment(storage: &StorageHandle) -> Result<RuntimeSettingsDeployment, ApiError> {
    let config = crate::config::get_config()?;
    Ok(RuntimeSettingsDeployment {
        query_observations: storage.deployment_query_observation_settings(),
        pagination: deployment_page_limits()?,
        authorization_backend: match config.permission_backend {
            crate::config::PermissionBackendKind::Local => {
                hubuum_storage_core::StorageAuthorizationBackend::Local
            }
            crate::config::PermissionBackendKind::Treetop => {
                hubuum_storage_core::StorageAuthorizationBackend::Treetop
            }
        },
    })
}

/// Reconcile the responding replica, then report what it applies.
///
/// A failed refresh is reported as staleness with the retained snapshot rather
/// than failing the read, so administrators can diagnose the replica.
pub async fn report(context: &AppContext) -> Result<RuntimeSettingsReport, ApiError> {
    let storage = context.clone_backend();
    if let Err(error) = context.refresh_runtime_settings().await {
        tracing::warn!(%error, "Runtime settings refresh failed; reporting the retained snapshot");
    }
    Ok(RuntimeSettingsReport::new(
        &storage.applied_runtime_settings(),
        deployment(&storage)?,
        Utc::now(),
        crate::auth::auth_provider_statuses()?
            .into_iter()
            .map(|(name, available)| RuntimeAuthenticationProviderStatus { name, available })
            .collect(),
        context.current_permission_backend_kind().to_string(),
        context.permission_backend_is_ready(),
    ))
}

/// Commit a revision-checked update, then apply it to the responding replica.
///
/// Success confirms the commit. Other replicas apply it asynchronously; the
/// response reports only the responding replica's applied revision.
pub async fn update(
    context: &AppContext,
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
            .is_some_and(|applied| applied >= committed_revision)
            && (group != RuntimeSettingsGroup::Authorization
                || context.permission_backend_is_ready()),
        settings,
    })
}

pub async fn preflight_authentication(
    context: &AppContext,
    expected_revision: ResourceRevision,
    providers: Vec<(String, StorageAuthenticationProviderKind, serde_json::Value)>,
    event_context: crate::events::EventContext,
) -> Result<RuntimeSettingsUpdateResponse, ApiError> {
    let storage = storage_handle(context);
    let current = storage.load_runtime_settings().await?;
    let current_record =
        current.record(hubuum_storage_core::StorageRuntimeSettingsGroup::Authentication);
    if current_record.revision() != expected_revision {
        return Err(ApiError::RevisionConflict(
            "The authentication settings revision is stale".to_string(),
            current_record.revision(),
        ));
    }

    let providers =
        crate::auth::protect_runtime_authentication_settings(providers, current.authentication())
            .await?;
    crate::auth::preflight_runtime_authentication_settings(&providers, &storage).await?;
    update(
        context,
        StorageRuntimeSettingsUpdate::new(
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authentication(
                providers,
            )),
            expected_revision,
            event_context,
        ),
    )
    .await
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
