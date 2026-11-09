//! Administrator use cases for database-managed runtime settings.
use chrono::Utc;
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup,
    StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsUpdate, StorageRuntimeSettingsValue,
};
use serde_json::{Value, json};

use crate::config::running::QueryObservationConfig;
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::LOCAL_IDENTITY_SCOPE;
use crate::models::runtime_settings::discovery::{RuntimeSettingsCatalogue, RuntimeSettingsPatch};
use crate::models::runtime_settings::{
    AuthorizationSettingsRequest, PaginationSettingsRequest, QueryObservationPolicyValue,
    QueryObservationSettingsRequest, RuntimeAuthenticationProviderRequest,
    RuntimeSettingsDeployment, RuntimeSettingsGroup, RuntimeSettingsHistoryParams,
    RuntimeSettingsHistoryResponse, RuntimeSettingsReport, RuntimeSettingsUpdateResponse,
};
use crate::pagination::deployment_page_limits;
use crate::permissions::{AppContext, PermissionBackend, PrincipalRef};
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

pub fn catalogue(context: &AppContext) -> Result<RuntimeSettingsCatalogue, ApiError> {
    RuntimeSettingsCatalogue::new(deployment(context.backend())?)
}

/// Merge only supplied fields into authoritative requested values, then run the
/// existing typed validation, authorization, audit and compare-and-swap path.
pub async fn patch(
    context: &AppContext,
    group: RuntimeSettingsGroup,
    request: RuntimeSettingsPatch,
    event: EventContext,
) -> Result<RuntimeSettingsUpdateResponse, ApiError> {
    let storage = context.backend();
    let deployment = deployment(storage)?;
    let descriptor = RuntimeSettingsCatalogue::new(deployment)?
        .groups
        .into_iter()
        .find(|descriptor| descriptor.key == group)
        .expect("every runtime settings group has a descriptor");
    if !descriptor.patch_supported {
        return Err(ApiError::BadRequest(
            descriptor.read_only_reason.unwrap_or_default(),
        ));
    }
    if request.changes.is_empty() {
        return Err(ApiError::BadRequest(
            "changes must contain at least one field".into(),
        ));
    }
    for (name, value) in &request.changes {
        if descriptor.schema["properties"].get(name).is_none() || value.is_null() {
            return Err(ApiError::BadRequest(format!(
                "Unknown or null runtime setting '{name}'; omit unchanged fields and use DELETE to release a group"
            )));
        }
    }
    let snapshot = storage.load_runtime_settings().await?;
    let record = snapshot.record(group.into());
    if record.revision() != request.expected_revision {
        return Err(ApiError::RevisionConflict(
            "The runtime settings revision is stale".into(),
            record.revision(),
        ));
    }
    let mut merged = match group {
        RuntimeSettingsGroup::Pagination => {
            json!({"default_page_limit": deployment.pagination.default_limit()})
        }
        RuntimeSettingsGroup::QueryObservations => serde_json::to_value(
            QueryObservationPolicyValue::from(deployment.query_observations),
        )?,
        RuntimeSettingsGroup::Authorization => {
            json!({"backend": deployment.authorization_backend.as_str()})
        }
        RuntimeSettingsGroup::Authentication => unreachable!("authentication uses preflight"),
    };
    if let Some(Value::Object(current)) = record.assignment().to_document() {
        for (name, value) in current {
            // Migrated enablement-only observation policies inherit their
            // omitted resource settings from the deployment until first edited.
            if !value.is_null() {
                merged[&name] = value;
            }
        }
    }
    for (name, value) in request.changes {
        merged[&name] = value;
    }
    merged["expected_revision"] = serde_json::to_value(request.expected_revision)?;
    let invalid = |error| ApiError::BadRequest(format!("Invalid runtime settings: {error}"));
    let update_request = match group {
        RuntimeSettingsGroup::Pagination => {
            serde_json::from_value::<PaginationSettingsRequest>(merged)
                .map_err(invalid)?
                .into_update(deployment.pagination, event)?
        }
        RuntimeSettingsGroup::QueryObservations => {
            serde_json::from_value::<QueryObservationSettingsRequest>(merged)
                .map_err(invalid)?
                .into_update(deployment.query_observations, event)?
        }
        RuntimeSettingsGroup::Authorization => {
            serde_json::from_value::<AuthorizationSettingsRequest>(merged)
                .map_err(invalid)?
                .into_update(event)
        }
        RuntimeSettingsGroup::Authentication => unreachable!("authentication uses preflight"),
    };
    update(context, update_request).await
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
        crate::auth::auth_provider_statuses()?,
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
    if matches!(
        request.group(),
        StorageRuntimeSettingsGroup::Authentication | StorageRuntimeSettingsGroup::Authorization
    ) {
        let administrator = LocalSettingsAdministrator::require(context, request.context()).await?;
        let current = storage_handle(context).load_runtime_settings().await?;
        let revision = current.record(request.group()).revision();
        if revision != request.expected_revision() {
            return Err(ApiError::RevisionConflict(
                "The security settings revision is stale".to_string(),
                revision,
            ));
        }
        if request.group() == StorageRuntimeSettingsGroup::Authorization {
            let config = crate::config::get_config()?.clone();
            let candidate = match request.assignment().value() {
                Some(StorageRuntimeSettingsValue::Authorization(selected)) => {
                    crate::permissions::build_permission_backend_for_runtime(
                        &config,
                        context.clone_backend(),
                        *selected,
                    )
                    .await?
                }
                None => {
                    crate::permissions::build_permission_backend(&config, context.clone_backend())
                        .await?
                }
                Some(_) => unreachable!("authorization group must contain authorization settings"),
            };
            administrator.check_backend(candidate.as_ref()).await?;
        } else if request.assignment().value().is_none() {
            crate::auth::preflight_deployment_authentication_settings(&context.clone_backend())
                .await?;
        }
    }
    commit(context, request).await
}

/// Proof that a password-capable local administrator can recover after a change.
struct LocalSettingsAdministrator(PrincipalRef);

impl LocalSettingsAdministrator {
    async fn require(context: &AppContext, event: &EventContext) -> Result<Self, ApiError> {
        let actor = event.actor_user_id().ok_or_else(|| {
            ApiError::Forbidden("Security settings require a local human administrator".to_string())
        })?;
        let user = super::identity::get_user(context, actor.id()).await?;
        let details = super::identity::get_user_point(context, actor.id()).await?;
        let scope =
            super::identity::resolve_identity_scope_name(context, details.identity_scope_id)
                .await?;
        if scope != LOCAL_IDENTITY_SCOPE
            || details.provider_managed
            || user.password.is_none()
            || user.anonymized_at.is_some()
        {
            return Err(ApiError::Forbidden(
                "Authentication and authorization settings require a local administrator with password login configured".to_string(),
            ));
        }
        let principal = PrincipalRef::load(context, &user).await?;
        if !context.permission_backend().is_admin(&principal).await? {
            return Err(ApiError::Forbidden(
                "Security settings require administrator access".to_string(),
            ));
        }
        Ok(Self(principal))
    }

    async fn check_backend(&self, candidate: &dyn PermissionBackend) -> Result<(), ApiError> {
        if !candidate.is_admin(&self.0).await? {
            return Err(ApiError::BadRequest(
                "The selected authorization backend must retain administrator access for the local administrator making this change".to_string(),
            ));
        }
        Ok(())
    }
}

async fn commit(
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
            && (group != RuntimeSettingsGroup::Authentication
                || context
                    .applied_authentication_revision()
                    .is_some_and(|applied| applied >= committed_revision))
            && (group != RuntimeSettingsGroup::Authorization
                || context.permission_backend_is_ready()),
        settings,
    })
}

pub async fn preflight_authentication(
    context: &AppContext,
    expected_revision: ResourceRevision,
    providers: Vec<RuntimeAuthenticationProviderRequest>,
    event_context: EventContext,
) -> Result<RuntimeSettingsUpdateResponse, ApiError> {
    let _administrator = LocalSettingsAdministrator::require(context, &event_context).await?;
    let storage = storage_handle(context);
    let current = storage.load_runtime_settings().await?;
    let current_record = current.record(StorageRuntimeSettingsGroup::Authentication);
    if current_record.revision() != expected_revision {
        return Err(ApiError::RevisionConflict(
            "The authentication settings revision is stale".to_string(),
            current_record.revision(),
        ));
    }

    let deployment = if current.authentication().is_some() {
        crate::auth::runtime_deployment_config()
    } else {
        crate::auth::load_auth_config()?
    };
    let providers = crate::auth::protect_runtime_authentication_settings(
        providers,
        current.authentication(),
        &deployment,
    )
    .await?;
    crate::auth::preflight_runtime_authentication_settings(&providers, &storage, &deployment)
        .await?;
    commit(
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
