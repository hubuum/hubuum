//! Adapter-facing browser identity boundary. Protocol adapters supply verified
//! evidence; storage consumes it under the interaction and identity fences.
use chrono::{DateTime, Utc};
use hubuum_auth_core::{
    ExternalAuthenticationContext, ExternalAuthenticationError, ExternalFreshnessPolicy,
    ExternalSubject, FreshExternalAuthenticationRequest,
};
use hubuum_domain::{
    IdentityScopeName, InteractionAuthenticationClaim, InteractionBinding,
    InteractionBrowserBinding, InteractionLifecycle,
};
use hubuum_storage_core::{
    ExternalIdentityStorage, RuntimeSettingsStorage, StorageBrowserProviderDefinition,
    StorageExternalInteractionAuthentication, StorageExternalPrincipalState,
    StorageInteractionAction, StorageInteractionAuthorization, StorageInteractionView,
    StorageProviderIdentityBinding, StorageRuntimeSettingsGroup,
};

use crate::errors::{ApiError, InteractionError};
use crate::permissions::AppContext;
use crate::storage::{StorageHandle, storage_handle};

/// Resolve an enabled provider from the exact revision carried by the interaction.
/// Browser definitions currently originate only from runtime security candidates.
pub async fn provider_context(
    context: &AppContext,
    binding: &InteractionBinding,
) -> Result<ExternalAuthenticationContext, ApiError> {
    let storage = storage_handle(context);
    storage
        .applied_runtime_settings()
        .ensure_security_current()
        .map_err(super::storage_error)?;
    let current = storage
        .load_runtime_settings()
        .await
        .map_err(super::storage_error)?;
    if current
        .record(StorageRuntimeSettingsGroup::Authentication)
        .revision()
        != binding.revision()
    {
        return Err(InteractionError::ConfigurationChanged.into());
    }
    let provider = current
        .authentication()
        .and_then(|providers| {
            providers.providers().iter().find(|provider| {
                provider.enabled() && provider.id() == binding.provider().external_id()
            })
        })
        .ok_or(InteractionError::ProviderUnavailable)?;
    let scope = IdentityScopeName::new(provider.name()).map_err(|_| InteractionError::Invalid)?;
    let expected = StorageProviderIdentityBinding::from_provider(provider)
        .map_err(|_| InteractionError::Invalid)?;
    if storage
        .authentication_provider_binding(scope.clone())
        .await
        .map_err(super::storage_error)?
        .as_ref()
        != Some(&expected)
    {
        return Err(InteractionError::ConfigurationChanged.into());
    }
    match provider.browser_definition() {
        Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
            ExternalAuthenticationContext::oidc(binding.clone(), scope, definition)
        }
        Some(StorageBrowserProviderDefinition::Oauth(definition)) => {
            ExternalAuthenticationContext::oauth(binding.clone(), scope, definition)
        }
        None => return Err(InteractionError::Unsupported.into()),
    }
    .map_err(|_| InteractionError::Invalid.into())
}

/// Start before redirect and preserve this request until the signed OIDC response
/// arrives. Its start is the persisted interaction start, never a callback value.
pub async fn fresh_approval_request(
    context: &AppContext,
    interaction: &InteractionLifecycle,
    clock_skew_seconds: u32,
) -> Result<FreshExternalAuthenticationRequest, ApiError> {
    let provider = provider_context(context, interaction.binding()).await?;
    let actor = interaction
        .binding()
        .authority()
        .actor()
        .ok_or(InteractionError::Invalid)?;
    let stored = storage_handle(context)
        .get_external_principal_state(actor.principal_id())
        .await
        .map_err(super::storage_error)?
        .ok_or(InteractionError::Invalid)?;
    let subject =
        ExternalSubject::new(stored.external_subject()).map_err(|_| InteractionError::Invalid)?;
    let identity = provider
        .identity(subject)
        .map_err(|_| InteractionError::Invalid)?;
    if identity.scope().as_str() != stored.identity_scope() {
        return Err(InteractionError::Invalid.into());
    }
    FreshExternalAuthenticationRequest::new(
        provider,
        identity,
        interaction.created_at(),
        clock_skew_seconds,
    )
    .map_err(|error| match error {
        ExternalAuthenticationError::CapabilityUnsupported => InteractionError::Unsupported.into(),
        _ => InteractionError::Invalid.into(),
    })
}

/// Complete the claimed attempt atomically with admission and membership writes.
/// No session or approval is issued until ordinary purpose-bound completion.
pub async fn authenticate(
    context: &AppContext,
    claim: InteractionAuthenticationClaim,
    authentication: StorageExternalInteractionAuthentication,
    approval: Option<InteractionBrowserBinding>,
) -> Result<StorageInteractionView, ApiError> {
    if authentication.authentication().binding() != claim.binding()
        || Utc::now() >= claim.deadline()
    {
        return Err(InteractionError::Invalid.into());
    }
    // Re-resolve before storage; the transaction repeats the revision fence.
    provider_context(context, claim.binding()).await?;
    super::view(
        super::advance(
            context,
            StorageInteractionAuthorization::Claim(claim),
            StorageInteractionAction::AuthenticateExternal {
                authentication: Box::new(authentication),
                approval,
            },
        )
        .await?,
    )
}

/// Browser authority has a separate upstream clock. Without a usable adapter
/// renewal mechanism, a due identity must sign in again; rotating Hubuum tokens
/// cannot extend it. Adapters report temporary failures through the existing
/// sync-failure operation, preserving only the bounded verified snapshot.
pub(crate) async fn check_existing_authority(
    storage: &StorageHandle,
    state: &StorageExternalPrincipalState,
) -> Result<bool, ApiError> {
    use hubuum_domain::ProviderAdmission;
    use hubuum_storage_core::StorageAuthenticationProviderKind;
    let scope =
        IdentityScopeName::new(state.identity_scope()).map_err(|_| InteractionError::Invalid)?;
    let Some(binding) = storage.authentication_provider_binding(scope).await? else {
        return Ok(false);
    };
    if binding.kind() == StorageAuthenticationProviderKind::Ldap {
        return Ok(false);
    }
    let current = storage.load_runtime_settings().await?;
    let provider = current.authentication().and_then(|providers| {
        providers.providers().iter().find(|provider| {
            provider.enabled()
                && provider.id() == Some(binding.id())
                && provider.name() == binding.name()
        })
    });
    let policy = provider.and_then(|provider| match provider.browser_definition() {
        Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
            Some((definition.admission(), definition.membership()))
        }
        Some(StorageBrowserProviderDefinition::Oauth(definition)) => {
            Some((definition.admission(), definition.membership()))
        }
        None => None,
    });
    let Some((admission, membership)) = policy else {
        return Err(ApiError::Unauthorized(
            "External authentication provider is unavailable".into(),
        ));
    };
    if matches!(admission,ProviderAdmission::SubjectAllowlist { subjects } if !subjects.iter().any(|subject|subject==state.external_subject()))
    {
        return Err(ApiError::Unauthorized(
            "External identity is no longer admitted".into(),
        ));
    }
    existing_authority(
        ExternalFreshnessPolicy::try_from(membership).map_err(|_| InteractionError::Invalid)?,
        state,
        Utc::now(),
    )?;
    Ok(true)
}

fn existing_authority(
    policy: ExternalFreshnessPolicy,
    state: &StorageExternalPrincipalState,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    use hubuum_auth_core::ExternalFreshness;
    let verified = state.last_sync_success_at();
    let temporary_failure = verified.is_some_and(|verified| {
        state
            .last_sync_attempted_at()
            .is_some_and(|attempt| attempt > verified)
    });
    match policy.evaluate(verified, now) {
        ExternalFreshness::Current => Ok(()),
        ExternalFreshness::RefreshRequired if temporary_failure => Ok(()),
        ExternalFreshness::Expired if temporary_failure => Err(ApiError::ServiceUnavailable(
            "External identity verification is unavailable and its cached authority has expired"
                .into(),
        )),
        _ => Err(ApiError::Unauthorized(
            "External identity requires a new upstream sign-in".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use hubuum_storage_core::StorageExternalPrincipalState;
    use rstest::rstest;

    #[rstest]
    #[case::current(299, false, 200)]
    #[case::due_without_renewal(300, false, 401)]
    #[case::outage_inside_bound(899, true, 200)]
    #[case::outage_at_bound(900, true, 503)]
    #[case::expired_without_renewal(900, false, 401)]
    #[case::future_observation(-1,false,401)]
    fn upstream_clock_controls_existing_authority(
        #[case] age: i64,
        #[case] outage: bool,
        #[case] status: u16,
    ) {
        use actix_web::ResponseError;
        let now = Utc::now();
        let verified = now - Duration::seconds(age);
        let attempted = if outage {
            verified + Duration::seconds(1)
        } else {
            verified
        };
        let state = StorageExternalPrincipalState::try_new(
            "company",
            "alice",
            "subject",
            Some(attempted),
            Some(verified),
        )
        .unwrap();
        let result =
            existing_authority(ExternalFreshnessPolicy::new(300, 900).unwrap(), &state, now);
        assert_eq!(
            result
                .map(|()| 200)
                .unwrap_or_else(|error| error.status_code().as_u16()),
            status
        );
    }
}
