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
    ExternalIdentityStorage, RuntimeSettingsStorage, StorageAuthenticationProvider,
    StorageBrowserProviderDefinition, StorageExternalInteractionAuthentication,
    StorageExternalPrincipalState, StorageInteractionAction, StorageInteractionAuthorization,
    StorageInteractionView, StorageRuntimeSettingsGroup,
};

use crate::errors::{ApiError, InteractionError};
use crate::permissions::AppContext;
use crate::storage::{StorageHandle, storage_handle};

/// Resolve the enabled definition under its revision and immutable namespace.
/// Password providers also need this fence when choosing their hosted form.
pub(super) async fn provider_configuration(
    context: &AppContext,
    binding: &InteractionBinding,
) -> Result<StorageAuthenticationProvider, ApiError> {
    provider_at_revision(context, binding.provider(), binding.revision()).await
}

pub(super) async fn provider_at_revision(
    context: &AppContext,
    provider_id: hubuum_domain::InteractionProviderId,
    revision: hubuum_domain::ResourceRevision,
) -> Result<StorageAuthenticationProvider, ApiError> {
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
        != revision
    {
        return Err(InteractionError::ConfigurationChanged.into());
    }
    if current.authentication().is_none() {
        let configured = crate::auth::interaction_password_providers(revision)?;
        let providers = super::resolve_password_providers(&storage, configured).await?;
        let deployed = providers
            .iter()
            .filter_map(|provider| provider.binding.as_ref())
            .find(|provider| Some(provider.id()) == provider_id.external_id())
            .ok_or(InteractionError::ProviderUnavailable)?;
        return StorageAuthenticationProvider::deployment(
            deployed.name().to_owned(),
            deployed.kind(),
            true,
        )
        .and_then(|provider| provider.with_id(deployed.id()))
        .map_err(|_| InteractionError::Invalid.into());
    }
    let provider = current
        .authentication()
        .and_then(|providers| {
            providers
                .providers()
                .iter()
                .find(|provider| provider.enabled() && provider.id() == provider_id.external_id())
        })
        .ok_or(InteractionError::ProviderUnavailable)?;
    let scope = IdentityScopeName::new(provider.name()).map_err(|_| InteractionError::Invalid)?;
    if !storage
        .authentication_provider_binding(scope)
        .await
        .map_err(super::storage_error)?
        .is_some_and(|binding| binding.matches_provider(provider))
    {
        return Err(InteractionError::ConfigurationChanged.into());
    }
    Ok(provider.clone())
}

/// Browser adapters require the stronger protocol-specific context after the
/// common definition has been resolved. LDAP retains its password transport.
pub async fn provider_context(
    context: &AppContext,
    binding: &InteractionBinding,
) -> Result<ExternalAuthenticationContext, ApiError> {
    let provider = provider_configuration(context, binding).await?;
    let scope = IdentityScopeName::new(provider.name()).map_err(|_| InteractionError::Invalid)?;
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
    principal: hubuum_domain::PrincipalId,
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
    let unavailable =
        || ApiError::Unauthorized("External authentication provider is unavailable".into());
    let provider = current
        .authentication()
        .and_then(|providers| {
            providers
                .providers()
                .iter()
                .find(|provider| provider.name() == binding.name())
        })
        .filter(|provider| provider.enabled() && binding.matches_provider(provider))
        .ok_or_else(unavailable)?;
    let (admission, membership) = match provider.browser_definition().ok_or_else(unavailable)? {
        StorageBrowserProviderDefinition::Oidc(definition) => {
            (definition.admission(), definition.membership())
        }
        StorageBrowserProviderDefinition::Oauth(definition) => {
            (definition.admission(), definition.membership())
        }
    };
    if matches!(admission,ProviderAdmission::SubjectAllowlist { subjects } if !subjects.iter().any(|subject|subject==state.external_subject()))
    {
        return Err(ApiError::Unauthorized(
            "External identity is no longer admitted".into(),
        ));
    }
    let policy =
        ExternalFreshnessPolicy::try_from(membership).map_err(|_| InteractionError::Invalid)?;
    if policy.evaluate(state.last_sync_success_at(), Utc::now())
        != hubuum_auth_core::ExternalFreshness::Current
    {
        if super::oidc::renew(
            storage,
            principal,
            current
                .record(StorageRuntimeSettingsGroup::Authentication)
                .revision(),
            provider,
        )
        .await?
        {
            return Ok(true);
        }
        let current = storage
            .get_external_principal_state(principal)
            .await?
            .ok_or(InteractionError::Invalid)?;
        existing_authority(policy, &current, Utc::now())?;
    }
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
