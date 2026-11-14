//! Server-owned OIDC redirects and single-use, cookie-bound callbacks.
use hubuum_auth_core::ExternalAuthenticationContext;
use hubuum_auth_oidc::{OidcAdapter, OidcError, PendingLogin};
use hubuum_domain::PrincipalId;
use hubuum_domain::{
    IdentityScopeName, InteractionBinding, InteractionDigest, InteractionId, InteractionPurpose,
};
use hubuum_secrets::SecretValue;
use hubuum_storage_core::StorageEncryptedUpstreamCredential;
use hubuum_storage_core::StorageRuntimeSettingsValue;
use hubuum_storage_core::StorageUpstreamSession;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageAuthenticationProvider, StorageBrowserProviderDefinition,
    StorageExternalInteractionAuthentication, StorageInteractionAction,
    StorageInteractionAuthorization, StorageInteractionOutcome,
    StorageProtectedAuthenticationState,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    errors::{ApiError, InteractionError},
    models::auth_interaction::browser::BrowserSession,
    permissions::AppContext,
    storage::storage_handle,
};

pub(crate) fn error(error: OidcError) -> ApiError {
    match error {
        OidcError::Unavailable => InteractionError::ProviderUnavailable.into(),
        OidcError::Configuration => InteractionError::Unsupported.into(),
        _ => InteractionError::Invalid.into(),
    }
}

pub(crate) async fn adapter(
    context: &AppContext,
    provider: &StorageAuthenticationProvider,
) -> Result<OidcAdapter, ApiError> {
    super::browser::settings(context)?;
    configured_adapter(provider).await
}

async fn configured_adapter(
    provider: &StorageAuthenticationProvider,
) -> Result<OidcAdapter, ApiError> {
    let Some(StorageBrowserProviderDefinition::Oidc(definition)) = provider.browser_definition()
    else {
        return Err(InteractionError::Unsupported.into());
    };
    let secret = crate::services::security_candidates::providers::browser_secret(provider)
        .await?
        .ok_or(InteractionError::ProviderUnavailable)?;
    let callback = format!(
        "{}/api/v1/auth/callbacks/{}",
        crate::config::get_config()?.auth_browser.origin()?,
        provider.id().ok_or(InteractionError::Invalid)?
    );
    OidcAdapter::new(
        definition.clone(),
        secret,
        &callback,
        crate::config::get_config()?.auth_runtime_allow_private_targets,
    )
    .map_err(error)
}

async fn provider(
    context: &AppContext,
    binding: &InteractionBinding,
) -> Result<StorageAuthenticationProvider, ApiError> {
    if binding.authority().purpose() == InteractionPurpose::ProviderVerification {
        let actor = crate::services::credential_approvals::CredentialActor::from_interaction(
            context,
            binding
                .authority()
                .actor()
                .ok_or(InteractionError::Invalid)?,
        )
        .await?;
        let (candidate, _) = crate::services::security_candidates::interaction_candidate(
            context,
            &actor,
            binding
                .authority()
                .candidate_id()
                .ok_or(InteractionError::Invalid)?,
            binding
                .provider()
                .external_id()
                .ok_or(InteractionError::Invalid)?,
        )
        .await?;
        if StorageInteractionAuthentication::candidate_digest(&candidate)
            != *binding
                .authority()
                .candidate_digest()
                .ok_or(InteractionError::Invalid)?
        {
            return Err(InteractionError::ConfigurationChanged.into());
        }
        let Some(StorageRuntimeSettingsValue::Authentication(providers)) =
            candidate.verification_assignment().value()
        else {
            return Err(InteractionError::Invalid.into());
        };
        return providers
            .providers()
            .iter()
            .find(|provider| {
                provider.id() == binding.provider().external_id() && provider.enabled()
            })
            .cloned()
            .ok_or_else(|| InteractionError::ConfigurationChanged.into());
    }
    super::external::provider_context(context, binding).await?;
    storage_handle(context)
        .load_runtime_settings()
        .await
        .map_err(super::storage_error)?
        .authentication()
        .and_then(|providers| {
            providers
                .providers()
                .iter()
                .find(|provider| {
                    provider.id() == binding.provider().external_id() && provider.enabled()
                })
                .cloned()
        })
        .ok_or_else(|| InteractionError::ConfigurationChanged.into())
}

fn state_digest(state: &str) -> InteractionDigest {
    let mut hash = Sha256::new();
    hash.update(b"hubuum:interaction:oidc-state:v1:");
    hash.update(state.as_bytes());
    InteractionDigest::new(crate::models::security_candidate::hex(&hash.finalize()))
        .expect("SHA-256")
}

fn associated_data(binding: &InteractionBinding) -> String {
    format!(
        "oidc:{}:{}:{}",
        binding.provider().external_id().unwrap_or_default(),
        binding.id().value(),
        binding.revision().get()
    )
}

async fn protect(
    binding: &InteractionBinding,
    purpose: &str,
    secret: SecretValue,
) -> Result<String, ApiError> {
    let reference = crate::config::get_config()?
        .runtime_settings_encryption_key_ref
        .clone()
        .ok_or(InteractionError::ProviderUnavailable)?;
    let key = crate::secrets::resolve_runtime_settings_key(&reference, None)
        .await
        .map_err(|_| InteractionError::ProviderUnavailable)?;
    let encrypted = crate::runtime_settings_secrets::encrypt(
        reference,
        crate::runtime_settings_secrets::ALIAS_KEY_VERSION.into(),
        key.value(),
        &format!("{}:{purpose}", associated_data(binding)),
        &secret,
    )
    .map_err(|_| InteractionError::ProviderUnavailable)?;
    serde_json::to_string(&encrypted).map_err(Into::into)
}

async fn unprotect(
    binding: &InteractionBinding,
    purpose: &str,
    ciphertext: &str,
) -> Result<SecretValue, ApiError> {
    let envelope: crate::runtime_settings_secrets::EncryptedSecret =
        serde_json::from_str(ciphertext).map_err(|_| InteractionError::Invalid)?;
    let key = crate::secrets::resolve_runtime_settings_key(envelope.key_ref(), None)
        .await
        .map_err(|_| InteractionError::ProviderUnavailable)?;
    crate::runtime_settings_secrets::decrypt(
        &envelope,
        key.value(),
        &format!("{}:{purpose}", associated_data(binding)),
    )
    .map_err(|_| InteractionError::Invalid.into())
}

pub async fn available(
    context: &AppContext,
    binding: &InteractionBinding,
) -> Result<bool, ApiError> {
    if binding.provider().external_id().is_none() {
        return Ok(false);
    }
    let provider = provider(context, binding).await?;
    Ok(matches!(
        provider.browser_definition(),
        Some(StorageBrowserProviderDefinition::Oidc(_))
    ))
}

pub async fn start(
    context: &AppContext,
    id: InteractionId,
    session: &BrowserSession,
) -> Result<String, ApiError> {
    let view = super::browser::inspect(context, id, session).await?;
    let provider = provider(context, view.binding()).await?;
    let (url, pending) = adapter(context, &provider)
        .await?
        .begin(
            id,
            view.binding().authority().purpose() == InteractionPurpose::CredentialApproval,
        )
        .await
        .map_err(error)?
        .into_parts();
    let protocol = StorageProtectedAuthenticationState::new(
        state_digest(pending.state()),
        protect(
            view.binding(),
            "callback",
            pending.protectable().map_err(error)?,
        )
        .await?,
    )
    .map_err(|_| InteractionError::Invalid)?;
    super::advance(
        context,
        StorageInteractionAuthorization::Browser {
            id,
            binding: session.binding(),
        },
        StorageInteractionAction::PrepareProtocol(protocol),
    )
    .await?;
    Ok(url)
}

pub fn callback_id(state: &str) -> Result<InteractionId, ApiError> {
    if state.len() > 256 {
        return Err(InteractionError::Invalid.into());
    }
    let (id, _) = state.split_once('.').ok_or(InteractionError::Invalid)?;
    InteractionId::new(Uuid::parse_str(id).map_err(|_| InteractionError::Invalid)?)
        .map_err(|_| InteractionError::Invalid.into())
}

pub struct Callback<'a> {
    pub provider: Uuid,
    pub state: &'a str,
    pub issuer: Option<&'a str>,
    pub code: Option<&'a str>,
    pub error: Option<&'a str>,
}

pub async fn callback(
    context: &AppContext,
    input: Callback<'_>,
    session: &BrowserSession,
) -> Result<(InteractionId, BrowserSession), ApiError> {
    let id = callback_id(input.state)?;
    let view = super::browser::inspect(context, id, session).await?;
    if view.binding().provider().external_id() != Some(input.provider) {
        return Err(InteractionError::Invalid.into());
    }
    let selected = provider(context, view.binding()).await?;
    let outcome = super::advance(
        context,
        StorageInteractionAuthorization::Browser {
            id,
            binding: session.binding(),
        },
        StorageInteractionAction::ClaimProtocol {
            attempt: Uuid::new_v4(),
            state: state_digest(input.state),
        },
    )
    .await?;
    let StorageInteractionOutcome::ProtocolClaim(claim) = outcome else {
        return Err(InteractionError::Invalid.into());
    };
    let rotated = session.rotate();
    let result = async {
        let pending = PendingLogin::restore(
            &unprotect(
                claim.claim().binding(),
                "callback",
                claim.protocol().ciphertext(),
            )
            .await?,
        )
        .map_err(error)?;
        let accepted = pending
            .accept(input.state, input.issuer, input.code, input.error)
            .map_err(error)?;
        let Some(StorageBrowserProviderDefinition::Oidc(definition)) =
            selected.browser_definition()
        else {
            return Err(InteractionError::Unsupported.into());
        };
        let provider_context = ExternalAuthenticationContext::oidc(
            claim.claim().binding().clone(),
            IdentityScopeName::new(selected.name()).map_err(|_| InteractionError::Invalid)?,
            definition,
        )
        .map_err(|_| InteractionError::Invalid)?;
        let authentication = adapter(context, &selected)
            .await?
            .exchange(accepted, &provider_context)
            .await
            .map_err(error)?;
        if view.binding().authority().purpose() == InteractionPurpose::ProviderVerification {
            let verified = crate::services::security_candidates::verify_oidc_interaction(
                context,
                authentication.proof(),
            )
            .await?;
            super::advance(
                context,
                StorageInteractionAuthorization::Claim(claim.claim().clone()),
                StorageInteractionAction::Authenticate {
                    authentication: Box::new(verified),
                    approval: Some(rotated.binding()),
                },
            )
            .await?;
        } else {
            let authentication = if view.binding().authority().purpose()
                == InteractionPurpose::CredentialApproval
            {
                let fresh =
                    super::external::fresh_approval_request(context, claim.lifecycle(), 1).await?;
                StorageExternalInteractionAuthentication::Approval(
                    authentication.fresh(fresh).map_err(error)?,
                )
            } else {
                let (proof, credentials) = authentication.into_parts();
                if definition.retain_refresh_tokens() {
                    let credentials = credentials.ok_or(InteractionError::Unsupported)?;
                    let ciphertext = protect(
                        proof.binding(),
                        &format!("upstream:{}", proof.identity().subject().as_str()),
                        credentials.protectable().map_err(error)?,
                    )
                    .await?;
                    StorageExternalInteractionAuthentication::LoginWithCredential {
                        authentication: proof,
                        credential: StorageEncryptedUpstreamCredential::new(ciphertext)
                            .map_err(|_| InteractionError::Invalid)?,
                    }
                } else {
                    StorageExternalInteractionAuthentication::Login(proof)
                }
            };
            super::external::authenticate(
                context,
                claim.claim().clone(),
                authentication,
                Some(rotated.binding()),
            )
            .await?;
        }
        Ok::<_, ApiError>(())
    }
    .await;
    if let Err(error) = result {
        let _ = super::advance(
            context,
            StorageInteractionAuthorization::Claim(claim.claim().clone()),
            StorageInteractionAction::Fail,
        )
        .await;
        return Err(error);
    }
    Ok((id, rotated))
}

use hubuum_storage_core::StorageInteractionAuthentication;

pub(crate) async fn renew(
    storage: &crate::storage::StorageHandle,
    principal: PrincipalId,
    provider: &StorageAuthenticationProvider,
) -> Result<bool, ApiError> {
    use hubuum_auth_oidc::RetainedCredentials;
    use hubuum_storage_core::{
        ExternalIdentityStorage, StorageEncryptedUpstreamCredential, StorageExternalSyncFailure,
        StorageUpstreamClaimOutcome, StorageUpstreamRenewal,
    };
    let Some(StorageBrowserProviderDefinition::Oidc(definition)) = provider.browser_definition()
    else {
        return Ok(false);
    };
    if !definition.retain_refresh_tokens() {
        return Ok(false);
    }
    let claim = match storage.claim_upstream_credential(principal).await? {
        StorageUpstreamClaimOutcome::Unavailable => return Ok(false),
        StorageUpstreamClaimOutcome::Busy => {
            return Err(ApiError::ServiceUnavailable(
                "External identity refresh is in progress".into(),
            ));
        }
        StorageUpstreamClaimOutcome::Claim(claim) => *claim,
    };
    // Bound the complete attempt, including secret resolution. Once claimed,
    // no failure may leave a reusable upstream token behind.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(25), async {
        let original = claim.session();
        let purpose = format!("upstream:{}", original.subject());
        let provider_context = ExternalAuthenticationContext::oidc(
            original.binding().clone(),
            IdentityScopeName::new(original.scope()).map_err(|_| InteractionError::Invalid)?,
            definition,
        )
        .map_err(|_| InteractionError::Invalid)?;
        let credentials = RetainedCredentials::restore(
            &unprotect(
                original.binding(),
                &purpose,
                claim.credential().ciphertext(),
            )
            .await?,
        )
        .map_err(error)?;
        let authentication = configured_adapter(provider)
            .await?
            .refresh(credentials, &provider_context)
            .await
            .map_err(error)?;
        let (proof, credentials) = authentication.into_parts();
        let credentials = match credentials {
            Some(credentials) => Some(
                StorageEncryptedUpstreamCredential::new(
                    protect(
                        original.binding(),
                        &purpose,
                        credentials.protectable().map_err(error)?,
                    )
                    .await?,
                )
                .map_err(|_| InteractionError::Invalid)?,
            ),
            None => None,
        };
        StorageUpstreamRenewal::new(claim.clone(), proof, credentials)
            .map_err(|_| ApiError::from(InteractionError::Invalid))
    })
    .await
    .unwrap_or_else(|_| Err(InteractionError::ProviderUnavailable.into()));
    match outcome {
        Ok(renewal) => {
            storage.finish_upstream_renewal(renewal).await?;
            Ok(true)
        }
        Err(failure) => {
            let temporary = matches!(
                failure,
                ApiError::AuthenticationInteraction(InteractionError::ProviderUnavailable)
                    | ApiError::ServiceUnavailable(_)
            );
            storage
                .fail_upstream_renewal(
                    claim,
                    if temporary {
                        StorageExternalSyncFailure::Unavailable
                    } else {
                        StorageExternalSyncFailure::Rejected
                    },
                )
                .await?;
            if temporary {
                Ok(false)
            } else {
                Err(ApiError::Unauthorized(
                    "External identity is no longer valid; sign in again".into(),
                ))
            }
        }
    }
}

/// Snapshot only after authorizing the local operation, before it commits.
pub(crate) async fn principal_revocations(
    context: &AppContext,
    principal: PrincipalId,
) -> Vec<StorageUpstreamSession> {
    session_revocations(
        context,
        hubuum_storage_core::SessionRevocation::All(principal),
    )
    .await
}

pub(crate) async fn session_revocations(
    context: &AppContext,
    target: hubuum_storage_core::SessionRevocation,
) -> Vec<StorageUpstreamSession> {
    use hubuum_storage_core::ExternalIdentityStorage;
    match storage_handle(context)
        .upstream_logout_credentials(target.into())
        .await
    {
        Ok(records) => records,
        Err(_) => {
            tracing::warn!(
                message = "OIDC revocation snapshot unavailable; continuing local revocation"
            );
            Vec::new()
        }
    }
}

/// Invoke only after local revocation has committed. Provider failure cannot
/// restore local credentials or delay the local response.
pub(crate) fn revoke_best_effort(records: Vec<StorageUpstreamSession>, context: AppContext) {
    queue_revocations(records, RevocationProvider::Live(context));
}

pub(crate) fn revoke_provider_best_effort(
    records: Vec<StorageUpstreamSession>,
    provider: StorageAuthenticationProvider,
) {
    queue_revocations(records, RevocationProvider::Previous(provider));
}

#[derive(Clone)]
enum RevocationProvider {
    Live(AppContext),
    Previous(StorageAuthenticationProvider),
}

fn queue_revocations(records: Vec<StorageUpstreamSession>, selection: RevocationProvider) {
    static REVOCATIONS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(32);
    for record in records {
        let Ok(permit) = REVOCATIONS.try_acquire() else {
            tracing::warn!(
                message = "OIDC revocation capacity reached; local logout remains complete"
            );
            continue;
        };
        let selection = selection.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = tokio::time::timeout(std::time::Duration::from_secs(25), async {
                let Some(credential) = record.credential() else {
                    return Ok::<_, ApiError>(());
                };
                let selected = match selection {
                    RevocationProvider::Live(context) => {
                        provider(&context, record.binding()).await?
                    }
                    RevocationProvider::Previous(provider) => provider,
                };
                let credentials = hubuum_auth_oidc::RetainedCredentials::restore(
                    &unprotect(
                        record.binding(),
                        &format!("upstream:{}", record.subject()),
                        credential.ciphertext(),
                    )
                    .await?,
                )
                .map_err(error)?;
                configured_adapter(&selected)
                    .await?
                    .revoke(credentials)
                    .await
                    .map_err(error)?;
                Ok(())
            })
            .await;
            if !matches!(result, Ok(Ok(()))) {
                tracing::warn!(
                    message = "OIDC revocation unavailable; local logout remains complete"
                );
            }
        });
    }
}
