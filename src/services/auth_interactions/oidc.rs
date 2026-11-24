//! Shared server-owned OIDC/OAuth redirects and single-use, cookie-bound callbacks.
use hubuum_auth_core::ExternalAuthenticationContext;
use hubuum_auth_oidc::{OidcAdapter, OidcError};

mod adapters;
mod revocation;
use adapters::{BrowserAdapter, PendingLogin, RetainedCredentials};
use hubuum_domain::{
    IdentityScopeName, InteractionBinding, InteractionDigest, InteractionId, InteractionPurpose,
};
use hubuum_domain::{PrincipalId, ResourceRevision};
use hubuum_secrets::SecretValue;
use hubuum_storage_core::StorageEncryptedUpstreamCredential;
use hubuum_storage_core::StorageRuntimeSettingsValue;
use hubuum_storage_core::{AuthSessionId, ExternalIdentityStorage, StorageError};
use hubuum_storage_core::{
    StorageAuthenticationProvider, StorageBrowserProviderDefinition,
    StorageExternalInteractionAuthentication, StorageInteractionAction,
    StorageInteractionAuthorization, StorageInteractionOutcome,
    StorageProtectedAuthenticationState,
};
use hubuum_storage_core::{StorageUpstreamCleanup, StorageUpstreamFailure};
pub(crate) use revocation::{revoke_best_effort, revoke_provider_best_effort};
use revocation::{revoke_uninstalled_oidc, revoke_unsubmitted_oidc};
use sha2::{Digest, Sha256};
use std::{future::Future, time::Duration};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use crate::{
    errors::{ApiError, InteractionError},
    models::auth_interaction::browser::BrowserSession,
    permissions::AppContext,
    restores::MaintenanceActivityGuard,
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
) -> Result<BrowserAdapter, ApiError> {
    super::browser::settings(context)?;
    configured_adapter(provider).await
}

async fn configured_adapter(
    provider: &StorageAuthenticationProvider,
) -> Result<BrowserAdapter, ApiError> {
    let secret = crate::services::security_candidates::providers::browser_secret(provider)
        .await?
        .ok_or(InteractionError::ProviderUnavailable)?;
    let config = crate::config::get_config()?;
    let callback = config
        .auth_browser
        .callback(provider.id().ok_or(InteractionError::Invalid)?)?;
    let private = config.auth_runtime_allow_private_targets;
    match provider.browser_definition() {
        Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
            OidcAdapter::new(definition.clone(), secret, &callback, private)
                .map(BrowserAdapter::Oidc)
        }
        Some(StorageBrowserProviderDefinition::Oauth(definition)) => {
            hubuum_auth_oauth::OAuthAdapter::new(definition.clone(), secret, &callback, private)
                .map(BrowserAdapter::OAuth)
        }
        None => return Err(InteractionError::Unsupported.into()),
    }
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
    super::external::provider_configuration(context, binding).await
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
        Some(
            StorageBrowserProviderDefinition::Oidc(_) | StorageBrowserProviderDefinition::Oauth(_)
        )
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
        .map_err(error)?;
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
    let mut failure_action = StorageInteractionAction::Fail;
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
        let adapter = adapter(context, &selected).await?;
        let accepted = adapter
            .accept(pending, input.state, input.issuer, input.code, input.error)
            .map_err(error)?;
        let scope =
            IdentityScopeName::new(selected.name()).map_err(|_| InteractionError::Invalid)?;
        let provider_context = match selected.browser_definition() {
            Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
                ExternalAuthenticationContext::oidc(
                    claim.claim().binding().clone(),
                    scope,
                    definition,
                )
            }
            Some(StorageBrowserProviderDefinition::Oauth(definition)) => {
                ExternalAuthenticationContext::oauth(
                    claim.claim().binding().clone(),
                    scope,
                    definition,
                )
            }
            None => return Err(InteractionError::Unsupported.into()),
        }
        .map_err(|_| InteractionError::Invalid)?;
        let authentication = match adapter.exchange(accepted, &provider_context).await {
            Err(OidcError::MembershipRejected(rejection))
                if view.binding().authority().purpose()
                    != InteractionPurpose::ProviderVerification =>
            {
                failure_action = StorageInteractionAction::RejectMembership(rejection);
                return Err(InteractionError::Invalid.into());
            }
            outcome => outcome.map_err(error)?,
        };
        if view.binding().authority().purpose() == InteractionPurpose::ProviderVerification {
            let (proof, credentials) = authentication.into_parts();
            if retain_credentials(&selected) && credentials.is_none() {
                return Err(InteractionError::Unsupported.into());
            }
            let verified =
                crate::services::security_candidates::verify_browser_interaction(context, &proof)
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
                let (proof, credentials) = authentication.fresh(fresh).map_err(error)?;
                if retain_credentials(&selected) {
                    let credentials = credentials.ok_or(InteractionError::Unsupported)?;
                    if let Some(approval) = storage_handle(context)
                        .upstream_approval_session(proof.clone())
                        .await?
                    {
                        let ciphertext = protect(
                            approval.binding(),
                            &format!(
                                "upstream:{}",
                                proof.authentication().identity().subject().as_str()
                            ),
                            credentials.protectable().map_err(error)?,
                        )
                        .await?;
                        StorageExternalInteractionAuthentication::ApprovalWithCredential {
                            approval: Box::new(approval),
                            credential: StorageEncryptedUpstreamCredential::new(ciphertext)
                                .map_err(|_| InteractionError::Invalid)?,
                        }
                    } else {
                        StorageExternalInteractionAuthentication::Approval(proof)
                    }
                } else {
                    StorageExternalInteractionAuthentication::Approval(proof)
                }
            } else {
                let (proof, credentials) = authentication.into_parts();
                if retain_credentials(&selected) {
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
        super::advance(
            context,
            StorageInteractionAuthorization::Claim(claim.claim().clone()),
            failure_action,
        )
        .await?;
        return Err(error);
    }
    Ok((id, rotated))
}

use hubuum_storage_core::StorageInteractionAuthentication;

pub(crate) async fn renew(
    storage: &crate::storage::StorageHandle,
    principal: PrincipalId,
    revision: ResourceRevision,
    provider: &StorageAuthenticationProvider,
) -> Result<bool, ApiError> {
    if !retain_credentials(provider) {
        return Ok(false);
    }
    // Keep the claim transaction and its cleanup alive if the HTTP client
    // disconnects, including while storage is acknowledging the claim.
    let storage = storage.clone();
    let provider = provider.clone();
    let oauth_deadline = matches!(
        provider.browser_definition(),
        Some(StorageBrowserProviderDefinition::Oauth(_))
    )
    .then(|| Instant::now() + RENEWAL_TIMEOUT);
    let worker = MaintenanceActivityGuard::begin().spawn(async move {
        let attempt = || claim_and_renew(&storage, principal, revision, &provider, oauth_deadline);
        if let Some(deadline) = oauth_deadline {
            return renew_oauth_grants(deadline, attempt).await;
        }
        Ok(matches!(attempt().await?, RenewalAttempt::Refreshed))
    });
    // The caller can time out while the worker is acknowledging a known
    // rejection. The worker keeps its drain guard through that final write.
    let joined = match oauth_deadline {
        Some(deadline) => timeout_at(deadline, worker)
            .await
            .map_err(|_| oauth_budget_exhausted())?,
        None => worker.await,
    };
    joined.map_err(|_| {
        ApiError::ServiceUnavailable("External identity refresh was interrupted".into())
    })?
}

const RENEWAL_TIMEOUT: Duration = Duration::from_secs(25);
const MAX_OAUTH_GRANTS_PER_REQUEST: usize = 8;

async fn renew_oauth_grants<F>(
    deadline: Instant,
    mut attempt: impl FnMut() -> F,
) -> Result<bool, ApiError>
where
    F: Future<Output = Result<RenewalAttempt, ApiError>>,
{
    for _ in 0..MAX_OAUTH_GRANTS_PER_REQUEST {
        if Instant::now() >= deadline {
            break;
        }
        match attempt().await? {
            RenewalAttempt::Refreshed => return Ok(true),
            RenewalAttempt::Unavailable => return Ok(false),
            RenewalAttempt::ExpiredCredential => continue,
        }
    }
    Err(oauth_budget_exhausted())
}

fn oauth_budget_exhausted() -> ApiError {
    ApiError::ServiceUnavailable(
        "OAuth credential checks exceeded the request budget; retry".into(),
    )
}

async fn claim_and_renew(
    storage: &crate::storage::StorageHandle,
    principal: PrincipalId,
    revision: ResourceRevision,
    provider: &StorageAuthenticationProvider,
    oauth_deadline: Option<Instant>,
) -> Result<RenewalAttempt, ApiError> {
    use hubuum_storage_core::StorageUpstreamClaimOutcome;
    let acquire = storage.claim_upstream_credential(revision, principal);
    // A cancelled OAuth claim leaves its read-only bearer recoverable when the lease
    // expires. Always await OIDC ownership and any subsequent finalization.
    let outcome = match oauth_deadline {
        Some(deadline) => timeout_at(deadline, acquire)
            .await
            .map_err(|_| oauth_budget_exhausted())??,
        None => acquire.await?,
    };
    let claim = match outcome {
        StorageUpstreamClaimOutcome::Unavailable
            if matches!(
                provider.browser_definition(),
                Some(StorageBrowserProviderDefinition::Oauth(_))
            ) =>
        {
            return Err(ApiError::Unauthorized(
                "External identity requires a new upstream sign-in".into(),
            ));
        }
        StorageUpstreamClaimOutcome::Unavailable | StorageUpstreamClaimOutcome::RetryLater => {
            return Ok(RenewalAttempt::Unavailable);
        }
        StorageUpstreamClaimOutcome::Busy => {
            return Err(ApiError::ServiceUnavailable(
                "External identity refresh is in progress".into(),
            ));
        }
        StorageUpstreamClaimOutcome::Claim(claim) => *claim,
    };
    attempt_renewal(
        storage,
        claim,
        provider,
        oauth_deadline.unwrap_or_else(|| Instant::now() + RENEWAL_TIMEOUT),
    )
    .await
}

enum RenewalAttempt {
    Refreshed,
    Unavailable,
    ExpiredCredential,
}

// Only a validation failure from the dispatched provider operation can invalidate
// the principal. Local envelope, configuration and persistence errors cannot.
enum RenewalFailure {
    Local(ApiError),
    Provider(OidcError),
}
impl From<ApiError> for RenewalFailure {
    fn from(error: ApiError) -> Self {
        Self::Local(error)
    }
}
impl From<InteractionError> for RenewalFailure {
    fn from(error: InteractionError) -> Self {
        Self::Local(error.into())
    }
}
impl RenewalFailure {
    fn disposition(
        self,
        submitted: bool,
    ) -> (StorageUpstreamFailure, Result<RenewalAttempt, ApiError>) {
        let rejected = matches!(
            &self,
            Self::Provider(
                OidcError::Denied | OidcError::MembershipRejected(_) | OidcError::InvalidResponse
            )
        );
        let failure = match self {
            Self::Local(failure) => failure,
            Self::Provider(failure) => error(failure),
        };
        let storage = if !submitted {
            StorageUpstreamFailure::UnavailableBeforeSubmission
        } else if rejected {
            StorageUpstreamFailure::Rejected
        } else {
            StorageUpstreamFailure::Unavailable
        };
        let result = if storage == StorageUpstreamFailure::Rejected {
            Err(ApiError::Unauthorized(
                "External identity is no longer valid; sign in again".into(),
            ))
        } else if matches!(
            failure,
            ApiError::AuthenticationInteraction(
                InteractionError::ProviderUnavailable | InteractionError::Unsupported
            ) | ApiError::ServiceUnavailable(_)
        ) {
            Ok(RenewalAttempt::Unavailable)
        } else {
            Err(ApiError::ServiceUnavailable(
                "Retained authentication could not be used".into(),
            ))
        };
        (storage, result)
    }
}

async fn attempt_renewal(
    storage: &crate::storage::StorageHandle,
    claim: hubuum_storage_core::StorageUpstreamClaim,
    provider: &StorageAuthenticationProvider,
    deadline: Instant,
) -> Result<RenewalAttempt, ApiError> {
    use hubuum_storage_core::StorageUpstreamRenewal;
    // Bound the complete attempt, including secret resolution, to the claim lease.
    let submitted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dispatch = submitted.clone();
    let session = claim.session().session();
    let mut returned_grant = None;
    let outcome = timeout_at(deadline, async {
        let original = claim.session();
        let purpose = format!("upstream:{}", original.subject());
        let scope =
            IdentityScopeName::new(original.scope()).map_err(|_| InteractionError::Invalid)?;
        let provider_context = match provider.browser_definition() {
            Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
                ExternalAuthenticationContext::oidc(original.binding().clone(), scope, definition)
            }
            Some(StorageBrowserProviderDefinition::Oauth(definition)) => {
                ExternalAuthenticationContext::oauth(original.binding().clone(), scope, definition)
            }
            None => return Err(InteractionError::Unsupported.into()),
        }
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
        let authentication = match configured_adapter(provider)
            .await?
            .refresh(
                credentials,
                &provider_context,
                move || dispatch.store(true, std::sync::atomic::Ordering::Relaxed),
                |grant| returned_grant = Some(grant),
            )
            .await
        {
            Ok(authentication) => authentication,
            Err(OidcError::CredentialExpired) => return Ok(None),
            Err(failure) => return Err(RenewalFailure::Provider(failure)),
        };
        let (proof, credentials) = authentication.into_parts();
        let credentials = match credentials {
            Some(credentials) => Some(
                StorageEncryptedUpstreamCredential::new(
                    protect(
                        original.binding(),
                        &purpose,
                        credentials
                            .protectable()
                            .map_err(|_| InteractionError::ProviderUnavailable)?,
                    )
                    .await?,
                )
                .map_err(|_| InteractionError::ProviderUnavailable)?,
            ),
            None => None,
        };
        StorageUpstreamRenewal::new(claim.clone(), proof, credentials)
            .map(Some)
            .map_err(|_| RenewalFailure::from(InteractionError::Invalid))
    })
    .await
    .unwrap_or_else(|_| Err(InteractionError::ProviderUnavailable.into()));
    match outcome {
        Ok(None) => {
            storage
                .fail_upstream_renewal(claim, StorageUpstreamFailure::CredentialExpired)
                .await?;
            Ok(RenewalAttempt::ExpiredCredential)
        }
        Ok(Some(renewal)) => {
            cleanup_after_storage(
                session,
                storage
                    .finish_upstream_renewal(renewal)
                    .await
                    .map(|()| StorageUpstreamCleanup::Preserve),
                || revoke_uninstalled_oidc(provider, returned_grant),
            )?;
            Ok(RenewalAttempt::Refreshed)
        }
        Err(failure) => {
            let (disposition, result) =
                failure.disposition(submitted.load(std::sync::atomic::Ordering::Relaxed));
            if disposition == StorageUpstreamFailure::UnavailableBeforeSubmission
                && matches!(
                    provider.browser_definition(),
                    Some(StorageBrowserProviderDefinition::Oidc(_))
                )
            {
                cleanup_after_storage(
                    session,
                    storage
                        .fail_upstream_renewal(claim.clone(), disposition)
                        .await,
                    || revoke_unsubmitted_oidc(provider, claim),
                )?;
            } else {
                // A fresh login may have superseded this claim and reused the same
                // upstream client session. Wait for storage before revoking anything.
                cleanup_after_storage(
                    session,
                    storage.fail_upstream_renewal(claim, disposition).await,
                    || revoke_uninstalled_oidc(provider, returned_grant),
                )?;
            }
            result
        }
    }
}

fn cleanup_after_storage(
    session: AuthSessionId,
    result: Result<StorageUpstreamCleanup, StorageError>,
    revoke: impl FnOnce(),
) -> Result<(), StorageError> {
    let safe_to_revoke = match &result {
        Ok(cleanup) => *cleanup == StorageUpstreamCleanup::Revoke,
        Err(error) => error.revoked_upstream_session() == Some(session),
    };
    if safe_to_revoke {
        revoke();
    }
    result.map(|_| ())
}

fn retain_credentials(provider: &StorageAuthenticationProvider) -> bool {
    match provider.browser_definition() {
        Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
            definition.retain_refresh_tokens()
        }
        Some(StorageBrowserProviderDefinition::Oauth(_)) => true,
        None => false,
    }
}

#[cfg(test)]
mod renewal_tests {
    use super::{
        ApiError, AuthSessionId, InteractionError, OidcError, RenewalFailure,
        StorageUpstreamCleanup, StorageUpstreamFailure, cleanup_after_storage,
    };
    use hubuum_storage_core::StorageError;
    use uuid::Uuid;

    #[tokio::test]
    async fn oauth_scan_stops_claiming_after_its_attempt_budget() {
        let mut calls = 0;
        let result =
            super::renew_oauth_grants(super::Instant::now() + super::RENEWAL_TIMEOUT, || {
                calls += 1;
                std::future::ready(Ok(super::RenewalAttempt::ExpiredCredential))
            })
            .await;
        assert!(matches!(result, Err(ApiError::ServiceUnavailable(_))));
        assert_eq!(calls, super::MAX_OAUTH_GRANTS_PER_REQUEST);
    }

    #[tokio::test]
    async fn oauth_scan_finishes_an_admitted_result_and_skips_the_next_claim_after_deadline() {
        let mut calls = 0;
        let deadline = super::Instant::now() + super::Duration::from_secs(1);
        let result = super::renew_oauth_grants(deadline, || {
            calls += 1;
            async move {
                tokio::time::sleep_until(deadline).await;
                Ok(super::RenewalAttempt::ExpiredCredential)
            }
        })
        .await;
        assert!(matches!(result, Err(ApiError::ServiceUnavailable(_))));
        assert_eq!(calls, 1);
    }

    #[rstest::rstest]
    #[case::logout_won(
        Err(StorageError::upstream_session_revoked(AuthSessionId::new(Uuid::nil()))),
        true
    )]
    #[case::different_session(
        Err(StorageError::upstream_session_revoked(AuthSessionId::new(Uuid::max()))),
        false
    )]
    #[case::superseded(
        Err(StorageError::authentication_required("Upstream session changed")),
        false
    )]
    #[case::unknown_commit(Err(StorageError::unavailable("lost acknowledgement")), false)]
    fn rejected_storage_write_only_revokes_after_confirmed_logout(
        #[case] result: Result<StorageUpstreamCleanup, StorageError>,
        #[case] expected: bool,
    ) {
        let mut revoked = false;
        assert!(
            cleanup_after_storage(AuthSessionId::new(Uuid::nil()), result, || revoked = true)
                .is_err()
        );
        assert_eq!(revoked, expected);
    }

    #[rstest::rstest]
    #[case::retained(StorageUpstreamCleanup::Preserve, false)]
    #[case::discarded(StorageUpstreamCleanup::Revoke, true)]
    fn committed_storage_write_only_revokes_a_discarded_grant(
        #[case] decision: StorageUpstreamCleanup,
        #[case] expected: bool,
    ) {
        let mut revoked = false;
        cleanup_after_storage(AuthSessionId::new(Uuid::nil()), Ok(decision), || {
            revoked = true
        })
        .unwrap();
        assert_eq!(revoked, expected);
    }

    #[rstest::rstest]
    #[case::unreadable_retained_grant(RenewalFailure::Local(InteractionError::Invalid.into()), false, StorageUpstreamFailure::UnavailableBeforeSubmission)]
    #[case::local_error_after_dispatch(RenewalFailure::Local(InteractionError::Invalid.into()), true, StorageUpstreamFailure::Unavailable)]
    #[case::mismatched_stored_protocol(
        RenewalFailure::Provider(OidcError::Denied),
        false,
        StorageUpstreamFailure::UnavailableBeforeSubmission
    )]
    #[case::malformed_id_token(
        RenewalFailure::Provider(OidcError::InvalidResponse),
        true,
        StorageUpstreamFailure::Rejected
    )]
    #[case::upstream_rejection(
        RenewalFailure::Provider(OidcError::Denied),
        true,
        StorageUpstreamFailure::Rejected
    )]
    fn failed_renewal_preserves_authority_until_provider_rejection(
        #[case] failure: RenewalFailure,
        #[case] submitted: bool,
        #[case] expected: StorageUpstreamFailure,
    ) {
        let (disposition, result) = failure.disposition(submitted);
        assert_eq!(disposition, expected);
        assert!(matches!(
            (disposition, result),
            (
                StorageUpstreamFailure::Rejected,
                Err(ApiError::Unauthorized(_))
            ) | (_, Err(ApiError::ServiceUnavailable(_)))
        ));
    }
}
