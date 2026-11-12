//! Resolve current provider authority and orchestrate secret-protected interactions.
pub mod browser;
use std::net::IpAddr;

use chrono::{Duration, Utc};
use hubuum_domain::{
    DEFAULT_INTERACTION_LIFETIME_SECONDS, IdentityScopeName, InteractionAuthority,
    InteractionBinding, InteractionBrowserBinding, InteractionId, InteractionLifecycle,
    InteractionMode, InteractionProviderId, ResourceRevision, VerifiedInteractionSubject,
};
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    AuthenticationStorage, ObservedSource, RuntimeSettingsStorage, StorageError, StorageErrorKind,
    StorageInteractionAccess, StorageInteractionAction, StorageInteractionAuthentication,
    StorageInteractionAuthorization, StorageInteractionCommand, StorageInteractionCreate,
    StorageInteractionOutcome, StorageInteractionView, StorageProviderIdentityBinding,
    StorageRuntimeSettingsGroup,
};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::errors::{ApiError, InteractionError};
use crate::middlewares::rate_limit::{
    LoginAttemptOutcome, begin_login_attempt, finish_login_attempt,
};
use crate::models::auth_interaction::{
    AuthenticationMethod, AuthenticationMethods, InteractionBeginRequest, InteractionBegun,
    InteractionCredentialsRequest, InteractionDelivery, InteractionNext, InteractionSecret,
    InteractionStatus, PasswordInteractionMode, PasswordProviderKind,
};
use crate::models::{LOCAL_IDENTITY_SCOPE, LoginUser};
use crate::permissions::AppContext;
use crate::storage::storage_handle;

struct PasswordProvider {
    id: InteractionProviderId,
    scope: IdentityScopeName,
    kind: PasswordProviderKind,
    binding: Option<StorageProviderIdentityBinding>,
}

async fn providers(
    context: &AppContext,
) -> Result<(ResourceRevision, Vec<PasswordProvider>), ApiError> {
    let storage = storage_handle(context);
    storage
        .applied_runtime_settings()
        .ensure_security_current()
        .map_err(storage_error)?;
    let snapshot = storage
        .load_runtime_settings()
        .await
        .map_err(storage_error)?;
    let revision = snapshot
        .record(StorageRuntimeSettingsGroup::Authentication)
        .revision();
    let mut providers = Vec::new();
    for (scope, kind) in crate::auth::interaction_password_providers(revision)? {
        let name = IdentityScopeName::new(scope.clone())
            .map_err(|_| InteractionError::ProviderUnavailable)?;
        let (id, kind, binding) = match kind.as_str() {
            "local" if scope == LOCAL_IDENTITY_SCOPE => (
                InteractionProviderId::local(),
                PasswordProviderKind::Local,
                None,
            ),
            "ldap" => {
                let binding = storage
                    .authentication_provider_binding(name.clone())
                    .await
                    .map_err(storage_error)?
                    .ok_or(InteractionError::ProviderUnavailable)?;
                if binding.kind().as_str() != "ldap" {
                    return Err(InteractionError::ProviderUnavailable.into());
                }
                (
                    InteractionProviderId::external(binding.id())
                        .map_err(|_| InteractionError::ProviderUnavailable)?,
                    PasswordProviderKind::Ldap,
                    Some(binding),
                )
            }
            _ => continue,
        };
        providers.push(PasswordProvider {
            id,
            scope: name,
            kind,
            binding,
        });
    }
    if providers.len() > 64 {
        return Err(InteractionError::ProviderUnavailable.into());
    }
    Ok((revision, providers))
}

pub async fn methods(context: &AppContext) -> Result<AuthenticationMethods, ApiError> {
    let (_, providers) = providers(context).await?;
    let browser_available = browser::settings(context)?.enabled();
    Ok(AuthenticationMethods {
        interaction_version: 1,
        methods: providers
            .into_iter()
            .map(|provider| AuthenticationMethod {
                provider_id: provider.id,
                name: provider.scope.as_str().to_owned(),
                kind: provider.kind,
                interaction: PasswordInteractionMode::Credentials,
                fresh_authentication: true,
                browser_available,
            })
            .collect(),
    })
}

pub async fn begin(
    context: &AppContext,
    mut input: InteractionBeginRequest,
    authenticated: Option<&crate::extractors::Authenticated>,
) -> Result<InteractionBegun, ApiError> {
    input.validate()?;
    let secret = InteractionSecret::generate();
    let mut verification_provider = None;
    let mut verification_receipt = None;
    let (authority, actor_lease) = if let Some(operation) = input.operation.as_mut() {
        use super::credential_approvals::{CredentialActor, interaction_operation_digest};
        use hubuum_domain::InteractionActor;
        use hubuum_storage_core::StorageInteractionActorLease;
        let authenticated = authenticated
            .ok_or_else(|| ApiError::Unauthorized("Existing human bearer required".into()))?;
        let actor = CredentialActor::resolve(context, authenticated).await?;
        actor.authorize(context, operation).await?;
        operation.resolve_defaults()?;
        if let crate::models::credential_approval::CredentialOperation::CreateToken {
            principal_id,
            token,
        } = operation
        {
            token.clone().into_create_request(*principal_id)?;
        }
        let digest = interaction_operation_digest(context, &actor, operation, &secret).await?;
        let owner = actor.owner();
        let expires = authenticated.token_meta.expires_at().unwrap_or(
            authenticated.token_meta.issued()
                + Duration::hours(crate::config::get_config()?.token_lifetime_hours),
        );
        (
            InteractionAuthority::approval(
                InteractionActor::new(owner.actor_id(), owner.token_id()),
                digest,
            ),
            Some(StorageInteractionActorLease::approval(expires)),
        )
    } else if let Some(candidate_id) = input.candidate_id {
        use super::credential_approvals::CredentialActor;
        use hubuum_domain::{InteractionActor, InteractionDigest};
        use hubuum_storage_core::StorageInteractionActorLease;
        let provider_id = input
            .provider_id
            .external_id()
            .ok_or(InteractionError::Unsupported)?;
        let authenticated = authenticated.ok_or_else(|| {
            ApiError::Unauthorized("Existing local administrator bearer required".into())
        })?;
        let actor = CredentialActor::resolve(context, authenticated).await?;
        let (candidate, provider) = super::security_candidates::interaction_candidate(
            context,
            &actor,
            candidate_id,
            provider_id,
        )
        .await?;
        let owner = actor.owner();
        let authority = InteractionAuthority::verification(
            InteractionActor::new(owner.actor_id(), owner.token_id()),
            candidate_id,
            StorageInteractionAuthentication::candidate_digest(&candidate),
        )
        .map_err(|_| InteractionError::Invalid)?;
        let expires = authenticated.token_meta.expires_at().unwrap_or(
            authenticated.token_meta.issued()
                + Duration::hours(crate::config::get_config()?.token_lifetime_hours),
        );
        verification_receipt = Some(
            InteractionDigest::new(secret.verification_receipt().digest().as_str())
                .map_err(|_| InteractionError::Invalid)?,
        );
        verification_provider = Some((
            candidate.base_revision(),
            PasswordProvider {
                id: input.provider_id,
                scope: IdentityScopeName::new(provider.name())
                    .map_err(|_| InteractionError::Invalid)?,
                kind: PasswordProviderKind::Ldap,
                binding: Some(provider),
            },
        ));
        (
            authority,
            Some(
                StorageInteractionActorLease::verification(
                    expires,
                    crate::config::get_config()?.admin_groupname.clone(),
                )
                .map_err(|_| InteractionError::Invalid)?,
            ),
        )
    } else {
        (InteractionAuthority::login(), None)
    };
    let (revision, provider) = match verification_provider {
        Some(prepared) => prepared,
        None => {
            let (revision, providers) = providers(context).await?;
            let provider = providers
                .into_iter()
                .find(|provider| provider.id == input.provider_id)
                .ok_or(InteractionError::Unsupported)?;
            (revision, provider)
        }
    };
    let browser_settings = browser::settings(context)?;
    let browser_mode = input.interaction_mode == PasswordInteractionMode::Browser
        || input.delivery == InteractionDelivery::Bff;
    if browser_mode {
        browser_settings.origin()?;
    }
    let return_target = match input.delivery {
        InteractionDelivery::Poll => None,
        InteractionDelivery::Bff => Some(
            browser_settings.return_target(
                input
                    .return_target_id
                    .as_deref()
                    .ok_or(InteractionError::Invalid)?,
                &input.client_id,
                input.return_nonce.ok_or(InteractionError::Invalid)?,
            )?,
        ),
    };
    let mut binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).expect("random UUID"),
        provider.id,
        input.client_id,
        revision,
        authority,
    )
    .map_err(|_| InteractionError::Invalid)?;
    if let Some(operation) = input.operation.as_ref() {
        let sealed = secret.seal_operation(binding.id(), operation)?;
        binding = binding
            .with_operation(sealed)
            .map_err(|_| InteractionError::Invalid)?;
    }
    if browser_mode {
        binding = binding.with_prompt(
            hubuum_domain::InteractionBrowserPrompt::new(
                provider.scope.clone(),
                browser::request_details(input.operation.as_ref(), input.candidate_id)?,
            )
            .map_err(|_| {
                ApiError::BadRequest("Browser approval details exceed the supported bound".into())
            })?,
        );
    }
    if let Some(receipt) = verification_receipt {
        binding = binding
            .with_verification_receipt(receipt)
            .map_err(|_| InteractionError::Invalid)?;
    }
    if let Some(target) = return_target {
        binding = binding.with_return_target(target);
    }
    let now = Utc::now();
    let lifecycle = InteractionLifecycle::try_new(
        binding,
        if browser_mode {
            InteractionMode::Browser
        } else {
            InteractionMode::Credentials
        },
        now,
        now + Duration::seconds(DEFAULT_INTERACTION_LIFETIME_SECONDS),
    )?;
    let mut create = StorageInteractionCreate::builder(lifecycle, secret.digest());
    if let Some(actor) = actor_lease {
        create = create.actor(actor);
    }
    let next = if browser_mode {
        let code = crate::models::auth_interaction::browser::BrowserUserCode::generate();
        create = create.user_code(code.digest());
        InteractionNext::Browser {
            verification_uri: browser_settings.verification_uri()?,
            user_code: code.display(),
        }
    } else {
        InteractionNext::Credentials
    };
    if let Some(binding) = provider.binding {
        create = create.provider(binding);
    }
    let request = create
        .try_build()
        .map_err(|_| ApiError::InternalServerError("Could not construct interaction".into()))?;
    let view = storage_handle(context)
        .create_auth_interaction(request)
        .await
        .map_err(|error| match error.kind() {
            StorageErrorKind::Conflict | StorageErrorKind::RateLimited => {
                InteractionError::SlowDown {
                    retry_after_seconds: 60,
                }
                .into()
            }
            StorageErrorKind::AuthenticationRequired => {
                InteractionError::ConfigurationChanged.into()
            }
            _ => storage_error(error),
        })?;
    Ok(InteractionBegun {
        id: view.binding().id(),
        interaction_version: 1,
        redemption_secret: secret,
        expires_at: view.expires_at(),
        interval_seconds: view.interval_seconds(),
        next,
    })
}

async fn advance(
    context: &AppContext,
    authorization: StorageInteractionAuthorization,
    action: StorageInteractionAction,
) -> Result<StorageInteractionOutcome, ApiError> {
    match storage_handle(context)
        .advance_auth_interaction(StorageInteractionCommand::new(authorization, action))
        .await
        .map_err(storage_error)?
    {
        StorageInteractionOutcome::Rejected(error) => Err(error.into()),
        outcome => Ok(outcome),
    }
}
fn view(outcome: StorageInteractionOutcome) -> Result<StorageInteractionView, ApiError> {
    match outcome {
        StorageInteractionOutcome::View(view) => Ok(view),
        StorageInteractionOutcome::Rejected(error) => Err(error.into()),
        _ => Err(ApiError::InternalServerError(
            "Unexpected interaction outcome".into(),
        )),
    }
}

pub async fn status(
    context: &AppContext,
    access: StorageInteractionAccess,
) -> Result<InteractionStatus, ApiError> {
    view(
        advance(
            context,
            StorageInteractionAuthorization::Redemption(access),
            StorageInteractionAction::Poll,
        )
        .await?,
    )
    .map(Into::into)
}

pub async fn cancel(
    context: &AppContext,
    access: StorageInteractionAccess,
) -> Result<(), ApiError> {
    view(
        advance(
            context,
            StorageInteractionAuthorization::Redemption(access),
            StorageInteractionAction::Cancel,
        )
        .await?,
    )?;
    Ok(())
}

pub async fn credentials(
    context: &AppContext,
    access: StorageInteractionAccess,
    input: InteractionCredentialsRequest,
    client_ip: Option<IpAddr>,
) -> Result<InteractionStatus, ApiError> {
    authenticate(
        context,
        StorageInteractionAuthorization::Redemption(access),
        input,
        client_ip,
        None,
    )
    .await
    .map(Into::into)
}

async fn authenticate(
    context: &AppContext,
    authorization: StorageInteractionAuthorization,
    input: InteractionCredentialsRequest,
    client_ip: Option<IpAddr>,
    approval: Option<InteractionBrowserBinding>,
) -> Result<StorageInteractionView, ApiError> {
    // Validate the password and account bounds before reserving a one-use claim.
    if input.name.is_empty() || input.name.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "name must contain 1-255 non-control characters".into(),
        ));
    }
    let validated = LoginUser {
        identity_scope: None,
        name: input.name,
        password: input.password,
    }
    .validate()?;
    let outcome = advance(
        context,
        authorization,
        StorageInteractionAction::Claim(Uuid::new_v4()),
    )
    .await?;
    let StorageInteractionOutcome::Claim(claim) = outcome else {
        return Err(ApiError::InternalServerError(
            "Expected authentication claim".into(),
        ));
    };
    let result = async {
        if claim.binding().authority().purpose()
            == hubuum_domain::InteractionPurpose::ProviderVerification
        {
            return browser::verify_candidate(context, claim.binding(), validated, client_ip).await;
        }

        let (revision, providers) = providers(context).await?;
        if revision != claim.binding().revision() {
            return Err(InteractionError::ConfigurationChanged.into());
        }
        let provider = providers
            .into_iter()
            .find(|provider| provider.id == claim.binding().provider())
            .ok_or(InteractionError::ConfigurationChanged)?;
        let login = validated.with_identity_scope(provider.scope);
        let Some(permit) =
            begin_login_attempt(login.identity_scope(), login.name(), client_ip).await?
        else {
            crate::observability::metrics::login_attempt("rate_limited");
            return Err(InteractionError::SlowDown {
                retry_after_seconds: 60,
            }
            .into());
        };
        let authenticated = crate::auth::login(context, login).await;
        let outcome = match &authenticated {
            Ok(_) => LoginAttemptOutcome::Succeeded,
            Err(ApiError::Unauthorized(_)) => LoginAttemptOutcome::Failed,
            Err(_) => LoginAttemptOutcome::Aborted,
        };
        finish_login_attempt(permit, outcome).await?;
        let authenticated = authenticated.map_err(|error| match error {
            ApiError::Unauthorized(_) => {
                crate::observability::metrics::login_attempt("bad_credentials");
                InteractionError::Invalid.into()
            }
            ApiError::ServiceUnavailable(_) => InteractionError::ProviderUnavailable.into(),
            other => other,
        })?;
        let login = super::authentication::login_evidence(context, &authenticated).await?;
        let subject = if provider.id.external_id().is_none() {
            authenticated.user().id.to_string()
        } else {
            crate::models::principal::load_principal_by_id(context, authenticated.user().id)
                .await?
                .external_subject
                .ok_or(InteractionError::Invalid)?
        };
        let proof = VerifiedInteractionSubject::new(claim.binding().clone(), subject)
            .map_err(|_| InteractionError::Invalid)?;
        StorageInteractionAuthentication::login(proof, &login, Utc::now())
            .map_err(|_| InteractionError::ConfigurationChanged.into())
    }
    .await;
    let authentication = match result {
        Ok(authentication) => authentication,
        Err(error) => {
            // Release a failed attempt; cancellation/crash leaves the bounded lease to fail closed.
            let _ = advance(
                context,
                StorageInteractionAuthorization::Claim(claim),
                StorageInteractionAction::Reject,
            )
            .await;
            return Err(api_storage_error(error));
        }
    };
    view(
        advance(
            context,
            StorageInteractionAuthorization::Claim(claim),
            StorageInteractionAction::Authenticate {
                authentication: Box::new(authentication),
                approval,
            },
        )
        .await?,
    )
}

pub async fn complete(
    context: &AppContext,
    access: StorageInteractionAccess,
    event_context: EventContext,
    source: ObservedSource,
    secret: InteractionSecret,
) -> Result<crate::models::auth_interaction::InteractionCompleted, ApiError> {
    use crate::models::auth_interaction::InteractionCompleted;
    use crate::models::credential_approval::{
        CredentialApprovalResponse, CredentialApprovalSecret,
    };
    use hubuum_domain::InteractionPurpose;
    use hubuum_storage_core::{
        StorageInteractionComplete, StorageInteractionCompletion, StorageInteractionIssuance,
    };
    let current = view(
        advance(
            context,
            StorageInteractionAuthorization::Redemption(access.clone()),
            StorageInteractionAction::Inspect,
        )
        .await?,
    )?;
    browser::settings(context)?.handoff(current.binding())?;
    if current.binding().authority().purpose() == InteractionPurpose::ProviderVerification {
        let binding = current.binding();
        let actor = super::credential_approvals::CredentialActor::from_interaction(
            context,
            binding
                .authority()
                .actor()
                .ok_or(InteractionError::Invalid)?,
        )
        .await?;
        let id = binding
            .authority()
            .candidate_id()
            .ok_or(InteractionError::Invalid)?;
        let (candidate, _) = super::security_candidates::interaction_candidate(
            context,
            &actor,
            id,
            binding
                .provider()
                .external_id()
                .ok_or(InteractionError::Invalid)?,
        )
        .await?;
        if binding.authority().candidate_digest()
            != Some(&StorageInteractionAuthentication::candidate_digest(
                &candidate,
            ))
        {
            return Err(InteractionError::ConfigurationChanged.into());
        }
        let receipt = secret.verification_receipt();
        if !binding.verification_receipt().is_some_and(|expected| {
            bool::from(
                expected
                    .as_str()
                    .as_bytes()
                    .ct_eq(receipt.digest().as_str().as_bytes()),
            )
        }) {
            return Err(InteractionError::Invalid.into());
        }
        let verification = crate::models::security_candidate::SecurityCandidateVerificationPreview::from_interaction(current.verification().ok_or(InteractionError::Pending)?)?;
        return match context
            .backend()
            .complete_auth_interaction(StorageInteractionComplete::new(
                access,
                StorageInteractionIssuance::ProviderVerification,
            ))
            .await
            .map_err(storage_error)?
        {
            StorageInteractionCompletion::ProviderVerification(id) => {
                let candidate = super::security_candidates::load(context, &actor, id).await?;
                Ok(InteractionCompleted::Verification(
                    crate::models::security_candidate::SecurityCandidateLdapVerificationResponse {
                        verification,
                        candidate: crate::models::security_candidate::SecurityCandidateReport::new(
                            &candidate,
                            Some(receipt),
                        ),
                    },
                ))
            }
            StorageInteractionCompletion::Rejected(error) => Err(error.into()),
            _ => Err(InteractionError::Invalid.into()),
        };
    }
    if current.binding().authority().purpose() == InteractionPurpose::CredentialApproval {
        let operation = secret.open_operation(current.binding())?;
        let bound_actor = current
            .binding()
            .authority()
            .actor()
            .ok_or(InteractionError::Invalid)?;
        let actor =
            super::credential_approvals::CredentialActor::from_interaction(context, bound_actor)
                .await?;
        let digest = super::credential_approvals::interaction_operation_digest(
            context, &actor, &operation, &secret,
        )
        .await?;
        if current.binding().authority().operation_digest() != Some(&digest) {
            return Err(InteractionError::Invalid.into());
        }
        actor.authorize(context, &operation).await?;
        let secret = CredentialApprovalSecret::generate();
        let claim = actor.claim(context, &secret, &operation).await?;
        let event_context = EventContext::user(
            bound_actor.principal_id(),
            event_context.request_id(),
            event_context.correlation_id().cloned(),
        );
        return match context
            .backend()
            .complete_auth_interaction(StorageInteractionComplete::new(
                access,
                StorageInteractionIssuance::Approval {
                    claim,
                    operation_digest: digest,
                    context: event_context,
                },
            ))
            .await
            .map_err(storage_error)?
        {
            StorageInteractionCompletion::Approval(result) => {
                Ok(InteractionCompleted::Approval(CredentialApprovalResponse {
                    approval: secret,
                    record: result.into_value().into(),
                    token_expires_at: operation.token_expiry(),
                }))
            }
            StorageInteractionCompletion::Rejected(error) => Err(error.into()),
            _ => Err(InteractionError::Invalid.into()),
        };
    }
    match super::auth_sessions::complete_interaction(context, access, event_context, source)
        .await
        .map_err(api_storage_error)?
    {
        super::auth_sessions::InteractionSessionCompletion::Issued(session) => {
            Ok(InteractionCompleted::Session(session))
        }
        super::auth_sessions::InteractionSessionCompletion::Rejected(error) => Err(error.into()),
    }
}

pub async fn revoke(
    context: &AppContext,
    access: StorageInteractionAccess,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(context)
        .revoke_auth_interaction_session(access, event_context)
        .await
        .map_err(|error| {
            if error.kind() == StorageErrorKind::Conflict {
                InteractionError::Pending.into()
            } else {
                storage_error(error)
            }
        })?
        .into_value();
    Ok(())
}
fn storage_error(error: StorageError) -> ApiError {
    match error.kind() {
        StorageErrorKind::AuthenticationRequired | StorageErrorKind::NotFound => {
            InteractionError::Invalid.into()
        }
        StorageErrorKind::Unavailable => InteractionError::ProviderUnavailable.into(),
        _ => error.into(),
    }
}
fn api_storage_error(error: ApiError) -> ApiError {
    match error {
        ApiError::Unauthorized(_) | ApiError::NotFound(_) => InteractionError::Invalid.into(),
        ApiError::ServiceUnavailable(_) => InteractionError::ProviderUnavailable.into(),
        other => other,
    }
}
