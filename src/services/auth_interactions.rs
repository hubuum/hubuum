//! Resolve current provider authority and orchestrate secret-protected interactions.
use std::net::IpAddr;

use chrono::{Duration, Utc};
use hubuum_domain::{
    DEFAULT_INTERACTION_LIFETIME_SECONDS, IdentityScopeName, InteractionAuthority,
    InteractionBinding, InteractionId, InteractionLifecycle, InteractionMode,
    InteractionProviderId, ResourceRevision, VerifiedInteractionSubject,
};
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    AuthenticationStorage, ObservedSource, RuntimeSettingsStorage, StorageError, StorageErrorKind,
    StorageInteractionAccess, StorageInteractionAction, StorageInteractionAuthentication,
    StorageInteractionAuthorization, StorageInteractionCommand, StorageInteractionCreate,
    StorageInteractionOutcome, StorageInteractionView, StorageProviderIdentityBinding,
    StorageRuntimeSettingsGroup,
};
use uuid::Uuid;

use crate::errors::{ApiError, InteractionError};
use crate::middlewares::rate_limit::{
    LoginAttemptOutcome, begin_login_attempt, finish_login_attempt,
};
use crate::models::auth_interaction::{
    AuthenticationMethod, AuthenticationMethods, InteractionBeginRequest, InteractionBegun,
    InteractionCredentialsRequest, InteractionNext, InteractionSecret, InteractionStatus,
    PasswordInteractionMode, PasswordProviderKind,
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
            })
            .collect(),
    })
}

pub async fn begin(
    context: &AppContext,
    input: InteractionBeginRequest,
) -> Result<InteractionBegun, ApiError> {
    input.validate()?;
    let (revision, providers) = providers(context).await?;
    let provider = providers
        .into_iter()
        .find(|provider| provider.id == input.provider_id)
        .ok_or(InteractionError::Unsupported)?;
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).expect("random UUID"),
        provider.id,
        input.client_id,
        revision,
        InteractionAuthority::login(),
    )
    .map_err(|_| InteractionError::Invalid)?;
    let now = Utc::now();
    let lifecycle = InteractionLifecycle::try_new(
        binding,
        InteractionMode::Credentials,
        now,
        now + Duration::seconds(DEFAULT_INTERACTION_LIFETIME_SECONDS),
    )?;
    let secret = InteractionSecret::generate();
    let mut create = StorageInteractionCreate::builder(lifecycle, secret.digest());
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
        next: InteractionNext::Credentials,
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
        StorageInteractionAuthorization::Redemption(access),
        StorageInteractionAction::Claim(Uuid::new_v4()),
    )
    .await?;
    let StorageInteractionOutcome::Claim(claim) = outcome else {
        return Err(ApiError::InternalServerError(
            "Expected authentication claim".into(),
        ));
    };
    let result = async {
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
                approval: None,
            },
        )
        .await?,
    )
    .map(Into::into)
}

pub async fn complete(
    context: &AppContext,
    access: StorageInteractionAccess,
    event_context: EventContext,
    source: ObservedSource,
) -> Result<crate::models::auth_session::SessionResponse, ApiError> {
    match super::auth_sessions::complete_interaction(context, access, event_context, source)
        .await
        .map_err(api_storage_error)?
    {
        super::auth_sessions::InteractionSessionCompletion::Issued(session) => Ok(session),
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
