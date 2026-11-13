use crate::{
    auth::AuthenticatedLogin,
    errors::ApiError,
    models::auth_session::{
        RefreshToken, SessionResponse, authentication_failed, session_credentials,
    },
    permissions::AppContext,
    storage::storage_handle,
    utilities::auth::generate_token,
};
use hubuum_domain::InteractionTransitionError;
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    AuthenticationStorage, CredentialIssuanceMethod, ObservedSource, SessionClientId,
    StorageCredentialCreation, StorageInteractionAccess, StorageInteractionComplete,
    StorageInteractionCompletion, StorageInteractionIssuance, StorageSessionCreate,
    StorageSessionRotate, StorageSessionRotation,
};

pub async fn issue(
    context: &AppContext,
    authenticated: &AuthenticatedLogin,
    client: SessionClientId,
    event_context: EventContext,
    source: ObservedSource,
) -> Result<SessionResponse, ApiError> {
    let login = super::authentication::login_evidence(context, authenticated).await?;
    let access = generate_token();
    let refresh = RefreshToken::generate();
    let request = StorageSessionCreate::new(
        login,
        client,
        session_credentials(&access, &refresh)?,
        crate::config::get_config()?.session_policy()?,
        event_context,
    );
    let result = storage_handle(context)
        .create_auth_session(request)
        .await?
        .into_value();
    super::credential_activity::creation(
        context,
        result.access_id(),
        StorageCredentialCreation::new(
            Some(result.principal_id()),
            CredentialIssuanceMethod::Login,
            source,
            result.issued_at(),
        ),
    )
    .await;
    crate::observability::metrics::token_authentication("session", "active", "issued");
    Ok(SessionResponse::new(access, refresh, result))
}

pub async fn refresh(
    context: &AppContext,
    presented: RefreshToken,
    client: SessionClientId,
    provenance: Option<crate::events::RequestProvenance>,
    source: ObservedSource,
) -> Result<SessionResponse, ApiError> {
    let credential = presented.credential()?;
    let principal = storage_handle(context)
        .refresh_principal(credential.clone())
        .await
        .map_err(|_| authentication_failed())?;
    crate::auth::refresh_principal_if_needed(context, principal.id())
        .await
        .map_err(|error| match error {
            ApiError::Unauthorized(_) | ApiError::Forbidden(_) | ApiError::NotFound(_) => {
                authentication_failed()
            }
            other => other,
        })?;
    let event_context = provenance
        .map(|p| p.user_event_context(principal.id()))
        .unwrap_or_else(|| EventContext::user(principal, None, None));
    let access = generate_token();
    let refresh = RefreshToken::generate();
    let request = StorageSessionRotate::new(
        credential,
        client,
        session_credentials(&access, &refresh)?,
        event_context,
    );
    match storage_handle(context)
        .rotate_auth_session(request)
        .await?
        .into_value()
    {
        StorageSessionRotation::Issued(metadata) => {
            super::credential_activity::observe(context, metadata.access_id(), source).await;
            crate::observability::metrics::token_authentication("session", "active", "refreshed");
            Ok(SessionResponse::new(access, refresh, metadata))
        }
        outcome => {
            crate::observability::metrics::token_authentication(
                "session",
                "active",
                if matches!(outcome, StorageSessionRotation::Replayed) {
                    "replayed"
                } else {
                    "rejected"
                },
            );
            Err(authentication_failed())
        }
    }
}

/// Credentials are generated here and returned only after storage atomically
/// issues the family and consumes the authenticated interaction.
#[derive(Debug)]
pub enum InteractionSessionCompletion {
    Issued(SessionResponse),
    Rejected(InteractionTransitionError),
}

pub async fn complete_interaction(
    context: &AppContext,
    access: StorageInteractionAccess,
    event_context: EventContext,
    source: ObservedSource,
) -> Result<InteractionSessionCompletion, ApiError> {
    let access_token = generate_token();
    let refresh_token = RefreshToken::generate();
    let request = StorageInteractionComplete::new(
        access,
        StorageInteractionIssuance::Session {
            credentials: session_credentials(&access_token, &refresh_token)?,
            policy: crate::config::get_config()?.session_policy()?,
            context: event_context,
        },
    );
    match storage_handle(context)
        .complete_auth_interaction(request)
        .await?
    {
        StorageInteractionCompletion::Session(result) => {
            let result = result.into_value();
            super::credential_activity::creation(
                context,
                result.access_id(),
                StorageCredentialCreation::new(
                    Some(result.principal_id()),
                    CredentialIssuanceMethod::Login,
                    source,
                    result.issued_at(),
                ),
            )
            .await;
            crate::observability::metrics::token_authentication("session", "active", "issued");
            Ok(InteractionSessionCompletion::Issued(SessionResponse::new(
                access_token,
                refresh_token,
                result,
            )))
        }
        StorageInteractionCompletion::Rejected(error) => {
            Ok(InteractionSessionCompletion::Rejected(error))
        }
        _ => Err(ApiError::InternalServerError(
            "Unexpected authentication completion result".into(),
        )),
    }
}
