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
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    AuthenticationStorage, SessionClientId, StorageSessionCreate, StorageSessionRotate,
    StorageSessionRotation,
};

pub async fn issue(
    context: &AppContext,
    authenticated: &AuthenticatedLogin,
    client: SessionClientId,
    event_context: EventContext,
    source: hubuum_storage_core::ObservedSource,
) -> Result<SessionResponse, ApiError> {
    let issuer = authenticated.user().id;
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
        hubuum_storage_core::StorageCredentialCreation::new(
            Some(hubuum_domain::PrincipalId::new(issuer)?),
            hubuum_storage_core::CredentialIssuanceMethod::Login,
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
    source: hubuum_storage_core::ObservedSource,
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
