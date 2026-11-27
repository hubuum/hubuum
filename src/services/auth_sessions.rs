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
use hubuum_events_core::{EventContext, EventSubject};
use hubuum_storage_core::{
    AuthenticationStorage, CredentialIssuanceMethod, ObservedSource, SessionClientId,
    StorageCredentialCreation, StorageInteractionAccess, StorageInteractionComplete,
    StorageInteractionCompletion, StorageInteractionIssuance, StorageInteractionView,
    StorageSessionCreate, StorageSessionRotate, StorageSessionRotation,
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
    current: &StorageInteractionView,
    event_context: EventContext,
    source: ObservedSource,
) -> Result<InteractionSessionCompletion, ApiError> {
    let audit_subject = match current.authenticated_subject() {
        Some(subject) if matches!(subject.authentication_provider_kind(), "oidc" | "oauth") => {
            Some(selected_session_subject(context, subject).await?)
        }
        _ => None,
    };
    let access_token = generate_token();
    let refresh_token = RefreshToken::generate();
    let request = StorageInteractionComplete::new(
        access,
        StorageInteractionIssuance::Session {
            credentials: session_credentials(&access_token, &refresh_token)?,
            policy: crate::config::get_config()?.session_policy()?,
            context: event_context,
            audit_subject,
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

async fn selected_session_subject(
    context: &AppContext,
    subject: &EventSubject,
) -> Result<EventSubject, ApiError> {
    let is_admin = context.is_admin(&subject.principal_id()).await?;
    Ok(subject.clone().with_administrator_status(is_admin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
    use crate::tests::{
        TestContext, app_context, app_context_with_permission_backend, create_test_group,
    };
    use hubuum_domain::{PrincipalId, PrincipalKind};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[case::local_admin(true, None, true)]
    #[case::local_non_admin(false, None, false)]
    #[case::external_denies_local_admin(true, Some(false), false)]
    #[case::external_grants_non_local_admin(false, Some(true), true)]
    #[actix_web::test]
    async fn browser_session_audit_uses_selected_admin_policy(
        #[case] local_admin: bool,
        #[case] external_admin: Option<bool>,
        #[case] expected: bool,
    ) {
        let fixture = TestContext::new().await;
        let user = if local_admin {
            &fixture.admin_user
        } else {
            &fixture.normal_user
        };
        let group = create_test_group(&fixture.pool).await;
        group
            .add_member_without_events(&fixture.pool, user)
            .await
            .unwrap();
        let context = match external_admin {
            None => app_context(&fixture.pool),
            Some(allowed) => {
                let policy = MockTreetopBackend::new();
                if allowed {
                    policy.add_admin_rule(group.id);
                }
                actix_web::web::Data::new(app_context_with_permission_backend(
                    fixture.pool.get_ref().clone(),
                    Arc::new(policy),
                ))
            }
        };
        let subject = EventSubject::try_new(
            PrincipalId::new(user.id).unwrap(),
            "verified-name".into(),
            PrincipalKind::Human,
            "company".into(),
            "oidc".into(),
            false,
        )
        .unwrap();
        let selected = selected_session_subject(&context, &subject).await.unwrap();
        assert_eq!(selected, subject.with_administrator_status(expected));
        group.delete_without_events(&fixture.pool).await.unwrap();
    }
}
