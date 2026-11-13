use std::net::IpAddr;

use hubuum_domain::InteractionId;
use hubuum_storage_core::{
    AuthenticationStorage, StorageInteractionAction, StorageInteractionAuthorization,
    StorageInteractionView,
};

use crate::config::auth_browser::BrowserSettings;
use crate::errors::ApiError;
use crate::models::auth_interaction::{
    InteractionCredentialsRequest,
    browser::{BrowserSession, BrowserUserCode},
};
use crate::permissions::AppContext;

pub fn settings(_context: &AppContext) -> Result<BrowserSettings, ApiError> {
    Ok(crate::config::runtime::current()?.auth_browser.clone())
}

pub async fn attach(
    context: &AppContext,
    code: BrowserUserCode,
    session: &BrowserSession,
) -> Result<StorageInteractionView, ApiError> {
    super::view(
        context
            .backend()
            .bind_auth_interaction_browser(code.digest(), session.binding())
            .await
            .map_err(super::storage_error)?,
    )
}

pub async fn inspect(
    context: &AppContext,
    id: InteractionId,
    session: &BrowserSession,
) -> Result<StorageInteractionView, ApiError> {
    super::view(
        super::advance(
            context,
            StorageInteractionAuthorization::Browser {
                id,
                binding: session.binding(),
            },
            StorageInteractionAction::Inspect,
        )
        .await?,
    )
}

pub async fn credentials(
    context: &AppContext,
    id: InteractionId,
    session: &BrowserSession,
    name: String,
    password: String,
    ip: Option<IpAddr>,
) -> Result<(StorageInteractionView, BrowserSession), ApiError> {
    let current = inspect(context, id, session).await?;
    let approval = session.rotate();
    let input = InteractionCredentialsRequest {
        client_id: current.binding().client().clone(),
        name,
        password,
    };
    let view = super::authenticate(
        context,
        StorageInteractionAuthorization::Browser {
            id,
            binding: session.binding(),
        },
        input,
        ip,
        Some(approval.binding()),
    )
    .await?;
    Ok((view, approval))
}

pub async fn decide(
    context: &AppContext,
    id: InteractionId,
    session: &BrowserSession,
    approve: bool,
) -> Result<Option<String>, ApiError> {
    let current = inspect(context, id, session).await?;
    let destination = settings(context)?.handoff(current.binding())?;
    super::view(
        super::advance(
            context,
            StorageInteractionAuthorization::Browser {
                id,
                binding: session.binding(),
            },
            StorageInteractionAction::Decide { approve },
        )
        .await?,
    )?;
    Ok(destination)
}

pub(super) async fn verify_candidate(
    context: &AppContext,
    binding: &hubuum_domain::InteractionBinding,
    login: crate::models::user::ValidatedLogin,
    client_ip: Option<IpAddr>,
) -> Result<hubuum_storage_core::StorageInteractionAuthentication, ApiError> {
    use crate::errors::InteractionError;
    use crate::models::security_candidate::SecurityCandidateLdapVerification;
    use crate::services::{credential_approvals::CredentialActor, security_candidates};
    use hubuum_domain::VerifiedInteractionSubject;
    use hubuum_storage_core::{StorageCredentialFingerprint, StorageInteractionAuthentication};
    let actor = CredentialActor::from_interaction(
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
    let provider_id = binding
        .provider()
        .external_id()
        .ok_or(InteractionError::Invalid)?;
    let (candidate, provider) =
        security_candidates::interaction_candidate(context, &actor, id, provider_id).await?;
    if binding.authority().candidate_digest()
        != Some(&StorageInteractionAuthentication::candidate_digest(
            &candidate,
        ))
    {
        return Err(InteractionError::ConfigurationChanged.into());
    }
    let receipt = StorageCredentialFingerprint::new(
        binding
            .verification_receipt()
            .ok_or(InteractionError::Invalid)?
            .as_str()
            .into(),
    )
    .map_err(|_| InteractionError::Invalid)?;
    let request = SecurityCandidateLdapVerification {
        provider_id,
        username: login.name().to_owned(),
        password: login.password().as_str().to_owned(),
    };
    use crate::middlewares::rate_limit::{
        LoginAttemptOutcome, begin_login_attempt, finish_login_attempt,
    };
    let Some(permit) = begin_login_attempt(provider.name(), &request.username, client_ip).await?
    else {
        return Err(InteractionError::SlowDown {
            retry_after_seconds: 60,
        }
        .into());
    };
    let result =
        security_candidates::authenticate_ldap_candidate(context, candidate, request, receipt)
            .await;
    let outcome = match &result {
        Ok(_) => LoginAttemptOutcome::Succeeded,
        Err(ApiError::Unauthorized(_)) => LoginAttemptOutcome::Failed,
        Err(_) => LoginAttemptOutcome::Aborted,
    };
    finish_login_attempt(permit, outcome).await?;
    let (candidate, preview) = result.map_err(|error| match error {
        ApiError::Unauthorized(_) => InteractionError::Invalid.into(),
        ApiError::ServiceUnavailable(_) => InteractionError::ProviderUnavailable.into(),
        other => other,
    })?;
    let details = hubuum_domain::InteractionVerificationDetails::new(
        preview.admitted,
        preview
            .groups
            .into_iter()
            .map(|group| (group.key, group.name))
            .collect(),
    )
    .map_err(|_| InteractionError::Invalid)?;
    let proof = VerifiedInteractionSubject::new(binding.clone(), preview.subject)
        .and_then(|proof| proof.with_verification(Some(details)))
        .map_err(|_| InteractionError::Invalid)?;
    StorageInteractionAuthentication::verification(proof, candidate)
        .map_err(|_| InteractionError::Invalid.into())
}

/// Explicitly select display fields: never serialize credential-bearing operations wholesale.
pub(super) fn request_details(
    operation: Option<&crate::models::credential_approval::CredentialOperation>,
    candidate: Option<uuid::Uuid>,
) -> Result<String, ApiError> {
    use crate::models::credential_approval::CredentialOperation;
    use serde_json::json;
    let details = match operation {
        Some(CredentialOperation::CreateToken {
            principal_id,
            token,
        }) => json!({"action":"Create a token", "principal_id":principal_id, "token":token}),
        Some(CredentialOperation::RenewToken {
            principal_id,
            token_id,
            token,
        }) => {
            json!({"action":"Renew a token", "principal_id":principal_id, "token_id":token_id, "expires_at":token.expires_at})
        }
        Some(CredentialOperation::CreateUser { user }) => {
            json!({"action":"Create a user with a password", "name":user.name,"identity_scope":user.identity_scope,"proper_name":user.proper_name,"email":user.email})
        }
        Some(CredentialOperation::UpdateUser { user_id, user }) => {
            json!({"action":"Update a user", "user_id":user_id,"changes_password":user.password.is_some(),"proper_name":user.proper_name,"email":user.email})
        }
        Some(CredentialOperation::ActivateSecurityCandidate {
            candidate_id,
            group,
            expected_revision,
        }) => {
            json!({"action":"Activate security settings", "candidate_id":candidate_id,"group":group,"expected_revision":expected_revision})
        }
        Some(CredentialOperation::ImportCredentials { import }) => {
            json!({"action":"Import credentials", "mode":import.mode,"dry_run":import.dry_run,"principal_count":import.graph.principals.len()})
        }
        Some(CredentialOperation::ConfirmRestore {
            restore_id,
            confirmation,
        }) => {
            json!({"action":"Replace all Hubuum data from a backup", "restore_id":restore_id,"sha256":confirmation.sha256})
        }
        None => {
            return Ok(match candidate {
                Some(id) => format!(
                    "Verify candidate {id}. This does not activate settings or create a session."
                ),
                None => "Create a session for the requesting client.".into(),
            });
        }
    };
    serde_json::to_string_pretty(&details)
        .map_err(|_| ApiError::InternalServerError("Could not describe approval".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{NewUser, credential_approval::CredentialOperation};

    #[test]
    fn browser_prompt_omits_credential_material() {
        let operation = CredentialOperation::CreateUser {
            user: NewUser {
                identity_scope: None,
                name: "human".into(),
                password: "never-display-this".into(),
                proper_name: None,
                email: None,
            },
        };
        let prompt = request_details(Some(&operation), None).unwrap();
        assert!(prompt.contains("human"));
        assert!(!prompt.contains("never-display-this"));
    }
}
