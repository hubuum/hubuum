//! Prepare and verify security settings without granting live authentication authority.
pub(crate) mod providers;

use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use hubuum_auth_core::ExternalIdentityProvider;
use hubuum_auth_ldap::LdapIdentityProvider;
use hubuum_storage_core::{
    ExternalIdentityStorage, RuntimeSettingsStorage, StorageAuthenticationProvider,
    StorageAuthenticationProviderKind, StorageAuthenticationProviders, StorageAuthorizationBackend,
    StorageCredentialFingerprint, StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup,
    StorageRuntimeSettingsUpdate, StorageRuntimeSettingsValue, StorageSecurityCandidate,
    StorageSecurityCandidateActivation, StorageSecurityCandidateBinding,
    StorageSecurityCandidateState, StorageSecurityCheck, StorageSecurityCheckKind as CheckKind,
    StorageSecurityCheckStatus as CheckStatus,
};
use serde_json::{Value, json};
use sha2::Sha256;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::credential_approval::CredentialOperation;
use crate::models::runtime_settings::RuntimeAuthorizationBackend;
use crate::models::security_candidate::{
    SecurityCandidateActivated, SecurityCandidateActivationRequest,
    SecurityCandidateLdapVerification, SecurityCandidateLdapVerificationResponse,
    SecurityCandidateProviderRequest, SecurityCandidateReport, SecurityCandidateRequest,
    SecurityCandidateVerificationPreview, SecurityConfigurationSource, SecurityVerificationReceipt,
    hex,
};
use crate::permissions::AppContext;
use crate::services::credential_approvals::{
    ApprovedCredentialOperation, CredentialActor, approved_security_candidate,
};
use crate::services::runtime_settings::LocalSettingsAdministrator;

fn invalid(error: impl std::fmt::Display) -> ApiError {
    ApiError::BadRequest(error.to_string())
}

fn fingerprint(
    purpose: &[u8],
    value: &impl serde::Serialize,
) -> Result<StorageCredentialFingerprint, ApiError> {
    let key = crate::config::token_hash_key_ring()
        .map_err(|_| ApiError::ServiceUnavailable("Security binding key unavailable".into()))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key.active_key_bytes()).expect("HMAC key");
    mac.update(b"hubuum:security-candidate:v1:");
    mac.update(purpose);
    mac.update(&serde_json::to_vec(value)?);
    Ok(StorageCredentialFingerprint::new(hex(&mac.finalize().into_bytes())).expect("HMAC digest"))
}

fn check(
    kind: CheckKind,
    status: CheckStatus,
    provider: Option<Uuid>,
    reason: &str,
) -> StorageSecurityCheck {
    StorageSecurityCheck::try_new(kind, status, provider, reason).expect("static diagnostic")
}

async fn ldap_configuration(
    provider: &StorageAuthenticationProvider,
) -> Result<crate::auth::ConfiguredLdapScope, ApiError> {
    if provider.configuration().is_some() {
        return crate::auth::runtime_provider_configuration(provider).await;
    }
    crate::auth::load_auth_config()?
        .ldap
        .into_iter()
        .find(|config| config.ldap.scope == provider.name())
        .ok_or_else(|| ApiError::BadRequest("Deployment provider unavailable".into()))
}

async fn binding(
    assignment: &StorageRuntimeSettingsAssignment,
    release_ownership: bool,
) -> Result<StorageSecurityCandidateBinding, ApiError> {
    crate::secrets::refresh_security_candidate_secrets()
        .await
        .map_err(|_| ApiError::ServiceUnavailable("Security secret sources unavailable".into()))?;
    let mut secret_versions = Vec::new();
    if let Some(StorageRuntimeSettingsValue::Authentication(providers)) = assignment.value() {
        for provider in providers
            .providers()
            .iter()
            .filter(|provider| provider.enabled())
        {
            if provider.kind() != StorageAuthenticationProviderKind::Ldap {
                let secret = providers::browser_secret(provider).await?;
                secret_versions.push(fingerprint(b"browser-secret", &json!({"id": provider.id(), "secret": secret.as_ref().map(|value| value.expose())}))?.as_str().to_owned());
                continue;
            }
            let configuration = ldap_configuration(provider).await?;
            // Provider-local version labels are not stable across replicas. A keyed
            // fingerprint covers the effective value as well as its source alias.
            let secret = if let Some(alias) = &configuration.ldap.bind_password_ref {
                Some(
                    crate::secrets::resolve_ldap_secret(alias)
                        .await
                        .map_err(|_| {
                            ApiError::ServiceUnavailable("Authentication secret unavailable".into())
                        })?,
                )
            } else {
                None
            };
            secret_versions.push(
                fingerprint(
                    b"provider-secret",
                    &json!({
                        "name": provider.name(), "configuration": configuration,
                        "secret": secret.as_ref().map(|value| value.value().expose()),
                    }),
                )?
                .as_str()
                .to_owned(),
            );
        }
    }
    let config = crate::config::get_config()?;
    let trust = config
        .treetop_ca_cert
        .as_ref()
        .filter(|_| {
            matches!(
                assignment.value(),
                Some(StorageRuntimeSettingsValue::RemoteAuthorization(_))
                    | Some(StorageRuntimeSettingsValue::Authorization(
                        StorageAuthorizationBackend::Treetop
                    ))
            )
        })
        .map(|path| {
            crate::utilities::bounded_file::read_bounded_regular_file(
                std::path::Path::new(path),
                "Treetop trust bundle",
                crate::utilities::bounded_file::MAX_CERTIFICATE_BUNDLE_BYTES,
            )
            .map_err(|_| ApiError::ServiceUnavailable("Deployment trust bundle unavailable".into()))
        })
        .transpose()?;
    Ok(StorageSecurityCandidateBinding::new(
        fingerprint(
            b"definition",
            &json!({"selection": assignment.to_document(), "release_ownership": release_ownership}),
        )?,
        fingerprint(b"secrets", &secret_versions)?,
        fingerprint(
            b"deployment",
            &json!({
                "admin_group": config.admin_groupname,
                "admin_identity_scope": config.admin_identity_scope,
                "allow_private_targets": config.auth_runtime_allow_private_targets,
                "locked_providers": config.auth_runtime_locked_providers,
                "encryption_key_ref": config.runtime_settings_encryption_key_ref,
                "treetop_url": config.treetop_url,
                "treetop_ca": config.treetop_ca_cert,
                "treetop_trust": trust,
                "treetop_connect_timeout_ms": config.treetop_connect_timeout_ms,
                "treetop_request_timeout_ms": config.treetop_request_timeout_ms,
                "treetop_accept_invalid_certs": config.treetop_accept_invalid_certs,
            }),
        )?,
    ))
}

pub(crate) async fn create(
    context: &AppContext,
    actor: &CredentialActor,
    request: SecurityCandidateRequest,
    event: EventContext,
) -> Result<SecurityCandidateReport, ApiError> {
    let administrator = LocalSettingsAdministrator::require(context, &event).await?;
    let current = context.backend().load_runtime_settings().await?;
    let (expected, group) = request.revision_and_group();
    let actual = current.record(group.into()).revision();
    if expected != actual {
        return Err(ApiError::RevisionConflict(
            "Security settings changed".into(),
            actual,
        ));
    }
    let mut checks = vec![
        check(
            CheckKind::Configuration,
            CheckStatus::Passed,
            None,
            "configuration_valid",
        ),
        check(
            CheckKind::RecoveryAdministrator,
            CheckStatus::Passed,
            None,
            "local_administrator_retained",
        ),
    ];
    let (revision, assignment, release_ownership) = match request {
        SecurityCandidateRequest::Authentication {
            expected_revision,
            providers,
            release_ownership,
        } => {
            if release_ownership && !providers.is_empty() {
                return Err(ApiError::BadRequest(
                    "Releasing ownership forbids provider overrides".into(),
                ));
            }
            let deployment = crate::auth::runtime_deployment_config();
            let mut ldap = Vec::new();
            let mut protected = Vec::new();
            for provider in providers {
                match provider {
                    SecurityCandidateProviderRequest::Ldap(provider) => ldap.push(provider),
                    SecurityCandidateProviderRequest::Browser(provider) => protected.push(
                        providers::prepare_browser(context, *provider, current.authentication())
                            .await?,
                    ),
                }
            }
            protected.extend(
                crate::auth::protect_runtime_authentication_settings(
                    ldap,
                    current.authentication(),
                    &deployment,
                )
                .await?
                .providers()
                .iter()
                .cloned(),
            );
            if release_ownership {
                protected = crate::auth::load_auth_config()?
                    .ldap
                    .iter()
                    .map(|provider| {
                        StorageAuthenticationProvider::deployment(
                            provider.ldap.scope.clone(),
                            StorageAuthenticationProviderKind::Ldap,
                            true,
                        )
                        .map_err(invalid)
                    })
                    .collect::<Result<_, _>>()?;
            }
            let mut identified = Vec::new();
            for provider in &protected {
                let previous = current.authentication().and_then(|all| {
                    all.providers()
                        .iter()
                        .find(|old| old.name() == provider.name())
                });
                let scope = hubuum_domain::IdentityScopeName::new(provider.name().to_owned())
                    .map_err(invalid)?;
                let bound = context
                    .backend()
                    .authentication_provider_binding(scope)
                    .await?;
                if bound
                    .as_ref()
                    .is_some_and(|bound| bound.kind() != provider.kind())
                {
                    return Err(ApiError::Conflict(
                        "Provider kind is immutable; use a new scope".into(),
                    ));
                }
                let bound_id = bound
                    .as_ref()
                    .map(hubuum_storage_core::StorageProviderIdentityBinding::id)
                    .or_else(|| previous.and_then(StorageAuthenticationProvider::id));
                if provider.kind() == StorageAuthenticationProviderKind::Ldap
                    && provider.id().is_some()
                    && provider.id() != bound_id
                {
                    return Err(ApiError::BadRequest(
                        "Provider IDs are server-issued and immutable".into(),
                    ));
                }
                let id = provider
                    .id()
                    .or_else(|| {
                        bound
                            .as_ref()
                            .map(hubuum_storage_core::StorageProviderIdentityBinding::id)
                    })
                    .or_else(|| previous.and_then(StorageAuthenticationProvider::id))
                    .unwrap_or_else(Uuid::new_v4);
                let provider = provider.clone().with_id(id).map_err(invalid)?;
                let unchanged = previous.is_some_and(|old| {
                    !release_ownership
                        && old.enabled()
                        && old.kind() == provider.kind()
                        && old.configuration() == provider.configuration()
                });
                if provider.enabled() && !unchanged {
                    if provider.kind() != StorageAuthenticationProviderKind::Ldap {
                        for endpoint in providers::browser_endpoints(&provider)? {
                            providers::screen_endpoint(&endpoint).await?;
                        }
                        if provider.kind() == StorageAuthenticationProviderKind::Oidc {
                            crate::services::auth_interactions::oidc::adapter(context, &provider)
                                .await?
                                .check_configuration()
                                .await
                                .map_err(crate::services::auth_interactions::oidc::error)?;
                        }
                        checks.push(check(
                            CheckKind::Identity,
                            CheckStatus::NotRun,
                            Some(id),
                            if provider.kind() == StorageAuthenticationProviderKind::Oidc {
                                "test_sign_in_required"
                            } else {
                                "browser_verification_not_implemented"
                            },
                        ));
                        identified.push(provider);
                        continue;
                    }
                    let configured = ldap_configuration(&provider).await?;
                    if configured.ldap.connect_timeout_seconds > 30
                        || configured.ldap.operation_timeout_seconds > 30
                    {
                        return Err(ApiError::BadRequest("Runtime LDAP connection and operation timeouts must not exceed 30 seconds".into()));
                    }
                    let connection = LdapIdentityProvider::with_secret_source(
                        configured.ldap,
                        std::sync::Arc::new(CandidateLdapSecrets),
                    )
                    .map_err(|_| {
                        ApiError::BadRequest("Invalid LDAP candidate configuration".into())
                    })?
                    .with_transport(std::sync::Arc::new(crate::auth::ApplicationLdapTransport));
                    let connected = tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        connection.test_connection(),
                    )
                    .await
                    .is_ok_and(|result| result.is_ok());
                    checks.push(check(
                        CheckKind::Identity,
                        if connected {
                            CheckStatus::NotRun
                        } else {
                            CheckStatus::Failed
                        },
                        Some(id),
                        if connected {
                            "test_sign_in_required"
                        } else {
                            "connection_failed"
                        },
                    ));
                }
                identified.push(provider);
            }
            for locked in &crate::config::get_config()?.auth_runtime_locked_providers {
                let proposed = identified.iter().find(|provider| provider.name() == locked);
                let existing = current.authentication().and_then(|providers| {
                    providers
                        .providers()
                        .iter()
                        .find(|provider| provider.name() == locked)
                });
                let unchanged = match (existing, proposed) {
                    (Some(old), Some(new)) => {
                        old.kind() == new.kind()
                            && old.enabled() == new.enabled()
                            && old.configuration() == new.configuration()
                    }
                    (None, Some(new)) => {
                        new.enabled()
                            && new.configuration().is_none()
                            && deployment
                                .ldap
                                .iter()
                                .any(|provider| provider.ldap.scope == *locked)
                    }
                    (None, None) => !deployment
                        .ldap
                        .iter()
                        .any(|provider| provider.ldap.scope == *locked),
                    _ => false,
                };
                if !unchanged {
                    return Err(ApiError::Forbidden(
                        "Provider is locked by deployment policy".into(),
                    ));
                }
            }
            (
                expected_revision,
                StorageRuntimeSettingsAssignment::Managed(
                    StorageRuntimeSettingsValue::Authentication(
                        StorageAuthenticationProviders::try_new(identified).map_err(invalid)?,
                    ),
                ),
                release_ownership,
            )
        }
        SecurityCandidateRequest::Authorization {
            expected_revision,
            backend,
            source,
            connection,
            release_ownership,
        } => {
            if release_ownership
                && (connection.is_some()
                    || !matches!(source, SecurityConfigurationSource::Deployment))
            {
                return Err(ApiError::BadRequest("Releasing authorization ownership requires the deployment source without a connection override".into()));
            }
            let value = match (backend, source, connection) {
                (RuntimeAuthorizationBackend::Treetop, SecurityConfigurationSource::Runtime, Some(connection)) => StorageRuntimeSettingsValue::RemoteAuthorization(connection),
                (RuntimeAuthorizationBackend::Treetop, SecurityConfigurationSource::Deployment, None) => StorageRuntimeSettingsValue::Authorization(StorageAuthorizationBackend::Treetop),
                (RuntimeAuthorizationBackend::Local, _, None) => StorageRuntimeSettingsValue::Authorization(StorageAuthorizationBackend::Local),
                _ => return Err(ApiError::BadRequest("Only runtime Treetop requires a connection profile; local and deployment sources forbid one".into())),
            };
            if release_ownership {
                let deployment = crate::config::get_config()?.permission_backend;
                let requested_local = matches!(
                    value,
                    StorageRuntimeSettingsValue::Authorization(StorageAuthorizationBackend::Local)
                );
                if requested_local
                    != matches!(deployment, crate::config::PermissionBackendKind::Local)
                {
                    return Err(ApiError::BadRequest(
                        "Release must verify the complete deployment authorization selection"
                            .into(),
                    ));
                }
            }
            let assignment = StorageRuntimeSettingsAssignment::Managed(value);
            let candidate = crate::permissions::build_permission_backend_for_assignment(
                &crate::config::get_config()?.clone(),
                context.clone_backend(),
                &assignment,
            )
            .await?;
            administrator.check_backend(candidate.as_ref()).await?;
            (expected_revision, assignment, release_ownership)
        }
    };
    let actual = current.record(assignment.group()).revision();
    if actual != revision {
        return Err(ApiError::RevisionConflict(
            "Security settings changed".into(),
            actual,
        ));
    }
    let candidate_binding = binding(&assignment, release_ownership).await?;
    let update = StorageRuntimeSettingsUpdate::new(assignment, revision, event);
    let mut candidate = StorageSecurityCandidate::try_new(
        Uuid::new_v4(),
        actor.owner(),
        &update,
        candidate_binding,
    )
    .map_err(invalid)?
    .with_checks(checks)
    .map_err(invalid)?;
    if release_ownership {
        candidate = candidate.release_ownership().map_err(invalid)?;
    }
    let receipt = if candidate
        .checks()
        .iter()
        .all(|check| check.status() == CheckStatus::Passed)
    {
        let receipt = SecurityVerificationReceipt::generate();
        candidate = candidate.verify(receipt.digest()).map_err(invalid)?;
        Some(receipt)
    } else {
        None
    };
    context
        .backend()
        .create_security_candidate(candidate.clone())
        .await?;
    Ok(SecurityCandidateReport::new(&candidate, receipt))
}

struct CandidateLdapSecrets;
#[async_trait::async_trait]
impl hubuum_auth_ldap::LdapSecretSource for CandidateLdapSecrets {
    async fn resolve(
        &self,
        alias: &str,
    ) -> Result<hubuum_secrets::ResolvedSecret, hubuum_secrets::SecretError> {
        crate::secrets::resolve_ldap_secret(alias).await
    }
}

pub(crate) async fn load(
    context: &AppContext,
    actor: &CredentialActor,
    id: Uuid,
) -> Result<StorageSecurityCandidate, ApiError> {
    let event = EventContext::user(actor.owner().actor_id(), None, None);
    LocalSettingsAdministrator::require(context, &event).await?;
    Ok(context
        .backend()
        .get_security_candidate(id, actor.owner())
        .await?)
}

async fn require_current(
    context: &AppContext,
    candidate: &StorageSecurityCandidate,
) -> Result<(), ApiError> {
    if candidate.state(Utc::now()) == StorageSecurityCandidateState::Expired {
        return Err(ApiError::Gone("Security candidate expired".into()));
    }
    if candidate.state(Utc::now()) == StorageSecurityCandidateState::Activated {
        return Err(ApiError::Conflict(
            "Security candidate already activated".into(),
        ));
    }
    let current = context.backend().load_runtime_settings().await?;
    let revision = current.record(candidate.assignment().group()).revision();
    if revision != candidate.base_revision() {
        return Err(ApiError::RevisionConflict(
            "Security settings changed".into(),
            revision,
        ));
    }
    if &binding(
        candidate.verification_assignment(),
        candidate.releases_ownership(),
    )
    .await?
        != candidate.binding()
    {
        return Err(ApiError::Conflict(
            "Security candidate inputs changed; verify a new candidate".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn verify_ldap(
    context: &AppContext,
    actor: &CredentialActor,
    id: Uuid,
    request: SecurityCandidateLdapVerification,
) -> Result<SecurityCandidateLdapVerificationResponse, ApiError> {
    let candidate = load(context, actor, id).await?;
    let receipt = SecurityVerificationReceipt::generate();
    let (candidate, verification) =
        authenticate_ldap_candidate(context, candidate, request, receipt.digest()).await?;
    let receipt =
        (candidate.state(Utc::now()) == StorageSecurityCandidateState::Verified).then_some(receipt);
    context
        .backend()
        .verify_security_candidate(candidate.clone())
        .await?;
    Ok(SecurityCandidateLdapVerificationResponse {
        candidate: SecurityCandidateReport::new(&candidate, receipt),
        verification,
    })
}

pub(crate) async fn authenticate_ldap_candidate(
    context: &AppContext,
    mut candidate: StorageSecurityCandidate,
    request: SecurityCandidateLdapVerification,
    receipt: StorageCredentialFingerprint,
) -> Result<
    (
        StorageSecurityCandidate,
        SecurityCandidateVerificationPreview,
    ),
    ApiError,
> {
    let id = candidate.id();
    require_current(context, &candidate).await?;
    if candidate.state(Utc::now()) != StorageSecurityCandidateState::Unverified {
        return Err(ApiError::Conflict(
            "Only an unverified candidate can accept a test sign-in".into(),
        ));
    }
    let Some(StorageRuntimeSettingsValue::Authentication(providers)) =
        candidate.verification_assignment().value()
    else {
        return Err(ApiError::BadRequest(
            "Authentication candidate required".into(),
        ));
    };
    let provider = providers
        .providers()
        .iter()
        .find(|provider| provider.id() == Some(request.provider_id) && provider.enabled())
        .ok_or_else(|| ApiError::BadRequest("Enabled candidate provider required".into()))?;
    if provider.kind() != StorageAuthenticationProviderKind::Ldap {
        return Err(ApiError::BadRequest("LDAP provider required".into()));
    }
    let configured = ldap_configuration(provider).await?;
    let adapter = LdapIdentityProvider::with_secret_source(
        configured.ldap,
        std::sync::Arc::new(CandidateLdapSecrets),
    )
    .map_err(|_| ApiError::BadRequest("Invalid LDAP candidate".into()))?
    .with_transport(std::sync::Arc::new(crate::auth::ApplicationLdapTransport));
    // Adapter authentication returns normalized identity/groups only. In particular,
    // never call auth::login, identity synchronization or session issuance here.
    let identity = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        adapter.authenticate(&request.username, &request.password),
    )
    .await
    .map_err(|_| ApiError::ServiceUnavailable("LDAP verification timed out".into()))?
    .map_err(|error| match error {
        hubuum_auth_core::AuthProviderError::AuthenticationFailed => {
            ApiError::Unauthorized("LDAP verification failed".into())
        }
        _ => ApiError::ServiceUnavailable("LDAP verification unavailable".into()),
    })?;
    require_current(context, &candidate).await?;
    let verification =
        SecurityCandidateVerificationPreview::from_ldap(id, request.provider_id, identity)?;
    let checks = candidate
        .checks()
        .iter()
        .map(|existing| {
            if existing.provider_id() == Some(request.provider_id)
                && existing.kind() == CheckKind::Identity
            {
                check(
                    CheckKind::Identity,
                    CheckStatus::Passed,
                    Some(request.provider_id),
                    "test_sign_in_verified",
                )
            } else {
                existing.clone()
            }
        })
        .collect();
    candidate = candidate.with_checks(checks).map_err(invalid)?;

    if candidate
        .checks()
        .iter()
        .all(|check| check.status() == CheckStatus::Passed)
    {
        candidate = candidate.verify(receipt).map_err(invalid)?;
    }
    Ok((candidate, verification))
}

pub(crate) async fn verify_oidc_interaction(
    context: &AppContext,
    authentication: &hubuum_auth_core::VerifiedExternalAuthentication,
) -> Result<hubuum_storage_core::StorageInteractionAuthentication, ApiError> {
    use crate::errors::InteractionError;
    use hubuum_domain::{
        InteractionPurpose, InteractionVerificationDetails, VerifiedInteractionSubject,
    };
    use hubuum_storage_core::StorageInteractionAuthentication;
    let binding = authentication.binding();
    if binding.authority().purpose() != InteractionPurpose::ProviderVerification {
        return Err(InteractionError::Invalid.into());
    }
    let actor = CredentialActor::from_interaction(
        context,
        binding
            .authority()
            .actor()
            .ok_or(InteractionError::Invalid)?,
    )
    .await?;
    let provider_id = binding
        .provider()
        .external_id()
        .ok_or(InteractionError::Invalid)?;
    let (candidate, _) = interaction_candidate(
        context,
        &actor,
        binding
            .authority()
            .candidate_id()
            .ok_or(InteractionError::Invalid)?,
        provider_id,
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
    let existing = context
        .backend()
        .find_external_identity(authentication.identity().clone())
        .await?;
    let admitted = authentication
        .preview_admission(existing.map(|_| authentication.identity()))
        .map_err(|_| InteractionError::Invalid)?;
    let details = InteractionVerificationDetails::new(
        admitted,
        authentication
            .memberships()
            .provider_groups()
            .unwrap_or_default()
            .iter()
            .map(|group| (group.key().to_owned(), group.key().to_owned()))
            .collect(),
    )
    .map_err(|_| InteractionError::Invalid)?;
    let proof = VerifiedInteractionSubject::new(
        binding.clone(),
        authentication.identity().subject().as_str(),
    )
    .and_then(|proof| proof.with_verification(Some(details)))
    .map_err(|_| InteractionError::Invalid)?;
    let checks = candidate
        .checks()
        .iter()
        .map(|existing| {
            if existing.provider_id() == Some(provider_id) && existing.kind() == CheckKind::Identity
            {
                check(
                    CheckKind::Identity,
                    CheckStatus::Passed,
                    Some(provider_id),
                    "test_sign_in_verified",
                )
            } else {
                existing.clone()
            }
        })
        .collect();
    let receipt = StorageCredentialFingerprint::new(
        binding
            .verification_receipt()
            .ok_or(InteractionError::Invalid)?
            .as_str()
            .to_owned(),
    )
    .map_err(invalid)?;
    let candidate = candidate
        .with_checks(checks)
        .map_err(invalid)?
        .verify(receipt)
        .map_err(invalid)?;
    // No identity synchronization, membership writes or session issuance occurs.
    StorageInteractionAuthentication::verification(proof, candidate).map_err(invalid)
}

pub(crate) async fn interaction_candidate(
    context: &AppContext,
    actor: &CredentialActor,
    id: Uuid,
    provider_id: Uuid,
) -> Result<
    (
        StorageSecurityCandidate,
        hubuum_storage_core::StorageProviderIdentityBinding,
    ),
    ApiError,
> {
    let candidate = load(context, actor, id).await?;
    require_current(context, &candidate).await?;
    if candidate.state(Utc::now()) != StorageSecurityCandidateState::Unverified {
        return Err(ApiError::Conflict("Unverified candidate required".into()));
    }
    let Some(StorageRuntimeSettingsValue::Authentication(providers)) =
        candidate.verification_assignment().value()
    else {
        return Err(ApiError::BadRequest(
            "Authentication candidate required".into(),
        ));
    };
    let provider = providers
        .providers()
        .iter()
        .find(|provider| provider.id() == Some(provider_id) && provider.enabled())
        .ok_or(crate::errors::InteractionError::Unsupported)?;
    if !matches!(
        provider.kind(),
        StorageAuthenticationProviderKind::Ldap | StorageAuthenticationProviderKind::Oidc
    ) || candidate.checks().iter().any(|check| {
        check.status() != CheckStatus::Passed
            && !(check.kind() == CheckKind::Identity && check.provider_id() == Some(provider_id))
    }) {
        return Err(crate::errors::InteractionError::Unsupported.into());
    }
    let binding = hubuum_storage_core::StorageProviderIdentityBinding::from_provider(provider)
        .map_err(invalid)?;
    Ok((candidate, binding))
}

pub(crate) async fn approval_binding(
    context: &AppContext,
    actor: &CredentialActor,
    operation: &CredentialOperation,
) -> Result<Option<Value>, ApiError> {
    let CredentialOperation::ActivateSecurityCandidate {
        candidate_id,
        group,
        expected_revision,
    } = operation
    else {
        return Ok(None);
    };
    let candidate = load(context, actor, *candidate_id).await?;
    require_current(context, &candidate).await?;
    if candidate.assignment().group() != StorageRuntimeSettingsGroup::from(*group)
        || candidate.base_revision() != *expected_revision
        || candidate.state(Utc::now()) != StorageSecurityCandidateState::Verified
    {
        return Err(ApiError::Conflict(
            "Verified candidate and exact revision required".into(),
        ));
    }
    Ok(Some(
        json!({"definition": candidate.binding().definition().as_str(), "secrets": candidate.binding().secrets().as_str(), "deployment": candidate.binding().deployment().as_str()}),
    ))
}

pub(crate) async fn activate(
    context: &AppContext,
    actor: &CredentialActor,
    id: Uuid,
    request: SecurityCandidateActivationRequest,
    approved: ApprovedCredentialOperation,
    event: EventContext,
) -> Result<SecurityCandidateActivated, ApiError> {
    let candidate = load(context, actor, id).await?;
    require_current(context, &candidate).await?;
    if candidate.assignment().group() == StorageRuntimeSettingsGroup::Authorization {
        let administrator = LocalSettingsAdministrator::require(context, &event).await?;
        let backend = crate::permissions::build_permission_backend_for_assignment(
            &crate::config::get_config()?.clone(),
            context.clone_backend(),
            candidate.assignment(),
        )
        .await?;
        administrator.check_backend(backend.as_ref()).await?;
    }
    let update = StorageRuntimeSettingsUpdate::new(
        candidate.assignment().clone(),
        request.expected_revision,
        event,
    );
    let activation = StorageSecurityCandidateActivation::try_new(
        &candidate,
        request.verification_receipt.digest(),
        update,
        approved_security_candidate(approved)?,
        crate::config::get_config()?.admin_groupname.clone(),
    )
    .map_err(invalid)?;
    // Capture the previous trusted definitions before the commit destroys their
    // credentials. Revocation may use only those definitions after activation.
    let mut upstream = Vec::new();
    if candidate.assignment().group() == StorageRuntimeSettingsGroup::Authentication {
        let previous = context.backend().load_runtime_settings().await?;
        if let Some(providers) = previous.authentication() {
            for provider in providers.providers().iter().filter(|provider| {
                provider.enabled() && provider.kind() == StorageAuthenticationProviderKind::Oidc
            }) {
                let binding =
                    hubuum_storage_core::StorageProviderIdentityBinding::from_provider(provider)
                        .map_err(invalid)?;
                let records = context
                    .backend()
                    .upstream_logout_credentials(
                        hubuum_storage_core::StorageUpstreamRevocation::Provider(binding),
                    )
                    .await
                    .unwrap_or_default();
                upstream.push((provider.clone(), records));
            }
        }
    }
    let outcome = context
        .backend()
        .activate_security_candidate(activation)
        .await?;
    if outcome.is_committed() {
        for (provider, records) in upstream {
            crate::services::auth_interactions::oidc::revoke_provider_best_effort(
                records, provider,
            );
        }
    }
    if let Err(error) = context.refresh_runtime_settings().await {
        tracing::warn!(%error, "Security settings committed; replica reconciliation remains pending");
    }
    Ok(SecurityCandidateActivated {
        group: outcome.record().group().into(),
        revision: outcome.record().revision(),
        changed: outcome.is_committed(),
    })
}
