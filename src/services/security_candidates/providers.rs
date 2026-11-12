use hubuum_domain::IdentityScopeName;
use hubuum_secrets::{SecretName, SecretValue};
use hubuum_storage_core::{
    StorageAuthenticationProviderKind as Kind, StorageBrowserProviderDefinition,
    StorageProviderIdentityBinding,
};

use super::*;
use crate::models::security_candidate::{
    BrowserProviderConfiguration, BrowserProviderKind, BrowserProviderRequest,
    ProviderCredentialInput,
};

pub(super) async fn prepare_browser(
    context: &AppContext,
    input: BrowserProviderRequest,
    current: Option<&StorageAuthenticationProviders>,
) -> Result<StorageAuthenticationProvider, ApiError> {
    let name = IdentityScopeName::new(input.name.clone()).map_err(invalid)?;
    let bound = context
        .backend()
        .authentication_provider_binding(name)
        .await?;
    let previous = current.and_then(|providers| {
        providers
            .providers()
            .iter()
            .find(|provider| provider.name() == input.name)
    });
    let existing_id = bound
        .as_ref()
        .map(StorageProviderIdentityBinding::id)
        .or_else(|| previous.and_then(StorageAuthenticationProvider::id));
    let id =
        match (existing_id, input.provider_id) {
            (Some(expected), Some(actual)) if expected == actual => actual,
            (None, None) => Uuid::new_v4(),
            _ => return Err(ApiError::BadRequest(
                "Existing provider IDs are immutable; omit provider_id only for a new definition"
                    .into(),
            )),
        };
    let kind = match input.kind {
        BrowserProviderKind::Oidc => Kind::Oidc,
        BrowserProviderKind::Oauth => Kind::Oauth,
    };
    if previous.is_some_and(|provider| provider.kind() != kind) {
        return Err(ApiError::Conflict(
            "Provider kind is immutable; use a new scope".into(),
        ));
    }
    if matches!(input.source, SecurityConfigurationSource::Deployment) || input.adopt_deployment {
        return Err(ApiError::BadRequest(
            "No deployment browser provider is registered; create a runtime definition".into(),
        ));
    }
    let definition = input
        .configuration
        .ok_or_else(|| ApiError::BadRequest("Runtime browser definition required".into()))?;
    let (mut configuration, credential) = match (input.kind, definition) {
        (
            BrowserProviderKind::Oidc,
            BrowserProviderConfiguration::Oidc {
                definition,
                credential,
            },
        ) => (serde_json::to_value(definition)?, credential),
        (
            BrowserProviderKind::Oauth,
            BrowserProviderConfiguration::Oauth {
                definition,
                credential,
            },
        ) => (serde_json::to_value(definition)?, credential),
        _ => {
            return Err(ApiError::BadRequest(
                "Definition does not match provider kind".into(),
            ));
        }
    };
    let fields = configuration
        .as_object_mut()
        .expect("typed provider object");
    let protected = match credential {
        ProviderCredentialInput::Keep => previous
            .and_then(StorageAuthenticationProvider::configuration)
            .and_then(|value| value.get("protected_credential"))
            .cloned(),
        ProviderCredentialInput::Clear => None,
        ProviderCredentialInput::Reference { alias } => {
            SecretName::new(alias.clone())
                .map_err(|_| ApiError::BadRequest("Invalid provider secret alias".into()))?;
            Some(json!({"alias": alias}))
        }
        ProviderCredentialInput::Replace { value } => {
            let plaintext = SecretValue::new(value.into_bytes())
                .map_err(|_| ApiError::BadRequest("Invalid provider credential".into()))?;
            let reference = crate::config::get_config()?
                .runtime_settings_encryption_key_ref
                .clone()
                .ok_or_else(|| {
                    ApiError::BadRequest("Runtime credential encryption key is required".into())
                })?;
            let key = crate::secrets::resolve_runtime_settings_key(&reference, None)
                .await
                .map_err(|_| {
                    ApiError::ServiceUnavailable("Runtime credential key unavailable".into())
                })?;
            let envelope = crate::runtime_settings_secrets::encrypt(
                reference,
                crate::runtime_settings_secrets::ALIAS_KEY_VERSION.into(),
                key.value(),
                &format!("authentication:{id}:client_secret"),
                &plaintext,
            )
            .map_err(|_| {
                ApiError::ServiceUnavailable("Provider credential encryption failed".into())
            })?;
            Some(json!({"envelope": envelope}))
        }
    };
    if input.enabled && protected.is_none() {
        return Err(ApiError::BadRequest(
            "Enabled browser provider requires a credential".into(),
        ));
    }
    if let Some(protected) = protected {
        fields.insert("protected_credential".into(), protected);
    }
    let provider = StorageAuthenticationProvider::try_new(input.name, kind, configuration)
        .map_err(invalid)?
        .with_id(id)
        .map_err(invalid)?
        .with_enabled(input.enabled);
    let namespace = StorageProviderIdentityBinding::from_provider(&provider).map_err(invalid)?;
    if bound.is_some_and(|bound| bound != namespace) {
        return Err(ApiError::Conflict(
            "Provider identity namespace is immutable; use a new scope".into(),
        ));
    }
    Ok(provider)
}

pub(super) async fn browser_secret(
    provider: &StorageAuthenticationProvider,
) -> Result<Option<SecretValue>, ApiError> {
    let Some(credential) = provider
        .configuration()
        .and_then(|config| config.get("protected_credential"))
    else {
        return Ok(None);
    };
    if let Some(alias) = credential.get("alias").and_then(Value::as_str) {
        let secret = crate::secrets::resolve_browser_provider_secret(provider.kind(), alias)
            .await
            .map_err(|_| ApiError::ServiceUnavailable("Provider secret unavailable".into()))?;
        return SecretValue::new(secret.value().expose().to_vec())
            .map(Some)
            .map_err(invalid);
    }
    let envelope: crate::runtime_settings_secrets::EncryptedSecret =
        serde_json::from_value(credential.get("envelope").cloned().ok_or_else(|| {
            ApiError::ServiceUnavailable("Invalid protected provider credential".into())
        })?)
        .map_err(|_| {
            ApiError::ServiceUnavailable("Invalid protected provider credential".into())
        })?;
    let key = crate::secrets::resolve_runtime_settings_key(envelope.key_ref(), None)
        .await
        .map_err(|_| ApiError::ServiceUnavailable("Provider credential key unavailable".into()))?;
    crate::runtime_settings_secrets::decrypt(
        &envelope,
        key.value(),
        &format!(
            "authentication:{}:client_secret",
            provider
                .id()
                .ok_or_else(|| ApiError::ServiceUnavailable("Provider ID missing".into()))?
        ),
    )
    .map(Some)
    .map_err(|_| ApiError::ServiceUnavailable("Provider credential unavailable".into()))
}

pub(super) fn browser_endpoints(
    provider: &StorageAuthenticationProvider,
) -> Result<Vec<String>, ApiError> {
    match provider.browser_definition() {
        Some(StorageBrowserProviderDefinition::Oidc(definition)) => {
            Ok(vec![definition.issuer().as_str().into()])
        }
        Some(StorageBrowserProviderDefinition::Oauth(definition)) => Ok(definition
            .endpoints()
            .iter()
            .map(|endpoint| endpoint.as_str().into())
            .collect()),
        None => Err(ApiError::BadRequest(
            "Validated browser provider required".into(),
        )),
    }
}

pub(super) async fn screen_endpoint(endpoint: &str) -> Result<(), ApiError> {
    let parts = hubuum_outbound_http::validate_outbound_url(endpoint)
        .map_err(|_| ApiError::BadRequest("Invalid authentication endpoint".into()))?;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        hubuum_outbound_http::screen_host(
            parts.host(),
            parts.port(),
            crate::config::get_config()?.auth_runtime_allow_private_targets,
            false,
        ),
    )
    .await
    .map_err(|_| {
        ApiError::ServiceUnavailable("Authentication endpoint resolution timed out".into())
    })?
    .map_err(|_| {
        ApiError::BadRequest(
            "Authentication destination is not permitted by deployment policy".into(),
        )
    })?;
    Ok(())
}
