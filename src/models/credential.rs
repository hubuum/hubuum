use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use hubuum_domain::CollectionId;
use hubuum_outbound_http::{AuthorizedDestination, CredentialOrigin};
use hubuum_secrets::SecretName;
use serde::Deserialize;

use crate::errors::ApiError;
use crate::models::RemoteAuthConfig;

#[derive(Clone)]
struct CredentialPolicy {
    collections: Vec<CollectionId>,
    origins: Vec<CredentialOrigin>,
}

/// Server-owned authority; a collection editor cannot grant themselves access
/// by changing a target's alias, URL template, or invocation parameters.
#[derive(Clone, Default)]
pub struct RemoteCredentialPolicies(BTreeMap<String, CredentialPolicy>);

impl fmt::Debug for RemoteCredentialPolicies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteCredentialPolicies")
            .field("bindings", &self.0.len())
            .finish()
    }
}

impl FromStr for RemoteCredentialPolicies {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value).map_err(|error| error.to_string())
    }
}

impl<'de> Deserialize<'de> for RemoteCredentialPolicies {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = serde_json::Value::deserialize(deserializer)?;
        Self::parse(&raw.to_string()).map_err(serde::de::Error::custom)
    }
}

impl RemoteCredentialPolicies {
    fn parse(value: &str) -> Result<Self, ApiError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawPolicy {
            collection_ids: Vec<CollectionId>,
            origins: Vec<String>,
        }
        let raw: BTreeMap<String, RawPolicy> = serde_json::from_str(value).map_err(|_| {
            ApiError::BadRequest(
                "remote credential policies must map aliases to collection_ids and HTTPS origins"
                    .into(),
            )
        })?;
        let mut policies = BTreeMap::new();
        for (alias, policy) in raw {
            SecretName::new(&alias).map_err(|_| {
                ApiError::BadRequest("invalid remote credential policy alias".into())
            })?;
            if policy.collection_ids.is_empty() || policy.origins.is_empty() {
                return Err(ApiError::BadRequest(
                    "remote credential policies require at least one collection and origin".into(),
                ));
            }
            let origins = policy
                .origins
                .iter()
                .map(|origin| {
                    CredentialOrigin::new(origin)
                        .map_err(|error| ApiError::BadRequest(error.to_string()))
                })
                .collect::<Result<_, _>>()?;
            policies.insert(
                alias,
                CredentialPolicy {
                    collections: policy.collection_ids,
                    origins,
                },
            );
        }
        Ok(Self(policies))
    }
}

impl RemoteCredentialPolicies {
    pub(crate) fn for_execution() -> Self {
        let configured = crate::config::get_config()
            .map(|config| config.remote_credential_policies.clone())
            .unwrap_or_default();
        #[cfg(any(test, feature = "integration-test-support"))]
        {
            let mut policies = configured;
            let bindings = TEST_BINDINGS
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            policies.0.extend(
                bindings
                    .iter()
                    .map(|(alias, policy)| (alias.clone(), policy.clone())),
            );
            policies
        }
        #[cfg(not(any(test, feature = "integration-test-support")))]
        configured
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn authorize(
        &self,
        alias: &str,
        collection: CollectionId,
        url: &str,
    ) -> Result<AuthorizedRemoteCredential, ApiError> {
        let policy = self
            .0
            .get(alias)
            .filter(|policy| policy.collections.contains(&collection))
            .ok_or_else(|| {
                ApiError::Forbidden("credential use is not permitted for this collection".into())
            })?;
        let destination = AuthorizedDestination::authorize(url, &policy.origins)
            .map_err(|error| ApiError::Forbidden(error.to_string()))?;
        Ok(AuthorizedRemoteCredential {
            alias: alias.to_string(),
            destination,
        })
    }
}

pub(crate) struct AuthorizedRemoteCredential {
    alias: String,
    destination: AuthorizedDestination,
}

impl AuthorizedRemoteCredential {
    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }
    pub(crate) fn destination(&self) -> &AuthorizedDestination {
        &self.destination
    }
}

/// Keeps the selected authentication and its authorized destination together.
pub(crate) struct AuthorizedRemoteAuth {
    config: RemoteAuthConfig,
    credential: Option<AuthorizedRemoteCredential>,
    url: String,
}

impl AuthorizedRemoteAuth {
    pub(crate) fn authorize(
        policies: &RemoteCredentialPolicies,
        config: &RemoteAuthConfig,
        collection: CollectionId,
        url: &str,
    ) -> Result<Self, ApiError> {
        let alias = match config {
            RemoteAuthConfig::None => None,
            RemoteAuthConfig::BearerSecret { secret }
            | RemoteAuthConfig::BasicSecret { secret, .. }
            | RemoteAuthConfig::ApiKeySecret { secret, .. } => Some(secret.as_str()),
        };
        let credential = alias
            .map(|alias| policies.authorize(alias, collection, url))
            .transpose()?;
        let url = match &credential {
            Some(credential) => credential.destination().url().to_string(),
            None => hubuum_outbound_http::validate_outbound_url(url)
                .map_err(|error| ApiError::BadRequest(error.to_string()))?
                .url()
                .to_string(),
        };
        Ok(Self {
            config: config.clone(),
            credential,
            url,
        })
    }
    pub(crate) fn config(&self) -> &RemoteAuthConfig {
        &self.config
    }
    pub(crate) fn credential(&self) -> Result<&AuthorizedRemoteCredential, ApiError> {
        self.credential
            .as_ref()
            .ok_or_else(|| ApiError::Forbidden("request has no authorized credential".into()))
    }
    pub(crate) fn url(&self) -> &str {
        &self.url
    }
}

/// Authorizes both collection access and the final webhook destination before
/// the sink credential is resolved. The prepared delivery owns its config.
pub(crate) struct AuthorizedWebhookDelivery {
    prepared: hubuum_event_sink_webhook::PreparedWebhookDelivery,
    secret_ref: Option<String>,
}

impl AuthorizedWebhookDelivery {
    pub(crate) fn authorize(
        config: &serde_json::Value,
        routing: &serde_json::Value,
        secret_ref: Option<&str>,
        collection: CollectionId,
    ) -> Result<Self, ApiError> {
        let uses_credentials = secret_ref.is_some()
            || config
                .get("headers")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|headers| !headers.is_empty());
        if uses_credentials {
            let collections = webhook_credential_collections(config)?;
            if !collections.contains(&collection) {
                return Err(ApiError::Forbidden(
                    "webhook credential use is not permitted for this collection".into(),
                ));
            }
        }
        let delivery = hubuum_event_sinks_common::SinkDelivery::new(config, routing, None);
        let prepared = hubuum_event_sink_webhook::PreparedWebhookDelivery::new(
            &delivery,
            secret_ref.is_some(),
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        Ok(Self {
            prepared,
            secret_ref: secret_ref.map(ToOwned::to_owned),
        })
    }

    pub(crate) async fn deliver(
        self,
        sink: &hubuum_event_sink_webhook::WebhookSink,
        envelope: &hubuum_events_core::EventEnvelope,
    ) -> Result<(), hubuum_event_sinks_common::SinkError> {
        let secret = match self.secret_ref {
            Some(alias) => Some(crate::secrets::resolve_event_sink_secret(&alias).await?),
            None => None,
        };
        sink.deliver_prepared(
            envelope,
            self.prepared,
            secret.as_ref().map(|secret| secret.value()),
        )
        .await
    }
}

pub(crate) fn validate_webhook_credential_policy(
    config: &serde_json::Value,
    has_secret: bool,
) -> Result<(), ApiError> {
    if !has_secret
        && config
            .get("headers")
            .and_then(serde_json::Value::as_object)
            .is_none_or(|headers| headers.is_empty())
    {
        return Ok(());
    }
    webhook_credential_collections(config)?;
    let origins: Vec<String> =
        serde_json::from_value(config.get("allowed_origins").cloned().unwrap_or_default())
            .map_err(|_| {
                ApiError::BadRequest(
                    "credential-bearing webhook sinks require allowed_origins".into(),
                )
            })?;
    if origins.is_empty() {
        return Err(ApiError::BadRequest(
            "webhook allowed_origins must not be empty".into(),
        ));
    }
    for origin in origins {
        CredentialOrigin::new(&origin).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    }
    Ok(())
}

fn webhook_credential_collections(
    config: &serde_json::Value,
) -> Result<Vec<CollectionId>, ApiError> {
    let collections: Vec<CollectionId> = serde_json::from_value(
        config
            .get("allowed_collection_ids")
            .cloned()
            .unwrap_or_default(),
    )
    .map_err(|_| {
        ApiError::BadRequest(
            "credential-bearing webhook sinks require positive allowed_collection_ids".into(),
        )
    })?;
    if collections.is_empty() {
        return Err(ApiError::BadRequest(
            "webhook allowed_collection_ids must not be empty".into(),
        ));
    }
    Ok(collections)
}

// Scoped fixtures use the same parser and destination proof as production.
// This registry is absent from production builds; aliases remain isolated.
#[cfg(any(test, feature = "integration-test-support"))]
static TEST_BINDINGS: std::sync::Mutex<BTreeMap<String, CredentialPolicy>> =
    std::sync::Mutex::new(BTreeMap::new());

#[cfg(any(test, feature = "integration-test-support"))]
pub struct TestCredentialBinding {
    alias: String,
}
#[cfg(any(test, feature = "integration-test-support"))]
impl Drop for TestCredentialBinding {
    fn drop(&mut self) {
        TEST_BINDINGS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.alias);
    }
}
#[cfg(any(test, feature = "integration-test-support"))]
pub fn bind_test_remote_credential(
    alias: &str,
    collection_id: i32,
    origin: &str,
) -> Result<TestCredentialBinding, ApiError> {
    let mut parsed = RemoteCredentialPolicies::parse(
        &serde_json::json!({alias: {"collection_ids": [collection_id], "origins": [origin]}})
            .to_string(),
    )?;
    let policy = parsed
        .0
        .remove(alias)
        .ok_or_else(|| ApiError::BadRequest("Missing test credential policy".into()))?;
    let mut bindings = TEST_BINDINGS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if bindings.contains_key(alias) {
        return Err(ApiError::Conflict(
            "Test credential alias is already bound".into(),
        ));
    }
    bindings.insert(alias.into(), policy);
    Ok(TestCredentialBinding {
        alias: alias.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    const HARDENING_GUIDE: &str = include_str!("../../docs/runtime_hardening.md");

    #[test]
    fn documented_remote_policy_authorizes_only_its_destination() {
        let example = crate::tests::docs_examples::required_labeled_block(
            HARDENING_GUIDE,
            "hardening/remote-credential-policy",
        )
        .unwrap();
        let policy: RemoteCredentialPolicies = example.body.parse().unwrap();
        assert!(
            policy
                .authorize(
                    "inventory_api",
                    CollectionId::new(42).unwrap(),
                    "https://inventory.example/jobs"
                )
                .is_ok()
        );
    }

    #[test]
    fn documented_webhook_policy_validates() {
        let example = crate::tests::docs_examples::required_labeled_block(
            HARDENING_GUIDE,
            "hardening/webhook-credential-policy",
        )
        .unwrap();
        let config = serde_json::from_str(&example.body).unwrap();
        validate_webhook_credential_policy(&config, true).unwrap();
    }

    #[rstest]
    #[case::allowed(1, "https://inventory.example/jobs", true)]
    #[case::other_collection(2, "https://inventory.example/jobs", false)]
    #[case::rendered_host_changed(1, "https://attacker.example/jobs", false)]
    #[case::different_port(1, "https://inventory.example:8443/jobs", false)]
    fn credential_authority_binds_collection_and_rendered_origin(
        #[case] collection: i32,
        #[case] url: &str,
        #[case] allowed: bool,
    ) {
        let policies: RemoteCredentialPolicies =
            r#"{"inventory":{"collection_ids":[1],"origins":["https://inventory.example"]}}"#
                .parse()
                .unwrap();
        assert_eq!(
            policies
                .authorize("inventory", CollectionId::new(collection).unwrap(), url)
                .is_ok(),
            allowed
        );
    }

    #[test]
    fn missing_binding_denies_existing_secret_aliases() {
        assert!(
            RemoteCredentialPolicies::default()
                .authorize(
                    "inventory",
                    CollectionId::new(1).unwrap(),
                    "https://inventory.example"
                )
                .is_err()
        );
    }
}
