//! Deployment-owned browser origin and exact BFF registrations.
use std::str::FromStr;

use hubuum_domain::{InteractionClientId, InteractionReturnTarget};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::{ApiError, InteractionError};

#[derive(Clone, Debug, Default, Serialize, ToSchema)]
pub struct BrowserSettings {
    origin: Option<String>,
    allow_loopback_http: bool,
    return_targets: Vec<ReturnRegistration>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct ReturnRegistration {
    id: String,
    client_id: String,
    uri: String,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct SettingsInput {
    origin: Option<String>,
    allow_loopback_http: bool,
    return_targets: Vec<ReturnRegistration>,
}

impl FromStr for BrowserSettings {
    type Err = String;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.len() > 32_768 {
            return Err("Browser configuration exceeds 32 KiB".into());
        }
        let input: SettingsInput =
            serde_json::from_str(raw).map_err(|_| "Invalid browser configuration JSON")?;
        if input.return_targets.len() > 32 {
            return Err("At most 32 BFF return targets are allowed".into());
        }
        let Some(origin) = input.origin else {
            return if input.return_targets.is_empty() && !input.allow_loopback_http {
                Ok(Self::default())
            } else {
                Err("Browser origin is required".into())
            };
        };
        let probe =
            InteractionReturnTarget::try_new("origin".into(), origin.clone(), "a".repeat(32))
                .map_err(
                    |_| "Browser origin must use HTTPS, or explicitly enabled loopback HTTP",
                )?;
        let url = Url::parse(probe.uri()).map_err(|_| "Invalid browser origin")?;
        if url.path() != "/" || url.scheme() != "https" && !input.allow_loopback_http {
            return Err(
                "Browser origin must have no path and use HTTPS; HTTP requires allow_loopback_http"
                    .into(),
            );
        }
        let mut targets = Vec::new();
        for mut target in input.return_targets {
            InteractionClientId::new(target.client_id.clone()).map_err(|e| e.to_string())?;
            let checked =
                InteractionReturnTarget::try_new(target.id.clone(), target.uri, "a".repeat(32))
                    .map_err(|e| e.to_string())?;
            if checked.uri().starts_with("http:") && !input.allow_loopback_http {
                return Err("HTTP BFF return targets require allow_loopback_http".into());
            }
            if targets
                .iter()
                .any(|existing: &ReturnRegistration| existing.id == target.id)
            {
                return Err("Duplicate BFF return target ID".into());
            }
            target.uri = checked.uri().to_owned();
            targets.push(target);
        }
        Ok(Self {
            origin: Some(url.origin().ascii_serialization()),
            allow_loopback_http: input.allow_loopback_http,
            return_targets: targets,
        })
    }
}

impl<'de> Deserialize<'de> for BrowserSettings {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        value.to_string().parse().map_err(serde::de::Error::custom)
    }
}

impl BrowserSettings {
    pub fn enabled(&self) -> bool {
        self.origin.is_some()
    }
    pub fn origin(&self) -> Result<&str, ApiError> {
        self.origin
            .as_deref()
            .ok_or_else(|| InteractionError::Unsupported.into())
    }
    pub fn secure(&self) -> bool {
        !self
            .origin
            .as_ref()
            .is_some_and(|origin| origin.starts_with("http:"))
    }
    pub fn verification_uri(&self) -> Result<String, ApiError> {
        Ok(format!(
            "{}/api/v1/auth/interactions/browser",
            self.origin()?
        ))
    }
    pub fn return_target(
        &self,
        id: &str,
        client: &InteractionClientId,
        nonce: String,
    ) -> Result<InteractionReturnTarget, ApiError> {
        let registration = self
            .return_targets
            .iter()
            .find(|target| target.id == id && target.client_id == client.as_str())
            .ok_or_else(|| {
                ApiError::BadRequest("Unregistered return target for this client".into())
            })?;
        InteractionReturnTarget::try_new(registration.id.clone(), registration.uri.clone(), nonce)
            .map_err(|e| ApiError::BadRequest(e.to_string()))
    }
    pub fn handoff(
        &self,
        binding: &hubuum_domain::InteractionBinding,
    ) -> Result<Option<String>, ApiError> {
        let Some(target) = binding.return_target() else {
            return Ok(None);
        };
        let current =
            self.return_target(target.id(), binding.client(), target.nonce().to_owned())?;
        if &current != target {
            return Err(InteractionError::ConfigurationChanged.into());
        }
        let mut url = Url::parse(target.uri()).map_err(|_| InteractionError::Invalid)?;
        url.query_pairs_mut()
            .append_pair("interaction_id", &binding.id().value().to_string())
            .append_pair("nonce", target.nonce());
        Ok(Some(url.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case::remote_http("http://auth.example")]
    #[case::userinfo("https://user@auth.example")]
    #[case::path("https://auth.example/login")]
    #[case::query("https://auth.example/?token=x")]
    #[case::fragment("https://auth.example/#token")]
    fn invalid_origin_is_rejected(#[case] origin: &str) {
        assert!(
            json!({"origin": origin, "allow_loopback_http":true})
                .to_string()
                .parse::<BrowserSettings>()
                .is_err()
        );
    }

    #[test]
    fn loopback_http_requires_explicit_development_opt_in() {
        assert!(
            json!({"origin":"http://127.0.0.1:8080"})
                .to_string()
                .parse::<BrowserSettings>()
                .is_err()
        );
        let enabled: BrowserSettings =
            json!({"origin":"http://127.0.0.1:8080", "allow_loopback_http":true})
                .to_string()
                .parse()
                .unwrap();
        assert!(!enabled.secure());
    }

    #[test]
    fn return_registration_is_bound_to_exact_client() {
        let settings: BrowserSettings = json!({"origin":"https://auth.example", "return_targets":[{"id":"web", "client_id":"web-client", "uri":"https://web.example/callback"}]}).to_string().parse().unwrap();
        assert!(
            settings
                .return_target(
                    "web",
                    &InteractionClientId::new("another-client").unwrap(),
                    "a".repeat(32)
                )
                .is_err()
        );
    }

    #[rstest]
    #[case::query("https://web.example/callback?redirect=evil")]
    #[case::fragment("https://web.example/callback#secret")]
    #[case::javascript("javascript:alert(1)")]
    #[case::credentials("https://user:password@web.example/callback")]
    #[case::remote_http("http://web.example/callback")]
    fn invalid_return_target_is_rejected(#[case] uri: &str) {
        assert!(json!({"origin":"https://auth.example", "return_targets":[{"id":"web", "client_id":"web-client", "uri":uri}]}).to_string().parse::<BrowserSettings>().is_err());
    }
}
