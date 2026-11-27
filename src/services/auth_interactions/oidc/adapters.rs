//! Normalize protocol outputs so storage claiming, encryption and lifecycle
//! orchestration have one implementation for both browser protocols.
use hubuum_auth_core::protocol::ProtocolError;
use hubuum_auth_core::{
    ExternalAuthenticationContext, FreshExternalAuthentication, FreshExternalAuthenticationRequest,
    VerifiedExternalAuthentication,
};
use hubuum_auth_oauth::OAuthAdapter;
use hubuum_auth_oidc::OidcAdapter;
use hubuum_domain::InteractionId;
use hubuum_secrets::SecretValue;
use serde::{Deserialize, Serialize};

pub(crate) enum BrowserAdapter {
    Oidc(OidcAdapter),
    OAuth(OAuthAdapter),
}

impl BrowserAdapter {
    pub(crate) async fn check_configuration(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Oidc(adapter) => adapter.check_configuration().await,
            Self::OAuth(_) => Ok(()),
        }
    }
    pub(super) async fn begin(
        &self,
        id: InteractionId,
        fresh: bool,
    ) -> Result<(String, PendingLogin), ProtocolError> {
        match self {
            Self::Oidc(adapter) => {
                let (url, pending) = adapter.begin(id, fresh).await?.into_parts();
                Ok((url, PendingLogin::Oidc(pending)))
            }
            Self::OAuth(adapter) if !fresh => {
                let (url, pending) = adapter.begin(id)?;
                Ok((url, PendingLogin::OAuth(pending)))
            }
            Self::OAuth(_) => Err(ProtocolError::Configuration),
        }
    }
    pub(super) fn accept(
        &self,
        pending: PendingLogin,
        state: &str,
        issuer: Option<&str>,
        code: Option<&str>,
        error: Option<&str>,
    ) -> Result<AcceptedCallback, ProtocolError> {
        match (self, pending) {
            (Self::Oidc(_), PendingLogin::Oidc(pending)) => pending
                .accept(state, issuer, code, error)
                .map(AcceptedCallback::Oidc),
            (Self::OAuth(adapter), PendingLogin::OAuth(pending)) => adapter
                .accept(pending, state, issuer, code, error)
                .map(AcceptedCallback::OAuth),
            _ => Err(ProtocolError::Denied),
        }
    }
    pub(super) async fn exchange(
        &self,
        accepted: AcceptedCallback,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, ProtocolError> {
        match (self, accepted) {
            (Self::Oidc(adapter), AcceptedCallback::Oidc(accepted)) => adapter
                .exchange(accepted, context)
                .await
                .map(Authentication::Oidc),
            (Self::OAuth(adapter), AcceptedCallback::OAuth(accepted)) => adapter
                .exchange(accepted, context)
                .await
                .map(Authentication::OAuth),
            _ => Err(ProtocolError::Denied),
        }
    }
    pub(super) async fn refresh(
        &self,
        credentials: RetainedCredentials,
        context: &ExternalAuthenticationContext,
        submitting: impl Fn() -> bool + Clone + Sync,
    ) -> Result<Authentication, ProtocolError> {
        match (self, credentials) {
            (Self::Oidc(adapter), RetainedCredentials::Oidc(credentials)) => adapter
                .refresh(credentials, context, submitting)
                .await
                .map(Authentication::Oidc),
            (Self::OAuth(adapter), RetainedCredentials::OAuth(credentials)) => adapter
                .refresh_with_dispatch(credentials, context, submitting)
                .await
                .map(Authentication::OAuth),
            _ => Err(ProtocolError::Denied),
        }
    }
    pub(super) async fn revoke(
        &self,
        credentials: RetainedCredentials,
    ) -> Result<bool, ProtocolError> {
        match (self, credentials) {
            (Self::Oidc(adapter), RetainedCredentials::Oidc(credentials)) => {
                adapter.revoke(credentials).await
            }
            (Self::OAuth(adapter), RetainedCredentials::OAuth(credentials)) => {
                adapter.revoke(credentials).await
            }
            _ => Err(ProtocolError::Denied),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "protocol", content = "state")]
pub(super) enum PendingLogin {
    Oidc(hubuum_auth_oidc::PendingLogin),
    OAuth(hubuum_auth_oauth::PendingLogin),
}
impl PendingLogin {
    pub(super) fn state(&self) -> &str {
        match self {
            Self::Oidc(state) => state.state(),
            Self::OAuth(state) => state.state(),
        }
    }
    pub(super) fn protectable(&self) -> Result<SecretValue, ProtocolError> {
        match self {
            Self::Oidc(value) => value.protectable(),
            Self::OAuth(_) => protectable(self),
        }
    }
    pub(super) fn restore(secret: &SecretValue) -> Result<Self, ProtocolError> {
        serde_json::from_slice(secret.expose())
            .or_else(|_| hubuum_auth_oidc::PendingLogin::restore(secret).map(Self::Oidc))
            .map_err(|_| ProtocolError::Denied)
    }
}
pub(super) enum AcceptedCallback {
    Oidc(hubuum_auth_oidc::AcceptedCallback),
    OAuth(hubuum_auth_oauth::AcceptedCallback),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "protocol", content = "credential")]
pub(super) enum RetainedCredentials {
    Oidc(hubuum_auth_oidc::RetainedCredentials),
    OAuth(hubuum_auth_oauth::RetainedCredentials),
}
impl RetainedCredentials {
    pub(super) fn protectable(&self) -> Result<SecretValue, ProtocolError> {
        match self {
            Self::Oidc(value) => value.protectable(),
            Self::OAuth(_) => protectable(self),
        }
    }
    pub(super) fn restore(secret: &SecretValue) -> Result<Self, ProtocolError> {
        // SDK 0.11 OIDC retained refresh credentials predate protocol tagging.
        serde_json::from_slice(secret.expose())
            .or_else(|_| hubuum_auth_oidc::RetainedCredentials::restore(secret).map(Self::Oidc))
            .map_err(|_| ProtocolError::Denied)
    }
}
pub(super) enum Authentication {
    Oidc(hubuum_auth_oidc::Authentication),
    OAuth(hubuum_auth_oauth::Authentication),
}
impl Authentication {
    pub(super) fn into_parts(
        self,
    ) -> (VerifiedExternalAuthentication, Option<RetainedCredentials>) {
        match self {
            Self::Oidc(value) => {
                let (proof, credentials) = value.into_parts();
                (proof, credentials.map(RetainedCredentials::Oidc))
            }
            Self::OAuth(value) => {
                let (proof, credentials) = value.into_parts();
                (proof, Some(RetainedCredentials::OAuth(credentials)))
            }
        }
    }
    pub(super) fn fresh(
        self,
        request: FreshExternalAuthenticationRequest,
    ) -> Result<(FreshExternalAuthentication, Option<RetainedCredentials>), ProtocolError> {
        match self {
            Self::Oidc(value) => value
                .fresh_with_credentials(request)
                .map(|(proof, credentials)| (proof, credentials.map(RetainedCredentials::Oidc))),
            Self::OAuth(_) => Err(ProtocolError::Configuration),
        }
    }
}
fn protectable(value: &impl Serialize) -> Result<SecretValue, ProtocolError> {
    SecretValue::new(serde_json::to_vec(value).map_err(|_| ProtocolError::InvalidResponse)?)
        .map_err(|_| ProtocolError::InvalidResponse)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepted_oidc_pending_format_remains_readable() {
        let secret = SecretValue::new(
            serde_json::to_vec(&json!({
                "state":"original-state", "verifier":"original-verifier", "nonce":"nonce",
                "issuer":"https://identity.example/realm", "client_id":"hubuum",
                "callback":"https://hubuum.example/callback", "response_issuer":true
            }))
            .unwrap(),
        )
        .unwrap();
        let PendingLogin::Oidc(pending) = PendingLogin::restore(&secret).unwrap() else {
            panic!("Expected the accepted OIDC pending format");
        };
        pending
            .accept(
                "original-state",
                Some("https://identity.example/realm"),
                Some("code"),
                None,
            )
            .unwrap();
    }

    #[test]
    fn accepted_oidc_retained_format_remains_readable_after_tagging() {
        let secret = SecretValue::new(
            serde_json::to_vec(&json!({
                "refresh":"original-refresh", "nonce":"nonce", "subject":"stable",
                "issuer":"https://identity.example/realm", "client_id":"hubuum", "auth_time":null
            }))
            .unwrap(),
        )
        .unwrap();
        let credentials = RetainedCredentials::restore(&secret).unwrap();
        assert!(matches!(
            RetainedCredentials::restore(&credentials.protectable().unwrap()).unwrap(),
            RetainedCredentials::Oidc(_)
        ));
    }
}
