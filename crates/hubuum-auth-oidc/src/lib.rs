//! OIDC protocol adapter. Protocol types and upstream credentials stay private.
use std::{collections::BTreeMap, fmt, time::Duration};

use chrono::{DateTime, Utc};
use hubuum_auth_core::{
    ExternalAuthenticationContext, ExternalIdentityProfile, ExternalSubject,
    FreshExternalAuthentication, FreshExternalAuthenticationRequest, MembershipClaims,
    VerifiedExternalAuthentication,
};
use hubuum_domain::{
    MembershipClaimSource, OidcProviderDefinition, OidcSigningAlgorithm, ProviderMembership,
};
use hubuum_secrets::SecretValue;
use openidconnect::{
    AccessTokenHash, AdditionalClaims, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EndpointMaybeSet, EndpointNotSet, EndpointSet, IdToken, Nonce, NonceVerifier,
    OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, RefreshToken, Scope,
    TokenResponse,
    core::{
        CoreAuthenticationFlow, CoreClient, CoreGenderClaim, CoreJsonWebKeySet,
        CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm, CoreProviderMetadata,
        CoreTokenResponse,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

mod transport;
use transport::Transport;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OidcError {
    #[error("OIDC provider configuration is unsupported")]
    Configuration,
    #[error("OIDC provider is temporarily unavailable")]
    Unavailable,
    #[error("OIDC response failed validation")]
    InvalidResponse,
    #[error("OIDC authentication was denied or revoked")]
    Denied,
}

type Client = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;
type ClaimsToken = IdToken<
    Additional,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;

#[derive(Clone, Serialize, Deserialize)]
struct Additional(BTreeMap<String, Value>);
impl AdditionalClaims for Additional {}
impl fmt::Debug for Additional {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Additional(<redacted>)")
    }
}

/// Selected by trusted server configuration, never from callback parameters.
pub struct OidcAdapter {
    definition: OidcProviderDefinition,
    secret: SecretValue,
    callback: RedirectUrl,
    transport: Transport,
}

struct Discovered {
    client: Client,
    userinfo: Option<String>,
    revocation: Option<String>,
    response_issuer: bool,
}

impl OidcAdapter {
    pub fn new(
        definition: OidcProviderDefinition,
        secret: SecretValue,
        callback: &str,
        allow_private_targets: bool,
    ) -> Result<Self, OidcError> {
        endpoint(callback)?;
        if secret.len() > 16 * 1024 || secret.expose_utf8().is_err() {
            return Err(OidcError::Configuration);
        }
        Ok(Self {
            definition,
            secret,
            callback: RedirectUrl::new(callback.to_owned())
                .map_err(|_| OidcError::Configuration)?,
            transport: Transport {
                allow_private_targets,
            },
        })
    }

    async fn discover(&self) -> Result<Discovered, OidcError> {
        tokio::time::timeout(Duration::from_secs(15), self.discover_inner())
            .await
            .map_err(|_| OidcError::Unavailable)?
    }

    async fn discover_inner(&self) -> Result<Discovered, OidcError> {
        // Deliberately no protocol cache: each operation observes current keys
        // and endpoints. This also bounds memory independently of provider count.
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.definition.issuer().as_str().trim_end_matches('/')
        );
        let document = self.transport.json(&url).await?;
        validate_metadata(&document, &self.definition)?;
        for name in [
            "authorization_endpoint",
            "token_endpoint",
            "jwks_uri",
            "userinfo_endpoint",
            "revocation_endpoint",
            "end_session_endpoint",
        ] {
            if let Some(endpoint) = document.get(name).and_then(Value::as_str) {
                let parts = hubuum_outbound_http::validate_outbound_url(endpoint)
                    .map_err(|_| OidcError::Configuration)?;
                hubuum_outbound_http::screen_host(
                    parts.host(),
                    parts.port(),
                    self.transport.allow_private_targets,
                    false,
                )
                .await
                .map_err(|_| OidcError::Configuration)?;
            }
        }
        let metadata: CoreProviderMetadata =
            serde_json::from_value(document.clone()).map_err(|_| OidcError::InvalidResponse)?;
        let keys = self.transport.json(metadata.jwks_uri().as_str()).await?;
        let key_count = keys
            .get("keys")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        if key_count == 0 || key_count > 32 {
            return Err(OidcError::InvalidResponse);
        }
        let jwks: CoreJsonWebKeySet =
            serde_json::from_value(keys).map_err(|_| OidcError::InvalidResponse)?;
        let optional = |name: &str| {
            document
                .get(name)
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        };
        Ok(Discovered {
            client: CoreClient::from_provider_metadata(
                metadata.set_jwks(jwks),
                ClientId::new(self.definition.client_id().to_owned()),
                Some(ClientSecret::new(
                    self.secret
                        .expose_utf8()
                        .map_err(|_| OidcError::Configuration)?
                        .to_owned(),
                )),
            )
            .set_redirect_uri(self.callback.clone()),
            userinfo: optional("userinfo_endpoint"),
            revocation: optional("revocation_endpoint"),
            response_issuer: document.get("authorization_response_iss_parameter_supported")
                == Some(&Value::Bool(true)),
        })
    }

    /// Discovery and all discovered destinations are validated before redirecting.
    pub async fn check_configuration(&self) -> Result<(), OidcError> {
        self.discover().await.map(|_| ())
    }

    pub async fn begin(
        &self,
        interaction: hubuum_domain::InteractionId,
        fresh: bool,
    ) -> Result<Authorization, OidcError> {
        let discovered = self.discover().await?;
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let mut request = discovered
            .client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                move || {
                    CsrfToken::new(format!(
                        "{}.{}",
                        interaction.value(),
                        CsrfToken::new_random().secret()
                    ))
                },
                Nonce::new_random,
            )
            .set_pkce_challenge(challenge)
            .add_extra_param("response_mode", "query");
        for scope in self
            .definition
            .scopes()
            .iter()
            .filter(|scope| scope.as_str() != "openid")
        {
            request = request.add_scope(Scope::new(scope.clone()));
        }
        if fresh {
            request = request.set_max_age(Duration::ZERO);
        }
        let (url, state, nonce) = request.url();
        Ok(Authorization {
            url: url.to_string(),
            pending: PendingLogin {
                state: state.secret().to_owned(),
                nonce: nonce.secret().to_owned(),
                verifier: verifier.secret().to_owned(),
                issuer: self.definition.issuer().as_str().to_owned(),
                client_id: self.definition.client_id().to_owned(),
                callback: self.callback.as_str().to_owned(),
                response_issuer: discovered.response_issuer,
            },
        })
    }

    pub async fn exchange(
        &self,
        accepted: AcceptedCallback,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, OidcError> {
        tokio::time::timeout(
            Duration::from_secs(25),
            self.exchange_inner(accepted, context),
        )
        .await
        .map_err(|_| OidcError::Unavailable)?
    }

    async fn exchange_inner(
        &self,
        accepted: AcceptedCallback,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, OidcError> {
        self.check_pending(&accepted.pending)?;
        let discovered = self.discover().await?;
        let response = discovered
            .client
            .exchange_code(AuthorizationCode::new(accepted.code))
            .map_err(|_| OidcError::Configuration)?
            .set_pkce_verifier(PkceCodeVerifier::new(accepted.pending.verifier))
            .request_async(&|request| self.transport.send(request))
            .await
            .map_err(token_error)?;
        self.verify(
            &discovered,
            response,
            &accepted.pending.nonce,
            context,
            None,
        )
        .await
    }

    /// The application serializes rotation and destroys the previous credential
    /// on any ambiguous exchange. This method never retries a refresh token.
    pub async fn refresh(
        &self,
        retained: RetainedCredentials,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, OidcError> {
        if retained.issuer != self.definition.issuer().as_str()
            || retained.client_id != self.definition.client_id()
        {
            return Err(OidcError::Denied);
        }
        let discovered = self.discover().await?;
        let response = discovered
            .client
            .exchange_refresh_token(&RefreshToken::new(retained.refresh))
            .map_err(|_| OidcError::Configuration)?
            .request_async(&|request| self.transport.send(request))
            .await
            .map_err(token_error)?;
        self.verify(
            &discovered,
            response,
            &retained.nonce,
            context,
            Some((&retained.subject, retained.auth_time)),
        )
        .await
    }

    async fn verify(
        &self,
        discovered: &Discovered,
        response: CoreTokenResponse,
        nonce: &str,
        context: &ExternalAuthenticationContext,
        expected: Option<(&str, Option<DateTime<Utc>>)>,
    ) -> Result<Authentication, OidcError> {
        // CoreTokenResponse preserves the original compact JWT. Reparse with
        // bounded additional claims, then verify before accessing any claims.
        let token: ClaimsToken = response
            .id_token()
            .ok_or(OidcError::InvalidResponse)?
            .to_string()
            .parse()
            .map_err(|_| OidcError::InvalidResponse)?;
        let now = Utc::now();
        let verifier = discovered
            .client
            .id_token_verifier()
            .set_allowed_algs(
                self.definition
                    .signing_algorithms()
                    .iter()
                    .map(signing_algorithm),
            )
            .set_issue_time_verifier_fn(move |issued| {
                if issued > now + chrono::Duration::seconds(60) {
                    Err("future issue time".into())
                } else {
                    Ok(())
                }
            });
        let nonce = Nonce::new(nonce.to_owned());
        let claims = token
            .claims(&verifier, |received: Option<&Nonce>| {
                // OIDC Core 12.2 permits omission only in a refresh response. Any
                // supplied nonce must still match the original authentication.
                if expected.is_some() && received.is_none() {
                    Ok(())
                } else {
                    (&nonce).verify(received)
                }
            })
            .map_err(|_| OidcError::InvalidResponse)?;
        if claims.issuer().as_str() != self.definition.issuer().as_str()
            || claims.audiences().len() != 1
            || claims.audiences()[0].as_str() != self.definition.client_id()
            || claims
                .authorized_party()
                .is_some_and(|party| party.as_str() != self.definition.client_id())
            || claims.expiration() <= claims.issue_time()
            || expected.is_some_and(|(subject, auth_time)| {
                subject != claims.subject().as_str() || auth_time != claims.auth_time()
            })
        {
            return Err(OidcError::Denied);
        }
        if let Some(expected_hash) = claims.access_token_hash() {
            let hash = AccessTokenHash::from_token(
                response.access_token(),
                token
                    .signing_alg()
                    .map_err(|_| OidcError::InvalidResponse)?,
                token
                    .signing_key(&verifier)
                    .map_err(|_| OidcError::InvalidResponse)?,
            )
            .map_err(|_| OidcError::InvalidResponse)?;
            if &hash != expected_hash {
                return Err(OidcError::InvalidResponse);
            }
        }
        let id_document = serde_json::to_value(claims).map_err(|_| OidcError::InvalidResponse)?;
        validate_claim_document(&id_document, now)?;
        // UserInfo is requested only when the configured membership source needs
        // it; its subject must match the signed ID token exactly.
        let userinfo = if matches!(
            self.definition.membership(),
            ProviderMembership::ProviderGroups {
                claim_source: MembershipClaimSource::Userinfo,
                ..
            }
        ) {
            let endpoint = discovered
                .userinfo
                .as_ref()
                .ok_or(OidcError::Configuration)?;
            let request = openidconnect::http::Request::builder()
                .uri(endpoint)
                .header("accept", "application/json")
                .header(
                    "authorization",
                    format!("Bearer {}", response.access_token().secret()),
                )
                .body(Vec::new())
                .map_err(|_| OidcError::InvalidResponse)?;
            let reply = self.transport.send(request).await?;
            if reply.status() != openidconnect::http::StatusCode::OK {
                return Err(OidcError::Denied);
            }
            let document: Value =
                serde_json::from_slice(reply.body()).map_err(|_| OidcError::InvalidResponse)?;
            validate_claim_document(&document, now)?;
            if document.get("sub").and_then(Value::as_str) != Some(claims.subject().as_str()) {
                return Err(OidcError::Denied);
            }
            Some(document)
        } else {
            None
        };
        let profile = ExternalIdentityProfile::new(
            ExternalSubject::new(claims.subject().as_str())
                .map_err(|_| OidcError::InvalidResponse)?,
            id_document
                .get("preferred_username")
                .or_else(|| id_document.get("name"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            claims.email().map(|value| value.as_str().to_owned()),
        )
        .map_err(|_| OidcError::InvalidResponse)?;
        let memberships = match userinfo.as_ref() {
            Some(document) => MembershipClaims::Complete {
                source: MembershipClaimSource::Userinfo,
                document,
            },
            None => MembershipClaims::Complete {
                source: MembershipClaimSource::IdToken,
                document: &id_document,
            },
        };
        let authentication = context
            .verified_oidc(self.definition.issuer().as_str(), profile, memberships, now)
            .map_err(|_| OidcError::Denied)?;
        let credentials = response.refresh_token().map(|refresh| RetainedCredentials {
            refresh: refresh.secret().to_owned(),
            nonce: nonce.secret().to_owned(),
            subject: claims.subject().as_str().to_owned(),
            issuer: self.definition.issuer().as_str().to_owned(),
            client_id: self.definition.client_id().to_owned(),
            auth_time: claims.auth_time(),
        });
        Ok(Authentication {
            authentication,
            auth_time: claims.auth_time(),
            credentials,
        })
    }

    fn check_pending(&self, pending: &PendingLogin) -> Result<(), OidcError> {
        if pending.issuer != self.definition.issuer().as_str()
            || pending.client_id != self.definition.client_id()
            || pending.callback != self.callback.as_str()
        {
            return Err(OidcError::Denied);
        }
        Ok(())
    }

    pub async fn revoke(&self, credentials: RetainedCredentials) -> Result<bool, OidcError> {
        use openidconnect::{RevocationUrl, core::CoreRevocableToken};
        if credentials.issuer != self.definition.issuer().as_str()
            || credentials.client_id != self.definition.client_id()
        {
            return Err(OidcError::Denied);
        }
        let discovered = self.discover().await?;
        let Some(endpoint) = discovered.revocation else {
            return Ok(false);
        };
        discovered
            .client
            .set_revocation_url(RevocationUrl::new(endpoint).map_err(|_| OidcError::Configuration)?)
            .revoke_token(CoreRevocableToken::RefreshToken(RefreshToken::new(
                credentials.refresh,
            )))
            .map_err(|_| OidcError::Configuration)?
            .request_async(&|request| self.transport.send(request))
            .await
            .map_err(|_| OidcError::Unavailable)?;
        Ok(true)
    }
}

pub struct Authorization {
    url: String,
    pending: PendingLogin,
}
impl Authorization {
    pub fn into_parts(self) -> (String, PendingLogin) {
        (self.url, self.pending)
    }
}

/// Serialize only into the application's encrypted, interaction-bound storage.
/// The application also binds browser cookie, revision, deadline and single use.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLogin {
    state: String,
    nonce: String,
    verifier: String,
    issuer: String,
    client_id: String,
    callback: String,
    response_issuer: bool,
}
impl PendingLogin {
    pub fn state(&self) -> &str {
        &self.state
    }
    pub fn protectable(&self) -> Result<SecretValue, OidcError> {
        serialize_secret(self)
    }
    pub fn restore(secret: &SecretValue) -> Result<Self, OidcError> {
        serde_json::from_slice(secret.expose()).map_err(|_| OidcError::Denied)
    }
    pub fn accept(
        self,
        state: &str,
        issuer: Option<&str>,
        code: Option<&str>,
        error: Option<&str>,
    ) -> Result<AcceptedCallback, OidcError> {
        if CsrfToken::new(self.state.clone()) != CsrfToken::new(state.to_owned())
            || issuer.is_some_and(|issuer| issuer != self.issuer)
            || self.response_issuer && issuer.is_none()
            || error.is_some()
        {
            return Err(OidcError::Denied);
        }
        let code = code
            .filter(|code| {
                !code.is_empty() && code.len() <= 4096 && !code.chars().any(char::is_control)
            })
            .ok_or(OidcError::Denied)?;
        Ok(AcceptedCallback {
            pending: self,
            code: code.to_owned(),
        })
    }
}
pub struct AcceptedCallback {
    pending: PendingLogin,
    code: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedCredentials {
    refresh: String,
    nonce: String,
    subject: String,
    issuer: String,
    client_id: String,
    auth_time: Option<DateTime<Utc>>,
}
impl RetainedCredentials {
    pub fn protectable(&self) -> Result<SecretValue, OidcError> {
        serialize_secret(self)
    }
    pub fn restore(secret: &SecretValue) -> Result<Self, OidcError> {
        serde_json::from_slice(secret.expose()).map_err(|_| OidcError::Denied)
    }
}

pub struct Authentication {
    authentication: VerifiedExternalAuthentication,
    auth_time: Option<DateTime<Utc>>,
    credentials: Option<RetainedCredentials>,
}
impl Authentication {
    pub fn proof(&self) -> &VerifiedExternalAuthentication {
        &self.authentication
    }
    pub fn into_parts(self) -> (VerifiedExternalAuthentication, Option<RetainedCredentials>) {
        (self.authentication, self.credentials)
    }
    pub fn fresh(
        self,
        request: FreshExternalAuthenticationRequest,
    ) -> Result<FreshExternalAuthentication, OidcError> {
        request
            .verify(self.authentication, self.auth_time, Utc::now())
            .map_err(|_| OidcError::Denied)
    }
}

fn serialize_secret(value: &impl Serialize) -> Result<SecretValue, OidcError> {
    SecretValue::new(serde_json::to_vec(value).map_err(|_| OidcError::InvalidResponse)?)
        .map_err(|_| OidcError::InvalidResponse)
}
fn signing_algorithm(value: &OidcSigningAlgorithm) -> CoreJwsSigningAlgorithm {
    match value {
        OidcSigningAlgorithm::RS256 => CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        OidcSigningAlgorithm::ES256 => CoreJwsSigningAlgorithm::EcdsaP256Sha256,
    }
}
fn endpoint(value: &str) -> Result<(), OidcError> {
    let url = Url::parse(value).map_err(|_| OidcError::Configuration)?;
    if value.len() > 2048
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
        || url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(OidcError::Configuration);
    }
    Ok(())
}
fn validate_metadata(value: &Value, definition: &OidcProviderDefinition) -> Result<(), OidcError> {
    if value.get("issuer").and_then(Value::as_str) != Some(definition.issuer().as_str()) {
        return Err(OidcError::InvalidResponse);
    }
    for name in ["authorization_endpoint", "token_endpoint", "jwks_uri"] {
        endpoint(
            value
                .get(name)
                .and_then(Value::as_str)
                .ok_or(OidcError::Configuration)?,
        )?;
    }
    for name in [
        "userinfo_endpoint",
        "revocation_endpoint",
        "end_session_endpoint",
    ] {
        if let Some(value) = value.get(name) {
            endpoint(value.as_str().ok_or(OidcError::Configuration)?)?;
        }
    }
    for (name, required) in [
        ("code_challenge_methods_supported", "S256"),
        ("response_types_supported", "code"),
        ("response_modes_supported", "query"),
        (
            "token_endpoint_auth_methods_supported",
            "client_secret_basic",
        ),
    ] {
        if !value
            .get(name)
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(required)))
        {
            return Err(OidcError::Configuration);
        }
    }
    let advertised = value
        .get("id_token_signing_alg_values_supported")
        .and_then(Value::as_array)
        .ok_or(OidcError::Configuration)?;
    if !definition.signing_algorithms().iter().any(|algorithm| {
        let name = match algorithm {
            OidcSigningAlgorithm::RS256 => "RS256",
            OidcSigningAlgorithm::ES256 => "ES256",
        };
        advertised.iter().any(|value| value.as_str() == Some(name))
    }) {
        return Err(OidcError::Configuration);
    }
    Ok(())
}
fn validate_claim_document(value: &Value, now: DateTime<Utc>) -> Result<(), OidcError> {
    if value.get("_claim_names").is_some()
        || value.get("_claim_sources").is_some()
        || value
            .get("nbf")
            .is_some_and(|value| value.as_i64().is_none_or(|nbf| nbf > now.timestamp()))
    {
        return Err(OidcError::InvalidResponse);
    }
    Ok(())
}
fn token_error<T: openidconnect::ErrorResponse>(
    error: openidconnect::RequestTokenError<OidcError, T>,
) -> OidcError {
    match error {
        openidconnect::RequestTokenError::Request(error) => error,
        openidconnect::RequestTokenError::ServerResponse(_) => OidcError::Denied,
        _ => OidcError::InvalidResponse,
    }
}

#[cfg(test)]
mod tests;
