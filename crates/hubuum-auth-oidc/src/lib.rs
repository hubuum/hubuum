//! OIDC protocol adapter. Protocol types and upstream credentials stay private.
use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

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
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointMaybeSet,
    EndpointNotSet, EndpointSet, HttpRequest, HttpResponse, Nonce, NonceVerifier,
    OAuth2TokenResponse, PkceCodeVerifier, RedirectUrl, RefreshToken, Scope, TokenResponse,
    core::{
        CoreAuthenticationFlow, CoreClient, CoreJsonWebKeySet, CoreJwsSigningAlgorithm,
        CoreProviderMetadata, CoreTokenResponse,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

pub use hubuum_auth_core::protocol::ProtocolError as OidcError;
use hubuum_auth_core::protocol::{CodeState, Transport, profile};

type Client = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;
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
        callback: &hubuum_domain::BrowserCallbackUrl,
        allow_private_targets: bool,
    ) -> Result<Self, OidcError> {
        if secret.len() > 16 * 1024 || secret.expose_utf8().is_err() {
            return Err(OidcError::Configuration);
        }
        Ok(Self {
            definition,
            secret,
            callback: RedirectUrl::new(callback.as_str().to_owned())
                .map_err(|_| OidcError::Configuration)?,
            transport: Transport::new(allow_private_targets),
        })
    }

    async fn discover(&self) -> Result<Discovered, OidcError> {
        tokio::time::timeout(Duration::from_secs(15), self.discover_inner())
            .await
            .map_err(|_| OidcError::Unavailable)?
            .map_err(discovery_error)
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
                    self.transport.allow_private_targets(),
                    false,
                )
                .await
                .map_err(|error| {
                    if error.is_dns_failure() {
                        OidcError::Unavailable
                    } else {
                        OidcError::Configuration
                    }
                })?;
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
        let code_state = CodeState::new(interaction);
        let challenge = code_state.challenge();
        let state = code_state.state().to_owned();
        let mut request = discovered
            .client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                move || CsrfToken::new(state),
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
        let (url, _state, nonce) = request.url();
        Ok(Authorization {
            url: url.to_string(),
            pending: PendingLogin {
                code_state,
                nonce: nonce.secret().to_owned(),
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
        let grant = GrantResponse::default();
        let response = discovered
            .client
            .exchange_code(AuthorizationCode::new(accepted.code))
            .map_err(|_| OidcError::Configuration)?
            .set_pkce_verifier(PkceCodeVerifier::new(
                accepted.pending.code_state.verifier().to_owned(),
            ))
            .request_async(&|request| grant.clone().inspect(self.transport.send(request)))
            .await;
        let response = grant.decode(response)?;
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
        submitting: impl Fn() -> bool + Clone + Sync,
    ) -> Result<Authentication, OidcError> {
        if retained.issuer != self.definition.issuer().as_str()
            || retained.client_id != self.definition.client_id()
        {
            return Err(OidcError::Denied);
        }
        let discovered = self.discover().await?;
        let transport = self.transport;
        self.refresh_using(&discovered, retained, context, move |request| {
            transport.send_with_dispatch(request, submitting.clone())
        })
        .await
    }

    async fn refresh_using<F>(
        &self,
        discovered: &Discovered,
        retained: RetainedCredentials,
        context: &ExternalAuthenticationContext,
        send: impl Fn(HttpRequest) -> F,
    ) -> Result<Authentication, OidcError>
    where
        F: Future<Output = Result<HttpResponse, OidcError>>,
    {
        let grant = GrantResponse::default();
        let response = discovered
            .client
            .exchange_refresh_token(&RefreshToken::new(retained.refresh.clone()))
            .map_err(|_| OidcError::Configuration)?
            .request_async(&|request| grant.clone().inspect(send(request)))
            .await;
        self.verify_refresh(discovered, grant.decode(response)?, retained, context)
            .await
    }

    async fn verify_refresh(
        &self,
        discovered: &Discovered,
        response: CoreTokenResponse,
        retained: RetainedCredentials,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, OidcError> {
        let mut authentication = self
            .verify(
                discovered,
                response,
                &retained.nonce,
                context,
                Some((&retained.subject, retained.auth_time)),
            )
            .await?;
        authentication.credentials.get_or_insert(retained);
        Ok(authentication)
    }

    async fn verify(
        &self,
        discovered: &Discovered,
        response: CoreTokenResponse,
        nonce: &str,
        context: &ExternalAuthenticationContext,
        expected: Option<(&str, Option<DateTime<Utc>>)>,
    ) -> Result<Authentication, OidcError> {
        let token = response.id_token().ok_or(OidcError::InvalidResponse)?;
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
                subject != claims.subject().as_str()
                    || claims
                        .auth_time()
                        .is_some_and(|supplied| Some(supplied) != auth_time)
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
        // CoreIdToken retains the original signing input. Decoding that payload
        // preserves duplicate custom members until the strict JSON parser sees them.
        let compact = token.to_string();
        let payload = compact
            .split('.')
            .nth(1)
            .ok_or(OidcError::InvalidResponse)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| OidcError::InvalidResponse)?;
        let id_document = profile::parse(&bytes)?;
        let id_memberships =
            self.membership_claims(&id_document, MembershipClaimSource::IdToken, now, false)?;
        // UserInfo is requested only when the configured membership source needs
        // it; its subject must match the signed ID token exactly.
        let userinfo = if matches!(
            self.definition.membership(),
            ProviderMembership::ProviderGroups {
                claim_source: MembershipClaimSource::Userinfo,
                ..
            }
        ) {
            if !response
                .token_type()
                .as_ref()
                .eq_ignore_ascii_case("bearer")
            {
                return Err(OidcError::Configuration);
            }
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
            Some(userinfo_profile(&reply, claims.subject().as_str())?)
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
            Some((document, paginated)) => {
                self.membership_claims(document, MembershipClaimSource::Userinfo, now, *paginated)?
            }
            None => id_memberships,
        };
        let authentication = context
            .verified_oidc(self.definition.issuer().as_str(), profile, memberships, now)
            .map_err(OidcError::from)?;
        let credentials = response.refresh_token().map(|refresh| RetainedCredentials {
            refresh: refresh.secret().to_owned(),
            nonce: nonce.secret().to_owned(),
            subject: claims.subject().as_str().to_owned(),
            issuer: self.definition.issuer().as_str().to_owned(),
            client_id: self.definition.client_id().to_owned(),
            auth_time: expected.map_or_else(|| claims.auth_time(), |(_, original)| original),
        });
        Ok(Authentication {
            authentication,
            auth_time: claims.auth_time(),
            credentials,
        })
    }

    fn membership_claims<'a>(
        &self,
        document: &'a Value,
        source: MembershipClaimSource,
        now: DateTime<Utc>,
        paginated: bool,
    ) -> Result<MembershipClaims<'a>, OidcError> {
        if document
            .get("nbf")
            .is_some_and(|value| value.as_i64().is_none_or(|nbf| nbf > now.timestamp()))
        {
            return Err(OidcError::InvalidResponse);
        }
        if paginated {
            return Ok(MembershipClaims::Incomplete);
        }
        if document.get("_claim_names").is_none() && document.get("_claim_sources").is_none() {
            return Ok(MembershipClaims::Complete { source, document });
        }
        if let ProviderMembership::ProviderGroups {
            claim_source,
            claim_pointer,
            ..
        } = self.definition.membership()
            && *claim_source == source
            && let Some(root) = claim_pointer.split('/').nth(1)
            && document.pointer(&format!("/_claim_names/{root}")).is_some()
        {
            return Ok(MembershipClaims::Incomplete);
        }
        Err(OidcError::InvalidResponse)
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
pub struct PendingLogin {
    #[serde(flatten)]
    code_state: CodeState,
    nonce: String,
    issuer: String,
    client_id: String,
    callback: String,
    response_issuer: bool,
}
impl PendingLogin {
    pub fn state(&self) -> &str {
        self.code_state.state()
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
        let code = self.code_state.accept(state, code, error)?;
        if issuer.is_some_and(|issuer| issuer != self.issuer)
            || self.response_issuer && issuer.is_none()
        {
            return Err(OidcError::Denied);
        }
        Ok(AcceptedCallback {
            pending: self,
            code,
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
        self.fresh_with_credentials(request).map(|(proof, _)| proof)
    }
    pub fn fresh_with_credentials(
        self,
        request: FreshExternalAuthenticationRequest,
    ) -> Result<(FreshExternalAuthentication, Option<RetainedCredentials>), OidcError> {
        let proof = request
            .verify(self.authentication, self.auth_time, Utc::now())
            .map_err(|_| OidcError::Denied)?;
        Ok((proof, self.credentials))
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
fn userinfo_profile(
    reply: &openidconnect::HttpResponse,
    signed_subject: &str,
) -> Result<(Value, bool), OidcError> {
    if reply.status() != openidconnect::http::StatusCode::OK {
        return Err(OidcError::Denied);
    }
    if !profile::has_json_content_type(reply.headers()) {
        return Err(OidcError::Unavailable);
    }
    let document = profile::parse(reply.body())?;
    if document.get("sub").and_then(Value::as_str) != Some(signed_subject) {
        return Err(OidcError::Denied);
    }
    Ok((document, profile::paginated(reply.headers())?))
}

fn validate_metadata(value: &Value, definition: &OidcProviderDefinition) -> Result<(), OidcError> {
    if value.get("issuer").and_then(Value::as_str) != Some(definition.issuer().as_str())
        || value
            .get("authorization_response_iss_parameter_supported")
            .is_some_and(|supported| !supported.is_boolean())
    {
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
    for (name, required, supported_by_default) in [
        ("code_challenge_methods_supported", "S256", false),
        ("response_types_supported", "code", false),
        ("response_modes_supported", "query", true),
        (
            "token_endpoint_auth_methods_supported",
            "client_secret_basic",
            true,
        ),
    ] {
        let supported = match value.get(name) {
            None => supported_by_default,
            Some(Value::Array(items)) => {
                items.iter().all(Value::is_string)
                    && items.iter().any(|item| item.as_str() == Some(required))
            }
            _ => false,
        };
        if !supported {
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
// Discovery and JWKS are provider-level requests. Neither submits a user grant.
fn discovery_error(error: OidcError) -> OidcError {
    match error {
        OidcError::Configuration => OidcError::Configuration,
        _ => OidcError::Unavailable,
    }
}

type TokenError = openidconnect::RequestTokenError<
    OidcError,
    openidconnect::StandardErrorResponse<openidconnect::core::CoreErrorResponseType>,
>;

/// Check the wire envelope independently of the SDK, which accepts unknown
/// members and decodes the ID token while deserializing the response.
#[derive(Clone, Default)]
struct GrantResponse {
    envelope: Arc<OnceLock<Result<(), OidcError>>>,
}

impl GrantResponse {
    async fn inspect(
        self,
        pending: impl Future<Output = Result<HttpResponse, OidcError>>,
    ) -> Result<HttpResponse, OidcError> {
        let reply = pending.await?;
        if reply.status() == openidconnect::http::StatusCode::OK {
            self.envelope
                .get_or_init(|| {
                    validate_grant_envelope(reply.body())?;
                    profile::has_json_content_type(reply.headers())
                        .then_some(())
                        .ok_or(OidcError::Unavailable)
                })
                .clone()?;
        }
        Ok(reply)
    }

    fn decode(
        self,
        response: Result<CoreTokenResponse, TokenError>,
    ) -> Result<CoreTokenResponse, OidcError> {
        match self.envelope.get() {
            Some(Ok(())) => response.map_err(|error| match error {
                openidconnect::RequestTokenError::Parse(..) => OidcError::InvalidResponse,
                other => token_error(other),
            }),
            Some(Err(error)) => Err(error.clone()),
            None => response.map_err(token_error),
        }
    }
}

fn validate_grant_envelope(body: &[u8]) -> Result<(), OidcError> {
    let has_grant_field = |document: &Value| {
        ["access_token", "id_token", "refresh_token"]
            .iter()
            .any(|key| document.get(key).is_some())
    };
    let document = profile::parse(body).map_err(|_| {
        // An unreadable response provides no identity decision. Use the lenient
        // parser only to distinguish a malformed grant from a gateway error.
        if serde_json::from_slice::<Value>(body).is_ok_and(|doc| has_grant_field(&doc)) {
            OidcError::InvalidResponse
        } else {
            OidcError::Unavailable
        }
    })?;
    if !has_grant_field(&document) {
        return Err(OidcError::Unavailable);
    }
    if document.get("error").is_some() {
        return Err(OidcError::InvalidResponse);
    }
    Ok(())
}

fn token_error(error: TokenError) -> OidcError {
    use openidconnect::core::CoreErrorResponseType::*;
    match error {
        openidconnect::RequestTokenError::Request(OidcError::InvalidResponse) => {
            OidcError::Unavailable
        }
        openidconnect::RequestTokenError::Request(error) => error,
        openidconnect::RequestTokenError::ServerResponse(response) => match response.error() {
            InvalidGrant => OidcError::Denied,
            InvalidClient | UnauthorizedClient | InvalidRequest | InvalidScope
            | UnsupportedGrantType => OidcError::Configuration,
            Extension(_) => OidcError::Unavailable,
        },
        // An empty or unparseable endpoint response contains no grant decision.
        _ => OidcError::Unavailable,
    }
}

#[cfg(test)]
mod tests;
