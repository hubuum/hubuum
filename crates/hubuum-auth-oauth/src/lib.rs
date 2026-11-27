//! OAuth identity comes only from a server code exchange and pinned user API.
use std::time::Duration;

use chrono::{DateTime, Utc};
pub use hubuum_auth_core::protocol::ProtocolError as OAuthError;
use hubuum_auth_core::protocol::{CodeState, Transport};
use hubuum_auth_core::{
    ExternalAuthenticationContext, MembershipClaims, VerifiedExternalAuthentication,
};
use hubuum_domain::{
    InteractionId, MembershipClaimSource, OAuthProfile, OAuthProviderDefinition,
    ProviderClientAuthentication,
};
use hubuum_secrets::SecretValue;
use oauth2::{
    AsyncHttpClient, AuthType, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EndpointNotSet, EndpointSet, PkceCodeVerifier, RedirectUrl, Scope, TokenResponse, TokenUrl,
    basic::{BasicClient, BasicRequestTokenError, BasicTokenResponse},
    http,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use hubuum_auth_core::protocol::profile;

type Client = BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;
const GITHUB_ISSUER: &str = "https://github.com/login/oauth";

pub struct OAuthAdapter {
    definition: OAuthProviderDefinition,
    secret: SecretValue,
    callback: RedirectUrl,
    transport: Transport,
}

impl OAuthAdapter {
    pub fn new(
        definition: OAuthProviderDefinition,
        secret: SecretValue,
        callback: &hubuum_domain::BrowserCallbackUrl,
        allow_private_targets: bool,
    ) -> Result<Self, OAuthError> {
        if secret.len() > 16 * 1024 || secret.expose_utf8().is_err() {
            return Err(OAuthError::Configuration);
        }
        Ok(Self {
            definition,
            secret,
            callback: RedirectUrl::new(callback.as_str().to_owned())
                .map_err(|_| OAuthError::Configuration)?,
            transport: Transport::new(allow_private_targets),
        })
    }

    fn client(&self) -> Result<Client, OAuthError> {
        let [authorization, token, _] = self.definition.endpoints();
        Ok(
            BasicClient::new(ClientId::new(self.definition.client_id().to_owned()))
                .set_client_secret(ClientSecret::new(
                    self.secret
                        .expose_utf8()
                        .map_err(|_| OAuthError::Configuration)?
                        .to_owned(),
                ))
                .set_auth_uri(
                    AuthUrl::new(authorization.as_str().to_owned())
                        .map_err(|_| OAuthError::Configuration)?,
                )
                .set_token_uri(
                    TokenUrl::new(token.as_str().to_owned())
                        .map_err(|_| OAuthError::Configuration)?,
                )
                .set_auth_type(match self.definition.client_authentication() {
                    ProviderClientAuthentication::ClientSecretBasic => AuthType::BasicAuth,
                    ProviderClientAuthentication::ClientSecretPost => AuthType::RequestBody,
                })
                .set_redirect_uri(self.callback.clone()),
        )
    }

    pub fn begin(&self, interaction: InteractionId) -> Result<(String, PendingLogin), OAuthError> {
        let code_state = CodeState::new(interaction);
        let client = self.client()?;
        let mut request = client
            .authorize_url(|| CsrfToken::new(code_state.state().to_owned()))
            .set_pkce_challenge(code_state.challenge());
        for scope in self.definition.scopes() {
            request = request.add_scope(Scope::new(scope.clone()));
        }
        let (url, _) = request.url();
        Ok((
            url.to_string(),
            PendingLogin {
                code_state,
                definition_digest: definition_digest(&self.definition),
                callback: self.callback.as_str().to_owned(),
            },
        ))
    }

    pub fn accept(
        &self,
        pending: PendingLogin,
        state: &str,
        issuer: Option<&str>,
        code: Option<&str>,
        error: Option<&str>,
    ) -> Result<AcceptedCallback, OAuthError> {
        // Match RFC 9207 issuers exactly. Generic profiles have no configured
        // issuer contract; GitHub's documented issuer is pinned to its profile.
        let issuer_matches = matches!(
            (self.definition.profile(), issuer),
            (OAuthProfile::Github, Some(GITHUB_ISSUER)) | (OAuthProfile::JsonUser, None)
        );
        if !issuer_matches
            || pending.definition_digest != definition_digest(&self.definition)
            || pending.callback != self.callback.as_str()
        {
            return Err(OAuthError::Denied);
        }
        let code = pending.code_state.accept(state, code, error)?;
        Ok(AcceptedCallback { pending, code })
    }

    pub async fn exchange(
        &self,
        accepted: AcceptedCallback,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, OAuthError> {
        self.exchange_using(accepted, context, &|request| self.transport.send(request))
            .await
    }

    async fn exchange_using<C: for<'c> AsyncHttpClient<'c, Error = OAuthError>>(
        &self,
        accepted: AcceptedCallback,
        context: &ExternalAuthenticationContext,
        client: &C,
    ) -> Result<Authentication, OAuthError> {
        if accepted.pending.definition_digest != definition_digest(&self.definition)
            || accepted.pending.callback != self.callback.as_str()
        {
            return Err(OAuthError::Denied);
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            let response = self
                .client()?
                .exchange_code(AuthorizationCode::new(accepted.code))
                .set_pkce_verifier(PkceCodeVerifier::new(
                    accepted.pending.code_state.verifier().to_owned(),
                ))
                .request_async(&|request| async {
                    let reply = client.call(request).await?;
                    if reply.status() == http::StatusCode::OK {
                        validate_token_response(&reply)?;
                    }
                    Ok(reply)
                })
                .await
                .map_err(token_error)?;
            let credentials = self.credentials(response)?;
            self.authenticate_using(credentials, context, client).await
        })
        .await
        .map_err(|_| OAuthError::Unavailable)?
    }

    fn credentials(&self, response: BasicTokenResponse) -> Result<RetainedCredentials, OAuthError> {
        if !response
            .token_type()
            .as_ref()
            .eq_ignore_ascii_case("bearer")
        {
            return Err(OAuthError::InvalidResponse);
        }
        if let Some(scopes) = response.scopes() {
            for required in self
                .definition
                .scopes()
                .iter()
                .filter(|scope| scope.as_str() != "offline_access")
            {
                let allowed = scopes.iter().any(|scope| {
                    if *self.definition.profile() == OAuthProfile::Github {
                        scope.as_str().split(',').any(|scope| {
                            scope == required
                                || scope == "user"
                                    && matches!(
                                        required.as_str(),
                                        "read:user" | "user:email" | "user:follow"
                                    )
                        })
                    } else {
                        scope.as_str() == required
                    }
                });
                if !allowed {
                    return Err(OAuthError::Denied);
                }
            }
        }
        validate_token(response.access_token().secret())?;
        let expires = response
            .expires_in()
            .map(|duration| {
                chrono::Duration::from_std(duration)
                    .ok()
                    .and_then(|duration| Utc::now().checked_add_signed(duration))
                    .ok_or(OAuthError::InvalidResponse)
            })
            .transpose()?;
        Ok(RetainedCredentials {
            access: response.access_token().secret().to_owned(),
            expires,
            definition_digest: definition_digest(&self.definition),
            client_id: self.definition.client_id().to_owned(),
            subject: None,
        })
    }

    /// Re-read eligibility using the retained bearer. No refresh-token grant is
    /// assumed: expired access requires a new interactive sign-in.
    pub async fn refresh(
        &self,
        credentials: RetainedCredentials,
        context: &ExternalAuthenticationContext,
    ) -> Result<Authentication, OAuthError> {
        self.refresh_with_dispatch(credentials, context, || true)
            .await
    }

    pub async fn refresh_with_dispatch(
        &self,
        credentials: RetainedCredentials,
        context: &ExternalAuthenticationContext,
        submitting: impl Fn() -> bool + Clone + Sync,
    ) -> Result<Authentication, OAuthError> {
        self.authenticate_using(credentials, context, &|request| {
            self.transport
                .send_with_dispatch(request, submitting.clone())
        })
        .await
    }

    async fn authenticate_using<'c, C: AsyncHttpClient<'c, Error = OAuthError>>(
        &self,
        mut credentials: RetainedCredentials,
        context: &ExternalAuthenticationContext,
        client: &'c C,
    ) -> Result<Authentication, OAuthError> {
        self.check_credentials(&credentials)?;
        let request = self.profile_request(&credentials.access)?;
        let response = client.call(request).await?;
        if *self.definition.profile() == OAuthProfile::Github && github_rate_limited(&response) {
            return Err(OAuthError::Unavailable);
        }
        // The authenticated-user endpoint rejected this bearer. Another session
        // must still prove the same subject and current eligibility independently.
        if response.status() == http::StatusCode::UNAUTHORIZED && credentials.subject.is_some() {
            return Err(OAuthError::CredentialExpired);
        }
        if response.status() != http::StatusCode::OK {
            return Err(OAuthError::Denied);
        }
        if !profile::has_json_content_type(response.headers()) {
            return Err(OAuthError::InvalidResponse);
        }
        let document = profile::parse(response.body())?;
        let proof =
            self.verify_document(&document, context, profile::paginated(response.headers())?)?;
        if credentials
            .subject
            .as_ref()
            .is_some_and(|subject| subject != proof.identity().subject().as_str())
        {
            return Err(OAuthError::Denied);
        }
        credentials.subject = Some(proof.identity().subject().as_str().to_owned());
        Ok(Authentication { proof, credentials })
    }

    fn profile_request(&self, access: &str) -> Result<http::Request<Vec<u8>>, OAuthError> {
        let github = *self.definition.profile() == OAuthProfile::Github;
        let mut request = http::Request::builder()
            .uri(self.definition.endpoints()[2].as_str())
            .header(
                "accept",
                if github {
                    "application/vnd.github+json"
                } else {
                    "application/json"
                },
            )
            .header("user-agent", "Hubuum-OAuth")
            .header("authorization", format!("Bearer {access}"));
        if github {
            request = request.header("x-github-api-version", "2026-03-10");
        }
        let mut request = request
            .body(Vec::new())
            .map_err(|_| OAuthError::Configuration)?;
        for (name, value) in self.definition.profile_headers() {
            request.headers_mut().insert(
                http::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| OAuthError::Configuration)?,
                http::header::HeaderValue::from_str(value)
                    .map_err(|_| OAuthError::Configuration)?,
            );
        }
        Ok(request)
    }

    fn verify_document(
        &self,
        document: &Value,
        context: &ExternalAuthenticationContext,
        paginated: bool,
    ) -> Result<VerifiedExternalAuthentication, OAuthError> {
        if *self.definition.profile() == OAuthProfile::Github
            && (document.get("type").and_then(Value::as_str) != Some("User")
                || document
                    .get("id")
                    .and_then(Value::as_u64)
                    .is_none_or(|id| id == 0))
        {
            return Err(OAuthError::InvalidResponse);
        }
        let proof = context
            .verified_oauth(
                document,
                if paginated {
                    MembershipClaims::Incomplete
                } else {
                    MembershipClaims::Complete {
                        source: MembershipClaimSource::Profile,
                        document,
                    }
                },
                Utc::now(),
            )
            .map_err(|error| match OAuthError::from(error) {
                OAuthError::MembershipRejected(rejection)
                    if rejection.identity().namespace() != self.definition.identity_namespace() =>
                {
                    OAuthError::Denied
                }
                error => error,
            })?;
        if proof.identity().namespace() != self.definition.identity_namespace() {
            return Err(OAuthError::Denied);
        }
        Ok(proof)
    }

    fn check_credentials(&self, credentials: &RetainedCredentials) -> Result<(), OAuthError> {
        // Configuration revision changes invalidate sessions at storage. Match
        // the exact definition here too, before sending a bearer anywhere.
        if credentials.definition_digest != definition_digest(&self.definition)
            || credentials.client_id != self.definition.client_id()
        {
            return Err(OAuthError::Denied);
        }
        if credentials
            .expires
            .is_some_and(|expiry| expiry <= Utc::now())
        {
            return Err(OAuthError::CredentialExpired);
        }
        validate_token(&credentials.access)
    }

    pub async fn revoke(&self, credentials: RetainedCredentials) -> Result<bool, OAuthError> {
        self.revoke_using(credentials, &|request| self.transport.send(request))
            .await
    }

    async fn revoke_using<C: for<'c> AsyncHttpClient<'c, Error = OAuthError>>(
        &self,
        credentials: RetainedCredentials,
        client: &C,
    ) -> Result<bool, OAuthError> {
        // An expired token can still be revoked. Endpoint and client bindings
        // must match before disclosing it to the GitHub revocation API.
        if credentials.definition_digest != definition_digest(&self.definition)
            || credentials.client_id != self.definition.client_id()
        {
            return Err(OAuthError::Denied);
        }
        if *self.definition.profile() != OAuthProfile::Github {
            return Ok(false);
        }
        let mut url = url::Url::parse("https://api.github.com/applications/")
            .map_err(|_| OAuthError::Configuration)?;
        url.path_segments_mut()
            .map_err(|_| OAuthError::Configuration)?
            .pop_if_empty()
            .push(self.definition.client_id())
            .push("token");
        let request = http::Request::builder()
            .method(http::Method::DELETE)
            .uri(url.as_str())
            .header("accept", "application/vnd.github+json")
            .header("user-agent", "Hubuum-OAuth")
            .header("x-github-api-version", "2026-03-10")
            .header("content-type", "application/json")
            .header(
                "authorization",
                basic_auth(
                    self.definition.client_id(),
                    self.secret
                        .expose_utf8()
                        .map_err(|_| OAuthError::Configuration)?,
                ),
            )
            .body(
                serde_json::to_vec(&serde_json::json!({"access_token": credentials.access}))
                    .map_err(|_| OAuthError::InvalidResponse)?,
            )
            .map_err(|_| OAuthError::Configuration)?;
        let response = client.call(request).await?;
        if !matches!(response.status().as_u16(), 204 | 404) {
            return Err(OAuthError::Denied);
        }
        Ok(true)
    }
}

fn github_rate_limited(response: &http::Response<Vec<u8>>) -> bool {
    response.status() == http::StatusCode::FORBIDDEN
        && (response
            .headers()
            .get("x-ratelimit-remaining")
            .is_some_and(|value| value == "0")
            || response.headers().contains_key(http::header::RETRY_AFTER)
            || serde_json::from_slice::<Value>(response.body())
                .ok()
                .is_some_and(|document| {
                    document
                        .get("message")
                        .and_then(Value::as_str)
                        .is_some_and(|message| {
                            message
                                .to_ascii_lowercase()
                                .contains("secondary rate limit")
                        })
                }))
}

fn basic_auth(client: &str, secret: &str) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    format!("Basic {}", STANDARD.encode(format!("{client}:{secret}")))
}

fn validate_token(token: &str) -> Result<(), OAuthError> {
    if token.is_empty()
        || token.len() > 16 * 1024
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(OAuthError::InvalidResponse);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLogin {
    code_state: CodeState,
    definition_digest: [u8; 32],
    callback: String,
}
impl PendingLogin {
    pub fn state(&self) -> &str {
        self.code_state.state()
    }
    pub fn protectable(&self) -> Result<SecretValue, OAuthError> {
        serialize_secret(self)
    }
    pub fn restore(secret: &SecretValue) -> Result<Self, OAuthError> {
        serde_json::from_slice(secret.expose()).map_err(|_| OAuthError::Denied)
    }
}
pub struct AcceptedCallback {
    pending: PendingLogin,
    code: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedCredentials {
    access: String,
    expires: Option<DateTime<Utc>>,
    definition_digest: [u8; 32],
    client_id: String,
    subject: Option<String>,
}
impl RetainedCredentials {
    pub fn protectable(&self) -> Result<SecretValue, OAuthError> {
        serialize_secret(self)
    }
    pub fn restore(secret: &SecretValue) -> Result<Self, OAuthError> {
        serde_json::from_slice(secret.expose()).map_err(|_| OAuthError::Denied)
    }
}
pub struct Authentication {
    proof: VerifiedExternalAuthentication,
    credentials: RetainedCredentials,
}
impl Authentication {
    pub fn proof(&self) -> &VerifiedExternalAuthentication {
        &self.proof
    }
    pub fn into_parts(self) -> (VerifiedExternalAuthentication, RetainedCredentials) {
        (self.proof, self.credentials)
    }
}
fn serialize_secret(value: &impl Serialize) -> Result<SecretValue, OAuthError> {
    SecretValue::new(serde_json::to_vec(value).map_err(|_| OAuthError::InvalidResponse)?)
        .map_err(|_| OAuthError::InvalidResponse)
}
fn validate_token_response(reply: &oauth2::HttpResponse) -> Result<(), OAuthError> {
    if !profile::has_json_content_type(reply.headers()) {
        return Err(OAuthError::InvalidResponse);
    }
    let document = profile::parse(reply.body())?;
    let Some(error) = document.get("error") else {
        return Ok(());
    };
    if document.get("access_token").is_some() || document.get("refresh_token").is_some() {
        return Err(OAuthError::InvalidResponse);
    }
    Err(token_endpoint_error(
        error.as_str().ok_or(OAuthError::InvalidResponse)?,
    ))
}

fn token_endpoint_error(code: &str) -> OAuthError {
    match code {
        "server_error" | "temporarily_unavailable" => OAuthError::Unavailable,
        "invalid_client"
        | "unauthorized_client"
        | "invalid_scope"
        | "unsupported_grant_type"
        | "invalid_request"
        | "incorrect_client_credentials"
        | "redirect_uri_mismatch" => OAuthError::Configuration,
        _ => OAuthError::Denied,
    }
}

fn token_error(error: BasicRequestTokenError<OAuthError>) -> OAuthError {
    match error {
        oauth2::RequestTokenError::Request(error) => error,
        oauth2::RequestTokenError::ServerResponse(response) => {
            token_endpoint_error(response.error().as_ref())
        }
        _ => OAuthError::InvalidResponse,
    }
}

fn definition_digest(definition: &OAuthProviderDefinition) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(
        serde_json::to_vec(definition).expect("validated OAuth definition is serializable"),
    )
    .into()
}

#[cfg(test)]
mod tests;
