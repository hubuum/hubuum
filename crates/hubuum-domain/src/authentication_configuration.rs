//! Validated, non-secret configuration shared across authentication boundaries.
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationConfigurationError(&'static str);
impl std::fmt::Display for AuthenticationConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for AuthenticationConfigurationError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = String, format = "uri", max_length = 2048))]
pub struct AuthenticationEndpoint(String);
impl TryFrom<String> for AuthenticationEndpoint {
    type Error = AuthenticationConfigurationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let url = Url::parse(&value)
            .map_err(|_| AuthenticationConfigurationError("Invalid authentication endpoint"))?;
        if value.len() > 2048
            || value.chars().any(|c| c.is_whitespace() || c.is_control())
            || url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
        {
            return Err(AuthenticationConfigurationError(
                "Authentication endpoints require HTTPS without userinfo, query or fragment",
            ));
        }
        Ok(Self(value))
    }
}
impl AuthenticationEndpoint {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ProviderClientAuthentication {
    ClientSecretBasic,
    ClientSecretPost,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ProviderAdmission {
    ExistingOnly,
    ProvisionWithoutGrants,
    SubjectAllowlist { subjects: Vec<String> },
}
impl ProviderAdmission {
    fn validate(&self) -> Result<(), AuthenticationConfigurationError> {
        if let Self::SubjectAllowlist { subjects } = self {
            validate_strings(subjects, 1000, 255)?;
            if subjects.is_empty() {
                return Err(AuthenticationConfigurationError(
                    "Subject allowlist must not be empty",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum MembershipClaimSource {
    IdToken,
    Userinfo,
    Profile,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum MembershipKeyMode {
    FullPath,
    ImmutableId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ProviderMembership {
    LocalOnly {
        refresh_interval_seconds: u32,
        max_stale_seconds: u32,
    },
    ProviderGroups {
        refresh_interval_seconds: u32,
        max_stale_seconds: u32,
        claim_source: MembershipClaimSource,
        claim_pointer: String,
        key_mode: MembershipKeyMode,
    },
}
impl ProviderMembership {
    fn validate(&self, oidc: bool) -> Result<(), AuthenticationConfigurationError> {
        let (interval, stale) = match self {
            Self::LocalOnly {
                refresh_interval_seconds,
                max_stale_seconds,
            } => (*refresh_interval_seconds, *max_stale_seconds),
            Self::ProviderGroups {
                refresh_interval_seconds,
                max_stale_seconds,
                claim_source,
                claim_pointer,
                ..
            } => {
                validate_pointer(claim_pointer)?;
                if oidc == (*claim_source == MembershipClaimSource::Profile) {
                    return Err(AuthenticationConfigurationError(
                        "Membership claim source does not match provider kind",
                    ));
                }
                (*refresh_interval_seconds, *max_stale_seconds)
            }
        };
        if interval == 0 || stale < interval || interval > 300 || stale > 900 {
            return Err(AuthenticationConfigurationError(
                "Membership freshness must satisfy 1 <= interval <= 300 and interval <= max_stale <= 900",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum OidcSigningAlgorithm {
    RS256,
    ES256,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "OidcDefinitionInput")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct OidcProviderDefinition {
    issuer: AuthenticationEndpoint,
    client_id: String,
    client_authentication: ProviderClientAuthentication,
    scopes: Vec<String>,
    signing_algorithms: Vec<OidcSigningAlgorithm>,
    membership: ProviderMembership,
    admission: ProviderAdmission,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    retain_refresh_tokens: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcDefinitionInput {
    issuer: AuthenticationEndpoint,
    client_id: String,
    client_authentication: ProviderClientAuthentication,
    scopes: Vec<String>,
    signing_algorithms: Vec<OidcSigningAlgorithm>,
    membership: ProviderMembership,
    admission: ProviderAdmission,
    #[serde(default)]
    retain_refresh_tokens: bool,
}
impl TryFrom<OidcDefinitionInput> for OidcProviderDefinition {
    type Error = AuthenticationConfigurationError;
    fn try_from(value: OidcDefinitionInput) -> Result<Self, Self::Error> {
        validate_text(&value.client_id, 255)?;
        validate_strings(&value.scopes, 32, 128)?;
        if value.client_authentication != ProviderClientAuthentication::ClientSecretBasic
            || !value.scopes.iter().any(|scope| scope == "openid")
            || value.signing_algorithms.is_empty()
            || value.signing_algorithms.len() > 2
            || value
                .signing_algorithms
                .iter()
                .enumerate()
                .any(|(i, algorithm)| value.signing_algorithms[..i].contains(algorithm))
        {
            return Err(AuthenticationConfigurationError(
                "OIDC requires openid, client_secret_basic and unique supported signing algorithms",
            ));
        }
        value.membership.validate(true)?;
        value.admission.validate()?;
        Ok(Self {
            issuer: value.issuer,
            client_id: value.client_id,
            client_authentication: value.client_authentication,
            scopes: value.scopes,
            signing_algorithms: value.signing_algorithms,
            membership: value.membership,
            admission: value.admission,
            retain_refresh_tokens: value.retain_refresh_tokens,
        })
    }
}
impl OidcProviderDefinition {
    pub fn retain_refresh_tokens(&self) -> bool {
        self.retain_refresh_tokens
    }
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
    pub fn signing_algorithms(&self) -> &[OidcSigningAlgorithm] {
        &self.signing_algorithms
    }
    pub fn membership(&self) -> &ProviderMembership {
        &self.membership
    }
    pub fn admission(&self) -> &ProviderAdmission {
        &self.admission
    }
    pub fn issuer(&self) -> &AuthenticationEndpoint {
        &self.issuer
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum OAuthProfile {
    Github,
    JsonUser,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum OAuthSubjectType {
    String,
    Integer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "OAuthDefinitionInput")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct OAuthProviderDefinition {
    profile: OAuthProfile,
    authorization_endpoint: AuthenticationEndpoint,
    token_endpoint: AuthenticationEndpoint,
    profile_endpoint: AuthenticationEndpoint,
    client_id: String,
    client_authentication: ProviderClientAuthentication,
    scopes: Vec<String>,
    subject_pointer: String,
    subject_type: OAuthSubjectType,
    name_pointer: String,
    membership: ProviderMembership,
    admission: ProviderAdmission,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OAuthDefinitionInput {
    profile: OAuthProfile,
    authorization_endpoint: AuthenticationEndpoint,
    token_endpoint: AuthenticationEndpoint,
    profile_endpoint: AuthenticationEndpoint,
    client_id: String,
    client_authentication: ProviderClientAuthentication,
    scopes: Vec<String>,
    subject_pointer: String,
    subject_type: OAuthSubjectType,
    name_pointer: String,
    membership: ProviderMembership,
    admission: ProviderAdmission,
}
impl TryFrom<OAuthDefinitionInput> for OAuthProviderDefinition {
    type Error = AuthenticationConfigurationError;
    fn try_from(value: OAuthDefinitionInput) -> Result<Self, Self::Error> {
        validate_text(&value.client_id, 255)?;
        validate_strings(&value.scopes, 32, 128)?;
        validate_pointer(&value.subject_pointer)?;
        validate_pointer(&value.name_pointer)?;
        value.membership.validate(false)?;
        value.admission.validate()?;
        if value.profile == OAuthProfile::Github
            && (value.authorization_endpoint.as_str() != "https://github.com/login/oauth/authorize"
                || value.token_endpoint.as_str() != "https://github.com/login/oauth/access_token"
                || value.profile_endpoint.as_str() != "https://api.github.com/user"
                || value.subject_pointer != "/id"
                || value.subject_type != OAuthSubjectType::Integer
                || value.client_authentication != ProviderClientAuthentication::ClientSecretPost)
        {
            return Err(AuthenticationConfigurationError(
                "GitHub profile requires its pinned endpoints, integer /id and client_secret_post",
            ));
        }
        Ok(Self {
            profile: value.profile,
            authorization_endpoint: value.authorization_endpoint,
            token_endpoint: value.token_endpoint,
            profile_endpoint: value.profile_endpoint,
            client_id: value.client_id,
            client_authentication: value.client_authentication,
            scopes: value.scopes,
            subject_pointer: value.subject_pointer,
            subject_type: value.subject_type,
            name_pointer: value.name_pointer,
            membership: value.membership,
            admission: value.admission,
        })
    }
}
impl OAuthProviderDefinition {
    pub fn subject_pointer(&self) -> &str {
        &self.subject_pointer
    }
    pub fn subject_type(&self) -> &OAuthSubjectType {
        &self.subject_type
    }
    pub fn name_pointer(&self) -> &str {
        &self.name_pointer
    }
    pub fn membership(&self) -> &ProviderMembership {
        &self.membership
    }
    pub fn admission(&self) -> &ProviderAdmission {
        &self.admission
    }
    pub fn endpoints(&self) -> [&AuthenticationEndpoint; 3] {
        [
            &self.authorization_endpoint,
            &self.token_endpoint,
            &self.profile_endpoint,
        ]
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn identity_namespace(&self) -> String {
        // Encode component boundaries so delimiters in a URL path or JSON
        // Pointer cannot make distinct upstream identities share a binding.
        serde_json::to_string(&(
            self.profile_endpoint.as_str(),
            &self.subject_pointer,
            &self.subject_type,
        ))
        .expect("OAuth identity components are serializable")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AuthorizationConnectionInput")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuthorizationConnection {
    url: AuthenticationEndpoint,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    trust_profile_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationConnectionInput {
    url: AuthenticationEndpoint,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    trust_profile_id: String,
}
impl TryFrom<AuthorizationConnectionInput> for AuthorizationConnection {
    type Error = AuthenticationConfigurationError;
    fn try_from(value: AuthorizationConnectionInput) -> Result<Self, Self::Error> {
        validate_text(&value.trust_profile_id, 64)?;
        if value.connect_timeout_ms == 0
            || value.connect_timeout_ms > value.request_timeout_ms
            || value.request_timeout_ms > 60_000
        {
            return Err(AuthenticationConfigurationError(
                "Connection timeouts must satisfy 1 <= connect <= request <= 60000 milliseconds",
            ));
        }
        Ok(Self {
            url: value.url,
            connect_timeout_ms: value.connect_timeout_ms,
            request_timeout_ms: value.request_timeout_ms,
            trust_profile_id: value.trust_profile_id,
        })
    }
}
impl AuthorizationConnection {
    pub fn url(&self) -> &AuthenticationEndpoint {
        &self.url
    }
    pub const fn connect_timeout_ms(&self) -> u64 {
        self.connect_timeout_ms
    }
    pub const fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }
    pub fn trust_profile_id(&self) -> &str {
        &self.trust_profile_id
    }
}

fn validate_text(value: &str, maximum: usize) -> Result<(), AuthenticationConfigurationError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(AuthenticationConfigurationError(
            "Provider value is empty, too long or contains control characters",
        ));
    }
    Ok(())
}
fn validate_strings(
    values: &[String],
    count: usize,
    maximum: usize,
) -> Result<(), AuthenticationConfigurationError> {
    if values.len() > count {
        return Err(AuthenticationConfigurationError(
            "Provider list exceeds its limit",
        ));
    }
    for (index, value) in values.iter().enumerate() {
        validate_text(value, maximum)?;
        if values[..index].contains(value) {
            return Err(AuthenticationConfigurationError(
                "Provider list contains duplicate values",
            ));
        }
    }
    Ok(())
}
fn validate_pointer(value: &str) -> Result<(), AuthenticationConfigurationError> {
    validate_text(value, 255)?;
    if !value.starts_with('/') || value.split('/').count() > 17 {
        return Err(AuthenticationConfigurationError(
            "Provider field must be a bounded JSON Pointer",
        ));
    }
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
            return Err(AuthenticationConfigurationError(
                "Invalid JSON Pointer escape",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
