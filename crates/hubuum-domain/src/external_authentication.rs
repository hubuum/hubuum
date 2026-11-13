//! Validated browser-provider facts, not a protocol verifier. Adapters must
//! authenticate the upstream response (including signature, audience, nonce and
//! UserInfo subject checks) before calling this boundary. Nothing here accepts a
//! client-supplied token or profile as authentication. Proofs are not deserializable.
use std::{collections::BTreeSet, fmt};

use crate::{
    IdentityScopeName, InteractionBinding, InteractionProviderId, InteractionPurpose,
    MembershipClaimSource, MembershipKeyMode, OAuthProviderDefinition, OAuthSubjectType,
    OidcProviderDefinition, ProviderAdmission, ProviderMembership,
};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

use crate::ExternalFreshnessPolicy;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExternalAuthenticationError {
    #[error("external subject must be nonempty, at most 255 bytes and free of control characters")]
    InvalidSubject,
    #[error("OAuth subject does not have the configured exact string or integer type")]
    InvalidOAuthSubject,
    #[error("external authentication requires an external provider and a non-local scope")]
    InvalidProvider,
    #[error("authentication evidence does not match the provider identity or interaction")]
    IdentityMismatch,
    #[error(
        "optional provider profile values must be nonempty, bounded and free of control characters"
    )]
    InvalidProfile,
    #[error("required membership claim is absent")]
    MissingMemberships,
    #[error("membership claim must be a bounded array of unique valid group strings")]
    MalformedMemberships,
    #[error("provider membership result is incomplete")]
    IncompleteMemberships,
    #[error("membership evidence does not come from the configured source")]
    MembershipSourceMismatch,
    #[error("admission policy does not permit this identity")]
    AdmissionDenied,
    #[error("automatic account linking is unsupported")]
    LinkingUnsupported,
    #[error("only a login interaction can admit an external identity")]
    InvalidAdmissionPurpose,
    #[error("membership freshness requires 1 <= interval <= 300 and interval <= max_stale <= 900")]
    InvalidFreshnessPolicy,
    #[error("provider cannot prove fresh authentication for credential approval")]
    CapabilityUnsupported,
    #[error("fresh authentication requires an operation-bound approval interaction")]
    InvalidPurpose,
    #[error("fresh authentication time is missing, stale or outside the permitted clock skew")]
    InvalidAuthenticationTime,
}

/// An opaque subject. Preserve case and exact bytes; never use email or username.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExternalSubject(String);

impl ExternalSubject {
    pub fn new(value: impl Into<String>) -> Result<Self, ExternalAuthenticationError> {
        let value = value.into();
        if !valid_text(&value, 255) {
            return Err(ExternalAuthenticationError::InvalidSubject);
        }
        Ok(Self(value))
    }

    /// Integer subjects are parsed losslessly. Decimal/exponent/floating-point
    /// encodings are rejected rather than rounded, even when numerically integral.
    pub fn from_oauth_value(
        value: &Value,
        subject_type: OAuthSubjectType,
    ) -> Result<Self, ExternalAuthenticationError> {
        let subject = match (subject_type, value) {
            (OAuthSubjectType::String, Value::String(value)) => value.clone(),
            (OAuthSubjectType::Integer, Value::Number(value)) => {
                let text = value.to_string();
                let digits = text.strip_prefix('-').unwrap_or(&text);
                if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(ExternalAuthenticationError::InvalidOAuthSubject);
                }
                // Canonical decimal, including arbitrary-precision integers.
                if text == "-0" { "0".to_owned() } else { text }
            }
            _ => return Err(ExternalAuthenticationError::InvalidOAuthSubject),
        };
        Self::new(subject)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ExternalSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExternalSubject(<redacted>)")
    }
}

/// Optional mutable profile metadata; none of these fields can link an account.
#[derive(Clone, PartialEq, Eq)]
pub struct ExternalIdentityProfile {
    subject: ExternalSubject,
    name: Option<String>,
    email: Option<String>,
}

impl ExternalIdentityProfile {
    pub fn new(
        subject: ExternalSubject,
        name: Option<String>,
        email: Option<String>,
    ) -> Result<Self, ExternalAuthenticationError> {
        if name.as_ref().is_some_and(|name| !valid_text(name, 255))
            || email.as_ref().is_some_and(|email| !valid_text(email, 512))
        {
            return Err(ExternalAuthenticationError::InvalidProfile);
        }
        Ok(Self {
            subject,
            name,
            email,
        })
    }
    pub fn subject(&self) -> &ExternalSubject {
        &self.subject
    }
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }
}

impl fmt::Debug for ExternalIdentityProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExternalIdentityProfile(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
enum Namespace {
    Oidc(String),
    OAuth(String),
}

/// Stable identity including provider instance, scope and immutable upstream
/// namespace. Configuration revision and mutable profile fields are not identity.
#[derive(Clone, PartialEq, Eq)]
pub struct ExternalIdentity {
    provider: InteractionProviderId,
    scope: IdentityScopeName,
    namespace: Namespace,
    subject: ExternalSubject,
}

impl ExternalIdentity {
    pub const fn provider(&self) -> InteractionProviderId {
        self.provider
    }
    pub fn scope(&self) -> &IdentityScopeName {
        &self.scope
    }
    pub fn subject(&self) -> &ExternalSubject {
        &self.subject
    }
    pub fn provider_kind(&self) -> &'static str {
        match self.namespace {
            Namespace::Oidc(_) => "oidc",
            Namespace::OAuth(_) => "oauth",
        }
    }
    pub fn namespace(&self) -> &str {
        match &self.namespace {
            Namespace::Oidc(value) | Namespace::OAuth(value) => value,
        }
    }
}

impl fmt::Debug for ExternalIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExternalIdentity(<redacted>)")
    }
}

/// The configured source's observation. An empty complete array is authoritative;
/// absent, malformed and incomplete observations cannot produce a group proof.
pub enum MembershipClaims<'a> {
    Absent,
    Malformed,
    Incomplete,
    Complete {
        source: MembershipClaimSource,
        document: &'a Value,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderGroup {
    key: String,
}

impl ProviderGroup {
    /// Full-path keys intentionally treat a path rename as removal plus addition.
    /// Immutable-ID mappings retain the same key through a display-name change.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Debug for ProviderGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderGroup(<redacted>)")
    }
}

/// `None` preserves local assignments; `Some([])` removes the owning provider's
/// grants. Groups carry no local group IDs or implicit administrator authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalMemberships {
    groups: Option<Vec<ProviderGroup>>,
}

impl ExternalMemberships {
    fn validate(
        policy: &ProviderMembership,
        claims: MembershipClaims<'_>,
    ) -> Result<Self, ExternalAuthenticationError> {
        let ProviderMembership::ProviderGroups {
            claim_source,
            claim_pointer,
            key_mode,
            ..
        } = policy
        else {
            return Ok(Self { groups: None });
        };
        let (source, document) = match claims {
            MembershipClaims::Absent => {
                return Err(ExternalAuthenticationError::MissingMemberships);
            }
            MembershipClaims::Malformed => {
                return Err(ExternalAuthenticationError::MalformedMemberships);
            }
            MembershipClaims::Incomplete => {
                return Err(ExternalAuthenticationError::IncompleteMemberships);
            }
            MembershipClaims::Complete { source, document } => (source, document),
        };
        if &source != claim_source {
            return Err(ExternalAuthenticationError::MembershipSourceMismatch);
        }
        let claim = document
            .pointer(claim_pointer)
            .ok_or(ExternalAuthenticationError::MissingMemberships)?;
        let values = claim
            .as_array()
            .filter(|values| values.len() <= 1024)
            .ok_or(ExternalAuthenticationError::MalformedMemberships)?;
        let mut keys = BTreeSet::new();
        for value in values {
            let key = value
                .as_str()
                .filter(|key| valid_text(key, 255))
                .ok_or(ExternalAuthenticationError::MalformedMemberships)?;
            if (*key_mode == MembershipKeyMode::FullPath
                && (key == "/"
                    || !key.starts_with('/')
                    || key.ends_with('/')
                    || key.contains("//")))
                || !keys.insert(key.to_owned())
            {
                return Err(ExternalAuthenticationError::MalformedMemberships);
            }
        }
        Ok(Self {
            groups: Some(keys.into_iter().map(|key| ProviderGroup { key }).collect()),
        })
    }

    pub fn provider_groups(&self) -> Option<&[ProviderGroup]> {
        self.groups.as_deref()
    }
}

/// Trusted server configuration pinned to a single interaction. Construct this
/// from the selected configuration snapshot, never from provider-returned values.
#[derive(Clone)]
pub struct ExternalAuthenticationContext {
    binding: InteractionBinding,
    scope: IdentityScopeName,
    configuration: ProviderConfiguration,
}

#[derive(Clone)]
enum ProviderConfiguration {
    Oidc(OidcProviderDefinition),
    OAuth(OAuthProviderDefinition),
}

impl ProviderConfiguration {
    fn namespace(&self) -> Namespace {
        match self {
            Self::Oidc(definition) => Namespace::Oidc(definition.issuer().as_str().to_owned()),
            Self::OAuth(definition) => Namespace::OAuth(definition.identity_namespace()),
        }
    }
    fn membership(&self) -> &ProviderMembership {
        match self {
            Self::Oidc(definition) => definition.membership(),
            Self::OAuth(definition) => definition.membership(),
        }
    }
    fn admission(&self) -> &ProviderAdmission {
        match self {
            Self::Oidc(definition) => definition.admission(),
            Self::OAuth(definition) => definition.admission(),
        }
    }
}

impl ExternalAuthenticationContext {
    pub fn oidc(
        binding: InteractionBinding,
        scope: IdentityScopeName,
        definition: &OidcProviderDefinition,
    ) -> Result<Self, ExternalAuthenticationError> {
        Self::new(
            binding,
            scope,
            ProviderConfiguration::Oidc(definition.clone()),
        )
    }

    pub fn oauth(
        binding: InteractionBinding,
        scope: IdentityScopeName,
        definition: &OAuthProviderDefinition,
    ) -> Result<Self, ExternalAuthenticationError> {
        Self::new(
            binding,
            scope,
            ProviderConfiguration::OAuth(definition.clone()),
        )
    }

    fn new(
        binding: InteractionBinding,
        scope: IdentityScopeName,
        configuration: ProviderConfiguration,
    ) -> Result<Self, ExternalAuthenticationError> {
        if binding.provider().external_id().is_none() || scope.as_str() == "local" {
            return Err(ExternalAuthenticationError::InvalidProvider);
        }
        Ok(Self {
            binding,
            scope,
            configuration,
        })
    }

    /// Validate identity data loaded from storage, without authenticating it.
    pub fn identity(
        &self,
        subject: ExternalSubject,
    ) -> Result<ExternalIdentity, ExternalAuthenticationError> {
        if matches!(self.configuration, ProviderConfiguration::Oidc(_))
            && !subject.as_str().is_ascii()
        {
            return Err(ExternalAuthenticationError::InvalidSubject);
        }
        if let ProviderConfiguration::OAuth(definition) = &self.configuration
            && definition.subject_type() == &OAuthSubjectType::Integer
        {
            let raw = subject.as_str();
            let digits = raw.strip_prefix('-').unwrap_or(raw);
            if digits.is_empty()
                || !digits.bytes().all(|byte| byte.is_ascii_digit())
                || (digits.len() > 1 && digits.starts_with('0'))
                || raw == "-0"
            {
                return Err(ExternalAuthenticationError::InvalidOAuthSubject);
            }
        }
        Ok(ExternalIdentity {
            provider: self.binding.provider(),
            scope: self.scope.clone(),
            namespace: self.configuration.namespace(),
            subject,
        })
    }

    pub fn binding(&self) -> &InteractionBinding {
        &self.binding
    }

    pub fn freshness_policy(&self) -> ExternalFreshnessPolicy {
        ExternalFreshnessPolicy::try_from(self.configuration.membership())
            .expect("validated provider membership policy")
    }

    /// Call only after the adapter validates the signed OIDC response and any
    /// subject-matched UserInfo. Issuer comparison preserves the exact spelling.
    pub fn verified_oidc(
        &self,
        issuer: &str,
        profile: ExternalIdentityProfile,
        claims: MembershipClaims<'_>,
        verified_at: DateTime<Utc>,
    ) -> Result<VerifiedExternalAuthentication, ExternalAuthenticationError> {
        if self.configuration.namespace() != Namespace::Oidc(issuer.to_owned()) {
            return Err(ExternalAuthenticationError::IdentityMismatch);
        }
        self.verified(profile, claims, verified_at)
    }

    /// Call only after server-owned exchange and authenticated-user lookup at
    /// the pinned endpoint. A supplied email/name is never used for identity.
    pub fn verified_oauth(
        &self,
        document: &Value,
        claims: MembershipClaims<'_>,
        verified_at: DateTime<Utc>,
    ) -> Result<VerifiedExternalAuthentication, ExternalAuthenticationError> {
        let ProviderConfiguration::OAuth(definition) = &self.configuration else {
            return Err(ExternalAuthenticationError::IdentityMismatch);
        };
        if !document.is_object() {
            return Err(ExternalAuthenticationError::InvalidProfile);
        }
        let subject = document
            .pointer(definition.subject_pointer())
            .ok_or(ExternalAuthenticationError::InvalidOAuthSubject)?;
        let name = match document.pointer(definition.name_pointer()) {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.clone()),
            _ => return Err(ExternalAuthenticationError::InvalidProfile),
        };
        let profile = ExternalIdentityProfile::new(
            ExternalSubject::from_oauth_value(subject, definition.subject_type().clone())?,
            name,
            None,
        )?;
        self.verified(profile, claims, verified_at)
    }

    fn verified(
        &self,
        profile: ExternalIdentityProfile,
        claims: MembershipClaims<'_>,
        verified_at: DateTime<Utc>,
    ) -> Result<VerifiedExternalAuthentication, ExternalAuthenticationError> {
        let identity = self.identity(profile.subject.clone())?;
        Ok(VerifiedExternalAuthentication {
            binding: self.binding.clone(),
            identity,
            profile,
            memberships: ExternalMemberships::validate(self.configuration.membership(), claims)?,
            admission: self.configuration.admission().clone(),
            verified_at,
        })
    }
}

/// A verified upstream identity and complete membership observation for exactly
/// one provider/revision/purpose/client interaction. Not an admission or session.
#[derive(Clone)]
pub struct VerifiedExternalAuthentication {
    binding: InteractionBinding,
    identity: ExternalIdentity,
    profile: ExternalIdentityProfile,
    memberships: ExternalMemberships,
    admission: ProviderAdmission,
    verified_at: DateTime<Utc>,
}

impl VerifiedExternalAuthentication {
    pub fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub fn identity(&self) -> &ExternalIdentity {
        &self.identity
    }
    pub fn profile(&self) -> &ExternalIdentityProfile {
        &self.profile
    }
    pub fn memberships(&self) -> &ExternalMemberships {
        &self.memberships
    }
    pub fn verified_at(&self) -> DateTime<Utc> {
        self.verified_at
    }

    /// Check inside the same transaction that resolves/provisions the principal.
    /// `existing` must come from a stable-identity lookup, never email or username.
    /// Database uniqueness and locking still serialize concurrent provisioning.
    /// Reapply on every login, including already materialized allowlisted users.
    fn check_admission(
        &self,
        existing: Option<&ExternalIdentity>,
    ) -> Result<(), ExternalAuthenticationError> {
        if existing.is_some_and(|identity| identity != &self.identity) {
            return Err(ExternalAuthenticationError::LinkingUnsupported);
        }
        match &self.admission {
            ProviderAdmission::ExistingOnly if existing.is_none() => {
                Err(ExternalAuthenticationError::AdmissionDenied)
            }
            ProviderAdmission::SubjectAllowlist { subjects }
                if !subjects
                    .iter()
                    .any(|subject| subject == self.identity.subject.as_str()) =>
            {
                Err(ExternalAuthenticationError::AdmissionDenied)
            }
            _ => Ok(()),
        }
    }

    /// Preserve the admission decision as a proof across the transaction's
    /// application/storage boundary. Provider verification and approval cannot
    /// use this path to create principals or issue a login session.
    pub fn admit(
        self,
        existing: Option<&ExternalIdentity>,
    ) -> Result<AdmittedExternalAuthentication, ExternalAuthenticationError> {
        if self.binding.authority().purpose() != InteractionPurpose::Login {
            return Err(ExternalAuthenticationError::InvalidAdmissionPurpose);
        }
        self.check_admission(existing)?;
        Ok(AdmittedExternalAuthentication {
            authentication: self,
        })
    }
}

#[derive(Debug)]
pub struct AdmittedExternalAuthentication {
    authentication: VerifiedExternalAuthentication,
}

impl AdmittedExternalAuthentication {
    pub fn authentication(&self) -> &VerifiedExternalAuthentication {
        &self.authentication
    }
}

impl fmt::Debug for VerifiedExternalAuthentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedExternalAuthentication")
            .field("identity", &self.identity)
            .field("verified_at", &self.verified_at)
            .finish_non_exhaustive()
    }
}

/// Created before redirect, from the existing actor's persisted immutable
/// identity and the operation-bound interaction. The OIDC adapter must request
/// `max_age=0`; an ordinary login/redirect cannot substitute for this request.
pub struct FreshExternalAuthenticationRequest {
    context: ExternalAuthenticationContext,
    expected_identity: ExternalIdentity,
    started_at: DateTime<Utc>,
    clock_skew: Duration,
}

impl FreshExternalAuthenticationRequest {
    pub fn new(
        context: ExternalAuthenticationContext,
        expected_identity: ExternalIdentity,
        started_at: DateTime<Utc>,
        clock_skew_seconds: u32,
    ) -> Result<Self, ExternalAuthenticationError> {
        if context.binding.authority().purpose() != InteractionPurpose::CredentialApproval {
            return Err(ExternalAuthenticationError::InvalidPurpose);
        }
        if !matches!(context.configuration, ProviderConfiguration::Oidc(_)) {
            return Err(ExternalAuthenticationError::CapabilityUnsupported);
        }
        if context.identity(expected_identity.subject.clone())? != expected_identity {
            return Err(ExternalAuthenticationError::IdentityMismatch);
        }
        if clock_skew_seconds > 60 {
            return Err(ExternalAuthenticationError::InvalidAuthenticationTime);
        }
        Ok(Self {
            context,
            expected_identity,
            started_at,
            clock_skew: Duration::seconds(i64::from(clock_skew_seconds)),
        })
    }

    pub fn context(&self) -> &ExternalAuthenticationContext {
        &self.context
    }
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    /// `signed_auth_time` must be extracted from the validated signed ID token,
    /// not `iat`, UserInfo, a client field or a Hubuum session timestamp. Profile-
    /// specific required acr/amr checks remain the OIDC adapter's responsibility.
    pub fn verify(
        self,
        authentication: VerifiedExternalAuthentication,
        signed_auth_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<FreshExternalAuthentication, ExternalAuthenticationError> {
        if authentication.binding != self.context.binding
            || authentication.identity != self.expected_identity
        {
            return Err(ExternalAuthenticationError::IdentityMismatch);
        }
        authentication.check_admission(Some(&self.expected_identity))?;
        let auth_time =
            signed_auth_time.ok_or(ExternalAuthenticationError::InvalidAuthenticationTime)?;
        if now < self.started_at
            || authentication.verified_at < self.started_at
            || authentication.verified_at > now
            || auth_time - self.started_at < -self.clock_skew
            || auth_time - authentication.verified_at > self.clock_skew
        {
            return Err(ExternalAuthenticationError::InvalidAuthenticationTime);
        }
        Ok(FreshExternalAuthentication {
            authentication,
            authenticated_at: auth_time,
            started_at: self.started_at,
        })
    }
}

/// Fresh evidence is still subject to atomic interaction consumption, origin
/// token validity, current authorization and operation checks at the final write.
#[derive(Clone, Debug)]
pub struct FreshExternalAuthentication {
    authentication: VerifiedExternalAuthentication,
    authenticated_at: DateTime<Utc>,
    started_at: DateTime<Utc>,
}

impl FreshExternalAuthentication {
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
    pub fn authentication(&self) -> &VerifiedExternalAuthentication {
        &self.authentication
    }
    pub fn authenticated_at(&self) -> DateTime<Utc> {
        self.authenticated_at
    }
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests;
