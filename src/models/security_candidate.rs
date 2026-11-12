//! Non-activating security configuration requests and sanitized verification reports.
use chrono::{DateTime, Utc};
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::{
    StorageCredentialFingerprint, StorageSecurityCandidate, StorageSecurityCandidateState,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::models::runtime_settings::{
    RuntimeAuthenticationProviderRequest, RuntimeAuthorizationBackend, RuntimeSettingsGroup,
};

#[derive(Deserialize, ToSchema)]
#[serde(tag = "group", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecurityCandidateRequest {
    Authentication {
        expected_revision: ResourceRevision,
        #[serde(default)]
        providers: Vec<SecurityCandidateProviderRequest>,
        #[serde(default)]
        release_ownership: bool,
    },
    Authorization {
        expected_revision: ResourceRevision,
        backend: RuntimeAuthorizationBackend,
        source: SecurityConfigurationSource,
        connection: Option<hubuum_domain::AuthorizationConnection>,
        #[serde(default)]
        release_ownership: bool,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SecurityConfigurationSource {
    Deployment,
    Runtime,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(try_from = "String")]
#[schema(value_type = String, pattern = "^hcv1\\.[0-9a-f]{64}$")]
pub struct SecurityVerificationReceipt(String);

impl std::fmt::Debug for SecurityVerificationReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl TryFrom<String> for SecurityVerificationReceipt {
    type Error = ApiError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 69
            || !value.starts_with("hcv1.")
            || !value.as_bytes()[5..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(ApiError::Forbidden(
                "Invalid security verification receipt".into(),
            ));
        }
        Ok(Self(value))
    }
}

impl SecurityVerificationReceipt {
    pub(crate) fn generate() -> Self {
        let bytes: [u8; 32] = rand::random();
        Self(format!("hcv1.{}", hex(&bytes)))
    }
    pub(crate) fn digest(&self) -> StorageCredentialFingerprint {
        StorageCredentialFingerprint::new(hex(&Sha256::digest(self.0.as_bytes())))
            .expect("SHA-256 digest")
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SecurityCandidateActivationRequest {
    pub expected_revision: ResourceRevision,
    pub verification_receipt: SecurityVerificationReceipt,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SecurityCandidateLdapVerification {
    pub provider_id: Uuid,
    pub username: String,
    /// Used only for this test; never stored or logged.
    #[schema(write_only)]
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateCheck {
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<Uuid>,
    pub reason: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateProvider {
    pub provider_id: Uuid,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateReport {
    pub id: Uuid,
    pub group: RuntimeSettingsGroup,
    pub base_revision: ResourceRevision,
    pub expires_at: DateTime<Utc>,
    pub checks: Vec<SecurityCandidateCheck>,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_receipt: Option<SecurityVerificationReceipt>,
    pub providers: Vec<SecurityCandidateProvider>,
}

impl SecurityCandidateReport {
    pub(crate) fn new(
        candidate: &StorageSecurityCandidate,
        receipt: Option<SecurityVerificationReceipt>,
    ) -> Self {
        use hubuum_storage_core::StorageRuntimeSettingsValue;
        let providers = match candidate.verification_assignment().value() {
            Some(StorageRuntimeSettingsValue::Authentication(selection)) => selection
                .providers()
                .iter()
                .filter_map(|provider| {
                    provider.id().map(|id| SecurityCandidateProvider {
                        provider_id: id,
                        name: provider.name().into(),
                        kind: provider.kind().as_str().into(),
                    })
                })
                .collect(),
            _ => Vec::new(),
        };
        Self {
            id: candidate.id(),
            group: candidate.assignment().group().into(),
            base_revision: candidate.base_revision(),
            expires_at: candidate.expires_at(),
            checks: candidate
                .checks()
                .iter()
                .map(|check| SecurityCandidateCheck {
                    kind: serde_json::to_value(check.kind())
                        .expect("check kind")
                        .as_str()
                        .expect("string")
                        .into(),
                    status: serde_json::to_value(check.status())
                        .expect("check status")
                        .as_str()
                        .expect("string")
                        .into(),
                    provider_id: check.provider_id(),
                    reason: check.reason().into(),
                })
                .collect(),
            state: match candidate.state(Utc::now()) {
                StorageSecurityCandidateState::Unverified => "unverified",
                StorageSecurityCandidateState::Verified => "verified",
                StorageSecurityCandidateState::Failed => "failed",
                StorageSecurityCandidateState::Expired => "expired",
                StorageSecurityCandidateState::Activated => "activated",
            }
            .into(),
            verification_receipt: receipt,
            providers,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateActivated {
    pub group: RuntimeSettingsGroup,
    pub revision: ResourceRevision,
    pub changed: bool,
}

#[derive(Deserialize, ToSchema)]
#[serde(untagged)]
pub enum SecurityCandidateProviderRequest {
    Ldap(RuntimeAuthenticationProviderRequest),
    Browser(Box<BrowserProviderRequest>),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BrowserProviderKind {
    Oidc,
    Oauth,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BrowserProviderRequest {
    pub provider_id: Option<Uuid>,
    pub name: String,
    pub enabled: bool,
    pub source: SecurityConfigurationSource,
    pub kind: BrowserProviderKind,
    #[serde(default)]
    pub adopt_deployment: bool,
    /// Validated OIDC or OAuth definition and a write-only credential action.
    pub configuration: Option<BrowserProviderConfiguration>,
}

#[derive(ToSchema)]
#[serde(untagged)]
pub enum BrowserProviderConfiguration {
    Oidc {
        #[serde(flatten)]
        definition: hubuum_domain::OidcProviderDefinition,
        credential: ProviderCredentialInput,
    },
    Oauth {
        #[serde(flatten)]
        definition: hubuum_domain::OAuthProviderDefinition,
        credential: ProviderCredentialInput,
    },
}

impl<'de> Deserialize<'de> for BrowserProviderConfiguration {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Deserialize the remaining definition directly: flattening a validated
        // domain type would otherwise discard fields its constructor must reject.
        let mut fields = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        let credential = fields
            .remove("credential")
            .ok_or_else(|| serde::de::Error::missing_field("credential"))?;
        let credential = serde_json::from_value(credential).map_err(serde::de::Error::custom)?;
        let definition = serde_json::Value::Object(fields);
        if definition.get("issuer").is_some() {
            Ok(Self::Oidc {
                definition: serde_json::from_value(definition).map_err(serde::de::Error::custom)?,
                credential,
            })
        } else {
            Ok(Self::Oauth {
                definition: serde_json::from_value(definition).map_err(serde::de::Error::custom)?,
                credential,
            })
        }
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderCredentialInput {
    Keep,
    Clear,
    Reference {
        alias: String,
    },
    Replace {
        #[schema(write_only)]
        value: String,
    },
}

impl SecurityCandidateRequest {
    pub(crate) fn revision_and_group(&self) -> (ResourceRevision, RuntimeSettingsGroup) {
        match self {
            Self::Authentication {
                expected_revision, ..
            } => (*expected_revision, RuntimeSettingsGroup::Authentication),
            Self::Authorization {
                expected_revision, ..
            } => (*expected_revision, RuntimeSettingsGroup::Authorization),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateGroupPreview {
    pub key: String,
    pub name: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateVerificationPreview {
    pub candidate_id: Uuid,
    pub provider_id: Uuid,
    pub subject: String,
    pub admitted: bool,
    pub membership: String,
    pub groups: Vec<SecurityCandidateGroupPreview>,
    pub issued_session: bool,
    pub provisioned: bool,
}

impl SecurityCandidateVerificationPreview {
    pub(crate) fn from_ldap(
        candidate_id: Uuid,
        provider_id: Uuid,
        identity: hubuum_auth_core::AuthenticatedExternalUser,
    ) -> Result<Self, ApiError> {
        let bounded = |value: &str| {
            !value.is_empty() && value.len() <= 255 && !value.chars().any(char::is_control)
        };
        if !bounded(&identity.profile.subject)
            || identity.groups.len() > 1000
            || identity
                .groups
                .iter()
                .any(|group| !bounded(&group.key) || !bounded(&group.name))
        {
            return Err(ApiError::ServiceUnavailable(
                "LDAP identity or group preview exceeds supported bounds".into(),
            ));
        }
        Ok(Self {
            candidate_id,
            provider_id,
            subject: identity.profile.subject,
            admitted: true,
            membership: "complete".into(),
            groups: identity
                .groups
                .into_iter()
                .map(|group| SecurityCandidateGroupPreview {
                    key: group.key,
                    name: group.name,
                })
                .collect(),
            issued_session: false,
            provisioned: false,
        })
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecurityCandidateLdapVerificationResponse {
    pub candidate: SecurityCandidateReport,
    pub verification: SecurityCandidateVerificationPreview,
}
