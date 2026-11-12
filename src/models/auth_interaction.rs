//! Public v1 interaction DTOs and redacted redemption capabilities.
use std::fmt;

use chrono::{DateTime, Utc};
use hubuum_domain::{InteractionClientId, InteractionDigest, InteractionId, InteractionProviderId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::errors::{ApiError, InteractionError};
use crate::models::credential_approval::CredentialOperation;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InteractionBeginRequest {
    #[schema(value_type = String, pattern = "^[A-Za-z0-9_.:-]{1,128}$")]
    pub client_id: InteractionClientId,
    #[schema(value_type = String, pattern = "^(local|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")]
    pub provider_id: InteractionProviderId,
    /// This implementation accepts login. Privileged purposes return capability_unsupported.
    pub purpose: AuthenticationPurpose,
    /// Poll is supported. BFF return targets require the later browser transport.
    pub delivery: InteractionDelivery,
    pub operation: Option<CredentialOperation>,
    pub candidate_id: Option<uuid::Uuid>,
    pub return_target_id: Option<String>,
    pub return_nonce: Option<String>,
}

impl InteractionBeginRequest {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        let fields_match = match self.purpose {
            AuthenticationPurpose::Login => self.operation.is_none() && self.candidate_id.is_none(),
            AuthenticationPurpose::CredentialApproval => {
                self.operation.is_some() && self.candidate_id.is_none()
            }
            AuthenticationPurpose::ProviderVerification => {
                self.operation.is_none() && self.candidate_id.is_some_and(|id| !id.is_nil())
            }
        };
        let delivery_matches = match self.delivery {
            InteractionDelivery::Poll => {
                self.return_target_id.is_none() && self.return_nonce.is_none()
            }
            InteractionDelivery::Bff => {
                self.return_target_id
                    .as_ref()
                    .is_some_and(|s| !s.is_empty() && s.len() <= 64)
                    && self
                        .return_nonce
                        .as_ref()
                        .is_some_and(|s| (32..=128).contains(&s.len()))
            }
        };
        if !fields_match || !delivery_matches {
            return Err(ApiError::BadRequest(
                "Interaction fields do not match the purpose and delivery mode".into(),
            ));
        }
        if self.purpose != AuthenticationPurpose::Login
            || self.delivery != InteractionDelivery::Poll
        {
            return Err(InteractionError::Unsupported.into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationPurpose {
    Login,
    CredentialApproval,
    ProviderVerification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InteractionDelivery {
    Poll,
    Bff,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InteractionClientRequest {
    #[schema(value_type = String, pattern = "^[A-Za-z0-9_.:-]{1,128}$")]
    pub client_id: InteractionClientId,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InteractionCredentialsRequest {
    #[schema(value_type = String, pattern = "^[A-Za-z0-9_.:-]{1,128}$")]
    pub client_id: InteractionClientId,
    #[schema(min_length = 1, max_length = 255)]
    pub name: String,
    #[schema(min_length = 1, max_length = 4096, write_only)]
    pub password: String,
}
impl fmt::Debug for InteractionCredentialsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InteractionCredentialsRequest")
            .field("client_id", &self.client_id)
            .field("name", &self.name)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Serialize, ToSchema)]
#[serde(transparent)]
#[schema(value_type = String, pattern = "^hai1\\.[0-9a-f]{64}$")]
pub struct InteractionSecret(String);
impl fmt::Debug for InteractionSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InteractionSecret(<redacted>)")
    }
}
impl InteractionSecret {
    pub(crate) fn generate() -> Self {
        let bytes: [u8; 32] = rand::random();
        Self(format!("hai1.{}", hex(&bytes)))
    }
    pub(crate) fn parse(value: &str) -> Result<Self, ApiError> {
        if value.len() != 69
            || !value.starts_with("hai1.")
            || !value.as_bytes()[5..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(InteractionError::Invalid.into());
        }
        Ok(Self(value.to_owned()))
    }
    pub(crate) fn digest(&self) -> InteractionDigest {
        let mut hash = Sha256::new();
        hash.update(b"hubuum:interaction:redemption:v1:");
        hash.update(self.0.as_bytes());
        InteractionDigest::new(hex(&hash.finalize())).expect("SHA-256 hex")
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InteractionBegun {
    #[schema(value_type = uuid::Uuid)]
    pub id: InteractionId,
    pub interaction_version: u8,
    pub redemption_secret: InteractionSecret,
    pub expires_at: DateTime<Utc>,
    pub interval_seconds: u32,
    pub next: InteractionNext,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InteractionNext {
    Credentials,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InteractionStatus {
    #[schema(value_type = uuid::Uuid)]
    pub id: InteractionId,
    pub state: InteractionStatusState,
    pub expires_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InteractionStatusState {
    Pending,
    Ready,
    Denied,
    Expired,
    Cancelled,
    Consumed,
    Failed,
}
impl From<hubuum_storage_core::StorageInteractionView> for InteractionStatus {
    fn from(view: hubuum_storage_core::StorageInteractionView) -> Self {
        use hubuum_domain::{InteractionIssuedResult, InteractionState};
        let state = match view.state() {
            InteractionState::Pending => InteractionStatusState::Pending,
            InteractionState::Ready => InteractionStatusState::Ready,
            InteractionState::Denied => InteractionStatusState::Denied,
            InteractionState::Expired => InteractionStatusState::Expired,
            InteractionState::Cancelled => InteractionStatusState::Cancelled,
            InteractionState::Consumed => InteractionStatusState::Consumed,
            InteractionState::Failed => InteractionStatusState::Failed,
        };
        Self {
            id: view.binding().id(),
            state,
            expires_at: view.expires_at(),
            session_id: match view.issued() {
                Some(InteractionIssuedResult::Session(id)) => Some(id),
                _ => None,
            },
            approval_id: match view.issued() {
                Some(InteractionIssuedResult::Approval(id)) => Some(id),
                _ => None,
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuthenticationMethods {
    pub interaction_version: u8,
    pub methods: Vec<AuthenticationMethod>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuthenticationMethod {
    #[schema(value_type = String)]
    pub provider_id: InteractionProviderId,
    pub name: String,
    pub kind: PasswordProviderKind,
    pub interaction: PasswordInteractionMode,
    pub fresh_authentication: bool,
}
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PasswordProviderKind {
    Local,
    Ldap,
}
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PasswordInteractionMode {
    Credentials,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn secret_debug_is_redacted_and_hash_is_domain_separated() {
        let secret = InteractionSecret::generate();
        assert!(!format!("{secret:?}").contains(&secret.0));
        assert_ne!(
            secret.digest().as_str(),
            hex(&Sha256::digest(secret.0.as_bytes()))
        );
        assert_eq!(
            InteractionSecret::parse(&secret.0).unwrap().digest(),
            secret.digest()
        );
    }

    #[rstest]
    #[case::empty("")]
    #[case::ordinary_bearer("hbt1.key.id.secret")]
    #[case::uppercase("hai1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    #[case::short("hai1.123")]
    fn rejects_noncanonical_capabilities(#[case] raw: &str) {
        assert!(InteractionSecret::parse(raw).is_err());
    }
}
