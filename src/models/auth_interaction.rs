//! Public v1 interaction DTOs and redacted redemption capabilities.
pub mod browser;
use std::fmt;

use chrono::{DateTime, Utc};
use hubuum_domain::{InteractionClientId, InteractionDigest, InteractionId, InteractionProviderId};
use ring::hkdf::{HKDF_SHA256, Salt};
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
    /// Login, credential approval or supported candidate-provider verification.
    pub purpose: AuthenticationPurpose,
    /// Poll directly, or use a deployment-registered BFF return target.
    pub delivery: InteractionDelivery,
    /// Optional hosted password flow. Defaults to direct server-to-server credentials.
    #[serde(default)]
    pub interaction_mode: PasswordInteractionMode,
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

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub enum InteractionCompleted {
    Session(super::auth_session::SessionResponse),
    Approval(super::credential_approval::CredentialApprovalResponse),
    Verification(super::security_candidate::SecurityCandidateLdapVerificationResponse),
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
    /// Key operation content with the private capability, so stored digests
    /// cannot be used to guess passwords embedded in credential operations.
    pub(crate) fn bind_operation(
        &self,
        value: &impl Serialize,
    ) -> Result<InteractionDigest, ApiError> {
        use hmac::{Hmac, KeyInit, Mac};
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_bytes()).expect("HMAC key");
        mac.update(b"hubuum:interaction:operation:v1:");
        mac.update(&serde_json::to_vec(value)?);
        InteractionDigest::new(hex(&mac.finalize().into_bytes()))
            .map_err(|_| ApiError::InternalServerError("Could not bind approval operation".into()))
    }
    /// Derive independent keys/capabilities with RFC 5869 HKDF-SHA-256.
    /// The public salt identifies this protocol; the info identifies its purpose.
    fn derive(&self, purpose: &[u8]) -> [u8; 32] {
        let salt = Salt::new(HKDF_SHA256, b"hubuum:interaction:derivation:v1:");
        let prk = salt.extract(self.0.as_bytes());
        let mut output = [0; 32];
        prk.expand(&[purpose], HKDF_SHA256)
            .expect("32 bytes is a valid HKDF-SHA-256 output length")
            .fill(&mut output)
            .expect("output length matches HKDF-SHA-256");
        output
    }
    fn operation_key(&self) -> hubuum_secrets::SecretValue {
        hubuum_secrets::SecretValue::new(self.derive(b"operation-encryption").to_vec())
            .expect("32-byte key")
    }
    pub(crate) fn seal_operation(
        &self,
        id: InteractionId,
        operation: &CredentialOperation,
    ) -> Result<hubuum_domain::InteractionSealedOperation, ApiError> {
        let plaintext = hubuum_secrets::SecretValue::new(serde_json::to_vec(operation)?)
            .map_err(|_| InteractionError::Invalid)?;
        let envelope = crate::runtime_settings_secrets::encrypt(
            "interaction".into(),
            "hkdf-sha256-v1".into(),
            &self.operation_key(),
            &id.value().to_string(),
            &plaintext,
        )
        .map_err(|_| {
            ApiError::InternalServerError("Could not protect approval operation".into())
        })?;
        serde_json::to_string(&envelope)?
            .try_into()
            .map_err(|_| ApiError::BadRequest("Approval operation is too large".into()))
    }
    pub(crate) fn open_operation(
        &self,
        binding: &hubuum_domain::InteractionBinding,
    ) -> Result<CredentialOperation, ApiError> {
        let envelope = serde_json::from_str(
            binding
                .sealed_operation()
                .ok_or(InteractionError::Invalid)?
                .as_str(),
        )
        .map_err(|_| InteractionError::Invalid)?;
        let plaintext = crate::runtime_settings_secrets::decrypt(
            &envelope,
            &self.operation_key(),
            &binding.id().value().to_string(),
        )
        .map_err(|_| InteractionError::Invalid)?;
        serde_json::from_slice(plaintext.expose()).map_err(|_| InteractionError::Invalid.into())
    }
    pub(crate) fn verification_receipt(
        &self,
    ) -> super::security_candidate::SecurityVerificationReceipt {
        format!("hcv1.{}", hex(&self.derive(b"verification-receipt")))
            .try_into()
            .expect("32-byte receipt")
    }
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
    Browser {
        verification_uri: String,
        user_code: String,
    },
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
    pub browser_available: bool,
}
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PasswordProviderKind {
    Local,
    Ldap,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PasswordInteractionMode {
    #[default]
    Credentials,
    Browser,
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

    // Fixed vectors computed independently with RFC 5869 extract/expand. Keep
    // derivations stable across replicas and restarts, and separate by purpose.
    #[rstest]
    #[case::encryption(
        b"operation-encryption",
        "206a36308591d96fc08213c0fbaae4981851ba34d3c14ac7c04b4d73c6ef953b"
    )]
    #[case::receipt(
        b"verification-receipt",
        "ca42fe00f1aac16429ed31e157f375f228c111577c492c2986a904869fd0dbb3"
    )]
    fn capability_derivation_matches_hkdf_vectors(#[case] purpose: &[u8], #[case] expected: &str) {
        let secret = InteractionSecret::parse(&format!("hai1.{}", "0".repeat(64))).unwrap();
        assert_eq!(hex(&secret.derive(purpose)), expected);
    }

    #[test]
    fn operation_digest_is_bound_to_private_redemption_capability() {
        let operation = serde_json::json!({"kind":"update_user", "password":"low-entropy"});
        assert_ne!(
            InteractionSecret::generate()
                .bind_operation(&operation)
                .unwrap(),
            InteractionSecret::generate()
                .bind_operation(&operation)
                .unwrap()
        );
    }

    #[rstest]
    #[case::unchanged("none", true)]
    #[case::wrong_capability("secret", false)]
    #[case::wrong_interaction("interaction", false)]
    #[case::tampered_ciphertext("ciphertext", false)]
    fn encrypted_operation_requires_original_capability_and_context(
        #[case] change: &str,
        #[case] accepted: bool,
    ) {
        use hubuum_domain::{
            InteractionActor, InteractionAuthority, InteractionBinding, PrincipalId,
            ResourceRevision, TokenId,
        };
        let secret = InteractionSecret::generate();
        let id = InteractionId::new(uuid::Uuid::new_v4()).unwrap();
        let operation: CredentialOperation = serde_json::from_value(serde_json::json!({"kind":"create_user","user":{"name":"human", "password":"never-retain-plaintext"}})).unwrap();
        let mut sealed = secret.seal_operation(id, &operation).unwrap();
        assert!(!sealed.as_str().contains("never-retain-plaintext"));
        if change == "ciphertext" {
            let mut envelope: serde_json::Value = serde_json::from_str(sealed.as_str()).unwrap();
            let ciphertext = envelope["ciphertext"].as_str().unwrap();
            let first = if ciphertext.starts_with('A') {
                "B"
            } else {
                "A"
            };
            envelope["ciphertext"] = format!("{first}{}", &ciphertext[1..]).into();
            sealed = envelope.to_string().try_into().unwrap();
        }
        let id = if change == "interaction" {
            InteractionId::new(uuid::Uuid::new_v4()).unwrap()
        } else {
            id
        };
        let binding = InteractionBinding::try_new(
            id,
            InteractionProviderId::local(),
            InteractionClientId::new("client").unwrap(),
            ResourceRevision::new(1).unwrap(),
            InteractionAuthority::approval(
                InteractionActor::new(PrincipalId::new(1).unwrap(), TokenId::new(1).unwrap()),
                secret.bind_operation(&operation).unwrap(),
            ),
        )
        .unwrap()
        .with_operation(sealed)
        .unwrap();
        let opener = if change == "secret" {
            InteractionSecret::generate()
        } else {
            secret
        };
        assert_eq!(opener.open_operation(&binding).is_ok(), accepted);
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
