//! Fresh-authentication requests and operation-bound approval secrets.
use crate::errors::ApiError;
use crate::models::{
    ImportRequest, NewUser, PrincipalID, PrincipalTokenCreateRequest, RestoreConfirmRequest,
    RestoreJobID, TokenID, TokenScopeDetails, UpdateUser, UserID,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use hubuum_storage_core::{
    StorageCredentialApprovalMetadata, StorageCredentialFingerprint, StorageCredentialOperation,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

pub const APPROVAL_HEADER: &str = "x-hubuum-credential-approval";
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewTokenRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    /// Requested expiry. It must be in the future and no farther from issuance
    /// than the server's public maximum token lifetime. When omitted, the
    /// server applies the public default lifetime.
    pub expires_at: Option<chrono::NaiveDateTime>,
    /// Optional permission and resource boundaries. Omit or send `null` for an
    /// unscoped token.
    pub scope: Option<TokenScopeDetails>,
}

impl NewTokenRequest {
    pub(crate) fn into_create_request(
        self,
        principal_id: PrincipalID,
    ) -> Result<PrincipalTokenCreateRequest, ApiError> {
        let scope = self
            .scope
            .map(TokenScopeDetails::into_request_scope)
            .transpose()?;
        Ok(PrincipalTokenCreateRequest::new(principal_id)
            .name(self.name)
            .description(self.description)
            .expires_at(self.expires_at)
            .scope(scope))
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RenewTokenRequest {
    /// Optional expiry for the new token. When omitted, the server applies its
    /// public default token lifetime. The source token's expiry is never
    /// copied.
    pub expires_at: Option<chrono::NaiveDateTime>,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialOperation {
    ImportCredentials {
        import: Box<ImportRequest>,
    },
    ConfirmRestore {
        restore_id: RestoreJobID,
        confirmation: RestoreConfirmRequest,
    },
    CreateToken {
        principal_id: PrincipalID,
        token: NewTokenRequest,
    },
    RenewToken {
        principal_id: PrincipalID,
        token_id: TokenID,
        token: RenewTokenRequest,
    },
    CreateUser {
        user: NewUser,
    },
    UpdateUser {
        user_id: UserID,
        user: UpdateUser,
    },
}
impl std::fmt::Debug for CredentialOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.kind().as_str())
    }
}
impl CredentialOperation {
    pub fn kind(&self) -> StorageCredentialOperation {
        match self {
            Self::ImportCredentials { .. } => StorageCredentialOperation::ImportCredentials,
            Self::ConfirmRestore { .. } => StorageCredentialOperation::ConfirmRestore,
            Self::CreateToken { .. } => StorageCredentialOperation::CreateToken,
            Self::RenewToken { .. } => StorageCredentialOperation::RenewToken,
            Self::CreateUser { .. } => StorageCredentialOperation::CreateUser,
            Self::UpdateUser { .. } => StorageCredentialOperation::UpdateUser,
        }
    }
    pub fn target(&self) -> Option<PrincipalID> {
        match self {
            Self::CreateToken { principal_id, .. } | Self::RenewToken { principal_id, .. } => {
                Some(*principal_id)
            }
            Self::UpdateUser { user_id, .. } => {
                Some(PrincipalID::new(user_id.id()).expect("validated user ID"))
            }
            Self::CreateUser { .. }
            | Self::ImportCredentials { .. }
            | Self::ConfirmRestore { .. } => None,
        }
    }
    pub fn token_expiry(&self) -> Option<NaiveDateTime> {
        match self {
            Self::CreateToken { token, .. } => token.expires_at,
            Self::RenewToken { token, .. } => token.expires_at,
            _ => None,
        }
    }
    pub fn resolve_defaults(&mut self) -> Result<(), ApiError> {
        let expiry = match self {
            Self::CreateToken { token, .. } => &mut token.expires_at,
            Self::RenewToken { token, .. } => &mut token.expires_at,
            _ => return Ok(()),
        };
        let policy = crate::models::token::configured_token_issuance_policy()?;
        *expiry = Some(policy.resolve_expiry(Utc::now().naive_utc(), *expiry)?);
        Ok(())
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CredentialApprovalRequest {
    /// The acting human's current password. Never retained or logged.
    pub password: String,
    pub operation: CredentialOperation,
}
impl std::fmt::Debug for CredentialApprovalRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialApprovalRequest")
            .field("password", &"<redacted>")
            .field("operation", &self.operation)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(try_from = "String")]
#[schema(value_type = String, pattern = "^hca1\\.[0-9a-f]{64}$")]
pub struct CredentialApprovalSecret(String);
impl std::fmt::Debug for CredentialApprovalSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}
impl TryFrom<String> for CredentialApprovalSecret {
    type Error = ApiError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 69
            || !value.starts_with("hca1.")
            || !value.as_bytes()[5..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(ApiError::ReauthenticationRequired);
        }
        Ok(Self(value))
    }
}
impl CredentialApprovalSecret {
    pub fn generate() -> Self {
        let bytes: [u8; 32] = rand::random();
        Self(format!("hca1.{}", hex(&bytes)))
    }
    pub fn digest(&self) -> StorageCredentialFingerprint {
        StorageCredentialFingerprint::new(hex(&Sha256::digest(self.0.as_bytes())))
            .expect("SHA-256 hex is valid")
    }
    pub fn bind(
        &self,
        operation: &CredentialOperation,
    ) -> Result<StorageCredentialFingerprint, ApiError> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(self.0.as_bytes()).expect("HMAC accepts any key length");
        mac.update(b"hubuum:credential-operation:v1:");
        let bytes = serde_json::to_vec(operation).map_err(|_| {
            ApiError::InternalServerError("Could not bind credential operation".into())
        })?;
        mac.update(&bytes);
        Ok(
            StorageCredentialFingerprint::new(hex(&mac.finalize().into_bytes()))
                .expect("SHA-256 hex is valid"),
        )
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CredentialApprovalRecord {
    pub id: i32,
    pub actor_id: PrincipalID,
    pub token_id: TokenID,
    pub operation: String,
    pub target_id: Option<PrincipalID>,
    pub restore_job_id: Option<RestoreJobID>,
    pub invalidated_at: Option<DateTime<Utc>>,
    pub authenticated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}
impl From<StorageCredentialApprovalMetadata> for CredentialApprovalRecord {
    fn from(m: StorageCredentialApprovalMetadata) -> Self {
        Self {
            id: m.id(),
            actor_id: m.actor_id(),
            token_id: m.token_id(),
            operation: m.operation().as_str().to_string(),
            target_id: m.target_id(),
            restore_job_id: m.restore_job_id(),
            invalidated_at: m.invalidated_at(),
            authenticated_at: m.authenticated_at(),
            expires_at: m.expires_at(),
            consumed_at: m.consumed_at(),
        }
    }
}
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CredentialApprovalResponse {
    pub approval: CredentialApprovalSecret,
    pub record: CredentialApprovalRecord,
    /// Use this exact value as token.expires_at in the subsequent creation/renewal request.
    pub token_expires_at: Option<NaiveDateTime>,
}
