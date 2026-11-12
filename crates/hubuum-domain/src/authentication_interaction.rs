//! Correlation values shared by browser adapters and application interactions.
//!
//! These types validate protocol bindings. They do not persist transactions,
//! choose configuration, provision identities, or issue Hubuum credentials.
use std::fmt;

use crate::{PrincipalId, ResourceRevision, TokenId};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use uuid::Uuid;

mod browser;
pub use browser::{
    InteractionBrowserPrompt, InteractionReturnTarget, InteractionSealedOperation,
    InteractionVerificationDetails,
};

mod lifecycle;
pub use lifecycle::{
    DEFAULT_INTERACTION_LIFETIME_SECONDS, INITIAL_INTERACTION_POLL_SECONDS,
    INTERACTION_AUTHENTICATION_CLAIM_SECONDS, InteractionAuthenticationClaim,
    InteractionBrowserBinding, InteractionIssuedResult, InteractionLifecycle, InteractionMode,
    InteractionState, InteractionTransitionError, MAX_INTERACTION_LIFETIME_SECONDS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionPurpose {
    Login,
    CredentialApproval,
    ProviderVerification,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InteractionValidationError {
    #[error("interaction identifier must not be nil")]
    NilIdentifier,
    #[error("invalid interaction client identifier")]
    InvalidClient,
    #[error("interaction provider must be local or an external provider UUID")]
    InvalidProvider,
    #[error("interaction purpose does not match its actor, operation or candidate")]
    InvalidPurpose,
    #[error("interaction digest must contain 64 lowercase hexadecimal characters")]
    InvalidDigest,
    #[error("external identity subject must contain 1-255 non-control characters")]
    InvalidSubject,
    #[error("proof belongs to another interaction or provider revision")]
    BindingMismatch,
    #[error("invalid browser return target or nonce")]
    InvalidReturnTarget,
    #[error("invalid browser prompt")]
    InvalidPrompt,
}

/// A public identifier is not an authentication capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct InteractionId(Uuid);

impl InteractionId {
    pub fn new(value: Uuid) -> Result<Self, InteractionValidationError> {
        if value.is_nil() {
            return Err(InteractionValidationError::NilIdentifier);
        }
        Ok(Self(value))
    }
    pub const fn value(self) -> Uuid {
        self.0
    }
}
impl TryFrom<Uuid> for InteractionId {
    type Error = InteractionValidationError;
    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<InteractionId> for Uuid {
    fn from(value: InteractionId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InteractionClientId(String);

impl InteractionClientId {
    pub fn new(value: impl Into<String>) -> Result<Self, InteractionValidationError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._:".contains(&b))
        {
            return Err(InteractionValidationError::InvalidClient);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for InteractionClientId {
    type Error = InteractionValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<InteractionClientId> for String {
    fn from(value: InteractionClientId) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InteractionProviderId(Provider);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    Local,
    External(Uuid),
}

impl InteractionProviderId {
    pub const fn local() -> Self {
        Self(Provider::Local)
    }
    pub fn external(value: Uuid) -> Result<Self, InteractionValidationError> {
        if value.is_nil() {
            return Err(InteractionValidationError::InvalidProvider);
        }
        Ok(Self(Provider::External(value)))
    }
    pub const fn external_id(self) -> Option<Uuid> {
        match self.0 {
            Provider::Local => None,
            Provider::External(value) => Some(value),
        }
    }
}
impl TryFrom<String> for InteractionProviderId {
    type Error = InteractionValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "local" {
            return Ok(Self::local());
        }
        let id =
            Uuid::parse_str(&value).map_err(|_| InteractionValidationError::InvalidProvider)?;
        if value != id.to_string() {
            return Err(InteractionValidationError::InvalidProvider);
        }
        Self::external(id)
    }
}
impl From<InteractionProviderId> for String {
    fn from(value: InteractionProviderId) -> Self {
        match value.0 {
            Provider::Local => "local".into(),
            Provider::External(id) => id.to_string(),
        }
    }
}

/// A digest of an operation, secret or configuration, never its plaintext.
#[derive(Clone, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InteractionDigest(String);

// Keep content-independent comparisons at the type boundary, so every adapter
// checks redemption, browser, operation and verification digests consistently.
impl PartialEq for InteractionDigest {
    fn eq(&self, other: &Self) -> bool {
        bool::from(self.0.as_bytes().ct_eq(other.0.as_bytes()))
    }
}

impl InteractionDigest {
    pub fn new(value: impl Into<String>) -> Result<Self, InteractionValidationError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(InteractionValidationError::InvalidDigest);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for InteractionDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InteractionDigest(<redacted>)")
    }
}
impl TryFrom<String> for InteractionDigest {
    type Error = InteractionValidationError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<InteractionDigest> for String {
    fn from(value: InteractionDigest) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionActor {
    principal_id: PrincipalId,
    token_id: TokenId,
}
impl InteractionActor {
    pub const fn new(principal_id: PrincipalId, token_id: TokenId) -> Self {
        Self {
            principal_id,
            token_id,
        }
    }
    pub const fn principal_id(self) -> PrincipalId {
        self.principal_id
    }
    pub const fn token_id(self) -> TokenId {
        self.token_id
    }
}

/// Purpose-specific authority cannot be attached to an ordinary login.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AuthorityDocument", into = "AuthorityDocument")]
pub struct InteractionAuthority(AuthorityDocument);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "purpose", rename_all = "snake_case", deny_unknown_fields)]
enum AuthorityDocument {
    Login {},
    CredentialApproval {
        actor: InteractionActor,
        operation: InteractionDigest,
    },
    ProviderVerification {
        actor: InteractionActor,
        candidate_id: Uuid,
        candidate: InteractionDigest,
    },
}

impl InteractionAuthority {
    pub const fn login() -> Self {
        Self(AuthorityDocument::Login {})
    }
    pub const fn approval(actor: InteractionActor, operation: InteractionDigest) -> Self {
        Self(AuthorityDocument::CredentialApproval { actor, operation })
    }
    pub fn verification(
        actor: InteractionActor,
        candidate_id: Uuid,
        candidate: InteractionDigest,
    ) -> Result<Self, InteractionValidationError> {
        if candidate_id.is_nil() {
            return Err(InteractionValidationError::InvalidPurpose);
        }
        Ok(Self(AuthorityDocument::ProviderVerification {
            actor,
            candidate_id,
            candidate,
        }))
    }
    pub const fn purpose(&self) -> InteractionPurpose {
        match self.0 {
            AuthorityDocument::Login {} => InteractionPurpose::Login,
            AuthorityDocument::CredentialApproval { .. } => InteractionPurpose::CredentialApproval,
            AuthorityDocument::ProviderVerification { .. } => {
                InteractionPurpose::ProviderVerification
            }
        }
    }
    pub const fn actor(&self) -> Option<InteractionActor> {
        match self.0 {
            AuthorityDocument::Login {} => None,
            AuthorityDocument::CredentialApproval { actor, .. }
            | AuthorityDocument::ProviderVerification { actor, .. } => Some(actor),
        }
    }
    pub const fn candidate_id(&self) -> Option<Uuid> {
        match self.0 {
            AuthorityDocument::ProviderVerification { candidate_id, .. } => Some(candidate_id),
            _ => None,
        }
    }
    pub const fn operation_digest(&self) -> Option<&InteractionDigest> {
        match &self.0 {
            AuthorityDocument::CredentialApproval { operation, .. } => Some(operation),
            _ => None,
        }
    }
    pub const fn candidate_digest(&self) -> Option<&InteractionDigest> {
        match &self.0 {
            AuthorityDocument::ProviderVerification { candidate, .. } => Some(candidate),
            _ => None,
        }
    }
}
impl TryFrom<AuthorityDocument> for InteractionAuthority {
    type Error = InteractionValidationError;
    fn try_from(value: AuthorityDocument) -> Result<Self, Self::Error> {
        match value {
            AuthorityDocument::Login {} => Ok(Self::login()),
            AuthorityDocument::CredentialApproval { actor, operation } => {
                Ok(Self::approval(actor, operation))
            }
            AuthorityDocument::ProviderVerification {
                actor,
                candidate_id,
                candidate,
            } => Self::verification(actor, candidate_id, candidate),
        }
    }
}
impl From<InteractionAuthority> for AuthorityDocument {
    fn from(value: InteractionAuthority) -> Self {
        value.0
    }
}

/// The complete immutable correlation context passed to a provider adapter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BindingDocument", into = "BindingDocument")]
pub struct InteractionBinding {
    id: InteractionId,
    provider: InteractionProviderId,
    client: InteractionClientId,
    revision: ResourceRevision,
    authority: InteractionAuthority,
    return_target: Option<Box<InteractionReturnTarget>>,
    verification_receipt: Option<InteractionDigest>,
    prompt: Option<Box<InteractionBrowserPrompt>>,
    sealed_operation: Option<InteractionSealedOperation>,
}
impl InteractionBinding {
    pub fn try_new(
        id: InteractionId,
        provider: InteractionProviderId,
        client: InteractionClientId,
        revision: ResourceRevision,
        authority: InteractionAuthority,
    ) -> Result<Self, InteractionValidationError> {
        if authority.purpose() == InteractionPurpose::ProviderVerification
            && provider.external_id().is_none()
        {
            return Err(InteractionValidationError::InvalidPurpose);
        }
        Ok(Self {
            id,
            provider,
            client,
            revision,
            authority,
            return_target: None,
            verification_receipt: None,
            prompt: None,
            sealed_operation: None,
        })
    }
    pub const fn id(&self) -> InteractionId {
        self.id
    }
    pub const fn provider(&self) -> InteractionProviderId {
        self.provider
    }
    pub const fn client(&self) -> &InteractionClientId {
        &self.client
    }
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
    pub const fn authority(&self) -> &InteractionAuthority {
        &self.authority
    }
    pub fn with_operation(
        mut self,
        operation: InteractionSealedOperation,
    ) -> Result<Self, InteractionValidationError> {
        if self.authority.purpose() != InteractionPurpose::CredentialApproval {
            return Err(InteractionValidationError::InvalidPurpose);
        }
        self.sealed_operation = Some(operation);
        Ok(self)
    }
    pub const fn sealed_operation(&self) -> Option<&InteractionSealedOperation> {
        self.sealed_operation.as_ref()
    }
    pub fn with_prompt(mut self, prompt: InteractionBrowserPrompt) -> Self {
        self.prompt = Some(Box::new(prompt));
        self
    }
    pub fn prompt(&self) -> Option<&InteractionBrowserPrompt> {
        self.prompt.as_deref()
    }
    pub fn with_return_target(mut self, target: InteractionReturnTarget) -> Self {
        self.return_target = Some(Box::new(target));
        self
    }
    pub fn return_target(&self) -> Option<&InteractionReturnTarget> {
        self.return_target.as_deref()
    }
    pub fn with_verification_receipt(
        mut self,
        receipt: InteractionDigest,
    ) -> Result<Self, InteractionValidationError> {
        if self.authority.purpose() != InteractionPurpose::ProviderVerification {
            return Err(InteractionValidationError::InvalidPurpose);
        }
        self.verification_receipt = Some(receipt);
        Ok(self)
    }
    pub const fn verification_receipt(&self) -> Option<&InteractionDigest> {
        self.verification_receipt.as_ref()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingDocument {
    id: InteractionId,
    provider: InteractionProviderId,
    client: InteractionClientId,
    revision: ResourceRevision,
    authority: InteractionAuthority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    return_target: Option<InteractionReturnTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verification_receipt: Option<InteractionDigest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prompt: Option<InteractionBrowserPrompt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sealed_operation: Option<InteractionSealedOperation>,
}
impl TryFrom<BindingDocument> for InteractionBinding {
    type Error = InteractionValidationError;
    fn try_from(value: BindingDocument) -> Result<Self, Self::Error> {
        let mut binding = Self::try_new(
            value.id,
            value.provider,
            value.client,
            value.revision,
            value.authority,
        )?;
        binding.return_target = value.return_target.map(Box::new);
        binding.prompt = value.prompt.map(Box::new);
        if let Some(operation) = value.sealed_operation {
            binding = binding.with_operation(operation)?;
        }
        if let Some(receipt) = value.verification_receipt {
            binding = binding.with_verification_receipt(receipt)?;
        }
        Ok(binding)
    }
}
impl From<InteractionBinding> for BindingDocument {
    fn from(value: InteractionBinding) -> Self {
        Self {
            id: value.id,
            provider: value.provider,
            client: value.client,
            revision: value.revision,
            authority: value.authority,
            return_target: value.return_target.map(|value| *value),
            verification_receipt: value.verification_receipt,
            prompt: value.prompt.map(|value| *value),
            sealed_operation: value.sealed_operation,
        }
    }
}

/// Provider-validated subject evidence. It cannot be deserialized from an HTTP body.
/// Protocol adapters must validate their protocol before constructing this proof.
#[derive(Clone, PartialEq, Eq)]
pub struct VerifiedInteractionSubject {
    binding: InteractionBinding,
    subject: String,
    verification: Option<InteractionVerificationDetails>,
}
impl VerifiedInteractionSubject {
    pub fn new(
        binding: InteractionBinding,
        subject: impl Into<String>,
    ) -> Result<Self, InteractionValidationError> {
        let subject = subject.into();
        if subject.is_empty() || subject.len() > 255 || subject.chars().any(char::is_control) {
            return Err(InteractionValidationError::InvalidSubject);
        }
        Ok(Self {
            binding,
            subject,
            verification: None,
        })
    }
    pub fn with_verification(
        mut self,
        details: Option<InteractionVerificationDetails>,
    ) -> Result<Self, InteractionValidationError> {
        if details.is_some()
            && self.binding.authority().purpose() != InteractionPurpose::ProviderVerification
        {
            return Err(InteractionValidationError::InvalidPurpose);
        }
        self.verification = details;
        Ok(self)
    }
    pub const fn verification(&self) -> Option<&InteractionVerificationDetails> {
        self.verification.as_ref()
    }
    pub const fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub fn subject(&self) -> &str {
        &self.subject
    }
    pub fn require_binding(
        &self,
        binding: &InteractionBinding,
    ) -> Result<(), InteractionValidationError> {
        if &self.binding != binding {
            return Err(InteractionValidationError::BindingMismatch);
        }
        Ok(())
    }
}
impl fmt::Debug for VerifiedInteractionSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedInteractionSubject")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
