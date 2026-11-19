//! Shared authorization-code correlation and hardened protocol transport.
use hubuum_domain::InteractionId;
use oauth2::{CsrfToken, PkceCodeChallenge, PkceCodeVerifier};
use serde::{Deserialize, Serialize};

pub mod profile;
mod transport;
pub use transport::Transport;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    #[error("Authentication provider configuration is unsupported")]
    Configuration,
    #[error("Authentication provider is temporarily unavailable")]
    Unavailable,
    #[error("Authentication response failed validation")]
    InvalidResponse,
    #[error("Authentication was denied or revoked")]
    Denied,
    #[error("Authenticated subject failed membership validation")]
    MembershipRejected(Box<hubuum_domain::VerifiedMembershipRejection>),
}

impl From<hubuum_domain::ExternalAuthenticationError> for ProtocolError {
    fn from(error: hubuum_domain::ExternalAuthenticationError) -> Self {
        match error {
            hubuum_domain::ExternalAuthenticationError::MembershipRejected(evidence) => {
                Self::MembershipRejected(evidence)
            }
            _ => Self::Denied,
        }
    }
}

/// Keep only in encrypted, interaction-bound storage. Storage additionally
/// enforces browser binding, provider/revision, expiry and one-use claiming.
#[derive(Serialize, Deserialize)]
pub struct CodeState {
    state: String,
    verifier: String,
}

impl CodeState {
    pub fn new(interaction: InteractionId) -> Self {
        let (_, verifier) = PkceCodeChallenge::new_random_sha256();
        Self {
            state: format!(
                "{}.{}",
                interaction.value(),
                CsrfToken::new_random().secret()
            ),
            verifier: verifier.secret().to_owned(),
        }
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn verifier(&self) -> &str {
        &self.verifier
    }

    /// Intentional protocol-library integration surface shared by both adapters.
    pub fn challenge(&self) -> PkceCodeChallenge {
        PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(self.verifier.clone()))
    }

    pub fn accept(
        &self,
        state: &str,
        code: Option<&str>,
        error: Option<&str>,
    ) -> Result<String, ProtocolError> {
        if CsrfToken::new(self.state.clone()) != CsrfToken::new(state.to_owned()) || error.is_some()
        {
            return Err(ProtocolError::Denied);
        }
        code.filter(|code| {
            !code.is_empty() && code.len() <= 4096 && !code.chars().any(char::is_control)
        })
        .map(ToOwned::to_owned)
        .ok_or(ProtocolError::Denied)
    }
}
