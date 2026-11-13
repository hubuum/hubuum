use super::*;

/// Application-encrypted protocol data and the digest of its callback state.
/// Storage can bind and atomically consume it without knowing any protocol or key.
#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "ProtectedStateInput")]
pub struct StorageProtectedAuthenticationState {
    state: InteractionDigest,
    ciphertext: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtectedStateInput {
    state: InteractionDigest,
    ciphertext: String,
}

impl TryFrom<ProtectedStateInput> for StorageProtectedAuthenticationState {
    type Error = StorageValidationError;
    fn try_from(value: ProtectedStateInput) -> Result<Self, Self::Error> {
        Self::new(value.state, value.ciphertext)
    }
}

impl StorageProtectedAuthenticationState {
    pub fn new(
        state: InteractionDigest,
        ciphertext: String,
    ) -> Result<Self, StorageValidationError> {
        if ciphertext.is_empty() || ciphertext.len() > 16 * 1024 {
            return Err(invalid());
        }
        Ok(Self { state, ciphertext })
    }
    pub fn ciphertext(&self) -> &str {
        &self.ciphertext
    }
    pub(super) fn state(&self) -> &InteractionDigest {
        &self.state
    }
}
impl std::fmt::Debug for StorageProtectedAuthenticationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StorageProtectedAuthenticationState(<redacted>)")
    }
}

/// The callback's state has been consumed under the browser and revision fence.
/// Any crash after this point leaves a bounded authentication lease, never a
/// reusable authorization code or pending redirect.
#[derive(Clone, Debug)]
pub struct StorageProtocolClaim {
    pub(super) claim: InteractionAuthenticationClaim,
    pub(super) lifecycle: InteractionLifecycle,
    pub(super) protocol: StorageProtectedAuthenticationState,
}
impl StorageProtocolClaim {
    pub fn claim(&self) -> &InteractionAuthenticationClaim {
        &self.claim
    }
    pub fn lifecycle(&self) -> &InteractionLifecycle {
        &self.lifecycle
    }
    pub fn protocol(&self) -> &StorageProtectedAuthenticationState {
        &self.protocol
    }
}
