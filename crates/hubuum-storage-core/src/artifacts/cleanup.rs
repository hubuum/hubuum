//! Verify a one-attempt provider response against its pre-admission commitment.
use super::{ArtifactError, ArtifactResult};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Private, non-wire evidence. The provider releases the response only after
/// durable removal. The storage adapter must still compare the complete
/// encoded intent with the original committed claim inside settlement.
pub struct ArtifactCleanupReceipt {
    encoded_intent: Vec<u8>,
}
impl ArtifactCleanupReceipt {
    pub fn from_provider(encoded_intent: Vec<u8>, response: [u8; 32]) -> ArtifactResult<Self> {
        #[derive(Deserialize)]
        struct Envelope {
            protocol: u8,
            filesystem: Challenge,
        }
        #[derive(Deserialize)]
        struct Challenge {
            cleanup_commitment: [u8; 32],
        }
        let invalid = || ArtifactError::Invalid("Unverified artifact cleanup response".into());
        if encoded_intent.len() > 256 * 1024 || response == [0; 32] {
            return Err(invalid());
        }
        let envelope: Envelope = serde_json::from_slice(&encoded_intent).map_err(|_| invalid())?;
        if envelope.protocol != 2
            || envelope.filesystem.cleanup_commitment != <[u8; 32]>::from(Sha256::digest(response))
        {
            return Err(invalid());
        }
        Ok(Self { encoded_intent })
    }

    pub fn encoded_intent(&self) -> &[u8] {
        &self.encoded_intent
    }
}
