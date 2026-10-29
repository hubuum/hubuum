use std::sync::{Arc, Mutex};

use hubuum_storage_core::ControlKeyFingerprint;

use super::{Payload, unavailable};
use crate::errors::ApiError;

/// One immutable authenticated decoding, scoped to a configured receipt service.
/// This never caches file availability, HEAD, a durable proof, or a lock. Readers
/// must independently check the complete signed file before consulting it.
#[derive(Default)]
pub(in crate::services::deletion_recovery) struct AuthenticatedPayloadCache {
    entry: Mutex<Option<Entry>>,
}

struct Entry {
    signed_sha256: String,
    key_fingerprint: ControlKeyFingerprint,
    payload: Arc<Payload>,
}

impl AuthenticatedPayloadCache {
    pub(super) fn get(
        &self,
        signed_sha256: &str,
        key_fingerprint: &ControlKeyFingerprint,
    ) -> Result<Option<Arc<Payload>>, ApiError> {
        let entry = self
            .entry
            .lock()
            .map_err(|_| unavailable("Authenticated checkpoint cache is unavailable"))?;
        Ok(entry.as_ref().and_then(|entry| {
            (entry.signed_sha256 == signed_sha256 && &entry.key_fingerprint == key_fingerprint)
                .then(|| Arc::clone(&entry.payload))
        }))
    }

    pub(super) fn remember(
        &self,
        signed_sha256: String,
        key_fingerprint: ControlKeyFingerprint,
        payload: Arc<Payload>,
    ) -> Result<(), ApiError> {
        let mut entry = self
            .entry
            .lock()
            .map_err(|_| unavailable("Authenticated checkpoint cache is unavailable"))?;
        *entry = Some(Entry {
            signed_sha256,
            key_fingerprint,
            payload,
        });
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
