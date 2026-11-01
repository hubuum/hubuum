use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use hubuum_storage_core::ControlKeyFingerprint;

use super::{Payload, unavailable};
use crate::errors::ApiError;

// Publication retains the current checkpoint and its predecessor, then collects
// the preceding checkpoint. Bound decoding reuse to that same three-file window.
const MAX_ENTRIES: usize = 3;

/// Bounded immutable authenticated decodings, scoped to a configured receipt service.
/// This never caches file availability, HEAD, a durable proof, or a lock. Readers
/// must independently check the complete signed file before consulting it.
#[derive(Default)]
pub(in crate::services::deletion_recovery) struct AuthenticatedPayloadCache {
    entries: Mutex<VecDeque<Entry>>,
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
        let entries = self
            .entries
            .lock()
            .map_err(|_| unavailable("Authenticated checkpoint cache is unavailable"))?;
        Ok(entries.iter().find_map(|entry| {
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
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| unavailable("Authenticated checkpoint cache is unavailable"))?;
        entries.retain(|entry| {
            entry.signed_sha256 != signed_sha256 || entry.key_fingerprint != key_fingerprint
        });
        if entries.len() == MAX_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(Entry {
            signed_sha256,
            key_fingerprint,
            payload,
        });
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
