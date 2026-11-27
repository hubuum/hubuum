use std::collections::BTreeMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    HISTORY_ARCHIVE_ROW_BYTES, HistoryArchiveAuthorization, HistoryArchiveDescriptor,
    HistoryArchiveInput, HistoryArchiveRow, VerifiedPrimary, history_archive_encoded_bytes,
    invalid,
};
use crate::StorageError;

// A full primary and a full artifact fit the exact-read wire allowance for
// smaller descriptions. Only larger descriptions need an additional commitment.
const LARGE_DESCRIPTION_BYTES: usize = 1024 * 1024;
const COMMITMENT_DOMAIN: &[u8] = b"hubuum:history:collection-primary:v1\0";

/// The original closed primary was observed at capture and matched both the
/// catalogued metadata and its independently observed description digest.
/// Only the descriptor comparison below can construct this admission input.
pub struct HistoryArchiveObservedPrimary {
    pub(super) proof: VerifiedPrimary,
    pub(super) commitment: [u8; 32],
}

#[cfg(test)]
mod tests;

fn commitment(payload: &Value, description_digest: [u8; 32]) -> Result<[u8; 32], StorageError> {
    let fields = payload
        .as_object()
        .ok_or_else(|| invalid("Collection payload must be an object"))?;
    // Bind every other field, including any additional or nested evidence.
    // Borrow the values so a large description is neither copied nor encoded.
    let metadata: BTreeMap<_, _> = fields
        .iter()
        .filter(|(key, _)| *key != "description")
        .collect();
    let mut digest = Sha256::new();
    digest.update(COMMITMENT_DOMAIN);
    digest.update(description_digest);
    digest.update(history_archive_encoded_bytes(
        &metadata,
        HISTORY_ARCHIVE_ROW_BYTES,
    )?);
    Ok(digest.finalize().into())
}

impl HistoryArchiveDescriptor {
    pub(super) fn primary_commitment(
        row: &HistoryArchiveRow,
    ) -> Result<Option<[u8; 32]>, StorageError> {
        let description = row.collection().description();
        (description.len() >= LARGE_DESCRIPTION_BYTES)
            .then(|| commitment(row.payload(), Sha256::digest(description.as_bytes()).into()))
            .transpose()
    }

    pub(super) fn matches_primary_commitment(&self, payload: &Value) -> Result<bool, StorageError> {
        let Some(expected) = self.primary_commitment_v1 else {
            return Ok(true);
        };
        let description = payload["description"]
            .as_str()
            .ok_or_else(|| invalid("Captured collection description missing"))?;
        Ok(commitment(payload, Sha256::digest(description.as_bytes()).into())? == expected)
    }

    /// Adapter boundary for a single source snapshot. The projected row has an
    /// empty description. The digest covers the original UTF-8 bytes and the
    /// byte count covers their JSON encoding, excluding the two outer quotes.
    pub fn observe_primary_description(
        &self,
        projection: HistoryArchiveRow,
        description_digest: [u8; 32],
        encoded_description_bytes: usize,
    ) -> Result<HistoryArchiveInput, StorageError> {
        let expected = self
            .primary_commitment_v1
            .ok_or_else(|| invalid("Archive descriptor has no primary observation commitment"))?;
        if !projection.collection().description().is_empty() {
            return Err(invalid(
                "Primary observation must project out its description",
            ));
        }
        let authorization = HistoryArchiveAuthorization::new(
            projection.collection().id(),
            projection.identity(),
            projection.collection().name(),
        )?;
        let projected_bytes =
            history_archive_encoded_bytes(&projection, HISTORY_ARCHIVE_ROW_BYTES)?.len();
        if encoded_description_bytes > HISTORY_ARCHIVE_ROW_BYTES - projected_bytes {
            return Ok(HistoryArchiveInput::BudgetLimited(authorization));
        }
        if self.identity != projection.identity()
            || self.valid_from != projection.valid_from()
            || Some(self.valid_to) != projection.valid_to()
            || commitment(projection.payload(), description_digest)? != expected
        {
            return Ok(HistoryArchiveInput::CorruptPrimary(authorization));
        }
        Ok(HistoryArchiveInput::ObservedPrimary(
            HistoryArchiveObservedPrimary {
                commitment: expected,
                proof: VerifiedPrimary {
                    authorization,
                    valid_from: self.valid_from,
                    valid_to: self.valid_to,
                    payload_digest: self.payload_digest,
                },
            },
        ))
    }
}
