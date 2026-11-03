//! Retained exact-version obligations. A receipt describes a committed outcome;
//! neither these constructors nor cancellation grant physical cleanup authority.

use uuid::Uuid;

use super::*;
use crate::{DeletionCheckpointIdentity, StorageRecordIdentity};

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryExpiryRemovalReceipt {
    target: StorageRecordIdentity,
    #[cfg_attr(feature = "openapi", schema(value_type = String, pattern = "^[0-9a-f]{64}$"))]
    checkpoint: DeletionCheckpointIdentity,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    removed_at: DateTime<Utc>,
}
impl HistoryExpiryRemovalReceipt {
    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub const fn checkpoint(&self) -> &DeletionCheckpointIdentity {
        &self.checkpoint
    }
    pub const fn removed_at(&self) -> DateTime<Utc> {
        self.removed_at
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryExpiryCancellation {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    principal: PrincipalIdentity,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    requested_at: DateTime<Utc>,
}
impl HistoryExpiryCancellation {
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn requested_at(&self) -> DateTime<Utc> {
        self.requested_at
    }
}

/// Retain membership after cancellation, changed policy, holds and physical
/// completion. Restored descendants remain excluded by immutable UUIDs.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "IntentInput")]
pub struct HistoryExpiryIntent {
    manifest: HistoryExpiryManifest,
    cancellation: Option<HistoryExpiryCancellation>,
    removals: Vec<HistoryExpiryRemovalReceipt>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentInput {
    manifest: HistoryExpiryManifest,
    cancellation: Option<HistoryExpiryCancellation>,
    removals: Vec<HistoryExpiryRemovalReceipt>,
}
impl TryFrom<IntentInput> for HistoryExpiryIntent {
    type Error = StorageValidationError;
    fn try_from(value: IntentInput) -> Result<Self, Self::Error> {
        if value.removals.len() > value.manifest.members().len()
            || value.cancellation.as_ref().is_some_and(|cancellation| {
                cancellation.requested_at() < value.manifest.selected_at()
            })
        {
            return Err(invalid(
                "Invalid retained expiry outcome count or cancellation clock",
            ));
        }
        let mut previous = None;
        for receipt in &value.removals {
            let identity = (
                *receipt.target().resource().as_bytes(),
                *receipt.target().version().as_bytes(),
            );
            if previous.is_some_and(|old| old >= identity)
                || receipt.removed_at() < value.manifest.selected_at()
                || !value
                    .manifest
                    .members()
                    .iter()
                    .any(|member| member.target() == receipt.target())
            {
                return Err(invalid(
                    "Retained removal changed membership, ordering or original clock",
                ));
            }
            previous = Some(identity);
        }
        Ok(Self {
            manifest: value.manifest,
            cancellation: value.cancellation,
            removals: value.removals,
        })
    }
}
impl HistoryExpiryIntent {
    pub fn new(manifest: HistoryExpiryManifest) -> Self {
        Self {
            manifest,
            cancellation: None,
            removals: Vec::new(),
        }
    }
    pub const fn identity(&self) -> Uuid {
        self.manifest.identity()
    }
    pub const fn manifest(&self) -> &HistoryExpiryManifest {
        &self.manifest
    }
    pub const fn cancellation(&self) -> Option<&HistoryExpiryCancellation> {
        self.cancellation.as_ref()
    }
    pub fn removals(&self) -> &[HistoryExpiryRemovalReceipt] {
        &self.removals
    }
    pub fn is_complete(&self) -> bool {
        self.removals.len() == self.manifest.members().len()
    }
    pub fn excludes(
        &self,
        family: StorageIdentityHistoryKind,
        target: StorageRecordIdentity,
    ) -> bool {
        self.manifest
            .members()
            .iter()
            .any(|member| member.family() == family && member.target() == target)
    }
    pub fn request_cancellation(
        &self,
        principal: PrincipalIdentity,
        at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if at < self.manifest.selected_at() {
            return Err(invalid("Cancellation cannot precede expiry selection"));
        }
        let mut next = self.clone();
        if next.cancellation.is_none() {
            next.cancellation = Some(HistoryExpiryCancellation {
                principal,
                requested_at: at,
            });
        }
        Ok(next)
    }
    /// Called only as part of the adapter transaction that removes or verifies
    /// absence of the exact copy under independently protected intent authority.
    /// Reapplication preserves the first outcome, checkpoint and occurrence time.
    pub fn record_removed(
        &self,
        target: StorageRecordIdentity,
        checkpoint: DeletionCheckpointIdentity,
        at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if !self
            .manifest
            .members()
            .iter()
            .any(|member| member.target() == target)
            || at < self.manifest.selected_at()
        {
            return Err(invalid(
                "Removal differs from retained exact expiry membership",
            ));
        }
        let mut next = self.clone();
        if !next.removals.iter().any(|receipt| receipt.target == target) {
            next.removals.push(HistoryExpiryRemovalReceipt {
                target,
                checkpoint,
                removed_at: at,
            });
            next.removals.sort_by_key(|receipt| {
                (
                    *receipt.target.resource().as_bytes(),
                    *receipt.target.version().as_bytes(),
                )
            });
        }
        Ok(next)
    }
    pub fn covers(&self, required: &Self) -> bool {
        self.manifest == required.manifest
            && required
                .cancellation
                .as_ref()
                .is_none_or(|cancellation| self.cancellation.as_ref() == Some(cancellation))
            && required
                .removals
                .iter()
                .all(|receipt| self.removals.contains(receipt))
    }
    /// Merge compatible descendants without losing newer local outcomes. Two
    /// contradictory original receipts or cancellation facts fail closed.
    pub fn merge(&self, required: &Self) -> Result<Self, StorageValidationError> {
        if self.manifest != required.manifest
            || matches!((&self.cancellation, &required.cancellation), (Some(local), Some(other)) if local != other)
            || self.removals.iter().any(|local| {
                required
                    .removals
                    .iter()
                    .any(|other| local.target == other.target && local != other)
            })
        {
            return Err(invalid(
                "Recovered expiry intent conflicts with retained descendants",
            ));
        }
        let mut merged = self.clone();
        merged.cancellation = self
            .cancellation
            .clone()
            .or_else(|| required.cancellation.clone());
        for receipt in &required.removals {
            if !merged.removals.contains(receipt) {
                merged.removals.push(receipt.clone());
            }
        }
        merged.removals.sort_by_key(|receipt| {
            (
                *receipt.target.resource().as_bytes(),
                *receipt.target.version().as_bytes(),
            )
        });
        Ok(merged)
    }
}
