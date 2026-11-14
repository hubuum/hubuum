//! One retained exact copy under authenticated recovery evidence. This carries
//! no selection authority and cannot expand to a synthesized descendant.
use super::*;
use crate::StorageRecordIdentity;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct HistoryExpiryReapplication {
    protection: HistoryExposureProtection,
    batch: Uuid,
    target: StorageRecordIdentity,
}
impl HistoryExpiryReapplication {
    pub fn new(
        protection: HistoryExposureProtection,
        batch: Uuid,
        target: StorageRecordIdentity,
    ) -> Result<Self, StorageValidationError> {
        let intent = protection
            .authority()
            .expiry()
            .intent(batch)
            .ok_or_else(|| invalid("Recovery evidence does not retain this expiry batch"))?;
        if !intent
            .manifest()
            .members()
            .iter()
            .any(|member| member.target() == target)
        {
            return Err(invalid(
                "Recovery target is outside retained exact expiry membership",
            ));
        }
        Ok(Self {
            protection,
            batch,
            target,
        })
    }
    pub const fn protection(&self) -> &HistoryExposureProtection {
        &self.protection
    }
    pub const fn batch(&self) -> Uuid {
        self.batch
    }
    pub const fn target(&self) -> StorageRecordIdentity {
        self.target
    }
    pub fn intent(&self) -> &HistoryExpiryIntent {
        self.protection
            .authority()
            .expiry()
            .intent(self.batch)
            .expect("validated recovery batch")
    }
    pub fn member(&self) -> &HistoryExpiryMember {
        self.intent()
            .manifest()
            .members()
            .iter()
            .find(|member| member.target() == self.target)
            .expect("validated recovery target")
    }
    pub fn has_receipt(&self) -> bool {
        self.intent()
            .removals()
            .iter()
            .any(|receipt| receipt.target() == self.target)
    }
    pub fn validate(
        &self,
        snapshot: &HistoryLifecycleSnapshot,
    ) -> Result<(), StorageValidationError> {
        if self.protection.authority() != snapshot.control().authority()
            || self.protection.fence().incarnation() != snapshot.fence().incarnation()
            || self.protection.fence().revision() as u64
                != snapshot.fence().deletion_revision().value()
        {
            return Err(invalid(
                "Protected recovery authority or application image changed",
            ));
        }
        Ok(())
    }
}
