use std::fmt;

use hubuum_domain::MaintenanceState;

use super::*;

/// Identity of the stable, owner-controlled process-exclusion file. This value
/// does not itself prove that the caller holds its shared or exclusive lock.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "IsolationInput")]
pub struct RuntimeIsolationDomain {
    nonce: Uuid,
    device: u64,
    inode: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IsolationInput {
    nonce: Uuid,
    device: u64,
    inode: u64,
}

impl RuntimeIsolationDomain {
    pub fn new(nonce: Uuid, device: u64, inode: u64) -> Result<Self, StorageValidationError> {
        if nonce.is_nil() || inode == 0 {
            return Err(invalid("Invalid runtime isolation domain"));
        }
        Ok(Self {
            nonce,
            device,
            inode,
        })
    }
    pub const fn nonce(&self) -> Uuid {
        self.nonce
    }
    pub const fn device(&self) -> u64 {
        self.device
    }
    pub const fn inode(&self) -> u64 {
        self.inode
    }
}

impl TryFrom<IsolationInput> for RuntimeIsolationDomain {
    type Error = StorageValidationError;
    fn try_from(value: IsolationInput) -> Result<Self, Self::Error> {
        Self::new(value.nonce, value.device, value.inode)
    }
}

/// Cloneable execution identity derived from an owned registration. Registration
/// removal still consumes its separate non-cloneable shutdown capability.
#[derive(Clone, Eq, PartialEq)]
pub struct RuntimeOwner {
    id: Uuid,
    ownership_token: Uuid,
}

impl RuntimeOwner {
    pub(super) const fn new(id: Uuid, ownership_token: Uuid) -> Self {
        Self {
            id,
            ownership_token,
        }
    }
    pub const fn id(&self) -> Uuid {
        self.id
    }
    /// Adapter comparison material; never log it or expose it in responses.
    pub const fn ownership_token(&self) -> Uuid {
        self.ownership_token
    }
}

impl fmt::Debug for RuntimeOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeOwner")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// The current application image and exact deletion-authority revision.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "FenceInput")]
pub struct ApplicationFence {
    incarnation: Uuid,
    revision: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceInput {
    incarnation: Uuid,
    revision: i64,
}

impl ApplicationFence {
    pub fn new(incarnation: Uuid, revision: i64) -> Result<Self, StorageValidationError> {
        if incarnation.is_nil() || revision < 0 {
            return Err(invalid(
                "Invalid application incarnation or authority revision",
            ));
        }
        Ok(Self {
            incarnation,
            revision,
        })
    }
    pub const fn incarnation(self) -> Uuid {
        self.incarnation
    }
    pub const fn revision(self) -> i64 {
        self.revision
    }
}

impl TryFrom<FenceInput> for ApplicationFence {
    type Error = StorageValidationError;
    fn try_from(value: FenceInput) -> Result<Self, Self::Error> {
        Self::new(value.incarnation, value.revision)
    }
}

/// An adapter-admitted unit of work. It cannot be deserialized from requests.
/// Every database/state-lock entry verifies the owner and fence again.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageExecutionPermit {
    owner: Option<RuntimeOwner>,
    fence: ApplicationFence,
}

impl StorageExecutionPermit {
    /// Adapter boundary: construct only after validating live ownership and
    /// released application state under the authority lock.
    pub const fn new(owner: Option<RuntimeOwner>, fence: ApplicationFence) -> Self {
        Self { owner, fence }
    }
    pub const fn owner(&self) -> Option<&RuntimeOwner> {
        self.owner.as_ref()
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
}

/// Admission and maintenance observed together under the adapter's authority fence.
/// The permit remains the original producer identity through all subsequent work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationWorkAdmission {
    permit: StorageExecutionPermit,
    maintenance: MaintenanceState,
}

impl ApplicationWorkAdmission {
    /// Adapter boundary after validating ownership and the current or inherited permit.
    pub const fn new(permit: StorageExecutionPermit, maintenance: MaintenanceState) -> Self {
        Self {
            permit,
            maintenance,
        }
    }
    pub fn into_parts(self) -> (StorageExecutionPermit, MaintenanceState) {
        (self.permit, self.maintenance)
    }
}

/// A committed reapplication of every supported retained decision to this
/// application image. Independent publication is required before release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ApplicationReapplication {
    fence: ApplicationFence,
}

impl ApplicationReapplication {
    /// Adapter boundary after transactional exact-copy verification/removal.
    pub const fn new(fence: ApplicationFence) -> Self {
        Self { fence }
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub const fn revision(&self) -> i64 {
        self.fence.revision
    }
    pub const fn incarnation(&self) -> Uuid {
        self.fence.incarnation
    }
}

/// Primary-local exposure state, deliberately separate from recoverable
/// authority. Importing a checkpoint never imports this release decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationControlState {
    fence: ApplicationFence,
    reapplication: Option<ApplicationReapplication>,
    released_revision: Option<i64>,
    checkpoint: Option<DeletionCheckpointIdentity>,
}

impl ApplicationControlState {
    pub fn new(
        fence: ApplicationFence,
        reapplication: Option<ApplicationReapplication>,
        released_revision: Option<i64>,
        checkpoint: Option<DeletionCheckpointIdentity>,
    ) -> Result<Self, StorageValidationError> {
        if reapplication.as_ref().is_some_and(|receipt| {
            receipt.incarnation() != fence.incarnation || receipt.revision() > fence.revision
        }) || released_revision.is_some_and(|revision| revision < 0 || revision > fence.revision)
            || (checkpoint.is_some()
                && (released_revision.is_none()
                    || reapplication
                        .as_ref()
                        .map(ApplicationReapplication::revision)
                        != released_revision))
        {
            return Err(invalid(
                "Invalid application reapplication or release state",
            ));
        }
        Ok(Self {
            fence,
            reapplication,
            released_revision,
            checkpoint,
        })
    }
    pub fn initial(authority: &DeletionControlState) -> Self {
        Self {
            fence: ApplicationFence {
                incarnation: Uuid::new_v4(),
                revision: authority.revision(),
            },
            reapplication: None,
            released_revision: authority
                .intents()
                .is_empty()
                .then_some(authority.revision()),
            checkpoint: None,
        }
    }
    pub const fn quarantined(fence: ApplicationFence) -> Self {
        Self {
            fence,
            reapplication: None,
            released_revision: None,
            checkpoint: None,
        }
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub const fn reapplication(&self) -> Option<&ApplicationReapplication> {
        self.reapplication.as_ref()
    }
    pub const fn released_revision(&self) -> Option<i64> {
        self.released_revision
    }
    pub const fn checkpoint(&self) -> Option<&DeletionCheckpointIdentity> {
        self.checkpoint.as_ref()
    }
    pub fn is_released_for(&self, authority: &DeletionControlState) -> bool {
        self.fence.revision == authority.revision()
            && self.released_revision == Some(authority.revision())
            && ((authority.intents().is_empty() && self.checkpoint.is_none())
                || (self.checkpoint.is_some()
                    && self
                        .reapplication
                        .as_ref()
                        .is_some_and(|receipt| receipt.fence() == self.fence)))
    }
    pub fn record_reapplication(
        &self,
        receipt: ApplicationReapplication,
    ) -> Result<Self, StorageValidationError> {
        if receipt.incarnation() != self.fence.incarnation
            || receipt.revision() < self.fence.revision
        {
            return Err(invalid(
                "Reapplication belongs to a different application or older authority",
            ));
        }
        Ok(Self {
            fence: receipt.fence(),
            reapplication: Some(receipt),
            released_revision: None,
            checkpoint: None,
        })
    }
    pub fn release(
        &self,
        receipt: &ApplicationReapplication,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<Self, StorageValidationError> {
        if self.reapplication.as_ref() != Some(receipt) || receipt.fence() != self.fence {
            return Err(invalid(
                "Application reapplication changed before exposure release",
            ));
        }
        Ok(Self {
            fence: self.fence,
            reapplication: self.reapplication.clone(),
            released_revision: Some(self.fence.revision),
            checkpoint: Some(checkpoint),
        })
    }
    pub fn validate_execution(
        &self,
        authority: &DeletionControlState,
        permit: &StorageExecutionPermit,
    ) -> Result<(), StorageError> {
        if permit.fence() != self.fence || !self.is_released_for(authority) {
            return Err(StorageError::unavailable(
                "Application work belongs to an unreleased or superseded deletion generation",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::missing_incarnation(Uuid::nil(), 0)]
    #[case::negative_revision(Uuid::new_v4(), -1)]
    fn invalid_application_fences_are_rejected(#[case] incarnation: Uuid, #[case] revision: i64) {
        let encoded = serde_json::json!({"incarnation": incarnation, "revision": revision});
        assert!(serde_json::from_value::<ApplicationFence>(encoded).is_err());
    }

    #[test]
    fn released_receipt_cannot_authorize_a_new_application_incarnation() {
        let authority = DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4()).unwrap();
        let old = ApplicationControlState::initial(&authority);
        let receipt = ApplicationReapplication::new(old.fence());
        let released = old
            .record_reapplication(receipt.clone())
            .unwrap()
            .release(
                &receipt,
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            )
            .unwrap();
        let permit = StorageExecutionPermit::new(None, released.fence());
        let restored = ApplicationControlState::quarantined(
            ApplicationFence::new(Uuid::new_v4(), authority.revision()).unwrap(),
        );
        assert!(released.validate_execution(&authority, &permit).is_ok());
        assert!(restored.validate_execution(&authority, &permit).is_err());
        assert!(
            restored
                .release(
                    &receipt,
                    DeletionCheckpointIdentity::new("a".repeat(64)).unwrap()
                )
                .is_err()
        );
    }

    #[test]
    fn empty_authority_recovery_still_requires_explicit_release() {
        let authority = DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4()).unwrap();
        let initial = ApplicationControlState::initial(&authority);
        let recovered = ApplicationControlState::quarantined(initial.fence());
        assert!(initial.is_released_for(&authority));
        assert!(!recovered.is_released_for(&authority));
    }
}
