//! Shared manual and policy-root admission. This is trusted service input, not
//! a deserializable client permission grant.
use super::*;
use crate::{ApplicationFence, StorageIdentityEvidence, StorageIdentitySubject, StorageTask};
use hubuum_domain::PrincipalId;

#[derive(Clone, Debug)]
pub struct HistoryExpiryTaskSubmission {
    payload: HistoryExpiryTaskPayload,
    submitter: Option<PrincipalId>,
}
impl HistoryExpiryTaskSubmission {
    pub const fn manual(
        scan: HistoryLifecycleScan,
        application: ApplicationFence,
        submitter: PrincipalId,
        principal: PrincipalIdentity,
    ) -> Self {
        Self {
            payload: HistoryExpiryTaskPayload::new(
                scan,
                HistoryExpiryOrigin::Manual { principal },
                application,
            ),
            submitter: Some(submitter),
        }
    }
    pub fn periodic(
        scan: HistoryLifecycleScan,
        application: ApplicationFence,
        root: &HistoryPolicyRecord,
    ) -> Result<Self, StorageValidationError> {
        if root.proposal().key() != scan.key()
            || !matches!(
                root.proposal().binding(),
                HistoryPolicyBinding::Expire { .. }
            )
        {
            return Err(invalid(
                "Periodic expiry requires a retained expire policy for its exact scope",
            ));
        }
        Ok(Self {
            payload: HistoryExpiryTaskPayload::new(
                scan,
                HistoryExpiryOrigin::Scheduled {
                    initiator: Some(root.principal()),
                    policy: Some(HistoryPolicyReference::from(root)),
                },
                application,
            ),
            submitter: None,
        })
    }
    pub const fn payload(&self) -> &HistoryExpiryTaskPayload {
        &self.payload
    }
    pub const fn submitter(&self) -> Option<PrincipalId> {
        self.submitter
    }
    pub fn validate_principal(
        &self,
        evidence: &StorageIdentityEvidence,
    ) -> Result<(), StorageValidationError> {
        if !matches!(evidence.subject(), Some(StorageIdentitySubject::Principal { identity })
            if matches!(self.payload.origin(), HistoryExpiryOrigin::Manual { principal }
                if identity.known_identity() == Some(*principal)))
        {
            return Err(invalid("History initiating principal changed"));
        }
        Ok(())
    }
    /// Adapters call this under the same authority lock as coalescing/creation.
    pub fn validate(
        &self,
        snapshot: &HistoryLifecycleSnapshot,
    ) -> Result<(), StorageValidationError> {
        if self.payload.application().incarnation() != snapshot.fence().incarnation()
            || self.payload.application().revision() as u64
                != snapshot.fence().deletion_revision().value()
        {
            return Err(invalid(
                "History task admission requires the current application image",
            ));
        }
        if let HistoryExpiryOrigin::Scheduled {
            policy: Some(root),
            initiator,
        } = self.payload.origin()
            && !snapshot
                .control()
                .authority()
                .policies()
                .records()
                .iter()
                .any(|record| {
                    HistoryPolicyReference::from(record) == *root
                        && Some(record.principal()) == *initiator
                        && matches!(
                            record.proposal().binding(),
                            HistoryPolicyBinding::Expire { .. }
                        )
                })
        {
            return Err(invalid(
                "Periodic expiry root is not retained by current authority",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct HistoryExpiryTaskAdmission {
    task: StorageTask,
    created: bool,
}
impl HistoryExpiryTaskAdmission {
    pub const fn new(task: StorageTask, created: bool) -> Self {
        Self { task, created }
    }
    pub const fn task(&self) -> &StorageTask {
        &self.task
    }
    /// True when this call inserted a fresh task or a replacement cleanup owner.
    pub const fn created(&self) -> bool {
        self.created
    }
}
