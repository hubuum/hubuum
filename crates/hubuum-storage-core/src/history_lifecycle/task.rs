//! Durable, payload-free continuation of one expiry task. The retained journal
//! owns exact membership; this checkpoint only names that original admission.

use hubuum_domain::TaskIdentity;
use serde_json::Value;
use uuid::Uuid;

use super::*;
use crate::{
    ApplicationFence, StorageCapturedIdentity, StorageIdentityEvidence, StorageIdentitySubject,
    StorageTask, StorageTaskCheckpoint, StorageTaskEventInput, StorageTaskKind, StorageTaskLease,
    TaskMetadataDetails,
};

#[cfg(test)]
mod tests;

/// Approval issued by the trusted service after configured authorization.
/// It is neither deserializable task input nor a durable permission grant.
#[derive(Clone, Debug)]
pub struct HistoryExpiryTaskAuthorization(ExpiryAuthorization);

#[derive(Clone, Debug)]
enum ExpiryAuthorization {
    Manual(PrincipalIdentity),
    Scheduled(Option<HistoryPolicyReference>),
}

impl HistoryExpiryTaskAuthorization {
    pub const fn manual(principal: PrincipalIdentity) -> Self {
        Self(ExpiryAuthorization::Manual(principal))
    }

    pub const fn scheduled() -> Self {
        Self(ExpiryAuthorization::Scheduled(None))
    }

    pub const fn periodic(policy: HistoryPolicyReference) -> Self {
        Self(ExpiryAuthorization::Scheduled(Some(policy)))
    }

    fn matches(&self, origin: &HistoryExpiryOrigin) -> bool {
        match (&self.0, origin) {
            (ExpiryAuthorization::Manual(expected), HistoryExpiryOrigin::Manual { principal }) => {
                expected == principal
            }
            (
                ExpiryAuthorization::Scheduled(expected),
                HistoryExpiryOrigin::Scheduled { policy, .. },
            ) => expected == policy,
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HistoryExpiryTaskRequest {
    lease: StorageTaskLease,
    authorization: Option<HistoryExpiryTaskAuthorization>,
    protection: Option<HistoryExposureProtection>,
}

impl HistoryExpiryTaskRequest {
    pub const fn new(
        lease: StorageTaskLease,
        authorization: Option<HistoryExpiryTaskAuthorization>,
    ) -> Self {
        Self {
            lease,
            authorization,
            protection: None,
        }
    }

    /// Trusted service evidence from authenticated independent read-back. This
    /// is deliberately absent from the serialized task request and checkpoint.
    pub fn with_protection(mut self, protection: HistoryExposureProtection) -> Self {
        self.protection = Some(protection);
        self
    }

    pub const fn protection(&self) -> Option<&HistoryExposureProtection> {
        self.protection.as_ref()
    }

    pub const fn lease(&self) -> &StorageTaskLease {
        &self.lease
    }

    /// A fresh page needs current approval bound to its original initiation.
    /// Resuming already admitted membership does not use this approval.
    pub fn require_selection_authorization(
        &self,
        checkpoint: &HistoryExpiryTaskCheckpoint,
    ) -> Result<(), StorageValidationError> {
        if !self
            .authorization
            .as_ref()
            .is_some_and(|approval| approval.matches(checkpoint.0.payload.origin()))
        {
            return Err(invalid(
                "History expiry selection requires current initiation-bound authorization",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PayloadInput")]
pub struct HistoryExpiryTaskPayload {
    version: u8,
    action: ExpiryAction,
    scan: HistoryLifecycleScan,
    origin: HistoryExpiryOrigin,
    application: ApplicationFence,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery: Option<RetainedBatch>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedBatch {
    batch: Uuid,
    run: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExpiryAction {
    ExpireConfiguredHotHistory,
    ResumeRetainedBatch,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PayloadInput {
    version: u8,
    action: ExpiryAction,
    scan: HistoryLifecycleScan,
    origin: HistoryExpiryOrigin,
    application: ApplicationFence,
    #[serde(default, deserialize_with = "deserialize_recovery")]
    recovery: Option<RetainedBatch>,
}

fn deserialize_recovery<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<RetainedBatch>, D::Error> {
    RetainedBatch::deserialize(deserializer).map(Some)
}

impl TryFrom<PayloadInput> for HistoryExpiryTaskPayload {
    type Error = StorageValidationError;

    fn try_from(input: PayloadInput) -> Result<Self, Self::Error> {
        input.origin.validate_key(input.scan.key())?;
        let supported = match (input.version, input.action, input.recovery) {
            (1, ExpiryAction::ExpireConfiguredHotHistory, None) => true,
            (2, ExpiryAction::ResumeRetainedBatch, Some(binding)) => {
                !binding.batch.is_nil() && !binding.run.is_nil()
            }
            _ => false,
        };
        if !supported {
            return Err(invalid("Unsupported history expiry task payload version"));
        }
        Ok(Self {
            version: input.version,
            action: input.action,
            scan: input.scan,
            origin: input.origin,
            application: input.application,
            recovery: input.recovery,
        })
    }
}

impl HistoryExpiryTaskPayload {
    pub const fn new(
        scan: HistoryLifecycleScan,
        origin: HistoryExpiryOrigin,
        application: ApplicationFence,
    ) -> Self {
        Self {
            version: 1,
            action: ExpiryAction::ExpireConfiguredHotHistory,
            scan,
            origin,
            application,
            recovery: None,
        }
    }

    /// Cleanup is bound to an existing decision; it can never select a page.
    /// Its task ownership is separate from the immutable original run/origin.
    pub fn resume_retained(
        manifest: &HistoryExpiryManifest,
    ) -> Result<Self, StorageValidationError> {
        Ok(Self {
            version: 2,
            action: ExpiryAction::ResumeRetainedBatch,
            scan: HistoryLifecycleScan::new(manifest.key(), manifest.members().len() as u32)?,
            origin: manifest.origin().clone(),
            application: ApplicationFence::new(
                manifest.fence().incarnation(),
                manifest.fence().deletion_revision().value() as i64,
            )?,
            recovery: Some(RetainedBatch {
                batch: manifest.identity(),
                run: manifest.run(),
            }),
        })
    }

    pub fn recovery_batch(&self) -> Option<Uuid> {
        self.recovery.map(|binding| binding.batch)
    }

    /// Shared queued audit input, preserving the retained-cleanup event contract.
    pub fn admission_event(&self) -> StorageTaskEventInput {
        match self.recovery {
            Some(binding) => {
                StorageTaskEventInput::new("queued", "Retained history batch adopted for cleanup")
                    .with_data(Some(
                        serde_json::json!({"batch": binding.batch, "original_run": binding.run}),
                    ))
            }
            None => StorageTaskEventInput::new("queued", "History expiry task admitted").with_data(
                Some(serde_json::json!({"key": self.scan.key(), "origin": self.origin})),
            ),
        }
    }

    pub const fn scan(&self) -> &HistoryLifecycleScan {
        &self.scan
    }

    pub const fn origin(&self) -> &HistoryExpiryOrigin {
        &self.origin
    }

    pub const fn application(&self) -> ApplicationFence {
        self.application
    }

    pub fn decode(value: &Value) -> Result<Self, StorageValidationError> {
        serde_json::from_value(value.clone())
            .map_err(|_| invalid("Invalid history expiry task payload"))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum ExpiryPosition {
    Scanning {
        batch: Uuid,
        progress: Option<HistoryExpiryScanProgress>,
    },
    Admitted {
        batch: Uuid,
        progress: HistoryExpiryScanProgress,
    },
    Completed {
        batch: Uuid,
        removed: u32,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CheckpointInput {
    run: TaskIdentity,
    payload: HistoryExpiryTaskPayload,
    position: ExpiryPosition,
}

/// Validated task/run binding. Unknown formats, changed scope/origin and stale
/// application images cannot be treated as permission to restart selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryExpiryTaskCheckpoint(CheckpointInput);

impl HistoryExpiryTaskCheckpoint {
    const FORMAT: &str = "hubuum.history_expiry";
    const VERSION: u32 = 1;

    /// Read the stored task's original UUID and request once. Missing or
    /// derived/uncorrelated identity is not permission to invent a new run.
    pub fn from_task(
        task: &StorageTask,
        evidence: &StorageIdentityEvidence,
    ) -> Result<Self, StorageValidationError> {
        if task.kind() != StorageTaskKind::HistoryExpiry {
            return Err(invalid(
                "History expiry execution requires its dedicated task kind",
            ));
        }
        if evidence.has_unknown_task_restore_source() {
            return Err(invalid(
                "History expiry task requires its original logical identity",
            ));
        }
        let run = match evidence.subject() {
            Some(StorageIdentitySubject::Task { identity }) => identity.known_identity(),
            _ => None,
        }
        .ok_or_else(|| invalid("History expiry task identity is unresolved"))?;
        let payload = HistoryExpiryTaskPayload::decode(
            task.request_payload()
                .ok_or_else(|| invalid("History expiry task payload is absent"))?,
        )?;
        let owner_batch = match task.metadata().map(|metadata| metadata.details()) {
            Some(TaskMetadataDetails::HistoryExpiry {
                recovery_batch,
                admission_scope,
            }) => {
                if admission_scope.is_some_and(|key| key != payload.scan().key()) {
                    return Err(invalid(
                        "History task scope differs from its durable admission binding",
                    ));
                }
                *recovery_batch
            }
            _ => None,
        };
        if owner_batch != payload.recovery_batch() {
            return Err(invalid(
                "History cleanup payload differs from its durable owner binding",
            ));
        }
        if let HistoryExpiryOrigin::Manual { principal } = payload.origin()
            && payload.recovery.is_none()
        {
            let captured = match evidence.references().get("/submitted_by") {
                Some(StorageCapturedIdentity::Principal { reference }) => {
                    reference.known_identity()
                }
                _ => None,
            };
            if captured != Some(*principal) {
                return Err(invalid(
                    "History expiry origin differs from its original submitting principal",
                ));
            }
        }
        match task.checkpoint() {
            Some(checkpoint) => Self::decode(checkpoint, run, &payload),
            None => Ok(Self::new(run, payload)),
        }
    }

    pub const fn task_identity(&self) -> TaskIdentity {
        self.0.run
    }

    pub fn new(run: TaskIdentity, payload: HistoryExpiryTaskPayload) -> Self {
        let position = match payload.recovery {
            Some(binding) => ExpiryPosition::Admitted {
                batch: binding.batch,
                progress: HistoryExpiryScanProgress::PassFinished,
            },
            None => ExpiryPosition::Scanning {
                batch: Uuid::new_v4(),
                progress: None,
            },
        };
        Self(CheckpointInput {
            run,
            payload,
            position,
        })
    }

    pub fn decode(
        checkpoint: &StorageTaskCheckpoint,
        run: TaskIdentity,
        payload: &HistoryExpiryTaskPayload,
    ) -> Result<Self, StorageValidationError> {
        if checkpoint.format() != Self::FORMAT || checkpoint.version() != Self::VERSION {
            return Err(invalid("Unsupported history expiry task checkpoint"));
        }
        let input: CheckpointInput = serde_json::from_value(checkpoint.data().clone())
            .map_err(|_| invalid("Invalid history expiry task checkpoint"))?;
        if input.run != run || &input.payload != payload {
            return Err(invalid(
                "History expiry checkpoint differs from its original task",
            ));
        }
        let result = Self(input);
        if let Some(binding) = payload.recovery
            && (result.batch() != binding.batch
                || matches!(result.0.position, ExpiryPosition::Scanning { .. }))
        {
            return Err(invalid(
                "Retained history expiry cleanup cannot restart selection",
            ));
        }
        let application = payload.application();
        let fence = HistoryReviewFence::new(
            application.incarnation(),
            HistoryAuthorityRevision::new(0)?,
            HistoryDeletionRevision::new(application.revision() as u64)?,
        )?;
        // This validates UUIDs and any finite continuation's original binding,
        // including an admitted batch whose membership must now be resumed.
        let (batch, progress) = match &result.0.position {
            ExpiryPosition::Scanning { batch, progress } => (*batch, progress.as_ref()),
            ExpiryPosition::Admitted { batch, progress } => (*batch, Some(progress)),
            ExpiryPosition::Completed { batch, removed } => {
                if *removed == 0 || *removed as usize > HistoryExpiryManifest::MAX_MEMBERS {
                    return Err(invalid("Invalid history expiry completion count"));
                }
                (*batch, None)
            }
        };
        result.request_for(batch, progress, fence)?;
        Ok(result)
    }

    pub fn encode(&self) -> Result<StorageTaskCheckpoint, StorageValidationError> {
        StorageTaskCheckpoint::try_new(
            Self::FORMAT,
            Self::VERSION,
            serde_json::to_value(&self.0)
                .map_err(|_| invalid("Cannot encode history expiry task checkpoint"))?,
        )
    }

    fn request_for(
        &self,
        batch: Uuid,
        progress: Option<&HistoryExpiryScanProgress>,
        fence: HistoryReviewFence,
    ) -> Result<HistoryExpiryAdmissionRequest, StorageValidationError> {
        let application = self.0.payload.application();
        if fence.incarnation() != application.incarnation()
            || fence.deletion_revision().value() != application.revision() as u64
        {
            return Err(invalid("History expiry task application image changed"));
        }
        let request = HistoryExpiryAdmissionRequest::new(
            batch,
            self.original_run(),
            self.0.payload.origin().clone(),
            self.0.payload.scan().clone(),
            fence,
        )?;
        match progress {
            Some(HistoryExpiryScanProgress::Continue(cursor)) => {
                request.continue_from(cursor.clone())
            }
            _ => Ok(request),
        }
    }

    /// A finite pass can finish without proving that the source is empty. A
    /// later scheduled/manual run starts a new pass. An admitted batch can
    /// never use this path to replace its original membership.
    pub fn selection_request(
        &self,
        fence: HistoryReviewFence,
    ) -> Result<Option<HistoryExpiryAdmissionRequest>, StorageValidationError> {
        if self.0.payload.recovery.is_some() && !self.finished() {
            return Err(invalid(
                "Retained history expiry cleanup cannot select new membership",
            ));
        }
        match &self.0.position {
            ExpiryPosition::Scanning {
                progress: Some(HistoryExpiryScanProgress::PassFinished),
                ..
            } => {
                self.request_for(self.batch(), None, fence)?;
                Ok(None)
            }
            ExpiryPosition::Scanning { batch, progress } => {
                self.request_for(*batch, progress.as_ref(), fence).map(Some)
            }
            ExpiryPosition::Admitted { .. } => Err(invalid(
                "History expiry task must resume its admitted batch before selecting new work",
            )),
            ExpiryPosition::Completed { .. } => {
                self.request_for(self.batch(), None, fence)?;
                Ok(None)
            }
        }
    }

    pub fn record_page(
        &self,
        page: &HistoryExpiryAdmissionResult,
    ) -> Result<Self, StorageValidationError> {
        if !matches!(self.0.position, ExpiryPosition::Scanning { .. }) || self.pass_finished() {
            return Err(invalid(
                "History expiry task cannot replace a committed continuation",
            ));
        }
        let position = if let Some(manifest) = page.manifest() {
            self.validate_manifest(manifest)?;
            ExpiryPosition::Admitted {
                batch: manifest.identity(),
                progress: page.progress().clone(),
            }
        } else {
            ExpiryPosition::Scanning {
                batch: Uuid::new_v4(),
                progress: Some(page.progress().clone()),
            }
        };
        let next = Self(CheckpointInput {
            position,
            ..self.0.clone()
        });
        // Also reject traversal progress manufactured for a different run.
        Self::decode(&next.encode()?, self.0.run, &self.0.payload)
    }

    fn validate_manifest(
        &self,
        manifest: &HistoryExpiryManifest,
    ) -> Result<(), StorageValidationError> {
        if manifest.identity() != self.batch()
            || manifest.run() != self.original_run()
            || manifest.key() != self.0.payload.scan().key()
            || manifest.origin() != self.0.payload.origin()
        {
            return Err(invalid("History expiry task batch correspondence changed"));
        }
        self.request_for(manifest.identity(), None, manifest.fence())?;
        Ok(())
    }

    fn original_run(&self) -> Uuid {
        self.0.payload.recovery.map_or_else(
            || Uuid::from_bytes(*self.0.run.as_bytes()),
            |binding| binding.run,
        )
    }

    pub const fn batch(&self) -> Uuid {
        match self.0.position {
            ExpiryPosition::Scanning { batch, .. }
            | ExpiryPosition::Admitted { batch, .. }
            | ExpiryPosition::Completed { batch, .. } => batch,
        }
    }

    pub const fn admitted(&self) -> bool {
        matches!(self.0.position, ExpiryPosition::Admitted { .. })
    }

    pub fn pass_finished(&self) -> bool {
        matches!(
            self.0.position,
            ExpiryPosition::Scanning {
                progress: Some(HistoryExpiryScanProgress::PassFinished),
                ..
            }
        )
    }

    pub fn finished(&self) -> bool {
        self.pass_finished() || matches!(self.0.position, ExpiryPosition::Completed { .. })
    }

    pub const fn removed_count(&self) -> u32 {
        match self.0.position {
            ExpiryPosition::Completed { removed, .. } => removed,
            _ => 0,
        }
    }

    /// A serialized completion marker cannot substitute for the retained
    /// ledger. Adapters check this again under their finalization lock.
    pub fn validate_completion(
        &self,
        snapshot: &HistoryLifecycleSnapshot,
    ) -> Result<(), StorageValidationError> {
        if let ExpiryPosition::Completed { removed, .. } = self.0.position {
            let manifest = self.retained_manifest(snapshot)?;
            let intent = snapshot
                .control()
                .authority()
                .expiry()
                .intent(manifest.identity())
                .expect("validated retained manifest");
            if !intent.is_complete() || intent.removals().len() != removed as usize {
                return Err(invalid(
                    "History expiry completion differs from retained receipts",
                ));
            }
        } else if !self.pass_finished() {
            return Err(invalid("History expiry task still has unfinished work"));
        }
        Ok(())
    }

    /// Finish one bounded admitted batch only after independent completion
    /// protection. A subsequent task starts a fresh scan, including late rows.
    pub fn complete_batch(
        &self,
        snapshot: &HistoryLifecycleSnapshot,
        protection: &HistoryExposureProtection,
    ) -> Result<Self, StorageValidationError> {
        let intent = self.protected_intent(snapshot, protection)?;
        if !intent.is_complete()
            || !protection
                .authority()
                .expiry()
                .intent(self.batch())
                .is_some_and(|protected| protected.covers(intent))
        {
            return Err(invalid(
                "History expiry completion lacks independent protection",
            ));
        }
        Ok(Self(CheckpointInput {
            position: ExpiryPosition::Completed {
                batch: self.batch(),
                removed: intent.removals().len() as u32,
            },
            ..self.0.clone()
        }))
    }

    pub fn protected_intent<'a>(
        &self,
        snapshot: &'a HistoryLifecycleSnapshot,
        protection: &HistoryExposureProtection,
    ) -> Result<&'a HistoryExpiryIntent, StorageValidationError> {
        let manifest = self.retained_manifest(snapshot)?;
        if protection.fence() != self.0.payload.application()
            || !snapshot
                .control()
                .authority()
                .covers(protection.authority())
            || !protection
                .authority()
                .expiry()
                .intent(self.batch())
                .is_some_and(|intent| intent.manifest() == manifest)
        {
            return Err(invalid(
                "History expiry membership lacks independent protection",
            ));
        }
        Ok(snapshot
            .control()
            .authority()
            .expiry()
            .intent(self.batch())
            .expect("validated intent"))
    }

    /// Resolve the original retained manifest, never reconstruct it from current
    /// source rows. Independent recovery protection is a separate service step.
    pub fn retained_manifest<'a>(
        &self,
        snapshot: &'a HistoryLifecycleSnapshot,
    ) -> Result<&'a HistoryExpiryManifest, StorageValidationError> {
        if !self.admitted() && !matches!(self.0.position, ExpiryPosition::Completed { .. }) {
            return Err(invalid("History expiry task has no admitted batch"));
        }
        self.request_for(self.batch(), None, snapshot.fence())?;
        let manifest = snapshot
            .control()
            .authority()
            .expiry()
            .intent(self.batch())
            .ok_or_else(|| invalid("History expiry task lacks its original retained admission"))?
            .manifest();
        self.validate_manifest(manifest)?;
        Ok(manifest)
    }
}
