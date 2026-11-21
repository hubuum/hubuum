//! Affine archive work ownership, distinct from a cloneable read context.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::future::Future;
use uuid::Uuid;

use crate::{
    HistoryProducerContext, HistoryProducerOwner, HistoryProducerSettlement,
    HistoryPublicationKind, HistoryPublicationOwner, HistoryPublicationSettlement, StorageError,
    StorageExecutionPermit, StorageExecutionScope,
};

/// Protected capture metadata, never an ownership capability. The commitment
/// binds private nonces without putting them into persisted/serialized captures.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryArchiveExecutionBinding {
    parent: Uuid,
    child: Uuid,
    history: crate::HistoryReviewFence,
    generation: crate::HistoryPublicationGeneration,
    commitment: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) deadline: Option<chrono::DateTime<chrono::Utc>>,
}
impl HistoryArchiveExecutionBinding {
    pub(super) fn is_well_formed(&self) -> bool {
        !self.parent.is_nil() && !self.child.is_nil() && self.commitment != [0; 32]
    }
    pub(super) fn valid_for(&self, fence: crate::ApplicationFence) -> bool {
        self.is_well_formed()
            && self.history.incarnation() == fence.incarnation()
            && self.history.deletion_revision().value() == fence.revision() as u64
    }
}

/// Only a correlated admitted parent/child pair creates this non-wire permit.
/// Context copies neither settle work nor prove physical completion.
#[derive(Clone)]
pub struct HistoryArchiveExecutionPermit {
    producer: HistoryProducerContext,
    scope: StorageExecutionScope,
    deadline: Option<chrono::DateTime<chrono::Utc>>,
}
impl fmt::Debug for HistoryArchiveExecutionPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryArchiveExecutionPermit")
            .field("parent", &self.producer.parent().id())
            .field("child", &self.producer.id())
            .finish_non_exhaustive()
    }
}
impl HistoryArchiveExecutionPermit {
    pub fn producer(&self) -> &HistoryProducerContext {
        &self.producer
    }
    pub fn application(&self) -> &StorageExecutionPermit {
        self.producer.parent().epoch().application()
    }
    pub fn execution_scope(&self) -> &StorageExecutionScope {
        &self.scope
    }
    /// Captured once from the original execution context. It narrows UTC work
    /// admission; it never replaces or renews the original monotonic stop scope.
    pub const fn execution_deadline(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.deadline
    }
    pub(super) fn binding(&self) -> HistoryArchiveExecutionBinding {
        let parent = self.producer.parent();
        let mut hash = Sha256::new();
        hash.update(b"hubuum-history-archive-execution-v2");
        hash.update(parent.id().as_bytes());
        hash.update(parent.ownership_token().as_bytes());
        hash.update(self.producer.id().as_bytes());
        hash.update(self.producer.ownership_token().as_bytes());
        let runtime = parent
            .epoch()
            .application()
            .owner()
            .expect("validated registered epoch");
        hash.update(runtime.id().as_bytes());
        hash.update(runtime.ownership_token().as_bytes());
        if let Some(deadline) = self.deadline {
            hash.update([1]);
            hash.update(deadline.timestamp().to_be_bytes());
            hash.update(deadline.timestamp_subsec_nanos().to_be_bytes());
        } else {
            hash.update([0]);
        }
        HistoryArchiveExecutionBinding {
            parent: parent.id(),
            child: self.producer.id(),
            history: parent.epoch().history(),
            generation: parent.epoch().generation(),
            commitment: hash.finalize().into(),
            deadline: self.deadline,
        }
    }
    /// Re-enter the SAME captured authority, filling only absent ambient facts.
    /// Present conflicting API/task/application facts cannot be overwritten.
    pub fn reenter_scope(
        &self,
        current: &StorageExecutionScope,
    ) -> Result<StorageExecutionScope, StorageError> {
        if current.storage_callback_active()
            || current
                .history_read_override()
                .is_some_and(|parent| parent != self.producer.parent())
        {
            return Err(StorageError::unavailable(
                "Archive cannot replace an active execution context",
            ));
        }
        self.producer
            .parent()
            .epoch()
            .validate_scope(&current.resume_history_binding(&self.scope))?;
        Ok(self.scope.clone())
    }
    /// Every direct adapter entry requires the already installed original scope.
    /// A supplied application permit alone is not archive publication authority.
    pub fn validate_scope(&self, current: &StorageExecutionScope) -> Result<(), StorageError> {
        if current.storage_callback_active() {
            return Err(StorageError::unavailable(
                "Archive cannot enter inside a storage callback",
            ));
        }
        if current.history_read_override() != Some(self.producer.parent()) {
            return Err(StorageError::unavailable(
                "Archive requires its original parent context",
            ));
        }
        if let Some(Some(execution)) = current.task_execution_override() {
            execution.check().map_err(StorageError::task_stopped)?;
        }
        self.producer.parent().epoch().validate_scope(current)
    }
}

/// Returned only after BOTH parent and child admission commit. Drop retains
/// their native obligations; neither this owner nor completion has a wire form.
pub struct HistoryArchiveExecutionOwner {
    publication: HistoryPublicationOwner,
    producer: HistoryProducerOwner,
    permit: HistoryArchiveExecutionPermit,
}
impl fmt::Debug for HistoryArchiveExecutionOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HistoryArchiveExecutionOwner")
            .field(&self.permit)
            .finish()
    }
}
impl HistoryArchiveExecutionOwner {
    /// Trusted complete-adapter boundary: keep the value private until the
    /// atomic admission transaction commits. Never return it from a callback.
    pub fn from_admitted(
        publication: HistoryPublicationOwner,
        producer: HistoryProducerOwner,
        scope: StorageExecutionScope,
    ) -> Result<Self, StorageError> {
        let parent = publication.context();
        if scope.storage_callback_active()
            || parent.kind() != HistoryPublicationKind::ArchiveRead
            || producer.context().parent() != &parent
            || scope.history_read_override().is_some()
        {
            return Err(StorageError::unavailable(
                "Archive admission owners are not correlated",
            ));
        }
        parent.epoch().validate_scope(&scope)?;
        let deadline = scope
            .task_execution_override()
            .and_then(Option::as_ref)
            .map(|execution| {
                // Sample the wall clock BEFORE remaining monotonic duration:
                // sampling overhead can narrow this cutoff, never renew it.
                let wall = chrono::Utc::now();
                chrono::Duration::from_std(execution.remaining())
                    .ok()
                    .and_then(|remaining| wall.checked_add_signed(remaining))
                    // An unrepresentable bound must not create a longer scope.
                    .unwrap_or(wall)
            });
        let permit = HistoryArchiveExecutionPermit {
            producer: producer.context().clone(),
            scope: scope.with_history_read(parent),
            deadline,
        };
        Ok(Self {
            publication,
            producer,
            permit,
        })
    }
    pub fn permit(&self) -> &HistoryArchiveExecutionPermit {
        &self.permit
    }
    /// Successful future completion must mean positive driver release AND the
    /// separate issued producer drain. Timeout/drop/error never creates proof.
    pub async fn after_drained<D, P>(
        self,
        driver: D,
        producer: P,
    ) -> Result<HistoryArchiveExecutionSettlement, StorageError>
    where
        D: Future<Output = Result<(), StorageError>>,
        P: Future<Output = Result<(), StorageError>>,
    {
        let driver = self.publication.archive_driver_released(driver).await?;
        let child = self.producer.after_completed(producer).await?;
        let parent = self.publication.after_archive_drained(driver, &child)?;
        Ok(HistoryArchiveExecutionSettlement { child, parent })
    }
}

/// Settle child first, then parent, outside all productive transactions. Its
/// construction records completed obligations; it does not reacquire authority.
pub struct HistoryArchiveExecutionSettlement {
    child: HistoryProducerSettlement,
    parent: HistoryPublicationSettlement,
}
impl HistoryArchiveExecutionSettlement {
    pub fn producer(&self) -> &HistoryProducerSettlement {
        &self.child
    }
    pub fn into_publication(self) -> HistoryPublicationSettlement {
        self.parent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ApiRequestPolicy, ApplicationFence, HistoryDeletionRevision, HistoryLifecycleControl,
        HistoryLifecycleSnapshot, HistoryPublicationGeneration, HistoryPublicationRegistry,
        HistoryReadEpoch, RuntimeRegistration, StorageApiRequestPermit, StorageCallSite,
        StorageQueryBudget, TaskPolicyRevision,
    };
    use std::task::{Context, Poll, Waker};
    use uuid::Uuid;

    fn request() -> StorageApiRequestPermit {
        StorageApiRequestPermit::try_new(
            Uuid::new_v4(),
            TaskPolicyRevision::new(1).unwrap(),
            ApiRequestPolicy::default(),
        )
        .unwrap()
    }
    fn parts() -> (
        HistoryPublicationRegistry,
        HistoryPublicationOwner,
        HistoryProducerOwner,
        StorageExecutionScope,
    ) {
        parts_with_execution(None)
    }
    fn parts_with_execution(
        execution: Option<hubuum_task_core::TaskExecutionContext>,
    ) -> (
        HistoryPublicationRegistry,
        HistoryPublicationOwner,
        HistoryProducerOwner,
        StorageExecutionScope,
    ) {
        // Pure model fixture only; native actual-admission proof is separate.
        let runtime = RuntimeRegistration::generate();
        let incarnation = Uuid::new_v4();
        let application = StorageExecutionPermit::new(
            Some(runtime.owner()),
            ApplicationFence::new(incarnation, 0).unwrap(),
        );
        let mut scope = StorageExecutionScope::default()
            .with_application_access(application.clone())
            .with_api_request(request())
            .with_query_budget(StorageQueryBudget::from_millis(100));
        if let Some(execution) = execution {
            scope = scope.with_task_execution(Some(execution));
        }
        let snapshot = HistoryLifecycleSnapshot::new(
            HistoryLifecycleControl::empty(),
            incarnation,
            HistoryDeletionRevision::new(0).unwrap(),
            chrono::Utc::now(),
        )
        .unwrap();
        let epoch = HistoryReadEpoch::for_publication(
            HistoryPublicationKind::ArchiveRead,
            application,
            &snapshot,
            HistoryPublicationGeneration::new(0).unwrap(),
        )
        .unwrap()
        .bind_scope(&scope)
        .unwrap();
        let mut registry = HistoryPublicationRegistry::default();
        let parent = registry
            .admit(HistoryPublicationKind::ArchiveRead, epoch.clone())
            .unwrap();
        let producer = registry.admit_producer(&parent.context(), &epoch).unwrap();
        (registry, parent, producer, scope)
    }
    fn fixture() -> (HistoryPublicationRegistry, HistoryArchiveExecutionOwner) {
        let (registry, parent, producer, scope) = parts();
        (
            registry,
            HistoryArchiveExecutionOwner::from_admitted(parent, producer, scope).unwrap(),
        )
    }
    fn immediate<T>(future: impl Future<Output = T>) -> T {
        match std::pin::pin!(future)
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("ready fixture unexpectedly pending"),
        }
    }

    #[rstest::rstest]
    #[case::original(0, true)]
    #[case::missing_parent(1, false)]
    #[case::changed_api(2, false)]
    #[case::cleared_api(3, false)]
    #[case::changed_budget(4, false)]
    #[case::changed_application(5, false)]
    #[case::changed_parent(6, false)]
    #[case::diagnostic_only(7, true)]
    #[case::cleared_budget(8, false)]
    #[case::prior_callback(9, false)]
    fn archive_permit_rejects_scope_refresh(#[case] change: u8, #[case] allowed: bool) {
        let (_, owner) = fixture();
        let permit = owner.permit();
        let original = permit.execution_scope().clone();
        let current = match change {
            0 => original,
            1 => StorageExecutionScope::default(),
            2 => original.with_api_request(request()),
            3 => StorageExecutionScope::default()
                .with_application_access(permit.application().clone())
                .with_query_budget(StorageQueryBudget::from_millis(100))
                .with_history_read(permit.producer().parent().clone()),
            4 => original.with_query_budget(StorageQueryBudget::from_millis(200)),
            5 => original.with_application_access(StorageExecutionPermit::new(
                Some(RuntimeRegistration::generate().owner()),
                permit.application().fence(),
            )),
            6 => original.with_history_read(
                HistoryPublicationOwner::generate(
                    HistoryPublicationKind::ArchiveRead,
                    permit.producer().parent().epoch().clone(),
                )
                .context(),
            ),
            7 => original.with_call_site(StorageCallSite::TaskWorker),
            8 => original.with_query_budget(None),
            9 => original.with_storage_callback(),
            _ => unreachable!(),
        };
        assert_eq!(permit.validate_scope(&current).is_ok(), allowed);
    }

    #[rstest::rstest]
    #[case::absent(0, true)]
    #[case::same(1, true)]
    #[case::explicit_budget_clear(2, false)]
    #[case::changed_api(3, false)]
    #[case::callback(4, false)]
    fn archive_reentry_never_overwrites_present_authority(
        #[case] change: u8,
        #[case] allowed: bool,
    ) {
        let (_, owner) = fixture();
        let permit = owner.permit();
        let current = match change {
            0 => StorageExecutionScope::default(),
            1 => permit.execution_scope().clone(),
            2 => StorageExecutionScope::default().with_query_budget(None),
            3 => StorageExecutionScope::default().with_api_request(request()),
            4 => StorageExecutionScope::default().with_storage_callback(),
            _ => unreachable!(),
        };
        let result = permit.reenter_scope(&current);
        assert_eq!(result.is_ok(), allowed);
        if let Ok(scope) = result {
            assert_eq!(&scope, permit.execution_scope());
        }
    }

    #[rstest::rstest]
    #[case::original(0, true)]
    #[case::different_parent(1, false)]
    #[case::old_unbound_capture(2, false)]
    #[case::different_commitment(3, false)]
    fn archive_persisted_binding_is_not_a_reusable_permit(
        #[case] change: u8,
        #[case] allowed: bool,
    ) {
        use crate::{
            HistoryArchiveCapture, HistoryArchiveCaptureRequest, HistoryArchiveCatalog,
            HistoryArchiveInput, HistoryArchivePoint, HistoryArchiveSelection,
            StorageBackupHistorySection,
        };
        use hubuum_domain::{PrincipalIdentity, ResourceIdentity};
        let (mut registry, parent, child, scope) = parts();
        let epoch = parent.context().epoch().clone();
        let owner =
            HistoryArchiveExecutionOwner::from_admitted(parent, child, scope.clone()).unwrap();
        let parent = registry
            .admit(HistoryPublicationKind::ArchiveRead, epoch.clone())
            .unwrap();
        let child = registry.admit_producer(&parent.context(), &epoch).unwrap();
        let other = HistoryArchiveExecutionOwner::from_admitted(parent, child, scope).unwrap();
        assert_eq!(owner.permit().application(), other.permit().application());
        let now = chrono::Utc::now();
        let resource = ResourceIdentity::generate();
        let request = HistoryArchiveCaptureRequest::new(
            PrincipalIdentity::generate(),
            HistoryArchiveSelection::new(
                StorageBackupHistorySection::CollectionHistory,
                resource,
                HistoryArchivePoint::AsOf { at: now },
            ),
            crate::HistoryArchiveCatalogGeneration::default(),
            now + chrono::Duration::hours(1),
        );
        let capture = HistoryArchiveCapture::capture(
            request.clone(),
            &HistoryArchiveCatalog::empty(resource),
            owner.permit().application(),
            HistoryArchiveInput::Unavailable,
            now,
        )
        .unwrap()
        .bind_execution(owner.permit())
        .unwrap();
        let mut wire = serde_json::to_value(&capture).unwrap();
        let text = wire.to_string();
        assert!(!text.contains(&owner.permit().producer().ownership_token().to_string()));
        assert!(
            !text.contains(
                &owner
                    .permit()
                    .producer()
                    .parent()
                    .ownership_token()
                    .to_string()
            )
        );
        match change {
            2 => {
                wire.as_object_mut().unwrap().remove("execution");
            }
            3 => {
                let byte = wire["execution"]["commitment"][0].as_u64().unwrap();
                wire["execution"]["commitment"][0] = serde_json::json!(byte ^ 1);
            }
            _ => {}
        }
        let decoded: HistoryArchiveCapture = serde_json::from_value(wire).unwrap();
        let permit = if change == 1 {
            other.permit()
        } else {
            owner.permit()
        };
        assert_eq!(decoded.validate_execution(permit).is_ok(), allowed);
        let error = decoded.staging_budget_error();
        let facts = error.history_archive_budget_evidence().unwrap();
        facts
            .validate_request(&request, permit.application(), now)
            .unwrap();
        assert_eq!(facts.validate_execution(permit).is_ok(), allowed);
    }

    #[test]
    fn archive_owner_requires_exact_parent_child_pair() {
        let (_, parent, _, scope) = parts();
        let (_, _, unrelated_child, _) = parts();
        assert!(
            HistoryArchiveExecutionOwner::from_admitted(parent, unrelated_child, scope).is_err()
        );
    }

    #[rstest::rstest]
    #[case::driver_unknown(false, true, false)]
    #[case::producer_unknown(true, false, false)]
    #[case::both_joined(true, true, true)]
    fn archive_settlement_requires_both_joined_obligations(
        #[case] driver: bool,
        #[case] producer: bool,
        #[case] settled: bool,
    ) {
        let (mut registry, owner) = fixture();
        let result = |proved| {
            if proved {
                Ok(())
            } else {
                Err(StorageError::unavailable("unknown physical completion"))
            }
        };
        if let Ok(completion) = immediate(owner.after_drained(
            std::future::ready(result(driver)),
            std::future::ready(result(producer)),
        )) {
            registry.settle_producer(completion.producer()).unwrap();
            registry.settle(completion.into_publication()).unwrap();
        }
        assert_eq!(registry.active(), usize::from(!settled));
        assert_eq!(registry.active_producers(), usize::from(!settled));
    }

    #[test]
    fn archive_permit_debug_redacts_ownership_and_api_material() {
        let (_, owner) = fixture();
        let permit = owner.permit();
        let debug = format!("{permit:?}");
        assert!(!debug.contains(&permit.producer().ownership_token().to_string()));
        assert!(!debug.contains(&permit.producer().parent().ownership_token().to_string()));
        assert!(
            !debug.contains(
                &permit
                    .execution_scope()
                    .api_request_override()
                    .unwrap()
                    .owner()
                    .to_string()
            )
        );
    }
    fn publication_capture(
        owner: &HistoryArchiveExecutionOwner,
        closed: bool,
        as_of: bool,
    ) -> crate::HistoryArchiveCapture {
        use crate::{
            HistoryArchiveCapture, HistoryArchiveCaptureRequest, HistoryArchiveCatalog,
            HistoryArchiveCatalogGeneration, HistoryArchiveInput, HistoryArchivePoint,
            HistoryArchiveRow, HistoryArchiveSelection, StorageBackupHistorySection,
        };
        let row = super::super::tests::row();
        let end = row.valid_from() + chrono::Duration::seconds(10);
        let mut payload = row.payload().clone();
        payload["name"] = serde_json::json!("confidential original publication collection");
        let row = HistoryArchiveRow::new(
            row.identity(),
            row.valid_from(),
            closed.then_some(end),
            payload,
        )
        .unwrap();
        let selection = HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            row.identity().resource(),
            if as_of {
                HistoryArchivePoint::AsOf {
                    at: row.valid_from(),
                }
            } else {
                HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                }
            },
        );
        let now = chrono::Utc::now();
        HistoryArchiveCapture::capture(
            HistoryArchiveCaptureRequest::new(
                hubuum_domain::PrincipalIdentity::generate(),
                selection,
                HistoryArchiveCatalogGeneration::default(),
                now + chrono::Duration::minutes(5),
            ),
            &HistoryArchiveCatalog::empty(row.identity().resource()),
            owner.permit().application(),
            HistoryArchiveInput::Available(row),
            now,
        )
        .unwrap()
        .bind_execution(owner.permit())
        .unwrap()
    }

    fn owner_with_deadline() -> HistoryArchiveExecutionOwner {
        let (execution, _) =
            hubuum_task_core::TaskExecutionContext::new(std::time::Duration::from_secs(60))
                .unwrap();
        let (_, parent, child, scope) = parts_with_execution(Some(execution));
        HistoryArchiveExecutionOwner::from_admitted(parent, child, scope).unwrap()
    }

    #[test]
    fn publication_execution_cutoff_is_fixed_across_reconstruction_and_expiry() {
        use crate::HistoryArchivePublicationManifest;
        use crate::artifacts::{ArtifactIdentity, DestinationId};
        let owner = owner_with_deadline();
        let capture = publication_capture(&owner, true, false);
        let location = ArtifactIdentity::new(DestinationId::generate());
        let plan = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            location.clone(),
            capture.captured_at(),
        )
        .unwrap();
        let cutoff = owner.permit().execution_deadline().unwrap();
        assert_eq!(plan.execution_deadline(), cutoff);
        assert!(cutoff < capture.available_until());
        let later = capture.captured_at() + chrono::Duration::seconds(1);
        let repeated = HistoryArchivePublicationManifest::from_capture(
            &capture,
            &owner.permit().clone(),
            location.clone(),
            later,
        )
        .unwrap();
        assert_eq!(repeated, plan);
        plan.validate_capture(&capture, owner.permit(), later)
            .unwrap();
        assert_eq!(
            capture.coverage(cutoff),
            crate::HistoryArchiveCoverageState::Complete
        );
        assert!(
            plan.validate_capture(&capture, owner.permit(), cutoff)
                .is_err()
        );
        assert!(
            HistoryArchivePublicationManifest::from_capture(
                &capture,
                owner.permit(),
                location,
                cutoff,
            )
            .is_err()
        );
    }

    #[rstest::rstest]
    #[case::extends(true)]
    #[case::erases(false)]
    fn publication_rejects_changed_original_execution_cutoff(#[case] extends: bool) {
        use crate::HistoryArchivePublicationManifest;
        use crate::artifacts::{ArtifactIdentity, DestinationId};
        let owner = owner_with_deadline();
        let capture = publication_capture(&owner, true, false);
        let plan = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            ArtifactIdentity::new(DestinationId::generate()),
            capture.captured_at(),
        )
        .unwrap();
        let mut wire = serde_json::to_value(&plan).unwrap();
        wire["execution"]["deadline"] = if extends {
            serde_json::json!(plan.execution_deadline() + chrono::Duration::seconds(10))
        } else {
            serde_json::Value::Null
        };
        // Well-formed metadata remains non-authoritative until compared with
        // the SAME admitted permit and original retained capture.
        let changed: HistoryArchivePublicationManifest = serde_json::from_value(wire).unwrap();
        assert!(
            changed
                .validate_capture(&capture, owner.permit(), capture.captured_at())
                .is_err()
        );
    }

    #[test]
    fn publication_cutoff_cannot_precede_its_original_capture() {
        use crate::HistoryArchivePublicationManifest;
        use crate::artifacts::{ArtifactIdentity, DestinationId};
        let owner = owner_with_deadline();
        let capture = publication_capture(&owner, true, false);
        let plan = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            ArtifactIdentity::new(DestinationId::generate()),
            capture.captured_at(),
        )
        .unwrap();
        let mut wire = serde_json::to_value(plan).unwrap();
        wire["execution"]["deadline"] =
            serde_json::json!(capture.captured_at() - chrono::Duration::seconds(1));
        assert!(serde_json::from_value::<HistoryArchivePublicationManifest>(wire).is_err());
    }

    #[rstest::rstest]
    #[case::exact(false)]
    #[case::as_of(true)]
    fn archive_publication_manifest_retains_one_original_closed_version(#[case] as_of: bool) {
        use crate::artifacts::{ArtifactIdentity, DestinationId};
        use crate::{HISTORY_ARCHIVE_PUBLICATION_BYTES, HistoryArchivePublicationManifest};
        let (_, owner) = fixture();
        let mut capture = publication_capture(&owner, true, as_of);
        let location = ArtifactIdentity::new(DestinationId::generate());
        let plan = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            location.clone(),
            capture.captured_at(),
        )
        .unwrap();
        let wire = serde_json::to_vec(&plan).unwrap();
        let decoded: HistoryArchivePublicationManifest = serde_json::from_slice(&wire).unwrap();
        assert_eq!(decoded, plan);
        assert_eq!(plan.capture(), capture.id());
        assert_eq!(plan.location(), &location);
        assert_eq!(plan.identity(), capture.hot().unwrap().identity());
        assert_eq!(plan.generation(), capture.generation());
        assert_eq!(plan.available_until(), capture.available_until());
        assert_eq!(
            plan.source_bytes(),
            serde_json::to_vec(capture.hot().unwrap()).unwrap().len() as u64
        );
        assert_eq!(
            plan.reserved_artifact_bytes(),
            HISTORY_ARCHIVE_PUBLICATION_BYTES
        );
        assert!(wire.len() < 256 * 1024);
        // Cumulative read work is charged separately; it cannot renew or
        // invalidate the immutable producer input commitment.
        capture.reserve(1024, capture.captured_at()).unwrap();
        decoded
            .validate_capture(&capture, owner.permit(), capture.captured_at())
            .unwrap();
        let wire = String::from_utf8(wire).unwrap();
        assert!(!wire.contains(&owner.permit().producer().ownership_token().to_string()));
        assert!(
            !wire.contains(
                &owner
                    .permit()
                    .producer()
                    .parent()
                    .ownership_token()
                    .to_string()
            )
        );
        let debug = format!("{plan:?}");
        assert!(!debug.contains(capture.authorization().unwrap().name()));
        assert!(!wire.contains(capture.authorization().unwrap().name()));
    }

    #[rstest::rstest]
    #[case::open(0)]
    #[case::deadline(1)]
    #[case::before_capture(2)]
    #[case::different_original_owner(3)]
    #[case::different_capture(4)]
    #[case::tampered_payload(5)]
    #[case::renewed_deadline(6)]
    fn archive_publication_manifest_rejects_refreshed_or_changed_input(#[case] change: u8) {
        use crate::HistoryArchivePublicationManifest;
        use crate::artifacts::{ArtifactIdentity, DestinationId};
        let (_, owner) = fixture();
        let capture = publication_capture(&owner, true, false);
        let location = ArtifactIdentity::new(DestinationId::generate());
        let now = capture.captured_at();
        let plan = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            location.clone(),
            now,
        )
        .unwrap();
        match change {
            0 => {
                let open = publication_capture(&owner, false, false);
                assert!(
                    HistoryArchivePublicationManifest::from_capture(
                        &open,
                        owner.permit(),
                        location,
                        open.captured_at()
                    )
                    .is_err()
                );
            }
            1 => assert!(
                plan.validate_capture(&capture, owner.permit(), capture.available_until())
                    .is_err()
            ),
            2 => assert!(
                plan.validate_capture(
                    &capture,
                    owner.permit(),
                    now - chrono::Duration::nanoseconds(1)
                )
                .is_err()
            ),
            3 => {
                let (_, other) = fixture();
                assert!(
                    plan.validate_capture(&capture, other.permit(), now)
                        .is_err()
                );
            }
            4 => {
                let other = publication_capture(&owner, true, false);
                assert!(
                    plan.validate_capture(&other, owner.permit(), other.captured_at())
                        .is_err()
                );
            }
            5 | 6 => {
                let mut wire = serde_json::to_value(&capture).unwrap();
                if change == 5 {
                    wire["hot"]["payload"]["description"] =
                        serde_json::json!("changed immutable content");
                } else {
                    wire["available_until"] = serde_json::to_value(
                        capture.available_until() + chrono::Duration::seconds(1),
                    )
                    .unwrap();
                }
                let changed = serde_json::from_value(wire).unwrap();
                assert!(
                    plan.validate_capture(&changed, owner.permit(), now)
                        .is_err()
                );
            }
            _ => unreachable!(),
        }
    }

    #[rstest::rstest]
    #[case::zero_input("source_bytes", serde_json::json!(0))]
    #[case::oversize_input("source_bytes", serde_json::json!(crate::HISTORY_ARCHIVE_ROW_BYTES + 1))]
    #[case::nil_capture("capture", serde_json::json!(Uuid::nil()))]
    #[case::empty_commitment("source_digest", serde_json::json!(vec![0; 32]))]
    #[case::invalid_interval("valid_to", serde_json::json!("1970-01-01T00:00:10Z"))]
    fn archive_publication_manifest_decode_preserves_bounds(
        #[case] key: &str,
        #[case] value: serde_json::Value,
    ) {
        use crate::HistoryArchivePublicationManifest;
        use crate::artifacts::{ArtifactIdentity, DestinationId};
        let (_, owner) = fixture();
        let capture = publication_capture(&owner, true, false);
        let plan = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            ArtifactIdentity::new(DestinationId::generate()),
            capture.captured_at(),
        )
        .unwrap();
        let mut wire = serde_json::to_value(plan).unwrap();
        wire[key] = value;
        assert!(serde_json::from_value::<HistoryArchivePublicationManifest>(wire).is_err());
    }
    #[test]
    fn publication_request_retains_exact_opaque_intent_without_minting_ownership() {
        use crate::artifacts::{ArtifactIdentity, ArtifactLimits, DestinationId};
        use crate::{
            HistoryArchiveEncodedArtifactIntent, HistoryArchivePublicationManifest,
            HistoryArchivePublicationRequest,
        };
        let (_, owner) = fixture();
        let capture = publication_capture(&owner, true, false);
        let manifest = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            ArtifactIdentity::new(DestinationId::generate()),
            capture.captured_at(),
        )
        .unwrap();
        let encoded = b"opaque private producer encoding, not interpreted by core".to_vec();
        let limits = ArtifactLimits::builder()
            .artifact_bytes(crate::HISTORY_ARCHIVE_PUBLICATION_BYTES)
            .build()
            .unwrap();
        let intent = HistoryArchiveEncodedArtifactIntent::try_new(
            manifest.location().clone(),
            limits,
            encoded.clone(),
            manifest.request_commitment().unwrap(),
        )
        .unwrap();
        let request = HistoryArchivePublicationRequest::try_new(manifest.clone(), intent).unwrap();
        assert_eq!(request.intent().encoded(), encoded);
        assert_eq!(request.intent().identity(), manifest.location());
        assert_eq!(request.intent().limits(), limits);
        assert_eq!(
            request.intent().digest(),
            &<[u8; 32]>::from(Sha256::digest(&encoded))
        );
        assert_eq!(
            request.retained_manifest_bytes().unwrap(),
            manifest.retained_manifest_bytes().unwrap() + encoded.len()
        );
        request
            .validate_capture(&capture, owner.permit(), capture.captured_at())
            .unwrap();
        assert!(!format!("{request:?}").contains("opaque private producer"));
    }

    #[rstest::rstest]
    #[case::foreign_identity(0)]
    #[case::different_request(1)]
    #[case::input_exceeds_ceiling(2)]
    fn publication_request_rejects_uncorrelated_provider_metadata(#[case] change: u8) {
        use crate::artifacts::{ArtifactIdentity, ArtifactLimits, DestinationId};
        use crate::{
            HistoryArchiveEncodedArtifactIntent, HistoryArchivePublicationManifest,
            HistoryArchivePublicationRequest,
        };
        let (_, owner) = fixture();
        let capture = publication_capture(&owner, true, false);
        let manifest = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            ArtifactIdentity::new(DestinationId::generate()),
            capture.captured_at(),
        )
        .unwrap();
        let identity = if change == 0 {
            ArtifactIdentity::new(DestinationId::generate())
        } else {
            manifest.location().clone()
        };
        let mut commitment = manifest.request_commitment().unwrap();
        if change == 1 {
            commitment[0] ^= 1;
        }
        let ceiling = if change == 2 {
            1
        } else {
            crate::HISTORY_ARCHIVE_PUBLICATION_BYTES
        };
        let limits = ArtifactLimits::builder()
            .artifact_bytes(ceiling)
            .build()
            .unwrap();
        let intent =
            HistoryArchiveEncodedArtifactIntent::try_new(identity, limits, vec![1], commitment)
                .unwrap();
        assert!(HistoryArchivePublicationRequest::try_new(manifest, intent).is_err());
    }

    #[rstest::rstest]
    #[case::exact_boundary(0, true)]
    #[case::one_over_aggregate(1, false)]
    fn publication_request_accounts_manifest_and_intent_as_one_budget(
        #[case] extra: usize,
        #[case] allowed: bool,
    ) {
        use crate::artifacts::{ArtifactIdentity, ArtifactLimits, DestinationId};
        use crate::{
            HistoryArchiveEncodedArtifactIntent, HistoryArchivePublicationManifest,
            HistoryArchivePublicationRequest,
        };
        let (_, owner) = fixture();
        let capture = publication_capture(&owner, true, false);
        let manifest = HistoryArchivePublicationManifest::from_capture(
            &capture,
            owner.permit(),
            ArtifactIdentity::new(DestinationId::generate()),
            capture.captured_at(),
        )
        .unwrap();
        let manifest_bytes = crate::history_archive_encoded_bytes(
            &manifest,
            crate::HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES,
        )
        .unwrap()
        .len();
        let encoded =
            vec![0x20; crate::HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES - manifest_bytes + extra];
        let limits = ArtifactLimits::builder()
            .artifact_bytes(crate::HISTORY_ARCHIVE_PUBLICATION_BYTES)
            .build()
            .unwrap();
        let intent = HistoryArchiveEncodedArtifactIntent::try_new(
            manifest.location().clone(),
            limits,
            encoded,
            manifest.request_commitment().unwrap(),
        )
        .unwrap();
        let result = HistoryArchivePublicationRequest::try_new(manifest, intent);
        assert_eq!(result.is_ok(), allowed);
        if let Err(error) = result {
            assert_eq!(error.kind(), crate::StorageErrorKind::InputTooLarge);
        }
    }
}
