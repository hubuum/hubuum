use std::path::PathBuf;
use std::sync::Arc;

use hubuum_domain::PrincipalIdentity;
use hubuum_storage_core::{
    ControlKeyFingerprint, DeletionCheckpointIdentity, DeletionControlStorage,
    ProtectedReceiptExpiry, ProtectedTaskAdmission, ProtectedTaskPurge, ReceiptExpirySelection,
    StorageTaskKind, TaskActivationOutcome, TaskAdmissionTicket, TaskControlCheckpoint,
    TaskControlRevision, TaskLegacyReceiptBinding, TaskLegacyReceiptCandidate, TaskPurgeBudget,
    TaskPurgeProgress, TaskPurgeSelection, TaskPurgeTicket, TaskQueueStorage,
    TaskReceiptActivation, TaskReceiptBinding, TaskRequestFingerprint, TaskRetentionStorage,
    TaskSubmissionIdentity, TaskSubmissionLookup, TaskSubmissionResolution,
};
use hubuum_task_core::IdempotencyKey;
use serde::Serialize;
use tokio::sync::Mutex;

use super::repository::{AuthenticatedPayloadCache, CheckpointRepository, CheckpointSession};
use super::task_key::TaskReceiptKey;
use super::{invalid_control, unavailable};
use crate::config::ControlRecoveryOptions;
use crate::errors::ApiError;
use crate::observability::metrics::{CheckpointOperation, CheckpointTimer};
use crate::storage::{StorageContext, storage_handle};

const MAX_PENDING_ADMISSIONS_PER_PASS: u32 = 8;
const MAX_PURGE_BATCHES_PER_PASS: u32 = 8;
const MAX_SELECTED_TASKS: u32 = 64;

#[derive(Clone, Default)]
pub(crate) struct TaskReceiptService {
    configured: Option<Arc<Configuration>>,
}

struct Configuration {
    directory: PathBuf,
    key_file: PathBuf,
    key: TaskReceiptKey,
    // Coalesce work within this process. The owned repository session supplies
    // cross-process exclusion and survives cancellation during blocking IO.
    publication: Mutex<()>,
    payload_cache: Arc<AuthenticatedPayloadCache>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskRetentionSweep {
    Disabled,
    Protected { rows: u32, bytes: u64, more: bool },
}

impl TaskReceiptService {
    pub(crate) async fn configured(options: &ControlRecoveryOptions) -> Result<Self, ApiError> {
        let Some((directory, key_file)) = options.paths()? else {
            return Ok(Self::default());
        };
        let directory = directory.to_owned();
        let key_file = key_file.to_owned();
        let configured = tokio::task::spawn_blocking(move || {
            let repository = CheckpointRepository::open(&directory, &key_file)?;
            Ok::<_, ApiError>(Configuration {
                directory,
                key_file,
                key: repository.task_receipt_key()?,
                publication: Mutex::new(()),
                payload_cache: Arc::new(AuthenticatedPayloadCache::default()),
            })
        })
        .await
        .map_err(|_| unavailable("Task receipt configuration worker failed"))??;
        Ok(Self {
            configured: Some(Arc::new(configured)),
        })
    }

    pub(crate) fn is_configured(&self) -> bool {
        self.configured.is_some()
    }

    pub(crate) fn key_fingerprint(&self) -> Result<ControlKeyFingerprint, ApiError> {
        self.configuration()?.key.fingerprint()
    }

    pub(crate) fn protected_legacy_binding(
        &self,
        candidate: &TaskLegacyReceiptCandidate,
    ) -> Result<TaskLegacyReceiptBinding, ApiError> {
        let raw = candidate.task().idempotency_key().ok_or_else(|| {
            unavailable("Legacy task candidate lacks its original idempotency key")
        })?;
        let key = IdempotencyKey::new(raw)
            .map_err(|_| unavailable("Legacy task candidate has an invalid idempotency key"))?;
        Ok(TaskLegacyReceiptBinding::new(
            candidate.identity(),
            TaskSubmissionIdentity::new(
                candidate.principal(),
                self.configuration()?
                    .key
                    .protected_key(candidate.principal(), &key)?,
            ),
            crate::services::tasks::legacy_task_fingerprint(self, candidate.task())?,
        ))
    }

    fn configuration(&self) -> Result<&Arc<Configuration>, ApiError> {
        self.configured.as_ref().ok_or_else(|| {
            unavailable(
                "Protected task admission requires the configured independent control repository",
            )
        })
    }

    fn validate_activation(&self, activation: &TaskReceiptActivation) -> Result<(), ApiError> {
        if let Some(expected) = activation.key_fingerprint()
            && expected != &self.key_fingerprint()?
        {
            return Err(unavailable(
                "Task control key does not match protected receipt authority",
            ));
        }
        Ok(())
    }

    pub(crate) fn prepare_submission<T: Serialize + ?Sized>(
        &self,
        principal: PrincipalIdentity,
        key: &IdempotencyKey,
        kind: StorageTaskKind,
        operation: &str,
        original: &T,
        legacy_digest: Option<&str>,
    ) -> Result<Option<TaskReceiptBinding>, ApiError> {
        self.configured
            .as_ref()
            .map(|configuration| {
                configuration
                    .key
                    .binding(principal, key, kind, operation, original, legacy_digest)
            })
            .transpose()
    }

    pub(crate) fn fingerprint<T: Serialize + ?Sized>(
        &self,
        kind: StorageTaskKind,
        operation: &str,
        original: &T,
    ) -> Result<TaskRequestFingerprint, ApiError> {
        self.configuration()?
            .key
            .request_fingerprint(kind, operation, original)
    }

    pub(crate) fn legacy_fingerprint(
        &self,
        kind: StorageTaskKind,
        digest: &str,
    ) -> Result<TaskRequestFingerprint, ApiError> {
        self.configuration()?.key.legacy_fingerprint(kind, digest)
    }

    pub(crate) async fn resolve_submission(
        &self,
        context: &impl StorageContext,
        lookup: TaskSubmissionLookup,
    ) -> Result<TaskSubmissionResolution, ApiError> {
        let storage = storage_handle(context);
        // A different key must never select a different namespace and look like
        // a fresh submission. Storage rechecks activation atomically on create.
        self.validate_activation(&storage.read_task_receipt_activation().await?)?;
        let result = storage.lookup_task_submission(lookup.clone()).await?;
        if !matches!(
            result,
            TaskSubmissionResolution::ExpiryProtectionRequired(_)
        ) {
            return Ok(result);
        }
        let configuration = self.configuration()?;
        let wait = CheckpointTimer::start(CheckpointOperation::CoalescingWait);
        let _coalescing = configuration.publication.lock().await;
        wait.finish(true);
        let session = open_session(configuration).await?;
        let (_session, _) = expire_one_batch(context, session).await?;
        // Pending protection is intentionally returned to the application for
        // current receipt authorization before it may help activate the task.
        Ok(storage.lookup_task_submission(lookup).await?)
    }

    pub(crate) async fn protect_admission(
        &self,
        context: &impl StorageContext,
        ticket: TaskAdmissionTicket,
    ) -> Result<TaskActivationOutcome, ApiError> {
        let configuration = self.configuration()?;
        self.validate_activation(
            &storage_handle(context)
                .read_task_receipt_activation()
                .await?,
        )?;
        let wait = CheckpointTimer::start(CheckpointOperation::CoalescingWait);
        let _coalescing = configuration.publication.lock().await;
        wait.finish(true);
        let mut session = open_session(configuration).await?;
        let already_captured = session.checkpoint().task_control().is_some_and(|control| {
            control
                .receipt(ticket.receipt().submission())
                .is_some_and(|receipt| receipt.ticket() == &ticket)
        });
        if !already_captured {
            session = publish_current(context, session).await?;
        }
        activate_captured(context, &session, ticket).await
    }

    pub(crate) async fn sweep_task_retention(
        &self,
        context: &impl StorageContext,
    ) -> Result<TaskRetentionSweep, ApiError> {
        let storage = storage_handle(context);
        let activation = storage.read_task_receipt_activation().await?;
        if matches!(activation, TaskReceiptActivation::Disabled) {
            return Ok(TaskRetentionSweep::Disabled);
        }
        self.validate_activation(&activation)?;
        let configuration = self.configuration()?;
        let wait = CheckpointTimer::start(CheckpointOperation::CoalescingWait);
        let _coalescing = configuration.publication.lock().await;
        wait.finish(true);
        let session = open_session(configuration).await?;
        if !matches!(activation, TaskReceiptActivation::Protected { .. }) {
            return Err(unavailable(
                "Task receipt backfill must finish before retention runs",
            ));
        }
        // A previous process may have committed expiry or removal and lost its
        // acknowledgement before publishing the resulting complete authority.
        // Such work no longer appears in pending selectors; repair it first.
        let session = publish_current(context, session).await?;
        let (session, activated) = activate_pending(context, session).await?;
        let (mut session, expired) = expire_one_batch(context, session).await?;
        let budget = storage.read_task_retention_policy().await?.cleanup_budget();
        let selection =
            TaskPurgeSelection::new(MAX_SELECTED_TASKS, budget).map_err(invalid_control)?;
        // New eligible work must not wait behind an old held manifest. Both
        // adapters exclude held/dependent targets before their bounded selection.
        storage.prepare_task_details_purge(selection).await?;
        // A details manifest already covers its owned outputs; select it first
        // so one eligible task does not consume two permanent manifests.
        storage.prepare_expired_task_outputs(selection).await?;
        let pending = storage
            .list_pending_task_purges(MAX_PURGE_BATCHES_PER_PASS)
            .await?;
        if !pending.is_empty() {
            session = publish_current(context, session).await?;
        }
        let mut rows = 0;
        let mut bytes = 0;
        let mut more = expired || activated;
        for ticket in pending {
            if rows == budget.max_rows() || bytes == budget.max_bytes() {
                more = true;
                break;
            }
            let remaining =
                TaskPurgeBudget::new(budget.max_rows() - rows, budget.max_bytes() - bytes)
                    .map_err(invalid_control)?;
            require_captured_purge(&session, &ticket)?;
            let protected = ProtectedTaskPurge::new(ticket.clone(), checkpoint_identity(&session)?)
                .map_err(invalid_control)?;
            match storage
                .purge_task_details_batch(protected, remaining)
                .await?
            {
                TaskPurgeProgress::More {
                    rows: removed_rows,
                    bytes: removed_bytes,
                } => {
                    charge_progress(&mut rows, &mut bytes, removed_rows, removed_bytes, budget)?;
                    more = true;
                }
                // Persisted attempt ordering rotates blocked manifests behind
                // other candidates. A blocked-only pass uses normal cadence.
                TaskPurgeProgress::Blocked(_) => {}
                TaskPurgeProgress::Removed {
                    revision,
                    rows: removed_rows,
                    bytes: removed_bytes,
                } => {
                    charge_progress(&mut rows, &mut bytes, removed_rows, removed_bytes, budget)?;
                    session = publish_current(context, session).await?;
                    require_purge_completion(&session, &ticket, revision)?;
                    more = true;
                }
            }
        }
        Ok(TaskRetentionSweep::Protected { rows, bytes, more })
    }
}

async fn activate_captured(
    context: &impl StorageContext,
    session: &CheckpointSession,
    ticket: TaskAdmissionTicket,
) -> Result<TaskActivationOutcome, ApiError> {
    let captured = session
        .checkpoint()
        .task_control()
        .ok_or_else(|| unavailable("Task admission has no independent receipt checkpoint"))?;
    if !captured
        .receipt(ticket.receipt().submission())
        .is_some_and(|receipt| receipt.ticket() == &ticket)
    {
        return Err(unavailable(
            "Task admission generation changed before protection",
        ));
    }
    let storage = storage_handle(context);
    let fence = storage.read_application_control().await?.fence();
    let protected = ProtectedTaskAdmission::new(ticket, checkpoint_identity(session)?, fence)
        .map_err(invalid_control)?;
    // Keep independent publication excluded through the exact generation CAS.
    Ok(storage.activate_task_submission(protected).await?)
}

async fn activate_pending(
    context: &impl StorageContext,
    mut session: CheckpointSession,
) -> Result<(CheckpointSession, bool), ApiError> {
    let pending = storage_handle(context)
        .list_pending_task_admissions(MAX_PENDING_ADMISSIONS_PER_PASS)
        .await?;
    if pending.is_empty() {
        return Ok((session, false));
    }
    // Deadline-expired admissions are excluded by the adapters and settled by
    // the worker's preceding bounded lease recovery. Remaining tickets retain
    // their original admission provenance; this helper cannot invent it.
    session = publish_current(context, session).await?;
    let mut activated = false;
    for ticket in pending {
        match activate_captured(context, &session, ticket).await {
            Ok(TaskActivationOutcome::Activated(_)) => activated = true,
            Ok(
                TaskActivationOutcome::AlreadyActive(_) | TaskActivationOutcome::DetailsExpired(_),
            ) => {}
            // A cancellation or deadline can win after selection. The next
            // recovery/expiry pass settles that original generation.
            Err(ApiError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok((session, activated))
}

fn require_captured_purge(
    session: &CheckpointSession,
    ticket: &TaskPurgeTicket,
) -> Result<(), ApiError> {
    if !session.checkpoint().task_control().is_some_and(|control| {
        control
            .purge_batches()
            .get(&ticket.manifest().id())
            .is_some_and(|batch| batch.ticket() == ticket)
    }) {
        return Err(unavailable(
            "Independent checkpoint does not contain the exact purge manifest",
        ));
    }
    Ok(())
}

fn require_purge_completion(
    session: &CheckpointSession,
    ticket: &TaskPurgeTicket,
    revision: TaskControlRevision,
) -> Result<(), ApiError> {
    require_captured_purge(session, ticket)?;
    if !session.checkpoint().task_control().is_some_and(|control| {
        control.revision() >= revision
            && control
                .purge_batches()
                .get(&ticket.manifest().id())
                .is_some_and(|batch| batch.removed().is_some())
    }) {
        return Err(unavailable(
            "Independent checkpoint does not acknowledge exact purge completion",
        ));
    }
    Ok(())
}

fn charge_progress(
    rows: &mut u32,
    bytes: &mut u64,
    removed_rows: u32,
    removed_bytes: u64,
    budget: TaskPurgeBudget,
) -> Result<(), ApiError> {
    *rows = rows
        .checked_add(removed_rows)
        .filter(|value| *value <= budget.max_rows())
        .ok_or_else(|| unavailable("Task cleanup exceeded its row budget"))?;
    *bytes = bytes
        .checked_add(removed_bytes)
        .filter(|value| *value <= budget.max_bytes())
        .ok_or_else(|| unavailable("Task cleanup exceeded its byte budget"))?;
    Ok(())
}

async fn open_session(configuration: &Arc<Configuration>) -> Result<CheckpointSession, ApiError> {
    let configuration = Arc::clone(configuration);
    tokio::task::spawn_blocking(move || {
        let repository =
            CheckpointRepository::open(&configuration.directory, &configuration.key_file)?;
        if repository.task_receipt_key()?.fingerprint()? != configuration.key.fingerprint()? {
            return Err(unavailable(
                "Control authentication key changed while task admission was running",
            ));
        }
        repository
            .with_payload_cache(Arc::clone(&configuration.payload_cache))
            .into_session()
    })
    .await
    .map_err(|_| unavailable("Task control repository worker failed"))?
}

fn checkpoint_identity(session: &CheckpointSession) -> Result<TaskControlCheckpoint, ApiError> {
    let control = session
        .checkpoint()
        .task_control()
        .ok_or_else(|| unavailable("Missing protected task authority"))?;
    TaskControlCheckpoint::new(
        control.lineage(),
        control.epoch(),
        control.revision(),
        DeletionCheckpointIdentity::new(session.checkpoint().identity().to_owned())
            .map_err(invalid_control)?,
    )
    .map_err(invalid_control)
}

async fn publish_current(
    context: &impl StorageContext,
    mut session: CheckpointSession,
) -> Result<CheckpointSession, ApiError> {
    let capture = storage_handle(context).capture_control_checkpoint().await?;
    tokio::task::spawn_blocking(move || {
        session.publish_control(&capture)?;
        Ok(session)
    })
    .await
    .map_err(|_| unavailable("Task checkpoint publication worker failed"))?
}

async fn expire_one_batch(
    context: &impl StorageContext,
    mut session: CheckpointSession,
) -> Result<(CheckpointSession, bool), ApiError> {
    let storage = storage_handle(context);
    let Some(batch) = storage
        .select_expired_task_receipts(ReceiptExpirySelection::new(64).map_err(invalid_control)?)
        .await?
    else {
        return Ok((session, false));
    };
    // Publish terminal anchors before forgetting their matching fields. A last
    // independent MayExecute record cannot be silently expired by a timestamp.
    session = publish_current(context, session).await?;
    let captured = session
        .checkpoint()
        .task_control()
        .ok_or_else(|| unavailable("Receipt expiry has no independent checkpoint"))?;
    if !batch.receipts().iter().all(|reference| {
        captured
            .receipt(reference.submission())
            .is_some_and(|receipt| {
                receipt.reference() == reference
                    && receipt
                        .expires_at()
                        .is_some_and(|expiry| expiry <= batch.floor())
            })
    }) {
        return Err(unavailable(
            "Independent checkpoint does not protect the exact terminal expiry anchors",
        ));
    }
    let revision = storage
        .expire_task_receipts(
            ProtectedReceiptExpiry::new(batch.clone(), checkpoint_identity(&session)?)
                .map_err(invalid_control)?,
        )
        .await?;
    session = publish_current(context, session).await?;
    if !session.checkpoint().task_control().is_some_and(|control| {
        control.revision() >= revision
            && control.receipt_expiry_floor() >= batch.floor()
            && batch.receipts().iter().all(|reference| {
                control
                    .receipt(reference.submission())
                    .is_none_or(|receipt| receipt.reference() != reference)
            })
    }) {
        return Err(unavailable(
            "Independent checkpoint does not acknowledge exact receipt expiry",
        ));
    }
    Ok((session, true))
}

#[cfg(all(test, unix))]
mod tests;
