//! Application artifact ownership, bounded transfer and authorized-reader transport.
use std::future::ready;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, OnceLock};
use std::task::Poll;

use actix_web::web::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{FutureExt, Stream, stream};
use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactReader, ArtifactReference,
    ArtifactReservation, Destination, DestinationId, TaskArtifactStore,
};
use hubuum_storage_core::{
    HistoryLifecycleStorage, HistoryPublicationKind, HistoryPublicationOwner,
};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::models::TaskStatus;
use crate::restores::ApplicationWorkCompletion;
use crate::services::tasks::ClaimedTask;
use crate::storage::{
    ExecutionStorage, StorageContext, StorageExecutionScope, StorageHandle, storage_handle,
};

/// Owns one backup/export from its first domain read through joined producers,
/// final task publication or positively completed cleanup. Drop is not proof.
pub(crate) struct TaskPublication {
    storage: StorageHandle,
    scope: StorageExecutionScope,
    owner: Option<(HistoryPublicationOwner, ApplicationWorkCompletion)>,
    pending: bool,
}

impl TaskPublication {
    pub(crate) async fn admit(
        storage: &StorageHandle,
        kind: Option<HistoryPublicationKind>,
    ) -> Result<Self, ApiError> {
        let mut scope = storage.current_execution_scope();
        let owner = if storage.uses_online_history()
            && let Some(kind) = kind
        {
            if scope.history_read_override().is_some() {
                return Err(ApiError::ServiceUnavailable(
                    "Task publication cannot replace an inherited owner".into(),
                ));
            }
            let execution = scope
                .task_execution_override()
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    ApiError::ServiceUnavailable(
                        "Owned task publication requires supervised execution".into(),
                    )
                })?;
            execution.check()?;
            scope = scope.with_application_access(storage.admit_application_work().await?);
            let owner = storage
                .run_in_scope(scope.clone(), storage.admit_history_publication(kind))
                .await?;
            scope = scope.with_history_read(owner.context());
            Some(owner)
        } else {
            None
        };
        Ok(Self {
            storage: storage.clone(),
            scope,
            owner: owner.map(|owner| (owner, ApplicationWorkCompletion::begin())),
            pending: false,
        })
    }

    pub(crate) fn scope(&self) -> StorageExecutionScope {
        self.scope.clone()
    }

    pub(crate) async fn settle(
        mut self,
        result: Result<TaskStatus, ApiError>,
    ) -> Result<TaskStatus, ApiError> {
        if let Some((owner, mut completion)) = self.owner.take() {
            if self.pending {
                tracing::warn!("Task publication retains an unconfirmed output driver");
                return result.and_then(|_| {
                    Err(ApiError::ServiceUnavailable(
                        "Task publication cleanup remains unconfirmed".into(),
                    ))
                });
            }
            // Every stage producer has been joined and its spool released;
            // TaskOutput confirmed either final publication or awaited abort.
            let settlement = owner.after_drained(ready(Ok(())), ready(Ok(()))).await?;
            if let Err(error) = self.storage.settle_history_publication(settlement).await {
                tracing::warn!(%error, "Task publication settlement remains charged");
                return result.and_then(|_| Err(error.into()));
            }
            completion.confirm();
        }
        result
    }
}

#[cfg(test)]
mod tests;

const _: () = assert!(
    hubuum_templates::MAX_WORKER_EXECUTION_TIME.as_secs()
        < hubuum_storage_core::artifacts::LOCAL_SPOOL_CLEANUP_GRACE.as_secs()
);

pub(crate) fn error(error: ArtifactError) -> ApiError {
    match error {
        ArtifactError::Limit(message) => ApiError::PayloadTooLarge(message),
        ArtifactError::Invalid(message) => ApiError::BadRequest(message),
        ArtifactError::Conflict(message) => ApiError::Conflict(message),
        ArtifactError::NotFound => ApiError::NotFound("Artifact output is unavailable".into()),
        ArtifactError::Corrupt(message) | ArtifactError::Backend(message) => {
            ApiError::InternalServerError(message)
        }
        error @ (ArtifactError::Source(_) | ArtifactError::AbortFailed { .. }) => {
            // Only the private operation bridge may validate and consume its
            // original typed admission cause. Generic artifact callers fail closed.
            ApiError::InternalServerError(error.to_string())
        }
    }
}

pub(crate) fn spool_limit() -> u64 {
    crate::config::get_config()
        .map(|config| config.artifacts.max_spool_bytes)
        .unwrap_or_else(|_| crate::config::artifacts::ArtifactOptions::default().max_spool_bytes)
}

pub(crate) struct TaskOutput {
    pub store: Arc<dyn TaskArtifactStore>,
    pub identity: ArtifactIdentity,
    pub limits: ArtifactLimits,
}
impl TaskOutput {
    pub async fn begin(
        publication: &mut TaskPublication,
        task: &ClaimedTask,
        max_bytes: usize,
        spool_bytes: u64,
        expires: DateTime<Utc>,
    ) -> Result<Self, ApiError> {
        static WORKER: OnceLock<[u8; 16]> = OnceLock::new();
        let worker = *WORKER.get_or_init(|| *Uuid::new_v4().as_bytes());
        let blocks = (max_bytes as u64)
            .div_ceil(1024 * 1024)
            .try_into()
            .map_err(|_| ApiError::PayloadTooLarge("Artifact block count overflow".into()))?;
        let manifest_bytes = if blocks <= ArtifactLimits::default().blocks() {
            ArtifactLimits::default().manifest_bytes()
        } else {
            ArtifactLimits::format_ceiling().manifest_bytes()
        };
        let limits = ArtifactLimits::builder()
            .artifact_bytes(max_bytes as u64)
            .blocks(blocks)
            .manifest_bytes(manifest_bytes)
            .build()
            .map_err(error)?;
        let store = publication.storage.artifact_store();
        // Stable identity for this directly readable primary-backend destination.
        let destination = Destination::new(
            DestinationId::from_bytes([0x50; 16]).map_err(error)?,
            "primary-artifacts",
        )
        .map_err(error)?;
        store
            .register_destination(&destination)
            .await
            .map_err(error)?;
        let identity = ArtifactIdentity::new(destination.id());
        let reservation = ArtifactReservation::new(
            worker,
            spool_bytes,
            crate::config::get_config()
                .map(|config| config.artifacts.worker_resident_bytes)
                .unwrap_or_else(|_| {
                    crate::config::artifacts::ArtifactOptions::default().worker_resident_bytes
                }),
        )
        .map_err(error)?;
        publication.pending = true;
        if let Err(failure) = store.stage(&identity, limits).await {
            return Err(abort_output(store.as_ref(), &identity, publication, failure).await);
        }
        if let Err(failure) = store
            .bind_task(&identity, task.lease(), reservation, expires)
            .await
        {
            return Err(abort_output(store.as_ref(), &identity, publication, failure).await);
        }
        Ok(Self {
            store,
            identity,
            limits,
        })
    }

    pub(crate) async fn finish(
        &self,
        publication: &mut TaskPublication,
        result: Result<TaskStatus, ApiError>,
    ) -> Result<TaskStatus, ApiError> {
        if !matches!(result, Ok(TaskStatus::Succeeded))
            && let Err(cleanup) = self.store.abort(&self.identity).await
        {
            tracing::warn!(%cleanup, "Task output abort remains unconfirmed");
            return result.and_then(|_| Err(error(cleanup)));
        }
        publication.pending = false;
        result
    }
}

async fn abort_output(
    store: &dyn TaskArtifactStore,
    identity: &ArtifactIdentity,
    publication: &mut TaskPublication,
    primary: ArtifactError,
) -> ApiError {
    match store.abort(identity).await {
        Ok(()) => {
            publication.pending = false;
            error(primary)
        }
        Err(abort) => error(ArtifactError::AbortFailed {
            primary: Box::new(primary),
            abort: Box::new(abort),
        }),
    }
}

/// Call only after live task/output authorization. Stream completion is withheld
/// until the reader checks the last block, exact length and full-stream digest.
pub(crate) fn download(
    context: &impl StorageContext,
    reference: ArtifactReference,
) -> impl Stream<Item = Result<Bytes, io::Error>> + 'static {
    let storage = storage_handle(context);
    let scope = storage.current_execution_scope();
    let store = storage.artifact_store();
    // Poll the reader as part of the body, preserving its original scope even
    // after the handler has returned. Dropping the body drops the reader future
    // and its bounded queued block; no detached task can continue producing.
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let reader = async move {
        let result = storage
            .run_in_scope(scope, async {
                let mut reader = ArtifactReader::open(store.as_ref(), &reference)
                    .await
                    .map_err(error)?;
                while let Some(block) = reader.next_block().await.map_err(error)? {
                    if sender.send(Ok(Bytes::from(block))).await.is_err() {
                        return Ok(());
                    }
                }
                Ok::<_, ApiError>(())
            })
            .await;
        if let Err(error) = result {
            let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
        }
    };
    let mut reader = Some(Box::pin(AssertUnwindSafe(reader).catch_unwind()));
    let mut failed = false;
    stream::poll_fn(move |cx| {
        if failed {
            return Poll::Ready(None);
        }
        if let Some(work) = &mut reader
            && let Poll::Ready(result) = work.as_mut().poll(cx)
        {
            reader = None;
            if result.is_err() {
                // As with a failed joined task, a panic must not become EOF.
                failed = true;
                receiver.close();
                return Poll::Ready(Some(Err(io::Error::other("Artifact reader panicked"))));
            }
        }
        receiver.poll_recv(cx)
    })
}
