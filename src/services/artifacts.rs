//! Application artifact ownership, bounded transfer and authorized-reader transport.
use std::io;
use std::sync::{Arc, OnceLock};

use actix_web::web::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{Stream, stream};
use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactReader, ArtifactReference,
    ArtifactReservation, Destination, DestinationId, TaskArtifactStore,
};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::services::tasks::ClaimedTask;
use crate::storage::{StorageContext, storage_handle};

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
        context: &impl StorageContext,
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
        let store = storage_handle(context).artifact_store();
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
        store.stage(&identity, limits).await.map_err(error)?;
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
        if let Err(failure) = store
            .bind_task(&identity, task.lease(), reservation, expires)
            .await
        {
            let _ = store.abort(&identity).await;
            return Err(error(failure));
        }
        Ok(Self {
            store,
            identity,
            limits,
        })
    }
}

/// Call only after live task/output authorization. Stream completion is withheld
/// until the reader checks the last block, exact length and full-stream digest.
pub(crate) fn download(
    context: &impl StorageContext,
    reference: ArtifactReference,
) -> impl Stream<Item = Result<Bytes, io::Error>> + 'static {
    let store = storage_handle(context).artifact_store();
    // The generator owns the store. Opening and all block reads happen inside
    // the future, so the borrowed reader cannot outlive its storage handle.
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let worker = actix_web::rt::spawn(async move {
        let result = async {
            let mut reader = ArtifactReader::open(store.as_ref(), &reference)
                .await
                .map_err(error)?;
            while let Some(block) = reader.next_block().await.map_err(error)? {
                if sender.send(Ok(Bytes::from(block))).await.is_err() {
                    return Ok(());
                }
            }
            Ok::<_, ApiError>(())
        }
        .await;
        if let Err(error) = result {
            let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
        }
    });
    stream::unfold(Some((receiver, worker)), |state| async {
        let (mut receiver, worker) = state?;
        if let Some(value) = receiver.recv().await {
            Some((value, Some((receiver, worker))))
        } else {
            // A panicking read task must not turn a truncated stream into EOF.
            worker
                .await
                .err()
                .map(|error| (Err(io::Error::other(error)), None))
        }
    })
}
