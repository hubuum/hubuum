use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;

use actix_rt::time::sleep;
use async_trait::async_trait;
use serde::Serialize;
use tracing::{error, info};

use crate::config::{
    DEFAULT_EVENT_DELIVERY_RETENTION_DAYS, DEFAULT_EVENT_RETENTION_DAYS,
    DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED, DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE,
    DEFAULT_EVENT_RETENTION_PURGE_ENABLED, DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS,
    get_config,
};
use crate::errors::ApiError;
use crate::events::EventRetentionSettings;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::restores::MaintenanceActivityGuard;
use crate::storage::StorageContext;
use crate::storage::{
    EventArchiveSink, EventRetentionBatch, EventRetentionSummary, RetainedEvent, StorageError,
    StorageHandle, execute_event_retention_batch, storage_handle,
};
use crate::storage::{StorageCallSite, with_storage_call_site};

static EVENT_RETENTION_WORKER: std::sync::Once = std::sync::Once::new();

#[derive(Debug, Clone)]
struct EventRetentionWorkerConfig {
    enabled: bool,
    settings: EventRetentionSettings,
    interval: Duration,
    file_archive_enabled: bool,
    archive_path: Option<String>,
}

#[derive(Debug, Serialize)]
struct ArchivedEventRecord<'a> {
    retention_batch_id: uuid::Uuid,
    archived_at: chrono::NaiveDateTime,
    event: &'a serde_json::Value,
}

struct FileEventArchiveSink<'a> {
    path: Option<&'a Path>,
}

#[async_trait]
impl EventArchiveSink for FileEventArchiveSink<'_> {
    async fn archive(&self, batch: &EventRetentionBatch) -> Result<(), StorageError> {
        let Some(path) = self.path.filter(|_| !batch.is_empty()) else {
            return Ok(());
        };
        let path = path.to_path_buf();
        let batch = batch.clone();
        tokio::task::spawn_blocking(move || archive_event_batch(&path, &batch))
            .await
            .map_err(|error| {
                StorageError::internal(format!("Event archive task failed to complete: {error}"))
            })?
            .map_err(|error| {
                StorageError::internal(format!("Event archive output failed: {error}"))
            })
    }
}

trait EventArchiveSinkOutput: Write {
    fn sync_all(&self) -> io::Result<()>;
}

impl EventArchiveSinkOutput for File {
    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }
}

fn configured_event_retention_worker() -> Result<EventRetentionWorkerConfig, ApiError> {
    match get_config() {
        Ok(config) => Ok(EventRetentionWorkerConfig {
            enabled: config.event_retention_purge_enabled,
            settings: config.event_retention_settings()?,
            interval: Duration::from_secs(config.event_retention_purge_interval_seconds),
            file_archive_enabled: config.event_retention_file_archive_enabled,
            archive_path: config.event_retention_archive_path.clone(),
        }),
        Err(_) => Ok(EventRetentionWorkerConfig {
            enabled: DEFAULT_EVENT_RETENTION_PURGE_ENABLED,
            settings: EventRetentionSettings::new(
                DEFAULT_EVENT_RETENTION_DAYS,
                DEFAULT_EVENT_DELIVERY_RETENTION_DAYS,
                DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE,
            )
            .map_err(ApiError::from)?,
            interval: Duration::from_secs(DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS),
            file_archive_enabled: DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED,
            archive_path: None,
        }),
    }
}

pub async fn process_event_retention_batch(
    pool: &impl crate::storage::StorageContext,
    settings: EventRetentionSettings,
    archive_path: Option<&Path>,
) -> Result<EventRetentionSummary, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    let storage = storage_handle(pool);
    execute_event_retention_batch(
        &storage,
        settings,
        &FileEventArchiveSink { path: archive_path },
    )
    .await
    .map_err(Into::into)
}

fn retention_worker_should_continue(result: &Result<EventRetentionSummary, ApiError>) -> bool {
    match result {
        Ok(summary) => summary.did_work(),
        Err(error) => {
            error!(message = "Event retention worker iteration failed", error = %error);
            false
        }
    }
}

async fn event_retention_worker_loop(
    pool: StorageHandle,
    config: EventRetentionWorkerConfig,
    shutdown: ShutdownSignal,
) {
    loop {
        let archive_path = config.file_archive_path().map(Path::new);
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            result = with_storage_call_site(
                &pool,
                StorageCallSite::EventRetention,
                process_event_retention_batch(&pool, config.settings, archive_path),
            ) => result,
        };
        if retention_worker_should_continue(&result) {
            continue;
        }
        tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            _ = sleep(config.interval) => {}
        }
    }
}

fn spawn_event_retention_worker_loop(pool: StorageHandle, config: EventRetentionWorkerConfig) {
    spawn_background_worker("event-retention-worker", move |shutdown| {
        info!(
            message = "Starting event retention worker loop",
            event_retention_days = config.settings.event_retention_days(),
            delivery_retention_days = config.settings.delivery_retention_days(),
            batch_size = config.settings.batch_size(),
            interval = ?config.interval,
            file_archive_enabled = config.file_archive_enabled,
            archive_path_configured = config.archive_path.is_some()
        );
        let system = actix_rt::System::new();
        system.block_on(event_retention_worker_loop(pool, config, shutdown));
    });
}

pub fn ensure_event_retention_worker_running<C>(backend: C)
where
    C: StorageContext,
{
    let pool = storage_handle(&backend);
    if get_config().is_ok_and(|config| !config.runtime_role.runs_background_workers()) {
        return;
    }
    let config = match configured_event_retention_worker() {
        Ok(config) => config,
        Err(error) => {
            error!(message = "Event retention settings are invalid", error = %error);
            return;
        }
    };
    if !config.enabled {
        return;
    }

    EVENT_RETENTION_WORKER.call_once(move || {
        info!(
            message = "Initializing event retention worker",
            event_retention_days = config.settings.event_retention_days(),
            delivery_retention_days = config.settings.delivery_retention_days(),
            batch_size = config.settings.batch_size(),
            interval = ?config.interval,
            file_archive_enabled = config.file_archive_enabled,
            archive_path_configured = config.archive_path.is_some()
        );
        spawn_event_retention_worker_loop(pool, config);
    });
}

fn archive_event_batch(path: &Path, batch: &EventRetentionBatch) -> Result<(), ApiError> {
    secure_event_archive_directory(path)?;
    let final_path = path.join(format!("{}.jsonl", batch.id().as_uuid()));
    if final_path.try_exists().map_err(|error| {
        ApiError::InternalServerError(format!("Failed to inspect event archive batch: {error}"))
    })? {
        validate_existing_archive_batch(&final_path, batch)?;
        return Ok(());
    }

    let temporary_path = path.join(format!(
        ".{}.{}.tmp",
        batch.id().as_uuid(),
        uuid::Uuid::new_v4()
    ));
    let archived_at = chrono::Utc::now().naive_utc();
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let mut file = options.open(&temporary_path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to open event archive: {error}"))
    })?;
    secure_event_archive_file(&file).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to secure event archive: {error}"))
    })?;
    if let Err(error) =
        write_event_archive(&mut file, batch.id().as_uuid(), archived_at, batch.events())
    {
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }
    if let Err(error) = commit_event_archive_file(&temporary_path, &final_path, path) {
        let _ = fs::remove_file(&temporary_path);
        if final_path.try_exists().unwrap_or(false) {
            validate_existing_archive_batch(&final_path, batch)?;
            return Ok(());
        }
        return Err(ApiError::InternalServerError(format!(
            "Failed to commit event archive batch: {error}"
        )));
    }
    Ok(())
}

#[cfg(not(windows))]
fn commit_event_archive_file(
    temporary_path: &Path,
    final_path: &Path,
    directory: &Path,
) -> std::io::Result<()> {
    fs::rename(temporary_path, final_path)?;
    File::open(directory)?.sync_all()
}

#[cfg(windows)]
fn commit_event_archive_file(
    temporary_path: &Path,
    final_path: &Path,
    _directory: &Path,
) -> std::io::Result<()> {
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "MoveFileExW"]
        fn move_file_ex_w(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let temporary_path: Vec<u16> = temporary_path
        .as_os_str()
        .encode_wide()
        .chain(once(0))
        .collect();
    let final_path: Vec<u16> = final_path
        .as_os_str()
        .encode_wide()
        .chain(once(0))
        .collect();
    // SAFETY: Both paths are valid, nul-terminated UTF-16 buffers that remain
    // alive for the duration of the call.
    let result = unsafe {
        move_file_ex_w(
            temporary_path.as_ptr(),
            final_path.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn secure_event_archive_directory(path: &Path) -> Result<(), ApiError> {
    fs::create_dir_all(path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to create event archive directory: {error}"))
    })?;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to inspect event archive directory: {error}"
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ApiError::InternalServerError(
            "Event archive path must be a real directory".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
            ApiError::InternalServerError(format!(
                "Failed to secure event archive directory: {error}"
            ))
        })?;
    }
    Ok(())
}

fn validate_existing_archive_batch(
    path: &Path,
    batch: &EventRetentionBatch,
) -> Result<(), ApiError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to inspect event archive batch: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ApiError::InternalServerError(
            "Event archive batch must be a regular file".to_string(),
        ));
    }
    let contents = fs::read_to_string(path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to read event archive batch: {error}"))
    })?;
    let archived = contents
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).map_err(|error| {
                ApiError::InternalServerError(format!(
                    "Existing event archive batch is invalid JSON: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if archived.len() != batch.events().len() {
        return Err(ApiError::InternalServerError(
            "Existing event archive batch does not match the retention claim".to_string(),
        ));
    }
    let batch_id = batch.id().as_uuid().to_string();
    for (record, retained) in archived.iter().zip(batch.events()) {
        let archived_batch_id = record
            .get("retention_batch_id")
            .and_then(serde_json::Value::as_str);
        let archived_event = record.get("event");
        let retained_event: serde_json::Value =
            serde_json::from_str(retained.json()).map_err(|error| {
                ApiError::InternalServerError(format!(
                    "Storage returned an invalid retained event document: {error}"
                ))
            })?;
        if archived_batch_id != Some(batch_id.as_str()) || archived_event != Some(&retained_event) {
            return Err(ApiError::InternalServerError(
                "Existing event archive batch does not match the retention claim".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn secure_event_archive_file(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "event archive must be a regular file",
        ));
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn secure_event_archive_file(_file: &File) -> io::Result<()> {
    Ok(())
}

fn write_event_archive(
    file: &mut impl EventArchiveSinkOutput,
    retention_batch_id: uuid::Uuid,
    archived_at: chrono::NaiveDateTime,
    events: &[RetainedEvent],
) -> Result<(), ApiError> {
    for event in events {
        let event = serde_json::from_str(event.json()).map_err(|error| {
            ApiError::InternalServerError(format!(
                "Storage returned an invalid retained event document: {error}"
            ))
        })?;
        let record = ArchivedEventRecord {
            retention_batch_id,
            archived_at,
            event: &event,
        };
        serde_json::to_writer(&mut *file, &record).map_err(|error| {
            ApiError::InternalServerError(format!("Failed to serialize event archive: {error}"))
        })?;
        file.write_all(b"\n").map_err(|error| {
            ApiError::InternalServerError(format!("Failed to write event archive: {error}"))
        })?;
    }

    file.flush().map_err(|error| {
        ApiError::InternalServerError(format!("Failed to flush event archive: {error}"))
    })?;
    file.sync_all().map_err(|error| {
        ApiError::InternalServerError(format!("Failed to sync event archive: {error}"))
    })
}

impl EventRetentionWorkerConfig {
    fn file_archive_path(&self) -> Option<&str> {
        self.file_archive_enabled
            .then_some(self.archive_path.as_deref())
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct ArchiveOutputSpy {
        bytes: Vec<u8>,
        sync_called: Cell<bool>,
        sync_error: Option<io::ErrorKind>,
    }

    impl Write for ArchiveOutputSpy {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl EventArchiveSinkOutput for ArchiveOutputSpy {
        fn sync_all(&self) -> io::Result<()> {
            self.sync_called.set(true);
            self.sync_error
                .map_or(Ok(()), |kind| Err(io::Error::from(kind)))
        }
    }

    fn event() -> RetainedEvent {
        let event = serde_json::json!({
            "id": 1,
            "event_id": Uuid::new_v4(),
            "occurred_at": chrono::Utc::now().naive_utc(),
            "entity_type": "collection",
            "entity_id": 1,
            "entity_name": "example",
            "collection_id": 1,
            "action": "created",
            "actor_user_id": null,
            "actor_kind": "system",
            "request_id": null,
            "correlation_id": null,
            "summary": "collection created",
            "before": null,
            "after": null,
            "metadata": {},
            "schema_version": 1,
            "dispatched_at": null,
            "fanout_locked_until": null,
            "fanout_claim_token": null,
            "initiator_user_id": 17,
            "task_id": 18,
            "before_revision": null,
            "after_revision": null,
        });
        RetainedEvent::try_new(
            crate::events::EventSequence::new(1).unwrap(),
            serde_json::to_string(&event).unwrap(),
        )
        .unwrap()
    }

    fn batch() -> EventRetentionBatch {
        EventRetentionBatch::new(
            crate::storage::EventRetentionBatchId::new(Uuid::new_v4()),
            vec![event()],
        )
    }

    fn archive_directory() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("hubuum-event-archive-{}", Uuid::new_v4()))
    }

    #[test]
    fn retention_worker_retries_immediately_after_deleting_rows() {
        assert!(retention_worker_should_continue(&Ok(
            EventRetentionSummary::new(1, 0),
        )));
        assert!(retention_worker_should_continue(&Ok(
            EventRetentionSummary::new(0, 1),
        )));
        assert!(!retention_worker_should_continue(&Ok(
            EventRetentionSummary::default(),
        )));
        assert!(!retention_worker_should_continue(&Err(
            ApiError::InternalServerError("boom".to_string()),
        )));
    }

    #[test]
    fn event_archive_writes_one_atomic_file_per_batch() {
        let path = archive_directory();
        let batch = batch();
        archive_event_batch(&path, &batch).unwrap();

        let batch_path = path.join(format!("{}.jsonl", batch.id().as_uuid()));
        let archived = std::fs::read_to_string(batch_path).unwrap();

        assert_eq!(archived.lines().count(), 1);
        assert!(archived.contains(&batch.id().as_uuid().to_string()));
        assert!(archived.contains("\"archived_at\""));
        assert!(archived.contains("\"event\""));
        assert!(archived.contains("\"entity_type\":\"collection\""));
        assert!(archived.contains("\"initiator_user_id\":17"));
        assert!(archived.contains("\"task_id\":18"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn event_archive_restricts_directory_and_batch_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = archive_directory();
        let batch = batch();
        archive_event_batch(&path, &batch).unwrap();

        let batch_path = path.join(format!("{}.jsonl", batch.id().as_uuid()));

        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(batch_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn event_archive_rejects_symbolic_link_directory() {
        use std::os::unix::fs::symlink;

        let suffix = Uuid::new_v4();
        let target = std::env::temp_dir().join(format!("hubuum-event-archive-{suffix}"));
        let link = std::env::temp_dir().join(format!("hubuum-event-archive-{suffix}.link"));
        std::fs::create_dir(&target).unwrap();
        symlink(&target, &link).unwrap();

        let result = archive_event_batch(&link, &batch());

        assert!(matches!(
            result,
            Err(ApiError::InternalServerError(message))
                if message == "Event archive path must be a real directory"
        ));
        std::fs::remove_file(link).unwrap();
        std::fs::remove_dir(target).unwrap();
    }

    #[test]
    fn event_archive_is_idempotent_by_batch_id() {
        let path = archive_directory();
        let batch = batch();

        archive_event_batch(&path, &batch).unwrap();
        archive_event_batch(&path, &batch).unwrap();

        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn event_archive_rejects_an_existing_file_for_different_contents() {
        let path = archive_directory();
        let batch = batch();
        std::fs::create_dir_all(&path).unwrap();
        let batch_path = path.join(format!("{}.jsonl", batch.id().as_uuid()));
        std::fs::write(
            &batch_path,
            format!(
                "{{\"retention_batch_id\":\"{}\",\"archived_at\":\"2026-01-01T00:00:00\",\"event\":{{\"id\":999}}}}\n",
                batch.id().as_uuid(),
            ),
        )
        .unwrap();

        let result = archive_event_batch(&path, &batch);

        assert!(matches!(
            result,
            Err(ApiError::InternalServerError(message))
                if message == "Existing event archive batch does not match the retention claim"
        ));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn secure_event_archive_rejects_non_regular_file() {
        let path = std::env::temp_dir().join(format!("hubuum-event-archive-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        let directory = File::open(&path).unwrap();

        let error = secure_event_archive_file(&directory).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        std::fs::remove_dir(path).unwrap();
    }

    #[test]
    fn event_archive_is_synced_after_writing() {
        let mut output = ArchiveOutputSpy::default();

        write_event_archive(
            &mut output,
            Uuid::new_v4(),
            chrono::Utc::now().naive_utc(),
            &[event()],
        )
        .unwrap();

        assert!(output.sync_called.get());
        assert_eq!(output.bytes.last(), Some(&b'\n'));
    }

    #[test]
    fn event_archive_sync_failure_is_returned() {
        let mut output = ArchiveOutputSpy {
            sync_error: Some(io::ErrorKind::Other),
            ..ArchiveOutputSpy::default()
        };

        let result = write_event_archive(
            &mut output,
            Uuid::new_v4(),
            chrono::Utc::now().naive_utc(),
            &[event()],
        );

        assert!(matches!(
            result,
            Err(ApiError::InternalServerError(message))
                if message.starts_with("Failed to sync event archive:")
        ));
    }
}
