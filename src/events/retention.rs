use std::fs::{File, OpenOptions};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;

use actix_rt::time::sleep;
use serde::Serialize;
use tracing::{error, info};

use crate::config::{
    DEFAULT_EVENT_DELIVERY_RETENTION_DAYS, DEFAULT_EVENT_RETENTION_DAYS,
    DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED, DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE,
    DEFAULT_EVENT_RETENTION_PURGE_ENABLED, DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS,
    get_config,
};
use crate::errors::ApiError;
use crate::events::{Event, EventRetentionSettings};
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::restores::MaintenanceActivityGuard;
use crate::storage::StorageContext;
use crate::storage::postgres::operations::event_retention::{
    EventRetentionPurgeSummary, purge_event_retention_batch_conn,
    select_events_for_retention_purge_conn, try_acquire_event_retention_lock,
};
use crate::storage::postgres::operations::maintenance::maintenance_state_conn;
use crate::storage::postgres::with_transaction;
use crate::storage::postgres::{StorageCallSite, with_storage_call_site};
use crate::storage::{StorageHandle, storage_handle};

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
    archived_at: chrono::NaiveDateTime,
    event: &'a Event,
}

trait EventArchiveOutput: Write {
    fn sync_all(&self) -> io::Result<()>;
}

impl EventArchiveOutput for File {
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
            .map_err(ApiError::BadRequest)?,
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
) -> Result<EventRetentionPurgeSummary, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        if !maintenance_state_conn(conn).await?.is_normal() {
            return Ok(EventRetentionPurgeSummary::default());
        }
        if !try_acquire_event_retention_lock(conn).await? {
            return Ok(EventRetentionPurgeSummary::default());
        }

        let events = select_events_for_retention_purge_conn(conn, settings).await?;
        if let Some(path) = archive_path
            && !events.is_empty()
        {
            append_event_archive(path, &events)?;
        }

        let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
        purge_event_retention_batch_conn(conn, settings, &event_ids).await
    })
    .await
}

fn retention_worker_should_continue(result: &Result<EventRetentionPurgeSummary, ApiError>) -> bool {
    match result {
        Ok(summary) => summary.purged_events > 0 || summary.purged_terminal_deliveries > 0,
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

fn append_event_archive(path: &Path, events: &[Event]) -> Result<(), ApiError> {
    let archived_at = chrono::Utc::now().naive_utc();
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let mut file = options.open(path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to open event archive: {error}"))
    })?;
    secure_event_archive_file(&file).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to secure event archive: {error}"))
    })?;

    write_event_archive(&mut file, archived_at, events)
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
    file: &mut impl EventArchiveOutput,
    archived_at: chrono::NaiveDateTime,
    events: &[Event],
) -> Result<(), ApiError> {
    for event in events {
        let record = ArchivedEventRecord { archived_at, event };
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

    impl EventArchiveOutput for ArchiveOutputSpy {
        fn sync_all(&self) -> io::Result<()> {
            self.sync_called.set(true);
            self.sync_error
                .map_or(Ok(()), |kind| Err(io::Error::from(kind)))
        }
    }

    fn event() -> Event {
        Event {
            id: 1,
            event_id: Uuid::new_v4(),
            occurred_at: chrono::Utc::now().naive_utc(),
            entity_type: "collection".to_string(),
            entity_id: Some(1),
            entity_name: Some("example".to_string()),
            collection_id: Some(1),
            action: "created".to_string(),
            actor_user_id: None,
            actor_kind: "system".to_string(),
            request_id: None,
            correlation_id: None,
            summary: "collection created".to_string(),
            before: None,
            after: None,
            metadata: serde_json::json!({}),
            schema_version: 1,
            dispatched_at: None,
            fanout_locked_until: None,
            fanout_claim_token: None,
            initiator_user_id: Some(17),
            task_id: Some(18),
            before_revision: None,
            after_revision: None,
        }
    }

    #[test]
    fn retention_worker_retries_immediately_after_deleting_rows() {
        assert!(retention_worker_should_continue(&Ok(
            EventRetentionPurgeSummary {
                purged_events: 1,
                purged_terminal_deliveries: 0,
            },
        )));
        assert!(retention_worker_should_continue(&Ok(
            EventRetentionPurgeSummary {
                purged_events: 0,
                purged_terminal_deliveries: 1,
            },
        )));
        assert!(!retention_worker_should_continue(&Ok(
            EventRetentionPurgeSummary::default(),
        )));
        assert!(!retention_worker_should_continue(&Err(
            ApiError::InternalServerError("boom".to_string()),
        )));
    }

    #[test]
    fn append_event_archive_writes_json_lines() {
        let path =
            std::env::temp_dir().join(format!("hubuum-event-archive-{}.jsonl", Uuid::new_v4()));
        append_event_archive(&path, &[event()]).unwrap();

        let archived = std::fs::read_to_string(&path).unwrap();

        assert_eq!(archived.lines().count(), 1);
        assert!(archived.contains("\"archived_at\""));
        assert!(archived.contains("\"event\""));
        assert!(archived.contains("\"entity_type\":\"collection\""));
        assert!(archived.contains("\"initiator_user_id\":17"));
        assert!(archived.contains("\"task_id\":18"));
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn append_event_archive_restricts_existing_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("hubuum-event-archive-{}.jsonl", Uuid::new_v4()));
        std::fs::write(&path, b"").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        append_event_archive(&path, &[event()]).unwrap();

        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn append_event_archive_rejects_symbolic_link_path() {
        use std::os::unix::fs::symlink;

        let suffix = Uuid::new_v4();
        let target = std::env::temp_dir().join(format!("hubuum-event-archive-{suffix}.jsonl"));
        let link = std::env::temp_dir().join(format!("hubuum-event-archive-{suffix}.link"));
        std::fs::write(&target, b"existing\n").unwrap();
        symlink(&target, &link).unwrap();

        let result = append_event_archive(&link, &[event()]);

        assert!(matches!(
            result,
            Err(ApiError::InternalServerError(message))
                if message.starts_with("Failed to open event archive:")
        ));
        assert_eq!(std::fs::read(&target).unwrap(), b"existing\n");
        std::fs::remove_file(link).unwrap();
        std::fs::remove_file(target).unwrap();
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

        write_event_archive(&mut output, chrono::Utc::now().naive_utc(), &[event()]).unwrap();

        assert!(output.sync_called.get());
        assert_eq!(output.bytes.last(), Some(&b'\n'));
    }

    #[test]
    fn event_archive_sync_failure_is_returned() {
        let mut output = ArchiveOutputSpy {
            sync_error: Some(io::ErrorKind::Other),
            ..ArchiveOutputSpy::default()
        };

        let result = write_event_archive(&mut output, chrono::Utc::now().naive_utc(), &[event()]);

        assert!(matches!(
            result,
            Err(ApiError::InternalServerError(message))
                if message.starts_with("Failed to sync event archive:")
        ));
    }
}
