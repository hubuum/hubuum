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
use crate::db::DbPool;
use crate::db::traits::event_retention::{
    EventRetentionPurgeSummary, EventRetentionSettings, purge_event_retention_batch_conn,
    select_events_for_retention_purge_conn, try_acquire_event_retention_lock,
};
use crate::db::with_transaction;
use crate::errors::ApiError;
use crate::events::Event;
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};
use crate::restores::{MaintenanceActivityGuard, maintenance_state};

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

fn configured_event_retention_worker() -> EventRetentionWorkerConfig {
    get_config()
        .map(|config| EventRetentionWorkerConfig {
            enabled: config.event_retention_purge_enabled,
            settings: EventRetentionSettings {
                event_retention_days: config.event_retention_days,
                delivery_retention_days: config.event_delivery_retention_days,
                batch_size: config.event_retention_purge_batch_size,
            },
            interval: Duration::from_secs(config.event_retention_purge_interval_seconds),
            file_archive_enabled: config.event_retention_file_archive_enabled,
            archive_path: config.event_retention_archive_path.clone(),
        })
        .unwrap_or(EventRetentionWorkerConfig {
            enabled: DEFAULT_EVENT_RETENTION_PURGE_ENABLED,
            settings: EventRetentionSettings {
                event_retention_days: DEFAULT_EVENT_RETENTION_DAYS,
                delivery_retention_days: DEFAULT_EVENT_DELIVERY_RETENTION_DAYS,
                batch_size: DEFAULT_EVENT_RETENTION_PURGE_BATCH_SIZE,
            },
            interval: Duration::from_secs(DEFAULT_EVENT_RETENTION_PURGE_INTERVAL_SECONDS),
            file_archive_enabled: DEFAULT_EVENT_RETENTION_FILE_ARCHIVE_ENABLED,
            archive_path: None,
        })
}

pub async fn process_event_retention_batch(
    pool: &DbPool,
    settings: EventRetentionSettings,
    archive_path: Option<&Path>,
) -> Result<EventRetentionPurgeSummary, ApiError> {
    let _activity = MaintenanceActivityGuard::begin();
    if maintenance_state(pool).await? != "normal" {
        return Ok(EventRetentionPurgeSummary::default());
    }
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
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
    pool: DbPool,
    config: EventRetentionWorkerConfig,
    shutdown: ShutdownSignal,
) {
    loop {
        let archive_path = config.file_archive_path().map(Path::new);
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            result = process_event_retention_batch(&pool, config.settings, archive_path) => result,
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

fn spawn_event_retention_worker_loop(pool: DbPool, config: EventRetentionWorkerConfig) {
    spawn_background_worker("event-retention-worker", move |shutdown| {
        info!(
            message = "Starting event retention worker loop",
            event_retention_days = config.settings.event_retention_days,
            delivery_retention_days = config.settings.delivery_retention_days,
            batch_size = config.settings.batch_size,
            interval = ?config.interval,
            file_archive_enabled = config.file_archive_enabled,
            archive_path_configured = config.archive_path.is_some()
        );
        let system = actix_rt::System::new();
        system.block_on(event_retention_worker_loop(pool, config, shutdown));
    });
}

pub fn ensure_event_retention_worker_running(pool: DbPool) {
    if get_config().is_ok_and(|config| !config.runtime_role.runs_background_workers()) {
        return;
    }
    let config = configured_event_retention_worker();
    if !config.enabled {
        return;
    }

    EVENT_RETENTION_WORKER.call_once(move || {
        info!(
            message = "Initializing event retention worker",
            event_retention_days = config.settings.event_retention_days,
            delivery_retention_days = config.settings.delivery_retention_days,
            batch_size = config.settings.batch_size,
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
    options.mode(0o600);
    let mut file = options.open(path).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to open event archive: {error}"))
    })?;

    write_event_archive(&mut file, archived_at, events)
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
        std::fs::remove_file(path).unwrap();
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
