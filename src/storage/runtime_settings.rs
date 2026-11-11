//! Process-local application of shared runtime settings.
//!
//! Storage is the authority. Each process keeps the last snapshot it loaded
//! successfully so consumers read settings synchronously, without a storage
//! round trip on request paths.
use std::{
    future::Future,
    sync::{Arc, RwLock},
    time::Duration,
};

use chrono::{DateTime, Utc};
use hubuum_storage_core::{StorageError, StorageRuntimeSettingsSnapshot};

/// Notifications apply changes promptly; this poll recovers missed ones.
pub(crate) const RUNTIME_SETTINGS_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
/// Bound waiting for an in-flight refresh plus the storage read itself.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

/// How current this process's applied settings are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeSettingsApplication {
    /// No refresh has been attempted; deployment configuration applies.
    NotLoaded,
    /// The latest refresh succeeded within the expected reconcile window.
    Current,
    /// The latest refresh failed or reconciliation is overdue.
    Stale,
}

/// The settings this process applies, with refresh diagnostics.
#[derive(Clone, Debug, Default)]
pub(crate) struct AppliedRuntimeSettings {
    snapshot: Option<Arc<StorageRuntimeSettingsSnapshot>>,
    applied_at: Option<DateTime<Utc>>,
    last_attempt_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    refresh_failed: bool,
}

impl AppliedRuntimeSettings {
    /// Loaded security settings must stop authorizing new work when stale.
    /// Production startup loads a snapshot before serving; isolated test
    /// contexts may still use deployment configuration without reconciliation.
    pub(crate) fn ensure_security_current(&self) -> Result<(), StorageError> {
        if self.application(Utc::now()) == RuntimeSettingsApplication::Stale {
            return Err(StorageError::unavailable(
                "Security settings are stale; waiting for successful reconciliation",
            ));
        }
        Ok(())
    }

    /// Destructive maintenance pauses until this replica has reconciled policy.
    pub(crate) fn maintenance_allowed(&self) -> bool {
        self.application(Utc::now()) != RuntimeSettingsApplication::Stale
    }

    /// Last successfully loaded snapshot, retained across failed refreshes.
    pub(crate) fn snapshot(&self) -> Option<&StorageRuntimeSettingsSnapshot> {
        self.snapshot.as_deref()
    }

    pub(crate) const fn applied_at(&self) -> Option<DateTime<Utc>> {
        self.applied_at
    }

    pub(crate) const fn last_attempt_at(&self) -> Option<DateTime<Utc>> {
        self.last_attempt_at
    }

    pub(crate) const fn last_failure_at(&self) -> Option<DateTime<Utc>> {
        self.last_failure_at
    }

    /// Whether the most recent completed refresh failed.
    pub(crate) const fn refresh_failed(&self) -> bool {
        self.refresh_failed
    }

    pub(crate) fn application(&self, now: DateTime<Utc>) -> RuntimeSettingsApplication {
        if self.refresh_failed {
            return RuntimeSettingsApplication::Stale;
        }
        let Some(applied_at) = self.applied_at else {
            return RuntimeSettingsApplication::NotLoaded;
        };
        let window = RUNTIME_SETTINGS_RECONCILE_INTERVAL * 2 + REFRESH_TIMEOUT;
        if now
            .signed_duration_since(applied_at)
            .to_std()
            .unwrap_or_default()
            > window
        {
            RuntimeSettingsApplication::Stale
        } else {
            RuntimeSettingsApplication::Current
        }
    }
}

pub(crate) struct RuntimeSettingsReplica {
    refresh: tokio::sync::Mutex<()>,
    changes: tokio::sync::watch::Sender<()>,
    applied: RwLock<AppliedRuntimeSettings>,
}

impl RuntimeSettingsReplica {
    pub(crate) fn new() -> Self {
        Self {
            refresh: tokio::sync::Mutex::new(()),
            changes: tokio::sync::watch::channel(()).0,
            applied: RwLock::new(AppliedRuntimeSettings::default()),
        }
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes.subscribe()
    }

    pub(crate) fn current(&self) -> AppliedRuntimeSettings {
        self.applied
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Reload the authoritative snapshot.
    ///
    /// Loads are serialized so an older read cannot replace a newer one. Only
    /// the caller performing a load records its outcome; a caller that times
    /// out while waiting for another refresh leaves the state unchanged. A
    /// failed load retains the previous snapshot and marks it stale.
    pub(crate) async fn refresh(
        &self,
        load: impl Future<Output = Result<StorageRuntimeSettingsSnapshot, StorageError>>,
    ) -> Result<AppliedRuntimeSettings, StorageError> {
        self.refresh_within(load, REFRESH_TIMEOUT).await
    }

    async fn refresh_within(
        &self,
        load: impl Future<Output = Result<StorageRuntimeSettingsSnapshot, StorageError>>,
        timeout: Duration,
    ) -> Result<AppliedRuntimeSettings, StorageError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let timed_out = || StorageError::unavailable("Runtime settings refresh timed out");
        let _guard = tokio::time::timeout_at(deadline, self.refresh.lock())
            .await
            .map_err(|_| timed_out())?;
        let attempted_at = Utc::now();
        let result = tokio::time::timeout_at(deadline, load)
            .await
            .unwrap_or_else(|_| Err(timed_out()));
        let mut applied = self
            .applied
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        applied.last_attempt_at = Some(attempted_at);
        match result {
            Ok(snapshot) => {
                let changed = !applied.maintenance_allowed()
                    || applied.snapshot().is_none_or(|previous| {
                        snapshot.records().iter().any(|record| {
                            previous.record(record.group()).revision() != record.revision()
                        })
                    });
                applied.snapshot = Some(Arc::new(snapshot));
                applied.applied_at = Some(Utc::now());
                applied.refresh_failed = false;
                if changed {
                    self.changes.send_replace(());
                }
                Ok(applied.clone())
            }
            Err(error) => {
                tracing::warn!(%error, "Runtime settings refresh failed; retaining the last valid snapshot");
                applied.last_failure_at = Some(Utc::now());
                applied.refresh_failed = true;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_domain::ResourceRevision;
    use hubuum_storage_core::{
        StoragePaginationPolicy, StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup,
        StorageRuntimeSettingsRecord, StorageRuntimeSettingsValue,
    };

    fn pagination(limit: u64) -> StorageRuntimeSettingsSnapshot {
        let now = Utc::now();
        let mut records = StorageRuntimeSettingsGroup::ALL
            .into_iter()
            .map(|group| StorageRuntimeSettingsRecord::initial(group, now))
            .collect::<Vec<_>>();
        records.retain(|record| record.group() != StorageRuntimeSettingsGroup::Pagination);
        records.push(StorageRuntimeSettingsRecord::new(
            StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
                StoragePaginationPolicy::try_new(limit).unwrap(),
            )),
            ResourceRevision::new(2).unwrap(),
            now,
            None,
        ));
        StorageRuntimeSettingsSnapshot::try_new(records).unwrap()
    }

    #[tokio::test]
    async fn failed_refresh_retains_the_last_valid_snapshot_and_reports_staleness() {
        let replica = RuntimeSettingsReplica::new();
        replica.refresh(async { Ok(pagination(7)) }).await.unwrap();
        replica
            .refresh(async { Err(StorageError::unavailable("database unavailable")) })
            .await
            .unwrap_err();
        let applied = replica.current();
        assert_eq!(
            (
                applied
                    .snapshot()
                    .and_then(|snapshot| snapshot.pagination()),
                applied.application(Utc::now()),
            ),
            (
                Some(StoragePaginationPolicy::try_new(7).unwrap()),
                RuntimeSettingsApplication::Stale
            )
        );
    }

    #[tokio::test]
    async fn successful_refresh_after_failure_is_current_again() {
        let replica = RuntimeSettingsReplica::new();
        replica
            .refresh(async { Err(StorageError::unavailable("database unavailable")) })
            .await
            .unwrap_err();
        replica.refresh(async { Ok(pagination(9)) }).await.unwrap();
        assert_eq!(
            replica.current().application(Utc::now()),
            RuntimeSettingsApplication::Current
        );
    }

    #[test]
    fn overdue_reconciliation_is_stale() {
        let applied = AppliedRuntimeSettings {
            snapshot: Some(Arc::new(pagination(5))),
            applied_at: Some(Utc::now() - chrono::Duration::minutes(5)),
            ..AppliedRuntimeSettings::default()
        };
        assert_eq!(
            applied.application(Utc::now()),
            RuntimeSettingsApplication::Stale
        );
    }

    #[rstest::rstest]
    #[case::failed(true, 0)]
    #[case::overdue(false, 120)]
    fn stale_security_settings_reject_new_requests(
        #[case] refresh_failed: bool,
        #[case] age_seconds: i64,
    ) {
        let applied = AppliedRuntimeSettings {
            snapshot: Some(Arc::new(pagination(5))),
            applied_at: Some(Utc::now() - chrono::Duration::seconds(age_seconds)),
            refresh_failed,
            ..AppliedRuntimeSettings::default()
        };
        assert!(applied.ensure_security_current().is_err());
    }

    const SHORT_TIMEOUT: Duration = Duration::from_millis(20);

    #[tokio::test]
    async fn stalled_refresh_times_out_and_marks_the_snapshot_stale() {
        let replica = RuntimeSettingsReplica::new();
        replica.refresh(async { Ok(pagination(5)) }).await.unwrap();
        replica
            .refresh_within(std::future::pending(), SHORT_TIMEOUT)
            .await
            .unwrap_err();
        assert!(replica.current().refresh_failed());
    }

    #[tokio::test]
    async fn a_waiter_that_times_out_does_not_record_an_outcome() {
        let replica = RuntimeSettingsReplica::new();
        replica.refresh(async { Ok(pagination(5)) }).await.unwrap();
        let guard = replica.refresh.lock().await;
        replica
            .refresh_within(
                async { panic!("a waiter must not load while another refresh holds the lock") },
                SHORT_TIMEOUT,
            )
            .await
            .unwrap_err();
        drop(guard);
        assert!(!replica.current().refresh_failed());
    }
}
