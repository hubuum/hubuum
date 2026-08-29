use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::ExportTemplateId;
use std::collections::{HashMap, HashSet};

use crate::{
    StorageError, StorageEventFanoutSnapshot, StorageEventQueueSnapshot, StorageTaskKind,
    StorageTaskStatus, StorageValidationError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageInventoryMetricsSnapshot {
    collections: i64,
    classes: i64,
    objects: i64,
    users: i64,
    groups: i64,
    service_accounts: i64,
    remote_targets: i64,
}

impl StorageInventoryMetricsSnapshot {
    pub fn try_new(
        collections: i64,
        classes: i64,
        objects: i64,
        users: i64,
        groups: i64,
        service_accounts: i64,
        remote_targets: i64,
    ) -> Result<Self, StorageValidationError> {
        if [
            collections,
            classes,
            objects,
            users,
            groups,
            service_accounts,
            remote_targets,
        ]
        .into_iter()
        .any(|value| value < 0)
        {
            return Err(StorageValidationError::invalid(
                "inventory metric counts must not be negative",
            ));
        }
        Ok(Self {
            collections,
            classes,
            objects,
            users,
            groups,
            service_accounts,
            remote_targets,
        })
    }

    #[must_use]
    pub const fn collections(self) -> i64 {
        self.collections
    }

    #[must_use]
    pub const fn classes(self) -> i64 {
        self.classes
    }

    #[must_use]
    pub const fn objects(self) -> i64 {
        self.objects
    }

    #[must_use]
    pub const fn users(self) -> i64 {
        self.users
    }

    #[must_use]
    pub const fn groups(self) -> i64 {
        self.groups
    }

    #[must_use]
    pub const fn service_accounts(self) -> i64 {
        self.service_accounts
    }

    #[must_use]
    pub const fn remote_targets(self) -> i64 {
        self.remote_targets
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageExportTemplateMetricIdentity {
    id: ExportTemplateId,
    name: String,
}

impl StorageExportTemplateMetricIdentity {
    #[must_use]
    pub fn new(id: ExportTemplateId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> ExportTemplateId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_ages() -> Vec<StorageTaskGaugeAge> {
        StorageTaskKind::ALL
            .into_iter()
            .map(|kind| StorageTaskGaugeAge::new(kind, None, None))
            .collect()
    }

    #[test]
    fn export_template_metric_identity_requires_a_positive_id() {
        let identity = StorageExportTemplateMetricIdentity::new(
            hubuum_domain::ExportTemplateId::new(1).unwrap(),
            "valid",
        );
        assert_eq!(identity.id().id(), 1);
        assert_eq!(identity.name(), "valid");
    }

    #[test]
    fn task_gauge_last_terminal_rejects_a_nonterminal_status() {
        let error = StorageTaskGaugeLastTerminal::try_new(
            StorageTaskKind::Import,
            StorageTaskStatus::Running,
            Utc::now(),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn task_gauge_snapshot_requires_ages_for_every_task_kind() {
        let error =
            StorageTaskGaugeSnapshot::try_new(Vec::new(), Vec::new(), Vec::new()).unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn task_gauge_snapshot_rejects_a_missing_queued_timestamp() {
        let counts = vec![
            StorageTaskGaugeCount::try_new(StorageTaskKind::Import, StorageTaskStatus::Queued, 1)
                .unwrap(),
        ];
        let error =
            StorageTaskGaugeSnapshot::try_new(counts, empty_ages(), Vec::new()).unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn task_gauge_snapshot_requires_a_timestamp_for_positive_terminal_counts() {
        let counts = vec![
            StorageTaskGaugeCount::try_new(
                StorageTaskKind::Import,
                StorageTaskStatus::Succeeded,
                1,
            )
            .unwrap(),
        ];
        let error =
            StorageTaskGaugeSnapshot::try_new(counts, empty_ages(), Vec::new()).unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInventoryGaugeSnapshot {
    counts: StorageInventoryMetricsSnapshot,
    export_templates: Vec<StorageExportTemplateMetricIdentity>,
}

impl StorageInventoryGaugeSnapshot {
    pub fn try_new(
        counts: StorageInventoryMetricsSnapshot,
        export_templates: Vec<StorageExportTemplateMetricIdentity>,
    ) -> Result<Self, StorageValidationError> {
        let mut ids = std::collections::HashSet::with_capacity(export_templates.len());
        if export_templates
            .iter()
            .any(|template| !ids.insert(template.id()))
        {
            return Err(StorageValidationError::invalid(
                "inventory gauge export-template ids must be unique",
            ));
        }
        Ok(Self {
            counts,
            export_templates,
        })
    }

    #[must_use]
    pub const fn counts(&self) -> StorageInventoryMetricsSnapshot {
        self.counts
    }

    #[must_use]
    pub fn export_templates(&self) -> &[StorageExportTemplateMetricIdentity] {
        &self.export_templates
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageTaskGaugeCount {
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    count: i64,
}

impl StorageTaskGaugeCount {
    pub fn try_new(
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        count: i64,
    ) -> Result<Self, StorageValidationError> {
        if count < 0 {
            return Err(StorageValidationError::invalid(
                "task gauge count must not be negative",
            ));
        }
        Ok(Self {
            kind,
            status,
            count,
        })
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn status(self) -> StorageTaskStatus {
        self.status
    }

    #[must_use]
    pub const fn count(self) -> i64 {
        self.count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageTaskGaugeAge {
    kind: StorageTaskKind,
    oldest_queued_at: Option<DateTime<Utc>>,
    oldest_active_at: Option<DateTime<Utc>>,
}

impl StorageTaskGaugeAge {
    #[must_use]
    pub const fn new(
        kind: StorageTaskKind,
        oldest_queued_at: Option<DateTime<Utc>>,
        oldest_active_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            kind,
            oldest_queued_at,
            oldest_active_at,
        }
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn oldest_queued_at(self) -> Option<DateTime<Utc>> {
        self.oldest_queued_at
    }

    #[must_use]
    pub const fn oldest_active_at(self) -> Option<DateTime<Utc>> {
        self.oldest_active_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageTaskGaugeLastTerminal {
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    finished_at: DateTime<Utc>,
}

impl StorageTaskGaugeLastTerminal {
    pub fn try_new(
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        finished_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if !status.is_terminal() {
            return Err(StorageValidationError::invalid(
                "task gauge last-terminal status must be terminal",
            ));
        }
        Ok(Self {
            kind,
            status,
            finished_at,
        })
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn status(self) -> StorageTaskStatus {
        self.status
    }

    #[must_use]
    pub const fn finished_at(self) -> DateTime<Utc> {
        self.finished_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTaskGaugeSnapshot {
    counts: Vec<StorageTaskGaugeCount>,
    ages: Vec<StorageTaskGaugeAge>,
    last_terminal: Vec<StorageTaskGaugeLastTerminal>,
}

impl StorageTaskGaugeSnapshot {
    pub fn try_new(
        counts: Vec<StorageTaskGaugeCount>,
        ages: Vec<StorageTaskGaugeAge>,
        last_terminal: Vec<StorageTaskGaugeLastTerminal>,
    ) -> Result<Self, StorageValidationError> {
        let mut counts_by_key = HashMap::with_capacity(counts.len());
        for count in &counts {
            if counts_by_key
                .insert((count.kind(), count.status()), count.count())
                .is_some()
            {
                return Err(StorageValidationError::invalid(
                    "task gauge count keys must be unique",
                ));
            }
        }
        let mut age_kinds = HashSet::with_capacity(ages.len());
        if ages.iter().any(|age| !age_kinds.insert(age.kind())) {
            return Err(StorageValidationError::invalid(
                "task gauge age kinds must be unique",
            ));
        }
        if age_kinds.len() != StorageTaskKind::ALL.len()
            || StorageTaskKind::ALL
                .iter()
                .any(|kind| !age_kinds.contains(kind))
        {
            return Err(StorageValidationError::invalid(
                "task gauge ages must cover every task kind",
            ));
        }
        for age in &ages {
            let queued = counts_by_key
                .get(&(age.kind(), StorageTaskStatus::Queued))
                .copied()
                .unwrap_or(0);
            let active = counts_by_key
                .get(&(age.kind(), StorageTaskStatus::Validating))
                .copied()
                .unwrap_or(0)
                .checked_add(
                    counts_by_key
                        .get(&(age.kind(), StorageTaskStatus::Running))
                        .copied()
                        .unwrap_or(0),
                )
                .ok_or_else(|| {
                    StorageValidationError::invalid("task gauge active count must not overflow")
                })?;
            if age.oldest_queued_at().is_some() != (queued > 0)
                || age.oldest_active_at().is_some() != (active > 0)
            {
                return Err(StorageValidationError::invalid(
                    "task gauge counts and age timestamps are inconsistent",
                ));
            }
        }
        let mut terminal_keys = HashSet::with_capacity(last_terminal.len());
        if last_terminal
            .iter()
            .any(|terminal| !terminal_keys.insert((terminal.kind(), terminal.status())))
        {
            return Err(StorageValidationError::invalid(
                "task gauge terminal keys must be unique",
            ));
        }
        if terminal_keys
            .iter()
            .any(|key| counts_by_key.get(key).copied().unwrap_or(0) == 0)
            || counts_by_key.iter().any(|(key, count)| {
                key.1.is_terminal() && *count > 0 && !terminal_keys.contains(key)
            })
        {
            return Err(StorageValidationError::invalid(
                "task gauge terminal counts and timestamps are inconsistent",
            ));
        }
        Ok(Self {
            counts,
            ages,
            last_terminal,
        })
    }

    #[must_use]
    pub fn counts(&self) -> &[StorageTaskGaugeCount] {
        &self.counts
    }

    #[must_use]
    pub fn ages(&self) -> &[StorageTaskGaugeAge] {
        &self.ages
    }

    #[must_use]
    pub fn last_terminal(&self) -> &[StorageTaskGaugeLastTerminal] {
        &self.last_terminal
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageEventMetricsSnapshot {
    fanout: StorageEventFanoutSnapshot,
    delivery: StorageEventQueueSnapshot,
}

impl StorageEventMetricsSnapshot {
    #[must_use]
    pub const fn new(
        fanout: StorageEventFanoutSnapshot,
        delivery: StorageEventQueueSnapshot,
    ) -> Self {
        Self { fanout, delivery }
    }

    #[must_use]
    pub const fn fanout(self) -> StorageEventFanoutSnapshot {
        self.fanout
    }

    #[must_use]
    pub const fn delivery(self) -> StorageEventQueueSnapshot {
        self.delivery
    }
}

/// Metrics data every selectable storage backend must provide.
///
/// Implementations translate their native failures into [`StorageError`]
/// before crossing this boundary. Application metrics code therefore neither
/// selects a database nor knows how its queries and pool are implemented.
#[async_trait]
pub trait MetricsStorage: Send + Sync {
    async fn get_inventory_metrics_snapshot(
        &self,
    ) -> Result<StorageInventoryGaugeSnapshot, StorageError>;

    async fn get_task_metrics_snapshot(&self) -> Result<StorageTaskGaugeSnapshot, StorageError>;

    async fn get_event_metrics_snapshot(&self)
    -> Result<StorageEventMetricsSnapshot, StorageError>;
}
