//! Validated task discovery predicates shared by every storage adapter.
use chrono::{DateTime, Utc};
use hubuum_task_core::TaskStopReason;

use crate::{StorageTask, StorageTaskKind, StorageTaskStatus, StorageValidationError};

/// A half-open UTC interval. Missing task timestamps never match a bound.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskTimeRange {
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
}

impl TaskTimeRange {
    pub fn try_new(
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        if after.zip(before).is_some_and(|(a, b)| a >= b) {
            return Err(StorageValidationError::invalid(
                "Time range requires after < before",
            ));
        }
        Ok(Self { after, before })
    }

    #[must_use]
    pub const fn after(&self) -> Option<DateTime<Utc>> {
        self.after
    }
    #[must_use]
    pub const fn before(&self) -> Option<DateTime<Utc>> {
        self.before
    }
    #[must_use]
    pub fn matches(&self, value: Option<DateTime<Utc>>) -> bool {
        self.after
            .is_none_or(|bound| value.is_some_and(|v| v >= bound))
            && self
                .before
                .is_none_or(|bound| value.is_some_and(|v| v < bound))
    }
}

/// AND-combined predicates; members of either lifecycle set combine with OR.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageTaskSearch {
    discovery: Option<crate::TaskDiscoverySearch>,
    kinds: Option<Vec<StorageTaskKind>>,
    statuses: Option<Vec<StorageTaskStatus>>,
    created: TaskTimeRange,
    started: TaskTimeRange,
    finished: TaskTimeRange,
    cancel_requested: Option<bool>,
    terminal_reason: Option<TaskStopReason>,
    trace_id: Option<String>,
}

impl StorageTaskSearch {
    pub fn discovering(
        mut self,
        discovery: crate::TaskDiscoverySearch,
    ) -> Result<Self, StorageValidationError> {
        let mut applicable = StorageTaskKind::ALL.to_vec();
        for predicate in discovery.predicates() {
            applicable.retain(|kind| predicate.applicable_kinds().contains(kind));
        }
        if applicable.is_empty()
            || self
                .kinds
                .as_ref()
                .is_some_and(|kinds| kinds.iter().any(|kind| !applicable.contains(kind)))
        {
            return Err(StorageValidationError::invalid(
                "Conflicting task kind restrictions",
            ));
        }
        if self.kinds.is_none() && !discovery.predicates().is_empty() {
            self.kinds = Some(applicable);
        }
        self.discovery = Some(discovery);
        Ok(self)
    }
    #[must_use]
    pub const fn discovery(&self) -> Option<&crate::TaskDiscoverySearch> {
        self.discovery.as_ref()
    }

    pub fn lifecycle(
        mut self,
        kinds: Option<Vec<StorageTaskKind>>,
        statuses: Option<Vec<StorageTaskStatus>>,
        terminal: Option<bool>,
    ) -> Result<Self, StorageValidationError> {
        if kinds.as_ref().is_some_and(Vec::is_empty) || statuses.as_ref().is_some_and(Vec::is_empty)
        {
            return Err(StorageValidationError::invalid(
                "Task filter sets must not be empty",
            ));
        }
        if let Some(terminal) = terminal {
            if statuses
                .as_ref()
                .is_some_and(|values| values.iter().any(|s| s.is_terminal() != terminal))
            {
                return Err(StorageValidationError::invalid(
                    "status contradicts terminal",
                ));
            }
            self.statuses = Some(statuses.unwrap_or_else(|| {
                StorageTaskStatus::ALL
                    .into_iter()
                    .filter(|s| s.is_terminal() == terminal)
                    .collect()
            }));
        } else {
            self.statuses = statuses;
        }
        self.kinds = kinds;
        if let Some(discovery) = self.discovery.take() {
            return self.discovering(discovery);
        }
        Ok(self)
    }
    #[must_use]
    pub fn time_ranges(
        mut self,
        created: TaskTimeRange,
        started: TaskTimeRange,
        finished: TaskTimeRange,
    ) -> Self {
        self.created = created;
        self.started = started;
        self.finished = finished;
        self
    }
    #[must_use]
    pub fn operations(
        mut self,
        cancel_requested: Option<bool>,
        terminal_reason: Option<TaskStopReason>,
    ) -> Self {
        self.cancel_requested = cancel_requested;
        self.terminal_reason = terminal_reason;
        self
    }
    pub fn with_trace_id(
        mut self,
        trace_id: Option<String>,
    ) -> Result<Self, StorageValidationError> {
        if trace_id.as_ref().is_some_and(|id| {
            id.len() != 32
                || !id.bytes().all(|b| b.is_ascii_hexdigit())
                || id.bytes().all(|b| b == b'0')
        }) {
            return Err(StorageValidationError::invalid(
                "trace_id must be a nonzero 32-digit hexadecimal trace ID",
            ));
        }
        self.trace_id = trace_id.map(|id| id.to_ascii_lowercase());
        Ok(self)
    }
    #[must_use]
    pub fn kinds(&self) -> Option<&[StorageTaskKind]> {
        self.kinds.as_deref()
    }
    #[must_use]
    pub fn statuses(&self) -> Option<&[StorageTaskStatus]> {
        self.statuses.as_deref()
    }
    #[must_use]
    pub const fn created(&self) -> &TaskTimeRange {
        &self.created
    }
    #[must_use]
    pub const fn started(&self) -> &TaskTimeRange {
        &self.started
    }
    #[must_use]
    pub const fn finished(&self) -> &TaskTimeRange {
        &self.finished
    }
    #[must_use]
    pub const fn cancel_requested(&self) -> Option<bool> {
        self.cancel_requested
    }
    #[must_use]
    pub const fn terminal_reason(&self) -> Option<TaskStopReason> {
        self.terminal_reason
    }
    #[must_use]
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }
    #[must_use]
    pub fn matches(&self, task: &StorageTask) -> bool {
        self.discovery.as_ref().is_none_or(|d| d.matches(task))
            && self.kinds().is_none_or(|v| v.contains(&task.kind()))
            && self.statuses().is_none_or(|v| v.contains(&task.status()))
            && self.created.matches(Some(task.created_at()))
            && self.started.matches(task.started_at())
            && self.finished.matches(task.finished_at())
            && self
                .cancel_requested
                .is_none_or(|v| task.control().cancellation().is_some() == v)
            && self
                .terminal_reason
                .is_none_or(|v| task.control().terminal_reason() == Some(v))
            && self
                .trace_id()
                .is_none_or(|v| task.trace_link().is_some_and(|link| link.trace_id() == v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn instant(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    #[rstest]
    #[case(None, false)]
    #[case(Some(9), false)]
    #[case(Some(10), true)]
    #[case(Some(19), true)]
    #[case(Some(20), false)]
    fn range_is_half_open(#[case] value: Option<i64>, #[case] expected: bool) {
        let range = TaskTimeRange::try_new(Some(instant(10)), Some(instant(20))).unwrap();
        assert_eq!(range.matches(value.map(instant)), expected);
    }

    #[rstest]
    #[case(StorageTaskStatus::Queued, false)]
    #[case(StorageTaskStatus::Validating, false)]
    #[case(StorageTaskStatus::Running, false)]
    #[case(StorageTaskStatus::Succeeded, true)]
    #[case(StorageTaskStatus::Failed, true)]
    #[case(StorageTaskStatus::PartiallySucceeded, true)]
    #[case(StorageTaskStatus::Cancelled, true)]
    fn terminal_set_covers_all_states(#[case] status: StorageTaskStatus, #[case] expected: bool) {
        let search = StorageTaskSearch::default()
            .lifecycle(None, None, Some(true))
            .unwrap();
        assert_eq!(search.statuses().unwrap().contains(&status), expected);
    }
}
