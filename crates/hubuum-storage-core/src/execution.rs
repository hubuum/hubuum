use std::fmt;
use std::future::Future;
use std::pin::Pin;

use hubuum_events_core::MutationProvenance;

/// Bounded attribution for storage work initiated by one application
/// subsystem.
///
/// These labels are part of the complete storage contract so every selectable
/// backend receives the same low-cardinality diagnostic context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageCallSite {
    EventDelivery,
    EventFanout,
    EventRetention,
    HttpRequest,
    MetricsRefresh,
    Readiness,
    RequestMaintenance,
    RestoreCoordinator,
    TaskLease,
    TaskWorker,
    TokenRetention,
    #[default]
    Unattributed,
}

impl StorageCallSite {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventDelivery => "event_delivery",
            Self::EventFanout => "event_fanout",
            Self::EventRetention => "event_retention",
            Self::HttpRequest => "http_request",
            Self::MetricsRefresh => "metrics_refresh",
            Self::Readiness => "readiness",
            Self::RequestMaintenance => "request_maintenance",
            Self::RestoreCoordinator => "restore_coordinator",
            Self::TaskLease => "task_lease",
            Self::TaskWorker => "task_worker",
            Self::TokenRetention => "token_retention",
            Self::Unattributed => "unattributed",
        }
    }
}

/// Backend-neutral optimistic-concurrency assertion.
///
/// An empty revision list represents an existence-only wildcard assertion.
/// Non-empty lists contain the positive revisions accepted by the caller.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRevisionPrecondition {
    owner_key: String,
    revisions: Vec<i64>,
}

impl fmt::Debug for StorageRevisionPrecondition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRevisionPrecondition")
            .field("owner_key", &"<redacted>")
            .field("revision_count", &self.revisions.len())
            .finish()
    }
}

impl StorageRevisionPrecondition {
    pub fn new(
        owner_key: impl Into<String>,
        revisions: Vec<i64>,
    ) -> Result<Self, StorageRevisionPreconditionError> {
        let owner_key = owner_key.into();
        if owner_key.is_empty() {
            return Err(StorageRevisionPreconditionError::EmptyOwnerKey);
        }
        if revisions.iter().any(|revision| *revision <= 0) {
            return Err(StorageRevisionPreconditionError::InvalidRevision);
        }
        Ok(Self {
            owner_key,
            revisions,
        })
    }

    #[must_use]
    pub fn owner_key(&self) -> &str {
        &self.owner_key
    }

    #[must_use]
    pub fn revisions(&self) -> &[i64] {
        &self.revisions
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRevisionPreconditionError {
    EmptyOwnerKey,
    InvalidRevision,
}

impl fmt::Display for StorageRevisionPreconditionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyOwnerKey => "revision precondition owner key cannot be empty",
            Self::InvalidRevision => "revision precondition revisions must be positive",
        })
    }
}

impl std::error::Error for StorageRevisionPreconditionError {}

/// Execution context every selectable backend must honor.
///
/// The contract carries diagnostic attribution, durable mutation provenance,
/// and optimistic-concurrency assertions without exposing task locals,
/// transaction settings, connections, or database-specific session state.
pub trait StorageExecution: Send + Sync {
    fn run_with_call_site<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;

    /// Send-capable form used by work that crosses a task or thread boundary.
    fn run_with_call_site_send<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a;

    fn run_with_mutation_provenance<'a, F, R>(
        &'a self,
        provenance: Option<MutationProvenance>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;

    fn run_with_revision_precondition<'a, F, R>(
        &'a self,
        precondition: Option<StorageRevisionPrecondition>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_site_labels_are_stable_and_bounded() {
        assert_eq!(
            [
                StorageCallSite::EventDelivery,
                StorageCallSite::EventFanout,
                StorageCallSite::EventRetention,
                StorageCallSite::HttpRequest,
                StorageCallSite::MetricsRefresh,
                StorageCallSite::Readiness,
                StorageCallSite::RequestMaintenance,
                StorageCallSite::RestoreCoordinator,
                StorageCallSite::TaskLease,
                StorageCallSite::TaskWorker,
                StorageCallSite::TokenRetention,
                StorageCallSite::Unattributed,
            ]
            .map(StorageCallSite::as_str),
            [
                "event_delivery",
                "event_fanout",
                "event_retention",
                "http_request",
                "metrics_refresh",
                "readiness",
                "request_maintenance",
                "restore_coordinator",
                "task_lease",
                "task_worker",
                "token_retention",
                "unattributed",
            ]
        );
    }

    #[test]
    fn wildcard_revision_preconditions_are_valid() {
        let precondition = StorageRevisionPrecondition::new("collection:7", Vec::new()).unwrap();

        assert!(precondition.revisions().is_empty());
    }

    #[test]
    fn revision_preconditions_reject_invalid_parts() {
        assert_eq!(
            StorageRevisionPrecondition::new("", vec![1]).unwrap_err(),
            StorageRevisionPreconditionError::EmptyOwnerKey
        );
        assert_eq!(
            StorageRevisionPrecondition::new("collection:7", vec![0]).unwrap_err(),
            StorageRevisionPreconditionError::InvalidRevision
        );
    }
}
