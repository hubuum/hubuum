//! Database-managed runtime settings shared by every process using one store.
//!
//! Each settings group is owned either by every process's deployment
//! configuration or by one persisted, revisioned value that is authoritative for
//! the whole deployment. Values cross this boundary as validated types; adapters
//! may persist them as JSON documents but never expose arbitrary keys.

use std::num::{NonZeroU16, NonZeroU32};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalId, ResourceRevision};
use hubuum_events_core::{ActorKind, CorrelationId, EventContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{StorageError, StorageValidationError};

/// Largest page accepted by [`StorageRuntimeSettingsHistoryQuery`].
pub const MAX_RUNTIME_SETTINGS_HISTORY_PAGE: u16 = 100;

/// Stable identifier for a group of correlated runtime settings.
///
/// Groups are updated atomically and revisioned independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageRuntimeSettingsGroup {
    QueryObservations,
    Pagination,
}

impl StorageRuntimeSettingsGroup {
    /// Every group a complete adapter must persist.
    pub const ALL: [Self; 2] = [Self::QueryObservations, Self::Pagination];

    /// Bounded persistence and diagnostic key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueryObservations => "query_observations",
            Self::Pagination => "pagination",
        }
    }

    /// Parse a persisted group key.
    pub fn parse(value: &str) -> Result<Self, StorageValidationError> {
        Self::ALL
            .into_iter()
            .find(|group| group.as_str() == value)
            .ok_or_else(|| {
                StorageValidationError::invalid(format!("Unknown runtime settings group '{value}'"))
            })
    }
}

/// Shared enablement of process-local query observation collection.
///
/// Sampling and capacity limits remain deployment resource ceilings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageQueryObservationPolicy {
    enabled: bool,
}

impl StorageQueryObservationPolicy {
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    #[must_use]
    pub const fn enabled(self) -> bool {
        self.enabled
    }
}

/// Shared default page size for requests that omit a limit.
///
/// Every process still clamps the value to its own deployment ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoragePaginationPolicy {
    default_page_limit: NonZeroU32,
}

impl StoragePaginationPolicy {
    pub fn try_new(default_page_limit: u64) -> Result<Self, StorageValidationError> {
        u32::try_from(default_page_limit)
            .ok()
            .and_then(NonZeroU32::new)
            .map(|default_page_limit| Self { default_page_limit })
            .ok_or_else(|| {
                StorageValidationError::invalid(format!(
                    "default_page_limit must be between 1 and {}",
                    u32::MAX
                ))
            })
    }

    #[must_use]
    pub const fn default_page_limit(self) -> u32 {
        self.default_page_limit.get()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryObservationDocument {
    enabled: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PaginationDocument {
    default_page_limit: u64,
}

/// A validated value for exactly one group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRuntimeSettingsValue {
    QueryObservations(StorageQueryObservationPolicy),
    Pagination(StoragePaginationPolicy),
}

impl StorageRuntimeSettingsValue {
    #[must_use]
    pub const fn group(self) -> StorageRuntimeSettingsGroup {
        match self {
            Self::QueryObservations(_) => StorageRuntimeSettingsGroup::QueryObservations,
            Self::Pagination(_) => StorageRuntimeSettingsGroup::Pagination,
        }
    }

    /// Stable persistence document. Adapters store it without interpretation.
    #[must_use]
    pub fn to_document(self) -> Value {
        let document = match self {
            Self::QueryObservations(policy) => serde_json::to_value(QueryObservationDocument {
                enabled: policy.enabled(),
            }),
            Self::Pagination(policy) => serde_json::to_value(PaginationDocument {
                default_page_limit: u64::from(policy.default_page_limit()),
            }),
        };
        document.expect("runtime settings documents contain only scalar fields")
    }

    /// Validate a persisted document for `group`.
    pub fn from_document(
        group: StorageRuntimeSettingsGroup,
        document: &Value,
    ) -> Result<Self, StorageValidationError> {
        let invalid = |error: serde_json::Error| {
            StorageValidationError::invalid(format!(
                "Invalid {} runtime settings document: {error}",
                group.as_str()
            ))
        };
        match group {
            StorageRuntimeSettingsGroup::QueryObservations => {
                let document = QueryObservationDocument::deserialize(document).map_err(invalid)?;
                Ok(Self::QueryObservations(StorageQueryObservationPolicy::new(
                    document.enabled,
                )))
            }
            StorageRuntimeSettingsGroup::Pagination => {
                let document = PaginationDocument::deserialize(document).map_err(invalid)?;
                Ok(Self::Pagination(StoragePaginationPolicy::try_new(
                    document.default_page_limit,
                )?))
            }
        }
    }
}

/// Owner of a group's effective value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRuntimeSettingsAssignment {
    /// Each process applies its own deployment configuration.
    Deployment(StorageRuntimeSettingsGroup),
    /// The persisted value is authoritative for every participating process.
    Managed(StorageRuntimeSettingsValue),
}

impl StorageRuntimeSettingsAssignment {
    #[must_use]
    pub const fn group(self) -> StorageRuntimeSettingsGroup {
        match self {
            Self::Deployment(group) => group,
            Self::Managed(value) => value.group(),
        }
    }

    #[must_use]
    pub const fn value(self) -> Option<StorageRuntimeSettingsValue> {
        match self {
            Self::Deployment(_) => None,
            Self::Managed(value) => Some(value),
        }
    }

    /// Persistence document; `None` records deployment ownership.
    #[must_use]
    pub fn to_document(self) -> Option<Value> {
        self.value().map(StorageRuntimeSettingsValue::to_document)
    }

    /// Validate persisted ownership for `group`.
    pub fn from_document(
        group: StorageRuntimeSettingsGroup,
        document: Option<&Value>,
    ) -> Result<Self, StorageValidationError> {
        match document {
            None => Ok(Self::Deployment(group)),
            Some(document) => Ok(Self::Managed(StorageRuntimeSettingsValue::from_document(
                group, document,
            )?)),
        }
    }
}

/// Current persisted state of one group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsRecord {
    assignment: StorageRuntimeSettingsAssignment,
    revision: ResourceRevision,
    changed_at: DateTime<Utc>,
    changed_by: Option<PrincipalId>,
}

impl StorageRuntimeSettingsRecord {
    #[must_use]
    pub const fn new(
        assignment: StorageRuntimeSettingsAssignment,
        revision: ResourceRevision,
        changed_at: DateTime<Utc>,
        changed_by: Option<PrincipalId>,
    ) -> Self {
        Self {
            assignment,
            revision,
            changed_at,
            changed_by,
        }
    }

    /// Initial deployment ownership before any administrator change.
    #[must_use]
    pub const fn initial(group: StorageRuntimeSettingsGroup, changed_at: DateTime<Utc>) -> Self {
        Self::new(
            StorageRuntimeSettingsAssignment::Deployment(group),
            ResourceRevision::INITIAL,
            changed_at,
            None,
        )
    }

    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        self.assignment.group()
    }

    #[must_use]
    pub const fn assignment(&self) -> StorageRuntimeSettingsAssignment {
        self.assignment
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    pub const fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    #[must_use]
    pub const fn changed_by(&self) -> Option<PrincipalId> {
        self.changed_by
    }
}

/// Complete persisted runtime settings: exactly one record for every group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsSnapshot {
    records: Vec<StorageRuntimeSettingsRecord>,
}

impl StorageRuntimeSettingsSnapshot {
    /// Reject partial or duplicated snapshots so callers never guess defaults.
    pub fn try_new(
        mut records: Vec<StorageRuntimeSettingsRecord>,
    ) -> Result<Self, StorageValidationError> {
        records.sort_by_key(StorageRuntimeSettingsRecord::group);
        let groups = records
            .iter()
            .map(StorageRuntimeSettingsRecord::group)
            .collect::<Vec<_>>();
        if groups != StorageRuntimeSettingsGroup::ALL {
            return Err(StorageValidationError::invalid(
                "Runtime settings must contain exactly one record for every group",
            ));
        }
        Ok(Self { records })
    }

    /// Every group owned by deployment configuration at its initial revision.
    #[must_use]
    pub fn initial(changed_at: DateTime<Utc>) -> Self {
        Self {
            records: StorageRuntimeSettingsGroup::ALL
                .into_iter()
                .map(|group| StorageRuntimeSettingsRecord::initial(group, changed_at))
                .collect(),
        }
    }

    #[must_use]
    pub fn records(&self) -> &[StorageRuntimeSettingsRecord] {
        &self.records
    }

    #[must_use]
    pub fn record(&self, group: StorageRuntimeSettingsGroup) -> &StorageRuntimeSettingsRecord {
        self.records
            .iter()
            .find(|record| record.group() == group)
            .expect("snapshots contain every runtime settings group")
    }

    /// Managed query observation policy, if an administrator set one.
    #[must_use]
    pub fn query_observations(&self) -> Option<StorageQueryObservationPolicy> {
        match self
            .record(StorageRuntimeSettingsGroup::QueryObservations)
            .assignment()
            .value()
        {
            Some(StorageRuntimeSettingsValue::QueryObservations(policy)) => Some(policy),
            _ => None,
        }
    }

    /// Managed pagination policy, if an administrator set one.
    #[must_use]
    pub fn pagination(&self) -> Option<StoragePaginationPolicy> {
        match self
            .record(StorageRuntimeSettingsGroup::Pagination)
            .assignment()
            .value()
        {
            Some(StorageRuntimeSettingsValue::Pagination(policy)) => Some(policy),
            _ => None,
        }
    }
}

/// Revision-checked replacement of one group's ownership and value.
#[derive(Clone, Debug)]
pub struct StorageRuntimeSettingsUpdate {
    assignment: StorageRuntimeSettingsAssignment,
    expected_revision: ResourceRevision,
    context: EventContext,
}

impl StorageRuntimeSettingsUpdate {
    #[must_use]
    pub const fn new(
        assignment: StorageRuntimeSettingsAssignment,
        expected_revision: ResourceRevision,
        context: EventContext,
    ) -> Self {
        Self {
            assignment,
            expected_revision,
            context,
        }
    }

    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        self.assignment.group()
    }

    #[must_use]
    pub const fn assignment(&self) -> StorageRuntimeSettingsAssignment {
        self.assignment
    }

    #[must_use]
    pub const fn expected_revision(&self) -> ResourceRevision {
        self.expected_revision
    }

    #[must_use]
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

/// Result of a revision-checked update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageRuntimeSettingsUpdateOutcome {
    /// A new revision and its audit record were committed together.
    Committed(StorageRuntimeSettingsRecord),
    /// The assignment was already current; no revision or audit record was written.
    Unchanged(StorageRuntimeSettingsRecord),
}

impl StorageRuntimeSettingsUpdateOutcome {
    #[must_use]
    pub const fn record(&self) -> &StorageRuntimeSettingsRecord {
        match self {
            Self::Committed(record) | Self::Unchanged(record) => record,
        }
    }

    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

/// Durable audit record for one committed revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsChange {
    revision: ResourceRevision,
    previous: StorageRuntimeSettingsAssignment,
    current: StorageRuntimeSettingsAssignment,
    changed_at: DateTime<Utc>,
    actor_kind: ActorKind,
    actor: Option<PrincipalId>,
    request_id: Option<Uuid>,
    correlation_id: Option<CorrelationId>,
}

/// Provenance captured with one audit record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsActor {
    kind: ActorKind,
    principal: Option<PrincipalId>,
    request_id: Option<Uuid>,
    correlation_id: Option<CorrelationId>,
}

impl StorageRuntimeSettingsActor {
    #[must_use]
    pub const fn new(
        kind: ActorKind,
        principal: Option<PrincipalId>,
        request_id: Option<Uuid>,
        correlation_id: Option<CorrelationId>,
    ) -> Self {
        Self {
            kind,
            principal,
            request_id,
            correlation_id,
        }
    }

    /// Project the provenance an adapter must persist for an update.
    #[must_use]
    pub fn from_context(context: &EventContext) -> Self {
        Self::new(
            context.actor_kind(),
            context.actor_user_id(),
            context.request_id(),
            context.correlation_id().cloned(),
        )
    }
}

impl StorageRuntimeSettingsChange {
    /// Validate one audit record. Revision 1 is the initial state and is never audited.
    pub fn try_new(
        revision: ResourceRevision,
        previous: StorageRuntimeSettingsAssignment,
        current: StorageRuntimeSettingsAssignment,
        changed_at: DateTime<Utc>,
        actor: StorageRuntimeSettingsActor,
    ) -> Result<Self, StorageValidationError> {
        if previous.group() != current.group() {
            return Err(StorageValidationError::invalid(
                "A runtime settings change must stay within one group",
            ));
        }
        if revision <= ResourceRevision::INITIAL {
            return Err(StorageValidationError::invalid(
                "Runtime settings changes start after the initial revision",
            ));
        }
        Ok(Self {
            revision,
            previous,
            current,
            changed_at,
            actor_kind: actor.kind,
            actor: actor.principal,
            request_id: actor.request_id,
            correlation_id: actor.correlation_id,
        })
    }

    #[must_use]
    pub const fn group(&self) -> StorageRuntimeSettingsGroup {
        self.current.group()
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    pub const fn previous(&self) -> StorageRuntimeSettingsAssignment {
        self.previous
    }

    #[must_use]
    pub const fn current(&self) -> StorageRuntimeSettingsAssignment {
        self.current
    }

    #[must_use]
    pub const fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    #[must_use]
    pub const fn actor(&self) -> Option<PrincipalId> {
        self.actor
    }

    #[must_use]
    pub const fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    #[must_use]
    pub fn correlation_id(&self) -> Option<&CorrelationId> {
        self.correlation_id.as_ref()
    }
}

/// Newest-first page of one group's audit records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRuntimeSettingsHistoryQuery {
    group: StorageRuntimeSettingsGroup,
    before: Option<ResourceRevision>,
    limit: NonZeroU16,
}

impl StorageRuntimeSettingsHistoryQuery {
    /// `before` selects records with strictly older revisions.
    pub fn try_new(
        group: StorageRuntimeSettingsGroup,
        before: Option<ResourceRevision>,
        limit: usize,
    ) -> Result<Self, StorageValidationError> {
        u16::try_from(limit)
            .ok()
            .filter(|limit| *limit <= MAX_RUNTIME_SETTINGS_HISTORY_PAGE)
            .and_then(NonZeroU16::new)
            .map(|limit| Self {
                group,
                before,
                limit,
            })
            .ok_or_else(|| {
                StorageValidationError::invalid(format!(
                    "limit must be between 1 and {MAX_RUNTIME_SETTINGS_HISTORY_PAGE}"
                ))
            })
    }

    #[must_use]
    pub const fn group(self) -> StorageRuntimeSettingsGroup {
        self.group
    }

    #[must_use]
    pub const fn before(self) -> Option<ResourceRevision> {
        self.before
    }

    #[must_use]
    pub const fn limit(self) -> usize {
        self.limit.get() as usize
    }
}

/// Shared, revisioned runtime configuration with a durable audit trail.
///
/// Values are deployment configuration rather than domain data: adapters keep
/// them, and their audit records, across logical backup restores and exclude
/// them from portable backups.
#[async_trait]
pub trait RuntimeSettingsStorage: Send + Sync {
    /// Load every group's current record. Never returns a partial snapshot.
    async fn load_runtime_settings(&self) -> Result<StorageRuntimeSettingsSnapshot, StorageError>;

    /// Replace one group's assignment when `expected_revision` is current.
    ///
    /// A committed change advances the group's revision and appends exactly one
    /// audit record in the same atomic operation. A stale revision returns
    /// `RevisionConflict` with the current revision. Requesting the current
    /// assignment returns `Unchanged` without a new revision or audit record.
    async fn update_runtime_settings(
        &self,
        request: StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError>;

    /// Newest-first audit records for one group, older than `before` when set.
    async fn list_runtime_settings_changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Result<Vec<StorageRuntimeSettingsChange>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case::query_observations(StorageRuntimeSettingsValue::QueryObservations(
        StorageQueryObservationPolicy::new(true)
    ))]
    #[case::pagination(StorageRuntimeSettingsValue::Pagination(
        StoragePaginationPolicy::try_new(25).unwrap()
    ))]
    fn documents_round_trip_through_validation(#[case] value: StorageRuntimeSettingsValue) {
        assert_eq!(
            StorageRuntimeSettingsValue::from_document(value.group(), &value.to_document())
                .unwrap(),
            value
        );
    }

    #[rstest]
    #[case::unknown_field(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 5, "max": 9}))]
    #[case::zero_limit(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": 0}))]
    #[case::oversized_limit(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": u64::from(u32::MAX) + 1}))]
    #[case::negative_limit(StorageRuntimeSettingsGroup::Pagination, json!({"default_page_limit": -1}))]
    #[case::wrong_group(StorageRuntimeSettingsGroup::QueryObservations, json!({"default_page_limit": 5}))]
    #[case::wrong_type(StorageRuntimeSettingsGroup::QueryObservations, json!({"enabled": "yes"}))]
    fn invalid_documents_are_rejected(
        #[case] group: StorageRuntimeSettingsGroup,
        #[case] document: Value,
    ) {
        assert!(StorageRuntimeSettingsValue::from_document(group, &document).is_err());
    }

    #[rstest]
    #[case::missing_group(vec![StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::Pagination, Utc::now())])]
    #[case::duplicate_group(vec![
        StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::Pagination, Utc::now()),
        StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::Pagination, Utc::now()),
        StorageRuntimeSettingsRecord::initial(StorageRuntimeSettingsGroup::QueryObservations, Utc::now()),
    ])]
    fn snapshots_require_exactly_one_record_per_group(
        #[case] records: Vec<StorageRuntimeSettingsRecord>,
    ) {
        assert!(StorageRuntimeSettingsSnapshot::try_new(records).is_err());
    }

    #[test]
    fn changes_cannot_cross_groups() {
        let result = StorageRuntimeSettingsChange::try_new(
            ResourceRevision::new(2).unwrap(),
            StorageRuntimeSettingsAssignment::Deployment(StorageRuntimeSettingsGroup::Pagination),
            StorageRuntimeSettingsAssignment::Managed(
                StorageRuntimeSettingsValue::QueryObservations(StorageQueryObservationPolicy::new(
                    true,
                )),
            ),
            Utc::now(),
            StorageRuntimeSettingsActor::from_context(&EventContext::system()),
        );
        assert!(result.is_err());
    }

    #[rstest]
    #[case::zero(0)]
    #[case::too_large(usize::from(MAX_RUNTIME_SETTINGS_HISTORY_PAGE) + 1)]
    fn history_pages_are_bounded(#[case] limit: usize) {
        assert!(
            StorageRuntimeSettingsHistoryQuery::try_new(
                StorageRuntimeSettingsGroup::Pagination,
                None,
                limit
            )
            .is_err()
        );
    }

    #[test]
    fn group_keys_are_stable() {
        for group in StorageRuntimeSettingsGroup::ALL {
            assert_eq!(
                StorageRuntimeSettingsGroup::parse(group.as_str()),
                Ok(group)
            );
        }
        assert_eq!(
            StorageRuntimeSettingsGroup::ALL.map(StorageRuntimeSettingsGroup::as_str),
            ["query_observations", "pagination"]
        );
    }
}
