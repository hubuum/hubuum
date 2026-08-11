//! Backend-neutral storage metadata and errors.
//!
//! This crate deliberately has no application, transport, database-driver, or
//! asynchronous-runtime dependencies. Application services and adapters share
//! these values without reversing the dependency from storage into the server.

mod authorization;
mod backup_snapshot;
mod catalog;
mod computed_field_lifecycle;
mod computed_objects;
mod events;
mod execution;
mod export_query;
mod history;
mod identity;
mod identity_operations;
mod object_aggregate;
mod operational;
mod relation_query;
mod remote_target;
mod restore;
mod task_execution;
mod task_queue;
mod unified_search;

pub use authorization::{
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantKey,
    AuthorizationGrantMutation, AuthorizationGroup, AuthorizationGroupGrant,
    AuthorizationGroupGrantPage, AuthorizationGroupIdentity, AuthorizationGroupMembershipQuery,
    AuthorizationGroupProfile, AuthorizationGroupSyncState, AuthorizationObjectResource,
    AuthorizationPermission, AuthorizationPolicySnapshotRow, AuthorizationPrincipal,
    AuthorizationResourceIds, AuthorizationStorage,
};
pub use backup_snapshot::{BackupSnapshotStorage, StorageBackupSections, StorageBackupSnapshot};
pub use catalog::{CatalogListQuery, CatalogPage, CatalogStorage};
pub use computed_field_lifecycle::{
    ComputedFieldLifecycleStorage, StorageClassComputationState, StorageComputedFieldDefinition,
    StorageComputedFieldDefinitionContent, StorageComputedFieldDefinitionInput,
    StorageComputedFieldDefinitionPatch, StorageComputedFieldMutation, StorageComputedFieldPage,
    StorageComputedFieldProvenance, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate,
};
pub use computed_objects::{
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectProjection, ComputedObjectStorage, ComputedObjectVisibility,
    StorageComputedFieldError, StorageComputedObject, StorageComputedScope,
    StorageSharedComputedScope,
};
pub use events::{
    EventArchive, EventDeliveryBatch, EventDeliveryClaim, EventDeliverySink, EventDeliveryStorage,
    EventDeliverySubscription, EventDeliveryWorkItem, EventFanoutStorage, EventRetentionStorage,
    EventRetentionSummary, RetainedEvent,
};
pub use execution::{
    StorageCallSite, StorageExecution, StorageRevisionPrecondition,
    StorageRevisionPreconditionError,
};
pub use export_query::{ExportQueryStorage, StorageQueryBudget};
pub use history::{
    ClassHistoryRecord, CollectionHistoryRecord, ExportTemplateHistoryRecord, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryPage, HistoryPrincipalName,
    HistoryStorage, ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord,
    RemoteTargetHistoryRecord,
};
pub use identity::{
    AuthenticatedToken, AuthenticatedTokenBuilder, AuthenticationCredential, AuthenticationHuman,
    AuthenticationIdentity, AuthenticationPrincipal, AuthenticationPrincipalKind,
    AuthenticationResourceScope, AuthenticationStorage, AuthenticationTokenScope,
    AuthenticationTokenScopeQuery,
};
pub use identity_operations::{
    IdentityStorage, StorageExternalGroup, StorageExternalPrincipalState, StorageExternalUserSync,
    StorageExternalUserSyncBuilder, StorageIdentityPage, StorageIdentityScope,
    StorageIdentityScopeEnsure, StoragePrincipalGroup, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountListItem, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountPoint, StorageServiceAccountUpdate,
    StorageSyncedHuman, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    StorageTokenMetadataBuilder,
};
pub use object_aggregate::{
    ObjectAggregateAuthorizationMode, ObjectAggregateAuthorizer, ObjectAggregateStorage,
    ObjectAggregateStorageQuery, ObjectAggregateStorageQueryBuilder,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
    StorageObjectAggregateMeasureState, StorageObjectAggregateMeasureValue,
    StorageObjectAggregatePage, StorageObjectAggregateRow, StorageObjectAggregateSort,
    StorageObjectAggregateSpec, StorageObjectAggregateTarget,
};
pub use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
    EventHealthStorage, EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
    EventSubscriptionHealthSnapshot, OperationalStateStorage, ReadinessSnapshot,
    TokenRetentionStorage,
};
pub use relation_query::{
    BidirectionalRelatedObjectsQuery, ObjectRelationsTouchingIdsQuery, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationPage, RelationQueryStorage,
    RelationTouchingQuery, StorageClassGraphRow, StorageClassRelation, StorageGraphClass,
    StorageGraphObject, StorageGraphResource, StorageObjectGraphRow, StorageObjectRelation,
    StorageRecordMetadata, StorageRelatedDirection, StorageRelatedObjectForRootRow,
    StorageRelatedObjectIncludeRow, StorageRelatedSort,
};
pub use remote_target::{
    RemoteTargetStorage, StorageRemoteTarget, StorageRemoteTargetCreate,
    StorageRemoteTargetDefinition, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPage, StorageRemoteTargetPatch,
    StorageRemoteTargetPatchParts, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate,
};
pub use restore::{
    RestoreStorage, StorageRestoreApply, StorageRestoreArtifactSummary, StorageRestoreCompletion,
    StorageRestoreCoordinatorSnapshot, StorageRestoreDocument, StorageRestoreDocumentMetadata,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreInitiator,
    StorageRestoreInstance, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus, StorageRestoreTimestamps,
};
pub use task_execution::{
    StorageBackupTaskArtifact, StorageExportTaskArtifact, StorageExportTaskArtifactBuilder,
    StorageExportTaskArtifactContent, StorageExportTaskArtifactIdentity,
    StorageExportTaskArtifactReport, StorageRemoteCallArtifactOutcome,
    StorageRemoteCallArtifactResponse, StorageRemoteCallArtifactTarget,
    StorageRemoteCallTaskArtifact, StorageTaskClaim, StorageTaskClaimToken, StorageTaskCompletion,
    StorageTaskCompletionArtifact, StorageTaskEventAppend, StorageTaskEventInput,
    StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration, StorageTaskResultCounts,
    StorageTaskStateUpdate, TaskExecutionStorage,
};
pub use task_queue::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageExportOutput,
    StorageExportOutputBuilder, StorageExportOutputSummary, StorageImportTaskResult,
    StorageImportTaskResultBuilder, StorageImportTaskResultPage, StorageTask, StorageTaskAccess,
    StorageTaskBuilder, StorageTaskCreateRequest, StorageTaskCreateRequestBuilder,
    StorageTaskDurations, StorageTaskEvent, StorageTaskEventBuilder, StorageTaskEventPage,
    StorageTaskKind, StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskPage,
    StorageTaskPageQuery, StorageTaskProgress, StorageTaskScopeSnapshot, StorageTaskStatus,
    TaskQueueStorage,
};
pub use unified_search::{
    UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchCursor, UnifiedSearchObject,
    UnifiedSearchQuery, UnifiedSearchResourceScope, UnifiedSearchStorage, UnifiedSearchVisibility,
};

/// Shared backend-neutral resource projections used by read capabilities.
pub type StorageCollection = UnifiedSearchCollection;
pub type StorageClass = UnifiedSearchClass;
pub type StorageObject = UnifiedSearchObject;
pub type StorageResourceScope = UnifiedSearchResourceScope;
pub type StorageVisibility = UnifiedSearchVisibility;

use std::fmt;

/// Version of the complete application storage contract.
///
/// Increment this when a selectable backend must implement a new capability
/// family or when an existing family's externally observable semantics change.
pub const STORAGE_CONTRACT_VERSION: u16 = 17;

/// Stable identity of a selectable storage backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageBackendKind {
    Postgresql,
}

impl StorageBackendKind {
    /// Every backend kind that can be selected by application composition.
    pub const ALL: [Self; 1] = [Self::Postgresql];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgresql => "postgresql",
        }
    }
}

/// Stable, bounded capability families required of every selectable backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageCapability {
    DomainLifecycle,
    CatalogQueries,
    ComputedObjectQueries,
    ComputedFieldLifecycle,
    ObjectAggregates,
    RelationQueries,
    IdentityAndAuthorizationData,
    TemporalHistory,
    UnifiedSearch,
    RemoteTargets,
    TaskQueue,
    TaskExecution,
    BackupSnapshots,
    Restores,
    Imports,
    ExportQueries,
    Operations,
}

impl StorageCapability {
    pub const ALL: [Self; 17] = [
        Self::DomainLifecycle,
        Self::CatalogQueries,
        Self::ComputedObjectQueries,
        Self::ComputedFieldLifecycle,
        Self::ObjectAggregates,
        Self::RelationQueries,
        Self::IdentityAndAuthorizationData,
        Self::TemporalHistory,
        Self::UnifiedSearch,
        Self::RemoteTargets,
        Self::TaskQueue,
        Self::TaskExecution,
        Self::BackupSnapshots,
        Self::Restores,
        Self::Imports,
        Self::ExportQueries,
        Self::Operations,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DomainLifecycle => "domain_lifecycle",
            Self::CatalogQueries => "catalog_queries",
            Self::ComputedObjectQueries => "computed_object_queries",
            Self::ComputedFieldLifecycle => "computed_field_lifecycle",
            Self::ObjectAggregates => "object_aggregates",
            Self::RelationQueries => "relation_queries",
            Self::IdentityAndAuthorizationData => "identity_and_authorization_data",
            Self::TemporalHistory => "temporal_history",
            Self::UnifiedSearch => "unified_search",
            Self::RemoteTargets => "remote_targets",
            Self::TaskQueue => "task_queue",
            Self::TaskExecution => "task_execution",
            Self::BackupSnapshots => "backup_snapshots",
            Self::Restores => "restores",
            Self::Imports => "imports",
            Self::ExportQueries => "export_queries",
            Self::Operations => "operations",
        }
    }
}

/// Non-secret metadata for the backend selected at application composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageBackendDescriptor {
    kind: StorageBackendKind,
}

impl StorageBackendDescriptor {
    #[must_use]
    pub const fn new(kind: StorageBackendKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub const fn kind(self) -> StorageBackendKind {
        self.kind
    }

    #[must_use]
    pub const fn contract_version(self) -> u16 {
        STORAGE_CONTRACT_VERSION
    }

    pub fn capabilities(self) -> impl Iterator<Item = StorageCapability> {
        StorageCapability::ALL.into_iter()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    AuthorizationUnavailable,
    BadRequest,
    Conflict,
    Database,
    Forbidden,
    Internal,
    NotFound,
    NotAcceptable,
    PayloadTooLarge,
    PreconditionFailed,
    TooManyRequests,
    Unavailable,
    Unauthorized,
    Validation,
}

impl StorageErrorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorizationUnavailable => "authorization_unavailable",
            Self::BadRequest => "bad_request",
            Self::Conflict => "conflict",
            Self::Database => "database",
            Self::Forbidden => "forbidden",
            Self::Internal => "internal",
            Self::NotFound => "not_found",
            Self::NotAcceptable => "not_acceptable",
            Self::PayloadTooLarge => "payload_too_large",
            Self::PreconditionFailed => "precondition_failed",
            Self::TooManyRequests => "too_many_requests",
            Self::Unavailable => "unavailable",
            Self::Unauthorized => "unauthorized",
            Self::Validation => "validation",
        }
    }

    #[must_use]
    pub const fn is_backend_failure(self) -> bool {
        matches!(
            self,
            Self::AuthorizationUnavailable | Self::Database | Self::Internal | Self::Unavailable
        )
    }
}

/// Backend-neutral failure returned by storage capabilities.
///
/// The representation deliberately carries no Diesel, Actix, or application
/// error types. The application error layer owns transport-facing translation.
#[derive(Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
    current_etag: Option<String>,
}

impl StorageError {
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::BadRequest, message, None)
    }

    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Internal, message, None)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::NotFound, message, None)
    }

    #[must_use]
    pub fn new(
        kind: StorageErrorKind,
        message: impl Into<String>,
        current_etag: Option<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            current_etag,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageErrorKind, String, Option<String>) {
        (self.kind, self.message, self.current_etag)
    }

    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StorageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_reports_the_complete_contract() {
        let descriptor = StorageBackendDescriptor::new(StorageBackendKind::Postgresql);

        assert_eq!(descriptor.contract_version(), STORAGE_CONTRACT_VERSION);
        assert_eq!(
            descriptor
                .capabilities()
                .map(StorageCapability::as_str)
                .collect::<Vec<_>>(),
            [
                "domain_lifecycle",
                "catalog_queries",
                "computed_object_queries",
                "computed_field_lifecycle",
                "object_aggregates",
                "relation_queries",
                "identity_and_authorization_data",
                "temporal_history",
                "unified_search",
                "remote_targets",
                "task_queue",
                "task_execution",
                "backup_snapshots",
                "restores",
                "imports",
                "export_queries",
                "operations",
            ]
        );
    }

    #[test]
    fn storage_errors_keep_classification_and_precondition_metadata() {
        let error = StorageError::new(
            StorageErrorKind::PreconditionFailed,
            "stale resource",
            Some("\"revision-2\"".to_string()),
        );

        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        assert_eq!(
            error.into_parts(),
            (
                StorageErrorKind::PreconditionFailed,
                "stale resource".to_string(),
                Some("\"revision-2\"".to_string()),
            )
        );
    }
}
