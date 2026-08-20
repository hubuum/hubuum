//! Backend-neutral storage metadata and errors.
//!
//! This crate deliberately has no application, transport, database-driver, or
//! asynchronous-runtime dependencies. Application services and adapters share
//! these values without reversing the dependency from storage into the server.

mod authorization;
mod backend;
mod backup_snapshot;
pub mod capabilities;
mod catalog;
mod collection_authorization;
mod computed_field_lifecycle;
mod computed_objects;
mod event_administration;
mod events;
mod execution;
mod export_query;
mod export_template_lifecycle;
mod history;
mod identity;
mod identity_operations;
mod identity_resources;
mod identity_tokens;
mod identity_users;
mod import_workflow;
mod inventory;
mod metrics;
mod mutation;
mod object_aggregate;
mod operational;
mod page;
mod record;
mod relation_lifecycle;
mod relation_query;
mod remote_target;
mod resource_lifecycle;
mod restore;
mod task_execution;
mod task_queue;
mod telemetry;
mod transaction;
mod unified_search;
mod worker_notifications;

pub use authorization::{
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup, AuthorizationGroupGrant,
    AuthorizationGroupGrantPage, AuthorizationGroupIdentity, AuthorizationGroupMembershipQuery,
    AuthorizationGroupProfile, AuthorizationGroupSyncState, AuthorizationObjectResource,
    AuthorizationPermission, AuthorizationPermissionSet, AuthorizationPermissionSetQuery,
    AuthorizationPolicySnapshotRow, AuthorizationPrincipal, AuthorizationResourceIds,
    AuthorizationStorage,
};
pub use backend::StorageBackend;
pub use backup_snapshot::{
    BACKUP_AUXILIARY_HISTORY_SECTIONS, BACKUP_STATE_SECTIONS, BACKUP_TEMPORAL_HISTORY_SECTIONS,
    BackupSnapshotStorage, StorageBackupSections, StorageBackupSnapshot,
};
pub use catalog::{CatalogListQuery, CatalogStorage};
pub use collection_authorization::{
    AuthorizationCollectionGroupsPageQuery, AuthorizationCollectionGroupsQuery,
    AuthorizationCollectionVisibilityQuery, AuthorizationEffectiveGroupGrant,
    AuthorizationGroupCollectionQuery, AuthorizationGroupPage,
    AuthorizationPrincipalCollectionPageQuery, AuthorizationPrincipalCollectionQuery,
    CollectionAuthorizationStorage,
};
pub use computed_field_lifecycle::{
    ComputedFieldLifecycleStorage, StorageClassComputationState, StorageComputationRevision,
    StorageComputedFieldDefinition, StorageComputedFieldDefinitionContent,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldMutation, StorageComputedFieldProvenance,
    StorageComputedFieldRebuildRequest, StorageComputedFieldVisibility,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate,
    StorageSharedComputedFieldCreate, StorageSharedComputedFieldDelete,
    StorageSharedComputedFieldUpdate,
};
pub use computed_objects::{
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectProjection, ComputedObjectQueryOptions, ComputedObjectStorage,
    ComputedObjectVisibility, StorageComputedFieldError, StorageComputedObject,
    StorageComputedScope, StorageSharedComputedScope,
};
pub use event_administration::{
    AuditEventStorage, EventDeliveryAdministrationStorage, EventSubscriptionStorage,
    StorageAuditEvent, StorageAuditEventFilters, StorageAuditEventListQuery, StorageEventDelivery,
    StorageEventDeliveryBuilder, StorageEventDeliveryListQuery, StorageEventSink,
    StorageEventSinkBuilder, StorageEventSinkCreate, StorageEventSinkCreateBuilder,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionBuilder, StorageEventSubscriptionCreate,
    StorageEventSubscriptionCreateBuilder, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate,
};
pub use events::{
    EventArchiveSink, EventDeliveryBatch, EventDeliveryClaim, EventDeliverySink,
    EventDeliveryStorage, EventDeliverySubscription, EventDeliveryWorkItem, EventFanoutStorage,
    EventRetentionBatch, EventRetentionBatchId, EventRetentionStorage, EventRetentionSummary,
    RetainedEvent, StorageRecordedEvent, execute_event_retention_batch,
};
pub use execution::{
    ExecutionStorage, StorageCallSite, StorageExecutionScope, StorageRevisionPrecondition,
    StorageRevisionTarget,
};
pub use export_query::StorageQueryBudget;
pub use export_template_lifecycle::{
    ExportTemplateStorage, StorageExportTemplate, StorageExportTemplateCreate,
    StorageExportTemplateDefinition, StorageExportTemplateDefinitionParts,
    StorageExportTemplateDelete, StorageExportTemplateListQuery, StorageExportTemplateReplace,
};
pub use history::{
    ClassHistoryRecord, CollectionHistoryRecord, ExportTemplateHistoryRecord, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryPage, HistoryPrincipalName,
    HistoryStorage, ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord,
    RemoteTargetHistoryRecord,
};
pub use identity::{
    AuthenticatedToken, AuthenticatedTokenBuilder, AuthenticationAttempt,
    AuthenticationAttemptError, AuthenticationCredential, AuthenticationHuman,
    AuthenticationIdentity, AuthenticationPrincipal, AuthenticationPrincipalKind,
    AuthenticationResourceScope, AuthenticationStorage, AuthenticationTokenScope,
    AuthenticationTokenScopeQuery,
};
pub use identity_operations::{
    BootstrapStorage, ExternalIdentityStorage, IdentityMembershipStorage, IdentityScopeStorage,
    ServiceAccountStorage, StorageDefaultAdminBootstrap, StorageExternalGroup,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageExternalUserSyncBuilder,
    StorageGroupListQuery, StorageIdentityGroup, StorageIdentityGroupBuilder, StorageIdentityScope,
    StorageIdentityScopeEnsure, StorageLocalPasswordReset, StoragePrincipalGroup,
    StoragePrincipalGroupListQuery, StorageServiceAccount, StorageServiceAccountCreate,
    StorageServiceAccountDisableOutcome, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate, StorageSyncedHuman, StorageTokenListQuery, StorageTokenListState,
    StorageTokenMetadata, StorageTokenMetadataBuilder, StorageTokenObservation,
    StorageTokenObservationError,
};
pub use identity_resources::{
    GroupStorage, PrincipalStorage, StorageGroupCreate, StorageGroupUpdate, StoragePrincipal,
    StoragePrincipalBuilder, StoragePrincipalSettings, StoragePrincipalSettingsMutation,
};
pub use identity_tokens::{
    StoragePrincipalTokensRevoke, StorageTokenCreate, StorageTokenCreateParts,
    StorageTokenHashRevoke, StorageTokenIssuancePolicy, StorageTokenIssuancePolicyError,
    StorageTokenRenew, StorageTokenRevoke, TokenStorage,
};
pub use identity_users::{
    StorageUser, StorageUserAnonymize, StorageUserCreate, StorageUserDelete, StorageUserListItem,
    StorageUserListItemParts, StorageUserListQuery, StorageUserParts, StorageUserPasswordUpdate,
    StorageUserPoint, StorageUserPointParts, StorageUserUpdate, UserStorage,
};
pub use import_workflow::{
    ImportStorage, StorageImportApply, StorageImportApplyItem, StorageImportAtomicity,
    StorageImportClass, StorageImportClassKey, StorageImportClassKeyParts, StorageImportClassParts,
    StorageImportClassRelation, StorageImportClassRelationParts, StorageImportCollection,
    StorageImportCollectionKey, StorageImportCollectionKeyParts, StorageImportCollectionParts,
    StorageImportCollectionPermission, StorageImportCollectionPermissionParts,
    StorageImportCollisionPolicy, StorageImportComputedField, StorageImportComputedFieldParts,
    StorageImportComputedFieldVisibility, StorageImportEventSink, StorageImportEventSinkKey,
    StorageImportEventSinkKeyParts, StorageImportEventSinkParts, StorageImportEventSubscription,
    StorageImportEventSubscriptionParts, StorageImportExportTemplate,
    StorageImportExportTemplateParts, StorageImportGroup, StorageImportGroupKey,
    StorageImportGroupKeyParts, StorageImportGroupMembership, StorageImportGroupMembershipParts,
    StorageImportGroupParts, StorageImportIdentityScope, StorageImportIdentityScopeKey,
    StorageImportIdentityScopeKeyParts, StorageImportIdentityScopeParts,
    StorageImportMembershipSource, StorageImportMembershipSourceParts, StorageImportMode,
    StorageImportObject, StorageImportObjectKey, StorageImportObjectKeyParts,
    StorageImportObjectParts, StorageImportObjectRelation, StorageImportObjectRelationParts,
    StorageImportOperation, StorageImportPermissionPolicy, StorageImportPlan,
    StorageImportPlanItem, StorageImportPreflight, StorageImportPreflightItem,
    StorageImportPrincipal, StorageImportPrincipalKey, StorageImportPrincipalKeyParts,
    StorageImportPrincipalParts, StorageImportPrincipalSubtype, StorageImportRemoteTarget,
    StorageImportRemoteTargetParts, StorageImportResult, StorageImportResultBuilder,
    StorageImportRevision, StorageImportTimestamps, StorageImportWriteCondition,
};
pub use inventory::{InventoryStorage, StorageInventoryCounts, StorageObjectsByClassCount};
pub use metrics::{
    EventMetricsSnapshot, ExportTemplateMetricIdentity, InventoryGaugeSnapshot,
    InventoryMetricsSnapshot, MetricsStorage, TaskGaugeAge, TaskGaugeCount, TaskGaugeLastTerminal,
    TaskGaugeSnapshot,
};
pub use mutation::{AuditReceipt, AuditReceipts, MutationOutcome};
pub use object_aggregate::{
    ObjectAggregateAuthorizationMode, ObjectAggregateAuthorizer, ObjectAggregateStorage,
    ObjectAggregateStorageQuery, ObjectAggregateStorageQueryBuilder, StorageComputedFieldSelector,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
    StorageObjectAggregateCursor, StorageObjectAggregateDimension, StorageObjectAggregateMeasure,
    StorageObjectAggregateMeasureField, StorageObjectAggregateMeasureOperation,
    StorageObjectAggregateMeasureState, StorageObjectAggregateMeasureValue,
    StorageObjectAggregatePage, StorageObjectAggregateRow, StorageObjectAggregateScalarField,
    StorageObjectAggregateSort, StorageObjectAggregateSpec, StorageObjectAggregateTarget,
};
pub use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
    EventHealthStorage, EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
    EventSubscriptionHealthSnapshot, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalTaskActiveCounts,
    OperationalTaskKindCounts, OperationalTaskQueueSnapshot, OperationalTaskStatusCounts,
    OperationalTaskTerminalCounts, ReadinessSnapshot, TokenRetentionStorage,
};
pub use page::StoragePage;
pub use record::StorageRecordMetadata;
pub use relation_lifecycle::{
    ClassRelationStorage, ObjectRelationStorage, StorageClassRelationCreate,
    StorageClassRelationCreateBuilder, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationEndpoint,
    StorageObjectRelationSelector, StoragePreparedClassRelation, StoragePreparedObjectRelation,
    StorageResolvedClassRelation, StorageResolvedObjectRelation,
};
pub use relation_query::{
    BidirectionalRelatedObjectsQuery, ObjectRelationsTouchingIdsQuery, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationQueryStorage,
    RelationTouchingQuery, StorageClassGraphRow, StorageClassRelation, StorageGraphClass,
    StorageGraphObject, StorageGraphResource, StorageObjectGraphRow, StorageObjectRelation,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedSort,
};
pub use remote_target::{
    RemoteTargetStorage, StorageRemoteTarget, StorageRemoteTargetCreate,
    StorageRemoteTargetDefinition, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPatch, StorageRemoteTargetPatchParts,
    StorageRemoteTargetPolicy, StorageRemoteTargetTransport, StorageRemoteTargetUpdate,
};
pub use resource_lifecycle::{
    ClassStorage, CollectionStorage, ObjectStorage, StorageClassCreate, StorageClassCreateBuilder,
    StorageClassRecord, StorageClassRecordBuilder, StorageClassSelector, StorageClassUpdate,
    StorageClassUpdateBuilder, StorageCollectionCreate, StorageCollectionUpdate,
    StorageObjectCreate, StorageObjectDataPatch, StorageObjectSelector, StorageObjectUpdate,
    StorageObjectUpdateBuilder, StorageResolvedClass, StorageResolvedObject,
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
pub use telemetry::{StorageObservation, StorageObserver};
pub use transaction::{
    StorageTransaction, StorageTransactionFuture, TransactionStorage, TransactionalClassRelations,
    TransactionalClasses, TransactionalCollections, TransactionalObjectRelations,
    TransactionalObjects,
};
pub use unified_search::{
    StorageClass, StorageClassBuilder, StorageCollection, StorageObject, StorageResourceScope,
    StorageVisibility, UnifiedSearchCursor, UnifiedSearchQuery, UnifiedSearchStorage,
};
pub use worker_notifications::{
    StorageNotification, StorageNotificationListener, StorageNotificationShutdown,
    WorkerNotificationStorage,
};

use hubuum_domain::ResourceRevision;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    AuthorizationUnavailable,
    InvalidInput,
    Conflict,
    Backend,
    PermissionDenied,
    Internal,
    NotFound,
    Unsupported,
    InputTooLarge,
    RevisionConflict,
    PreconditionFailed,
    RateLimited,
    Unavailable,
    AuthenticationRequired,
    Validation,
}

impl StorageErrorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorizationUnavailable => "authorization_unavailable",
            Self::InvalidInput => "invalid_input",
            Self::Conflict => "conflict",
            Self::Backend => "backend",
            Self::PermissionDenied => "permission_denied",
            Self::Internal => "internal",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::InputTooLarge => "input_too_large",
            Self::RevisionConflict => "revision_conflict",
            Self::PreconditionFailed => "precondition_failed",
            Self::RateLimited => "rate_limited",
            Self::Unavailable => "unavailable",
            Self::AuthenticationRequired => "authentication_required",
            Self::Validation => "validation",
        }
    }

    #[must_use]
    pub const fn is_backend_failure(self) -> bool {
        matches!(
            self,
            Self::AuthorizationUnavailable | Self::Backend | Self::Internal | Self::Unavailable
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
    current_revision: Option<ResourceRevision>,
}

impl StorageError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::invalid_input(message)
    }

    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::InvalidInput, message, None)
    }

    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    #[must_use]
    pub fn authorization_unavailable(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::AuthorizationUnavailable, message, None)
    }

    #[must_use]
    pub fn backend_failure(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Backend, message, None)
    }

    #[must_use]
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::PermissionDenied, message, None)
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
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unsupported, message, None)
    }

    #[must_use]
    pub fn input_too_large(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::InputTooLarge, message, None)
    }

    #[must_use]
    pub fn revision_conflict(
        message: impl Into<String>,
        current_revision: ResourceRevision,
    ) -> Self {
        Self::new(
            StorageErrorKind::RevisionConflict,
            message,
            Some(current_revision),
        )
    }

    #[must_use]
    pub fn precondition_failed(
        message: impl Into<String>,
        current_revision: Option<ResourceRevision>,
    ) -> Self {
        Self::new(
            StorageErrorKind::PreconditionFailed,
            message,
            current_revision,
        )
    }

    #[must_use]
    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::RateLimited, message, None)
    }

    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unavailable, message, None)
    }

    #[must_use]
    pub fn authentication_required(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::AuthenticationRequired, message, None)
    }

    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Validation, message, None)
    }

    fn new(
        kind: StorageErrorKind,
        message: impl Into<String>,
        current_revision: Option<ResourceRevision>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            current_revision,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageErrorKind, String, Option<ResourceRevision>) {
        (self.kind, self.message, self.current_revision)
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
    fn storage_errors_keep_classification_and_revision_metadata() {
        let current_revision = ResourceRevision::new(2).unwrap();
        let error = StorageError::revision_conflict("stale resource", current_revision);

        assert_eq!(error.kind(), StorageErrorKind::RevisionConflict);
        assert_eq!(
            error.into_parts(),
            (
                StorageErrorKind::RevisionConflict,
                "stale resource".to_string(),
                Some(current_revision),
            )
        );
    }
}
