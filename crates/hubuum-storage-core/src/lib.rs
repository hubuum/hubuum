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
mod computed_fields;
mod computed_objects;
mod event_administration;
mod events;
mod execution;
mod export_query;
mod export_template_lifecycle;
mod families;
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
mod validation;
mod worker_notifications;

pub use authorization::{
    AuthorizationDataStorage, StorageAuthorizationClassResource, StorageAuthorizationCollection,
    StorageAuthorizationCollectionAccessQuery, StorageAuthorizationCollectionCandidateQuery,
    StorageAuthorizationCollectionGrantListQuery, StorageAuthorizationCollectionsAccessQuery,
    StorageAuthorizationCollectionsQuery, StorageAuthorizationGrant,
    StorageAuthorizationGrantDelete, StorageAuthorizationGrantKey,
    StorageAuthorizationGrantMutation, StorageAuthorizationGroup,
    StorageAuthorizationGroupCandidateQuery, StorageAuthorizationGroupGrant,
    StorageAuthorizationGroupIdentity, StorageAuthorizationGroupMembershipQuery,
    StorageAuthorizationGroupProfile, StorageAuthorizationGroupSyncState,
    StorageAuthorizationObjectResource, StorageAuthorizationPermission,
    StorageAuthorizationPermissionSet, StorageAuthorizationPermissionSetQuery,
    StorageAuthorizationPolicySnapshotRow, StorageAuthorizationPrincipal,
    StorageAuthorizationResourceIds,
};
pub use backend::StorageBackend;
pub use backup_snapshot::{
    BackupSnapshotStorage, StorageBackupHistorySection, StorageBackupHistorySections,
    StorageBackupRow, StorageBackupSnapshot, StorageBackupStateSection, StorageBackupStateSections,
};
pub use catalog::{CatalogStorage, StorageCatalogListQuery};
pub use collection_authorization::{
    CollectionAuthorizationQueryStorage, StorageAuthorizationCollectionGroupsPageQuery,
    StorageAuthorizationCollectionGroupsQuery, StorageAuthorizationCollectionVisibilityQuery,
    StorageAuthorizationEffectiveGroupGrant, StorageAuthorizationGroupCollectionQuery,
    StorageAuthorizationPrincipalCollectionPageQuery, StorageAuthorizationPrincipalCollectionQuery,
};
pub use computed_fields::{
    ComputedFieldStorage, StorageClassComputationState, StorageClassComputationStateBuilder,
    StorageComputationRebuildStatus, StorageComputationRevision, StorageComputedFieldDefinition,
    StorageComputedFieldDefinitionContent, StorageComputedFieldDefinitionInput,
    StorageComputedFieldDefinitionPatch, StorageComputedFieldMutation,
    StorageComputedFieldProvenance, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate,
};
pub use computed_objects::{
    ComputedObjectStorage, StorageComputedFieldError, StorageComputedObject,
    StorageComputedObjectEnrichmentQuery, StorageComputedObjectListQuery,
    StorageComputedObjectPage, StorageComputedObjectProjection, StorageComputedObjectQueryOptions,
    StorageComputedObjectVisibility, StorageComputedScope, StorageSharedComputedScope,
};
pub use event_administration::{
    AuditEventStorage, EventConfigurationStorage, EventDeliveryAdministrationStorage,
    StorageAuditEvent, StorageAuditEventFilters, StorageAuditEventListQuery, StorageEventDelivery,
    StorageEventDeliveryBuilder, StorageEventDeliveryListQuery, StorageEventSink,
    StorageEventSinkBuilder, StorageEventSinkCreate, StorageEventSinkCreateBuilder,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionBuilder, StorageEventSubscriptionCreate,
    StorageEventSubscriptionCreateBuilder, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate,
};
pub use events::{
    EventArchiveSink, EventDeliveryWorkerStorage, EventFanoutStorage, EventRetentionStorage,
    StorageEventDeliveryBatch, StorageEventDeliveryClaim, StorageEventDeliverySink,
    StorageEventDeliverySubscription, StorageEventDeliveryWorkItem, StorageEventRetentionBatch,
    StorageEventRetentionBatchId, StorageEventRetentionSummary, StorageRecordedEvent,
    StorageRetainedEvent, execute_event_retention_batch,
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
pub use families::{
    EventStorage, IdentityStorage, OperationalStorage, QueryStorage, ResourceStorage,
    WorkflowStorage,
};
pub use history::{
    HistoryStorage, StorageClassHistoryRecord, StorageCollectionHistoryRecord,
    StorageExportTemplateHistoryRecord, StorageHistoryAsOfQuery, StorageHistoryCollectionScope,
    StorageHistoryListQuery, StorageHistoryMetadata, StorageHistoryMetadataParts,
    StorageHistoryOperation, StorageHistoryPrincipalName, StorageObjectHistoryAsOfQuery,
    StorageObjectHistoryListQuery, StorageObjectHistoryRecord, StorageRemoteTargetHistoryRecord,
};
pub use hubuum_domain::PrincipalKind;
pub use identity::{
    AuthenticationStorage, StorageAuthenticatedToken, StorageAuthenticatedTokenBuilder,
    StorageAuthenticationAttempt, StorageAuthenticationCredential, StorageAuthenticationHuman,
    StorageAuthenticationIdentity, StorageAuthenticationPrincipal,
    StorageAuthenticationResourceScope, StorageAuthenticationTokenScope,
    StorageAuthenticationTokenScopeQuery,
};
pub use identity_operations::{
    ExternalIdentityStorage, GroupMembershipStorage, IdentityScopeStorage,
    LocalIdentityCredentialStorage, ServiceAccountStorage, StorageDefaultAdminBootstrap,
    StorageExternalGroup, StorageExternalPrincipalState, StorageExternalUserSync,
    StorageExternalUserSyncBuilder, StorageGroupListQuery, StorageIdentityGroup,
    StorageIdentityGroupBuilder, StorageIdentityScope, StorageIdentityScopeEnsure,
    StorageLocalPasswordReset, StoragePrincipalGroup, StoragePrincipalGroupListQuery,
    StorageServiceAccount, StorageServiceAccountCreate, StorageServiceAccountDetails,
    StorageServiceAccountDisableOutcome, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountUpdate,
    StorageSyncedHuman, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    StorageTokenMetadataBuilder, StorageTokenObservation,
};
pub use identity_resources::{
    GroupStorage, PrincipalStorage, StorageGroupCreate, StorageGroupMember, StorageGroupUpdate,
    StoragePrincipal, StoragePrincipalBuilder, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation,
};
pub use identity_tokens::{
    StoragePrincipalTokensRevoke, StorageTokenCreate, StorageTokenCreateParts,
    StorageTokenHashRevoke, StorageTokenIssuancePolicy, StorageTokenIssuancePolicyError,
    StorageTokenRenew, StorageTokenRevoke, TokenStorage,
};
pub use identity_users::{
    StorageUser, StorageUserAnonymize, StorageUserCreate, StorageUserDelete, StorageUserDetails,
    StorageUserDetailsParts, StorageUserListItem, StorageUserListItemBuilder,
    StorageUserListItemParts, StorageUserListQuery, StorageUserParts, StorageUserPasswordUpdate,
    StorageUserUpdate, UserStorage,
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
pub use inventory::{InventoryStorage, StorageInventoryCounts, StorageObjectCountByClass};
pub use metrics::{
    MetricsStorage, StorageEventMetricsSnapshot, StorageExportTemplateMetricIdentity,
    StorageInventoryGaugeSnapshot, StorageInventoryMetricsSnapshot, StorageTaskGaugeAge,
    StorageTaskGaugeCount, StorageTaskGaugeLastTerminal, StorageTaskGaugeSnapshot,
};
pub use mutation::{StorageAuditReceipt, StorageAuditReceipts, StorageMutationOutcome};
pub use object_aggregate::{
    ObjectAggregateAuthorizer, ObjectAggregateStorage, StorageComputedFieldSelector,
    StorageObjectAggregateAuthorization, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateCursor,
    StorageObjectAggregateDimension, StorageObjectAggregateMeasure,
    StorageObjectAggregateMeasureField, StorageObjectAggregateMeasureOperation,
    StorageObjectAggregateMeasureState, StorageObjectAggregateMeasureValue,
    StorageObjectAggregatePage, StorageObjectAggregateQuery, StorageObjectAggregateQueryBuilder,
    StorageObjectAggregateRow, StorageObjectAggregateScalarField, StorageObjectAggregateSort,
    StorageObjectAggregateSpec, StorageObjectAggregateTarget,
};
pub use operational::{
    EventHealthStorage, OperationalStateStorage, StorageEventDeliveryHealthSnapshot,
    StorageEventDeliveryStatusSnapshot, StorageEventFanoutSnapshot, StorageEventQueueSnapshot,
    StorageEventSinkHealthSnapshot, StorageEventSinkSnapshot,
    StorageEventSubscriptionHealthSnapshot, StorageOperationalExportTemplateAuditEntry,
    StorageOperationalExportTemplateHealth, StorageOperationalTaskActiveCounts,
    StorageOperationalTaskKindCounts, StorageOperationalTaskQueueSnapshot,
    StorageOperationalTaskStatusCounts, StorageOperationalTaskTerminalCounts,
    StorageReadinessSnapshot, TokenRetentionStorage,
};
pub use page::{
    MAX_STORAGE_CANDIDATE_PAGE_SIZE, StorageCandidatePage, StorageCandidatePageLimit, StoragePage,
    StoragePageParts,
};
pub use record::StorageRecordMetadata;
pub use relation_lifecycle::{
    ClassRelationStorage, ObjectRelationStorage, StorageClassRelationCreate,
    StorageClassRelationCreateBuilder, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationEndpoint,
    StorageObjectRelationSelector, StoragePreparedClassRelation, StoragePreparedObjectRelation,
    StorageResolvedClassRelation, StorageResolvedObjectRelation,
};
pub use relation_query::{
    RelationQueryStorage, StorageBidirectionalRelatedObjectsQuery, StorageClassGraphRow,
    StorageClassRelation, StorageGraphClass, StorageGraphObject, StorageGraphResource,
    StorageObjectGraphRow, StorageObjectRelation, StorageObjectRelationsTouchingIdsQuery,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedObjectsForRootsQuery, StorageRelatedSort, StorageRelationGraphQuery,
    StorageRelationIdsQuery, StorageRelationListQuery, StorageRelationTouchingQuery,
};
pub use remote_target::{
    RemoteTargetStorage, StorageRemoteTarget, StorageRemoteTargetCreate,
    StorageRemoteTargetDefinition, StorageRemoteTargetDelete, StorageRemoteTargetHttpMethod,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetPatch,
    StorageRemoteTargetPatchParts, StorageRemoteTargetPolicy, StorageRemoteTargetSubjectType,
    StorageRemoteTargetTransport, StorageRemoteTargetTransportParts, StorageRemoteTargetUpdate,
};
pub use resource_lifecycle::{
    ClassStorage, CollectionStorage, ObjectStorage, StorageClass, StorageClassBuilder,
    StorageClassCreate, StorageClassCreateBuilder, StorageClassSelector, StorageClassUpdate,
    StorageClassUpdateBuilder, StorageCollectionCreate, StorageCollectionUpdate,
    StorageObjectCreate, StorageObjectDataPatch, StorageObjectSelector, StorageObjectUpdate,
    StorageObjectUpdateBuilder, StorageResolvedClass, StorageResolvedObject,
};
pub use restore::{
    RestoreStorage, StorageRestoreApply, StorageRestoreArtifactSummary,
    StorageRestoreArtifactSummaryParts, StorageRestoreCompletion,
    StorageRestoreCoordinatorSnapshot, StorageRestoreDocument, StorageRestoreDocumentMetadata,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreInitiator,
    StorageRestoreInitiatorParts, StorageRestoreInstance, StorageRestoreJob,
    StorageRestoreJobStatus, StorageRestoreJobSummary, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageRestoreTimestamps, StorageRestoreTimestampsParts,
};
pub use task_execution::{
    StorageBackupTaskArtifact, StorageExportTaskArtifact, StorageExportTaskArtifactBuilder,
    StorageExportTaskArtifactContent, StorageExportTaskArtifactIdentity,
    StorageExportTaskArtifactReport, StorageRemoteCallArtifactOutcome,
    StorageRemoteCallArtifactResponse, StorageRemoteCallArtifactTarget,
    StorageRemoteCallArtifactTargetParts, StorageRemoteCallTaskArtifact, StorageTaskActiveUpdate,
    StorageTaskClaim, StorageTaskClaimToken, StorageTaskCompletion, StorageTaskCompletionArtifact,
    StorageTaskEventAppend, StorageTaskEventInput, StorageTaskFailure, StorageTaskLease,
    StorageTaskLeaseDuration, StorageTaskResultCounts, StorageTaskTerminalUpdate,
    TaskExecutionStorage,
};
pub use task_queue::{
    StorageBackupOutput, StorageBackupOutputSummary, StorageExportOutput,
    StorageExportOutputBuilder, StorageExportOutputSummary, StorageImportTaskResult,
    StorageImportTaskResultBuilder, StorageTask, StorageTaskAccess, StorageTaskBuilder,
    StorageTaskChildListQuery, StorageTaskCreateRequest, StorageTaskCreateRequestBuilder,
    StorageTaskDurations, StorageTaskEvent, StorageTaskEventBuilder, StorageTaskKind,
    StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskProgress, StorageTaskScopeSnapshot,
    StorageTaskStatus, TaskQueueStorage,
};
pub use telemetry::{StorageCapability, StorageObservation, StorageObserver};
pub use transaction::{
    StorageTransaction, StorageTransactionFuture, TransactionStorage, TransactionalClassRelations,
    TransactionalClasses, TransactionalCollections, TransactionalObjectRelations,
    TransactionalObjects,
};
pub use unified_search::{
    StorageClassWithCollection, StorageClassWithCollectionBuilder, StorageCollection,
    StorageObject, StorageResourceScope, StorageUnifiedSearchCandidate, StorageUnifiedSearchCursor,
    StorageUnifiedSearchQuery, StorageVisibility, UnifiedSearchStorage,
};
pub use validation::{StorageValidationError, StorageValidationErrorKind};
pub use worker_notifications::{
    StorageNotification, StorageNotificationListener, StorageNotificationShutdown,
    WorkerNotificationProvider,
};

use hubuum_domain::ResourceRevision;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// The configured authorization provider could not answer safely.
    AuthorizationUnavailable,
    /// Caller-supplied storage input is malformed or outside its valid range.
    InvalidInput,
    /// The requested state conflicts with an existing durable state.
    Conflict,
    /// The adapter's persistence service or driver failed.
    Backend,
    /// The authenticated caller lacks the required permission.
    PermissionDenied,
    /// Hubuum or the adapter violated an internal invariant.
    Internal,
    /// The requested durable resource does not exist.
    NotFound,
    /// Caller-supplied content exceeds a documented storage limit.
    InputTooLarge,
    /// An optimistic revision precondition conflicts with the current row.
    RevisionConflict,
    /// A general conditional write precondition failed.
    PreconditionFailed,
    /// The operation is temporarily rejected by a configured rate limit.
    RateLimited,
    /// The storage service is temporarily unavailable.
    Unavailable,
    /// The operation requires an authenticated identity.
    AuthenticationRequired,
    /// Well-formed domain content failed semantic validation.
    ValidationFailed,
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
            Self::InputTooLarge => "input_too_large",
            Self::RevisionConflict => "revision_conflict",
            Self::PreconditionFailed => "precondition_failed",
            Self::RateLimited => "rate_limited",
            Self::Unavailable => "unavailable",
            Self::AuthenticationRequired => "authentication_required",
            Self::ValidationFailed => "validation_failed",
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
/// Messages must be safe to expose, but are diagnostic prose rather than stable
/// identifiers. Callers match [`StorageErrorKind`] and structured fields, never
/// message text.
#[derive(Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
    current_revision: Option<ResourceRevision>,
}

impl StorageError {
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
    pub fn validation_failed(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::ValidationFailed, message, None)
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

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn current_revision(&self) -> Option<ResourceRevision> {
        self.current_revision
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
        assert_eq!(error.message(), "stale resource");
        assert_eq!(error.current_revision(), Some(current_revision));
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
