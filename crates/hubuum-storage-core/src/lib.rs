//! Backend-neutral storage metadata and errors.
//!
//! This crate deliberately has no application, transport, database-driver, or
//! asynchronous-runtime dependencies. Application services and adapters share
//! these values without reversing the dependency from storage into the server.

mod authorization;
mod backend;
mod backup_snapshot;
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
mod object_aggregate;
mod operational;
mod record;
mod relation_lifecycle;
mod relation_query;
mod remote_target;
mod resource_lifecycle;
mod restore;
mod task_execution;
mod task_queue;
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
pub use catalog::{CatalogListQuery, CatalogPage, CatalogStorage};
pub use collection_authorization::{
    AuthorizationCollectionGroupsPageQuery, AuthorizationCollectionGroupsQuery,
    AuthorizationCollectionVisibilityQuery, AuthorizationEffectiveGroupGrant,
    AuthorizationGroupCollectionQuery, AuthorizationGroupPage,
    AuthorizationPrincipalCollectionPageQuery, AuthorizationPrincipalCollectionQuery,
    CollectionAuthorizationStorage,
};
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
pub use event_administration::{
    AuditEventStorage, EventDeliveryAdministrationStorage, EventSubscriptionStorage,
    StorageAuditEvent, StorageAuditEventFilters, StorageAuditEventListQuery, StorageEventDelivery,
    StorageEventDeliveryBuilder, StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink,
    StorageEventSinkBuilder, StorageEventSinkCreate, StorageEventSinkCreateBuilder,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionBuilder, StorageEventSubscriptionCreate,
    StorageEventSubscriptionCreateBuilder, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate,
};
pub use events::{
    EventArchive, EventDeliveryBatch, EventDeliveryClaim, EventDeliverySink, EventDeliveryStorage,
    EventDeliverySubscription, EventDeliveryWorkItem, EventFanoutStorage, EventRetentionStorage,
    EventRetentionSummary, RetainedEvent, StorageRecordedEvent,
};
pub use execution::{
    StorageCallSite, StorageExecution, StorageRevisionPrecondition,
    StorageRevisionPreconditionError,
};
pub use export_query::{ExportQueryStorage, StorageQueryBudget};
pub use export_template_lifecycle::{
    ExportTemplateStorage, StorageExportTemplate, StorageExportTemplateCreate,
    StorageExportTemplateDefinition, StorageExportTemplateDefinitionParts,
    StorageExportTemplateDelete, StorageExportTemplateListQuery, StorageExportTemplatePage,
    StorageExportTemplateReplace,
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
    IdentityStorage, StorageDefaultAdminBootstrap, StorageExternalGroup,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageExternalUserSyncBuilder,
    StorageGroupListQuery, StorageIdentityGroup, StorageIdentityGroupBuilder, StorageIdentityPage,
    StorageIdentityScope, StorageIdentityScopeEnsure, StorageLocalPasswordReset,
    StoragePrincipalGroup, StoragePrincipalGroupListQuery, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountListItem, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountPoint, StorageServiceAccountUpdate,
    StorageSyncedHuman, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    StorageTokenMetadataBuilder, StorageTokenObservation, StorageTokenObservationError,
};
pub use identity_resources::{
    GroupStorage, PrincipalStorage, StorageGroupCreate, StorageGroupUpdate, StoragePrincipal,
    StoragePrincipalBuilder, StoragePrincipalParts, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation,
};
pub use identity_tokens::{
    StorageTokenCreate, StorageTokenHashRevoke, StorageTokenIssuancePolicy, StorageTokenRenew,
    StorageTokenRevoke, TokenStorage,
};
pub use identity_users::{
    StorageUser, StorageUserCreate, StorageUserDelete, StorageUserListItem, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserPoint, StorageUserUpdate, UserStorage,
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
    StorageImportOperation, StorageImportPermissionPolicy, StorageImportPlanItem,
    StorageImportPreflight, StorageImportPreflightItem, StorageImportPrincipal,
    StorageImportPrincipalKey, StorageImportPrincipalKeyParts, StorageImportPrincipalParts,
    StorageImportPrincipalSubtype, StorageImportRemoteTarget, StorageImportRemoteTargetParts,
    StorageImportResult, StorageImportResultBuilder, StorageImportRevision,
    StorageImportTimestamps, StorageImportWriteCondition,
};
pub use inventory::{InventoryStorage, StorageInventoryCounts, StorageObjectsByClassCount};
pub use metrics::{
    EventMetricsSnapshot, ExportTemplateMetricIdentity, InventoryGaugeSnapshot,
    InventoryMetricsSnapshot, MetricsStorage, StoragePoolAcquisitionState, StoragePoolCapacity,
    StoragePoolConnectionState, StoragePoolState, TaskGaugeAge, TaskGaugeCount,
    TaskGaugeLastTerminal, TaskGaugeSnapshot,
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
    EventSubscriptionHealthSnapshot, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalStorageSnapshot,
    OperationalTaskActiveCounts, OperationalTaskKindCounts, OperationalTaskQueueSnapshot,
    OperationalTaskStatusCounts, OperationalTaskTerminalCounts, ReadinessSnapshot,
    TokenRetentionStorage,
};
pub use record::StorageRecordMetadata;
pub use relation_lifecycle::{
    ClassRelationStore, ObjectRelationStore, StorageClassRelationCreate,
    StorageClassRelationCreateBuilder, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationEndpoint,
    StorageObjectRelationSelector, StoragePreparedClassRelation, StoragePreparedObjectRelation,
    StorageResolvedClassRelation, StorageResolvedObjectRelation,
};
pub use relation_query::{
    BidirectionalRelatedObjectsQuery, ObjectRelationsTouchingIdsQuery, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationPage, RelationQueryStorage,
    RelationTouchingQuery, StorageClassGraphRow, StorageClassRelation, StorageGraphClass,
    StorageGraphObject, StorageGraphResource, StorageObjectGraphRow, StorageObjectRelation,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedSort,
};
pub use remote_target::{
    RemoteTargetStorage, StorageRemoteTarget, StorageRemoteTargetCreate,
    StorageRemoteTargetDefinition, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPage, StorageRemoteTargetPatch,
    StorageRemoteTargetPatchParts, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate,
};
pub use resource_lifecycle::{
    ClassStore, CollectionStore, ObjectStore, StorageClassCreate, StorageClassCreateBuilder,
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
pub use unified_search::{
    UnifiedSearchClass, UnifiedSearchClassBuilder, UnifiedSearchCollection, UnifiedSearchCursor,
    UnifiedSearchObject, UnifiedSearchQuery, UnifiedSearchResourceScope, UnifiedSearchStorage,
    UnifiedSearchVisibility,
};
pub use worker_notifications::{StorageNotification, WorkerNotificationStorage};

/// Shared backend-neutral resource projections used by read capabilities.
pub type StorageCollection = UnifiedSearchCollection;
pub type StorageClass = UnifiedSearchClass;
pub type StorageObject = UnifiedSearchObject;
pub type StorageResourceScope = UnifiedSearchResourceScope;
pub type StorageVisibility = UnifiedSearchVisibility;

use std::fmt;

/// Backend identity used for diagnostics and complete-backend composition.
pub trait StorageIdentity: Send + Sync {
    fn storage_name(&self) -> &'static str;
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
