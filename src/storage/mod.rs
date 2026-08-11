#[cfg(test)]
pub(crate) mod capabilities;
mod class_relations;
mod classes;
mod collections;
mod context;
mod contract;
mod execution;
mod imports;
#[cfg(test)]
mod memory;
mod metrics;
mod object_relations;
mod objects;
mod observed;
mod operational;
#[doc(hidden)]
pub mod postgres;

pub(crate) use class_relations::ClassRelationStore;
pub(crate) use classes::ClassStore;
pub(crate) use collections::CollectionStore;
pub use context::StorageContext;
pub(crate) use context::{StorageHandle, storage_handle};
#[cfg(test)]
pub(crate) use contract::STORAGE_CONTRACT_VERSION;
pub(crate) use contract::{
    DynLifecycleStorage, LifecycleStorage, StorageBackend, StorageBackendDescriptor,
    StorageBackendKind, StorageIdentity,
};
pub use execution::{
    with_mutation_provenance, with_revision_precondition, with_storage_call_site,
    with_storage_call_site_send,
};
pub(crate) use hubuum_storage_core::{
    AuditEventStorage, AuthenticationCredential, AuthenticationHuman, AuthenticationIdentity,
    AuthenticationPrincipal, AuthenticationResourceScope, AuthenticationStorage,
    AuthenticationTokenScope, AuthenticationTokenScopeQuery, AuthorizationClassResource,
    AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup, AuthorizationGroupGrant,
    AuthorizationGroupGrantPage, AuthorizationGroupIdentity, AuthorizationGroupMembershipQuery,
    AuthorizationGroupProfile, AuthorizationGroupSyncState, AuthorizationObjectResource,
    AuthorizationPermission, AuthorizationPermissionSet, AuthorizationPermissionSetQuery,
    AuthorizationPolicySnapshotRow, AuthorizationPrincipal, AuthorizationResourceIds,
    AuthorizationStorage, BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage,
    CatalogStorage, ComputedFieldLifecycleStorage, ComputedObjectEnrichmentQuery,
    ComputedObjectListQuery, ComputedObjectPage, ComputedObjectProjection, ComputedObjectStorage,
    ComputedObjectVisibility, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryBatch,
    EventDeliveryClaim, EventDeliverySink, EventDeliveryStorage, EventDeliverySubscription,
    EventDeliveryWorkItem, EventFanoutStorage, EventRetentionStorage, EventRetentionSummary,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateHistoryRecord, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryPage, HistoryPrincipalName,
    HistoryStorage, IdentityStorage, ObjectAggregateAuthorizationMode, ObjectAggregateAuthorizer,
    ObjectAggregateStorage, ObjectAggregateStorageQuery, ObjectHistoryAsOfQuery,
    ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationsTouchingIdsQuery,
    RelatedObjectsForRootsQuery, RelationGraphQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationQueryStorage, RelationTouchingQuery, RemoteTargetHistoryRecord,
    RemoteTargetStorage, RestoreStorage, RetainedEvent, StorageAuditEvent,
    StorageAuditEventFilters, StorageAuditEventListQuery, StorageClass,
    StorageClassComputationState, StorageClassGraphRow, StorageClassRelation, StorageCollection,
    StorageComputedFieldDefinition, StorageComputedFieldDefinitionContent,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldError, StorageComputedFieldMutation, StorageComputedFieldPage,
    StorageComputedFieldProvenance, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StorageComputedObject, StorageComputedScope,
    StorageEventDelivery, StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink,
    StorageEventSinkCreate, StorageEventSinkDelete, StorageEventSinkListQuery,
    StorageEventSinkUpdate, StorageEventSubscription, StorageEventSubscriptionCreate,
    StorageEventSubscriptionDelete, StorageEventSubscriptionListQuery,
    StorageEventSubscriptionUpdate, StorageExternalGroup, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageGraphClass, StorageGraphObject, StorageGraphResource,
    StorageIdentityPage, StorageIdentityScope, StorageIdentityScopeEnsure, StorageObject,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
    StorageObjectAggregateMeasureState, StorageObjectAggregateMeasureValue,
    StorageObjectAggregatePage, StorageObjectAggregateRow, StorageObjectAggregateSort,
    StorageObjectAggregateSpec, StorageObjectAggregateTarget, StorageObjectGraphRow,
    StorageObjectRelation, StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate,
    StoragePrincipalGroup, StorageQueryBudget, StorageRecordMetadata, StorageRelatedDirection,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRelatedSort,
    StorageRemoteCallArtifactOutcome, StorageRemoteCallArtifactResponse,
    StorageRemoteCallArtifactTarget, StorageRemoteCallTaskArtifact, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDefinition, StorageRemoteTargetDelete,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetPage,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate, StorageResourceScope, StorageRestoreApply,
    StorageRestoreArtifactSummary, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDocument, StorageRestoreDocumentMetadata, StorageRestoreDrainState,
    StorageRestoreFailure, StorageRestoreInitiator, StorageRestoreInstance, StorageRestoreJob,
    StorageRestoreJobStatus, StorageRestoreJobSummary, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageRestoreTimestamps, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountListItem, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountPoint, StorageServiceAccountUpdate,
    StorageSharedComputedFieldCreate, StorageSharedComputedFieldDelete,
    StorageSharedComputedFieldUpdate, StorageSharedComputedScope, StorageSyncedHuman, StorageTask,
    StorageTaskAccess, StorageTaskClaim, StorageTaskClaimToken, StorageTaskCompletion,
    StorageTaskCompletionArtifact, StorageTaskCreateRequest, StorageTaskDurations,
    StorageTaskEvent, StorageTaskEventAppend, StorageTaskEventInput, StorageTaskEventPage,
    StorageTaskFailure, StorageTaskKind, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskPage, StorageTaskPageQuery,
    StorageTaskProgress, StorageTaskResultCounts, StorageTaskScopeSnapshot, StorageTaskStateUpdate,
    StorageTaskStatus, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    StorageVisibility, TaskExecutionStorage, TaskQueueStorage, UnifiedSearchClass,
    UnifiedSearchCollection, UnifiedSearchCursor, UnifiedSearchObject, UnifiedSearchQuery,
    UnifiedSearchStorage,
};
pub use hubuum_storage_core::{
    AuthenticatedToken, StorageCallSite, StorageExecution, StorageRevisionPrecondition,
};
pub(crate) use hubuum_storage_core::{
    BackupSnapshotStorage, StorageBackupOutput, StorageBackupOutputSummary, StorageBackupSnapshot,
    StorageBackupTaskArtifact, StorageExportOutput, StorageExportOutputSummary,
    StorageExportTaskArtifact, StorageImportTaskResult, StorageImportTaskResultPage,
};
pub(crate) use hubuum_storage_core::{ClassHistoryRecord, CollectionHistoryRecord};
pub(crate) use hubuum_storage_core::{StorageError, StorageErrorKind};
pub(crate) use imports::{
    ImportStorage, StorageImportApply, StorageImportApplyItem, StorageImportOperation,
    StorageImportPlanItem, StorageImportPreflight, StorageImportPreflightItem, StorageImportResult,
};
#[cfg(test)]
pub(crate) use memory::MemoryStorageModel;
pub(crate) use metrics::{
    EventMetricsSnapshot, ExportTemplateMetricIdentity, InventoryGaugeSnapshot,
    InventoryMetricsSnapshot, MetricsStorage, StoragePoolState, TaskGaugeAge, TaskGaugeCount,
    TaskGaugeLastTerminal, TaskGaugeSnapshot,
};
pub(crate) use object_relations::ObjectRelationStore;
pub(crate) use objects::ObjectStore;
pub(crate) use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
    EventHealthStorage, EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
    EventSubscriptionHealthSnapshot, OperationalStateStorage, OperationalStorageSnapshot,
    OperationalTaskActiveCounts, OperationalTaskKindCounts, OperationalTaskQueueSnapshot,
    OperationalTaskStatusCounts, OperationalTaskTerminalCounts, ReadinessSnapshot,
    TokenRetentionStorage,
};
pub(crate) use postgres::PostgresStorage;
