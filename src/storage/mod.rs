mod context;
mod contract;
mod database_diagnostics;
mod execution;
mod factory;
mod imports;
#[cfg(test)]
mod memory;
mod notifications;
mod observed;
mod operational;
mod registry;

pub use context::StorageContext;
#[cfg(feature = "postgres-bench")]
#[doc(hidden)]
pub use context::StorageHandle as BenchmarkStorageContext;
pub(crate) use context::{StorageHandle, storage_handle};
pub(crate) use contract::{CertifiedStorageBackend, StorageBackendDescriptor};
pub(crate) use database_diagnostics::{
    DatabaseDiagnosticsProvider, DatabasePoolAcquisitions, DatabasePoolCapacity,
    DatabasePoolConnections, DatabasePoolState, DatabaseStorageSnapshot,
};
pub use execution::{
    with_mutation_provenance, with_revision_precondition, with_storage_call_site,
    with_storage_call_site_send, with_storage_execution_scope, with_storage_execution_scope_send,
};
#[cfg(feature = "embedded-migrations")]
pub(crate) use factory::run_storage_migrations;
pub(crate) use factory::{StorageSettings, initialize_storage};
pub(crate) use hubuum_storage_core::{
    AuditEventStorage, AuthenticationAttempt, AuthenticationCredential, AuthenticationHuman,
    AuthenticationIdentity, AuthenticationPrincipal, AuthenticationResourceScope,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery, AuthorizationDataStorage,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupCandidateQuery, AuthorizationGroupCollectionQuery, AuthorizationGroupGrant,
    AuthorizationGroupMembershipQuery, AuthorizationObjectResource, AuthorizationPermission,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationPrincipalCollectionPageQuery,
    AuthorizationPrincipalCollectionQuery, AuthorizationResourceIds,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogStorage, ClassRelationStorage,
    ClassStorage, CollectionAuthorizationQueryStorage, CollectionStorage, ComputedFieldStorage,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectProjection, ComputedObjectQueryOptions, ComputedObjectStorage,
    ComputedObjectVisibility, EventArchiveSink, EventConfigurationStorage,
    EventDeliveryAdministrationStorage, EventDeliveryBatch, EventDeliveryClaim, EventDeliverySink,
    EventDeliverySubscription, EventDeliveryWorkItem, EventDeliveryWorkerStorage,
    EventFanoutStorage, EventRetentionBatch, EventRetentionBatchId, EventRetentionStorage,
    EventRetentionSummary, ExportTemplateHistoryRecord, ExportTemplateStorage,
    ExternalIdentityStorage, GroupMembershipStorage, GroupStorage, HistoryAsOfQuery,
    HistoryCollectionScope, HistoryListQuery, HistoryMetadata, HistoryPrincipalName,
    HistoryStorage, IdentityScopeStorage, InventoryStorage, LocalIdentityCredentialStorage,
    MutationOutcome, ObjectAggregateAuthorization, ObjectAggregateAuthorizer,
    ObjectAggregateStorage, ObjectAggregateStorageQuery, ObjectHistoryAsOfQuery,
    ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStorage,
    ObjectRelationsTouchingIdsQuery, ObjectStorage, PrincipalStorage, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationQueryStorage,
    RelationTouchingQuery, RemoteTargetHistoryRecord, RemoteTargetStorage, RestoreStorage,
    RetainedEvent, ServiceAccountStorage, StorageAuditEvent, StorageAuditEventFilters,
    StorageAuditEventListQuery, StorageCapability, StorageClass, StorageClassComputationState,
    StorageClassCreate, StorageClassGraphRow, StorageClassRecord, StorageClassRelation,
    StorageClassRelationCreate, StorageClassSelector, StorageClassUpdate, StorageCollection,
    StorageCollectionCreate, StorageCollectionUpdate, StorageComputedFieldDefinition,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldError, StorageComputedFieldMutation, StorageComputedFieldRebuildRequest,
    StorageComputedFieldSelector, StorageComputedFieldVisibility, StorageComputedObject,
    StorageComputedScope, StorageDefaultAdminBootstrap, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDefinition, StorageExportTemplateDelete,
    StorageExportTemplateListQuery, StorageExportTemplateReplace, StorageExternalGroup,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageGraphClass, StorageGraphObject,
    StorageGroupCreate, StorageGroupListQuery, StorageGroupMember, StorageGroupUpdate,
    StorageHistoryOperation, StorageIdentityGroup, StorageIdentityScope,
    StorageIdentityScopeEnsure, StorageInventoryCounts, StorageLocalPasswordReset,
    StorageNotification, StorageNotificationListener, StorageNotificationShutdown, StorageObject,
    StorageObjectAggregateAuthorizationCandidate, StorageObjectAggregateAuthorizationTarget,
    StorageObjectAggregateDimension, StorageObjectAggregateMeasure,
    StorageObjectAggregateMeasureField, StorageObjectAggregateMeasureOperation,
    StorageObjectAggregateMeasureState, StorageObjectAggregatePage, StorageObjectAggregateRow,
    StorageObjectAggregateScalarField, StorageObjectAggregateSort, StorageObjectAggregateSpec,
    StorageObjectAggregateTarget, StorageObjectCreate, StorageObjectDataPatch,
    StorageObjectGraphRow, StorageObjectRelation, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationEndpoint,
    StorageObjectRelationSelector, StorageObjectSelector, StorageObjectUpdate, StorageObservation,
    StorageObserver, StoragePage, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StoragePreparedClassRelation,
    StoragePreparedObjectRelation, StoragePrincipal, StoragePrincipalGroup,
    StoragePrincipalGroupListQuery, StoragePrincipalSettings, StoragePrincipalSettingsMutation,
    StoragePrincipalTokensRevoke, StorageQueryBudget, StorageRecordMetadata,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedSort, StorageRemoteCallArtifactOutcome, StorageRemoteCallArtifactResponse,
    StorageRemoteCallArtifactTarget, StorageRemoteCallTaskArtifact, StorageRemoteHttpMethod,
    StorageRemoteTarget, StorageRemoteTargetCreate, StorageRemoteTargetDefinition,
    StorageRemoteTargetDelete, StorageRemoteTargetInvocation, StorageRemoteTargetListQuery,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetSubjectType,
    StorageRemoteTargetTransport, StorageRemoteTargetUpdate, StorageResolvedClass,
    StorageResolvedClassRelation, StorageResolvedObject, StorageResolvedObjectRelation,
    StorageResourceScope, StorageRestoreApply, StorageRestoreArtifactSummary,
    StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot, StorageRestoreDocument,
    StorageRestoreDocumentMetadata, StorageRestoreDrainState, StorageRestoreFailure,
    StorageRestoreInitiator, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountDetails, StorageServiceAccountDisableOutcome,
    StorageServiceAccountListItem, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageSyncedHuman,
    StorageTask, StorageTaskAccess, StorageTaskActiveUpdate, StorageTaskClaim,
    StorageTaskCompletion, StorageTaskCompletionArtifact, StorageTaskCreateRequest,
    StorageTaskDurations, StorageTaskEvent, StorageTaskEventAppend, StorageTaskEventInput,
    StorageTaskFailure, StorageTaskKind, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskPageQuery, StorageTaskResultCounts,
    StorageTaskScopeSnapshot, StorageTaskStatus, StorageTaskTerminalUpdate, StorageTokenCreate,
    StorageTokenHashRevoke, StorageTokenIssuancePolicy, StorageTokenListQuery,
    StorageTokenListState, StorageTokenMetadata, StorageTokenObservation, StorageTokenRenew,
    StorageTokenRevoke, StorageUser, StorageUserAnonymize, StorageUserCreate, StorageUserDelete,
    StorageUserDetails, StorageUserListItem, StorageUserListQuery, StorageUserPasswordUpdate,
    StorageUserUpdate, StorageVisibility, TaskExecutionStorage, TaskQueueStorage, TokenStorage,
    UnifiedSearchCursor, UnifiedSearchQuery, UnifiedSearchStorage, UserStorage,
    WorkerNotificationProvider,
};
pub use hubuum_storage_core::{
    AuthenticatedToken, ExecutionStorage, StorageCallSite, StorageExecutionScope,
    StorageRevisionPrecondition, StorageRevisionTarget, StorageTransaction,
    StorageTransactionFuture, TransactionStorage, TransactionalClassRelations,
    TransactionalClasses, TransactionalCollections, TransactionalObjectRelations,
    TransactionalObjects, execute_event_retention_batch,
};
pub(crate) use hubuum_storage_core::{
    BackupSnapshotStorage, StorageBackupHistorySection, StorageBackupOutput,
    StorageBackupOutputSummary, StorageBackupRow, StorageBackupSnapshot, StorageBackupStateSection,
    StorageBackupTaskArtifact, StorageExportOutput, StorageExportOutputSummary,
    StorageExportTaskArtifact, StorageImportTaskResult,
};
pub(crate) use hubuum_storage_core::{ClassHistoryRecord, CollectionHistoryRecord};
pub(crate) use hubuum_storage_core::{
    EventMetricsSnapshot, InventoryGaugeSnapshot, MetricsStorage, TaskGaugeSnapshot,
};
pub(crate) use hubuum_storage_core::{
    ImportStorage, StorageImportApply, StorageImportCollectionKey, StorageImportMode,
    StorageImportPlan, StorageImportPlanItem, StorageImportPreflight, StorageImportResult,
};
pub(crate) use hubuum_storage_core::{StorageError, StorageErrorKind};
pub(crate) use imports::ApplicationImportOperation;
#[cfg(test)]
pub(crate) use memory::MemoryStorageModel;
pub(crate) use notifications::spawn_storage_notification_listener;
pub(crate) use observed::{ApplicationStorageObserver, ObservedStorage};
pub(crate) use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventHealthStorage,
    OperationalExportTemplateAuditEntry, OperationalExportTemplateHealth, OperationalStateStorage,
    OperationalTaskQueueSnapshot, ReadinessSnapshot, TokenRetentionStorage,
};
pub use registry::StorageBackendKind;
