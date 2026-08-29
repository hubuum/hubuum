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
    AuditEventStorage, AuthenticationStorage, AuthorizationDataStorage, CatalogStorage,
    ClassRelationStorage, ClassStorage, CollectionAuthorizationQueryStorage, CollectionStorage,
    ComputedFieldStorage, ComputedObjectStorage, EventArchiveSink, EventConfigurationStorage,
    EventDeliveryAdministrationStorage, EventDeliveryWorkerStorage, EventFanoutStorage,
    EventRetentionStorage, ExportTemplateStorage, ExternalIdentityStorage, GroupMembershipStorage,
    GroupStorage, HistoryStorage, IdentityScopeStorage, InventoryStorage,
    LocalIdentityCredentialStorage, ObjectAggregateAuthorizer, ObjectAggregateStorage,
    ObjectRelationStorage, ObjectStorage, PrincipalStorage, RelationQueryStorage,
    RemoteTargetStorage, RestoreStorage, ServiceAccountStorage, StorageAuditEvent,
    StorageAuditEventFilters, StorageAuditEventListQuery, StorageAuthenticationAttempt,
    StorageAuthenticationCredential, StorageAuthenticationHuman, StorageAuthenticationIdentity,
    StorageAuthenticationPrincipal, StorageAuthenticationResourceScope,
    StorageAuthenticationTokenScope, StorageAuthenticationTokenScopeQuery,
    StorageAuthorizationClassResource, StorageAuthorizationCollection,
    StorageAuthorizationCollectionAccessQuery, StorageAuthorizationCollectionGrantListQuery,
    StorageAuthorizationCollectionGroupsPageQuery, StorageAuthorizationCollectionGroupsQuery,
    StorageAuthorizationCollectionVisibilityQuery, StorageAuthorizationCollectionsAccessQuery,
    StorageAuthorizationCollectionsQuery, StorageAuthorizationEffectiveGroupGrant,
    StorageAuthorizationGrant, StorageAuthorizationGrantDelete, StorageAuthorizationGrantKey,
    StorageAuthorizationGrantMutation, StorageAuthorizationGroup,
    StorageAuthorizationGroupCandidateQuery, StorageAuthorizationGroupCollectionQuery,
    StorageAuthorizationGroupGrant, StorageAuthorizationGroupMembershipQuery,
    StorageAuthorizationObjectResource, StorageAuthorizationPermission,
    StorageAuthorizationPermissionSet, StorageAuthorizationPermissionSetQuery,
    StorageAuthorizationPolicySnapshotRow, StorageAuthorizationPrincipal,
    StorageAuthorizationPrincipalCollectionPageQuery, StorageAuthorizationPrincipalCollectionQuery,
    StorageAuthorizationResourceIds, StorageBidirectionalRelatedObjectsQuery, StorageCapability,
    StorageCatalogListQuery, StorageClass, StorageClassComputationState, StorageClassCreate,
    StorageClassGraphRow, StorageClassRelation, StorageClassRelationCreate, StorageClassSelector,
    StorageClassUpdate, StorageClassWithCollection, StorageCollection, StorageCollectionCreate,
    StorageCollectionUpdate, StorageComputedFieldDefinition, StorageComputedFieldDefinitionInput,
    StorageComputedFieldDefinitionPatch, StorageComputedFieldError, StorageComputedFieldMutation,
    StorageComputedFieldRebuildRequest, StorageComputedFieldSelector,
    StorageComputedFieldVisibility, StorageComputedObject, StorageComputedObjectEnrichmentQuery,
    StorageComputedObjectListQuery, StorageComputedObjectPage, StorageComputedObjectProjection,
    StorageComputedObjectQueryOptions, StorageComputedObjectVisibility, StorageComputedScope,
    StorageDefaultAdminBootstrap, StorageEventDelivery, StorageEventDeliveryBatch,
    StorageEventDeliveryClaim, StorageEventDeliveryListQuery, StorageEventDeliverySink,
    StorageEventDeliverySubscription, StorageEventDeliveryWorkItem, StorageEventRetentionBatch,
    StorageEventRetentionBatchId, StorageEventRetentionSummary, StorageEventSink,
    StorageEventSinkCreate, StorageEventSinkDelete, StorageEventSinkListQuery,
    StorageEventSinkUpdate, StorageEventSubscription, StorageEventSubscriptionCreate,
    StorageEventSubscriptionDelete, StorageEventSubscriptionListQuery,
    StorageEventSubscriptionUpdate, StorageExportTemplate, StorageExportTemplateCreate,
    StorageExportTemplateDefinition, StorageExportTemplateDelete,
    StorageExportTemplateHistoryRecord, StorageExportTemplateListQuery,
    StorageExportTemplateReplace, StorageExternalGroup, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageGraphClass, StorageGraphObject, StorageGroupCreate,
    StorageGroupListQuery, StorageGroupMember, StorageGroupUpdate, StorageHistoryAsOfQuery,
    StorageHistoryCollectionScope, StorageHistoryListQuery, StorageHistoryMetadata,
    StorageHistoryOperation, StorageHistoryPrincipalName, StorageIdentityGroup,
    StorageIdentityScope, StorageIdentityScopeEnsure, StorageInventoryCounts,
    StorageLocalPasswordReset, StorageMutationOutcome, StorageNotification,
    StorageNotificationListener, StorageNotificationShutdown, StorageObject,
    StorageObjectAggregateAuthorization, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateDimension,
    StorageObjectAggregateMeasure, StorageObjectAggregateMeasureField,
    StorageObjectAggregateMeasureOperation, StorageObjectAggregateMeasureState,
    StorageObjectAggregatePage, StorageObjectAggregateQuery, StorageObjectAggregateRow,
    StorageObjectAggregateScalarField, StorageObjectAggregateSort, StorageObjectAggregateSpec,
    StorageObjectAggregateTarget, StorageObjectCreate, StorageObjectDataPatch,
    StorageObjectGraphRow, StorageObjectHistoryAsOfQuery, StorageObjectHistoryListQuery,
    StorageObjectHistoryRecord, StorageObjectRelation, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationEndpoint,
    StorageObjectRelationSelector, StorageObjectRelationsTouchingIdsQuery, StorageObjectSelector,
    StorageObjectUpdate, StorageObservation, StorageObserver, StoragePage,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate,
    StoragePreparedClassRelation, StoragePreparedObjectRelation, StoragePrincipal,
    StoragePrincipalGroup, StoragePrincipalGroupListQuery, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation, StoragePrincipalTokensRevoke, StorageQueryBudget,
    StorageRecordMetadata, StorageRelatedDirection, StorageRelatedObjectForRootRow,
    StorageRelatedObjectIncludeRow, StorageRelatedObjectsForRootsQuery, StorageRelatedSort,
    StorageRelationGraphQuery, StorageRelationIdsQuery, StorageRelationListQuery,
    StorageRelationTouchingQuery, StorageRemoteCallArtifactOutcome,
    StorageRemoteCallArtifactResponse, StorageRemoteCallArtifactTarget,
    StorageRemoteCallTaskArtifact, StorageRemoteTarget, StorageRemoteTargetCreate,
    StorageRemoteTargetDefinition, StorageRemoteTargetDelete, StorageRemoteTargetHistoryRecord,
    StorageRemoteTargetHttpMethod, StorageRemoteTargetInvocation, StorageRemoteTargetListQuery,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetSubjectType,
    StorageRemoteTargetTransport, StorageRemoteTargetUpdate, StorageResolvedClass,
    StorageResolvedClassRelation, StorageResolvedObject, StorageResolvedObjectRelation,
    StorageResourceScope, StorageRestoreApply, StorageRestoreArtifactSummary,
    StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot, StorageRestoreDocument,
    StorageRestoreDocumentMetadata, StorageRestoreDrainState, StorageRestoreFailure,
    StorageRestoreInitiator, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus, StorageRetainedEvent, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountDetails, StorageServiceAccountDisableOutcome,
    StorageServiceAccountListItem, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageSyncedHuman,
    StorageTask, StorageTaskAccess, StorageTaskActiveUpdate, StorageTaskChildListQuery,
    StorageTaskClaim, StorageTaskCompletion, StorageTaskCompletionArtifact,
    StorageTaskCreateRequest, StorageTaskDurations, StorageTaskEvent, StorageTaskEventAppend,
    StorageTaskEventInput, StorageTaskFailure, StorageTaskKind, StorageTaskLease,
    StorageTaskLeaseDuration, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskResultCounts, StorageTaskScopeSnapshot, StorageTaskStatus,
    StorageTaskTerminalUpdate, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenIssuancePolicy, StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata,
    StorageTokenObservation, StorageTokenRenew, StorageTokenRevoke, StorageUnifiedSearchCursor,
    StorageUnifiedSearchQuery, StorageUser, StorageUserAnonymize, StorageUserCreate,
    StorageUserDelete, StorageUserDetails, StorageUserListItem, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserUpdate, StorageVisibility, TaskExecutionStorage,
    TaskQueueStorage, TokenStorage, UnifiedSearchStorage, UserStorage, WorkerNotificationProvider,
};
pub(crate) use hubuum_storage_core::{
    BackupSnapshotStorage, StorageBackupHistorySection, StorageBackupOutput,
    StorageBackupOutputSummary, StorageBackupRow, StorageBackupSnapshot, StorageBackupStateSection,
    StorageBackupTaskArtifact, StorageExportOutput, StorageExportOutputSummary,
    StorageExportTaskArtifact, StorageExportTaskArtifactContent, StorageImportTaskResult,
};
pub use hubuum_storage_core::{
    ExecutionStorage, StorageAuthenticatedToken, StorageCallSite, StorageExecutionScope,
    StorageRevisionPrecondition, StorageRevisionTarget, StorageTransaction,
    StorageTransactionFuture, TransactionStorage, TransactionalClassRelations,
    TransactionalClasses, TransactionalCollections, TransactionalObjectRelations,
    TransactionalObjects, execute_event_retention_batch,
};
pub(crate) use hubuum_storage_core::{
    ImportStorage, StorageImportApply, StorageImportCollectionKey, StorageImportMode,
    StorageImportPlan, StorageImportPlanItem, StorageImportPreflight, StorageImportResult,
};
pub(crate) use hubuum_storage_core::{
    MetricsStorage, StorageEventMetricsSnapshot, StorageInventoryGaugeSnapshot,
    StorageTaskGaugeSnapshot,
};
pub(crate) use hubuum_storage_core::{StorageClassHistoryRecord, StorageCollectionHistoryRecord};
pub(crate) use hubuum_storage_core::{StorageError, StorageErrorKind};
pub(crate) use imports::ApplicationImportOperation;
#[cfg(test)]
pub(crate) use memory::MemoryStorageModel;
pub(crate) use notifications::spawn_storage_notification_listener;
pub(crate) use observed::{ApplicationStorageObserver, ObservedStorage};
pub(crate) use operational::{
    EventHealthStorage, OperationalStateStorage, StorageEventDeliveryHealthSnapshot,
    StorageEventDeliveryStatusSnapshot, StorageOperationalExportTemplateAuditEntry,
    StorageOperationalExportTemplateHealth, StorageOperationalTaskQueueSnapshot,
    StorageReadinessSnapshot, TokenRetentionStorage,
};
pub use registry::StorageBackendKind;
