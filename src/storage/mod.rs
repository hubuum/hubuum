mod context;
mod contract;
mod execution;
mod factory;
mod imports;
#[cfg(test)]
mod memory;
mod observed;
mod operational;
#[doc(hidden)]
pub mod postgres;
mod registry;

pub use context::StorageContext;
#[cfg(feature = "postgres-bench")]
#[doc(hidden)]
pub use context::StorageHandle as BenchmarkStorageContext;
pub(crate) use context::{StorageHandle, storage_handle};
pub(crate) use contract::{
    StorageBackend, StorageBackendDescriptor, StorageBackendKind, StorageIdentity,
};
pub use execution::{
    with_mutation_provenance, with_revision_precondition, with_storage_call_site,
    with_storage_call_site_send,
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
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupCollectionQuery, AuthorizationGroupGrant, AuthorizationGroupGrantPage,
    AuthorizationGroupMembershipQuery, AuthorizationGroupPage, AuthorizationObjectResource,
    AuthorizationPermission, AuthorizationPermissionSet, AuthorizationPermissionSetQuery,
    AuthorizationPolicySnapshotRow, AuthorizationPrincipal,
    AuthorizationPrincipalCollectionPageQuery, AuthorizationPrincipalCollectionQuery,
    AuthorizationResourceIds, AuthorizationStorage, BidirectionalRelatedObjectsQuery,
    CatalogListQuery, CatalogPage, CatalogStorage, ClassRelationStore, ClassStore,
    CollectionAuthorizationStorage, CollectionStore, ComputedFieldLifecycleStorage,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectProjection, ComputedObjectQueryOptions, ComputedObjectStorage,
    ComputedObjectVisibility, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryBatch,
    EventDeliveryClaim, EventDeliverySink, EventDeliveryStorage, EventDeliverySubscription,
    EventDeliveryWorkItem, EventFanoutStorage, EventRetentionStorage, EventRetentionSummary,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateHistoryRecord,
    ExportTemplateStorage, GroupStorage, HistoryAsOfQuery, HistoryCollectionScope,
    HistoryListQuery, HistoryMetadata, HistoryPage, HistoryPrincipalName, HistoryStorage,
    IdentityStorage, InventoryStorage, ObjectAggregateAuthorizationMode, ObjectAggregateAuthorizer,
    ObjectAggregateStorage, ObjectAggregateStorageQuery, ObjectHistoryAsOfQuery,
    ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStore,
    ObjectRelationsTouchingIdsQuery, ObjectStore, PrincipalStorage, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationPage, RelationQueryStorage,
    RelationTouchingQuery, RemoteTargetHistoryRecord, RemoteTargetStorage, RestoreStorage,
    RetainedEvent, StorageAuditEvent, StorageAuditEventFilters, StorageAuditEventListQuery,
    StorageClass, StorageClassComputationState, StorageClassCreate, StorageClassGraphRow,
    StorageClassRecord, StorageClassRelation, StorageClassRelationCreate, StorageClassSelector,
    StorageClassUpdate, StorageCollection, StorageCollectionCreate, StorageCollectionUpdate,
    StorageComputedFieldDefinition, StorageComputedFieldDefinitionContent,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldError, StorageComputedFieldMutation, StorageComputedFieldPage,
    StorageComputedFieldProvenance, StorageComputedFieldRebuildRequest,
    StorageComputedFieldSelector, StorageComputedFieldVisibility, StorageComputedObject,
    StorageComputedScope, StorageDefaultAdminBootstrap, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDefinition, StorageExportTemplateDelete,
    StorageExportTemplateListQuery, StorageExportTemplatePage, StorageExportTemplateReplace,
    StorageExternalGroup, StorageExternalPrincipalState, StorageExternalUserSync,
    StorageGraphClass, StorageGraphObject, StorageGroupCreate, StorageGroupListQuery,
    StorageGroupUpdate, StorageIdentityGroup, StorageIdentityPage, StorageIdentityScope,
    StorageIdentityScopeEnsure, StorageInventoryCounts, StorageLocalPasswordReset,
    StorageNotification, StorageObject, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateDimension,
    StorageObjectAggregateMeasure, StorageObjectAggregateMeasureField,
    StorageObjectAggregateMeasureOperation, StorageObjectAggregateMeasureState,
    StorageObjectAggregatePage, StorageObjectAggregateRow, StorageObjectAggregateScalarField,
    StorageObjectAggregateSort, StorageObjectAggregateSpec, StorageObjectAggregateTarget,
    StorageObjectCreate, StorageObjectDataPatch, StorageObjectGraphRow, StorageObjectRelation,
    StorageObjectRelationCreate, StorageObjectRelationCreateSelector,
    StorageObjectRelationEndpoint, StorageObjectRelationSelector, StorageObjectSelector,
    StorageObjectUpdate, StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate,
    StoragePreparedClassRelation, StoragePreparedObjectRelation, StoragePrincipal,
    StoragePrincipalGroup, StoragePrincipalGroupListQuery, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation, StorageQueryBudget, StorageRecordMetadata,
    StorageRelatedDirection, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRelatedSort, StorageRemoteCallArtifactOutcome, StorageRemoteCallArtifactResponse,
    StorageRemoteCallArtifactTarget, StorageRemoteCallTaskArtifact, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDefinition, StorageRemoteTargetDelete,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetPage,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate, StorageResolvedClass, StorageResolvedClassRelation,
    StorageResolvedObject, StorageResolvedObjectRelation, StorageResourceScope,
    StorageRestoreApply, StorageRestoreArtifactSummary, StorageRestoreCompletion,
    StorageRestoreCoordinatorSnapshot, StorageRestoreDocument, StorageRestoreDocumentMetadata,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreInitiator,
    StorageRestoreInstance, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus, StorageRestoreTimestamps,
    StorageServiceAccount, StorageServiceAccountCreate, StorageServiceAccountDisableOutcome,
    StorageServiceAccountListItem, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountPoint, StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageSyncedHuman,
    StorageTask, StorageTaskAccess, StorageTaskClaim, StorageTaskClaimToken, StorageTaskCompletion,
    StorageTaskCompletionArtifact, StorageTaskCreateRequest, StorageTaskDurations,
    StorageTaskEvent, StorageTaskEventAppend, StorageTaskEventInput, StorageTaskEventPage,
    StorageTaskFailure, StorageTaskKind, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskPage, StorageTaskPageQuery,
    StorageTaskProgress, StorageTaskResultCounts, StorageTaskScopeSnapshot, StorageTaskStateUpdate,
    StorageTaskStatus, StorageTokenCreate, StorageTokenHashRevoke, StorageTokenIssuancePolicy,
    StorageTokenListQuery, StorageTokenListState, StorageTokenMetadata, StorageTokenObservation,
    StorageTokenRenew, StorageTokenRevoke, StorageUser, StorageUserCreate, StorageUserDelete,
    StorageUserListItem, StorageUserListQuery, StorageUserPasswordUpdate, StorageUserPoint,
    StorageUserUpdate, StorageVisibility, TaskExecutionStorage, TaskQueueStorage, TokenStorage,
    UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchCursor, UnifiedSearchObject,
    UnifiedSearchQuery, UnifiedSearchStorage, UserStorage, WorkerNotificationStorage,
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
pub(crate) use hubuum_storage_core::{
    EventMetricsSnapshot, InventoryGaugeSnapshot, MetricsStorage, StoragePoolState,
    TaskGaugeSnapshot,
};
pub(crate) use hubuum_storage_core::{
    ImportStorage, StorageImportApply, StorageImportApplyItem, StorageImportCollectionKey,
    StorageImportMode, StorageImportOperation, StorageImportPlanItem, StorageImportPreflight,
    StorageImportPreflightItem, StorageImportResult,
};
pub(crate) use hubuum_storage_core::{StorageError, StorageErrorKind};
pub(crate) use imports::ApplicationImportOperation;
#[cfg(test)]
pub(crate) use memory::MemoryStorageModel;
#[cfg(test)]
pub(crate) use observed::ObservedStorage;
pub(crate) use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventHealthStorage,
    OperationalExportTemplateAuditEntry, OperationalExportTemplateHealth, OperationalStateStorage,
    OperationalStorageSnapshot, OperationalTaskQueueSnapshot, ReadinessSnapshot,
    TokenRetentionStorage,
};
pub(crate) use postgres::PostgresStorage;
