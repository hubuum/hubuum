use actix_web::web::Data;
use chrono::NaiveDateTime;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

use crate::events::{
    EventContext, EventDeliverySettings, EventFanoutSettings, EventRetentionSettings,
    MutationProvenance,
};
use crate::models::search::QueryOptions;
use crate::models::{MaintenanceState, TokenRetentionSettings};
use crate::permissions::AppContext;
use crate::storage::observed::{
    ObservedStorage, observe_infallible_storage_call, observe_storage_call,
};
use crate::storage::postgres::{PostgresPool, PostgresPoolSettings};
use crate::storage::{
    AuditEventStorage, AuthenticatedToken, AuthenticationAttempt, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupCollectionQuery, AuthorizationGroupGrant, AuthorizationGroupGrantPage,
    AuthorizationGroupMembershipQuery, AuthorizationGroupPage, AuthorizationObjectResource,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationPrincipalCollectionPageQuery,
    AuthorizationPrincipalCollectionQuery, AuthorizationResourceIds, AuthorizationStorage,
    BackupSnapshotStorage, BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage,
    CatalogStorage, ClassRelationStore, ClassStore, CollectionAuthorizationStorage,
    CollectionStore, ComputedFieldLifecycleStorage, ComputedObjectEnrichmentQuery,
    ComputedObjectListQuery, ComputedObjectPage, ComputedObjectStorage, EventArchive,
    EventDeliveryAdministrationStorage, EventDeliveryBatch, EventDeliveryClaim,
    EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage, EventHealthStorage,
    EventMetricsSnapshot, EventRetentionStorage, EventRetentionSummary, EventSubscriptionStorage,
    ExportQueryStorage, ExportTemplateHistoryRecord, ExportTemplateStorage, GroupStorage,
    HistoryAsOfQuery, HistoryListQuery, HistoryPage, HistoryPrincipalName, HistoryStorage,
    IdentityStorage, ImportStorage, InventoryGaugeSnapshot, InventoryStorage, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStore,
    ObjectRelationsTouchingIdsQuery, ObjectStore, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalStorageSnapshot,
    OperationalTaskQueueSnapshot, PostgresStorage, PrincipalStorage, ReadinessSnapshot,
    RelatedObjectsForRootsQuery, RelationGraphQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationQueryStorage, RelationTouchingQuery, RemoteTargetHistoryRecord,
    RemoteTargetStorage, RestoreStorage, StorageAuditEvent, StorageAuditEventListQuery,
    StorageBackend, StorageBackendDescriptor, StorageBackendKind, StorageBackupOutput,
    StorageBackupOutputSummary, StorageBackupSnapshot, StorageCallSite, StorageClass,
    StorageClassComputationState, StorageClassGraphRow, StorageClassRecord, StorageClassRelation,
    StorageCollection, StorageComputedFieldDefinition, StorageComputedFieldMutation,
    StorageComputedFieldPage, StorageComputedFieldRebuildRequest, StorageComputedObject,
    StorageDefaultAdminBootstrap, StorageError, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecution,
    StorageExportOutput, StorageExportOutputSummary, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDelete, StorageExportTemplateListQuery,
    StorageExportTemplatePage, StorageExportTemplateReplace, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageGroupCreate, StorageGroupListQuery, StorageGroupUpdate,
    StorageIdentity, StorageIdentityGroup, StorageIdentityPage, StorageIdentityScope,
    StorageIdentityScopeEnsure, StorageImportApply, StorageImportCollectionKey, StorageImportMode,
    StorageImportPlanItem, StorageImportPreflight, StorageImportResult,
    StorageImportTaskResultPage, StorageInventoryCounts, StorageLocalPasswordReset,
    StorageNotification, StorageObject, StorageObjectAggregatePage, StorageObjectGraphRow,
    StorageObjectRelation, StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate, StoragePoolState,
    StoragePrincipal, StoragePrincipalGroup, StoragePrincipalGroupListQuery,
    StoragePrincipalSettings, StoragePrincipalSettingsMutation, StorageQueryBudget,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPage, StorageRemoteTargetUpdate,
    StorageRestoreApply, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreJob, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageRevisionPrecondition, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountListItem, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountPoint, StorageServiceAccountUpdate,
    StorageSharedComputedFieldCreate, StorageSharedComputedFieldDelete,
    StorageSharedComputedFieldUpdate, StorageSyncedHuman, StorageTask, StorageTaskAccess,
    StorageTaskClaim, StorageTaskCompletion, StorageTaskCreateRequest, StorageTaskEventAppend,
    StorageTaskEventPage, StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskListQuery, StorageTaskOutputLookup, StorageTaskPage, StorageTaskPageQuery,
    StorageTaskStateUpdate, StorageTokenCreate, StorageTokenHashRevoke, StorageTokenListQuery,
    StorageTokenMetadata, StorageTokenObservation, StorageTokenRenew, StorageTokenRevoke,
    StorageUser, StorageUserCreate, StorageUserDelete, StorageUserListItem, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserPoint, StorageUserUpdate, TaskExecutionStorage,
    TaskGaugeSnapshot, TaskQueueStorage, TokenRetentionStorage, TokenStorage, UnifiedSearchClass,
    UnifiedSearchCollection, UnifiedSearchObject, UnifiedSearchQuery, UnifiedSearchStorage,
    UserStorage, WorkerNotificationStorage,
};
use crate::storage::{ClassHistoryRecord, CollectionHistoryRecord};
use async_trait::async_trait;

mod private {
    use super::StorageHandle;

    pub trait BackendAccess {
        fn storage_handle(&self) -> StorageHandle;
    }
}

/// An opaque handle to Hubuum's configured persistence backend.
///
/// Application code passes this handle to domain operations without selecting
/// a database implementation or handling a connection pool directly.
#[derive(Clone)]
pub struct StorageHandle {
    inner: Arc<StorageHandleInner>,
}

struct StorageHandleInner {
    implementation: BackendImplementation,
    resource_ports: ResourceStoragePorts,
}

enum BackendImplementation {
    Postgresql(PostgresStorage),
}

struct ResourceStoragePorts {
    collections: Arc<dyn CollectionStore>,
    classes: Arc<dyn ClassStore>,
    objects: Arc<dyn ObjectStore>,
    class_relations: Arc<dyn ClassRelationStore>,
    object_relations: Arc<dyn ObjectRelationStore>,
}

impl ResourceStoragePorts {
    fn observed<S>(storage: S) -> Self
    where
        S: StorageIdentity
            + CollectionStore
            + ClassStore
            + ObjectStore
            + ClassRelationStore
            + ObjectRelationStore
            + 'static,
    {
        let observed = Arc::new(ObservedStorage::new(storage));
        Self {
            collections: observed.clone(),
            classes: observed.clone(),
            objects: observed.clone(),
            class_relations: observed.clone(),
            object_relations: observed,
        }
    }
}

/// Dispatch one capability call to the selected complete backend.
///
/// Keeping the exhaustive match here means adding a selectable backend has one
/// dispatch change instead of one change per storage operation. The aggregate
/// [`StorageBackend`] bound still makes missing capability implementations a
/// compile error before a backend can be composed.
macro_rules! dispatch_backend {
    ($handle:expr, |$backend:ident| $call:expr) => {
        match &$handle.inner.implementation {
            BackendImplementation::Postgresql($backend) => $call,
        }
    };
}

mod api;
mod computed_fields;
mod diagnostics;
mod execution;
mod identity;
mod operations;
mod queries;
mod relations;
mod resources;
mod tasks;
mod workflows;

pub use api::StorageContext;
use api::assert_complete_storage_backend;
pub(crate) use api::storage_handle;

impl StorageHandle {
    #[cfg(any(test, feature = "integration-test-support", feature = "postgres-bench"))]
    pub(crate) fn postgres(pool: PostgresPool) -> Self {
        Self::from_postgres_backend(PostgresStorage::new(pool))
    }

    pub(crate) fn postgres_with_notification_pool_settings(
        pool: PostgresPool,
        notification_pool_settings: PostgresPoolSettings,
    ) -> Self {
        Self::from_postgres_backend(PostgresStorage::with_notification_pool_settings(
            pool,
            notification_pool_settings,
        ))
    }

    fn from_postgres_backend(backend: PostgresStorage) -> Self {
        let backend_kind = StorageBackendKind::Postgresql;
        assert_complete_storage_backend(&backend, backend_kind);
        let resource_ports = ResourceStoragePorts::observed(backend.clone());
        Self {
            inner: Arc::new(StorageHandleInner {
                implementation: BackendImplementation::Postgresql(backend),
                resource_ports,
            }),
        }
    }

    pub(crate) fn descriptor(&self) -> StorageBackendDescriptor {
        match &self.inner.implementation {
            BackendImplementation::Postgresql(_) => {
                StorageBackendDescriptor::new(StorageBackendKind::Postgresql)
            }
        }
    }

    fn backend_name(&self) -> &'static str {
        self.descriptor().kind().as_str()
    }

    pub(crate) fn collection_store(&self) -> Arc<dyn CollectionStore> {
        self.inner.resource_ports.collections.clone()
    }

    pub(crate) fn class_store(&self) -> Arc<dyn ClassStore> {
        self.inner.resource_ports.classes.clone()
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.inner.resource_ports.objects.clone()
    }

    pub(crate) fn class_relation_store(&self) -> Arc<dyn ClassRelationStore> {
        self.inner.resource_ports.class_relations.clone()
    }

    pub(crate) fn object_relation_store(&self) -> Arc<dyn ObjectRelationStore> {
        self.inner.resource_ports.object_relations.clone()
    }
}
